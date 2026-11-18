"""从维护版本抽取的通用离线回归；行情、订单与账本均为测试构造。"""
import asyncio


import sys


import tempfile


import time


from decimal import Decimal


from pathlib import Path


from signal_ledger.core import (  # noqa: E402
    D, floor_to_step, quantize, now_str, parse_platform_active, parse_fill,
    parse_gate_contract_filters, parse_gate_fill, gate_text_id, gate_friendly,
    Database, Params, OrderResult, SimExchange, ExchangeApiError,
    ST_HOLDING, ST_CLOSED, ST_SKIPPED, ST_UNKNOWN, ST_PENDING,
    SKIP_SAME_SYMBOL, SKIP_FUSED, SKIP_NO_BITGET, SKIP_DAILY, SKIP_MAXPOS,
)


from signal_ledger.engine import TradingEngine, head_entry_qty  # noqa: E402


from signal_ledger.core import BitgetExchange  # noqa: E402




class FakeExchange(SimExchange):
    """假交易所：不联网。固定标记价和精度参数，并记录每一次“下单”调用。"""

    def __init__(self, marks, filters):
        super().__init__()
        self.marks = marks        # 交易对 -> 标记价
        self.filters = filters    # 精度参数
        self.orders = []          # 记录所有下单动作，测试用来断言

    async def fetch_mark_price(self, symbol):
        if symbol not in self.marks:
            self.last_price_error = "The symbol has been removed（测试数据）"
            return None
        return D(self.marks[symbol])

    async def fetch_lot_filters(self, symbol):
        if symbol not in self.marks:
            self.last_contract_error = "The symbol has been removed（测试数据）"
            return None
        return dict(self.filters)

    async def market_open(self, symbol, side, qty, client_id, margin_mode=None):
        self.orders.append(("open", symbol, side, str(qty), client_id))
        return OrderResult(True, simulated=True)

    async def place_stop_close_position(self, symbol, side, stop_price, client_id, qty=None):
        self.orders.append(("stop", symbol, side, str(stop_price), client_id))
        return OrderResult(True, simulated=True)

    async def cancel_all_orders(self, symbol):
        self.orders.append(("cancel_all", symbol, "", "", ""))
        return True

    async def market_close(self, symbol, side, qty, client_id):
        self.orders.append(("close", symbol, side, str(qty), client_id))
        return OrderResult(True, simulated=True)

    async def get_order_fills(self, symbol, order_id):
        return []   # 模拟盘不查交易所成交明细，手续费由引擎按费率估算


def make_env(marks, filters, total_funds=""):
    """建一套独立的测试环境：临时数据库 + 参数 + 引擎 + 假交易所"""
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    db = Database(Path(tmp.name) / "test.db", quiet=True)
    params = Params(db)
    params.save({"total_funds": total_funds})
    eng = TradingEngine(db=db, params=params)
    eng.exchange = FakeExchange(marks, filters)
    OPEN_DBS.append(db)
    return tmp, db, eng


STD_FILTERS = {"stepSize": "0.001", "minQty": "0.001",
               "minNotional": "5", "tickSize": "0.0001",
               "takerFeeRate": "0.0006", "symbolStatus": "normal"}


class TimeoutExchange(FakeExchange):
    """模拟开仓时网络超时：结果未知，程序绝对不能自动重试下单"""

    async def market_open(self, symbol, side, qty, client_id, margin_mode=None):
        self.orders.append(("open", symbol, side, str(qty), client_id))
        return OrderResult(None, error="连接交易所超时，订单结果未知")


class NoPositionExchange(FakeExchange):
    """模拟平仓时交易所返回“无持仓”（可能已被止损单平掉），应视为正常情况"""

    async def market_close(self, symbol, side, qty, client_id):
        self.orders.append(("close", symbol, side, str(qty), client_id))
        return OrderResult(True, error="交易所返回无持仓（可能已被止损单平掉）")


class FakeRealExchange:
    """假“实盘”交易所（不继承 SimExchange，让引擎走实盘分支，但完全不联网）。
    用来验证：实盘开/平仓后，用交易所成交明细回填实际手续费和实际盈亏。"""

    name = "假实盘"

    def __init__(self):
        self.orders = []

    async def fetch_mark_price(self, symbol):
        return D("1.5")

    async def fetch_lot_filters(self, symbol):
        return dict(STD_FILTERS)

    async def fetch_account_config(self, symbol, strict=False, fresh=False):
        return {"margin_mode": "crossed", "hold_mode": "one_way_mode"}

    async def set_leverage(self, symbol, leverage, hold_side=None):
        return True

    async def set_margin_mode(self, symbol, mode="isolated"):
        return True, ""

    async def ensure_margin_mode(self, symbol, mode="isolated", tries=3, delay=0.4):
        return True, ""

    async def market_open(self, symbol, side, qty, client_id, margin_mode=None):
        self.orders.append(("open", symbol, side, str(qty), client_id))
        return OrderResult(True, order_id="OPEN123")

    async def place_limit_open(self, symbol, side, qty, price, client_id, margin_mode=None):
        self.orders.append(("limit", symbol, side, str(qty), str(price), client_id))
        return OrderResult(True, order_id="LIMIT123")

    async def cancel_order(self, symbol, order_id):
        self.orders.append(("cancel_order", symbol, str(order_id), "", ""))
        return True

    async def place_stop_close_position(self, symbol, hold_side, stop_price, client_id, qty=None):
        self.orders.append(("stop", symbol, hold_side, str(stop_price), client_id))
        return OrderResult(True)

    async def cancel_all_orders(self, symbol):
        self.orders.append(("cancel_all", symbol, "", "", ""))
        return True

    async def cancel_plan_orders(self, symbol):
        self.orders.append(("cancel_plan", symbol, "", "", ""))
        return True

    async def fetch_open_orders(self, symbol=""):
        return []

    async def market_close(self, symbol, side, qty, client_id):
        self.orders.append(("close", symbol, side, str(qty), client_id))
        return OrderResult(True, order_id="CLOSE123")

    async def get_order_fills(self, symbol, order_id):
        now = time.time()
        if order_id == "OPEN123":
            return [{"fee": D("0.0712"), "profit": D("0"),
                     "price": D("1.5"), "volume": D("66.666"), "fee_coin": "USDT",
                     "order_id": "OPEN123", "side": "buy", "ts": now - 10}]
        if order_id == "CLOSE123":
            return [{"fee": D("0.0655"), "profit": D("2.1"),
                     "price": D("1.4685"), "volume": D("66.666"), "fee_coin": "USDT",
                     "order_id": "CLOSE123", "side": "sell", "ts": now + 1}]
        return []

    async def get_symbol_fills(self, symbol, start_ts, end_ts):
        return (await self.get_order_fills(symbol, "OPEN123")
                + await self.get_order_fills(symbol, "CLOSE123"))

    async def fetch_positions(self):
        return []   # 模拟“交易所已经止损平仓，当前没有持仓”


class RouteExchange(FakeRealExchange):
    """只支持指定交易对的假实盘交易所，用于验证 Gate 优先 / Bitget 回退路由。"""

    def __init__(self, symbols, tag):
        super().__init__()
        self.symbols = set(symbols)
        self.tag = tag

    async def fetch_lot_filters(self, symbol):
        if symbol not in self.symbols:
            self.last_contract_error = f"{self.tag} 没有 {symbol}"
            return None
        return dict(STD_FILTERS)

    async def fetch_mark_price(self, symbol):
        if symbol not in self.symbols:
            self.last_price_error = f"{self.tag} 没有 {symbol}"
            return None
        return D("1.5")

    async def fetch_positions(self):
        return []




class SlowLimitExchange(FakeRealExchange):
    """标记价 1.2（做多市价未到 0.99 限价），走限价挂单路径"""
    def __init__(self, fill_total="33.333", fill_avg="1.15"):
        super().__init__()
        self.filled = False
        self.fill_total = fill_total
        self.fill_avg = fill_avg

    async def fetch_mark_price(self, symbol):
        return D("1.2")

    async def fetch_positions(self):
        if not self.filled:
            return []
        return [{"symbol": "MLUSDT", "total": self.fill_total,
                 "openPriceAvg": self.fill_avg, "cTime": str(int(time.time() * 1000))}]

    async def get_order_fills(self, symbol, order_id):
        if order_id == "LIMIT123" and self.filled:
            return [{"fee": D("0.02"), "profit": D("0"), "price": D("0.99"),
                     "volume": D("13.333"), "fee_coin": "USDT",
                     "order_id": "LIMIT123", "side": "buy", "ts": time.time()}]
        return []


class VanishedOrderExchange(SlowLimitExchange):
    """限价单在交易所被撤销/查不到：本地必须标记跳过，不能一直显示“挂单中”"""

    def __init__(self):
        super().__init__()
        self.open_orders = []
        self.detail_state = "canceled"

    async def fetch_open_orders(self, symbol=""):
        return list(self.open_orders)

    async def fetch_order_detail(self, symbol, order_id):
        return {"orderId": order_id, "state": self.detail_state}






class GuardExchange(FakeRealExchange):
    """记录开仓前的逐仓/杠杆调用，验证实盘开仓路径带上了方向与显式逐仓"""

    def __init__(self):
        super().__init__()
        self.calls = []

    async def ensure_margin_mode(self, symbol, mode="isolated", tries=3, delay=0.4):
        self.calls.append(("mode", symbol, mode))
        return True, ""

    async def set_leverage(self, symbol, leverage, hold_side=None):
        self.calls.append(("lev", symbol, str(leverage), str(hold_side or "")))
        return True

    async def market_open(self, symbol, side, qty, client_id, margin_mode=None):
        self.calls.append(("open", symbol, side, str(margin_mode or "")))
        return OrderResult(True, order_id="")






def active_msg(sig_id, symbol, entry, stop, target="2.0", direction="long",
               source="趋势信号", first_qty=None):
    msg = {"type": "signal.active", "id": sig_id, "symbol": symbol,
           "direction": direction, "source": source, "entry": entry,
           "target": target, "stop": stop, "created": int(time.time()),
           "balance": 50}
    if first_qty is not None:
        msg["first_qty"] = first_qty
    return msg


def ended_msg(sig_id, profit="20", final="1.8"):
    return {"type": "signal.ended", "id": sig_id, "end_reason": "target",
            "end_note": "", "final_price": final, "profit_percent": profit,
            "text": "测试结束信号"}


def check(cond, why):
    if not cond:
        raise AssertionError(why)


async def test_params_validation():
    """参数校验：错误参数必须被拦下，并给出中文提示"""
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    db = Database(Path(tmp.name) / "test.db", quiet=True)
    OPEN_DBS.append(db)
    p = Params(db)

    ok, errs = p.save({"notional": "0", "leverage": "10"})
    check(not ok, "名义价值=0 应该校验失败")
    check(any("名义价值必须大于 0" in e for e in errs), "应提示「名义价值必须大于 0」")

    ok, errs = p.save({"notional": "100", "leverage": "200"})
    check(not ok, "杠杆=200 应该校验失败")
    check(any("杠杆必须在 1-125 之间" in e for e in errs), "应提示「杠杆必须在 1-125 之间」")

    ok, errs = p.save({"notional": "abc"})
    check(not ok, "名义价值=abc 应该校验失败")

    ok, errs = p.save({"notional": "100", "leverage": "10"})
    check(ok, "合法参数应该保存成功")
    check(db.get_setting("notional") == "100", "保存后应能读回")
    return "通过"


async def test_decimal_floor():
    """Decimal 精度：按 stepSize 向下取整，绝不允许向上取整"""
    check(floor_to_step("14.2857142857", "0.001") == Decimal("14.285"),
          "14.2857 向下取整应为 14.285（向上取整 14.286 会被交易所拒单）")
    check(floor_to_step("66.6666", "0.001") == Decimal("66.666"), "66.6666 应取 66.666")
    check(floor_to_step("0.0029", "0.001") == Decimal("0.002"), "0.0029 应取 0.002")
    check(floor_to_step("100", "0.1") == Decimal("100.0"), "整数步长应保持不变")
    check(quantize("1.4567", "0.001") == Decimal("1.456"), "价格取整应向下")
    return "通过"


async def test_duplicate_signal():
    """重复信号：同一个信号 id 只开一次仓"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    msg = active_msg("S1", "RLCUSDT", "1.5", "1.4")
    await eng.dispatch(msg)
    await eng.dispatch(dict(msg))   # 同一条信号再来一次

    opens = [o for o in eng.exchange.orders if o[0] == "open"]
    check(len(opens) == 1, f"重复信号不应重复开仓（实际开了 {len(opens)} 次）")
    sig = db.get_signal("S1")
    check(sig is not None and sig["status"] == ST_HOLDING, "信号应记录为持仓中")
    return "通过"


async def test_fixed_notional():
    """固定头寸：不同止损距离、不同来源，数量算法完全一样（只按名义价值算）"""
    marks = {"RLCUSDT": "1.5", "DOTUSDT": "1.5", "ADAUSDT": "7.0", "LINKUSDT": "1.5"}
    tmp, db, eng = make_env(marks, STD_FILTERS)

    # 止损很近的信号（风险小）与止损很远的信号（风险大），数量必须一样
    await eng.dispatch(active_msg("A1", "RLCUSDT", "1.5", "1.49", source="波动信号"))
    await eng.dispatch(active_msg("A2", "DOTUSDT", "1.5", "0.5", source="成交量信号"))
    # 标记价不同，数量按 100 / 7 计算并向下取整
    await eng.dispatch(active_msg("A3", "ADAUSDT", "7.0", "6.5", source="事件信号"))

    s1 = db.get_signal("A1")
    s2 = db.get_signal("A2")
    s3 = db.get_signal("A3")
    check(s1["status"] == ST_HOLDING and s2["status"] == ST_HOLDING, "两条信号都应持仓")
    check(s1["qty"] == "66.666", f"100÷1.5 向下取整应为 66.666（实际 {s1['qty']}）")
    check(s2["qty"] == s1["qty"],
          f"止损距离不同不应影响数量：A1={s1['qty']}，A2={s2['qty']}")
    check(s3["qty"] == "14.285", f"100÷7 向下取整应为 14.285（实际 {s3['qty']}）")

    # 加仓绝对禁止：同一个交易对再来一条新信号（不同 id），必须跳过
    await eng.dispatch(active_msg("A4", "RLCUSDT", "1.5", "1.4"))
    s4 = db.get_signal("A4")
    check(s4["status"] == ST_SKIPPED and s4["skip_reason"] == SKIP_SAME_SYMBOL,
          f"同一交易对重复信号应被跳过（实际状态 {s4['status']}，原因 {s4['skip_reason']}）")
    opens = [o for o in eng.exchange.orders if o[0] == "open"]
    check(len(opens) == 3, f"总开仓次数应为 3（实际 {len(opens)}）")

    # 限价开仓策略下不再用 0.5% 偏离过滤（限价单本身限制最多优于平台价 1%）
    await eng.dispatch(active_msg("A5", "LINKUSDT", "1.6", "1.4"))
    s5 = db.get_signal("A5")
    check(s5["status"] == ST_HOLDING,
          f"限价策略下偏离不应直接跳过（实际 {s5['status']}：{s5.get('skip_reason')}）")
    return "通过"


async def test_head_multiplier_open():
    """头仓仓位倍数：平台显示 0.5X 时按 15U×0.5=7.5U 开仓，后续马丁按绝对累计倍数补"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    eng.params.save({"notional": "15", "head_multiplier": "0.5"})
    errs = eng.params.validate({"head_multiplier": "0"})
    check(any("头仓仓位倍数" in e for e in errs), "头仓倍数=0 应校验失败")

    await eng.dispatch(active_msg("H1", "RLCUSDT", "1.5", "1.4"))
    sig = db.get_signal("H1")
    check(sig["status"] == ST_HOLDING, "应正常开仓")
    check(D(sig["qty"]) == D("5"), f"15U×0.5÷1.5 应开 5（实际 {sig['qty']}）")
    check(D(sig["notional"]) == D("15"), "基础名义价值仍应记录 15U（马丁按它算）")
    check(D(sig["head_multiplier"]) == D("0.5"), "应记录头仓倍数 0.5")
    check(D(sig["martin_multiplier"]) == D("0.5"),
          f"初始倍数应为 0.5（实际 {sig['martin_multiplier']}）")
    view = eng._position_view(db.get_signal("H1"))
    check(D(view["value_multiplier"]) == D("0.5"),
          f"页面倍数应显示 0.5X（实际 {view['value_multiplier']}）")

    # 平台给绝对累计倍数 0.9X（反推头仓≈0.54，按头仓 0.5 处理 → 绝对）：目标 15×0.9=13.5U，补 6U
    trail = {
        "type": "signal.trailed", "id": "H1", "symbol": "RLCUSDT", "direction": "long",
        "source": "成交量信号", "reason": "martingale", "target": 2.0, "stop": 1.45,
        "text": "RLCUSDT · 马丁加仓第 1 档：仓位 0.9X",
        "martingale": {"grades": [1], "avg": 1.52, "qty": 0.9, "price": 1.55},
    }
    await eng.dispatch(trail)
    sig = db.get_signal("H1")
    check(D(sig["qty"]) == D("9"), f"绝对口径应加到 9（实际 {sig['qty']}）")
    check(D(sig["martin_multiplier"]) == D("0.9"),
          f"加仓后倍数应为 0.9X（实际 {sig['martin_multiplier']}）")
    check(db.get_setting("platform_qty_mode") == "absolute", "应判定为绝对累计倍数")

    # 另一种口径：平台 qty 是相对头仓的倍数（反推头仓≈1）→ 绝对目标 = 头仓 × qty
    tmp2, db2, eng2 = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    eng2.params.save({"notional": "15", "head_multiplier": "0.5"})
    await eng2.dispatch(active_msg("H2", "RLCUSDT", "1.5", "1.4"))
    trail2 = {
        "type": "signal.trailed", "id": "H2", "symbol": "RLCUSDT", "direction": "long",
        "source": "成交量信号", "reason": "martingale", "target": 2.0, "stop": 1.45,
        "text": "RLCUSDT · 马丁加仓第 1 档：仓位 2X",
        "martingale": {"grades": [1], "avg": 1.75, "qty": 2.0, "price": 2.0},
    }
    await eng2.dispatch(trail2)
    sig2 = db2.get_signal("H2")
    check(D(sig2["qty"]) == D("10"),
          f"相对口径应按 0.5×2=1X 加到 10（实际 {sig2['qty']}）")
    check(D(sig2["martin_multiplier"]) == D("1.0"),
          f"加仓后倍数应为 1.0X（实际 {sig2['martin_multiplier']}）")
    check(db2.get_setting("platform_qty_mode") == "relative", "应判定为相对头仓倍数")

    # 平台推送 first_qty 优先于网页参数：全局 1，推送 0.5 → 15U×0.5=7.5U
    tmp3, db3, eng3 = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    eng3.params.save({"notional": "15", "head_multiplier": "1"})
    await eng3.dispatch(active_msg("H3", "RLCUSDT", "1.5", "1.4", first_qty=0.5))
    sig3 = db3.get_signal("H3")
    check(D(sig3["qty"]) == D("5"),
          f"平台 first_qty=0.5 应开 5（实际 {sig3['qty']}）")
    check(D(sig3["head_multiplier"]) == D("0.5"),
          f"应记录平台首仓 0.5（实际 {sig3['head_multiplier']}）")

    # 平台推送 1.0 时不能被网页参数 0.5 误减半
    tmp4, db4, eng4 = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    eng4.params.save({"notional": "15", "head_multiplier": "0.5"})
    await eng4.dispatch(active_msg("H4", "RLCUSDT", "1.5", "1.4", first_qty=1.0))
    sig4 = db4.get_signal("H4")
    check(D(sig4["qty"]) == D("10"),
          f"平台 first_qty=1.0 应开 10（实际 {sig4['qty']}）")
    check(D(sig4["head_multiplier"]) == D("1.0"),
          f"应记录平台首仓 1.0（实际 {sig4['head_multiplier']}）")

    # first_qty 非法/缺失才回退网页参数
    tmp5, db5, eng5 = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    eng5.params.save({"notional": "15", "head_multiplier": "0.5"})
    await eng5.dispatch(active_msg("H5", "RLCUSDT", "1.5", "1.4", first_qty=0))
    sig5 = db5.get_signal("H5")
    check(D(sig5["qty"]) == D("5") and D(sig5["head_multiplier"]) == D("0.5"),
          f"first_qty 非法应回退网页参数 0.5（实际 {sig5['qty']}/{sig5['head_multiplier']}）")
    return "通过"


async def test_head_min_one_lot():
    """头仓金额连最小一手都不够时，按最小一手凑整开仓"""
    # 模拟 PIEVERSE：1 张=10 币，价格 1.6116U，1 张约 16.12U；15U 头仓不够 1 张
    filters = {"stepSize": "10", "minQty": "10", "minNotional": "5",
               "tickSize": "0.0001", "takerFeeRate": "0.00075", "symbolStatus": "normal"}
    tmp, db, eng = make_env({"PIEVERSEUSDT": "1.6116"}, filters)
    eng.params.save({"notional": "30", "head_multiplier": "1"})
    await eng.dispatch(active_msg("P1", "PIEVERSEUSDT", "1.6116", "1.5", first_qty=0.5))
    sig = db.get_signal("P1")
    check(sig["status"] == ST_HOLDING,
          f"应正常开仓（实际 {sig['status']}：{sig.get('skip_reason')}）")
    check(D(sig["qty"]) == D("10"), f"应凑最小 1 张=10 币（实际 {sig['qty']}）")
    check("凑整" in (sig.get("note") or ""),
          f"备注应说明按最小一手凑整（实际 {sig.get('note')}）")
    opens = [o for o in eng.exchange.orders if o[0] == "open"]
    check(len(opens) == 1 and D(opens[0][3]) == D("10"),
          f"应下 1 手 10 币（实际 {opens}）")

    # 正常够 1 张时不能放大：15U 目标、1 张 10U → 只开 1 张
    filters2 = {"stepSize": "1", "minQty": "1", "minNotional": "5",
                "tickSize": "0.0001", "takerFeeRate": "0.00075", "symbolStatus": "normal"}
    tmp2, db2, eng2 = make_env({"JSTUSDT": "10"}, filters2)
    eng2.params.save({"notional": "30"})
    await eng2.dispatch(active_msg("J1", "JSTUSDT", "10", "9", first_qty=0.5))
    sig2 = db2.get_signal("J1")
    check(D(sig2["qty"]) == D("1"), f"15U÷10U 应开 1 张（实际 {sig2['qty']}）")
    check("凑整" not in (sig2.get("note") or ""), "正常够 1 张时不应标记凑整")
    return "通过"


async def test_end_close():
    """结束信号：先撤单，再按本地数量市价平仓，并记录手续费、毛盈亏、净盈亏、仓位时间"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    await eng.dispatch(active_msg("E1", "RLCUSDT", "1.5", "1.4"))
    # 还在持仓时，已付的开仓手续费也应该计入“今日手续费”
    stats_open = db.today_stats()
    check(stats_open["fees"] == D("0.0600"),
          f"持仓中的开仓手续费应计入今日手续费（实际 {stats_open['fees']}）")
    await eng.dispatch(ended_msg("E1", profit="20", final="1.8"))

    sig = db.get_signal("E1")
    check(sig["status"] == ST_CLOSED, f"收到结束信号应平仓（实际 {sig['status']}）")
    check(sig["profit_percent"] == "20", "收益% 应写入数据库")
    acts = eng.exchange.orders
    check(any(a[0] == "cancel_all" and a[1] == "RLCUSDT" for a in acts), "平仓前必须撤销所有挂单")
    closes = [a for a in acts if a[0] == "close"]
    check(len(closes) == 1 and closes[0][3] == "66.666",
          f"应按本地记录数量 66.666 平仓（实际 {[a[3] for a in closes]}）")

    # 手续费与盈亏：数量 66.666，开仓均价 1.5，吃单费率 0.06%，平仓价 1.8
    # 开仓费 = 66.666×1.5×0.0006 = 0.0600；平仓费 = 66.666×1.8×0.0006 = 0.0720
    # 毛盈亏 = 66.666 × (1.8 − 1.5) = 19.9998；净盈亏 = 19.9998 − 0.0600 − 0.0720 = 19.8678
    check(sig["open_fee"] == "0.0600", f"开仓手续费应为 0.0600（实际 {sig['open_fee']}）")
    check(sig["close_fee"] == "0.0720", f"平仓手续费应为 0.0720（实际 {sig['close_fee']}）")
    check(sig["gross_pnl"] == "19.9998", f"毛盈亏应为 19.9998（实际 {sig['gross_pnl']}）")
    check(sig["net_pnl"] == "19.8678", f"净盈亏应为 19.8678（实际 {sig['net_pnl']}）")
    stats_closed = db.today_stats()
    check(stats_closed["fees"] == D("0.1320"),
          f"平仓后今日手续费应为开仓 0.0600 + 平仓 0.0720 = 0.1320（实际 {stats_closed['fees']}）")
    check("模拟" in (sig["fee_note"] or ""), "模拟盘手续费应标注为估算")

    # 仓位时间
    check(len(sig["open_time"] or "") == 19, "应记录开仓时间")
    check(len(sig["close_time"] or "") == 19, "应记录平仓时间")
    check((sig["close_ts"] or 0) >= (sig["open_ts"] or 0), "平仓时间不应早于开仓时间")
    return "通过"


async def test_history():
    """历史仓位：能查到已平仓记录，包含净盈亏和开平仓时间"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    await eng.dispatch(active_msg("H1", "RLCUSDT", "1.5", "1.4"))
    await eng.dispatch(ended_msg("H1", profit="10", final="1.6"))

    hist = db.closed_history()
    check(len(hist) == 1 and hist[0]["id"] == "H1", f"历史仓位应包含 H1（实际 {[h['id'] for h in hist]}）")
    check(db.closed_count() == 1, f"历史仓位总数应为 1（实际 {db.closed_count()}）")
    h = hist[0]
    check(h["net_pnl"] != "", "历史仓位应有净盈亏")
    check(len(h["open_time"]) == 19 and len(h["close_time"]) == 19, "历史仓位应有开仓和平仓时间")
    check(h["qty"] == "66.666", "历史仓位应保留数量")
    return "通过"


async def test_receipt_reconcile():
    """回执对账：平台回执不可靠，既不自动平仓也不告警；刚开仓的信号也不能误平"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    conn_t = time.time()
    eng.conn_established_at = conn_t

    # 模拟：连接建立之前就有两条持仓记录
    db.upsert_signal(id="OLD1", symbol="RLCUSDT", direction="long", qty="66.666",
                     entry="1.5", stop="1.4", target="1.8", status=ST_HOLDING,
                     open_time=now_str(), open_ts=conn_t - 100)
    db.upsert_signal(id="NEW1", symbol="DOTUSDT", direction="long", qty="66.666",
                     entry="1.5", stop="1.4", target="1.8", status=ST_HOLDING,
                     open_time=now_str(), open_ts=conn_t + 5)

    # 服务器回执只给 NEW1。平台回执经常不完整，不能据此平掉 OLD1。
    await eng.dispatch({"type": "signal.receipt", "count": 1, "ids": ["NEW1"]})

    old1 = db.get_signal("OLD1")
    new1 = db.get_signal("NEW1")
    check(old1["status"] == ST_HOLDING,
          f"回执未包含的 OLD1 不应自动平仓（实际 {old1['status']}）")
    check(new1["status"] == ST_HOLDING,
          f"刚开仓的 NEW1 不应被误平（实际 {new1['status']}）")
    closes = [a for a in eng.exchange.orders if a[0] == "close"]
    check(len(closes) == 0, f"回执对账不应下平仓单（实际下了 {len(closes)} 次）")
    alerts = db.get_alerts(10)
    check(not any("需人工核对" in a["msg"] and "回执" in a["msg"] for a in alerts),
          "回执未包含持仓时不应再产生人工核对告警")
    return "通过"


async def test_fuse():
    """单日亏损熔断：启用时达标拦新仓；关闭后不再熔断（用户可自行关闭）"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5", "DOTUSDT": "1.5"}, STD_FILTERS, total_funds="100")
    # 造两条今天已平仓、各亏 2% 的记录：合计亏 4 USDT（名义 100 × 2% × 2）
    for i in ("F1", "F2"):
        db.upsert_signal(id=i, symbol="RLCUSDT", direction="long", qty="66.666",
                         notional="100", status=ST_CLOSED, profit_percent="-2",
                         open_time=now_str(), open_ts=time.time(),
                         close_time=now_str(), close_ts=time.time())
    check(eng._is_fused(), "亏损 4 USDT ≥ 100×3%=3 USDT，应触发熔断")

    # 新信号应被熔断拦下
    await eng.dispatch(active_msg("F3", "RLCUSDT", "1.5", "1.4"))
    s3 = db.get_signal("F3")
    check(s3["status"] == ST_SKIPPED and s3["skip_reason"] == SKIP_FUSED,
          f"熔断期间新信号应跳过（实际 {s3['status']}：{s3['skip_reason']}）")

    # 资金改成 1000 后不再熔断
    eng.params.save({"total_funds": "1000"})
    check(not eng._is_fused(), "总资金 1000 时亏损 4 USDT 不应熔断")


    eng.params.save({"total_funds": "100", "fuse_enabled": "0"})
    check(not eng._is_fused(), "关闭熔断后不应触发熔断")
    await eng.dispatch(active_msg("F4", "DOTUSDT", "1.5", "1.4"))
    s4 = db.get_signal("F4")
    check(s4["status"] == ST_HOLDING,
          f"关闭熔断后新信号应正常开仓（实际 {s4['status']}：{s4.get('skip_reason', '')}）")
    return "通过"


async def test_timeout_unknown():
    """下单超时：结果未知时绝不自动重试，记录“待核对”并告警"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    eng.exchange = TimeoutExchange({"RLCUSDT": "1.5"}, STD_FILTERS)
    await eng.dispatch(active_msg("T1", "RLCUSDT", "1.5", "1.4"))
    # 即使同一条信号再来，也不能重新下单
    await eng.dispatch(active_msg("T1", "RLCUSDT", "1.5", "1.4"))

    sig = db.get_signal("T1")
    check(sig["status"] == ST_UNKNOWN,
          f"下单超时应记录为「待核对」（实际 {sig['status']}）")
    opens = [o for o in eng.exchange.orders if o[0] == "open"]
    check(len(opens) == 1, f"结果未知后绝不能自动重试下单（实际下了 {len(opens)} 次）")
    alerts = db.get_alerts(10)
    check(any("结果未知" in a["msg"] for a in alerts), "应产生红色告警，提醒人工核对")
    return "通过"


async def test_close_no_position():
    """平仓时交易所返回“无持仓”：这是正常情况，不应当成错误告警"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    eng.exchange = NoPositionExchange({"RLCUSDT": "1.5"}, STD_FILTERS)
    await eng.dispatch(active_msg("N1", "RLCUSDT", "1.5", "1.4"))
    await eng.dispatch(ended_msg("N1"))

    sig = db.get_signal("N1")
    check(sig["status"] == ST_CLOSED, f"无持仓时应按已平仓记录（实际 {sig['status']}）")
    alerts = [a for a in db.get_alerts(10) if a["level"] == "alert" and "N1" in a["msg"]]
    check(len(alerts) == 0, "无持仓被止损单平掉是正常情况，不应该报警")
    return "通过"


async def test_real_fee_enrichment():
    """实盘手续费回填：开/平仓后从交易所成交明细取实际手续费和实际盈亏，重算净盈亏"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    # 切换到“假实盘”（不联网）：参数里填入假密钥，引擎才会走实盘分支
    eng.params.save({"sim_mode": "0", "exchange_api_key": "k",
                     "exchange_secret": "s", "exchange_passphrase": "p"})
    eng.exchange = FakeRealExchange()
    eng._exchange_creds = ("k", "s", "p")   # 告诉引擎“当前连接就是这套密钥”，避免重建真实连接

    await eng.dispatch(active_msg("R1", "RLCUSDT", "1.5", "1.4"))
    await asyncio.sleep(1.6)          # 等开仓手续费回填
    sig = db.get_signal("R1")
    check(sig["status"] == ST_HOLDING, "实盘开仓应成功")
    check(sig["open_fee"] == "0.0712", f"开仓手续费应回填为交易所实际值 0.0712（实际 {sig['open_fee']}）")
    check("实际" in (sig["fee_note"] or ""), "手续费说明应标注为交易所实际值")

    await eng.dispatch(ended_msg("R1", profit="20", final="1.8"))
    await asyncio.sleep(3.2)          # 等平仓手续费和实际盈亏回填
    sig = db.get_signal("R1")
    check(sig["status"] == ST_CLOSED, "实盘平仓应成功")
    check(sig["close_fee"] == "0.0655", f"平仓手续费应回填为 0.0655（实际 {sig['close_fee']}）")
    check(D(sig["gross_pnl"]) == D("2.1000"), f"毛盈亏应取交易所实际值 2.1000（实际 {sig['gross_pnl']}）")
    check(sig["net_pnl"] == "1.9633",
          f"净盈亏应为 2.1 − 0.0712 − 0.0655 = 1.9633（实际 {sig['net_pnl']}）")
    check(D(sig["final_price"]) == D("1.4685"),
          f"平仓价应回填为交易所成交加权均价 1.4685（实际 {sig['final_price']}）")
    stats = db.today_stats()
    check(D(stats["realized_pnl"]) == D("2.1"),
          f"今日已实现应按交易所实际毛盈亏统计为 2.1（实际 {stats['realized_pnl']}）")
    check(D(stats["net_pnl"]) == D("1.9633"),
          f"今日净盈亏应为 1.9633（实际 {stats['net_pnl']}）")
    return "通过"


async def test_funding():
    """资金费：多头正费率支付、空头正费率收取；计入未实现/已实现净盈亏；同一结算时间不重复扣"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5", "DOTUSDT": "1.5"}, STD_FILTERS)
    await eng.dispatch(active_msg("FU1", "RLCUSDT", "1.5", "1.4"))
    sig = db.get_signal("FU1")
    ts = time.time() - 60
    await eng.apply_funding(sig, ts, D("0.01"), D("1.5"))
    sig = db.get_signal("FU1")
    # 66.666 × 1.5 × 0.01 = 0.99999 → 1.0000（多头正费率=支付）
    check(D(sig["funding_fee"]) == D("1.0"),
          f"多头正费率应支付约 1U（实际 {sig['funding_fee']}）")

    await eng.apply_funding(sig, ts, D("0.01"), D("1.5"))
    sig = db.get_signal("FU1")
    check(D(sig["funding_fee"]) == D("1.0"), "同一结算时间不能重复扣资金费")

    view = eng._position_view(sig)
    # 未实现 = 0 − 开仓费 0.0600 − 资金费 1.0000 = −1.0600
    check(D(view["unrealized_pnl"]) == D("-1.06"),
          f"未实现净盈亏应扣除资金费（实际 {view['unrealized_pnl']}）")
    snap = eng.snapshot()
    check(D(snap.get("unrealized_pnl", "0")) == D("-1.06"),
          f"顶部未实现盈亏合计应为 −1.06（实际 {snap.get('unrealized_pnl')}）")
    check(D(snap.get("total_realized_pnl", "0")) == D("0"),
          f"还没平仓时总已实现盈亏应为 0（实际 {snap.get('total_realized_pnl')}）")
    check(D(db.today_stats()["funding"]) == D("1.0"),
          f"今日资金费统计应约 1U（实际 {db.today_stats()['funding']}）")

    await eng.dispatch(ended_msg("FU1", profit="20", final="1.8"))
    sig = db.get_signal("FU1")
    # 毛 19.9998 − 开仓费 0.0600 − 平仓费 0.0720 − 资金费 1.0000 = 18.8678
    check(sig["net_pnl"] == "18.8678",
          f"已实现净盈亏应扣除资金费（实际 {sig['net_pnl']}）")
    check(D(db.total_realized_pnl()) == D("18.8678"),
          f"总已实现盈亏应为 18.8678（实际 {db.total_realized_pnl()}）")
    snap_closed = eng.snapshot()
    check(D(snap_closed.get("total_realized_pnl", "0")) == D("18.8678"),
          f"顶部总已实现盈亏应为 18.8678（实际 {snap_closed.get('total_realized_pnl')}）")

    # 空头：正费率由多头支付给空头，资金费应为负（收取）
    await eng.dispatch(active_msg("FU2", "DOTUSDT", "1.5", "1.6",
                                  target="1.2", direction="short"))
    s2 = db.get_signal("FU2")
    await eng.apply_funding(s2, ts, D("0.001"), D("1.5"))
    s2 = db.get_signal("FU2")
    check(D(s2["funding_fee"]) < 0,
          f"空头正费率应收取资金费（实际 {s2['funding_fee']}）")
    return "通过"


async def test_min_notional():
    """名义价值小于交易所最小要求时：放弃信号，不下单"""
    filters = {"stepSize": "0.001", "minQty": "0.001",
               "minNotional": "200", "tickSize": "0.0001"}
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, filters)
    await eng.dispatch(active_msg("M1", "RLCUSDT", "1.5", "1.4"))

    sig = db.get_signal("M1")
    check(sig["status"] == ST_SKIPPED and sig["skip_reason"] == "数量或名义价值不达标",
          f"应跳过并记录原因（实际 {sig['status']}：{sig['skip_reason']}）")
    opens = [o for o in eng.exchange.orders if o[0] == "open"]
    check(len(opens) == 0, "名义价值不达标时不应下单")
    return "通过"


async def test_no_bitget_symbol():
    """Bitget 不存在的合约：即使是模拟盘，也必须跳过，绝不能假装开仓"""
    tmp, db, eng = make_env({"BTCUSDT": "80000"}, STD_FILTERS)
    await eng.dispatch(active_msg("G1", "GTCUSDT", "0.24713", "0.2710"))
    sig = db.get_signal("G1")
    check(sig["status"] == ST_SKIPPED and sig["skip_reason"] == SKIP_NO_BITGET,
          f"Bitget 不存在的合约应跳过（实际 {sig['status']}：{sig['skip_reason']}）")
    opens = [o for o in eng.exchange.orders if o[0] == "open"]
    check(len(opens) == 0, "不存在的合约绝不能假装开仓")
    return "通过"


async def test_martingale():
    """马丁加仓：按平台层数和累计仓位倍数同步加仓，并更新均价、手续费、止损"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    await eng.dispatch(active_msg("M1", "RLCUSDT", "1.5", "1.4"))
    trail = {
        "type": "signal.trailed", "id": "M1", "symbol": "RLCUSDT", "direction": "long",
        "source": "费率信号", "reason": "martingale", "target": 2.1, "stop": 1.42,
        "text": "RLCUSDT · 马丁加仓第 1 档：均价 1.48、仓位 1.2X",
        "martingale": {"grades": [1], "avg": 1.48, "qty": 1.2, "breached": False, "price": 1.5},
    }
    await eng.dispatch(trail)

    sig = db.get_signal("M1")
    # 按仓位价值计算：目标价值 = 100U × 1.2X = 120U；当前 66.666×1.5 ≈ 99.999U
    # 需增加约 20.001U，数量 = 20.001 ÷ 1.5 = 13.334（向下取整）；总数量 80.000
    check(sig["qty"] == "80.000", f"加仓后总数量应为 80.000（实际 {sig['qty']}）")
    check(int(sig["martin_layers"]) == 1, f"应记录加仓 1 层（实际 {sig['martin_layers']}）")
    check(D(sig["martin_multiplier"]) == D("1.2"),
          f"仓位倍数应为 1.2X（实际 {sig['martin_multiplier']}）")
    check(D(sig["avg_entry"]) == D("1.5"), f"自己账户均价应为 1.5（实际 {sig['avg_entry']}）")
    check(sig["open_fee"] == "0.0720", f"累计开仓手续费应为 0.0720（实际 {sig['open_fee']}）")
    check(db.today_stats()["fees"] == D("0.0720"),
          "加仓后的累计手续费应计入今日手续费")
    check(D(sig["stop"]) == D("1.42"), f"止损应更新为平台新值 1.42（实际 {sig['stop']}）")
    check(D(sig["target"]) == D("2.1"), f"止盈应更新为平台新值 2.1（实际 {sig['target']}）")
    view = eng._position_view(db.get_signal("M1"))
    check(D(view["value_multiplier"]) == D("1.2"),
          f"页面倍数应按仓位价值计算（80.000×1.5÷100 = 1.2X，实际 {view['value_multiplier']}）")

    adds = [o for o in eng.exchange.orders if o[0] == "open" and "_add1" in o[4]]
    check(len(adds) == 1 and adds[0][3] == "13.334",
          f"应同步加仓 13.334（实际 {[a[3] for a in adds]}）")
    stops = [o for o in eng.exchange.orders if o[0] == "stop"]
    check(len(stops) == 2, f"应有初次止损 + 加仓后新止损共 2 次挂单（实际 {len(stops)} 次）")

    # 同一条马丁消息重复推送：不能重复加仓
    await eng.dispatch(dict(trail))
    sig2 = db.get_signal("M1")
    adds2 = [o for o in eng.exchange.orders if o[0] == "open" and "_add1" in o[4]]
    check(len(adds2) == 1, "重复的马丁加仓消息不能重复下单")
    check(sig2["qty"] == "80.000", "重复消息不应改变总数量")

    # 结束时按加仓后的总数量平仓
    await eng.dispatch(ended_msg("M1", profit="20", final="1.8"))
    sig3 = db.get_signal("M1")
    closes = [o for o in eng.exchange.orders if o[0] == "close"]
    check(len(closes) == 1 and closes[0][3] == "80.000",
          f"应按总数量 80.000 平仓（实际 {[c[3] for c in closes]}）")
    check(sig3["status"] == ST_CLOSED, "加仓后的信号应能正常平仓")
    check(sig3["net_pnl"] == "23.8416", f"净盈亏应为 23.8416（实际 {sig3['net_pnl']}）")
    return "通过"


async def test_martingale_below_min():
    """马丁差额低于 Bitget 5U：不加仓，但记录平台目标倍数并同步止损止盈"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    eng.params.save({"notional": "20"})
    await eng.dispatch(active_msg("M2", "RLCUSDT", "1.5", "1.4"))
    sig = db.get_signal("M2")
    qty0 = sig["qty"]
    trail = {
        "type": "signal.trailed", "id": "M2", "symbol": "RLCUSDT", "direction": "long",
        "source": "费率信号", "reason": "martingale", "target": "1.55", "stop": "1.45",
        "text": "RLCUSDT · 马丁加仓第 1 档：仓位 1.2346343959X",
        "martingale": {"grades": [1], "avg": 1.5, "qty": "1.2346343959", "price": 1.5},
    }
    await eng.dispatch(trail)
    sig = db.get_signal("M2")
    check(sig["qty"] == qty0, f"差额低于 5U 不应加仓（实际数量 {sig['qty']}）")
    check(int(sig["martin_layers"]) == 0, "未加仓时不应增加层数")
    check(D(sig["platform_multiplier"]) == D("1.2346343959"),
          f"应记录平台目标倍数 1.2346343959（实际 {sig['platform_multiplier']}）")
    check("未加仓" in (sig["note"] or "") and "5" in (sig["note"] or ""),
          f"备注应说明未加仓和 5U 限制（实际 {sig['note']}）")
    check(D(sig["stop"]) == D("1.45"), f"止损仍应同步为 1.45（实际 {sig['stop']}）")
    check(D(sig["target"]) == D("1.55"), f"止盈仍应同步为 1.55（实际 {sig['target']}）")
    adds = [o for o in eng.exchange.orders if o[0] == "open" and "_add1" in o[4]]
    check(not adds, "低于最小加仓价值时不能下单加仓")
    return "通过"


async def test_source_stats():
    """按来源统计：已平仓笔数、胜率、净盈亏、结果分类和当前持仓数"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)

    def add(sid, source, net, gross, of, cf, ff, reason, status):
        db._conn.execute(
            "INSERT INTO signals(id,symbol,direction,status,source,net_pnl,gross_pnl,"
            "open_fee,close_fee,funding_fee,end_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (sid, "RLCUSDT", "long", status, source, net, gross, of, cf, ff, reason))

    add("SS1", "成交量信号", "2.0", "2.1", "0.05", "0.05", "0", "target", ST_CLOSED)
    add("SS2", "成交量信号", "-1.0", "-0.9", "0.05", "0.05", "0", "exchange_stop", ST_CLOSED)
    add("SS3", "波动信号", "1.0", "1.1", "0.02", "0.03", "-0.01", "trail_stop", ST_CLOSED)
    add("SS4", "成交量信号", "", "", "", "", "", "", ST_HOLDING)
    db._conn.commit()

    stats = {s["source"]: s for s in db.source_stats()}
    a = stats["成交量信号"]
    check(a["closed_count"] == 2, f"成交量信号已平仓应为 2（实际 {a['closed_count']}）")
    check(a["wins"] == 1 and a["losses"] == 1, "成交量信号应 1 胜 1 负")
    check(D(a["net_pnl"]) == D("1.0"), f"成交量信号净盈亏应为 1.0（实际 {a['net_pnl']}）")
    check(a["target_count"] == 1 and a["stop_count"] == 1, "成交量信号应各 1 次止盈/止损")
    check(a["open_count"] == 1, f"成交量信号当前持仓应为 1（实际 {a['open_count']}）")
    check(D(a["win_rate"]) == D("50.0"), f"成交量信号胜率应为 50%（实际 {a['win_rate']}）")
    b = stats["波动信号"]
    check(b["trail_count"] == 1, "波动信号应有 1 次止盈保护")
    return "通过"


async def test_totals():
    """累计统计：总开仓数、总手续费、总资金费"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    db._conn.execute(
        "INSERT INTO signals(id,symbol,direction,status,open_fee,close_fee,funding_fee) "
        "VALUES(?,?,?,?,?,?,?)",
        ("T1", "RLCUSDT", "long", ST_HOLDING, "0.5", "", "0.1"))
    db._conn.execute(
        "INSERT INTO signals(id,symbol,direction,status,open_fee,close_fee,funding_fee) "
        "VALUES(?,?,?,?,?,?,?)",
        ("T2", "RLCUSDT", "long", ST_CLOSED, "0.2", "0.1", "-0.05"))
    db._conn.execute(
        "INSERT INTO signals(id,symbol,direction,status,open_fee,close_fee,funding_fee) "
        "VALUES(?,?,?,?,?,?,?)",
        ("T3", "RLCUSDT", "long", ST_SKIPPED, "0.9", "", "0.2"))
    db._conn.commit()
    t = db.totals()
    check(t["opened"] == 2, f"总开仓数应为 2（跳过的不算，实际 {t['opened']}）")
    check(D(t["fees"]) == D("0.8"), f"总手续费应为 0.8（实际 {t['fees']}）")
    check(D(t["funding"]) == D("0.05"), f"总资金费应为 0.05（实际 {t['funding']}）")
    return "通过"


async def test_platform_active_parse():
    """平台 active 响应解析：直接收益%与按价格/均价计算的收益%"""
    p = parse_platform_active({"data": [
        {"id": "A", "direction": "short", "profit_percent": "12.3"},
        {"id": "B", "direction": "long", "price": "1.2", "avg": "1.0"},
    ]})
    check(abs(p["A"]["percent"] - 12.3) < 1e-6,
          f"应解析平台收益 12.3（实际 {p['A']['percent']}）")
    check(abs(p["B"]["percent"] - 20.0) < 1e-6,
          f"应按价格/均价算出 20%（实际 {p['B']['percent']}）")
    p2 = parse_platform_active([{"id": "C", "direction": "short",
                                 "current_price": "0.8", "entry": "1.0"}])
    check(abs(p2["C"]["percent"] - 20.0) < 1e-6,
          f"空头应按价格算出 20%（实际 {p2['C']['percent']}）")
    return "通过"


async def test_no_local_stop():
    """关闭本地止损后：开仓/移动止损都不在交易所挂止损单，只记录平台止损止盈"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    eng.params.save({"local_stop_enabled": "0"})
    await eng.dispatch(active_msg("NS1", "RLCUSDT", "1.5", "1.4"))
    sig = db.get_signal("NS1")
    check(sig["status"] == ST_HOLDING, "开仓应成功")
    check(D(sig["stop"]) == D("1.4") and D(sig["target"]) == D("2.0"),
          "平台止损/止盈应记录在案")
    stops = [o for o in eng.exchange.orders if o[0] == "stop"]
    check(not stops, f"关闭本地止损后不应挂交易所止损单（实际 {stops}）")

    await eng.dispatch({"type": "signal.trailed", "id": "NS1", "symbol": "RLCUSDT",
                        "direction": "long", "stop": "1.45", "target": "2.1",
                        "text": "平台移动止损"})
    sig = db.get_signal("NS1")
    stops = [o for o in eng.exchange.orders if o[0] == "stop"]
    check(not stops, "移动止损也不应挂交易所止损单")
    check(D(sig["stop"]) == D("1.45") and D(sig["target"]) == D("2.1"),
          "平台新止损/止盈仍应记录")
    return "通过"


async def test_close_position_serialized():
    """同一仓位被平台结束/对账/自动平多并发触发时，只能真正下一次平仓单"""
    tmp, db, eng = make_env({}, STD_FILTERS)
    eng.exchange = FakeRealExchange()
    eng._ensure_exchange = lambda: None

    async def _skip(*a, **k):
        return None
    eng._enrich_close_fees = _skip
    db.upsert_signal(id="DUP1", symbol="DUPUSDT", direction="long",
                     source="成交量信号", qty="10", notional="20", entry="1.5",
                     avg_entry="1.5", base_qty="10", status=ST_HOLDING,
                     open_time=now_str(), open_ts=time.time() - 100,
                     martin_layers=0, martin_multiplier="1", open_fee="0",
                     funding_fee="0", margin_mode="isolated", leverage="3",
                     last_funding_ts=time.time())
    sig = db.get_signal("DUP1")
    await asyncio.gather(
        eng._close_position(sig, None, 0, "test", "", "并发测试一"),
        eng._close_position(sig, None, 0, "test", "", "并发测试二"),
    )
    closes = [o for o in eng.exchange.orders if o[0] == "close"]
    check(len(closes) == 1, f"并发平仓只能下一次单（实际 {closes}）")
    check(db.get_signal("DUP1")["status"] == ST_CLOSED, "仓位应已平仓")
    return "通过"


async def test_open_cross_guard():
    """实盘开仓：先回读确认全仓、杠杆带持仓方向、下单显式全仓"""
    tmp, db, eng = make_env({"GUSDT": "1.5"}, STD_FILTERS)
    eng.params.save({"sim_mode": "0", "leverage": "3"})
    eng.exchange = GuardExchange()
    eng._exchange_creds = ("k", "s", "p")
    eng._ensure_exchange = lambda: None

    async def _skip_verify(*a, **k):
        return None
    eng._verify_open_settings = _skip_verify

    await eng.dispatch(active_msg("G1", "GUSDT", "1.5", "1.45"))
    sig = db.get_signal("G1")
    check(sig["status"] == ST_HOLDING,
          f"开仓应成功（实际 {sig['status']}：{sig.get('skip_reason')}）")
    calls = eng.exchange.calls
    check(("mode", "GUSDT", "crossed") in calls,
          f"开仓前应先回读确认全仓（实际调用 {calls}）")
    check(("lev", "GUSDT", "3", "long") in calls,
          f"设置杠杆必须带持仓方向 long（实际调用 {calls}）")
    check(("open", "GUSDT", "BUY", "crossed") in calls,
          f"下单必须显式指定全仓（实际调用 {calls}）")
    check(sig["margin_mode"] == "crossed" and sig["leverage"] == "3",
          f"本地记录应写入全仓/3倍（实际 {sig['margin_mode']}/{sig['leverage']}）")
    return "通过"


async def test_pnl_curve():
    """净盈亏曲线：全部/当天累计，按平仓时间升序"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    db._conn.execute(
        "INSERT INTO signals(id,symbol,direction,source,status,close_ts,close_time,net_pnl) "
        "VALUES(?,?,?,?,?,?,?,?)",
        ("PC1", "RLCUSDT", "long", "费率信号", ST_CLOSED,
         946684800, "2000-01-01 00:00:00", "1"))
    db._conn.execute(
        "INSERT INTO signals(id,symbol,direction,source,status,close_ts,close_time,net_pnl) "
        "VALUES(?,?,?,?,?,?,?,?)",
        ("PC2", "RLCUSDT", "short", "成交量信号", ST_CLOSED,
         time.time(), now_str(), "-0.5"))
    db._conn.commit()
    allc = db.pnl_curve("all")
    check(allc["count"] == 2 and D(allc["total"]) == D("0.5"),
          f"全部曲线应为 2 笔合计 0.5（实际 {allc['count']}/{allc['total']}）")
    check(D(allc["points"][-1]["cum"]) == D("0.5"), "全部曲线最后累计应为 0.5")
    today = db.pnl_curve("today")
    check(today["count"] == 1 and D(today["total"]) == D("-0.5"),
          f"当天曲线应为 1 笔 -0.5（实际 {today['count']}/{today['total']}）")
    return "通过"


def _make_pending(db, sid, oid="LIMIT123", price="0.99", qty="20"):
    """手工造一条“挂单中”记录（首次开仓已改市价，测试直接写库）"""
    db.upsert_signal(
        id=sid, symbol="MLUSDT", direction="long", source="测试",
        qty=qty, notional="20", entry="1.0", avg_entry=price, base_qty=qty,
        status=ST_PENDING, pending_open_order_id=oid, pending_open_qty=qty,
        pending_open_price=price, open_time=now_str(), open_ts=time.time())


async def test_pending_order_vanished():
    """限价单在交易所消失（被撤销）时，本地要标记跳过并告警，不能一直显示挂单中"""
    # 1) 挂单还在交易所 → 保持挂单中
    tmp, db, eng = make_env({"MLUSDT": "1.2"}, STD_FILTERS)
    ex = VanishedOrderExchange()
    ex.open_orders = [{"orderId": "LIMIT123", "price": "0.99", "size": "20", "state": "live"}]
    eng.exchange = ex
    eng._exchange_creds = ("k", "s", "p")
    eng._ensure_exchange = lambda: None
    _make_pending(db, "G9")
    await eng._sync_pending_entries()
    check(db.get_signal("G9")["status"] == ST_PENDING, "挂单还在交易所时不能标记跳过")

    # 2) 挂单消失 + 详情已撤销 → 标记跳过并写明原因
    ex.open_orders = []
    await eng._sync_pending_entries()
    sig2 = db.get_signal("G9")
    check(sig2["status"] == ST_SKIPPED,
          f"交易所撤销后应标记跳过（实际 {sig2['status']}）")
    check("挂单已失效" in (sig2.get("skip_reason") or ""),
          f"应写明跳过原因（实际 {sig2.get('skip_reason')}）")
    check(not db.open_positions(), "标记跳过时不应凭空生成持仓")

    # 3) 查询接口抖动（抛异常）时不能误杀挂单
    tmp2, db2, eng2 = make_env({"MLUSDT": "1.2"}, STD_FILTERS)
    ex2 = VanishedOrderExchange()
    async def boom(symbol=""):
        raise RuntimeError("网络抖动")
    ex2.fetch_open_orders = boom
    eng2.exchange = ex2
    eng2._exchange_creds = ("k", "s", "p")
    eng2._ensure_exchange = lambda: None
    _make_pending(db2, "G10")
    await eng2._sync_pending_entries()
    check(db2.get_signal("G10")["status"] == ST_PENDING,
          "查询失败时不能把挂单标记跳过（避免接口抖动误杀）")

    # 4) 平台结束、我们自己撤单 → 不能再误报“挂单已失效”
    tmp3, db3, eng3 = make_env({"MLUSDT": "1.2"}, STD_FILTERS)
    ex3 = VanishedOrderExchange()
    ex3.detail_state = "canceled"
    eng3.exchange = ex3
    eng3._exchange_creds = ("k", "s", "p")
    eng3._ensure_exchange = lambda: None
    _make_pending(db3, "G11")
    await eng3.dispatch(ended_msg("G11"))
    sig3 = db3.get_signal("G11")
    check(sig3["status"] == ST_SKIPPED,
          f"平台结束时挂单未成交应标记跳过（实际 {sig3['status']}）")
    msgs = [a.get("msg") or "" for a in db3.get_alerts(50)]
    check(not any("挂单已失效" in m for m in msgs),
          f"平台结束时自己撤的单不能报“挂单已失效”（实际告警 {msgs}）")
    return "通过"


async def test_pending_add_cleanup():
    """加仓挂单已不在交易所时必须清掉，否则网页会长期显示幽灵挂单"""
    tmp, db, eng = make_env({"MLUSDT": "1.2"}, STD_FILTERS)
    ex = VanishedOrderExchange()
    eng.exchange = ex
    eng._exchange_creds = ("k", "s", "p")
    eng._ensure_exchange = lambda: None

    def make_pos(sid, oid, qty="10", price=""):
        db.upsert_signal(
            id=sid, symbol="MLUSDT", direction="long", source="测试",
            qty="100", notional="20", entry="1.0", avg_entry="1.0", base_qty="100",
            status=ST_HOLDING, pending_add_order_id=oid, pending_add_qty=qty,
            pending_add_price=price, open_time=now_str(), open_ts=time.time())

    # 1) 挂单还在 → 保留，并把交易所的限价回填（老记录没有价格）
    make_pos("A1", "ADD1", price="")
    ex.open_orders = [{"orderId": "ADD1", "price": "1.11", "size": "10", "state": "live"}]
    await eng._sync_pending_adds()
    sig = db.get_signal("A1")
    check(sig["pending_add_order_id"] == "ADD1", "挂单还在时不能清掉")
    check(D(sig["pending_add_price"]) == D("1.11"),
          f"应回填交易所限价（实际 {sig['pending_add_price']}）")

    # 2) 挂单被撤销 → 清空挂单字段
    ex.open_orders = []
    ex.detail_state = "canceled"
    await eng._sync_pending_adds()
    sig = db.get_signal("A1")
    check(sig["pending_add_order_id"] == "", "撤销后应清空挂单号")
    check(sig["pending_add_qty"] == "" and sig["pending_add_price"] == "",
          "撤销后应清空数量和限价")

    # 3) 已全部成交 → 必须立刻从“挂单中”清除，订单号留给数量/手续费对账
    make_pos("A3", "FILLED")
    ex.open_orders = []
    ex.detail_state = "filled"
    await eng._sync_pending_adds()
    sig = db.get_signal("A3")
    check(sig["pending_add_order_id"] == "", "成交后应清空挂单显示（不能一直显示已成交的单）")
    check("FILLED" in (sig["open_order_ids"] or ""), "成交订单号应记入 open_order_ids")

    # 4) 查询接口报错 → 不能误清
    make_pos("A2", "ADD2")
    async def boom(symbol=""):
        raise RuntimeError("网络抖动")
    ex.fetch_open_orders = boom
    await eng._sync_pending_adds()
    check(db.get_signal("A2")["pending_add_order_id"] == "ADD2",
          "查询失败时不能清掉加仓挂单")
    return "通过"


async def test_cancel_all_orders_is_targeted():
    """撤单必须按订单号逐个撤，不能调用批量接口（批量接口会误撤其他币种的挂单）"""
    ex = BitgetExchange("k", "s", "p")
    calls = []

    async def fake_fetch_open_orders(symbol=""):
        return [{"orderId": "A1"}, {"orderId": "A2"}]

    async def fake_signed(method, path, params=None, body=None):
        calls.append((method, path, dict(params or {}), dict(body or {})))
        return {"entrustedList": []}

    ex.fetch_open_orders = fake_fetch_open_orders
    ex._signed_request = fake_signed
    try:
        ok = await ex.cancel_all_orders("AAAUSDT")
    finally:
        await ex.close()
    check(ok is True, "撤单应返回成功")
    paths = [c[1] for c in calls]
    check("/api/v2/mix/order/cancel-all-orders" not in paths,
          f"不能调用批量撤单接口（实际调用 {paths}）")
    cancelled = [c[3].get("orderId") for c in calls if c[1].endswith("/cancel-order")]
    check(cancelled == ["A1", "A2"], f"应按订单号逐个撤（实际 {cancelled}）")
    check(all(c[3].get("symbol") == "AAAUSDT"
              for c in calls if c[1].endswith("/cancel-order")),
          "撤单必须带正确的 symbol")
    return "通过"


async def test_manual_open_fee_not_included():
    """手续费只按本信号自己的订单号汇总；同币其他信号的平仓成交不能算进来"""
    class ManualFillExchange(FakeRealExchange):
        async def get_symbol_fills(self, symbol, start_ts, end_ts):
            now = time.time()
            return [
                {"fee": D("0.02012"), "profit": D("0"), "price": D("1.0"),
                 "volume": D("20"), "fee_coin": "USDT", "order_id": "OPEN1",
                 "symbol": symbol, "side": "buy", "ts": now - 50},
                {"fee": D("0.00517"), "profit": D("0"), "price": D("1.1"),
                 "volume": D("5"), "fee_coin": "USDT", "order_id": "MANUAL9",
                 "symbol": symbol, "side": "buy", "ts": now - 20},
                {"fee": D("0.0085"), "profit": D("0"), "price": D("1.05"),
                 "volume": D("30"), "fee_coin": "USDT", "order_id": "OTHERCLOSE",
                 "symbol": symbol, "side": "buy", "ts": now - 10},
            ]

    tmp, db, eng = make_env({"FEEUSDT": "1.5"}, STD_FILTERS)
    eng.exchange = ManualFillExchange()
    eng._exchange_creds = ("k", "s", "p")
    eng._ensure_exchange = lambda: None
    db.upsert_signal(
        id="F1", symbol="FEEUSDT", direction="long", source="测试",
        qty="25", notional="20", entry="1.0", avg_entry="1.0", base_qty="25",
        status=ST_HOLDING, open_fee="0.02", open_order_ids="OPEN1",
        open_time=now_str(), open_ts=time.time() - 100)
    sig = db.get_signal("F1")
    await eng._sync_manual_open_fees(sig)
    sig = db.get_signal("F1")
    check(D(sig["open_fee"]) == D("0.02012"),
          f"只算本信号订单的成交（实际 {sig['open_fee']}）")
    check("手动" not in (sig["fee_note"] or ""), "备注不再声称含手动/未识别订单")
    logs_before = len([l for l in db.get_logs(200) if "校准开仓手续费" in l["msg"]])
    await eng._sync_manual_open_fees(sig)
    check(D(db.get_signal("F1")["open_fee"]) == D("0.02012"),
          "重复同步不能把手续费累加两次")
    logs_after = len([l for l in db.get_logs(200) if "校准开仓手续费" in l["msg"]])
    check(logs_after == logs_before, "手续费没变化时不能每次重复写提示")
    return "通过"


async def test_open_actual_qty_from_fills():
    """市价开仓部分成交：按成交明细校准实际数量，避免随后误报本地/交易所不一致"""
    class PartialFillExchange(FakeRealExchange):
        async def get_order_fills(self, symbol, order_id):
            if order_id == "OPENX":
                return [{"fee": D("0.02"), "profit": D("0"), "price": D("1.0"),
                         "volume": D("4890"), "fee_coin": "USDT",
                         "order_id": "OPENX", "symbol": symbol, "side": "buy",
                         "ts": time.time()}]
            return []

        async def get_symbol_fills(self, symbol, start_ts, end_ts):
            return []

    tmp, db, eng = make_env({"GUSDT": "1.0"}, STD_FILTERS)
    ex = PartialFillExchange()
    eng.exchange = ex
    eng._exchange_creds = ("k", "s", "p")
    eng._ensure_exchange = lambda: None
    db.upsert_signal(
        id="Q1", symbol="GUSDT", direction="long", source="测试",
        qty="4893", notional="20", entry="1.0", avg_entry="1.0", base_qty="4893",
        status=ST_HOLDING, open_order_ids="OPENX", open_fee="",
        open_time=now_str(), open_ts=time.time() - 10, last_funding_ts=time.time())
    await eng._enrich_open_fees("Q1", "GUSDT", ex, sync_qty=True)
    sig = db.get_signal("Q1")
    check(D(sig["qty"]) == D("4890"),
          f"开仓数量应按成交明细校准为 4890（实际 {sig['qty']}）")
    check(D(sig["base_qty"]) == D("4890"), "base_qty 也应校准")
    check(D(sig["open_fee"]) == D("0.02"), f"手续费应为 0.02（实际 {sig['open_fee']}）")
    return "通过"


async def test_cancel_pending_adds_once():
    """一次性取消当前所有加仓挂单，只执行一次；不能每次重启都撤新单"""
    tmp, db, eng = make_env({"AUSDT": "1.0", "BUSDT": "2.0"}, STD_FILTERS)
    ex = FakeRealExchange()
    eng.exchange = ex
    eng._exchange_creds = ("k", "s", "p")
    eng._ensure_exchange = lambda: None
    for i, sym in enumerate(("AUSDT", "BUSDT")):
        db.upsert_signal(
            id=f"C{i}", symbol=sym, direction="long", source="测试",
            qty="10", notional="20", entry="1", avg_entry="1", base_qty="10",
            status=ST_HOLDING, pending_add_order_id=f"ADD{i}",
            pending_add_qty="2", pending_add_price="1.1", pending_add_ts=time.time(),
            open_time=now_str(), open_ts=time.time())
    await eng.cancel_pending_adds_once("测试")
    cancels = [o for o in ex.orders if o[0] == "cancel_order"]
    check(len(cancels) == 2, f"应撤销 2 张旧加仓挂单（实际 {cancels}）")
    check(all(not p["pending_add_order_id"] for p in db.open_positions()),
          "撤销成功后应清空本地挂单字段")
    check(db.get_setting("cancel_pending_adds_update73") == "1", "应写入一次性标记")
    before = len(ex.orders)
    await eng.cancel_pending_adds_once("测试")
    check(len(ex.orders) == before, "一次性清理不能重复执行")
    return "通过"


async def test_today_fee_by_fill_time():
    """今日手续费按 Bitget 成交时间归集：老仓位今天加仓的费用也算今天，昨日成交不算今天"""
    tmp, db, eng = make_env({"FEE2USDT": "1.0"}, STD_FILTERS)
    now = time.time()
    today = time.strftime("%Y-%m-%d", time.localtime(now))
    # 昨天开的仓：open_fee 是整段累计（含今天加仓），旧口径会把它算到开仓日
    db.upsert_signal(
        id="OLD1", symbol="FEE2USDT", direction="long", source="测试",
        qty="20", notional="20", entry="1.0", avg_entry="1.0", base_qty="20",
        status=ST_HOLDING, open_fee="0.3000",
        open_time=time.strftime("%Y-%m-%d 10:00:00", time.localtime(now - 86400)),
        open_ts=now - 86400)
    fills = [
        {"fee": D("0.0300"), "profit": D("0"), "price": D("1.0"), "volume": D("20"),
         "fee_coin": "USDT", "fill_id": "T1", "order_id": "O1",
         "symbol": "FEE2USDT", "side": "buy", "ts": now - 60},
        {"fee": D("0.0200"), "profit": D("0"), "price": D("1.0"), "volume": D("10"),
         "fee_coin": "USDT", "fill_id": "T2", "order_id": "O2",
         "symbol": "FEE2USDT", "side": "buy", "ts": now - 30},
        {"fee": D("0.3000"), "profit": D("0"), "price": D("1.0"), "volume": D("20"),
         "fee_coin": "USDT", "fill_id": "T0", "order_id": "O0",
         "symbol": "FEE2USDT", "side": "buy", "ts": now - 86400},
    ]
    added = db.record_fee_events(fills, "OLD1")
    check(added == 3, f"应写入 3 笔手续费流水（实际 {added}）")
    # 今天还没完成同步时，回退旧口径（按开仓日），这笔昨天开的仓算不进今天
    check(db.today_stats()["fees"] == D("0"),
          "未完成今日同步时应回退旧口径")
    db.set_setting("fee_events_synced_day", today)
    check(db.today_stats()["fees"] == D("0.0500"),
          f"今日手续费应为今天实际成交 0.05（实际 {db.today_stats()['fees']}）")
    db.record_fee_events(fills, "OLD1")
    check(db.today_stats()["fees"] == D("0.0500"), "重复同步不能重复累加")
    check(D(db.get_signal("OLD1")["open_fee"]) == D("0.3000"),
          "手续费流水不能改动仓位记录")
    return "通过"


async def test_fee_event_fill_id_dedupe():
    """手续费流水用 tradeId 幂等去重；缺少 tradeId 时用订单号+时间拼稳定键"""
    tmp, db, eng = make_env({"FEE3USDT": "1.0"}, STD_FILTERS)
    now = time.time()
    raw = {"tradeId": "998877", "orderId": "O9", "price": "1", "baseVolume": "2",
           "feeDetail": {"totalFee": "-0.0123", "feeCoin": "USDT"},
           "side": "buy", "symbol": "FEE3USDT", "cTime": str(int(now * 1000))}
    f = parse_fill(raw)
    check(f["fill_id"] == "998877", f"应解析 tradeId（实际 {f['fill_id']}）")
    no_id = {"fee": D("0.0010"), "profit": D("0"), "price": D("1"),
             "volume": D("1"), "fee_coin": "USDT", "order_id": "O10",
             "symbol": "FEE3USDT", "side": "sell", "ts": now}
    check(db.record_fee_events([f]) == 1, "第一笔应写入")
    check(db.record_fee_events([f]) == 0, "同一 tradeId 不能重复写入")
    check(db.record_fee_events([no_id]) == 1, "无 tradeId 的成交应写入")
    check(db.record_fee_events([no_id]) == 0, "无 tradeId 时也要能稳定去重")
    fee, count = db.today_fee_events()
    check(count == 2 and fee == D("0.0133"),
          f"今日流水应为 2 笔共 0.0133（实际 {count} 笔 {fee}）")
    return "通过"


async def test_gate_adapter_parsing():
    """Gate 合约/成交解析：张数换算、手续费、订单号、错误提示（离线）"""
    contract = {"quanto_multiplier": "10", "enable_decimal": False,
                "order_size_min": "1", "status": "trading", "in_delisting": False,
                "taker_fee_rate": "0.00075", "order_price_round": "0.00001"}
    f = parse_gate_contract_filters(contract)
    check(f is not None, "Gate 合约参数应能解析")
    check(D(f["stepSize"]) == D("10"), f"非小数张 stepSize 应为 10（实际 {f['stepSize']}）")
    check(D(f["minQty"]) == D("10"), f"非小数张 minQty 应为 10（实际 {f['minQty']}）")
    check(D(f["minNotional"]) == D("0"),
          f"Gate 没有最小名义价值，不能沿用 5U 门槛（实际 {f['minNotional']}）")
    check(D(f["takerFeeRate"]) == D("0.00075"), "Gate taker 费率应为 0.075%")
    decimal_contract = dict(contract, enable_decimal=True, order_size_min="0")
    f2 = parse_gate_contract_filters(decimal_contract)
    check(D(f2["stepSize"]) == D("1"), f"小数张步进应为 0.1 张=1 币（实际 {f2['stepSize']}）")
    check(D(f2["minQty"]) == D("1"), f"小数张最小量应为 0.1 张=1 币（实际 {f2['minQty']}）")

    fill = parse_gate_fill({"size": -5, "price": "2", "fee": "0.001",
                            "id": 1, "order_id": "9", "create_time": 1000},
                           "10", "TESTUSDT")
    check(D(fill["volume"]) == D("50"), f"5 张 × 10 应为 50 个基础币（实际 {fill['volume']}）")
    check(fill["side"] == "sell", "size 为负应为卖出")
    check(D(fill["fee"]) == D("0.001"), "手续费应按成本记录")
    # /my_trades 返回 id，/my_trades_timerange 返回 trade_id：两者必须归一到同一个
    # 流水键，否则同一笔成交会被记两次手续费
    fill2 = parse_gate_fill({"size": -5, "price": "2", "fee": "0.001",
                             "trade_id": 1, "order_id": "9", "create_time": 1000},
                            "10", "TESTUSDT")
    check(fill["fill_id"] == fill2["fill_id"] == "1",
          f"两个成交接口的成交号应一致（实际 {fill['fill_id']} / {fill2['fill_id']}）")
    tid = gate_text_id("sl_中文信号_12345678901234567890")
    check(tid.startswith("t-") and len(tid) <= 28 and all(ord(ch) < 128 for ch in tid),
          f"Gate text 必须是 t- 开头、28 字节内 ASCII（实际 {tid}）")
    check("合约账户" in gate_friendly("USER_NOT_FOUND", ""), "Gate 错误码应转成中文提示")
    return "通过"


async def test_gate_decimal_min_size_step():
    """Gate 小数张的最小步进是 0.1 张：不足一手要在本地判 0，不能发出被拒的零散张数。"""

    btw = {"quanto_multiplier": "100", "enable_decimal": True, "order_size_min": "0",
           "status": "trading", "in_delisting": False,
           "taker_fee_rate": "0.00075", "order_price_round": "0.000001"}
    f = parse_gate_contract_filters(btw)
    check(f is not None, "合成合约参数应能解析")
    check(D(f["stepSize"]) == D("10"), f"合成合约步进应为 0.1 张=10 币（实际 {f['stepSize']}）")
    check(D(f["minQty"]) == D("10"), f"合成合约最小量应为 0.1 张=10 币（实际 {f['minQty']}）")
    check(D(f["orderSizeMin"]) == D("0.1"),
          f"合成合约最小张数应为 0.1（实际 {f['orderSizeMin']}）")
    # 实盘被拒的那笔：差 2.8079U ÷ 0.439989 = 6.38 币，按 10 币步进必须取 0
    # （旧代码按 0.01 张算出来是 6 币 = 0.06 张，发给 Gate 后被拒）
    add_qty = floor_to_step(D("2.807902687086131") / D("0.439989"), D(f["stepSize"]))
    check(add_qty == 0, f"6.38 币不足 0.1 张应取 0（实际 {add_qty}）")
    # Gate 会把 0.54 张按 0.5 张成交，本地也按 0.1 张向下取整，账本才与成交一致
    check(floor_to_step(D("0.54"), D("0.1")) == D("0.5"), "0.54 张应按 0.1 张向下取整为 0.5 张")
    # 30U 头仓：30 ÷ 0.44 = 68.18 币，应向下取 0.6 张 = 60 币，不能请求 68 币
    qty, filled, _ = head_entry_qty(D("30"), D("0.44"), f)
    check(qty == D("60") and not filled,
          f"30U 头仓应取 60 币且不算凑整（实际 {qty}，凑整={filled}）")
    # 15U 头仓：34.09 币 → 30 币
    qty2, _, _ = head_entry_qty(D("15"), D("0.44"), f)
    check(qty2 == D("30"), f"15U 头仓应取 30 币（实际 {qty2}）")
    return "通过"


async def test_gate_first_routing_and_fallback():
    """新单 Gate 优先；Gate 没有的币回退 Bitget；平仓回原交易所"""
    tmp, db, eng = make_env({"RLCUSDT": "1.5"}, STD_FILTERS)
    eng.params.save({"sim_mode": "0", "exchange_api_key": "k", "exchange_secret": "s",
                     "exchange_passphrase": "p", "gate_api_key": "gk",
                     "gate_secret": "gs", "gate_enabled": "1",
                     "new_entry_route": "gate_first"})
    bitget = RouteExchange({"ONLYBITUSDT"}, "Bitget")
    gate = RouteExchange({"GATEUSDT"}, "Gate")
    eng.exchange = bitget
    eng.exchanges = {"bitget": bitget, "gate": gate}
    eng._ensure_exchange = lambda: None

    await eng.dispatch(active_msg("G1", "GATEUSDT", "1.5", "1.4"))
    sig = db.get_signal("G1")
    check(sig["status"] == ST_HOLDING, f"Gate 有合约时应开仓（实际 {sig['status']}：{sig.get('skip_reason')}）")
    check(sig["exchange"] == "gate", f"G1 应记录为 Gate（实际 {sig['exchange']}）")
    check(any(o[0] == "open" for o in gate.orders), "应在 Gate 开仓")
    check(not any(o[0] == "open" for o in bitget.orders), "Gate 有时不应在 Bitget 重复开仓")

    await eng.dispatch(active_msg("B1", "ONLYBITUSDT", "1.5", "1.4"))
    sigb = db.get_signal("B1")
    check(sigb["status"] == ST_HOLDING, f"Gate 没有的币应回退 Bitget（实际 {sigb['status']}）")
    check(sigb["exchange"] == "bitget", f"B1 应记录为 Bitget（实际 {sigb['exchange']}）")
    check(any(o[0] == "open" for o in bitget.orders), "应在 Bitget 回退开仓")

    try:
        db.update_signal("G1", last_funding_ts=time.time() + 3600)
    except Exception:
        pass
    await eng.dispatch(ended_msg("G1", profit="10", final="1.6"))
    check(any(o[0] == "close" for o in gate.orders), "Gate 仓位应在 Gate 平仓")
    check(not any(o[0] == "close" and o[1] == "GATEUSDT" for o in bitget.orders),
          "Gate 仓位不能跑到 Bitget 平仓")
    return "通过"


async def test_gate_ledger_filters():
    """分交易所账本：gate 的统计不能混入 bitget 的记录"""
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    db = Database(Path(tmp.name) / "test.db", quiet=True)
    now = now_str()
    ts = time.time()
    db.upsert_signal(id="GB1", symbol="AIOTUSDT", direction="long", source="趋势信号",
                     exchange="gate", status=ST_CLOSED, open_time=now, open_ts=ts,
                     close_time=now, close_ts=ts, notional="30",
                     gross_pnl="1.2", open_fee="0.1", close_fee="0.1",
                     funding_fee="0", net_pnl="1.0", qty="100")
    db.upsert_signal(id="BB1", symbol="JCTUSDT", direction="short", source="成交量信号",
                     exchange="bitget", status=ST_CLOSED, open_time=now, open_ts=ts,
                     close_time=now, close_ts=ts, notional="30",
                     gross_pnl="2.2", open_fee="0.1", close_fee="0.1",
                     funding_fee="0", net_pnl="2.0", qty="100")
    OPEN_DBS.append(db)
    check(db.totals("gate")["opened"] == 1, "Gate 累计开仓应为 1")
    check(db.totals("bitget")["opened"] == 1, "Bitget 累计开仓应为 1")
    check(D(db.total_realized_pnl("gate")) == D("1.0"), "Gate 已实现净盈亏应为 1.0")
    check(D(db.total_realized_pnl("bitget")) == D("2.0"), "Bitget 已实现净盈亏应为 2.0")
    rows = db.closed_history(limit=10, exchange="gate")
    check(len(rows) == 1 and rows[0]["id"] == "GB1", f"Gate 历史应只有 GB1（实际 {len(rows)} 条）")
    check(db.closed_count(exchange="gate") == 1, "Gate 历史总数应为 1")
    st = db.today_stats("gate")
    check(st["opened"] == 1 and st["closed"] == 1, "Gate 今日开/平统计应为 1")
    check(D(st["net_pnl"]) == D("1.0"), f"Gate 今日净盈亏应为 1.0（实际 {st['net_pnl']}）")
    curve = db.pnl_curve("all", "gate")
    check(curve["count"] == 1 and D(curve["total"]) == D("1.0"), "Gate 收益曲线应只有 1 笔 1.0")
    stats = {s["source"]: s for s in db.source_stats("gate")}
    check(stats["趋势信号"]["closed_count"] == 1, "Gate 来源统计应只含趋势信号")
    check(stats["成交量信号"]["closed_count"] == 0, "Gate 来源统计不能混入 Bitget 的成交量信号")
    # 两家交易所的 tradeId 可能撞号；Gate 成交号带前缀，不能把 Bitget 的手续费吞掉
    fill = {"fee": D("0.1"), "ts": ts, "fill_id": "1",
            "symbol": "AIOTUSDT", "side": "buy"}
    check(db.record_fee_events([fill], "GB1", "gate") == 1, "Gate 手续费流水应写入")
    check(db.record_fee_events([fill], "BB1", "bitget") == 1, "Bitget 同号成交不能冲突")
    fee_rows = db._conn.execute("SELECT exchange,fill_id FROM fee_events").fetchall()
    check(len(fee_rows) == 2, f"两家同号成交应各留一条（实际 {len(fee_rows)} 条）")
    gate_fee, _ = db.today_fee_events("gate")
    check(D(gate_fee) == D("0.1"), f"Gate 今日手续费应为 0.1（实际 {gate_fee}）")
    return "通过"


async def test_fee_window_fail_closed():
    """币种、成交号缺失或区间不连续都不得破坏已有账本。"""
    from signal_ledger.core import day_start_ts
    tmp, db, eng = make_env({"SAFEUSDT":"1"}, STD_FILTERS)
    now=time.time()
    f={"fill_id":"S1","fee":D("0.01"),"ts":now-20,"symbol":"SAFEUSDT","fee_coin":"USDT",
       "volume":"1","price":"1"}
    db.replace_fee_window("gate",day_start_ts(),now,[f])
    for bad in [dict(f,fill_id=""),dict(f,fee_coin="BGB"),dict(f,fee=D("NaN")),dict(f,ts=0)]:
        try:
            db.replace_fee_window("gate",day_start_ts(),now,[bad])
            check(False,"异常流水必须拒绝")
        except ValueError:
            pass
    try:
        db.replace_fee_window("gate",now+10,now+20,[])
        check(False,"不连续覆盖区间必须拒绝")
    except ValueError:
        pass
    check(db.actual_fees("gate")==D("0.01"),"失败后保留原费用")
    check(db.ledger_state("gate","fees")["end_ts"]==now,"失败不得推进游标")
    return "通过"


async def test_partial_funding_no_double_count():
    """部分平仓父子记录共同分摊一张账单，合计严格等于实际账单。"""
    tmp,db,eng=make_env({"PARTUSDT":"1"},STD_FILTERS)
    now=time.time()
    for sid,qty,status,close in [("ROOT","60",ST_HOLDING,0),("ROOT_part1","40",ST_CLOSED,now-100)]:
        db.upsert_signal(id=sid,symbol="PARTUSDT",direction="long",exchange="gate",status=status,
            open_ts=now-400,close_ts=close,qty=qty,accounting_parent="ROOT" if sid!="ROOT" else "",
            open_fee="0",close_fee="0",gross_pnl="0",funding_fee="99")
    db.record_qty_event("ROOT",now-400,"100","开仓")
    db.replace_funding_window("gate",now-500,now,[{"id":"PF","symbol":"PARTUSDT","ts":now-200,"amount":"1.20"}])
    check(D(db.get_signal("ROOT")["funding_fee"])==D("0.72"),"父记录只能分配剩余六成")
    check(D(db.get_signal("ROOT_part1")["funding_fee"])==D("0.48"),"子记录分配四成")
    return "通过"


async def test_unassigned_funding_not_faked():
    """归属不明的实际账单不凭猜测塞入其他信号。"""
    tmp,db,eng=make_env({"AMBIGUSDT":"1"},STD_FILTERS)
    now=time.time()
    for sid in ["A1","A2"]:
        db.upsert_signal(id=sid,symbol="AMBIGUSDT",direction="long",exchange="gate",status=ST_HOLDING,
                         open_ts=now-100,qty="1",funding_fee="0.5")
    db.replace_funding_window("gate",now-200,now,[{"id":"UF","symbol":"AMBIGUSDT","ts":now-20,"amount":"1"}])
    check(db.actual_funding("gate")==D("1"),"账户总额仍须如实显示")
    check(all(db.get_signal(s)["funding_source"]=="pending" for s in ["A1","A2"]),"归属不明必须待核对")
    check(not db.accounting_quality(["gate"])["verified"],"不能宣称全量核对完成")
    return "通过"


async def test_actual_bill_adapter_signs():
    """两家实际账单接口解析收支方向，手续费返佣不能当成费用。"""
    from signal_ledger.core import GateExchange
    now=time.time()
    bg=object.__new__(BitgetExchange)
    async def bgread(*args,**kwargs):
        check(kwargs["params"]["businessType"]=="contract_settle_fee","仅请求资金费类型")
        return {"bills":[{"billId":"1","symbol":"XUSDT","cTime":str(int((now-10)*1000)),"amount":"0.1234"}]}
    bg._signed_request=bgread
    gt=object.__new__(GateExchange)
    gt._reverse_map={}
    async def gtread(*args,**kwargs):return [{"id":"2","text":"X_USDT","time":now-10,"change":"-0.2345"}]
    gt._request=gtread
    check((await bg.get_funding_bills(now-20,now))[0]["amount"]==D("-0.1234"),"Bitget 收入转为负成本")
    check((await gt.get_funding_bills(now-20,now))[0]["amount"]==D("0.2345"),"Gate 支出转为正成本")
    check(parse_fill({"feeDetail":{"totalFee":"0.1","feeCoin":"USDT"}})["fee"]==D("-0.1"),"Bitget 正向返佣保留为负成本")
    check(parse_gate_fill({"size":"1","price":"1","fee":"-0.1"},"1")["fee"]==D("-0.1"),"Gate 返佣不能取绝对值")
    check(parse_fill({"feeDetail":"坏数据"})["fee_known"] is False,"解析失败不能假装零费用")
    return "通过"


async def test_fill_pagination_repeat_rejected():
    """满页而游标不前进时明确失败，不能静默截断成完整数据。"""
    bg=object.__new__(BitgetExchange)
    now=time.time()
    rows=[{"tradeId":str(i),"cTime":str(int((now-10)*1000)),"feeDetail":{"totalFee":"-0.01","feeCoin":"USDT"}} for i in range(1,101)]
    async def read(*args,**kwargs):return {"fillList":rows,"endId":"100"}
    bg._signed_request=read
    try:
        await bg.get_all_fills(now-20,now)
        check(False,"分页重复必须拒绝")
    except ValueError as e:
        check("分页" in str(e),"应给出明确分页错误")
    return "通过"


async def test_partial_fee_allocation_preserved():
    """完整开仓订单不能把已拆给子记录的费用再次计入父记录。"""
    tmp,db,eng=make_env({"SPLITUSDT":"1"},STD_FILTERS)
    now=time.time()
    db.upsert_signal(id="SP",symbol="SPLITUSDT",direction="long",exchange="gate",status=ST_HOLDING,
        open_ts=now-300,qty="60",avg_entry="1",open_order_ids="OPEN",open_fee="0.06")
    db.upsert_signal(id="SP_part1",symbol="SPLITUSDT",direction="long",exchange="gate",status=ST_CLOSED,
        open_ts=now-300,close_ts=now-50,qty="40",avg_entry="1",accounting_parent="SP",open_fee="0.04")
    fills=[{"fill_id":"OP","order_id":"OPEN","symbol":"SPLITUSDT","side":"buy","ts":now-290,
            "price":D("1"),"volume":D("100"),"fee":D("0.123456"),"profit":D("0")},
           {"fill_id":"CL","order_id":"CLOSE","symbol":"SPLITUSDT","side":"sell","ts":now-50,
            "price":D("1.01"),"volume":D("40"),"fee":D("0.02"),"profit":D("0")}]
    db.replace_fee_window("gate",db.ledger_start("gate")-5,now,fills)
    db.reconcile_signal_fees("gate")
    check(D(db.get_signal("SP")["open_fee"])==D("0.083456"),"父记录只保留扣掉子记录后的开仓费")
    check(D(db.get_signal("SP_part1")["open_fee"])==D("0.04"),"子记录分摊不得被整单费用覆盖")
    return "通过"


async def test_true_zero_profit_not_estimated():
    """交易所明确返回零毛盈亏时，不能再按价格估算覆盖。"""
    f=parse_fill({"profit":"0","price":"2","baseVolume":"1","feeDetail":{"totalFee":"-0.01","feeCoin":"USDT"}})
    fields=TradingEngine._fill_accounting_fields({"qty":"1","avg_entry":"1","direction":"long","open_fee":"0","funding_fee":"0"},[f],[])
    check(D(fields["gross_pnl"])==D("0"),"实际零毛盈亏优先于价格估算")
    return "通过"


async def test_unknown_profit_never_zero():
    """缺少毛净盈亏的记录不能被曲线和来源统计伪装为零利润。"""
    tmp,db,eng=make_env({"MISSUSDT":"1"},STD_FILTERS)
    now=time.time()
    db.upsert_signal(id="MISS",symbol="MISSUSDT",direction="long",exchange="bitget",status=ST_CLOSED,
        source="趋势信号",open_ts=now-20,close_ts=now-10,open_time=now_str(),close_time=now_str())
    curve=db.pnl_curve()
    check(curve["total"]=="" and curve["missing_count"]==1,"曲线总额必须留空并报告缺失")
    check(db.today_stats()["net_pnl"] is None,"今日净盈亏不能把缺失当零")
    source=next(r for r in db.source_stats() if r["source"]=="趋势信号")
    check(source["net_pnl"]=="" and source["win_rate"] is None,"来源净值和胜率缺失时留空")
    return "通过"


async def test_net_quality_scope_isolation():
    """旧记录或另一家交易所待核对，不应给已确认的本项净值加约字。"""
    tmp,db,eng=make_env({"SCOPEUSDT":"1"},STD_FILTERS)
    now=time.time()
    for sid,ex,status,source,fee,close in [
        ("OKC","bitget",ST_CLOSED,"趋势信号","exchange",now-30),
        ("BADH","gate",ST_HOLDING,"资金流信号","pending",0),
        ("OLDC","gate",ST_CLOSED,"资金流信号","pending",now-86400)]:
        db.upsert_signal(id=sid,symbol="SCOPEUSDT",direction="long",exchange=ex,status=status,source=source,
            open_ts=now-90000,close_ts=close,close_time=now_str() if sid=="OKC" else "2026-01-01 00:00:00",
            qty="1",gross_pnl="1",net_pnl="1",fee_source=fee,funding_source="exchange")
    check(db.net_quality("bitget","closed")["verified"],"已确认的 Bitget 已平仓应保持确认")
    check(db.net_quality("","today")["verified"],"昨日待核对不能牵连今日平仓")
    check(not db.net_quality("","closed")["verified"],"累计包含未确认历史时仍须保留提示")
    check(not db.net_quality("gate","holding")["verified"],"未确认的 Gate 当前持仓不能隐藏风险")
    rows={x['source']:x for x in db.source_stats()}
    check(rows['趋势信号']['accounting_verified'] and not rows['资金流信号']['accounting_verified'],"来源必须独立判断")
    check(db.pnl_curve('today')['data_quality']['verified'],"当天曲线只依赖当天平仓")
    return "通过"










OPEN_DBS = []


ALL_TESTS = [
    ('test_params_validation', test_params_validation),
    ('test_decimal_floor', test_decimal_floor),
    ('test_duplicate_signal', test_duplicate_signal),
    ('test_fixed_notional', test_fixed_notional),
    ('test_head_multiplier_open', test_head_multiplier_open),
    ('test_head_min_one_lot', test_head_min_one_lot),
    ('test_end_close', test_end_close),
    ('test_history', test_history),
    ('test_receipt_reconcile', test_receipt_reconcile),
    ('test_fuse', test_fuse),
    ('test_timeout_unknown', test_timeout_unknown),
    ('test_close_no_position', test_close_no_position),
    ('test_real_fee_enrichment', test_real_fee_enrichment),
    ('test_funding', test_funding),
    ('test_min_notional', test_min_notional),
    ('test_no_bitget_symbol', test_no_bitget_symbol),
    ('test_martingale', test_martingale),
    ('test_martingale_below_min', test_martingale_below_min),
    ('test_source_stats', test_source_stats),
    ('test_totals', test_totals),
    ('test_platform_active_parse', test_platform_active_parse),
    ('test_no_local_stop', test_no_local_stop),
    ('test_close_position_serialized', test_close_position_serialized),
    ('test_open_cross_guard', test_open_cross_guard),
    ('test_pnl_curve', test_pnl_curve),
    ('test_pending_order_vanished', test_pending_order_vanished),
    ('test_pending_add_cleanup', test_pending_add_cleanup),
    ('test_cancel_all_orders_is_targeted', test_cancel_all_orders_is_targeted),
    ('test_manual_open_fee_not_included', test_manual_open_fee_not_included),
    ('test_open_actual_qty_from_fills', test_open_actual_qty_from_fills),
    ('test_cancel_pending_adds_once', test_cancel_pending_adds_once),
    ('test_today_fee_by_fill_time', test_today_fee_by_fill_time),
    ('test_fee_event_fill_id_dedupe', test_fee_event_fill_id_dedupe),
    ('test_gate_adapter_parsing', test_gate_adapter_parsing),
    ('test_gate_decimal_min_size_step', test_gate_decimal_min_size_step),
    ('test_gate_first_routing_and_fallback', test_gate_first_routing_and_fallback),
    ('test_gate_ledger_filters', test_gate_ledger_filters),
    ('test_fee_window_fail_closed', test_fee_window_fail_closed),
    ('test_partial_funding_no_double_count', test_partial_funding_no_double_count),
    ('test_unassigned_funding_not_faked', test_unassigned_funding_not_faked),
    ('test_actual_bill_adapter_signs', test_actual_bill_adapter_signs),
    ('test_fill_pagination_repeat_rejected', test_fill_pagination_repeat_rejected),
    ('test_partial_fee_allocation_preserved', test_partial_fee_allocation_preserved),
    ('test_true_zero_profit_not_estimated', test_true_zero_profit_not_estimated),
    ('test_unknown_profit_never_zero', test_unknown_profit_never_zero),
    ('test_net_quality_scope_isolation', test_net_quality_scope_isolation),
]
