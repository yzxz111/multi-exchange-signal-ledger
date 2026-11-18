"""使用合成行情和临时数据库，复用现有引擎的基础信号生命周期。"""

import asyncio
import json
import tempfile
from pathlib import Path
from decimal import Decimal

from .core import D, Database, Params, SimExchange, ST_HOLDING, signal_net
from .engine import TradingEngine

EXAMPLE_PATH = Path(__file__).resolve().parent / "fixtures" / "signals.json"
CATALOGS = {
    "gate": {"ALPHAUSDT": "10", "GAMMAUSDT": "2", "DELTAUSDT": "5", "EPSILONUSDT": "20"},
    "bitget": {"ALPHAUSDT": "10", "BETAUSDT": "25", "ZETAUSDT": "4"},
}


class OfflineExchange(SimExchange):
    """固定行情与合成订单；不创建 HTTP 客户端，也不接受真实凭据。"""

    def __init__(self, name, marks):
        self.exchange_name = name
        self.marks = {symbol: D(value) for symbol, value in marks.items()}
        self.orders = []
        super().__init__(price_fn=lambda symbol: self.marks[symbol], filters={
            "stepSize": "0.001", "minQty": "0.001", "minNotional": "5",
            "tickSize": "0.001", "takerFeeRate": "0.0006", "symbolStatus": "normal",
        })

    async def fetch_lot_filters(self, symbol):
        if symbol not in self.marks:
            self.last_contract_error = "合成合约目录中没有该标的"
            return None
        return dict(self.fixed_filters)

    async def fetch_mark_price(self, symbol):
        return self.marks.get(symbol)

    async def market_open(self, symbol, side, qty, client_id, margin_mode=None):
        self.orders.append({"kind": "open", "symbol": symbol, "side": side, "qty": str(qty)})
        return await super().market_open(symbol, side, qty, client_id, margin_mode)

    async def market_close(self, symbol, side, qty, client_id):
        self.orders.append({"kind": "close", "symbol": symbol, "side": side, "qty": str(qty)})
        return await super().market_close(symbol, side, qty, client_id)


class DemoEngine(TradingEngine):
    """演示专用子类：本地派发、合成目录路由、按原仓适配器模拟平仓。"""

    def __init__(self, db):
        super().__init__(db=db, params=Params(db))
        self.exchanges = {name: OfflineExchange(name, marks) for name, marks in CATALOGS.items()}
        self.exchange = self.exchanges["gate"]
        ok, errors = self.params.save({
            "notional": "100", "head_multiplier": "1", "leverage": "1",
            "sim_mode": "1", "fuse_enabled": "0", "local_stop_enabled": "0",
            "new_entry_route": "gate_first", "sources": "", "directions": "",
            "total_funds": "10000", "gate_total_funds": "10000",
        })
        if not ok:
            raise ValueError("演示参数无效：" + "；".join(errors))

    def _ensure_exchange(self):
        """固定使用构造时注入的离线适配器，不读取密钥或模式环境变量。"""
        return None

    def _ex(self, sig_or_exchange=None):
        name = sig_or_exchange if isinstance(sig_or_exchange, str) else (sig_or_exchange or {}).get("exchange")
        return self.exchanges.get(name, self.exchange)

    async def start(self):
        raise RuntimeError("演示版通过本地回放派发消息，不启动外部信号连接")

    async def accrue_funding(self, sig_id=None):
        """演示不查询外部资金费账单；费用来源始终标记为模拟。"""
        return None

    async def _handle_active(self, msg):
        route = self.db.get_setting("new_entry_route", "gate_first")
        order = {"gate_first": ["gate", "bitget"], "bitget_first": ["bitget", "gate"],
                 "gate_only": ["gate"], "bitget_only": ["bitget"]}[route]
        chosen = next((name for name in order if msg.get("symbol") in self.exchanges[name].marks), order[0])
        self.exchange = self.exchanges[chosen]
        # 基础引擎的模拟分支只接受一个适配器，在派发期间注入已选择的离线目录。
        self.db.set_setting("new_entry_route", chosen + "_only")
        try:
            await super()._handle_active(msg)
        finally:
            self.db.set_setting("new_entry_route", route)


class DemoSession:
    """拥有并清理自己的临时目录，拒绝使用外部传入的数据库路径。"""

    def __init__(self):
        self.temp = tempfile.TemporaryDirectory(prefix="signal-ledger-demo-")
        self.db = Database(Path(self.temp.name) / "demo.db", quiet=True)
        self.engine = DemoEngine(self.db)
        self.events = json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))
        self.lock = asyncio.Lock()
        self.replay_count = 0

    async def replay(self):
        async with self.lock:
            for event in self.events:
                await self.engine.dispatch(dict(event))
            for exchange in self.engine.exchanges.values():
                for symbol, price in exchange.marks.items():
                    if symbol == "GAMMAUSDT":
                        exchange.marks[symbol] = D("2.06")
                    elif symbol == "BETAUSDT":
                        exchange.marks[symbol] = D("24.75")
            self.replay_count += 1
            self.db.add_log("info", "合成事件回放完成；已处理的信号不会再次开仓。")
            return self.snapshot()

    def snapshot(self, exchange=""):
        rows = self.db.all_signals()
        if exchange:
            rows = [row for row in rows if row.get("exchange") == exchange]
        holdings, history = [], []
        realized, fees, floating = Decimal("0"), Decimal("0"), Decimal("0")
        for original in rows:
            row = dict(original)
            row["data_source"] = "合成演示数据"
            fees += D(row.get("open_fee") or "0") + D(row.get("close_fee") or "0")
            if row["status"] == ST_HOLDING:
                adapter = self.engine.exchanges[row["exchange"]]
                mark = adapter.marks[row["symbol"]]
                sign = 1 if row["direction"] == "long" else -1
                unrealized = (mark - D(row["avg_entry"])) * D(row["qty"]) * sign
                floating += unrealized
                row.update(mark=str(mark), unrealized=str(unrealized))
                holdings.append(row)
            else:
                net = signal_net(row)
                if net is not None:
                    realized += net
                row["display_net"] = str(net) if net is not None else ""
                history.append(row)
        return {
            "mode": "offline_demo", "data_source": "合成演示数据，与真实账户及投资收益无关",
            "replay_count": self.replay_count, "events_per_replay": len(self.events),
            "stats": {"holding": len(holdings), "closed": len(history), "realized": str(realized),
                      "fees": str(fees), "floating": str(floating)},
            "holdings": holdings, "history": history,
            "curve": self.db.pnl_curve(exchange=exchange),
            "logs": [dict(row) for row in self.db.get_logs(40)],
            "orders": {name: list(adapter.orders) for name, adapter in self.engine.exchanges.items()},
        }

    def close(self):
        self.db.close()
        self.temp.cleanup()
