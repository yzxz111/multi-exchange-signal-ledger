# -*- coding: utf-8 -*-
"""
信号 —— 交易引擎（Bitget + Gate 版）
负责：连接信号服务器（WebSocket）、断线自动重连、处理每一条信号消息、
     回执对账、单日亏损熔断、模拟盘 / 真实盘切换、手续费与净盈亏核算。

关键设计（务必保持，改动前先看懂）：
1. 接收循环只做两件事：记录原始消息 + 扔进队列。真正的处理在后台工作协程里做，
   这样下单、写数据库等耗时操作不会拖慢接收速度（收太慢会被服务器以 4408 断开）。
2. 固定头寸模式：每条信号数量 = 名义价值 ÷ 标记价，按 stepSize 向下取整。
   算法对所有信号完全一样，不因止损远近、来源、方向而改变。
3. 回执对账只在「连接建立之后」收到回执时生效，且只处理
   「开仓时间早于本次连接建立时间」的持仓记录，防止刚开仓的信号被误平。
4. 手续费：模拟盘按 Bitget 吃单费率估算（页面会注明“估算”）；
   实盘在开/平仓后异步查询交易所成交明细，把“实际手续费”回填到数据库。
5. 净盈亏 = 毛盈亏 − 开仓手续费 − 平仓手续费。拿不到收益%时毛盈亏按 0 计。
"""

import asyncio
import json
import os
import re
import time
import urllib.parse
from decimal import Decimal, ROUND_UP

try:
    import websockets
except ImportError:
    websockets = None

try:
    import httpx
except ImportError:
    httpx = None

from signal_ledger.core import (
    D, floor_to_step, quantize, pct_str, money_str, now_str, amount_str, day_start_ts, ledger_time,
    DB_PATH, JSONL_PATH, TG_JSONL_PATH, WS_URL_TEMPLATE, FUSE_RATIO, DEFAULT_TAKER_FEE,
    BITGET_BASE, PRODUCT_TYPE,
    ALL_SOURCES, DIRECTION_NAMES,
    ST_HOLDING, ST_CLOSED, ST_SKIPPED, ST_UNKNOWN, ST_DROPPED, ST_PENDING,
    SKIP_DUP, SKIP_FILTER, SKIP_FUSED, SKIP_DAILY, SKIP_MAXPOS,
    SKIP_SAME_SYMBOL, SKIP_NOPRICE, SKIP_DEVIATION, SKIP_TOOSMALL,
    SKIP_ORDERFAIL, SKIP_POSITION_MODE, SKIP_NO_BITGET, SKIP_NO_CONTRACT,
    parse_platform_active, gate_friendly,
    Database, Params, SimExchange, BitgetExchange, GateExchange, TelegramListener,
)

# 取不到过滤器时的兜底值（模拟盘专用；真实盘取不到会直接放弃信号）
DEFAULT_FILTERS = {"stepSize": "0.001", "minQty": "0.001",
                   "minNotional": "5", "tickSize": "0.0001",
                   "takerFeeRate": str(DEFAULT_TAKER_FEE)}


def head_entry_qty(target_value, price, filters):
    """首仓按目标名义价值计算数量并对齐步进；不足最小一手时，按合约过滤器凑到最小一手。"""
    step = D(filters.get("stepSize") or "0")
    min_qty = D(filters.get("minQty") or "0")
    min_open = max(min_qty, step)
    qty = floor_to_step(D(target_value) / D(price), step) if step > 0 else D(target_value) / D(price)
    if min_open > 0 and qty < min_open:
        return min_open, True, min_open * D(price)
    return qty, False, min_open * D(price)


class TradingEngine:
    """交易引擎：一个账户只允许一条连接（单引擎单连接，由网页的“开始/停止”按钮控制）"""

    def __init__(self, db=None, params=None):
        self.db = db or Database(DB_PATH)
        self.params = params or Params(self.db)
        self.inbox = asyncio.Queue(maxsize=5000)   # 收到的原始消息先放这里
        self.running = False                       # 是否处于“运行中”
        self.started_at = 0.0                      # 本次“开始运行”的时间戳（网页显示运行时长）
        self.connected = False                     # WebSocket 是否已连上
        self.ws = None
        self.conn_established_at = 0.0             # 本次连接建立的时间（对账要用）
        self.last_msg_time = 0.0                   # 最后收到消息的时间（含 ping）
        self.backoff = 5                           # 重连等待秒数（5 起步，翻倍，最长 60）
        self.fatal = ""                            # 致命错误（如 API Key 无效），网页红色显示
        self.balance = None                        # 点数余额（服务器 hello/结束/通知消息下发）
        self.exchange = None                       # 默认交易所（实盘优先 Bitget，模拟盘为 SimExchange）
        self.exchanges = {}                        # {"bitget": 适配器, "gate": 适配器}
        self._exchange_creds = None                # 当前交易所实例对应的密钥（改了密钥要重建）
        self.mark_cache = {}                       # symbol -> (时间戳, 标记价或 None)，页面显示盈亏用
        self._mark_batch_task = None               # 后台批量取价任务（避免阻塞页面接口）
        self._mark_wait_until = 0.0                # 冷启动首次取价最多等一次，之后不再拖慢轮询
        self._funding_client = None                # 资金费率公开接口的 HTTP 客户端
        self._funding_lock = asyncio.Lock()        # 防止资金费重复核算
        self._funding_price_cache = {}             # (symbol, 结算时间) -> 结算时价格
        self._funding_error_logged_at = 0.0        # 资金费接口报错日志的节流时间
        self._platform_active_cache = None          # 平台 active 接口缓存 (时间戳, 结果)
        self._pending_missing = {}                 # 信号id -> 连续查不到挂单的次数（防止接口抖动误判）
        self._task = None
        self._position_watch_task = None           # 定期对比交易所持仓（止损成交后自动对账）
        self._backfill_task = None                 # 启动后一次性后入平台浮亏标的
        self._startup_sync_task = None             # 启动时的交易所持仓对账（后入前要等它完成）
        self._telegram_task = None                 # Telegram 群消息轮询（平台 Webhook 中继）
        self._fee_sync_task = None                 # 按成交时间同步今日手续费流水
        self._fee_sync_cursor = 0.0                # 上次同步到的成交时间（增量拉取用）
        self._ledger_locks = {"fees": asyncio.Lock(), "funding": asyncio.Lock()}
        self._orphan_history_lock = asyncio.Lock()  # 缺失跟踪记录的已平历史只读补录锁
        self._orphan_history_error_at = 0.0
        self._ledger_progress = {}
        self._ledger_seen = set()
        self._fee_sync_error_logged_at = 0.0       # 手续费流水同步报错日志的节流时间
        self._close_locks = {}                     # 信号id -> asyncio.Lock，防止同一仓位被并发重复平仓
        self._close_attempted = set()              # 本次运行已发起过平仓的信号id（结果未知时不再重复下单）
        self._gate_symbols_primed = False          # 是否已把数据库里的 Gate 交易对映射到合约名
        self._worker_task = None                   # 后台消息处理协程（跨重连保留）
        self._busy = False                         # 正在处理一条消息（停机时要等它做完）

    # ==================== 启动 / 停止 ====================

    async def start(self):
        if self.running:
            return
        p = self.params.all()
        if not p.get("signal_api_key"):
            self.db.add_log("alert", "还没有填写信号平台 API Key，无法开始运行。请在网页参数区填写后点保存。")
            return
        if websockets is None:
            self.db.add_log("alert", "缺少依赖 websockets，请先双击运行「启动.bat」（Windows）或「启动.command」（Mac）自动安装。")
            return
        self.running = True
        self.started_at = time.time()
        self.fatal = ""
        self.backoff = 5
        self.db.set_setting("engine_running", "1")
        self._ensure_exchange()
        if self.exchange is None:
            self.running = False
            self.db.set_setting("engine_running", "0")
            return
        if not self._is_sim():
            try:
                self.db.backup_accounting_once()
            except Exception as e:
                self.db.add_log("warn", f"启动账本备份未完成：{e.__class__.__name__}；实际费用窗口修复须待备份成功。")

        self.disable_rate_research_once()

        self.disable_rate_research_v2_once()

        self.enable_capital_flow_source_once()


        self.enable_both_directions_once()
        self.set_notional_30_once()
        self.set_margin_mode_crossed_once()

        self.disable_position_limits_once()

        # 用设置标记保证只执行一次，之后新挂的加仓单不受影响。
        if not isinstance(self.exchange, SimExchange):
            asyncio.create_task(
                self.cancel_pending_adds_once("update73 全部改市价，取消旧加仓挂单"))
        # 启动时用交易所成交明细校准最近已平仓记录（修正平台价与实际成交价的差异）
        if not isinstance(self.exchange, SimExchange):
            asyncio.create_task(self._reconcile_recent_closed())
            self._startup_sync_task = asyncio.create_task(self._sync_exchange_closures())
            asyncio.create_task(self._sync_pending_adds())   # 启动就先清掉幽灵加仓挂单
            asyncio.create_task(self._sync_manual_open_fees_all())  # 补手动加仓等未识别成交的手续费
            self._fee_sync_task = asyncio.create_task(self._fee_sync_loop())  # 今日手续费按成交时间记账
            self._position_watch_task = asyncio.create_task(self._position_watch_loop())
            if not p.get("local_stop_enabled", True):
                asyncio.create_task(self.cancel_local_stops("启动时本地止损已关闭"))
        self.db.add_log("info", "开始运行。正在连接信号服务器...")
        self._telegram_task = asyncio.create_task(self._telegram_loop())
        self._task = asyncio.create_task(self._connect_loop())

    async def stop(self):
        self.running = False
        self.started_at = 0.0
        self.db.set_setting("engine_running", "0")
        # 取消连接循环：让“停止”在重连等待期间也立即生效
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        wt = self._position_watch_task
        self._position_watch_task = None
        if wt is not None and not wt.done():
            wt.cancel()
            try:
                await wt
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        ct = self._backfill_task
        self._backfill_task = None
        if ct is not None and not ct.done():
            ct.cancel()
            try:
                await ct
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        tt = self._telegram_task
        self._telegram_task = None
        if tt is not None and not tt.done():
            tt.cancel()
            try:
                await tt
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        ft = self._fee_sync_task
        self._fee_sync_task = None
        if ft is not None and not ft.done():
            ft.cancel()
            try:
                await ft
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        ws = self.ws
        self.ws = None
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        await self._stop_worker_gracefully()
        self.db.add_log("info", "已停止运行。已开的仓位不会被自动平掉；重新运行后会通过信号回执自动对账。")

    # ==================== 连接循环（含断线处理） ====================

    async def _connect_loop(self):
        while self.running:
            attempt_start = time.time()
            self._ensure_exchange()
            p = self.params.all()
            url = WS_URL_TEMPLATE.format(key=urllib.parse.quote(p.get("signal_api_key", "")))
            try:
                mode_name = (" + ".join(self._ex_label(k) for k in self._active_exchanges())
                             or (self.exchange.name if self.exchange else "未配置交易所"))
                self.db.add_log("info", f"正在连接信号服务器...（模式：{mode_name}）")
                # 自己发应用层心跳；60 秒没收到任何消息由看门狗强制断开。
                # ping_timeout 放宽到 30 秒，避免网络抖动时误判断线。
                async with websockets.connect(url, ping_interval=25, ping_timeout=30,
                                              max_size=2 ** 22, close_timeout=5) as ws:
                    self.ws = ws
                    self.connected = True
                    self.conn_established_at = time.time()
                    self.last_msg_time = time.time()
                    self.db.add_log("info", "已连接信号服务器，等待信号推送...")
                    # 如果上次是异常断开，记录本次断连持续了多久
                    try:
                        started = float(self.db.get_setting("disconnect_started_ts", "0") or 0)
                    except Exception:
                        started = 0.0
                    if started > 0:
                        downtime = max(0, int(time.time() - started))
                        self.db.set_setting("last_disconnect_seconds", str(downtime))
                        self.db.set_setting("disconnect_started_ts", "0")
                        at = self.db.get_setting("last_disconnect_at", "")
                        self.db.add_log("info", f"已重新连接（断连时间 {at or '-'}，持续 {downtime} 秒）。")
                    self._start_worker()
                    watcher = asyncio.create_task(self._watchdog(ws))
                    try:
                        await self._receive_loop(ws)
                    finally:
                        # 注意：这里不取消消息处理协程。断线时队列里可能还有没处理完的消息，
                        # 让它继续处理完（重连期间也会自动接上），避免下单做到一半被打断。
                        watcher.cancel()
            except asyncio.CancelledError:
                raise
            except websockets.exceptions.ConnectionClosed as e:
                # 服务器主动断开：按关闭码分类处理
                if not self.running:
                    break                      # 手动停止，不记作异常断连
                code = getattr(e, "code", None)
                if not code and getattr(e, "rcvd", None):
                    code = e.rcvd.code
                code = code or 0
                alive = time.time() - attempt_start
                last_age = int(max(0, time.time() - self.last_msg_time)) if self.last_msg_time else -1
                disconnect_at = now_str()
                if code != 4401:
                    self.db.set_setting("last_disconnect_at", disconnect_at)
                    self.db.set_setting("last_disconnect_code", str(code))
                    self.db.set_setting("last_disconnect_seconds", "")
                    self.db.set_setting("disconnect_started_ts", str(time.time()))
                delay = self._close_delay(code, alive)
                if delay is None:
                    break                      # 4401 等致命错误：停止重连
                reason = ""
                if code == 4409:
                    reason = "，账户在别处登录，连接被顶替"
                elif code == 1006:
                    reason = "，异常断开（网络中断或服务器未正常关闭）"
                elif code == 1000:
                    reason = "，服务器正常关闭"
                self.db.add_log(
                    "warn",
                    f"连接断开（关闭码 {code}{reason}），本次连接存活 {int(alive)} 秒，"
                    f"最后消息 {last_age} 秒前；断连时间 {disconnect_at}；{delay} 秒后重连。")
                await self._sleep(delay)
            except (OSError, asyncio.TimeoutError,
                    websockets.exceptions.WebSocketException) as e:
                if not self.running:
                    break
                self.db.add_log("warn", f"连接失败：{e}，{self.backoff} 秒后重连")
                await self._sleep(self.backoff)
                self.backoff = min(self.backoff * 2, 60)
            except Exception as e:
                if not self.running:
                    break
                self.db.add_log("warn", f"连接异常：{e}，{self.backoff} 秒后重连")
                await self._sleep(self.backoff)
                self.backoff = min(self.backoff * 2, 60)
            finally:
                self.connected = False
                self.ws = None
        self.running = False
        self.started_at = 0.0
        await self._stop_worker_gracefully()
        self.db.add_log("info", "连接循环已退出。")

    def _start_worker(self):
        """确保只有一个后台消息处理协程（重连后继续用同一个，队列里的旧消息不会丢）"""
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._worker())

    async def _stop_worker_gracefully(self):
        """停机时等正在处理的消息走完（最多 15 秒），避免下单做到一半被强行掐断"""
        for _ in range(75):
            if not self._busy:
                break
            await asyncio.sleep(0.2)
        if self._worker_task is not None:
            self._worker_task.cancel()
            self._worker_task = None

    def _close_delay(self, code, alive):
        """按关闭码决定重连策略。返回等待秒数；返回 None 表示停止重连。"""
        if code == 4401:
            self.fatal = "API Key 无效"
            self.db.set_setting("engine_running", "0")
            self.db.add_log("alert", "信号服务器返回 4401：API Key 无效。已停止重连，请在网页里修改 API Key 后重新点击“开始运行”。")
            return None
        # 上次连接存活超过 60 秒，退避重置回 5 秒
        if alive > 60:
            self.backoff = 5
        if code == 4409:
            return 30
        d = self.backoff
        self.backoff = min(self.backoff * 2, 60)
        return d

    async def _receive_loop(self, ws):
        """接收循环：只做两件事——记原始消息、扔进队列。绝不做耗时操作。"""
        async for raw in ws:
            self.last_msg_time = time.time()
            self._append_jsonl(raw)
            try:
                self.inbox.put_nowait(raw)
            except asyncio.QueueFull:
                pass   # 队列塞满（极端情况）：丢弃也不阻塞接收

    async def _watchdog(self, ws):
        """看门狗：超过 60 秒没收到任何消息（包括 ping），判定连接已死，主动断开重连"""
        while self.connected:
            await asyncio.sleep(10)
            if self.last_msg_time and time.time() - self.last_msg_time > 60:
                self.db.add_log("warn", "超过 60 秒没收到任何消息（含心跳），判定连接已死，主动断开重连。")
                try:
                    await ws.close()
                except Exception:
                    pass
                return

    async def _worker(self):
        """后台工作协程：逐条处理队列里的消息（下单、写库都在这里做，不阻塞接收）"""
        while True:
            raw = await self.inbox.get()
            try:
                data = json.loads(raw)
            except Exception:
                self.db.add_log("warn", f"收到无法解析的消息：{raw[:200]}")
                continue
            try:
                self._busy = True
                await self.dispatch(data)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.db.add_log("error", f"处理消息出错（{e.__class__.__name__}）：{e}")
            finally:
                self._busy = False

    async def _sleep(self, seconds):
        try:
            await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            raise

    def _append_jsonl(self, raw):
        """所有原始消息追加写入 signals.jsonl（一行一条，方便事后排查）"""
        try:
            with open(JSONL_PATH, "a", encoding="utf-8") as f:
                f.write(raw if isinstance(raw, str) else raw.decode("utf-8", "replace"))
                f.write("\n")
        except Exception:
            pass

    # ==================== 消息分发 ====================

    async def dispatch(self, data):
        """按消息类型分发。收到消息必须立刻处理完（实际耗时操作在协程内排队完成）"""
        self._ensure_exchange()
        mtype = data.get("type", "")
        # 平台会在信号开始、结束（按结果补扣）和通知里更新点数余额。
        # 不能只靠 hello/active，否则顶部点数会一直是连接时的旧值。
        self._update_balance_from_msg(data, mtype)

        if mtype == "hello":
            self.balance = data.get("balance")
            cost = data.get("cost")
            self.db.add_log("info", f"连接成功（hello）。当前点数余额：{self.balance}，每条信号扣除 {cost} 点。")
        elif mtype == "ping":
            pass   # 只用来证明连接还活着，时间戳已在接收循环里更新
        elif mtype == "signal.active":
            await self._handle_active(data)
        elif mtype == "signal.trailed":
            await self._handle_trailed(data)
        elif mtype == "signal.ended":
            await self._handle_ended(data)
        elif mtype == "signal.receipt":
            await self._handle_receipt(data)
        elif mtype == "notice":
            code = data.get("code")
            text = data.get("text", "")
            if code == "balance_empty":
                self.balance = 0
                self.db.add_log("alert", f"【点数不足】{text} 点数已用完，新信号将不再推送。请及时充值。")
            else:
                self.db.add_log("warn", f"服务器通知：{text or code}")
        else:
            self.db.add_log("warn", f"收到未知类型的消息：{mtype}")

    def _update_balance_from_msg(self, data, mtype=""):
        """从平台消息里同步点数余额：优先 balance/points 字段，
        没有字段时解析“当前 162 点 / 余额 162 点 / 剩余 162 点”这类通知文本。"""
        value = None
        for key in ("balance", "points", "point", "point_balance"):
            if key in data and data.get(key) not in (None, ""):
                value = data.get(key)
                break
        new = None
        if value not in (None, ""):
            try:
                new = int(float(value))
            except Exception:
                new = None
        if new is None:
            text = " ".join(str(data.get(k) or "") for k in ("text", "msg", "message"))
            m = re.search(r"(?:当前|剩余|余额)\s*[:：]?\s*(\d+)\s*点", text)
            if m:
                new = int(m.group(1))
        if new is not None:
            self.balance = new

    def _active_exchanges(self):
        """当前在用的适配器。测试或旧代码只设置了 self.exchange 时回退成单个 Bitget。"""
        if self.exchanges:
            return self.exchanges
        if self.exchange is not None:
            return {"bitget": self.exchange}
        return {}

    def _ex(self, sig_or_exchange=None):
        """按仓位记录选择交易所适配器；模拟盘统一用 SimExchange。"""
        if isinstance(self.exchange, SimExchange):
            return self.exchange
        if isinstance(sig_or_exchange, str):
            name = sig_or_exchange
        else:
            name = str((sig_or_exchange or {}).get("exchange") or "bitget")
        return self._active_exchanges().get(name) or self.exchange

    def _is_sim(self):
        return isinstance(self.exchange, SimExchange)

    async def _prime_gate_symbols(self):
        """把数据库里的 Gate 交易对映射成 Gate 合约名（含 1000 倍币等换名合约）。
        只在本次运行第一次对账前做一次，避免重启后 fetch_positions 的 symbol 对不上。"""
        if self._gate_symbols_primed:
            return
        gate = (self._active_exchanges() or {}).get("gate")
        if gate is None or not hasattr(gate, "resolve_symbol"):
            self._gate_symbols_primed = True
            return
        try:
            rows = (self.db.open_positions("gate")
                    + self.db.closed_history(500, exchange="gate"))
        except Exception:
            rows = []
        for sig in rows:
            try:
                await gate.resolve_symbol(sig.get("symbol"))
            except Exception:
                continue
        self._gate_symbols_primed = True

    async def _positions_by_exchange(self):
        """同时读取两家交易所的真实持仓，key = (交易所, 平台交易对)。"""
        out = {}
        for name, ex in self._active_exchanges().items():
            try:
                for pos in await ex.fetch_positions() or []:
                    try:
                        if D(pos.get("total") or "0") != 0:
                            out[(name, str(pos.get("symbol") or ""))] = pos
                    except Exception:
                        continue
            except Exception:
                continue
        return out

    async def _pick_entry_exchange(self, symbol, route=None):
        """按路由给新开仓选交易所：返回 (适配器, 交易所名, 合约参数, 错误)。
        默认 Gate 优先，Gate 没有该交易对或不可交易时回退 Bitget。"""
        p = self.params.all()
        route = str(route or p.get("new_entry_route") or "gate_first")
        preferred = "gate" if route in ("gate_first", "gate_only") else "bitget"
        if self._is_sim():
            ex = self.exchange
            return ex, preferred, await ex.fetch_lot_filters(symbol), ""
        order = {"gate_first": ["gate", "bitget"], "gate_only": ["gate"],
                 "bitget_first": ["bitget", "gate"],
                 "bitget_only": ["bitget"]}.get(route, ["gate", "bitget"])
        errors = []
        for name in order:
            candidate = self._active_exchanges().get(name)
            if candidate is None:
                errors.append(f"{self._ex_label(name)}未配置")
                continue
            f = await candidate.fetch_lot_filters(symbol)
            if f is None:
                err = getattr(candidate, "last_contract_error", "") or "无法获取合约信息"
                errors.append(f"{self._ex_label(name)}：{err}")
                continue
            st = str(f.get("symbolStatus") or "normal").lower()
            if st not in ("normal", "online", ""):
                errors.append(f"{self._ex_label(name)} 合约状态「{st}」不可交易")
                continue
            return candidate, name, f, ""
        return None, preferred, None, "；".join(errors) or "两家交易所都没有该交易对"

    @staticmethod
    def _ex_label(name):
        return "Gate" if str(name or "").lower() == "gate" else "Bitget"

    def _combined_total_funds(self):
        """两家交易所“账户总资金”之和，用于合计收益率和熔断。模拟盘只算一份。"""
        if self._is_sim():
            try:
                return self.params.effective_total_funds("bitget")
            except Exception:
                return Decimal("0")
        total = Decimal("0")
        for name in self._active_exchanges():
            try:
                total += self.params.effective_total_funds(name)
            except Exception:
                pass
        return total

    def _ensure_exchange(self):
        """按最新参数自动切换模拟盘 / 真实盘（改完参数点保存立即生效）。
        实盘同时创建 Bitget 和 Gate 两个适配器；新仓默认走 Gate，旧仓按记录的 exchange 路由。"""
        # 离线自检/外部注入的假交易所：不要用真实网络连接把它替换掉
        if (self.exchange is not None
                and not isinstance(self.exchange, (SimExchange, BitgetExchange, GateExchange))):
            return
        p = self.params.all()
        bitget_ready = bool(p.get("exchange_api_key") and p.get("exchange_secret")
                            and p.get("exchange_passphrase"))
        gate_ready = bool(p.get("gate_enabled") and p.get("gate_api_key")
                          and p.get("gate_secret"))
        # 云端切换实盘后会在 /etc/signal.env 写入 TRADE_MODE=live，锁定为真实盘，
        # 防止误切回模拟盘把实盘数据库污染。
        live_locked = os.environ.get("TRADE_MODE", "").strip().lower() == "live"
        # 参数要求实盘但密钥不全 -> 实际仍按模拟盘运行，避免误下单
        want_sim = (not live_locked) and (p["sim_mode"] or not (bitget_ready or gate_ready))
        is_sim = isinstance(self.exchange, SimExchange)
        creds = (p.get("exchange_api_key"), p.get("exchange_secret"),
                 p.get("exchange_passphrase"), p.get("gate_api_key"),
                 p.get("gate_secret"), bool(p.get("gate_enabled")))
        # 模式没变、密钥没变才继续用现有连接；改了密钥立即重建（保存即生效）
        if (self.exchange is not None and is_sim == want_sim
                and (want_sim or creds == self._exchange_creds)):
            return
        old_adapters = list({id(x): x for x in self._active_exchanges().values() if x}.values())
        self._exchange_creds = creds
        self._gate_symbols_primed = False
        if want_sim:
            sim = SimExchange()
            self.exchange = sim
            self.exchanges = {"bitget": sim, "gate": sim}
            if not p["sim_mode"] and not (bitget_ready or gate_ready):
                self.db.add_log("alert", "已要求真实交易，但 Bitget/Gate 的 API Key 都没填全，继续使用模拟盘。请在参数区填写后保存。")
            elif old_adapters:
                self.db.add_log("info", "已切换为模拟盘模式（所有下单只记日志，不发往交易所）。")
        else:
            bitget = (BitgetExchange(p["exchange_api_key"], p["exchange_secret"],
                                     p["exchange_passphrase"]) if bitget_ready else None)
            gate = (GateExchange(p["gate_api_key"], p["gate_secret"])
                    if gate_ready else None)
            self.exchanges = {}
            if bitget is not None:
                self.exchanges["bitget"] = bitget
            if gate is not None:
                self.exchanges["gate"] = gate
            self.exchange = bitget or gate
            if self.exchange is None:
                self.db.add_log("alert", "没有可用的交易所：Bitget/Gate 的 API Key 都没填全。程序不会下单，请先在参数区填写。")
            else:
                names = " + ".join(self._ex_label(k) for k in self.exchanges)
                if any(isinstance(x, SimExchange) for x in old_adapters):

                    self.db.add_log("alert", f"【已切换到真实交易模式】接下来的下单会使用 {names} 真实资金！")
                else:
                    self.db.add_log("info", f"已连接真实交易账户：{names}（实盘模式，下单使用真实资金）。")
        current_ids = {id(x) for x in self._active_exchanges().values() if x}
        for ex in old_adapters:
            if id(ex) not in current_ids:
                asyncio.create_task(self._safe_close_exchange(ex))

    async def _safe_close_exchange(self, ex):
        try:
            await ex.close()
        except Exception:
            pass

    # ==================== 信号处理 ====================

    def _passes_filter(self, source, direction):
        """来源 / 方向筛选。没勾选 = 全部通过。"""
        p = self.params.all()
        if p["sources"] and source not in p["sources"]:
            return False
        if p["directions"] and direction not in p["directions"]:
            return False
        return True

    def _is_fused(self):
        """当日累计亏损是否达到熔断线（账户总资金 × 3%）。
        口径与需求一致：按服务器给的 profit_percent × 名义价值计算，不含手续费。
        网页上可以关闭熔断；关闭后这里永远返回 False。"""
        if not self.params.all()["fuse_enabled"]:
            return False
        stats = self.db.today_stats()
        try:
            funds = self._combined_total_funds()
        except Exception:
            funds = Decimal("0")
        if funds <= 0:
            return False
        pnl = stats["realized_pnl"]
        loss = -pnl if pnl < 0 else Decimal("0")
        return loss >= funds * FUSE_RATIO

    @staticmethod
    def _fee_rate(filters):
        """从合约参数里取吃单费率；取不到用兜底 0.06%"""
        try:
            return D(filters.get("takerFeeRate") or DEFAULT_TAKER_FEE)
        except Exception:
            return DEFAULT_TAKER_FEE

    def _open_fee_for(self, filters, qty, price):
        """开仓手续费（USDT）：数量 × 价格 × 吃单费率"""
        try:
            return D(qty) * D(price) * self._fee_rate(filters)
        except Exception:
            return Decimal("0")

    @staticmethod
    def _favorable_limit_price(ref_price, direction, tick):
        """优于平台参考价 1% 的开仓限价：做多×0.99（向下取整），做空×1.01（向上取整）"""
        ref = D(ref_price)
        t = D(tick)
        if t <= 0:
            return ref * (Decimal("0.99") if direction == "long" else Decimal("1.01"))
        if direction == "long":
            return quantize(ref * Decimal("0.99"), t)
        try:
            return ((ref * Decimal("1.01")) / t).to_integral_value(rounding=ROUND_UP) * t
        except Exception:
            return quantize(ref * Decimal("1.01"), t)

    @staticmethod
    def _is_marketable(mark, limit_price, direction):
        return mark <= D(limit_price) if direction == "long" else mark >= D(limit_price)

    @staticmethod
    def _resolve_head_multiplier(msg, p):
        """优先使用信号中的 first_qty；字段缺失或非法时使用已配置的首仓倍数。"""
        raw = msg.get("first_qty")
        if raw not in (None, ""):
            try:
                hm = D(str(raw))
                if Decimal("0.01") <= hm <= Decimal("100"):
                    return hm, "平台推送"
            except Exception:
                pass
        try:
            hm = D(p.get("head_multiplier") or "1")
        except Exception:
            hm = Decimal("1")
        if hm <= 0:
            hm = Decimal("1")
        return hm, "网页参数"

    async def _handle_active(self, msg):
        """新信号：防重复 → 过滤 → 风控检查 → 算数量 → 市价开仓 → 挂止损保护单"""
        sig_id = str(msg.get("id", ""))
        symbol = str(msg.get("symbol", "")).upper()
        direction = str(msg.get("direction", ""))
        source = str(msg.get("source", ""))
        entry, target, stop = msg.get("entry"), msg.get("target"), msg.get("stop")
        if "balance" in msg:
            self.balance = msg.get("balance")
        p = self.params.all()
        route = str(p.get("new_entry_route") or "gate_first")
        preferred = "gate" if route in ("gate_first", "gate_only") else "bitget"
        chosen = {"name": ""}

        if not sig_id or not symbol or direction not in ("long", "short"):
            self.db.add_log("warn", f"收到内容不完整的信号：{json.dumps(msg, ensure_ascii=False)[:200]}")
            return

        # 1. 防重复开仓：本地已有这个 id 就忽略
        if self.db.get_signal(sig_id) is not None:
            self.db.add_log("info", f"跳过：信号 {sig_id} 之前已处理过（防重复开仓）。")
            return

        def skip(reason, why):
            self.db.upsert_signal(
                id=sig_id, symbol=symbol, direction=direction, source=source,
                entry=str(entry), stop=str(stop), target=str(target),
                exchange=chosen["name"] or preferred,
                status=ST_SKIPPED, skip_reason=reason,
                open_time=now_str(), open_ts=time.time(), note=why)
            self.db.add_log("info", f"跳过信号 {sig_id}（{symbol} {DIRECTION_NAMES.get(direction, direction)}）：{why}")

        # 2. 来源 / 方向筛选
        if not self._passes_filter(source, direction):
            skip(SKIP_FILTER, f"来源「{source}」或方向「{DIRECTION_NAMES.get(direction, direction)}」被筛选条件排除")
            return

        # 3. 单日亏损熔断检查
        if self._is_fused():
            skip(SKIP_FUSED, "当日亏损已触及熔断线，暂停开新仓")
            return


        stats = self.db.today_stats()
        if p["max_daily_opens"] > 0 and stats["opened"] >= p["max_daily_opens"]:
            skip(SKIP_DAILY, f"今日已开仓 {stats['opened']} 次，达到每日上限 {p['max_daily_opens']}")
            return


        positions = self.db.open_positions()
        pending_entries = self.db.pending_entries()
        if p["max_positions"] > 0 and len(positions) + len(pending_entries) >= p["max_positions"]:
            skip(SKIP_MAXPOS, f"当前持仓/挂单 {len(positions) + len(pending_entries)} 个，达到最大持仓数 {p['max_positions']}")
            return

        # 6. 防重复/防乱加仓：同一交易对只允许「同一条信号」的仓位。
        #    平台自己的马丁加仓通过 signal.trailed 下发（同 id），不在这里新建仓位。
        for pos in positions + pending_entries:
            if pos["symbol"] == symbol:
                skip(SKIP_SAME_SYMBOL,
                     f"交易对 {symbol} 已有持仓或挂单（信号 {pos['id']}）。不接受其他信号重复开仓")
                return

        # 7. 合约校验：按参数路由找交易对（默认 Gate 优先，Gate 没有的回退 Bitget）
        #    （模拟盘也要“实事求是”，不能对不存在的币种假装开仓）
        is_sim = self._is_sim()
        if is_sim:
            ex = self.exchange
            ex_name = preferred
            filters = await ex.fetch_lot_filters(symbol)
            if filters is None:
                err = getattr(ex, "last_contract_error", "") or "无法获取合约信息"
                skip(SKIP_NO_BITGET,
                     f"模拟盘行情里没有该交易对或合约不可用：{err}（不会假装开仓）")
                return
        else:
            order = {"gate_first": ["gate", "bitget"], "gate_only": ["gate"],
                     "bitget_first": ["bitget", "gate"],
                     "bitget_only": ["bitget"]}.get(route, ["gate", "bitget"])
            ex, filters, ex_name = None, None, ""
            errors = []
            for name in order:
                candidate = self._active_exchanges().get(name)
                if candidate is None:
                    errors.append(f"{self._ex_label(name)}未配置")
                    continue
                f = await candidate.fetch_lot_filters(symbol)
                if f is None:
                    err = getattr(candidate, "last_contract_error", "") or "无法获取合约信息"
                    errors.append(f"{self._ex_label(name)}：{err}")
                    continue
                st = str(f.get("symbolStatus") or "normal").lower()
                if st not in ("normal", "online", ""):
                    errors.append(f"{self._ex_label(name)} 合约状态「{st}」不可交易")
                    continue
                ex, filters, ex_name = candidate, f, name
                break
            if ex is None:
                skip(SKIP_NO_CONTRACT, "；".join(errors) or "两家交易所都没有该交易对")
                return
        chosen["name"] = ex_name
        ex_label = self._ex_label(ex_name)
        status = str(filters.get("symbolStatus") or "normal").lower()
        if status not in ("normal", "online", ""):
            skip(SKIP_NO_CONTRACT, f"该合约在 {ex_label} 的状态是「{status}」，当前不可交易")
            return

        # 8. 标记价：每次开仓前现取（不用缓存），取不到就跳过，不用入场价代替
        mark = await ex.fetch_mark_price(symbol)
        if mark is None:
            err = getattr(ex, "last_price_error", "") or "无法获取标记价"
            skip(SKIP_NOPRICE, f"无法获取 {ex_label} 标记价：{err}")
            return

        # 9. 开仓价为“优于平台 entry 1%”的限价；限价本身限制了我们愿意接受的最差价格，
        #    因此不再用原来的 0.5% 入场偏离过滤（否则永远等不到比平台优 1% 的价格）。

        # 10. 真实盘额外检查：必须使用单向持仓模式
        if not is_sim:
            cfg = await ex.fetch_account_config(symbol)
            if cfg.get("hold_mode") == "hedge_mode":
                skip(SKIP_POSITION_MODE,
                     f"{ex_label} 账户当前是「双向持仓模式」。本程序只支持单向持仓模式，请先在 {ex_label} 切换为单向持仓")
                self.db.add_log("alert", f"【需要设置】{ex_label} 账户是双向持仓模式。请到 {ex_label} 合约设置里切换为「单向持仓」后再使用实盘。")
                return


        #     头仓名义价值 = 名义价值 × 首仓倍数（逆势 0.5 → 30U×0.5=15U）。
        head_mult, head_src = self._resolve_head_multiplier(msg, p)
        try:
            step = D(filters["stepSize"])
            min_qty = D(filters["minQty"])
            min_notional = D(filters["minNotional"])
            tick = D(filters.get("tickSize") or "0")
            ref_price = D(entry) if entry not in (None, "") else mark
            exec_price = mark
            target_value = p["notional"] * head_mult
            qty, min_fill, min_fill_value = head_entry_qty(target_value, exec_price, filters)
        except Exception:
            skip(SKIP_TOOSMALL, f"{symbol} 的精度参数异常：{filters}")
            return
        if qty < min_qty:
            skip(SKIP_TOOSMALL, f"数量 {qty} 小于交易所最小数量 {min_qty}")
            return
        if qty * exec_price < min_notional:
            skip(SKIP_TOOSMALL, f"名义价值 {pct_str(qty * exec_price)} USDT 小于交易所最小要求 {pct_str(min_notional)} USDT")
            return
        fill_note = ""
        if min_fill:
            fill_note = (f"；头仓目标 {pct_str(target_value)}U 不够最小一手，"
                         f"按最小 {qty}（约 {pct_str(min_fill_value)}U）开仓")

        # 12. 开仓（真实盘先设置保证金模式和杠杆，并回读确认；失败不按相反模式硬开）
        hold_side = "long" if direction == "long" else "short"
        target_mode = "crossed" if p.get("margin_mode") == "crossed" else "isolated"
        mode_label = "全仓" if target_mode == "crossed" else "逐仓"
        if not is_sim:
            mm_ok, mm_err = await ex.ensure_margin_mode(symbol, target_mode)
            if not mm_ok:
                # 回读不一定可靠（个别币种/接口返回异常）：不硬开相反模式，但仍按“显式目标模式”下单；
                # 交易所若认为模式不一致会直接拒绝，不会开成相反模式。
                self.db.add_log(
                    "warn",
                    f"{symbol} 在 {ex_label} 的{mode_label}回读未确认（{mm_err}）；仍按显式{mode_label}尝试下单，交易所拒绝就不会开仓。")
            lev_ok = await ex.set_leverage(symbol, p["leverage"], hold_side)
            if not lev_ok:   # 个别情况下交易所不接受带方向的杠杆设置，退回不带方向的写法
                lev_ok = await ex.set_leverage(symbol, p["leverage"])
            if not lev_ok:
                skip(SKIP_ORDERFAIL, f"无法在 {ex_label} 设置杠杆 {p['leverage']} 倍，未开仓")
                self.db.add_log("alert", f"【需要设置】{symbol} 无法在 {ex_label} 设置杠杆 {p['leverage']} 倍，已跳过该信号。")
                return

        open_side = "BUY" if direction == "long" else "SELL"
        client_id = f"sl_{sig_id}_open"
        self.db.add_log("info",
                        f"信号 {sig_id}（{symbol} {DIRECTION_NAMES.get(direction, direction)}，来源：{source}）："
                        f"路由到 {ex_label}，标记价 {pct_str(mark)}，平台价 {pct_str(ref_price)}，"
                        f"头仓 {pct_str(head_mult)}X（{head_src}），"
                        f"市价开仓（{mode_label if not is_sim else '模拟'}），"
                        f"数量 {qty}（名义约 {pct_str(qty * exec_price)} USDT）{fill_note}")
        result = await ex.market_open(
            symbol, open_side, qty, client_id,
            margin_mode=None if is_sim else target_mode)

        if result.ok is None:
            # 结果未知（网络超时）：绝不盲目重试，记录待核对 + 红色告警
            self.db.upsert_signal(
                id=sig_id, symbol=symbol, direction=direction, source=source,
                exchange=ex_name,
                qty=str(qty), notional=str(p["notional"]), entry=str(entry),
                stop=str(stop), target=str(target), status=ST_UNKNOWN, skip_reason="",
                open_time=now_str(), open_ts=time.time(),
                note=f"开仓结果未知：{result.error}")
            self.db.add_log("alert",
                            f"【需人工核对】信号 {sig_id}（{symbol}）在 {ex_label} 开仓结果未知（网络超时），程序不会自动重试。"
                            f"请到 {ex_label} 核对是否已成交：{result.error}")
            return
        if result.ok is False:
            self.db.upsert_signal(
                id=sig_id, symbol=symbol, direction=direction, source=source,
                exchange=ex_name,
                qty=str(qty), notional=str(p["notional"]), entry=str(entry),
                stop=str(stop), target=str(target), status=ST_DROPPED, skip_reason=SKIP_ORDERFAIL,
                open_time=now_str(), open_ts=time.time(), note=result.error)
            self.db.add_log("alert", f"信号 {sig_id}（{symbol}）在 {ex_label} 开仓被拒绝：{result.error}")
            return

        # 13. 开仓成功 → 记录持仓、计算开仓手续费，然后挂止损保护单
        taker = self._fee_rate(filters)
        open_fee = self._open_fee_for(filters, qty, mark)
        if is_sim:
            fee_note = f"模拟盘手续费（按 taker 费率 {pct_str(taker * 100)}% 估算）"
        else:
            fee_note = "手续费暂为估算，正在获取交易所实际值..."
        self.db.upsert_signal(
            id=sig_id, symbol=symbol, direction=direction, source=source,
            exchange=ex_name,
            qty=str(qty), notional=str(p["notional"]), entry=str(entry),
            avg_entry=str(mark), base_qty=str(qty),
            martin_layers=0, martin_multiplier=str(head_mult),
            head_multiplier=str(head_mult), platform_multiplier=str(head_mult),
            stop=str(stop), target=str(target), status=ST_HOLDING, skip_reason="",
            open_time=now_str(), open_ts=time.time(),
            note="开仓成功" + ("；头仓目标不够最小一手，已按最小一手凑整" if min_fill else ""),
            open_order_id=result.order_id or "",
            open_order_ids=result.order_id or "",
            pending_open_order_id="", pending_open_qty="", pending_open_price="",
            open_fee=money_str(open_fee), close_fee="", gross_pnl="", net_pnl="",
            fee_note=fee_note,
            margin_mode="" if is_sim else target_mode,
            leverage="" if is_sim else str(p["leverage"]),
            funding_fee="0", funding_note=f"资金费（按 {ex_label} 公开资金费率估算）",
            last_funding_ts=time.time())
        self.mark_cache[(ex_name, symbol)] = (time.time(), mark)
        if self._is_sim():
            self.mark_cache[symbol] = (time.time(), mark)
        self.db.record_qty_event(sig_id, time.time(), str(qty), "开仓")
        if p.get("local_stop_enabled", True):
            self.db.add_log("info", f"信号 {sig_id}（{symbol}）开仓成功：数量 {qty}，均价 {pct_str(mark)}。开始挂止损保护单...")
        else:
            self.db.update_signal(sig_id, note="开仓成功；本地止损关闭，等平台结束信号")
            self.db.add_log("info", f"信号 {sig_id}（{symbol}）开仓成功：数量 {qty}，均价 {pct_str(mark)}。"
                                    f"本地止损已按策略关闭，等平台结束信号。")

        # 实盘：后台查询交易所实际开仓手续费（不阻塞后续信号处理）
        if not is_sim and result.order_id:
            asyncio.create_task(self._enrich_open_fees(
                sig_id, symbol, ex, sync_qty=True))
        # 实盘：开仓后回读实际保证金模式/杠杆，防止交易所“设置成功但没生效”
        if not is_sim:
            asyncio.create_task(self._verify_open_settings(
                sig_id, symbol, hold_side, int(p["leverage"]), target_mode,
                exchange=ex))

        if not p.get("local_stop_enabled", True):
            return

        try:
            stop_price = quantize(stop, tick)
        except Exception:
            stop_price = D(stop)
        # 止损价如果已经在现价“错误的一侧”，挂出去会被交易所立即触发/拒绝，先提示人工处理
        if (direction == "long" and stop_price >= mark) or (direction == "short" and stop_price <= mark):
            self.db.update_signal(sig_id, note="开仓成功，但止损价已越过现价，止损单未挂，请人工处理")
            self.db.add_log("alert", f"【需人工处理】信号 {sig_id}（{symbol}）止损价 {stop_price} 已越过现价 {pct_str(mark)}，止损单未挂。")
            return
        result2 = await ex.place_stop_close_position(
            symbol, direction, stop_price, f"sl_{sig_id}_sl", qty=qty)
        if result2.ok is not True:
            self.db.update_signal(sig_id, note=f"开仓成功，但止损单挂单失败：{result2.error}，请人工处理")
            self.db.add_log("alert", f"【需人工处理】信号 {sig_id}（{symbol}）开仓成功，但 {ex_label} 止损单挂单失败：{result2.error or '结果未知'}。")
        else:
            self.db.add_log("info", f"信号 {sig_id}（{symbol}）止损保护单已挂上（止损价 {stop_price}）。")

    async def _handle_trailed(self, msg):
        """止损止盈更新 / 马丁加仓：
        - 普通 trailed：更新止损止盈，撤旧止损单按新 stop 重挂
        - reason=martingale：先按平台数据同步加仓（层数、累计仓位倍数），再更新止损止盈
        """
        sig_id = str(msg.get("id", ""))
        new_target, new_stop = msg.get("target"), msg.get("stop")
        text = msg.get("text", "")
        sig = self.db.get_signal(sig_id)
        if sig is None:
            self.db.add_log("info", f"收到移动止损消息，但本地没有信号 {sig_id} 的记录，忽略。")
            return
        symbol = sig["symbol"]
        if sig["status"] == ST_PENDING:
            fields = {
                "stop": str(new_stop) if new_stop is not None else sig.get("stop", ""),
                "target": str(new_target) if new_target is not None else sig.get("target", ""),
                "note": f"限价开仓挂单中，已更新止盈止损：{text}",
            }
            m = msg.get("martingale") or {}
            if m.get("qty") not in (None, ""):
                fields["platform_multiplier"] = str(m.get("qty"))
            if m.get("avg") not in (None, ""):
                fields["platform_avg"] = str(m.get("avg"))
            self.db.update_signal(sig_id, **fields)
            self.db.add_log("info", f"{symbol} 限价开仓挂单中，仅更新止盈止损，尚未成交。")
            return
        if sig["status"] not in (ST_HOLDING, ST_UNKNOWN):
            # 影子跟踪：我们已平仓，但平台还在跟踪这个信号，只记录平台最新止损/止盈，不下单
            if sig["status"] == ST_CLOSED and str(sig.get("platform_tracking") or "") == "1":
                fields = {
                    "platform_stop": str(new_stop) if new_stop not in (None, "") else sig.get("platform_stop", ""),
                    "platform_target": str(new_target) if new_target not in (None, "") else sig.get("platform_target", ""),
                    "platform_update_time": now_str(),
                }
                try:
                    avg = (msg.get("martingale") or {}).get("avg")
                    if avg not in (None, ""):
                        fields["platform_avg"] = str(avg)
                except Exception:
                    pass
                self.db.update_signal(sig_id, **fields)
                self.db.add_log("info",
                                f"平台仍在跟踪信号 {sig_id}（{sig['symbol']}）：移动止损 {new_stop or '-'}，"
                                f"止盈 {new_target or '-'}。我们已平仓，仅记录平台状态。")
            else:
                self.db.add_log("info", f"信号 {sig_id} 状态是「{sig['status']}」，忽略移动止损消息。")
            return
        prev_stop = sig.get("stop")
        is_martin = (msg.get("reason") == "martingale") or isinstance(msg.get("martingale"), dict)


        # 先把平台给的止损止盈记下来（即使加仓失败，止损也要同步更新）
        self.db.update_signal(
            sig_id,
            stop=str(new_stop) if new_stop is not None else sig["stop"],
            target=str(new_target) if new_target is not None else sig["target"],
            note=("马丁加仓消息：" + text) if is_martin else f"止损止盈已更新：{text}")
        if sig["status"] == ST_UNKNOWN:
            self.db.add_log("warn", f"信号 {sig_id}（{symbol}）开仓结果未知，只更新记录，不重挂止损单。")
            return

        # 马丁加仓：先加仓，再更新止损（旧的全仓止损单会覆盖新数量，但止损价需要按平台更新）
        if is_martin:
            await self._sync_martingale(sig, msg)
            sig = self.db.get_signal(sig_id) or sig

        if not self.params.all().get("local_stop_enabled", True):
            self.db.add_log("info", f"信号 {sig_id}（{symbol}）止损/止盈已更新（本地止损关闭，等平台结束信号）。{text}")
            return

        stop_value = new_stop if new_stop not in (None, "") else sig.get("stop")
        if stop_value in (None, ""):
            return
        stop_changed = (new_stop not in (None, "")) and (str(new_stop) != str(prev_stop))
        if not stop_changed:
            self.db.add_log("info", f"信号 {sig_id}（{symbol}）止损价未变化，无需重挂。{text}")
            return
        await self._apply_stop(sig, stop_value, text)

    @staticmethod
    def _martingale_margin_mode(sig):
        """加仓沿用仓位现有保证金模式；缺失/异常时保守用逐仓。"""
        mode = str(sig.get("margin_mode") or "").strip().lower()
        if mode == "cross":
            mode = "crossed"
        return mode if mode in ("isolated", "crossed") else "isolated"

    async def _resolve_margin_mode(self, sig, fallback="isolated"):
        """加仓前取交易所持仓的真实保证金模式，避免本地同步滞后把全仓仓位当成逐仓。
        取不到时回退到数据库记录/逐仓。"""
        if self._is_sim():
            return fallback
        ex = self._ex(sig)
        try:
            for pos in await ex.fetch_positions() or []:
                try:
                    if (str(pos.get("symbol") or "") != str(sig.get("symbol") or "")
                            or D(pos.get("total") or "0") == 0):
                        continue
                    mode = str(pos.get("marginMode") or "").strip().lower()
                    if mode == "cross":
                        mode = "crossed"
                    if mode in ("isolated", "crossed"):
                        if mode != str(sig.get("margin_mode") or ""):
                            self.db.update_signal(sig["id"], margin_mode=mode)
                        return mode
                except Exception:
                    continue
        except Exception:
            pass
        return fallback

    def _resolve_platform_mult(self, sig, m):
        """把平台 martingale.qty 换算成「绝对累计倍数」（1X = 该记录的基础名义价值，含头仓）。
        平台可能给绝对倍数（0.5 头仓的第一档约 0.7765X），也可能给相对头仓的倍数（1.5529X）。
        用平台自己的入场价/加仓价/均价反推：≈1 → 相对；≈头仓倍数 → 绝对。判定结果存设置，之后沿用。"""
        try:
            raw = D(m.get("qty") or "1")
        except Exception:
            raw = Decimal("1")
        try:
            head = D(sig.get("head_multiplier") or "1")
        except Exception:
            head = Decimal("1")
        if head <= 0:
            head = Decimal("1")
        if head == 1:
            return raw
        mode = str(self.db.get_setting("platform_qty_mode", "") or "")
        if mode == "relative":
            return raw * head
        if mode == "absolute":
            return raw
        try:
            grades = m.get("grades") or []
            avg, price = D(m.get("avg")), D(m.get("price"))
            entry = D(sig.get("entry"))
            if (len(grades) == 1 and int(grades[0]) == 1
                    and avg > 0 and price > 0 and entry > 0 and entry != price):
                implied = raw * (avg - price) / (entry - price)
                if abs(implied - 1) <= Decimal("0.08"):
                    self.db.set_setting("platform_qty_mode", "relative")
                    self.db.add_log("info", "平台马丁倍数口径：相对头仓（目标 = 基础名义价值 × 头仓倍数 × 平台倍数）。")
                    return raw * head
                if abs(implied - head) <= max(head * Decimal("0.2"), Decimal("0.05")):
                    self.db.set_setting("platform_qty_mode", "absolute")
                    self.db.add_log("info", "平台马丁倍数口径：绝对累计倍数（目标 = 基础名义价值 × 平台倍数）。")
                    return raw
        except Exception:
            pass
        return raw

    async def _sync_martingale(self, sig, msg):
        """按平台的 martingale 数据同步加仓。
        平台字段说明：grades=已触发的加仓档位，qty=平台累计仓位倍数（1 = 原始仓位），
        avg=平台均价（仅作参考），price=平台加仓时价格。
        按兼容配置，倍数按【仓位价值】算：目标价值 = 每信号名义价值 × qty。
        例如基础 30U、qty=2 → 目标总仓位 60U；本次加仓价值 = 目标价值 − 当前仓位价值。"""
        sig_id = sig["id"]
        symbol = sig["symbol"]
        direction = sig["direction"]
        ex = self._ex(sig)
        ex_label = self._ex_label(sig.get("exchange"))
        m = msg.get("martingale") or {}
        try:
            grades = [int(x) for x in (m.get("grades") or [])]
        except Exception:
            grades = []
        try:
            cur_layers = int(sig.get("martin_layers") or 0)
        except Exception:
            cur_layers = 0
        layers = max(grades) if grades else cur_layers
        if layers < cur_layers:
            layers = cur_layers
        target_mult = self._resolve_platform_mult(sig, m)
        # 平台最新目标倍数先落库：即使加仓因最小下单限制失败，界面也能显示“平台目标 / 实际”
        self.db.update_signal(sig_id, platform_multiplier=str(target_mult))
        try:
            cur_mult = D(sig.get("martin_multiplier") or "1")
        except Exception:
            cur_mult = Decimal("1")

        # 重复 / 过期的加仓消息：不重复加仓，只记录层数
        if target_mult <= cur_mult + Decimal("0.0000001"):
            self.db.update_signal(sig_id, martin_layers=layers,
                                  note=f"马丁加仓第 {layers} 档消息已处理过（仓位 {target_mult}X），只同步止损止盈")
            self.db.add_log("info", f"信号 {sig_id}（{symbol}）：收到重复或过期的马丁加仓消息"
                                    f"（第 {layers} 档，仓位 {target_mult}X），不重复加仓。")
            return False

        # 加仓前现取标记价和合约参数（模拟盘同样要求 Bitget 真实存在）。

        mark = None
        filters = None
        for attempt in range(3):
            mark = await ex.fetch_mark_price(symbol)
            filters = await ex.fetch_lot_filters(symbol)
            if mark is not None and filters is not None:
                break
            if attempt < 2:
                await asyncio.sleep(2)
        if mark is None or filters is None:
            err = (getattr(ex, "last_contract_error", "")
                   or getattr(ex, "last_price_error", "") or "无法获取行情/合约数据")
            self.db.update_signal(sig_id, martin_layers=layers,
                                  note=f"收到马丁加仓第 {layers} 档，但 {ex_label} 数据不可用，未加仓")
            self.db.add_log("alert", f"【需人工处理】信号 {sig_id}（{symbol}）马丁加仓第 {layers} 档："
                                     f"无法获取 {ex_label} 行情/合约数据（{err}），未同步加仓。")
            return False


        # 不用标记价，否则价格波动会让实际倍数和平台目标对不上）。
        # 目标名义价值 = 每信号基础名义价值 × 平台累计倍数；加仓价值 = 目标 − 当前名义价值。
        base_notional = D(sig.get("notional") or "0")
        old_qty = D(sig.get("qty") or "0")
        old_avg = D(sig.get("avg_entry") or sig.get("entry") or "0")
        target_value = base_notional * target_mult
        current_value = old_qty * old_avg if old_avg > 0 else old_qty * mark
        add_value = target_value - current_value
        if add_value <= 0:
            self.db.update_signal(
                sig_id, martin_layers=layers, martin_multiplier=str(target_mult),
                note=f"马丁加仓第 {layers} 档：当前名义价值 {pct_str(current_value)}U "
                     f"已达到目标 {pct_str(target_value)}U，未加仓")
            self.db.add_log("warn",
                            f"信号 {sig_id}（{symbol}）马丁加仓第 {layers} 档："
                            f"当前名义价值 {pct_str(current_value)} USDT 已达到/超过目标 "
                            f"{pct_str(target_value)} USDT（{pct_str(base_notional)}U × {target_mult}X），未加仓。")
            return False
        add_qty = floor_to_step(add_value / mark, D(filters["stepSize"]))
        step_size = D(filters["stepSize"])
        min_qty = max(D(filters["minQty"]), step_size)
        min_notional = D(filters.get("minNotional") or "0")
        if add_qty <= 0 or add_qty < min_qty:
            self.db.update_signal(
                sig_id,
                platform_multiplier=str(target_mult),
                note=(f"马丁加仓第 {layers} 档：平台目标 {target_mult}X，"
                      f"需加 {pct_str(add_value)}U 不足 {ex_label} 最小一手，未加仓"))
            self.db.add_log("warn", f"信号 {sig_id}（{symbol}）马丁加仓第 {layers} 档："
                                    f"平台目标 {target_mult}X，需增加价值 {pct_str(add_value)} USDT"
                                    f"（数量 {add_qty}）不足 {ex_label} 最小一手 {min_qty}，未加仓。")
            return False
        if add_qty * mark < min_notional:
            self.db.update_signal(
                sig_id,
                platform_multiplier=str(target_mult),
                note=(f"马丁加仓第 {layers} 档：平台目标 {target_mult}X，"
                      f"需加 {pct_str(add_value)}U 低于最小 {pct_str(min_notional)}U，未加仓"))
            self.db.add_log("warn", f"信号 {sig_id}（{symbol}）马丁加仓第 {layers} 档："
                                    f"平台目标 {target_mult}X，需增加价值 {pct_str(add_value)} USDT"
                                    f"（数量 {add_qty}）低于 {ex_label} 最小 {pct_str(min_notional)}U，未加仓。")
            return False

        is_sim = self._is_sim()
        taker = self._fee_rate(filters)
        open_side = "BUY" if direction == "long" else "SELL"
        client_id = f"sl_{sig_id}_add{layers}"

        # 下逐仓单会被 Bitget 拒绝（错误码 45117“无法调整保证金模式”）。
        margin_mode = "isolated" if is_sim else await self._resolve_margin_mode(
            sig, fallback=self._martingale_margin_mode(sig))
        # 先撤掉上一次还没成交的限价加仓单，避免重复挂单。
        # 以数据库最新值为准：15 秒对账循环可能已经清掉或替换了旧挂单。
        latest = self.db.get_signal(sig_id)
        pending_oid = str((latest or sig).get("pending_add_order_id") or "")
        if pending_oid and not is_sim:
            try:
                canceled = await ex.cancel_order(symbol, pending_oid)
            except Exception:
                canceled = False
            if not canceled:
                # 网络抖动很常见：稍等再撤一次，避免一次接口错误就放弃整档加仓
                await asyncio.sleep(0.6)
                try:
                    canceled = await ex.cancel_order(symbol, pending_oid)
                except Exception:
                    canceled = False
            if not canceled:
                # 撤单失败不能盲目再挂：旧单可能已经成交，再挂就会变成重复加仓
                safe, why = await self._pending_add_replace_safe(sig, pending_oid)
                if not safe:
                    note = (f"马丁加仓第 {layers} 档：旧加仓挂单 {pending_oid} 状态未确认"
                            f"（{why}），为避免重复加仓，本次不挂新单")
                    self.db.update_signal(sig_id, note=note)
                    self.db.add_log("alert", f"【需人工核对】信号 {sig_id}（{symbol}）{note}。")
                    return False
            # 旧单确定不会再成交，清掉本地显示；本次直接市价加仓
            self.db.update_signal(sig_id, pending_add_order_id="", pending_add_qty="",
                                  pending_add_price="", pending_add_ts=0)


        mode_label = "全仓" if margin_mode == "crossed" else "逐仓"
        self.db.add_log("info",
                        f"信号 {sig_id}（{symbol}）马丁加仓第 {layers} 档：平台仓位 {target_mult}X"
                        f" → 目标仓位价值 {pct_str(target_value)} USDT（{pct_str(base_notional)}U × {target_mult}），"
                        f"当前名义价值 {pct_str(current_value)} USDT，增加价值 {pct_str(add_value)} USDT，"
                        f"标记价 {pct_str(mark)}"
                        + ("" if is_sim else f"（{mode_label}）")
                        + f" → 市价加仓 {add_qty}")
        result = await ex.market_open(
            symbol, open_side, add_qty, client_id,
            margin_mode=None if is_sim else margin_mode)
        if result.ok is None:
            self.db.update_signal(sig_id, note=f"马丁加仓第 {layers} 档结果未知：{result.error}")
            self.db.add_log("alert", f"【需人工核对】信号 {sig_id}（{symbol}）马丁加仓第 {layers} 档"
                                     f"结果未知（网络超时），程序不会自动重试：{result.error}")
            return False
        if result.ok is False:
            self.db.update_signal(sig_id, note=f"马丁加仓第 {layers} 档被拒绝：{result.error}")
            self.db.add_log("alert", f"信号 {sig_id}（{symbol}）马丁加仓第 {layers} 档被拒绝：{result.error}")
            return False

        # 加仓成功：更新总数量、自己账户的真实均价、手续费、订单号与层数
        total_qty = old_qty + add_qty
        old_avg = D(sig.get("avg_entry") or sig.get("entry") or "0")
        new_avg = (old_qty * old_avg + add_qty * mark) / total_qty if total_qty > 0 else mark
        open_fee = D(sig.get("open_fee") or "0") + add_qty * mark * taker
        ids = self._open_order_ids(sig)
        if result.order_id:
            ids.append(result.order_id)
        platform_avg = ""
        try:
            if m.get("avg") not in (None, ""):
                platform_avg = pct_str(D(m["avg"]))
        except Exception:
            platform_avg = ""
        self.db.update_signal(
            sig_id, qty=str(total_qty), avg_entry=str(new_avg),
            martin_layers=layers, martin_multiplier=str(target_mult),
            open_fee=money_str(open_fee), open_order_ids=",".join(ids),
            fee_source="pending",
            pending_add_order_id="", pending_add_qty="", pending_add_price="", pending_add_ts=0,
            note=f"已同步马丁加仓第 {layers} 档（目标仓位价值 {money_str(target_value)}U，"
                 f"平台参考均价 {platform_avg or '未提供'}）")
        self.mark_cache[(str(sig.get("exchange") or "bitget"), symbol)] = (time.time(), mark)
        self.db.record_qty_event(sig_id, time.time(), str(total_qty),
                                 f"马丁加仓第 {layers} 档")
        self.db.add_log("info",
                        f"信号 {sig_id}（{symbol}）已同步马丁加仓：第 {layers} 档，"
                        f"数量 +{add_qty}，总数量 {total_qty}，当前仓位价值约 "
                        f"{pct_str(total_qty * mark)} USDT，自己的均价 {pct_str(new_avg)}"
                        f"（平台参考均价 {platform_avg or '-'}）。")
        if not is_sim and result.order_id:
            asyncio.create_task(self._enrich_open_fees(sig_id, symbol, ex))
        return True

    @staticmethod
    def _error_text(e):
        """把交易所异常转成可排查的中文原因（带错误码）。"""
        raw = str(getattr(e, "raw_msg", "") or e or "")
        code = str(getattr(e, "code", "") or "")
        if code:
            return f"{raw}（错误码 {code}）"
        return f"{e.__class__.__name__}：{raw}" if raw else e.__class__.__name__

    async def _pending_add_replace_safe(self, sig, order_id):
        """撤单失败时，结合完整挂单列表与当前仓位判断旧单是否仍可能成交；无法确认时停止替换。"""
        symbol = sig["symbol"]
        ex = self._ex(sig)
        try:
            live = await ex.fetch_open_orders()   # 不带 symbol
        except Exception as e:
            return False, f"无法确认旧挂单状态（挂单列表查询失败：{self._error_text(e)}）"
        if any(str(o.get("orderId") or "") == str(order_id) for o in (live or [])):
            return False, "撤单未成功，旧挂单仍在交易所"

        try:
            actual = Decimal("0")
            for pos in await ex.fetch_positions() or []:
                try:
                    if str(pos.get("symbol") or "") == symbol:
                        actual = D(pos.get("total") or "0")
                        break
                except Exception:
                    continue
        except Exception as e:
            return False, f"无法确认旧挂单状态（仓位查询失败：{self._error_text(e)}）"

        latest = self.db.get_signal(sig["id"]) or sig
        try:
            local = D(latest.get("qty") or "0")
        except Exception:
            local = Decimal("0")
        tol = Decimal("0.0000001")
        if actual > local + tol:
            return False, f"旧挂单疑似已成交（交易所仓位 {actual} > 本地 {local}），等数量对账"
        if actual < local - tol:
            return False, f"交易所仓位 {actual} 小于本地 {local}，需先校准"
        return True, ""

    async def _apply_stop(self, sig, stop_value, context=""):
        """撤销旧止损单，按新的 stop 重新挂全仓止损"""
        symbol = sig["symbol"]
        direction = sig["direction"]
        sig_id = sig["id"]
        ex = self._ex(sig)
        try:
            filters = await ex.fetch_lot_filters(symbol) or dict(DEFAULT_FILTERS)
            stop_price = quantize(stop_value, D(filters["tickSize"]))
        except Exception:
            stop_price = D(stop_value)
        await ex.cancel_plan_orders(symbol)   # 只撤旧止损单，别碰马丁加仓单
        result = await ex.place_stop_close_position(
            symbol, direction, stop_price, f"sl_{sig_id}_sl", qty=sig.get("qty"))
        if result.ok is True:
            self.db.add_log("info", f"信号 {sig_id}（{symbol}）止损保护单已更新：止损价 {stop_price}。{context}")
        else:
            self.db.add_log("alert", f"【需人工处理】信号 {sig_id}（{symbol}）止损单更新失败："
                                     f"{result.error or '结果未知'}。旧止损单已撤。")

    async def _handle_ended(self, msg):
        """信号结束 → 平仓。先撤该交易对所有挂单，再按本地记录的数量市价平掉。"""
        sig_id = str(msg.get("id", ""))
        end_reason = msg.get("end_reason", "")
        end_note = msg.get("end_note", "")
        final_price = msg.get("final_price")
        profit = msg.get("profit_percent")
        text = msg.get("text", "")
        sig = self.db.get_signal(sig_id)
        if sig is None:
            self.db.add_log("info", f"收到结束信号 {sig_id}，但本地没有记录（开仓时可能已跳过），忽略。")
            return
        ex = self._ex(sig)
        ex_label = self._ex_label(sig.get("exchange"))
        if sig["status"] == ST_PENDING:
            oid = str(sig.get("pending_open_order_id") or "")
            cancelled = True
            if oid:
                try:
                    cancelled = await ex.cancel_order(sig["symbol"], oid)
                except Exception:
                    cancelled = False
            await self._sync_pending_entries(skip_verify_ids={sig_id})
            latest = self.db.get_signal(sig_id) or sig
            if latest.get("status") == ST_HOLDING:
                await self._close_position(latest, final_price=final_price, profit=profit,
                                           end_reason=end_reason, end_note=end_note,
                                           log_prefix=f"平台结束（限价单已成交）")
                return
            if not cancelled:
                self.db.update_signal(
                    sig_id, status=ST_UNKNOWN,
                    note="平台结束，但限价开仓撤单失败，可能稍后成交，请人工核对")
                self.db.add_log(
                    "alert",
                    f"【需人工核对】{sig['symbol']} 平台已结束，限价开仓单撤销失败，"
                    f"仓位可能稍后成交，请到 {ex_label} 核对。")
                return
            self.db.update_signal(
                sig_id, status=ST_SKIPPED, skip_reason="挂单未成交",
                note="平台结束信号到达，限价开仓单未成交，已撤销")
            self.db.add_log(
                "info",
                f"信号 {sig_id}（{sig['symbol']}）平台结束，限价开仓单未成交，已撤销，不再开仓。")
            return
        if sig["status"] not in (ST_HOLDING, ST_UNKNOWN):
            # 影子跟踪：平台结束了这个信号，记录平台的结果（我们此前已平仓）
            if sig["status"] == ST_CLOSED and str(sig.get("platform_tracking") or "") == "1":
                self.db.update_signal(
                    sig_id,
                    platform_tracking="0",
                    platform_end_reason=str(end_reason or ""),
                    platform_end_text=str(text or ""),
                    platform_end_time=now_str(),
                    platform_final_price=str(final_price or ""),
                    platform_profit_percent=str(profit or ""))
                self.db.add_log("info",
                                f"平台已结束信号 {sig_id}（{sig['symbol']}）：{text or end_reason or '已结束'}"
                                f"（我们此前已平仓，仅记录平台结果）。")
            else:
                self.db.add_log("info", f"信号 {sig_id} 已处于「{sig['status']}」，忽略重复的结束信号。")
            return
        await self._close_position(sig, final_price=final_price, profit=profit,
                                   end_reason=end_reason, end_note=end_note,
                                   log_prefix=f"收到结束信号（{text or end_reason}）")

    async def _handle_receipt(self, msg):
        """回执对账：平台回执经常只给部分 id（实测 5/37 不稳定），11-12 开仓的持仓长期
        不在回执里但马丁和结束推送仍正常；因此回执只用于撤销未成交挂单和结束影子跟踪，
        不据此平仓，也不再对“回执未包含”的持仓告警。"""
        ids = set(str(i) for i in (msg.get("ids") or []))
        self.db.add_log("info", f"收到信号回执：服务器仍在跟踪 {len(ids)} 个信号。")
        conn_time = self.conn_established_at
        # 挂单中的限价开仓：平台回执不再跟踪时撤销，避免平台结束后还成交
        for sig in self.db.pending_entries():
            if sig["id"] in ids:
                continue
            if sig.get("open_ts") and sig["open_ts"] >= conn_time:
                continue
            oid = str(sig.get("pending_open_order_id") or "")
            if oid:
                try:
                    await self._ex(sig).cancel_order(sig["symbol"], oid)
                except Exception:
                    pass
            self.db.update_signal(
                sig["id"], status=ST_SKIPPED, skip_reason="回执未跟踪",
                note="平台回执已不再跟踪，限价开仓单已撤销")
            self.db.add_log(
                "info",
                f"信号 {sig['id']}（{sig['symbol']}）平台回执已不再跟踪，"
                f"限价开仓挂单已撤销，不再开仓。")
        # 影子跟踪：平台回执不再列出我们已平仓的信号，说明平台已经结束它
        for sig in self.db.platform_shadows(300):
            if sig["id"] in ids:
                continue
            self.db.update_signal(sig["id"], platform_tracking="0",
                                  platform_end_reason="平台已不再跟踪（回执对账）",
                                  platform_end_time=now_str())
            self.db.add_log("info",
                            f"平台已停止跟踪信号 {sig['id']}（{sig['symbol']}）（回执对账）。")

        # 设置标记由 follow_floating_loss_once 写入，取价失败会重试且不标记完成。
        if (self.running
                and not self._is_sim()
                and self.db.get_setting("follow_all_floating_loss_20261108", "0") != "1"
                and (self._backfill_task is None or self._backfill_task.done())):
            self._backfill_task = asyncio.create_task(self._follow_floating_loss_safe())

    async def _close_position(self, sig, final_price, profit, end_reason, end_note, log_prefix):
        """同一信号的平仓串行化：平台结束、回执对账、手动平仓等并发触发时，
        只允许第一个真正下单；结果未知的仓位本次运行不再重复下单，避免平仓变反向开仓。"""
        sig_id = str(sig.get("id") or "")
        if not sig_id:
            return
        lock = self._close_locks.setdefault(sig_id, asyncio.Lock())
        async with lock:
            if sig_id in self._close_attempted:
                return
            latest = self.db.get_signal(sig_id)
            if latest is not None and latest.get("status") == ST_CLOSED:
                return
            self._close_attempted.add(sig_id)
            await self._close_position_locked(
                latest or sig, final_price, profit, end_reason, end_note, log_prefix)
            after = self.db.get_signal(sig_id)
            if after is not None and after.get("status") == ST_HOLDING:
                # 明确失败（仓位仍在），允许之后的结束信号/对账再试一次
                self._close_attempted.discard(sig_id)

    async def _close_position_locked(self, sig, final_price, profit, end_reason, end_note, log_prefix):
        """平仓公共流程：撤单 → 市价平仓 → 计算手续费/毛盈亏/净盈亏 → 更新记录 → 检查熔断。
        不依赖「止损单是否已成交」来判断，统一按本地记录的数量平。"""
        symbol = sig["symbol"]
        direction = sig["direction"]
        close_side = "SELL" if direction == "long" else "BUY"
        is_sim = self._is_sim()
        ex = self._ex(sig)
        ex_label = self._ex_label(sig.get("exchange"))

        # 平仓前把已到结算时间的资金费结清（并重新读取最新持仓数据）
        try:
            await self.accrue_funding(sig_id=sig["id"])
        except Exception:
            pass
        sig = self.db.get_signal(sig["id"]) or sig
        qty = sig["qty"]

        self.db.add_log("info", f"{log_prefix} → 平仓 {symbol}（{ex_label}）：先撤销所有挂单，再市价平掉 {qty} 个。")
        await ex.cancel_all_orders(symbol)

        # 平台结束可能刚好撞上马丁限价加仓成交，而 15 秒数量对账还没跑。
        # 先按交易所实际仓位校准（只读成交明细，不下新单），避免只平掉本地旧数量后留下残余仓位。
        if not is_sim and str(sig.get("pending_add_order_id") or ""):
            try:
                positions = await ex.fetch_positions()
                ex_pos = None
                for item in positions or []:
                    try:
                        if (str(item.get("symbol") or "") == symbol
                                and D(item.get("total") or "0") != 0):
                            ex_pos = item
                            break
                    except Exception:
                        continue
                if ex_pos is not None:
                    await self._reconcile_position_mismatch(sig, ex_pos)
                    latest_sig = self.db.get_signal(sig["id"]) or sig
                    new_qty = latest_sig.get("qty") or qty
                    if str(new_qty) != str(qty):
                        self.db.add_log(
                            "warn",
                            f"{symbol} 平仓前发现交易所实际数量 {new_qty} 与本地 {qty} 不一致，"
                            f"已按实际仓位校准后再平仓。")
                    sig = latest_sig
                    qty = new_qty
            except Exception as e:
                self.db.add_log("warn", f"{symbol} 平仓前核对交易所数量失败（{e}），按本地数量继续。")

        if not qty:
            # 没有记录数量（异常情况）：保守处理，只撤单，不猜数量乱平
            self.db.update_signal(sig["id"], status=ST_CLOSED, close_time=now_str(), close_ts=time.time(),
                                  final_price=str(final_price or ""), profit_percent=str(profit or ""),
                                  end_reason=end_reason, end_note=end_note,
                                  note="本地无数量记录，仅撤销挂单，未平仓（请人工核对）")
            self.db.add_log("alert", f"【需人工核对】信号 {sig['id']}（{symbol}）本地没有数量记录，未自动平仓。")
            return

        result = await ex.market_close(symbol, close_side, qty, f"sl_{sig['id']}_close")
        if result.ok is True:
            if result.error:   # 例如“交易所返回无持仓”，这是正常情况
                self.db.add_log("info", f"信号 {sig['id']}（{symbol}）平仓时交易所返回：{result.error}（正常情况，可能已被止损单平掉）。")

            # ---- 计算手续费与盈亏 ----
            close_price = None
            try:
                if final_price not in (None, ""):
                    close_price = D(final_price)
            except Exception:
                close_price = None
            if close_price is None:
                try:
                    m = await ex.fetch_mark_price(symbol)
                except Exception:
                    m = None
                close_price = m if m is not None else D(sig["entry"] or "0")

            try:
                filters = await ex.fetch_lot_filters(symbol) or dict(DEFAULT_FILTERS)
            except Exception:
                filters = dict(DEFAULT_FILTERS)
            taker = self._fee_rate(filters)

            try:
                avg_entry = D(sig.get("avg_entry") or sig.get("entry") or "0")
            except Exception:
                avg_entry = Decimal("0")
            sign = Decimal("1") if direction == "long" else Decimal("-1")
            # 毛盈亏：有结算价时按「数量 × (平仓价 − 均价)」计算，马丁加仓后的真实仓位也能算对；
            # 没有结算价（如回执对账）时，若服务器给了收益%就按 名义价值 × 收益% 计，否则按 0 计。
            gross = Decimal("0")
            if final_price not in (None, ""):
                try:
                    gross = D(qty) * (close_price - avg_entry) * sign
                except Exception:
                    gross = Decimal("0")
            elif profit not in (None, ""):
                try:
                    gross = D(sig.get("notional") or "0") * D(profit) / Decimal("100")
                except Exception:
                    gross = Decimal("0")

            try:
                open_fee = D(sig["open_fee"] or "0") if sig.get("open_fee") not in (None, "") else Decimal("0")
            except Exception:
                open_fee = Decimal("0")
            try:
                funding_fee = D(sig.get("funding_fee") or "0")
            except Exception:
                funding_fee = Decimal("0")
            close_fee = D(qty) * close_price * taker
            net = gross - open_fee - close_fee - funding_fee

            if is_sim:
                fee_note = f"模拟盘手续费（按 taker 费率 {pct_str(taker * 100)}% 估算）"
            else:
                fee_note = "手续费暂为估算，正在核对交易所实际值..."
            layers = int(sig.get("martin_layers") or 0)
            close_note = f"已平仓（{log_prefix}）"
            if layers > 0:
                close_note += f"，马丁加仓 {layers} 层"

            self.db.update_signal(
                sig["id"], status=ST_CLOSED, close_time=now_str(), close_ts=time.time(),
                final_price=str(final_price if final_price not in (None, "") else ""),
                profit_percent=str(profit if profit not in (None, "") else ""),
                end_reason=end_reason, end_note=end_note,
                close_order_id=result.order_id or "",
                open_fee=money_str(open_fee), close_fee=money_str(close_fee),
                gross_pnl=money_str(gross), net_pnl=money_str(net),
                fee_note=fee_note,
                note=close_note)
            pstr = f"{profit}%" if profit not in (None, "") else "无收益数据"
            self.db.add_log("info",
                            f"信号 {sig['id']}（{symbol}）平仓完成，收益 {pstr}，"
                            f"手续费 {money_str(open_fee + close_fee)} USDT（{'估算' if is_sim else '待核对'}），"
                            f"资金费 {money_str(funding_fee)} USDT（正数=支付、负数=收取），"
                            f"净盈亏 {money_str(net)} USDT{'，马丁加仓 ' + str(layers) + ' 层' if layers > 0 else ''}。")

            # 实盘：后台核对交易所实际手续费与实际平仓盈亏
            if not is_sim:
                asyncio.create_task(self._enrich_close_fees(sig["id"], symbol, ex))
        elif result.ok is None:
            self.db.update_signal(sig["id"], status=ST_UNKNOWN,
                                  note=f"平仓结果未知（网络超时）：{result.error}，请人工核对")
            self.db.add_log("alert", f"【需人工核对】信号 {sig['id']}（{symbol}）平仓结果未知（网络超时），程序不会自动重试：{result.error}")
            return
        else:
            self.db.update_signal(sig["id"], note=f"平仓失败：{result.error}，仓位仍在，请人工处理")
            self.db.add_log("alert", f"【需人工处理】信号 {sig['id']}（{symbol}）平仓失败：{result.error}")
            return

        # 平仓后检查熔断线
        if self._is_fused():
            self.db.add_log("alert", "【熔断】当日累计亏损已触及熔断线（账户总资金 × 3%），将暂停开新仓，直到明天自动解除。")

    # ==================== 实盘手续费回填 ====================

    @staticmethod
    def _open_order_ids(sig):
        raw = sig.get("open_order_ids") or sig.get("open_order_id") or ""
        return [x.strip() for x in str(raw).split(",") if x.strip()]

    @staticmethod
    def _fill_accounting_fields(sig, close_fills, open_fills):
        """按交易所成交明细生成回填字段。
        平仓均价 = 平仓成交的加权平均价（不是信号平台给的 final_price）；
        开仓均价 = 开仓/加仓成交的加权平均价；
        手续费、毛盈亏、净盈亏都用交易所实际值。
        成交明细没有价格/数量时（例如测试假数据），不覆盖原有价格字段。"""
        if any(str(f.get("fee_coin") or "USDT").upper() != "USDT" or f.get("fee_known") is False
               for f in close_fills + open_fills):
            raise ValueError("实际手续费缺失或币种不是 USDT，不能冒充准确费用")
        close_fee = sum((f.get("fee") or Decimal("0") for f in close_fills), Decimal("0"))
        gross = sum((f.get("profit") or Decimal("0") for f in close_fills), Decimal("0"))
        close_vol = sum((D(f.get("volume") or "0") for f in close_fills), Decimal("0"))
        close_cost = sum((D(f.get("price") or "0") * D(f.get("volume") or "0")
                          for f in close_fills), Decimal("0"))
        close_price = (close_cost / close_vol) if close_vol > 0 and close_cost > 0 else None
        fields = {"close_fee": amount_str(close_fee)}
        if close_price is not None:
            fields["final_price"] = str(close_price)

        avg_entry = D(sig.get("avg_entry") or sig.get("entry") or "0")
        open_fee = D(sig.get("open_fee") or "0")
        if open_fills:
            open_vol = sum((D(f.get("volume") or "0") for f in open_fills), Decimal("0"))
            open_cost = sum((D(f.get("price") or "0") * D(f.get("volume") or "0")
                             for f in open_fills), Decimal("0"))
            if open_vol > 0 and open_cost > 0:
                avg_entry = open_cost / open_vol
                fields["avg_entry"] = str(avg_entry)
            open_fee = sum((f.get("fee") or Decimal("0") for f in open_fills), Decimal("0"))
            fields["open_fee"] = amount_str(open_fee)

        qty = D(sig.get("qty") or "0")
        # Gate 没有逐笔已实现盈亏；用本次平仓成交量算，避免部分平仓时把整仓盈亏算进来
        calc_vol = close_vol if close_vol > 0 else qty
        if (gross == 0 and not any(f.get("profit_known") is True for f in close_fills)
                and close_price is not None and calc_vol > 0 and avg_entry > 0):
            sign = Decimal("1") if sig.get("direction") == "long" else Decimal("-1")
            gross = calc_vol * (close_price - avg_entry) * sign

        funding = D(sig.get("funding_fee") or "0")
        net = gross - open_fee - close_fee - funding
        fields["gross_pnl"] = amount_str(gross)
        fields["net_pnl"] = amount_str(net)

        coins = {f.get("fee_coin", "USDT") for f in close_fills + open_fills}
        note = "交易所实际手续费"
        if coins - {"USDT", "usdt"}:
            note += f"（含 {'/'.join(sorted(coins))} 抵扣）"
        if not open_fills:
            note += "；开仓手续费沿用原记录"
        if close_price is not None:
            note += "；平仓均价按成交明细校准"
        fields["fee_note"] = note
        return fields

    async def _reconcile_recent_closed(self, limit=100):
        """启动时用交易所成交明细校准最近已平仓记录（只读交易所，不改仓位）。
        按整段持仓时间取成交，能覆盖多次部分平仓；成交数量与记录数量对不上时不改，
        避免把别的仓位成交算进来。"""
        if self._is_sim():
            return
        try:
            rows = self.db.closed_history(limit)
        except Exception:
            return
        fixed = 0
        for sig in rows:
            if sig.get("record_source") == "exchange_recovered":
                continue  # 实际历史接口的原值不再被另一套汇总结果覆盖。
            try:
                ex = self._ex(sig)
                if ex is None:
                    continue
                qty = D(sig.get("qty") or "0")
                open_ts = float(sig.get("open_ts") or 0)
                close_ts = float(sig.get("close_ts") or 0)
                if qty <= 0 or close_ts <= 0:
                    continue
                start_ts = min(open_ts, close_ts) - 60 if open_ts else close_ts - 86400
                fills = await ex.get_symbol_fills(
                    sig["symbol"], start_ts, close_ts + 5)
                if not fills:
                    continue
                want_close = "buy" if sig.get("direction") == "short" else "sell"
                close_fills = [
                    f for f in fills
                    if str(f.get("side") or "").lower() == want_close
                    and float(f.get("ts") or 0) >= start_ts
                    and float(f.get("ts") or 0) <= close_ts + 5
                ]
                if not close_fills:
                    continue
                close_vol = sum((D(f.get("volume") or "0") for f in close_fills),
                                Decimal("0"))
                tolerance = max(qty * Decimal("0.02"), Decimal("0.0000001"))
                if abs(close_vol - qty) > tolerance:
                    continue   # 成交数量对不上，宁可不动，交给人工核对
                open_ids = set(self._open_order_ids(sig))
                open_fills = [
                    f for f in fills
                    if str(f.get("order_id") or "") in open_ids
                ] if open_ids else []
                # 部分平仓已经分摊过开仓费，不能再用整笔开仓订单覆盖。
                if "_part" in sig["id"] or sig.get("accounting_parent"):
                    open_fills = []
                try:
                    self.db.record_fee_events(close_fills + open_fills, sig["id"])
                except Exception:
                    pass
                fields = self._fill_accounting_fields(sig, close_fills, open_fills)
                old_final = str(sig.get("final_price") or "")
                self.db.update_signal(sig["id"], **fields)
                if fields.get("final_price") and fields["final_price"] != old_final:
                    fixed += 1
            except Exception:
                continue
        if fixed:
            self.db.add_log("info", f"已用交易所成交明细校准 {fixed} 笔历史平仓记录（平仓均价/手续费/盈亏）。")

    def disable_rate_research_once(self):
        """兼容配置迁移：停止接收指定费率来源的新信号，已有记录仍管理至结束。"""
        key = "disable_source_rate_research_v1"
        if self.db.get_setting(key, "0") == "1":
            return
        p = self.params.all()
        current = list(p.get("sources") or [])
        if current:
            new = [s for s in current if s != "费率信号"]
        else:
            # 原本是“全部来源”：先改成明确的允许列表（不含费率信号）；
            # 资金流信号会在 enable_capital_flow_source_once 里重新加入。
            new = [s for s in ALL_SOURCES if s not in ("费率信号", "资金流信号")]
        if new != current:
            ok, errs = self.params.save({"sources": ",".join(new)})
            if ok:
                self.db.add_log(
                    "info",
                    "已应用兼容配置迁移：停止接收「费率信号」新信号；"
                    "已有费率信号仓位继续按平台消息管理，直到平台结束。")
            else:
                self.db.add_log(
                    "alert",
                    f"停止「费率信号」来源失败：{'；'.join(errs) or '未知错误'}")
        self.db.set_setting(key, "1")

    def enable_capital_flow_source_once(self):
        """兼容配置迁移：将资金流来源加入来源筛选。"""
        key = "enable_source_capital_flow_v1"
        if self.db.get_setting(key, "0") == "1":
            return
        p = self.params.all()
        current = list(p.get("sources") or [])
        if current and "资金流信号" not in current:
            new = current + ["资金流信号"]
            ok, errs = self.params.save({"sources": ",".join(new)})
            if ok:
                self.db.add_log(
                    "info",
                    "已把「资金流信号」加入来源筛选：以后与其它来源一样正常跟单开仓，"
                    "并跟随平台马丁加仓。")
            else:
                self.db.add_log(
                    "alert",
                    f"启用「资金流信号」来源失败：{'；'.join(errs) or '未知错误'}")
                return
        self.db.set_setting(key, "1")

    def disable_rate_research_v2_once(self):
        """兼容配置迁移：更新来源筛选标记，已有记录不受影响。"""
        key = "disable_source_rate_research_v2"
        if self.db.get_setting(key, "0") == "1":
            return
        p = self.params.all()
        current = list(p.get("sources") or [])
        if current:
            new = [s for s in current if s != "费率信号"]
        else:
            # 原本是“全部来源”：改成明确的允许列表（不含费率信号）。
            new = [s for s in ALL_SOURCES if s != "费率信号"]
        if new != current:
            ok, errs = self.params.save({"sources": ",".join(new)})
            if not ok:
                self.db.add_log(
                    "alert",
                    f"关闭「费率信号」来源失败：{'；'.join(errs) or '未知错误'}")
                return
            self.db.add_log(
                "info", "已应用兼容配置迁移：关闭「费率信号」来源：除它以外的信号都跟随。")
        self.db.set_setting(key, "1")

    def enable_capital_flow_source_once(self):
        """兼容配置迁移：将资金流来源加入来源筛选。"""
        key = "enable_source_capital_flow_v1"
        if self.db.get_setting(key, "0") == "1":
            return
        p = self.params.all()
        current = list(p.get("sources") or [])
        if current and "资金流信号" not in current:
            new = current + ["资金流信号"]
            ok, errs = self.params.save({"sources": ",".join(new)})
            if ok:
                self.db.add_log(
                    "info",
                    "已把「资金流信号」加入来源筛选：以后与其它来源一样正常跟单开仓，"
                    "并跟随平台马丁加仓。")
            else:
                self.db.add_log(
                    "alert",
                    f"启用「资金流信号」来源失败：{'；'.join(errs) or '未知错误'}")
                return
        self.db.set_setting(key, "1")

    def enable_both_directions_once(self):
        """兼容配置迁移：恢复双向来源筛选，解除旧的仅做空设置。"""
        key = "both_directions_v2"
        if self.db.get_setting(key, "0") == "1":
            return
        p = self.params.all()
        if list(p.get("directions") or []):
            ok, errs = self.params.save({"directions": ""})
            if not ok:
                self.db.add_log(
                    "alert",
                    f"恢复做多/做空都跟随失败：{'；'.join(errs) or '未知错误'}")
                return
            self.db.add_log(
                "info",
                "已应用兼容配置迁移：恢复「做多、做空都跟随」；旧的只做空限制已解除。")
        self.db.set_setting("short_only_direction_v1", "0")
        self.db.set_setting(key, "1")

    def set_notional_30_once(self):
        """兼容配置迁移：调整以后开仓使用的基础名义价值，已有仓位保留各自记录。"""
        key = "set_notional_30_v1"
        if self.db.get_setting(key, "0") == "1":
            return
        p = self.params.all()
        try:
            cur = D(p.get("notional") or "0")
        except Exception:
            cur = Decimal("0")
        if cur != D("30"):
            ok, errs = self.params.save({"notional": "30"})
            if not ok:
                self.db.add_log(
                    "alert",
                    f"每信号名义价值改为 30U 失败：{'；'.join(errs) or '未知错误'}")
                return
            self.db.add_log(
                "info",
                f"已应用兼容配置迁移：把每信号名义价值从 {cur}U 改为 30U；"
                f"以后新开仓按 30U（逆势 0.5X = 15U，普通 1.0X = 30U），已有仓位不受影响。")
        self.db.set_setting(key, "1")

    def disable_position_limits_once(self):
        """兼容配置迁移：将开仓数量限制设为零；迁移标记完成后不再覆盖后续配置。"""
        key = "unlimited_position_limits_v1"
        if self.db.get_setting(key, "0") == "1":
            return
        p = self.params.all()
        try:
            cur_pos = int(p.get("max_positions") or 0)
            cur_day = int(p.get("max_daily_opens") or 0)
        except Exception:
            cur_pos, cur_day = 0, 0
        payload = {}
        if cur_pos != 0:
            payload["max_positions"] = "0"
        if cur_day != 0:
            payload["max_daily_opens"] = "0"
        if payload:
            ok, errs = self.params.save(payload)
            if not ok:
                self.db.add_log(
                    "alert",
                    f"取消「最大同时持仓数 / 每日开仓次数」限制失败：{'；'.join(errs) or '未知错误'}")
                return
            self.db.add_log(
                "info",
                f"已应用兼容配置迁移：取消数量限制：最大同时持仓数 {cur_pos} → 不限制，"
                f"每日开仓次数 {cur_day} → 不限制。"
                f"（只影响开仓数量门槛，不改名义价值/杠杆/保证金模式；"
                f"可用保证金不足时 Bitget 仍会拒绝开仓）")
        self.db.set_setting(key, "1")

    def set_margin_mode_crossed_once(self):
        """兼容配置迁移：调整新开仓的保证金模式。"""
        key = "margin_mode_crossed_v1"
        if self.db.get_setting(key, "0") == "1":
            return
        p = self.params.all()
        if p.get("margin_mode") != "crossed":
            ok, errs = self.params.save({"margin_mode": "crossed"})
            if not ok:
                self.db.add_log(
                    "alert",
                    f"新开仓改为全仓失败：{'；'.join(errs) or '未知错误'}")
                return
            self.db.add_log(
                "info", "已应用兼容配置迁移：把新开仓保证金模式改为「全仓」；"
                        "马丁加仓仍沿用各仓位在交易所的实际模式。")
        self.db.set_setting(key, "1")

    async def cancel_pending_adds_once(self, reason=""):
        """兼容配置迁移：只处理标记前存在的加仓挂单，不重复取消后续新订单。"""
        if self._is_sim():
            return
        key = "cancel_pending_adds_update73"
        if self.db.get_setting(key, "0") == "1":
            return
        pending = [p for p in self.db.open_positions()
                   if str(p.get("pending_add_order_id") or "")]
        canceled = failed = 0
        for sig in pending:
            oid = str(sig.get("pending_add_order_id") or "")
            ex = self._ex(sig)
            ok = False
            for attempt in range(2):
                try:
                    ok = await asyncio.wait_for(
                        ex.cancel_order(sig["symbol"], oid), timeout=6)
                except Exception:
                    ok = False
                if ok:
                    break
                if attempt == 0:
                    await asyncio.sleep(0.6)
            latest = self.db.get_signal(sig["id"]) or sig
            if ok and str(latest.get("pending_add_order_id") or "") == oid:
                self.db.update_signal(
                    sig["id"], pending_add_order_id="", pending_add_qty="",
                    pending_add_price="", pending_add_ts=0,
                    note="按兼容配置取消旧加仓挂单，后续马丁档补足")
                canceled += 1
                self.db.add_log(
                    "info",
                    f"{sig['symbol']} 旧加仓挂单 {oid} 已应用兼容配置迁移：取消，等后续马丁档补足。")
            else:
                failed += 1
                self.db.add_log(
                    "warn",
                    f"{sig['symbol']} 旧加仓挂单 {oid} 取消失败或已成交，"
                    f"保留本地记录交给数量对账。")
        self.db.set_setting(key, "1")
        if pending:
            self.db.add_log(
                "info",
                f"旧加仓挂单清理完成（{reason or '用户要求'}）："
                f"取消 {canceled} 个，失败/已成交 {failed} 个。")

    async def cancel_local_stops(self, reason=""):
        """撤销当前持仓的本地挂单（关闭本地止损/保存参数时用），只保留平台信号退出。"""
        try:
            self._ensure_exchange()
        except Exception:
            return
        if self._is_sim():
            return
        checked = 0
        for sig in self.db.open_positions():
            symbol = sig["symbol"]
            ex = self._ex(sig)
            try:
                # 只撤计划委托（止损/止盈），**不能撤普通限价单**：

                await ex.cancel_plan_orders(symbol)
                checked += 1
            except Exception as e:
                self.db.add_log("alert", f"撤销 {symbol} 本地挂单失败：{e}")
        if checked:
            self.db.add_log(
                "info",
                f"本地挂单检查：已检查 {checked} 个持仓的挂单（{reason or '本地止损关闭'}），"
                f"仓位不受影响，只等平台结束信号。")

    async def apply_margin_leverage(self, leverage, margin_mode=None):
        """尝试把当前所有持仓设为指定保证金模式并改杠杆；只动账户设置，不平仓、不重试。
        Bitget 可能因持仓/挂单拒绝切换模式，失败只记录，不影响仓位。"""
        self._ensure_exchange()
        if self._is_sim():
            return {"ok": False, "error": "模拟盘不适用"}
        try:
            lev = int(leverage)
        except Exception:
            return {"ok": False, "error": "杠杆必须是整数"}
        if lev < 1 or lev > 125:
            return {"ok": False, "error": "杠杆必须在 1-125 之间"}
        target_mode = margin_mode or self.params.all().get("margin_mode") or "crossed"
        target_mode = "isolated" if target_mode == "isolated" else "crossed"
        mode_label = "全仓" if target_mode == "crossed" else "逐仓"
        pos_map = await self._positions_by_exchange()
        mode_map = {k: str(v.get("marginMode") or "") for k, v in pos_map.items()}
        results = []
        for sig in self.db.open_positions():
            symbol = sig["symbol"]
            ex_name = str(sig.get("exchange") or "bitget")
            ex = self._ex(sig)
            if ex is None:
                continue
            mode_before = mode_map.get((ex_name, symbol), "")
            item = {"symbol": symbol, "exchange": ex_name,
                    "margin_ok": mode_before == target_mode,
                    "leverage_ok": False, "mode_before": mode_before, "error": ""}
            try:
                await ex.cancel_all_orders(symbol)
            except Exception as e:
                item["error"] = f"撤挂单失败：{e}"
            if not item["margin_ok"]:
                try:
                    mm_ok, mm_err = await ex.set_margin_mode(symbol, target_mode)
                    item["margin_ok"] = bool(mm_ok)
                    if not mm_ok:
                        item["error"] = mm_err or f"{mode_label}设置失败（交易所可能不允许持仓中切换）"
                except Exception as e:
                    item["error"] = f"{mode_label}设置异常：{e}"
            try:
                hold_side = sig["direction"] if item["margin_ok"] else None
                item["leverage_ok"] = bool(
                    await ex.set_leverage(symbol, lev, hold_side))
                if not item["leverage_ok"]:
                    item["error"] = (item["error"] + "；" if item["error"] else "") + \
                                    f"{lev}倍杠杆设置失败"
            except Exception as e:
                item["leverage_ok"] = False
                item["error"] = (item["error"] + "；" if item["error"] else "") + \
                                f"杠杆设置异常：{e}"
            results.append(item)

        actual_map = await self._positions_by_exchange()
        for item in results:
            pos = actual_map.get((item["exchange"], item["symbol"]))
            if not pos:
                continue
            item["actual_mode"] = str(pos.get("marginMode") or "")
            try:
                item["actual_leverage"] = str(int(D(pos.get("leverage") or "0")))
            except Exception:
                item["actual_leverage"] = str(pos.get("leverage") or "")
            if item["actual_mode"] and item["actual_mode"] != target_mode:
                item["margin_ok"] = False
                item["error"] = (item["error"] + "；" if item["error"] else "") + \
                                f"交易所实际仍为 {item['actual_mode']}"
            elif item["actual_mode"] == target_mode:
                item["margin_ok"] = True
            if item["actual_leverage"] and item["actual_leverage"] != str(lev):
                item["leverage_ok"] = False
                item["error"] = (item["error"] + "；" if item["error"] else "") + \
                                f"交易所实际仍为 {item['actual_leverage']} 倍"
            elif item["actual_leverage"] == str(lev):
                item["leverage_ok"] = True

        for item in results:
            symbol = item["symbol"]
            label = self._ex_label(item.get("exchange"))
            if item["margin_ok"] and item["leverage_ok"]:
                self.db.add_log("info", f"{label} {symbol} 已设为{mode_label} {lev} 倍。")
            else:
                self.db.add_log(
                    "alert",
                    f"{label} {symbol} 未能完整设为{mode_label} {lev} 倍：{item['error'] or '未知原因'}；仓位未动。")
        mode_bad = sum(1 for x in results if not x["margin_ok"])
        lev_bad = sum(1 for x in results if not x["leverage_ok"])
        self.db.add_log(
            "info",
            f"批量调整完成：共 {len(results)} 个持仓，{mode_label}失败 {mode_bad} 个，"
            f"{lev}倍杠杆失败 {lev_bad} 个；程序没有自动平仓。")
        return {"ok": True, "leverage": lev, "margin_mode": target_mode,
                "mode_label": mode_label, "results": results,
                "mode_bad": mode_bad, "leverage_bad": lev_bad}

    @staticmethod
    def _parse_tracked_from_jsonl(tail=50000, default_mult="1"):
        """从 signals.jsonl 重建「平台仍在跟踪」的信号状态：
        active 建立、trailed 更新止盈止损/马丁、ended 删除、receipt 以最新回执为准。
        default_mult：还没收到马丁消息的信号，平台当前倍数按头仓倍数算（默认 1）。"""
        tracked = {}
        try:
            with open(JSONL_PATH, "r", encoding="utf-8") as f:
                lines = f.readlines()[-tail:]
        except Exception:
            return tracked
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except Exception:
                continue
            mtype = str(msg.get("type") or "")
            sid = str(msg.get("id") or "")
            if mtype == "signal.active":
                if not sid:
                    continue
                head = str(msg.get("first_qty") or default_mult)
                tracked[sid] = {
                    "id": sid, "symbol": str(msg.get("symbol") or ""),
                    "direction": str(msg.get("direction") or ""),
                    "source": str(msg.get("source") or ""),
                    "entry": msg.get("entry"), "target": msg.get("target"),
                    "stop": msg.get("stop"), "mult": head, "head": head,
                    "grades": [], "avg": "",
                }
            elif mtype == "signal.trailed":
                if sid not in tracked:
                    continue
                item = tracked[sid]
                if msg.get("target") not in (None, ""):
                    item["target"] = msg.get("target")
                if msg.get("stop") not in (None, ""):
                    item["stop"] = msg.get("stop")
                m = msg.get("martingale")
                if isinstance(m, dict):
                    if m.get("qty") not in (None, ""):
                        item["mult"] = str(m.get("qty"))
                    if isinstance(m.get("grades"), list):
                        item["grades"] = [int(x) for x in m["grades"]
                                          if isinstance(x, (int, float))]
                    if m.get("avg") not in (None, ""):
                        item["avg"] = str(m.get("avg"))
            elif mtype == "signal.ended":
                tracked.pop(sid, None)
            elif mtype == "signal.receipt":
                ids = set(str(i) for i in (msg.get("ids") or []))
                tracked = {k: v for k, v in tracked.items() if k in ids}
        return tracked

    def catchup_preview(self, source=None):
        """只读预览：平台仍跟踪、但本地未持有的信号（用于后入/补开）。
        source=None 表示除「费率信号」外所有来源（仍按参数里的来源/方向筛选）。"""
        tracked = self._parse_tracked_from_jsonl(
            default_mult=str(self.params.all().get("head_multiplier") or "1"))
        open_symbols = {p.get("symbol")
                        for p in (self.db.open_positions() + self.db.pending_entries())}
        items = []
        for sid, m in tracked.items():
            src = str(m.get("source") or "")
            direction = str(m.get("direction") or "")
            if source is None:
                if src == "费率信号":
                    continue
                if not self._passes_filter(src, direction):
                    continue
            elif src != source:
                continue
            sig = self.db.get_signal(sid)
            held = bool(sig and sig.get("status") in (ST_HOLDING, ST_UNKNOWN, ST_PENDING))
            closed_before = self.db.closed_base_exists(sid)
            same_symbol = bool(m.get("symbol") and m.get("symbol") in open_symbols)
            to_open = not held and not same_symbol
            items.append({
                "id": sid, "symbol": m.get("symbol"), "direction": direction,
                "source": src,
                "entry": str(m.get("entry") if m.get("entry") is not None else ""),
                "stop": str(m.get("stop") if m.get("stop") is not None else ""),
                "target": str(m.get("target") if m.get("target") is not None else ""),
                "mult": str(m.get("mult") or "1"), "head": str(m.get("head") or "1"),
                "avg": str(m.get("avg") or ""),
                "grades": list(m.get("grades") or []),
                "held": held, "closed_before": closed_before,
                "same_symbol": same_symbol, "to_open": to_open,
            })
        items.sort(key=lambda x: x["symbol"])
        return {"ok": True, "source": source, "items": items,
                "to_open_count": sum(1 for i in items if i["to_open"])}

    async def open_catchup_signals(self, source=None, ids=None, reason="",
                                   follow_martingale=True, scale_by_mult=False):
        """补开/后入：把平台仍跟踪、本地未持有的信号按市价开仓。
        scale_by_mult=True 时按平台当前累计倍数开够（基础名义价值 × 倍数，当前 30U），
        用于“网站几层就开几层”。
        只开本地和交易所都未持有的；已平仓的旧记录会先归档保留，再以原平台 id 重新开。"""
        self._ensure_exchange()
        if self._is_sim():
            return {"ok": False, "error": "模拟盘不适用"}
        p = self.params.all()
        base_notional = D(p["notional"])
        target_mode = "crossed" if p.get("margin_mode") == "crossed" else "isolated"
        mode_label = "全仓" if target_mode == "crossed" else "逐仓"
        preview = self.catchup_preview(source)
        to_open = [i for i in preview["items"] if i["to_open"]]
        if ids is not None:
            wanted = set(str(x) for x in ids)
            to_open = [i for i in to_open if str(i["id"]) in wanted]
        results = []
        open_symbols = {pos.get("symbol")
                        for pos in (self.db.open_positions() + self.db.pending_entries())}

        for (_ex_name, symbol) in (await self._positions_by_exchange()).keys():
            if symbol:
                open_symbols.add(symbol)
        for it in to_open:
            symbol = it["symbol"]
            sid = it["id"]
            if symbol in open_symbols:
                results.append({"symbol": symbol, "ok": False, "error": "同交易对已有持仓"})
                continue
            max_pos = int(p.get("max_positions") or 0)
            if max_pos > 0 and len(open_symbols) >= max_pos:
                results.append({"symbol": symbol, "ok": False, "error": "达到最大持仓数"})
                continue
            ex, ex_name, filters, pick_err = await self._pick_entry_exchange(symbol)
            if ex is None or filters is None:
                results.append({"symbol": symbol, "ok": False,
                                "error": pick_err or "两家交易所都没有该合约"})
                continue
            ex_label = self._ex_label(ex_name)
            mark = await ex.fetch_mark_price(symbol)
            if mark is None:
                results.append({"symbol": symbol, "ok": False,
                                "error": f"无法获取 {ex_label} 标记价"})
                continue
            try:
                ref_price = D(it["entry"]) if it.get("entry") not in (None, "") else mark
                exec_price = mark          # 首次开仓一律市价（2026-11-01 策略调整）
                mult = D(it.get("mult") or "1")
                head = D(it.get("head") or "1")
                if head <= 0:
                    head = Decimal("1")
                target_value = (base_notional * mult if scale_by_mult
                                else base_notional * head)
                qty, min_fill, min_fill_value = head_entry_qty(target_value, exec_price, filters)
            except Exception:
                results.append({"symbol": symbol, "ok": False, "error": "精度参数异常"})
                continue
            if qty <= 0 or qty < D(filters["minQty"]) or qty * exec_price < D(filters["minNotional"]):
                results.append({"symbol": symbol, "ok": False, "error": "低于最小名义价值"})
                continue
            fill_note = ""
            if min_fill:
                fill_note = (f"；头仓目标 {pct_str(target_value)}U 不够最小一手，"
                             f"按最小 {qty}（约 {pct_str(min_fill_value)}U）开仓")
            hold_side = "long" if it["direction"] == "long" else "short"
            mm_ok, mm_err = await ex.ensure_margin_mode(symbol, target_mode)
            if not mm_ok:
                self.db.add_log(
                    "warn",
                    f"补开 {symbol} 在 {ex_label} 的{mode_label}回读未确认（{mm_err}）；仍按显式{mode_label}尝试下单，交易所拒绝就不会开仓。")
            lev_ok = await ex.set_leverage(symbol, p["leverage"], hold_side)
            if not lev_ok:
                lev_ok = await ex.set_leverage(symbol, p["leverage"])
            if not lev_ok:
                results.append({"symbol": symbol, "ok": False, "error": "杠杆设置失败"})
                continue
            old = self.db.get_signal(sid)
            if old and old.get("status") == ST_CLOSED:
                ts = int(float(old.get("close_ts") or time.time()))
                self.db.archive_signal_id(sid, f"{sid}_closed{ts}")
            open_side = "BUY" if it["direction"] == "long" else "SELL"
            client_id = (f"sl_{sid}_bf{int(time.time())}" if scale_by_mult
                         else f"sl_{sid}_catchup")
            layers = max([int(x) for x in (it.get("grades") or [])] or [0])
            self.db.add_log("info",
                            f"{reason or '补开'}：{sid}（{symbol} {it['direction']}，来源：{it.get('source') or source or '-'}）"
                            f"路由到 {ex_label}，标记价 {pct_str(mark)}，平台价 {pct_str(ref_price)}，市价开仓（{mode_label}），"
                            f"数量 {qty}（名义约 {pct_str(qty * exec_price)} USDT；"
                            f"首仓 {pct_str(head)}X，基础 {pct_str(base_notional)}U × 平台 {pct_str(mult)}X）"
                            f"{fill_note}")
            result = await ex.market_open(
                symbol, open_side, qty, client_id, margin_mode=target_mode)
            mult = str(it.get("mult") or "1")
            if result.ok is None:
                self.db.upsert_signal(
                    id=sid, symbol=symbol, direction=it["direction"], source=it.get("source") or source or "",
                    exchange=ex_name,
                    qty=str(qty), notional=str(base_notional), entry=str(it["entry"] or ""),
                    avg_entry=str(mark), stop=str(it["stop"] or ""), target=str(it["target"] or ""),
                    status=ST_UNKNOWN, skip_reason="", open_time=now_str(), open_ts=time.time(),
                    martin_layers=layers, martin_multiplier=mult, platform_multiplier=mult,
                    head_multiplier=str(head),
                    platform_avg=str(it.get("avg") or ""),
                    follow_martingale="1" if follow_martingale else "",
                    note=f"补开结果未知：{result.error}")
                results.append({"symbol": symbol, "ok": False, "error": f"结果未知：{result.error}"})
                continue
            elif result.ok is False:
                self.db.upsert_signal(
                    id=sid, symbol=symbol, direction=it["direction"], source=it.get("source") or source or "",
                    exchange=ex_name,
                    qty=str(qty), notional=str(base_notional), entry=str(it["entry"] or ""),
                    stop=str(it["stop"] or ""), target=str(it["target"] or ""),
                    status=ST_DROPPED, skip_reason=SKIP_ORDERFAIL,
                    open_time=now_str(), open_ts=time.time(), note=result.error or "开仓被拒绝")
                results.append({"symbol": symbol, "ok": False, "error": result.error or "开仓被拒绝"})
                continue
            taker = self._fee_rate(filters)
            open_fee = self._open_fee_for(filters, qty, mark)
            self.db.upsert_signal(
                id=sid, symbol=symbol, direction=it["direction"], source=it.get("source") or source or "",
                exchange=ex_name,
                qty=str(qty), notional=str(base_notional), entry=str(it["entry"] or ""),
                avg_entry=str(mark), base_qty=str(qty),
                stop=str(it["stop"] or ""), target=str(it["target"] or ""),
                status=ST_HOLDING, skip_reason="", open_time=now_str(), open_ts=time.time(),
                open_order_id=result.order_id or "", open_order_ids=result.order_id or "",
                pending_open_order_id="", pending_open_qty="", pending_open_price="",
                martin_layers=layers, martin_multiplier=mult, platform_multiplier=mult,
                head_multiplier=str(head),
                platform_avg=str(it.get("avg") or ""),
                follow_martingale="1" if follow_martingale else "",
                note=f"{reason or '补开'}（首仓 {pct_str(head)}X，基础 {pct_str(base_notional)}U × 平台 {mult}X"
                     f"{'，按 ' + str(layers) + ' 层开够' if scale_by_mult and layers else ''}"
                     f"{'，头仓目标不够最小一手，已按最小一手凑整' if min_fill else ''}）",
                open_fee=money_str(open_fee), close_fee="", gross_pnl="", net_pnl="",
                fee_note="手续费暂为估算，正在获取交易所实际值...",
                margin_mode=target_mode, leverage=str(p["leverage"]),
                funding_fee="0", funding_note=f"资金费（按 {ex_label} 公开资金费率估算）",
                last_funding_ts=time.time())
            self.mark_cache[(ex_name, symbol)] = (time.time(), mark)
            self.db.record_qty_event(sid, time.time(), str(qty), "补开")
            open_symbols.add(symbol)
            results.append({"symbol": symbol, "ok": True, "error": ""})
            if not self._is_sim() and result.order_id:
                asyncio.create_task(self._enrich_open_fees(
                    sid, symbol, ex, sync_qty=True))
            asyncio.create_task(self._verify_open_settings(
                sid, symbol, hold_side, int(p["leverage"]), target_mode, exchange=ex))
        for r in results:
            if not r["ok"]:
                self.db.add_log(
                    "warn",
                    f"{reason or '补开'}失败/跳过：{r['symbol']}：{r['error'] or '未知原因'}")
        ok_count = sum(1 for r in results if r["ok"])
        bad_count = len(results) - ok_count
        self.db.add_log(
            "info",
            f"{reason or '补开'}完成：成功 {ok_count} 个，失败/跳过 {bad_count} 个；"
            f"新开仓按 {mode_label}执行，后续信号会正常开仓并跟随马丁。")
        return {"ok": True, "source": source, "results": results,
                "ok_count": ok_count, "bad_count": bad_count}

    async def _wait_startup_sync(self, timeout=180):
        """后入前等启动时的交易所持仓对账完成，避免把用户已经手动平掉的旧本地仓位
        当成真实持仓，导致漏开后入标的或后续平仓误伤新仓。"""
        task = self._startup_sync_task
        if task is None:
            return True
        for attempt in range(2):
            if not task.done():
                self.db.add_log("info", "浮亏标的后入：等待启动时的交易所持仓对账完成...")
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
                except asyncio.TimeoutError:
                    self.db.add_log(
                        "warn", f"启动对账超过 {timeout} 秒未完成，本次不后入，稍后重试。")
                    return False
                except Exception:
                    pass
            try:
                task.result()
                return True
            except Exception as e:
                if attempt == 0:
                    self.db.add_log(
                        "warn", f"启动对账失败（{e.__class__.__name__}：{e}），重试一次后再后入。")
                    task = asyncio.create_task(self._sync_exchange_closures())
                    self._startup_sync_task = task
                    continue
                self.db.add_log(
                    "warn", f"启动对账仍失败（{e.__class__.__name__}：{e}），本次不后入，稍后重试。")
                return False
        return False

    async def _collect_floating_loss_candidates(self):
        """收集平台仍在跟踪、本地未持有、当前浮亏的标的（费率信号除外）。
        做多做空都包含；同币种有多个信号时只保留平台累计倍数最高的一个。"""
        preview = self.catchup_preview(None)
        best_by_symbol = {}
        for it in preview.get("items") or []:
            if not it.get("to_open"):
                continue
            sid = str(it.get("id") or "")
            sig = self.db.get_signal(sid)
            if sig and sig.get("status") in (ST_HOLDING, ST_UNKNOWN, ST_PENDING):
                continue
            direction = str(it.get("direction") or "")
            if direction not in ("long", "short"):
                continue
            symbol = str(it.get("symbol") or "")
            if not symbol:
                continue
            try:
                avg = D(it.get("avg") or it.get("entry") or "")
            except Exception:
                continue
            if avg <= 0:
                continue
            try:
                mult = D(it.get("mult") or "1")
            except Exception:
                mult = Decimal("1")
            if mult <= 0:
                mult = Decimal("1")
            old = best_by_symbol.get(symbol)
            if old is None or mult > old["_mult"]:
                item = dict(it)
                item["_avg"] = avg
                item["_mult"] = mult
                best_by_symbol[symbol] = item
        if not best_by_symbol:
            return [], [], []
        symbols = list(best_by_symbol.keys())
        await self._fetch_marks(symbols)
        base = D(self.params.all()["notional"])
        loss_items, price_failed, profit_items = [], [], []
        for symbol, it in best_by_symbol.items():
            cached = self.mark_cache.get(symbol)
            mark = None
            if cached and time.time() - cached[0] <= 30:
                mark = cached[1]
            if mark is None:
                seen_ex = set()
                for _name, ex in self._active_exchanges().items():
                    if id(ex) in seen_ex:
                        continue
                    seen_ex.add(id(ex))
                    try:
                        mark = await ex.fetch_mark_price(symbol)
                    except Exception:
                        mark = None
                    if mark is not None:
                        break
            if mark is None:
                price_failed.append(symbol)
                continue
            avg = it["_avg"]
            if it["direction"] == "long":
                pnl_pct = (mark - avg) / avg * Decimal("100")
            else:
                pnl_pct = (avg - mark) / avg * Decimal("100")
            it["mark"] = mark
            it["pnl_pct"] = pnl_pct
            it["layers"] = max([int(x) for x in (it.get("grades") or [])] or [0])
            it["target_value"] = base * it["_mult"]
            (loss_items if pnl_pct < 0 else profit_items).append(it)
        loss_items.sort(key=lambda x: str(x.get("symbol") or ""))
        return loss_items, price_failed, profit_items

    async def floating_loss_preview(self):
        """只读预览：后入清单（平台仍跟踪、本地未持有、当前浮亏，费率信号除外）。"""
        self._ensure_exchange()
        items, price_failed, profit_items = await self._collect_floating_loss_candidates()
        base = D(self.params.all()["notional"])
        out = []
        for it in items:
            out.append({
                "id": it.get("id"), "symbol": it.get("symbol"),
                "direction": it.get("direction"), "source": it.get("source") or "",
                "layers": it.get("layers") or 0, "mult": pct_str(it["_mult"]),
                "pnl_pct": f"{it['pnl_pct']:+.2f}%", "avg": pct_str(it["_avg"]),
                "mark": pct_str(it["mark"]), "target_value": pct_str(it["target_value"]),
                "base_notional": pct_str(base),
            })
        profits = [{"symbol": x.get("symbol"), "pnl_pct": f"{x['pnl_pct']:+.2f}%"}
                   for x in profit_items]
        return {"ok": True, "items": out, "to_open_count": len(out),
                "price_failed": price_failed, "profit_skipped": profits,
                "base_notional": pct_str(base)}

    async def _follow_floating_loss_safe(self):
        for attempt in range(3):
            try:
                res = await self.follow_floating_loss_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.db.add_log(
                    "warn", f"浮亏标的后入失败（第 {attempt + 1}/3 次）："
                            f"{e.__class__.__name__}：{e}")
                res = None
            if res and res.get("ok") and not res.get("failed"):
                return
            if attempt < 2:
                await asyncio.sleep(10)

    async def follow_floating_loss_once(self, force=False):
        """兼容接入流程：按外部服务的当前状态建立待跟踪记录；公开演示不调用此流程。"""
        key = "follow_all_floating_loss_20261108"
        if not force and self.db.get_setting(key, "0") == "1":
            return {"ok": True, "opened": 0, "candidates": [], "done_before": True}
        if self._is_sim():
            return {"ok": False, "error": "模拟盘不适用"}
        self._ensure_exchange()
        if not await self._wait_startup_sync():
            return {"ok": False, "error": "启动对账未完成", "failed": 1, "candidates": []}
        items, price_failed, profit_items = await self._collect_floating_loss_candidates()
        loss_text = "、".join(
            f"{x['symbol']} {'多' if x['direction'] == 'long' else '空'}"
            f" 平台{x['layers'] or '-'}层/{pct_str(x['_mult'])}X"
            f" 浮亏{x['pnl_pct']:+.2f}%→{pct_str(x['target_value'])}U"
            for x in items) if items else "无"
        profit_text = "、".join(f"{x['symbol']} {x['pnl_pct']:+.2f}%" for x in profit_items) or "无"
        if not items:
            if price_failed:
                self.db.add_log(
                    "warn",
                    f"浮亏标的后入：{len(price_failed)} 个标的取不到标记价（{'、'.join(price_failed)}），"
                    f"本次不标记完成，稍后重试（盈利跳过：{profit_text}）。")
                return {"ok": False, "error": "取价失败", "failed": len(price_failed),
                        "candidates": [], "price_failed": price_failed}
            self.db.add_log(
                "info",
                f"浮亏标的后入：当前平台跟踪标的中没有需要后入的浮亏标的"
                f"（盈利跳过：{profit_text}）。")
            if not force:
                self.db.set_setting(key, "1")
            return {"ok": True, "opened": 0, "candidates": [],
                    "profit_details": profit_text}
        base = D(self.params.all()["notional"])
        self.db.add_log(
            "info",
            f"浮亏标的后入：准备开 {len(items)} 个（{'；'.join(loss_text)}），"
            f"每个按 {pct_str(base)}U × 平台倍数市价开仓，并跟随后续马丁；盈利跳过：{profit_text}。")
        result = await self.open_catchup_signals(
            None, ids=[x["id"] for x in items],
            reason="后入平台浮亏标的（按平台层数）",
            follow_martingale=True, scale_by_mult=True)
        result["candidates"] = [x["id"] for x in items]
        result["loss_details"] = loss_text
        result["profit_details"] = profit_text
        if price_failed:
            result["failed"] = len(price_failed)
            result["ok"] = False
            result["price_failed"] = price_failed
            self.db.add_log(
                "warn",
                f"浮亏标的后入：另有 {len(price_failed)} 个标的取价失败"
                f"（{'、'.join(price_failed)}），本次不标记完成，稍后重试。")
            return result
        if not force:
            self.db.set_setting(key, "1")
        return result

    async def _telegram_loop(self):
        """轮询 Telegram Bot API，读取平台 Webhook 推到群里的信号消息。
        解析器完成前只做：按信封分类、原始消息记流水和日志，**绝不下单**。"""
        p = self.params.all()
        token = str(p.get("telegram_token") or "").strip()
        chat_id = str(p.get("telegram_chat_id") or "").strip()
        if not token or not chat_id:
            return
        listener = TelegramListener(token, chat_id)
        offset = None
        last_err_at = 0.0
        self.db.add_log("info", "Telegram 信号接收已开启（只读轮询；解析器完成前只记录、不下单）。")
        try:
            while self.running:
                try:
                    offset, msgs = await listener.poll(offset, timeout=25)
                    for _mid, _ts, text in msgs:
                        self._handle_telegram_text(text)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    now = time.time()
                    if now - last_err_at > 300:
                        self.db.add_log(
                            "warn", f"Telegram 轮询失败（10 分钟只报一次）：{e.__class__.__name__}：{e}")
                        last_err_at = now
                    await asyncio.sleep(5)
        finally:
            await listener.close()

    @staticmethod
    def classify_telegram_text(text):
        """按平台 Telegram 推送的信封格式解析：首行 '🔵 外部信号服务 · 类型'，尾行时间戳。
        返回 {"kind", "body", "footer", "raw"}；无法识别返回 None。"""
        lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
        if not lines:
            return None
        m = re.match(r"^(?:[^\s·]+\s*)?外部信号服务\s*·\s*(.+)$", lines[0])
        if not m:
            return None
        body = "\n".join(lines[1:-1]) if len(lines) > 2 else ""
        return {"kind": m.group(1).strip(), "body": body,
                "footer": lines[-1] if lines else "", "raw": text or ""}

    def _append_telegram_jsonl(self, text):
        try:
            with open(TG_JSONL_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": time.time(), "text": text},
                                   ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _handle_telegram_text(self, text):
        info = self.classify_telegram_text(text)
        if info is None:
            self.db.add_log(
                "warn",
                f"TG 消息：无法识别信封格式，已跳过（原文前 200 字）：{str(text)[:200]}")
            return
        kind = info["kind"]
        self._append_telegram_jsonl(text)
        if kind in ("测试消息",):
            self.db.add_log("info", f"TG 消息（{kind}）：{info['body'][:120] or '-'}")
        elif kind in ("新信号", "马丁加仓", "止盈调整", "止盈上移", "信号结算", "信号结束"):
            # 解析器未完成：只记录，不下单。等真实消息样例后在此接入 dispatch。
            self.db.add_log(
                "info",
                f"TG 消息（{kind}）：已记录原文（{len(text)} 字），解析器完成后将自动处理。")
        else:
            self.db.add_log("info", f"TG 消息（{kind}）：暂不处理。")

    async def _position_watch_loop(self):
        """实盘每 15 秒对比交易所实际持仓：止损计划单成交后，交易所已经没有仓位，
        但平台还没发结束信号时，本地必须按成交明细自动平仓记账。"""
        while self.running:
            try:
                await asyncio.sleep(15)
                if not self.running:
                    break
                if self._is_sim():
                    continue
                await self._prime_gate_symbols()
                await self._sync_pending_entries()
                await self._sync_exchange_closures()
                await self._sync_pending_adds()
            except asyncio.CancelledError:
                break
            except Exception:
                continue

    async def _sync_pending_entries(self, skip_verify_ids=None):
        """检查限价开仓挂单：交易所出现该交易对仓位就视为已成交，
        撤销剩余挂单，按实际数量/均价转为持仓并回填手续费。
        skip_verify_ids：这些记录刚被我们主动撤单（平台结束），不要再判“挂单失效”，否则误报。"""
        pending = self.db.pending_entries()
        if not pending:
            return
        skip = set(skip_verify_ids or ())
        pos_map = await self._positions_by_exchange()
        for sig in pending:
            symbol = sig["symbol"]
            ex = self._ex(sig)
            ex_pos = pos_map.get((str(sig.get("exchange") or "bitget"), symbol))
            if ex_pos is None:
                # 没成交：确认这笔限价单是否还挂在交易所，避免“本地一直显示挂单中”而实际早已没了
                if sig["id"] not in skip:
                    await self._verify_pending_order(sig)
                continue
            oid = str(sig.get("pending_open_order_id") or "")
            if oid:
                try:
                    await ex.cancel_order(symbol, oid)
                except Exception:
                    pass
            try:
                actual_qty = D(ex_pos.get("total") or "0")
                actual_avg = D(ex_pos.get("openPriceAvg") or sig.get("avg_entry") or "0")
            except Exception:
                continue
            if actual_qty <= 0:
                continue
            ids = self._open_order_ids(sig)
            if oid and oid not in ids:
                ids.append(oid)
            fields = {
                "status": ST_HOLDING, "qty": str(actual_qty),
                "avg_entry": str(actual_avg), "base_qty": str(actual_qty),
                "pending_open_order_id": "", "pending_open_qty": "",
                "pending_open_price": "",
                "open_order_id": oid or sig.get("open_order_id") or "",
                "open_order_ids": ",".join(ids),
                "open_fee": "", "fee_note": "手续费暂为估算，正在获取交易所实际值...",
                "funding_fee": "0",
                "funding_note": f"资金费（按 {self._ex_label(sig.get('exchange'))} 公开资金费率估算）",
                "last_funding_ts": time.time(),
                "note": "限价开仓已成交，按交易所实际数量/均价校准",
            }
            try:
                ctime = float(ex_pos.get("cTime") or 0) / 1000.0
                if ctime > 0:
                    fields["open_time"] = ledger_time(ctime)
                    fields["open_ts"] = ctime
            except Exception:
                pass
            self.db.update_signal(sig["id"], **fields)
            self.db.record_qty_event(sig["id"], time.time(), str(actual_qty), "限价开仓成交")
            self.db.add_log(
                "info",
                f"限价开仓成交：{sig['id']}（{symbol}）数量 {actual_qty}，"
                f"均价 {pct_str(actual_avg)}；已转为持仓并回填手续费。")
            if oid:
                asyncio.create_task(self._enrich_open_fees(sig["id"], symbol, ex))

    async def _verify_pending_order(self, sig):
        """核对未成交订单是否仍存在；明确不存在时更新本地状态，无法确认时保留待核对。"""
        if self._is_sim():
            return
        oid = str(sig.get("pending_open_order_id") or "")
        if not oid:
            return
        symbol = sig["symbol"]
        ex = self._ex(sig)
        try:
            orders = await ex.fetch_open_orders(symbol)
        except Exception:
            return                      # 查询失败就下一轮再查，绝不因为接口抖动误判
        if any(str(o.get("orderId") or "") == oid for o in (orders or [])):
            self._pending_missing.pop(sig["id"], None)
            return
        # 不在挂单列表里：查详情，区分“刚成交”和“已撤销/不存在”
        state = ""
        try:
            detail = await ex.fetch_order_detail(symbol, oid) or {}
            state = str(detail.get("state") or "").lower()
        except Exception:
            state = ""
        if state in ("filled", "partially_filled"):
            return                      # 刚成交，下一轮走“出现仓位→转持仓”
        if state in ("canceled", "cancelled", "canceled_partially_filled"):
            self._mark_pending_gone(sig, f"交易所状态 {state}")
            return
        # 详情也查不到（订单可能已被历史清理）：连续两次确认再判定，避免误杀
        n = int(self._pending_missing.get(sig["id"], 0)) + 1
        self._pending_missing[sig["id"]] = n
        if n >= 2:
            self._mark_pending_gone(sig, "交易所挂单列表和订单详情都查不到这笔单")

    def _mark_pending_gone(self, sig, reason):
        self._pending_missing.pop(sig["id"], None)
        price = sig.get("pending_open_price") or ""
        self.db.update_signal(
            sig["id"], status=ST_SKIPPED, skip_reason="挂单已失效",
            note=f"限价开仓单已不在交易所（{reason}），本地标记跳过")
        self.db.add_log(
            "alert",
            f"【挂单已失效】{sig['symbol']} 的限价开仓单（{price}）在交易所查不到了：{reason}。"
            f"本地已标记为跳过，不会再有仓位；这条信号如需跟单请人工处理。")

    async def pending_debug(self):
        """只读诊断：把本地“挂单中”的记录和交易所的挂单/订单详情对照，排查“看不到挂单”。"""
        out = []
        try:
            self._ensure_exchange()
        except Exception as e:
            return {"ok": False, "error": str(e), "pending": []}
        for sig in self.db.pending_entries():
            item = {
                "id": sig["id"], "symbol": sig["symbol"], "direction": sig["direction"],
                "price": sig.get("pending_open_price") or "",
                "qty": sig.get("pending_open_qty") or "",
                "order_id": sig.get("pending_open_order_id") or "",
                "open_time": sig.get("open_time") or "",
            }
            if self._is_sim():
                item["sim"] = True
                out.append(item)
                continue
            ex = self._ex(sig)
            try:
                orders = await ex.fetch_open_orders(sig["symbol"])
                item["exchange_open_orders"] = [
                    {"orderId": o.get("orderId"), "price": o.get("price"),
                     "size": o.get("size"), "state": o.get("state"),
                     "side": o.get("side"), "clientOid": o.get("clientOid")}
                    for o in (orders or [])]
                item["in_exchange"] = any(
                    str(o.get("orderId") or "") == item["order_id"] for o in (orders or []))
            except Exception as e:
                item["open_orders_error"] = str(e)
            if item["order_id"]:
                try:
                    item["order_detail"] = await ex.fetch_order_detail(
                        sig["symbol"], item["order_id"])
                except Exception as e:
                    item["detail_error"] = str(e)
            out.append(item)
        return {"ok": True, "pending": out}

    async def _sync_pending_adds(self):
        """通过完整挂单与持仓列表核对加仓单，避免单标的详情接口失败导致错误推断。"""
        if self._is_sim():
            return
        pending = [p for p in self.db.open_positions()
                   if str(p.get("pending_add_order_id") or "")]
        if not pending:
            return
        orders_by_ex = {}
        for name, ex in self._active_exchanges().items():
            try:
                orders_by_ex[name] = await ex.fetch_open_orders()   # 不带 symbol
            except Exception:
                orders_by_ex[name] = None
        pos_map = await self._positions_by_exchange()

        for sig in pending:
            oid = str(sig.get("pending_add_order_id") or "")
            ex_name = str(sig.get("exchange") or "bitget")
            orders = orders_by_ex.get(ex_name)
            if orders is None:
                continue      # 这家交易所查不到就跳过这一轮，绝不因为接口抖动误清
            live_ids = {str(o.get("orderId") or "") for o in (orders or [])}
            if oid in live_ids:
                hit = next((o for o in (orders or [])
                            if str(o.get("orderId") or "") == oid), None)
                price = str((hit or {}).get("price") or "")
                if price and str(sig.get("pending_add_price") or "") != price:
                    self.db.update_signal(sig["id"], pending_add_price=price)
                self._pending_missing.pop(sig["id"], None)
                continue
            ex_pos = pos_map.get((ex_name, sig["symbol"]))
            try:
                actual = D((ex_pos or {}).get("total") or "0")
                local = D(sig.get("qty") or "0")
            except Exception:
                continue
            # 先按交易所实际仓位校准数量，再清挂单；否则已成交的加仓还没入账，

            if ex_pos is not None and abs(actual - local) > Decimal("0.0000001"):
                try:
                    await self._reconcile_position_mismatch(sig, ex_pos)
                except Exception:
                    pass
                latest = self.db.get_signal(sig["id"]) or sig
                try:
                    local = D(latest.get("qty") or "0")
                except Exception:
                    pass
            filled = actual > local + Decimal("0.0000001")
            self._clear_gone_pending_add(sig, oid, filled)

    def _clear_gone_pending_add(self, sig, order_id, filled):
        """挂单已不在交易所：清掉“挂单中”显示，订单号留给手续费/数量对账。
        filled=True 表示交易所仓位已经比本地大，基本可确定是成交了。"""
        latest = self.db.get_signal(sig["id"]) or sig
        if str(latest.get("pending_add_order_id") or "") != str(order_id):
            return
        ids = self._open_order_ids(latest)
        if order_id not in ids:
            ids.append(order_id)
        self._pending_missing.pop(sig["id"], None)
        if filled:
            note = f"马丁加仓限价单已成交（{order_id}），挂单显示已清除，等数量对账"
        else:
            note = f"马丁加仓挂单已不在交易所（{order_id}），挂单显示已清除"
        self.db.update_signal(
            sig["id"], pending_add_order_id="", pending_add_qty="",
            pending_add_price="", pending_add_ts=0,
            open_order_ids=",".join(ids), note=note)
        self.db.add_log(
            "info",
            f"{latest.get('symbol') or sig.get('symbol')} 马丁加仓挂单已不在交易所"
            f"（{'已成交' if filled else '已撤销或已对账'}），已从“挂单中”清除。")
        asyncio.create_task(self._enrich_open_fees(
            sig["id"], sig["symbol"], self._ex(sig)))

    def _clear_pending_add(self, sig, reason):
        # 只能用“检查时的那笔旧单号”清本地记录；如果这期间已经挂了新单，
        # 直接清会把新挂单从页面上抹掉，旧单还可能继续挂在交易所造成重复加仓。
        checked_oid = str(sig.get("pending_add_order_id") or "")
        latest = self.db.get_signal(sig["id"]) or sig
        current_oid = str(latest.get("pending_add_order_id") or "")
        if not checked_oid or current_oid != checked_oid:
            return
        self._pending_missing.pop(sig["id"], None)
        self.db.update_signal(
            sig["id"], pending_add_order_id="", pending_add_qty="",
            pending_add_price="", pending_add_ts=0,
            note=f"马丁加仓限价单已失效（{reason}），已清除本地挂单状态")
        self.db.add_log(
            "info",
            f"{latest.get('symbol') or sig.get('symbol')} 马丁加仓挂单已不在交易所"
            f"（{reason}），已清除本地挂单显示。")

    async def _reconcile_position_mismatch(self, sig, ex_pos):
        """本地仓位数量/均价与交易所不一致时校准：
        - 交易所数量更少：差额视为部分平仓，按成交明细拆出一条已平仓记录；
        - 交易所数量更多/均价偏差：以交易所实际为准校准当前记录。
        只读成交明细，不下新单。"""
        symbol = sig["symbol"]
        direction = sig["direction"]
        sig_id = sig["id"]
        ex = self._ex(sig)
        try:
            open_ts = float(sig.get("open_ts") or 0)
        except Exception:
            open_ts = 0.0
        try:
            actual_qty = D(ex_pos.get("total") or "0")
            actual_avg = D(ex_pos.get("openPriceAvg") or sig.get("avg_entry") or "0")
            local_qty = D(sig.get("qty") or "0")
            local_avg = D(sig.get("avg_entry") or sig.get("entry") or "0")
        except Exception:
            return
        if actual_qty <= 0:
            return
        qty_diff = local_qty - actual_qty
        if abs(qty_diff) <= Decimal("0.0000001") and abs(local_avg - actual_avg) <= Decimal("0.0000001"):
            return
        if abs(qty_diff) <= Decimal("0.0000001"):
            self.db.update_signal(sig_id, avg_entry=str(actual_avg),
                                  note=f"已按交易所实际均价校准为 {actual_avg}")
            self.db.add_log("warn", f"{symbol} 均价与交易所不一致，已按实际均价 {actual_avg} 校准。")
            return
        if qty_diff <= 0:
            fill_explained = False
            pending_oid = str(sig.get("pending_add_order_id") or "")
            if pending_oid:
                try:
                    fills = await ex.get_order_fills(symbol, pending_oid)
                except Exception:
                    fills = []
                try:
                    vol = sum((D(f.get("volume") or "0") for f in fills), Decimal("0"))
                    if vol > 0:
                        fill_explained = True
                        ids = self._open_order_ids(sig)
                        if pending_oid not in ids:
                            ids.append(pending_oid)
                        fields = {"open_order_ids": ",".join(ids)}
                        pending_qty = D(sig.get("pending_add_qty") or "0")
                        latest = self.db.get_signal(sig_id) or sig
                        current_oid = str(latest.get("pending_add_order_id") or "")
                        if (pending_qty > 0 and vol >= pending_qty * Decimal("0.999")
                                and current_oid == pending_oid):
                            fields["pending_add_order_id"] = ""
                            fields["pending_add_qty"] = ""
                            fields["pending_add_price"] = ""
                            fields["pending_add_ts"] = 0
                        self.db.update_signal(sig_id, **fields)
                        ctime = float(ex_pos.get("cTime") or 0) / 1000.0
                        self.db.record_qty_event(
                            sig_id, ctime or time.time(), str(actual_qty),
                            "马丁限价加仓成交")
                        self.db.add_log(
                            "info",
                            f"{symbol} 马丁限价加仓已成交 {pct_str(vol)} 张，"
                            f"仓位按交易所实际校准，手续费按成交明细回填。")
                        asyncio.create_task(self._enrich_open_fees(sig_id, symbol, ex))
                except Exception:
                    pass
            self.db.update_signal(sig_id, qty=str(actual_qty), avg_entry=str(actual_avg),
                                  base_qty=str(actual_qty),
                                  note="已按交易所实际数量/均价校准（本地少记）")
            if not fill_explained:
                # 正常马丁成交上面已经写过 info；这里只提示无法用程序订单解释的差异
                self.db.add_log(
                    "warn",
                    f"{symbol} 交易所数量大于本地（实际 {actual_qty}，本地 {local_qty}），"
                    f"可能是手动加仓或交易所部分成交，已按实际校准。")
            latest = self.db.get_signal(sig_id) or sig
            asyncio.create_task(self._sync_manual_open_fees(latest))
            return

        try:
            fills = await ex.get_symbol_fills(symbol, open_ts - 60, time.time() + 5)
        except Exception:
            fills = []
        try:
            self.db.record_fee_events(fills, sig_id)
        except Exception:
            pass
        want_open = "sell" if direction == "short" else "buy"
        want_close = "buy" if direction == "short" else "sell"
        open_fills_all = [f for f in fills
                          if str(f.get("side") or "").lower() == want_open
                          and float(f.get("ts") or 0) >= open_ts]
        close_fills = [f for f in fills
                       if str(f.get("side") or "").lower() == want_close
                       and float(f.get("ts") or 0) >= open_ts]
        if not close_fills:
            self.db.update_signal(sig_id, qty=str(actual_qty), avg_entry=str(actual_avg),
                                  base_qty=str(actual_qty),
                                  note="本地数量与交易所不一致，已按交易所实际校准；未找到部分平仓成交")
            age = (time.time() - open_ts) if open_ts > 0 else 9999
            small_new = (0 <= age <= 60 and local_qty > 0
                         and abs(qty_diff) <= local_qty * Decimal("0.01"))
            level = "info" if small_new else "warn"
            why = ("新开仓市价单可能部分成交，已按实际成交校准"
                   if small_new else
                   "未找到部分平仓成交（可能是手动平仓或市价单部分成交），已先按交易所数量/均价校准")
            self.db.add_log(
                level,
                f"【仓位校准】{symbol} 本地 {local_qty} 张、交易所 {actual_qty} 张；{why}。")
            return

        close_ts = max(float(f.get("ts") or 0) for f in close_fills)
        current_opens = [f for f in open_fills_all if float(f.get("ts") or 0) > close_ts]
        current_open_vol = sum((D(f.get("volume") or "0") for f in current_opens), Decimal("0"))
        if current_opens and current_open_vol >= actual_qty * Decimal("0.99"):
            closed_open_fills = [f for f in open_fills_all if float(f.get("ts") or 0) <= close_ts]
        else:
            current_opens = []
            closed_open_fills = []

        partial_qty = qty_diff
        ratio = (partial_qty / local_qty) if local_qty > 0 else Decimal("0")
        total_open_fee = D(sig.get("open_fee") or "0")
        total_funding = D(sig.get("funding_fee") or "0")
        if closed_open_fills:
            partial_open_fee = sum((f.get("fee") or Decimal("0") for f in closed_open_fills), Decimal("0"))
        else:
            partial_open_fee = total_open_fee * ratio
        partial_funding = total_funding * ratio
        partial_sig = dict(sig)
        partial_sig["qty"] = str(partial_qty)
        partial_sig["notional"] = str(partial_qty * (local_avg if local_avg > 0 else actual_avg))
        partial_sig["avg_entry"] = str(local_avg)
        partial_sig["open_fee"] = money_str(partial_open_fee)
        partial_sig["funding_fee"] = money_str(partial_funding)
        fields = self._fill_accounting_fields(partial_sig, close_fills, closed_open_fills)
        fields["open_fee"] = money_str(partial_open_fee)
        fields["funding_fee"] = money_str(partial_funding)
        fields["funding_note"] = "资金费（按部分仓位拆分估算）"
        close_time = ledger_time(close_ts)
        last_close = max(close_fills, key=lambda f: float(f.get("ts") or 0))
        part_id = f"{sig_id}_part{int(close_ts)}"
        if not self.db.get_signal(part_id):
            self.db.upsert_signal(
                id=part_id, symbol=symbol, direction=direction, source=sig.get("source") or "",
                accounting_parent=sig_id,
                exchange=str(sig.get("exchange") or "bitget"),
                qty=str(partial_qty), notional=partial_sig["notional"], entry=str(local_avg),
                avg_entry=str(fields.get("avg_entry") or local_avg),
                stop=str(sig.get("stop") or ""), target=str(sig.get("target") or ""),
                status=ST_CLOSED, skip_reason="",
                open_time=sig.get("open_time") or "", open_ts=open_ts,
                close_time=close_time, close_ts=close_ts,
                final_price=str(fields.get("final_price") or ""),
                profit_percent="", end_reason="exchange_stop",
                end_note="本地止损部分成交，自动拆分对账",
                note="已平仓（部分止损成交，本地拆分对账）",
                close_order_id=str(last_close.get("order_id") or ""),
                open_order_ids=",".join(str(f.get("order_id") or "")
                                        for f in closed_open_fills if f.get("order_id")),
                open_fee=money_str(partial_open_fee),
                close_fee=str(fields.get("close_fee") or ""),
                gross_pnl=str(fields.get("gross_pnl") or ""),
                net_pnl=str(fields.get("net_pnl") or ""),
                fee_note=str(fields.get("fee_note") or "交易所实际手续费"),
                funding_fee=money_str(partial_funding),
                funding_note="资金费（按部分仓位拆分估算）",
                last_funding_ts=sig.get("last_funding_ts") or 0,
                base_qty=str(partial_qty), martin_layers=0, martin_multiplier="1",
            )

        current_fields = {
            "qty": str(actual_qty), "avg_entry": str(actual_avg), "base_qty": str(actual_qty),
            "note": "已按交易所实际仓位校准；原仓位部分止损已拆入历史",
            "martin_layers": 0, "martin_multiplier": "1",
        }
        if current_opens:
            current_fee = sum((f.get("fee") or Decimal("0") for f in current_opens), Decimal("0"))
            current_fields["open_fee"] = money_str(current_fee)
            current_fields["open_order_ids"] = ",".join(
                str(f.get("order_id") or "") for f in current_opens if f.get("order_id"))
            ctime = float(ex_pos.get("cTime") or 0) / 1000.0
            if ctime > 0:
                current_fields["open_time"] = ledger_time(ctime)
                current_fields["open_ts"] = ctime
                current_fields["last_funding_ts"] = ctime
            current_fields["funding_fee"] = money_str(total_funding - partial_funding)
        else:
            current_fields["open_fee"] = money_str(total_open_fee - partial_open_fee)
            current_fields["funding_fee"] = money_str(total_funding - partial_funding)
        self.db.update_signal(sig_id, **current_fields)
        self.db.record_qty_event(sig_id, close_ts, str(actual_qty), "部分平仓数量校准")
        self.db.add_log(
            "alert",
            f"【仓位校准】{symbol} 本地 {local_qty} 张、交易所 {actual_qty} 张；"
            f"已拆分部分平仓 {partial_qty} 张（净盈亏 {fields.get('net_pnl', '-')} USDT），"
            f"当前仓位已按交易所实际数量/均价校准。")

    async def _sync_exchange_closures(self):
        """对比交易所持仓：本地显示持仓中、交易所已无该仓位，且成交明细有反向平仓成交，
        就按真实成交价/手续费/盈亏把本地记录平掉。只读交易所，不会下新单。"""
        await self._prime_gate_symbols()
        pos_map = await self._positions_by_exchange()
        for sig in self.db.open_positions():
            if sig["status"] != ST_HOLDING:
                continue
            symbol = sig["symbol"]
            ex_name = str(sig.get("exchange") or "bitget")
            ex = self._ex(sig)
            ex_pos = pos_map.get((ex_name, symbol))
            if ex_pos is not None:
                try:
                    if "liquidationPrice" in ex_pos:
                        liq = str(ex_pos.get("liquidationPrice") or "")
                        if liq != str(sig.get("liquidation_price") or ""):
                            self.db.update_signal(sig["id"], liquidation_price=liq)
                except Exception:
                    pass
                try:
                    mode = str(ex_pos.get("marginMode") or "")
                    lev = str(ex_pos.get("leverage") or "")
                    fields = {}
                    if mode and mode != str(sig.get("margin_mode") or ""):
                        fields["margin_mode"] = mode
                    if lev and lev != str(sig.get("leverage") or ""):
                        fields["leverage"] = lev
                    if fields:
                        self.db.update_signal(sig["id"], **fields)
                except Exception:
                    pass
                try:
                    await self._reconcile_position_mismatch(sig, ex_pos)
                except Exception as e:
                    self.db.add_log("warn", f"{symbol} 仓位数量校准失败：{e}")
                continue
            try:
                open_ts = float(sig.get("open_ts") or 0)
                fills = await ex.get_symbol_fills(
                    symbol, open_ts - 60, time.time() + 5)
            except Exception:
                continue
            if not fills:
                continue
            want_side = "buy" if sig["direction"] == "short" else "sell"
            close_fills = [
                f for f in fills
                if str(f.get("side") or "").lower() == want_side
                and float(f.get("ts") or 0) >= open_ts
            ]
            if not close_fills:
                continue
            close_vol = sum((D(f.get("volume") or "0") for f in close_fills), Decimal("0"))
            qty = D(sig.get("qty") or "0")
            if qty > 0 and close_vol < qty * Decimal("0.99"):
                continue   # 只平了一部分，不当作已平仓
            open_ids = set(self._open_order_ids(sig))
            open_fills = [f for f in fills if str(f.get("order_id") or "") in open_ids]
            try:
                self.db.record_fee_events(close_fills + open_fills, sig["id"])
            except Exception:
                pass
            fields = self._fill_accounting_fields(sig, close_fills, open_fills)
            last_close = max(close_fills, key=lambda f: float(f.get("ts") or 0))
            close_ts = float(last_close.get("ts") or 0) or time.time()
            fields.update({
                "status": ST_CLOSED,
                "close_time": ledger_time(close_ts),
                "close_ts": close_ts,
                "close_order_id": str(last_close.get("order_id") or ""),
                "profit_percent": "",
                "end_reason": "exchange_stop",
                "end_note": f"{self._ex_label(ex_name)} 止损/交易所平仓成交，本地按成交明细对账",
                "note": "已平仓（交易所止损成交，本地自动对账）",
                "fee_note": (fields.get("fee_note") or "交易所实际手续费")
                            + "；止损成交后自动对账",
                # 平台可能还在跟踪这个信号，先标为“影子跟踪”，由后续 trailed/ended/回执修正
                "platform_tracking": "1",
                "platform_stop": str(sig.get("stop") or ""),
                "platform_target": str(sig.get("target") or ""),
                "platform_avg": str(sig.get("avg_entry") or sig.get("entry") or ""),
            })
            self.db.update_signal(sig["id"], **fields)
            self.db.add_log(
                "alert",
                f"【止损成交】{symbol} 的止损已在交易所触发，本地记录已按实际成交对账："
                f"平仓均价 {fields.get('final_price', '-')}，净盈亏 {fields.get('net_pnl', '-')} USDT。"
                f"信号平台可能仍在跟踪该信号，但我们的仓位已退出。")

    async def _verify_open_settings(self, sig_id, symbol, hold_side, target_lev,
                                    target_mode="crossed", exchange=None):
        """开仓后回读交易所实际保证金模式/杠杆。杠杆不符就自动改回来并告警；
        模式不符只告警（持仓中无法切换），绝不自动平仓。"""
        sig = self.db.get_signal(sig_id) or {}
        ex = exchange or self._ex(sig)
        ex_label = self._ex_label(sig.get("exchange"))
        try:
            await asyncio.sleep(2)
            positions = await ex.fetch_positions()
        except Exception:
            return
        pos = None
        for item in positions or []:
            try:
                if str(item.get("symbol")) == symbol and D(item.get("total") or "0") != 0:
                    pos = item
                    break
            except Exception:
                continue
        if not pos:
            return
        mode = str(pos.get("marginMode") or "")
        lev_raw = str(pos.get("leverage") or "")
        fields = {}
        if mode:
            fields["margin_mode"] = mode
        if lev_raw:
            fields["leverage"] = lev_raw
        if fields:
            try:
                self.db.update_signal(sig_id, **fields)
            except Exception:
                pass
        if mode and mode != target_mode:
            want_label = "全仓" if target_mode == "crossed" else "逐仓"
            self.db.add_log(
                "alert",
                f"【模式异常】{symbol} 实际是{('全仓' if mode == 'crossed' else '逐仓')}（目标{want_label}），"
                f"持仓中无法切换；请用网页“批量改保证金模式”按钮或手动处理，程序不会自动平仓。")
        try:
            actual = int(D(lev_raw))
        except Exception:
            actual = None
        if actual is None or actual == target_lev:
            return
        ok = await ex.set_leverage(symbol, target_lev, hold_side)
        await asyncio.sleep(1)
        try:
            positions = await ex.fetch_positions()
        except Exception:
            positions = []
        latest = None
        for item in positions or []:
            try:
                if str(item.get("symbol")) == symbol and D(item.get("total") or "0") != 0:
                    latest = item
                    break
            except Exception:
                continue
        new_lev = lev_raw
        if latest is not None:
            new_lev = str(latest.get("leverage") or lev_raw)
            try:
                self.db.update_signal(sig_id, leverage=new_lev,
                                      margin_mode=str(latest.get("marginMode") or mode))
            except Exception:
                pass
        if str(new_lev) == str(target_lev):
            self.db.add_log("warn", f"{symbol} 开仓时杠杆实际为 {lev_raw} 倍，已自动改为 {target_lev} 倍。")
        else:
            self.db.add_log(
                "alert",
                f"【杠杆异常】{symbol} 实际 {new_lev} 倍，目标 {target_lev} 倍；"
                f"自动调整{'成功但回读不一致' if ok else '失败'}，请到 {ex_label} 手动确认。")

    async def _enrich_open_fees(self, sig_id, symbol, exchange, sync_qty=False):
        """实盘开仓/加仓后：查交易所成交明细，用实际手续费替换估算值，
        并用实际成交价重算持仓均价（马丁加仓会累加所有开仓订单）。
        sync_qty=True 用于首次市价开仓：市价单可能只部分成交（如请求 4893、
        实际 4890），必须按成交明细校准数量，否则会误报“本地和交易所不一致”。"""
        sig = self.db.get_signal(sig_id)
        if not sig:
            return
        ids = self._open_order_ids(sig)
        if not ids:
            return
        fills = []
        for _ in range(6):
            await asyncio.sleep(1.2)
            fills = []
            try:
                for oid in ids:
                    fills.extend(await exchange.get_order_fills(symbol, oid))
            except Exception:
                fills = []
            if fills:
                break
        # 如果加仓期间又新增了订单号，交给更新的那个回填任务处理，避免旧数据覆盖新数据
        latest = self.db.get_signal(sig_id)
        if latest and set(self._open_order_ids(latest)) != set(ids):
            return
        if not fills:
            self.db.update_signal(sig_id, fee_note="未能获取交易所实际开仓手续费，当前显示为估算值")
            return
        if any(str(f.get("fee_coin") or "USDT").upper() != "USDT" or f.get("fee_known") is False for f in fills):
            self.db.update_signal(sig_id, fee_source="pending", fee_note="手续费缺失或币种不一致，等待实际 USDT 成本核对")
            return
        try:
            self.db.record_fee_events(fills, sig_id)
        except Exception:
            pass
        fee = sum((f["fee"] for f in fills), Decimal("0"))
        coins = {f.get("fee_coin", "USDT") for f in fills}
        note = "交易所实际手续费"
        if coins - {"USDT", "usdt"}:
            note += f"（含 {'/'.join(sorted(coins))} 抵扣）"
        fields = {"open_fee": amount_str(fee), "fee_note": note}
        # 用实际成交价按数量加权，算出自己账户的真实均价
        total_vol = Decimal("0")
        try:
            total_vol = sum((D(f.get("volume") or "0") for f in fills), Decimal("0"))
            if total_vol > 0:
                cost = sum((D(f.get("price") or "0") * D(f.get("volume") or "0") for f in fills), Decimal("0"))
                fields["avg_entry"] = str(cost / total_vol)
        except Exception:
            total_vol = Decimal("0")
        sig = latest or sig
        old_qty = Decimal("0")
        try:
            old_qty = D(sig.get("qty") or "0")
        except Exception:
            old_qty = Decimal("0")
        if (sync_qty and total_vol > 0
                and str(sig.get("status") or "") in (ST_HOLDING, ST_UNKNOWN)):
            fields["qty"] = str(total_vol)
            fields["base_qty"] = str(total_vol)
        if sig["status"] == ST_CLOSED and sig.get("gross_pnl") not in (None, ""):
            try:
                net = (D(sig["gross_pnl"] or "0") - fee - D(sig["close_fee"] or "0")
                       - D(sig.get("funding_fee") or "0"))
                fields["net_pnl"] = amount_str(net)
            except Exception:
                pass
        self.db.update_signal(sig_id, **fields)
        if sync_qty and total_vol > 0 and total_vol != old_qty:
            self.db.add_log(
                "info",
                f"{symbol} 开仓实际成交 {total_vol} 张（请求 {old_qty} 张），"
                f"已按成交明细校准数量。")
        # 程序只知道自己下的订单号；再按交易对全部成交补一次，把手动加仓也算进去
        try:
            latest = self.db.get_signal(sig_id) or sig
            await self._sync_manual_open_fees(latest)
        except Exception:
            pass

    async def _sync_manual_open_fees(self, sig):
        """按本信号自己记录的订单号，汇总这些订单在交易所的全部成交（含马丁加仓）。
        不属于本信号的成交只提示、不计入，避免把同一币其他信号的开仓/平仓
        算到这条记录上。重复执行是幂等的：每次都用本信号订单的成交重算总额。"""
        if self._is_sim():
            return
        sig_id = sig.get("id")
        symbol = sig.get("symbol")
        if not sig_id or not symbol:
            return
        latest = self.db.get_signal(sig_id) or sig
        if latest.get("status") not in (ST_HOLDING, ST_UNKNOWN):
            return
        known_ids = set(self._open_order_ids(latest))
        if not known_ids:
            return
        ex = self._ex(sig)
        try:
            open_ts = float(latest.get("open_ts") or 0)
            if open_ts <= 0:
                return
            # 只往前放 5 秒容忍时钟误差。放宽到分钟级会把同一币其他信号的

            fills = await ex.get_symbol_fills(symbol, open_ts - 5, time.time() + 5)
        except Exception:
            return
        if not fills:
            return
        mine = [f for f in fills if str(f.get("order_id") or "") in known_ids]
        if not mine:
            return
        if any(str(f.get("fee_coin") or "USDT").upper() != "USDT" or f.get("fee_known") is False for f in mine):
            self.db.update_signal(sig_id, fee_source="pending", fee_note="手续费缺失或币种不一致，等待实际 USDT 成本核对")
            return
        others = [f for f in fills if str(f.get("order_id") or "") not in known_ids]
        try:
            self.db.record_fee_events(mine, sig_id)
        except Exception:
            pass
        fee = sum((f.get("fee") or Decimal("0") for f in mine), Decimal("0"))
        # 父记录只保留尚未归入部分平仓记录的开仓成本。
        children = [s for s in self.db.all_signals(1000000)
                    if s.get("accounting_parent") == sig_id
                    or (str(s.get("id") or "").startswith(sig_id + "_part"))]
        for child in children:
            child_ids = set(self._open_order_ids(child))
            if not child_ids or child_ids <= known_ids:
                fee -= D(child.get("open_fee") or "0")
        new_fee = amount_str(fee)
        old_fee = amount_str(latest.get("open_fee") or "0")
        if D(new_fee) == D(old_fee):
            return
        self.db.update_signal(sig_id, open_fee=new_fee, fee_note="交易所实际手续费")
        extra = (f"；同期另有 {len(others)} 笔成交的订单号不属于本信号，未计入"
                 if others else "")
        self.db.add_log(
            "info",
            f"{symbol} 已按 {self._ex_label(sig.get('exchange'))} 成交明细校准开仓手续费："
            f"{old_fee or '0'} → {new_fee} USDT{extra}。")

    async def _sync_manual_open_fees_all(self):
        """启动后补一次所有持仓的手续费（含手动加仓），之后由数量变化触发。"""
        await asyncio.sleep(6)
        for sig in self.db.open_positions():
            if not self.running:
                return
            try:
                await self._sync_manual_open_fees(sig)
            except Exception:
                pass
            await asyncio.sleep(0.25)

    async def _enrich_close_fees(self, sig_id, symbol, exchange):
        """实盘平仓后：查所有开仓/加仓订单和平仓订单的成交明细，
        回填实际手续费、实际平仓盈亏，并用成交加权均价回填真实平仓价。"""
        sig = self.db.get_signal(sig_id)
        if not sig:
            return
        open_ids = self._open_order_ids(sig)
        close_order_id = sig.get("close_order_id") or ""
        close_fills = []
        for _ in range(6):
            await asyncio.sleep(1.5)
            try:
                if close_order_id:
                    close_fills = await exchange.get_order_fills(symbol, close_order_id)
            except Exception:
                close_fills = []
            if close_fills:
                break
        if not close_fills:
            self.db.update_signal(sig_id, fee_note="未能获取交易所实际平仓手续费，当前显示为估算值")
            return
        open_fills = []
        try:
            for oid in open_ids:
                open_fills.extend(await exchange.get_order_fills(symbol, oid))
        except Exception:
            pass
        try:
            self.db.record_fee_events(close_fills + open_fills, sig_id)
        except Exception:
            pass
        sig = self.db.get_signal(sig_id) or sig
        fields = self._fill_accounting_fields(sig, close_fills, open_fills)
        self.db.update_signal(sig_id, **fields)
        open_fee = D(fields.get("open_fee") or sig.get("open_fee") or "0")
        close_fee = D(fields.get("close_fee") or "0")
        price_text = (f"，平仓均价 {fields['final_price']}（按成交明细）"
                      if fields.get("final_price") else "")
        self.db.add_log("info",
                        f"信号 {sig_id}（{symbol}）已核对交易所实际手续费："
                        f"合计 {money_str(open_fee + close_fee)} USDT，"
                        f"净盈亏 {fields.get('net_pnl', '-')} USDT{price_text}。")

    async def _sync_today_fee_events(self, full=False):
        """按交易所分别维护完整同步区间，失败的交易所不推进游标。"""
        if self._is_sim():
            return
        await self._sync_account_ledger("fees", force=full)

    async def _recover_orphan_closed_positions(self):
        """读取真实持仓，按已同步实际成交补录缺失历史；绝不创建持仓或下单。"""
        if self._is_sim():
            return
        async with self._orphan_history_lock:
            candidates = self.db.orphan_closed_candidates("bitget")
            if not candidates:
                return
            ex = self._active_exchanges().get("bitget")
            if ex is None:
                return
            positions = await ex.fetch_positions()
            if not isinstance(positions, list):
                raise ValueError("Bitget实际持仓响应不完整，暂不补录缺失历史")
            symbols = set()
            for position in positions:
                if not isinstance(position, dict) or not position.get("symbol") or position.get("total") in (None, ""):
                    raise ValueError("Bitget实际持仓缺少币种或数量，暂不补录缺失历史")
                quantity = D(position["total"])
                if not quantity.is_finite():
                    raise ValueError("Bitget实际持仓数量异常，暂不补录缺失历史")
                if quantity != 0:
                    symbols.add(str(position["symbol"]).upper())
            history = []
            for symbol, opened in candidates.items():
                if symbol.upper() in symbols:
                    continue
                history.extend(await ex.get_closed_position_history(opened-5, time.time(), symbol))
            recovered = self.db.recover_orphan_closed_positions("bitget", symbols, history)
            for row in recovered:
                self.db.add_log("info", f"{row['symbol']}已按Bitget完整实际成交补录已平历史："
                    f"数量 {row['qty']}，开仓 {row['open_time']}，平仓 {row['close_time']}；未恢复自动交易跟踪。")

    async def _sync_account_ledger(self, kind, force=False, only_exchange=""):
        async with self._ledger_locks[kind]:
            errors = []
            current_ok = []
            label = "手续费" if kind == "fees" else "资金费"
            writer = self.db.replace_fee_window if kind == "fees" else self.db.replace_funding_window
            # 先完成两家当日增量，再补历史，不能让一家历史阻塞另一家的今日数据。
            for name, ex in self._active_exchanges().items():
                if only_exchange and name != only_exchange:
                    continue
                reader = getattr(ex, "get_all_fills" if kind == "fees" else "get_funding_bills", None)
                if not callable(reader):
                    self.db.ledger_error(name, kind, "交易所适配器暂不支持实际账单")
                    continue
                progress_key = name + ":" + kind
                first = progress_key not in self._ledger_seen
                if first:
                    self._ledger_seen.add(progress_key)
                    self.db.add_log("info", f"开始同步 {self._ex_label(name)} 实际{label}账单，先更新当日数据再补历史。")
                self._ledger_progress[progress_key] = {"phase": "更新当日", "label": self._ex_label(name)+label}
                state = self.db.ledger_state(name, kind)
                now, day = time.time(), day_start_ts()
                # 首次先修今天，再补齐历史；之后每家从自己的成功游标重叠补十分钟。
                start = max(day, float(state.get("end_ts") or day) - 600)
                if force or not state.get("end_ts") or float(state.get("end_ts") or 0) < day:
                    start = day
                if state.get("end_ts") and float(state.get("end_ts")) < start:
                    start = float(state["end_ts"]) - 600
                try:
                    rows = await reader(start, now)
                    writer(name, start, now, rows)
                    if kind == "fees":
                        self.db.reconcile_signal_fees(name)
                        key = "fee_events_synced_day_gate" if name == "gate" else "fee_events_synced_day"
                        from datetime import datetime, timezone, timedelta
                        self.db.set_setting(key, datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d"))
                    else:
                        self.db.reconcile_actual_funding(name)
                    current_ok.append((name, reader))
                    if first:
                        self.db.add_log("info", f"{self._ex_label(name)} 当日实际{label}已同步，正在检查历史覆盖。")
                except Exception as e:
                    message = f"{e.__class__.__name__}：{e}"
                    self.db.ledger_error(name, kind, message)
                    self._ledger_progress[progress_key]["phase"] = "当日同步失败"
                    errors.append(self._ex_label(name) + "：" + message)
            for name, reader in current_ok:
                progress_key = name + ":" + kind
                earliest = self.db.ledger_start(name) - 5
                began = time.monotonic()
                state = self.db.ledger_state(name, kind)
                had_gap = earliest < float(state["start_ts"])
                try:
                    # 每段成功立即提交和复核，进程重启可以从已成功的区间续补。
                    while time.monotonic() - began < 30:
                        repair = self.db.incomplete_fee_window(name) if kind == "fees" and name == "gate" else None
                        if repair:
                            left, end = repair["start"], repair["end"]
                        elif earliest < float(state["start_ts"]):
                            end = float(state["start_ts"])
                            left = max(earliest, end - 86400)
                        else:
                            break
                        self._ledger_progress[progress_key] = {"phase": "重取成交数量" if repair else "补历史",
                            "label": self._ex_label(name)+label, "covered_from": ledger_time(left)}
                        rows = await reader(left, end)
                        if repair:
                            found = {name+":"+str(f.get("fill_id") or "").removeprefix(name+":")
                                     for f in rows if left <= float(f.get("ts") or 0) <= end}
                            if not set(repair["fill_ids"]) <= found:
                                raise ValueError("Gate重取未返回原成交号，保留原账本继续核对")
                        writer(name, left, end, rows)
                        if kind == "fees":
                            self.db.reconcile_signal_fees(name)
                        self.db.ledger_error(name, kind + "_history", "")
                        state = self.db.ledger_state(name, kind)
                        if repair:
                            self.db.add_log("info", f"Gate已重新读取 {ledger_time(left)} 至 {ledger_time(end)} 的实际成交数量，费用按原始精度重新核验。")
                        else:
                            self.db.add_log("info", f"{self._ex_label(name)} {label}历史账单已补至 {ledger_time(left)}，已覆盖记录即时核验。")
                        # 历史请求耗时长时，先保鲜已展示的当日数据。
                        if time.time() - float(state["end_ts"]) > 45:
                            recent_end = time.time()
                            recent_start = max(day_start_ts(), float(state["end_ts"])-600)
                            recent = await reader(recent_start, recent_end)
                            writer(name, recent_start, recent_end, recent)
                            state = self.db.ledger_state(name, kind)
                        await asyncio.sleep(0)
                    needs_repair = kind == "fees" and name == "gate" and self.db.incomplete_fee_window(name) is not None
                    done = earliest >= float(state["start_ts"]) and not needs_repair
                    if done:
                        self.db.ledger_error(name, kind + "_history", "")
                    self._ledger_progress[progress_key] = {"phase": "已完成" if done else "补历史",
                        "label": self._ex_label(name)+label, "covered_from": ledger_time(state["start_ts"])}
                    if had_gap and done:
                        self.db.add_log("info", f"{self._ex_label(name)} 实际{label}历史同步完成。")
                except Exception as e:
                    message = f"{e.__class__.__name__}：{e}"
                    # 历史失败不抹掉已经同步成功的当日范围。
                    self.db.ledger_error(name, kind + "_history", message)
                    self._ledger_progress[progress_key] = {"phase": "历史同步失败", "label": self._ex_label(name)+label,
                                                           "error": message[:200]}
                    errors.append(self._ex_label(name) + "历史：" + message)
            if errors and time.time() - self._fee_sync_error_logged_at > 600:
                self._fee_sync_error_logged_at = time.time()
                self.db.add_log("warn", f"{label}实际账单同步未完成，保留原值并标为待核对：{'；'.join(errors)}")

    async def _fee_sync_loop(self):
        """每分钟同步一次今日成交手续费；失败不重试风暴，10 分钟内只提示一次。"""
        await asyncio.sleep(1)
        while self.running:
            try:
                await self._sync_today_fee_events()
            except Exception as e:
                now = time.time()
                if now - self._fee_sync_error_logged_at > 600:
                    self._fee_sync_error_logged_at = now
                    self.db.add_log(
                        "warn",
                        f"今日手续费流水同步失败（{e.__class__.__name__}）：{e}；稍后自动重试。")
            try:
                await self._recover_orphan_closed_positions()
            except Exception as e:
                if time.time() - self._orphan_history_error_at > 600:
                    self._orphan_history_error_at = time.time()
                    self.db.add_log("warn", f"缺失历史补录暂未完成：{e}；保留原记录，等待实际数据核对。")
            pending_history = any(k.endswith(":fees") and v.get("phase") == "补历史" for k,v in self._ledger_progress.items())
            await asyncio.sleep(1 if pending_history else 60)

    # ==================== 资金费核算 ====================

    async def _public_json(self, path, params):
        """调用 Bitget 公开接口（资金费率、K线），失败返回 None"""
        if httpx is None:
            return None
        if self._funding_client is None:
            self._funding_client = httpx.AsyncClient(base_url=BITGET_BASE, timeout=8)
        try:
            resp = await self._funding_client.get(path, params=params)
            payload = resp.json()
        except Exception:
            return None
        if str(payload.get("code", "")) not in ("00000", "0"):
            return None
        return payload.get("data")

    async def _fetch_funding_events(self, symbol, since_ts, now_ts, exchange=None):
        """获取某交易对在 (since_ts, now_ts] 之间的资金费结算记录"""
        if exchange is not None and hasattr(exchange, "fetch_funding_events"):
            return await exchange.fetch_funding_events(symbol, since_ts, now_ts)
        events = []
        for page in range(1, 6):
            data = await self._public_json(
                "/api/v2/mix/market/history-fund-rate",
                {"symbol": symbol, "productType": PRODUCT_TYPE,
                 "pageSize": "100", "pageNo": str(page)})
            items = data if isinstance(data, list) else []
            if not items:
                break
            oldest = None
            for it in items:
                try:
                    ts = int(it.get("fundingTime")) / 1000.0
                    rate = D(it.get("fundingRate") or "0")
                except Exception:
                    continue
                oldest = ts if oldest is None else min(oldest, ts)
                if since_ts < ts <= now_ts:
                    events.append({"ts": ts, "rate": rate})
            if oldest is None or oldest <= since_ts:
                break
        events.sort(key=lambda e: e["ts"])
        return events

    async def _funding_price(self, symbol, funding_ts, exchange=None):
        """取资金费结算时刻的收盘价（用 1 分钟 K 线），取不到返回 None"""
        if exchange is not None and hasattr(exchange, "fetch_funding_price"):
            return await exchange.fetch_funding_price(symbol, funding_ts)
        key = (symbol, int(funding_ts))
        if key in self._funding_price_cache:
            return self._funding_price_cache[key]
        price = None
        data = await self._public_json(
            "/api/v2/mix/market/history-candles",
            {"symbol": symbol, "productType": PRODUCT_TYPE, "granularity": "1m",
             "startTime": str(int(funding_ts * 1000) - 60000),
             "endTime": str(int(funding_ts * 1000) + 60000), "limit": "5"})
        if isinstance(data, list):
            best = None
            for row in data:
                try:
                    ts = float(row[0]) / 1000.0
                    close = D(row[4])
                except Exception:
                    continue
                if best is None or abs(ts - funding_ts) < abs(best[0] - funding_ts):
                    best = (ts, close)
            if best:
                price = best[1]
        self._funding_price_cache[key] = price
        if len(self._funding_price_cache) > 500:
            for k in list(self._funding_price_cache)[:200]:
                self._funding_price_cache.pop(k, None)
        return price

    async def apply_funding(self, sig, funding_ts, rate, price, qty=None):
        """结算一次资金费：正数=支付，负数=收取。幂等，重复结算时间不会重复记账。
        qty 可指定“结算当时的持仓数量”（马丁加仓前后不同）。"""
        try:
            qty = D(qty) if qty is not None else D(sig.get("qty") or "0")
            rate = D(rate)
            price = D(price)
        except Exception:
            return Decimal("0")
        if qty <= 0 or price <= 0:
            return Decimal("0")
        sign = Decimal("1") if sig["direction"] == "long" else Decimal("-1")
        amount = qty * price * rate * sign
        note = f"资金费（按 {self._ex_label(sig.get('exchange'))} 公开资金费率估算）"
        recorded = self.db.record_funding(
            sig["id"], funding_ts, money_str(amount), str(rate), str(price), note)
        if recorded:
            action = "支付" if amount > 0 else "收取"
            self.db.add_log("info",
                            f"资金费：{sig['symbol']} {action} {money_str(abs(amount))} USDT"
                            f"（费率 {pct_str(rate * 100)}%，结算价 {pct_str(price)}，"
                            f"数量 {qty}，{'多头' if sig['direction'] == 'long' else '空头'}）")
        return amount

    async def accrue_funding(self, sig_id=None):
        """把已到结算时间的资金费补齐。平仓前会单独调用一次，确保不漏算。"""
        self._ensure_exchange()
        if not self._is_sim() and any(callable(getattr(ex, "get_funding_bills", None))
                                      for ex in self._active_exchanges().values()):
            if sig_id:
                sig = self.db.get_signal(sig_id)
                if sig:
                    self.db.reconcile_actual_funding(str(sig.get("exchange") or "bitget"))
                return  # 平仓不等待历史账单网络请求，后台收到实际账单后再校准。
            sig = self.db.get_signal(sig_id) if sig_id else None
            await self._sync_account_ledger("funding", only_exchange=str((sig or {}).get("exchange") or ""))
            return
        if httpx is None:
            return
        async with self._funding_lock:
            if sig_id:
                one = self.db.get_signal(sig_id)
                positions = [one] if one else []
            else:
                positions = self.db.open_positions()
            now = time.time()
            for sig in positions:
                if not sig or sig.get("status") != ST_HOLDING:
                    continue
                symbol = sig["symbol"]
                try:
                    last = float(sig.get("last_funding_ts") or sig.get("open_ts") or 0)
                except Exception:
                    last = 0.0
                if last <= 0 or last >= now:
                    continue
                ex = self._ex(sig)
                try:
                    events = await self._fetch_funding_events(symbol, last, now, ex)
                except Exception:
                    events = []
                for ev in events:
                    price = await self._funding_price(symbol, ev["ts"], ex)
                    if price is None:
                        try:
                            price = await ex.fetch_mark_price(symbol)
                        except Exception:
                            price = None
                    if price is None or D(price) <= 0:
                        try:
                            price = D(sig.get("avg_entry") or sig.get("entry") or "0")
                        except Exception:
                            continue
                    qty_at = self.db.qty_at(sig["id"], ev["ts"]) or sig.get("qty")
                    await self.apply_funding(sig, ev["ts"], ev["rate"], D(price), qty=qty_at)

    async def funding_loop(self):
        """后台资金费监控：每 60 秒检查一次，到结算时间自动记账。
        即使停止接收信号，只要还有持仓，资金费仍会继续累计。"""
        await asyncio.sleep(3)
        while True:
            try:
                await self.accrue_funding()
                if not self.running and not self._is_sim():
                    await self._sync_today_fee_events()
                    await self._recover_orphan_closed_positions()
            except Exception as e:
                now = time.time()
                if now - self._funding_error_logged_at > 1800:
                    self._funding_error_logged_at = now
                    self.db.add_log("warn", f"资金费核算暂时失败（{e.__class__.__name__}）：{e}")
            await asyncio.sleep(60)

    # ==================== 标记价刷新（当前持仓的浮动盈亏） ====================

    async def refresh_marks(self):
        """兼容旧接口：触发一次后台刷新（不等待结果，避免拖慢网页）"""
        self.trigger_mark_refresh()

    def trigger_mark_refresh(self):
        """为当前持仓安排后台刷新标记价（5 秒缓存）。
        注意：这里绝不等待网络请求，页面接口立即返回，Bitget 慢的时候网页也不会卡。"""
        try:
            self._ensure_exchange()
            # 挂单中的交易对也要取价：网页“挂单中”要显示标记价，方便对比限价
            positions = self.db.open_positions() + self.db.pending_entries()
        except Exception:
            return
        now = time.time()
        stale = []
        for p in positions:
            sym = p["symbol"]
            hit = self._cached_mark(p)
            if hit and now - hit[0] < 5:
                continue
            if sym not in stale:
                stale.append(sym)
        if not stale:
            return
        task = self._mark_batch_task
        if task is not None and not task.done():
            return
        self._mark_batch_task = asyncio.create_task(self._fetch_marks(stale))

    async def ensure_marks(self, timeout=1.5):
        """页面刚打开且本进程还没取过任何标记价时，最多等一小会儿批量取价，
        让当前持仓的标记价/浮盈亏一次显示完整（重启后不再先显示一堆“-”）。
        只等“从未取过”的币种；取过但失败的不会反复拖慢轮询。"""
        try:
            positions = self.db.open_positions() + self.db.pending_entries()
        except Exception:
            return
        cold = []
        for p in positions:
            sym = p["symbol"]
            if self._cached_mark(p) is None and sym not in cold:
                cold.append(sym)
        if not cold:
            return
        now = time.time()
        if now < self._mark_wait_until:
            return
        self._mark_wait_until = now + 6   # 6 秒内最多阻塞一次，避免每个轮询都等
        self.trigger_mark_refresh()
        task = self._mark_batch_task
        if task is None:
            return
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        except Exception:
            pass

    def _cached_mark(self, sig):
        name = str(sig.get("exchange") or "bitget")
        sym = sig.get("symbol")
        hit = self.mark_cache.get((name, sym))
        if hit is None and (self._is_sim() or len(self._active_exchanges()) <= 1):
            hit = self.mark_cache.get(sym)
        return hit if hit and hit[1] is not None and time.time() - hit[0] <= 30 else None

    async def _fetch_marks(self, symbols):
        """优先批量获取标记价，个别标的缺失时才逐项回退，减少请求次数。"""
        symbols = list(dict.fromkeys(str(s) for s in symbols if s))
        if not symbols:
            return
        for name, ex in self._active_exchanges().items():
            got = {}
            batch = getattr(ex, "fetch_mark_prices", None)
            if callable(batch):
                try:
                    got = await asyncio.wait_for(batch(symbols), timeout=4) or {}
                except Exception:
                    pass
            missing = []
            for sym in symbols:
                value = got.get(sym)
                if value is not None and D(value).is_finite() and D(value) > 0:
                    self.mark_cache[(name, sym)] = (time.time(), D(value))
                    if len(self._active_exchanges()) == 1:
                        self.mark_cache[sym] = self.mark_cache[(name, sym)]
                else:
                    missing.append(sym)
            sem = asyncio.Semaphore(4)
            async def one(sym):
                async with sem:
                    await self._fetch_mark(sym, name)
            await asyncio.gather(*(one(s) for s in missing), return_exceptions=True)

    async def _fetch_mark(self, sym, exchange_name=""):
        mark = None
        seen_ex = set()
        for _name, ex in self._active_exchanges().items():
            if exchange_name and _name != exchange_name:
                continue
            if id(ex) in seen_ex:
                continue
            seen_ex.add(id(ex))
            try:
                mark = await asyncio.wait_for(ex.fetch_mark_price(sym), timeout=4)
            except Exception:
                mark = None
            if mark is not None and D(mark).is_finite() and D(mark) > 0:
                self.mark_cache[(_name, sym)] = (time.time(), D(mark))
                break
        now = time.time()
        if mark is not None and (self._is_sim() or len(self._active_exchanges()) == 1):
            self.mark_cache[sym] = (now, mark)

    async def account_reconcile(self, exchange=""):
        """只读账本核对：按交易所分别用实时权益倒推起始资金；exchange 为空时返回合计。
        账户权益 = 起始权益 + 已实现净盈亏 + (未实现毛盈亏 − 开仓手续费 − 资金费)。
        倒推值接近参数里的账户总资金，说明该交易所的账本与交易所一致。"""
        self._ensure_exchange()
        if self._is_sim():
            return {"ok": False, "sim_mode": True,
                    "error": "模拟盘没有真实账户权益；账本核对只在实盘可用。"}
        names = [exchange] if exchange else list(self._active_exchanges().keys())
        per, errors = [], []
        total = {"equity": Decimal("0"), "available": Decimal("0"),
                 "margin": Decimal("0"), "gross": Decimal("0"),
                 "realized": Decimal("0"), "open_fees": Decimal("0"),
                 "open_funding": Decimal("0"), "net_unrealized": Decimal("0"),
                 "change": Decimal("0"), "derived_start": Decimal("0"),
                 "configured": Decimal("0"), "open_count": 0}
        for name in names:
            adapter = self._active_exchanges().get(name)
            label = self._ex_label(name)
            if adapter is None:
                errors.append(f"{label}未配置")
                continue
            try:
                data = await asyncio.wait_for(adapter.fetch_account_summary(), timeout=12)
            except Exception as e:
                errors.append(f"读取 {label} 账户失败：{e}")
                continue
            if not data.get("equity"):
                errors.append(f"{label} 没有返回账户权益，请检查 API Key 是否有合约账户只读权限")
                continue
            try:
                equity = D(data.get("equity") or "0")
                gross = D(data.get("unrealized_pl") or "0")
                available = D(data.get("available") or "0")
                margin = D(data.get("margin") or "0") if data.get("margin") else Decimal("0")
            except Exception:
                errors.append(f"{label} 账户返回的权益/可用余额无法解析")
                continue
            try:
                realized = self.db.total_realized_pnl(name)
            except Exception:
                realized = Decimal("0")
            open_fees = open_funding = Decimal("0")
            try:
                positions = self.db.open_positions(name)
            except Exception:
                positions = []
            for s in positions:
                try:
                    open_fees += D(s.get("open_fee") or "0")
                except Exception:
                    pass
                try:
                    open_funding += D(s.get("funding_fee") or "0")
                except Exception:
                    pass
            net_unrealized = gross - open_fees - open_funding
            change = realized + net_unrealized
            derived_start = equity - change
            try:
                configured = self.params.effective_total_funds(name)
            except Exception:
                configured = Decimal("0")
            diff = derived_start - configured
            status = ("ok" if abs(diff) <= Decimal("1")
                      else ("small" if abs(diff) <= Decimal("3") else "check"))
            per.append({
                "ok": True, "sim_mode": False, "exchange_name": name, "label": label,
                "exchange": {
                    "equity": money_str(equity), "available": money_str(available),
                    "margin": money_str(margin) if data.get("margin") else "",
                    "unrealized_pl": money_str(gross),
                },
                "ledger": {
                    "realized": money_str(realized), "open_fees": money_str(open_fees),
                    "open_funding": money_str(open_funding),
                    "unrealized_net": money_str(net_unrealized),
                    "unrealized_verified": self.db.net_quality(name, "holding")["verified"],
                    "open_count": len(positions),
                },
                "configured_start": money_str(configured),
                "ledger_equity_change": money_str(change),
                "derived_start": money_str(derived_start),
                "diff_from_configured": money_str(diff),
                "status": status,
            })
            total["equity"] += equity
            total["available"] += available
            total["margin"] += margin
            total["gross"] += gross
            total["realized"] += realized
            total["open_fees"] += open_fees
            total["open_funding"] += open_funding
            total["net_unrealized"] += net_unrealized
            total["change"] += change
            total["derived_start"] += derived_start
            total["configured"] += configured
            total["open_count"] += len(positions)
        if exchange and per:
            out = dict(per[0])
            if errors:
                out["errors"] = errors
            return out
        if not per:
            return {"ok": False, "sim_mode": False,
                    "error": "；".join(errors) or "没有可用的交易所账户"}
        diff = total["derived_start"] - total["configured"]
        status = ("ok" if abs(diff) <= Decimal("1")
                  else ("small" if abs(diff) <= Decimal("3") else "check"))
        return {
            "ok": not errors, "sim_mode": False, "by_exchange": per, "errors": errors,
            "complete": not errors, "statistics": self.snapshot(), "as_of": now_str(),
            "exchange": {
                "equity": money_str(sum((D(x["exchange"]["equity"]) for x in per), Decimal("0"))) if not errors else "",
                "available": money_str(sum((D(x["exchange"]["available"]) for x in per), Decimal("0"))) if not errors else "",
                "margin": money_str(total["margin"]),
                "unrealized_pl": money_str(total["gross"]) if not errors else "",
            },
            "ledger": {
                "realized": money_str(total["realized"]),
                "open_fees": money_str(total["open_fees"]),
                "open_funding": money_str(total["open_funding"]),
                "unrealized_net": money_str(sum((D(x["ledger"]["unrealized_net"]) for x in per), Decimal("0"))) if not errors else "",
                "unrealized_verified": not errors and all(x["ledger"]["unrealized_verified"] for x in per),
                "open_count": total["open_count"],
            },
            "configured_start": money_str(total["configured"]),
            "ledger_equity_change": money_str(total["change"]),
            "derived_start": money_str(total["derived_start"]),
            "diff_from_configured": money_str(diff),
            "status": status,
        }

    # ==================== 状态快照（网页每 2 秒拉取） ====================

    def _pending_view(self, sig):
        """挂单中：补上当前标记价，方便和限价对比（标记价来自 mark_cache，5 秒缓存）"""
        mark = None
        hit = self._cached_mark(sig)
        if hit:
            mark = hit[1]
        return {
            "id": sig.get("id"), "symbol": sig.get("symbol"),
            "exchange": sig.get("exchange") or "bitget",
            "exchange_label": self._ex_label(sig.get("exchange")),
            "direction": sig.get("direction"), "source": sig.get("source") or "",
            "qty": sig.get("pending_open_qty") or sig.get("qty") or "",
            "price": sig.get("pending_open_price") or "",
            "mark_price": pct_str(mark) if mark is not None else "",
            "order_id": sig.get("pending_open_order_id") or "",
            "kind": "开仓",
            "open_time": sig.get("open_time") or "",
            "stop": sig.get("stop") or "", "target": sig.get("target") or "",
        }

    def _pending_add_view(self, sig):
        """马丁加仓的限价挂单（挂在持仓记录上，不是独立信号）。首次开仓已改市价，
        所以“挂单中”以后主要显示这类加仓单。时间用下单时记录的 pending_add_ts，
        老记录没有时间就留空显示“-”，不拿持仓开仓时间去冒充。"""
        mark = None
        hit = self._cached_mark(sig)
        if hit:
            mark = hit[1]
        when = ""
        try:
            ts = float(sig.get("pending_add_ts") or 0)
            if ts > 0:
                when = ledger_time(ts)
        except Exception:
            when = ""
        return {
            "id": sig.get("id"), "symbol": sig.get("symbol"),
            "exchange": sig.get("exchange") or "bitget",
            "exchange_label": self._ex_label(sig.get("exchange")),
            "direction": sig.get("direction"), "source": sig.get("source") or "",
            "qty": sig.get("pending_add_qty") or "",
            "price": sig.get("pending_add_price") or "",
            "mark_price": pct_str(mark) if mark is not None else "",
            "order_id": sig.get("pending_add_order_id") or "",
            "kind": "加仓",
            "layers": sig.get("martin_layers") or 0,
            "open_time": when,
        }

    def _position_view(self, sig):
        """给当前持仓补充：标记价、层数、未实现盈亏（已扣累计开仓手续费）"""
        pos = dict(sig)
        pos["accounting_verified"] = (self._is_sim() or
            (sig.get("fee_source") == "exchange" and sig.get("funding_source") == "exchange"))
        pos["exchange_label"] = self._ex_label(sig.get("exchange"))
        mark = None
        hit = self._cached_mark(sig)
        if hit:
            mark = hit[1]
        pos["mark_price"] = pct_str(mark) if mark is not None else ""
        try:
            qty = D(sig.get("qty") or "0")
            avg_entry = D(sig.get("avg_entry") or sig.get("entry") or "0")
            open_fee = D(sig.get("open_fee") or "0")
            funding_fee = D(sig.get("funding_fee") or "0")
            base_notional = D(sig.get("notional") or "0")
            pos["open_fee"] = money_str(open_fee)
            pos["funding_fee"] = money_str(funding_fee)
            pos["funding_note"] = sig.get("funding_note") or ""
            pos["avg_entry"] = pct_str(avg_entry)
            pos["liquidation_price"] = str(sig.get("liquidation_price") or "")
            pos["martin_layers"] = int(sig.get("martin_layers") or 0)
            pos["martin_multiplier"] = str(sig.get("martin_multiplier") or "1")
            # 页面展示的倍数按「名义价值（均价×数量）÷ 基础名义价值」计算

            try:
                if qty > 0 and base_notional > 0:
                    if avg_entry > 0:
                        value_mult = qty * avg_entry / base_notional
                    elif mark is not None:
                        value_mult = qty * mark / base_notional
                    else:
                        value_mult = D(sig.get("martin_multiplier") or "1")
                    pos["value_multiplier"] = format(value_mult.quantize(Decimal("0.0001")), "f")
                else:
                    pos["value_multiplier"] = pos["martin_multiplier"]
            except Exception:
                pos["value_multiplier"] = pos["martin_multiplier"]
            if mark is not None and qty > 0:
                sign = Decimal("1") if sig["direction"] == "long" else Decimal("-1")
                upnl = qty * (mark - avg_entry) * sign - open_fee - funding_fee
                pos["unrealized_pnl"] = money_str(upnl)
            else:
                pos["unrealized_pnl"] = ""
        except Exception:
            pos["open_fee"] = sig.get("open_fee", "")
            pos["unrealized_pnl"] = ""
        return pos

    @staticmethod
    def _platform_tracking_view(s):
        """影子跟踪（平台仍跟踪，我们已平仓）的页面字段"""
        return {
            "id": s.get("id"), "symbol": s.get("symbol"), "direction": s.get("direction"),
            "exchange": s.get("exchange") or "bitget",
            "source": s.get("source") or "", "our_net": s.get("net_pnl") or "",
            "net_pnl": s.get("net_pnl") or "", "qty": s.get("qty") or "",
            "avg_entry": s.get("avg_entry") or "", "notional": s.get("notional") or "",
            "our_close_time": s.get("close_time") or "",
            "platform_stop": s.get("platform_stop") or "",
            "platform_target": s.get("platform_target") or "",
            "platform_update_time": s.get("platform_update_time") or "",
        }

    @staticmethod
    def _platform_ended_view(s):
        """影子已结束（平台已发结束，我们此前已平仓）的页面字段"""
        return {
            "id": s.get("id"), "symbol": s.get("symbol"), "direction": s.get("direction"),
            "exchange": s.get("exchange") or "bitget",
            "source": s.get("source") or "", "our_net": s.get("net_pnl") or "",
            "net_pnl": s.get("net_pnl") or "", "qty": s.get("qty") or "",
            "avg_entry": s.get("avg_entry") or "", "notional": s.get("notional") or "",
            "our_close_time": s.get("close_time") or "",
            "platform_end_reason": s.get("platform_end_reason") or "",
            "platform_end_text": s.get("platform_end_text") or "",
            "platform_end_time": s.get("platform_end_time") or "",
            "platform_final_price": s.get("platform_final_price") or "",
            "platform_profit_percent": s.get("platform_profit_percent") or "",
        }

    async def _platform_estimate_signals(self):
        """用平台最后均价 + Bitget 实时标记价计算平台跟踪仓位的收益%。"""
        try:
            self._ensure_exchange()
        except Exception:
            return {}
        shadows = self.db.platform_shadows(100)
        if not shadows:
            return {}

        async def one(s):
            try:
                mark = await self._ex(s).fetch_mark_price(s["symbol"])
            except Exception:
                mark = None
            if mark is None:
                return None
            try:
                avg = D(s.get("platform_avg") or s.get("avg_entry") or s.get("entry") or "0")
            except Exception:
                avg = Decimal("0")
            if avg <= 0:
                return None
            sign = Decimal("1") if s.get("direction") == "long" else Decimal("-1")
            pct = (mark - avg) / avg * Decimal("100") * sign
            return s["id"], {
                "percent": float(pct),
                "price": float(mark),
                "avg": float(avg),
            }

        results = await asyncio.gather(*[one(s) for s in shadows], return_exceptions=True)
        signals = {}
        for r in results:
            if isinstance(r, Exception) or not r:
                continue
            sid, info = r
            signals[sid] = info
        return signals

    async def platform_active_snapshot(self):
        """平台跟踪中仓位的实时收益%：直接用平台最后均价 + Bitget 实时标记价计算。
        不再请求平台 HTTP 接口（现有 Key 无权限）；平台是否结束以平台推送的 ended/回执为准。"""
        now = time.time()
        if self._platform_active_cache and now - self._platform_active_cache[0] < 15:
            return self._platform_active_cache[1]
        try:
            signals = await self._platform_estimate_signals()
        except Exception:
            signals = {}
        if signals:
            out = {"ok": True, "signals": signals, "source": "exchanges", "updated": now_str()}
        else:
            out = {"ok": False, "error": "没有可计算的平台跟踪仓位，或取不到交易所实时价", "signals": {}}
        self._platform_active_cache = (now, out)
        return out

    def snapshot(self):
        with self.db.dashboard_read():
            return self._dashboard_snapshot()

    def _dashboard_snapshot(self):
        self.db.active_exchange_names = list(self._active_exchanges())
        stats = self.db.today_stats()
        try:
            funds = self._combined_total_funds()
        except Exception:
            funds = Decimal("0")
        pnl = stats["realized_pnl"]
        loss = -pnl if pnl < 0 else Decimal("0")
        p = self.params.all()
        fused = bool(p["fuse_enabled"] and funds > 0 and loss >= funds * FUSE_RATIO)
        open_rows = self.db.open_positions()
        positions = [self._position_view(s) for s in open_rows]
        complete_marks = all(p.get("unrealized_pnl") not in (None, "") for p in positions)
        unrealized = Decimal("0")
        for pos in positions:
            value = pos.get("unrealized_pnl")
            if value not in (None, ""):
                try:
                    unrealized += D(value)
                except Exception:
                    pass
        try:
            total_realized = self.db.total_realized_pnl()
        except Exception:
            total_realized = Decimal("0")
        quality = ({"verified": True, "message": "模拟数据", "issues": []} if self._is_sim()
                   else self.db.accounting_quality(self._active_exchanges().keys()))
        quality["sync_progress"] = dict(self._ledger_progress)
        progress_text = [v["label"]+"："+v["phase"]+("（已补至 "+v["covered_from"]+"）" if v.get("covered_from") else "")
                         for v in self._ledger_progress.values() if v.get("phase") != "已完成"]
        if progress_text:
            quality["message"] = "；".join(progress_text + quality.get("issues", []))
        if not complete_marks:
            quality["verified"] = False
            quality["issues"].append("部分持仓缺少有效的本交易所标记价")
            quality["message"] = "；".join(quality["issues"])
        return {
            "running": self.running,
            "data_version": "update116", "as_of": now_str(),
            "data_revision": self.db.data_revision(),
            "data_quality": quality,
            "started_at": self.started_at,
            "connected": self.connected,
            "fatal": self.fatal,
            "fused": fused,
            "fuse_enabled": p["fuse_enabled"],
            "balance": self.balance,
            "last_msg_time": self.last_msg_time,
            "last_disconnect_at": self.db.get_setting("last_disconnect_at", ""),
            "last_disconnect_seconds": self.db.get_setting("last_disconnect_seconds", ""),
            "last_disconnect_code": self.db.get_setting("last_disconnect_code", ""),
            "sim_mode": p["sim_mode"],
            "sim_locked": os.environ.get("TRADE_MODE", "").strip().lower() == "live",
            "real_mode_ready": bool(
                (p["exchange_api_key"] and p["exchange_secret"] and p["exchange_passphrase"])
                or (p.get("gate_enabled") and p.get("gate_api_key") and p.get("gate_secret"))),
            "exchanges": [{"name": name, "label": self._ex_label(name)}
                          for name in self._active_exchanges()],
            "by_exchange": [
                {"name": name, "label": self._ex_label(name),
                 "today": (lambda st: {
                     **st,
                     "realized_pnl": money_str(st["realized_pnl"]),
                     "fees": money_str(st["fees"]),
                     "funding": money_str(st["funding"]),
                     "net_pnl": money_str(st["net_pnl"]),
                 })(self.db.today_stats(name)),
                 "totals": self.db.totals(name),
                 "unrealized_verified": self._is_sim() or self.db.net_quality(name, "holding")["verified"],
                 "realized_verified": self._is_sim() or self.db.net_quality(name, "closed")["verified"],
                 "realized": (money_str(self.db.total_realized_pnl(name))
                              if not self.db.accounting_quality([name])["missing_net"] else ""),
                 "funds": pct_str(self.params.effective_total_funds(name))}
                for name in self._active_exchanges()
            ],
            "today": {
                "opened": stats["opened"],
                "closed": stats["closed"],
                "skipped": stats["skipped"],
                "skip_reasons": stats["skip_reasons"],
                "realized_pnl": money_str(stats["realized_pnl"]),
                "fees": money_str(stats["fees"]),
                "funding": money_str(stats["funding"]),
                "fees_verified": stats["fees_verified"],
                "funding_verified": stats["funding_verified"],
                "net_verified": self._is_sim() or stats["net_verified"],
                "net_pnl": money_str(stats["net_pnl"]),
                "loss": money_str(loss),
                "funds": pct_str(funds),
            },
            "unrealized_pnl": money_str(unrealized) if complete_marks else "",
            "unrealized_complete": complete_marks,
            "unrealized_verified": self._is_sim() or self.db.net_quality("", "holding")["verified"],
            "total_realized_pnl": money_str(total_realized) if not quality.get("missing_net") else "",
            "realized_verified": self._is_sim() or self.db.net_quality("", "closed")["verified"],
            "totals": self.db.totals(),
            "positions": positions,
            "pending_entries": (
                [self._pending_view(s) for s in self.db.pending_entries()]
                + [self._pending_add_view(p) for p in open_rows
                   if str(p.get("pending_add_order_id") or "")]),
            "platform_tracking": [self._platform_tracking_view(s)
                                  for s in self.db.platform_shadows(100)],
            "platform_ended": [self._platform_ended_view(s)
                               for s in self.db.platform_ended(100)],
            "alerts": self.db.get_alerts(30),
        }

    def history(self, limit=100, source="", symbol="", exchange=""):
        return self.db.closed_history(limit, source, symbol, exchange)
