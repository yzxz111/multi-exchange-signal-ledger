# -*- coding: utf-8 -*-
"""
信号 —— 核心模块（Bitget + Gate 版）
负责：参数存取、SQLite 数据库、Bitget/Gate USDT 本位合约对接、Decimal 精度计算。

本文件不直接运行，由公开演示入口和隔离测试调用。

交易所：Bitget USDT 本位永续合约（单向持仓模式）。
依赖：fastapi / uvicorn / websockets / httpx 由启动脚本自动安装；
      websockets / httpx 用 try 导入，即使没装，「自检.py」仍能运行（只测本地逻辑，不联网）。
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import re
import sqlite3
import threading
import time
import urllib.parse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from functools import wraps
from pathlib import Path

try:
    import httpx
except ImportError:
    httpx = None  # 没装也不报错；真实连接交易所时才需要

# ==================== 常量 ====================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("SIGNAL_LEDGER_DATA_DIR", str(BASE_DIR / ".demo-data")))                      # 数据库、原始信号记录都放这里
DB_PATH = DATA_DIR / "console.db"                 # SQLite 数据库文件
JSONL_PATH = DATA_DIR / "signals.jsonl"           # 所有原始消息的追加记录（排查用）
TG_JSONL_PATH = DATA_DIR / "telegram.jsonl"        # Telegram Webhook 中继消息的原始记录（解析排查用）

WS_URL_TEMPLATE = "wss://signal-provider.example.invalid/events?key={key}"   # 信号服务器地址
BITGET_BASE = "https://api.bitget.com"                            # Bitget REST 地址
GATE_BASE = "https://api.gateio.ws"                               # Gate REST 地址
GATE_SETTLE = "usdt"                                              # Gate U 本位永续
GATE_API_PREFIX = "/api/v4"                                       # Gate API v4 前缀
PRODUCT_TYPE = "USDT-FUTURES"                                     # U 本位永续合约
MARGIN_COIN = "USDT"
DEFAULT_TAKER_FEE = Decimal("0.0006")                             # 取不到费率时的兜底：0.06%
GATE_DEFAULT_TAKER_FEE = Decimal("0.00075")                       # Gate 接口取不到时的兜底：0.075%

# 信号来源选项（与 外部信号服务 平台一致）
ALL_SOURCES = ["趋势信号", "波动信号", "事件信号", "成交量信号", "费率信号", "资金流信号"]
# 方向选项：服务器值 -> 中文显示名
DIRECTION_NAMES = {"long": "做多", "short": "做空"}

# 单日亏损熔断比例：当日累计亏损达到 账户总资金 × 3% 时，停止开新仓
FUSE_RATIO = Decimal("0.03")

# 仓位的各种状态（写入数据库 status 字段）
ST_HOLDING = "持仓中"     # 已开仓，仍在跟踪
ST_CLOSED = "已平仓"      # 已正常平仓
ST_SKIPPED = "已跳过"     # 没开仓（原因见 skip_reason 字段）
ST_UNKNOWN = "待核对"     # 下单/平仓结果未知（网络超时），必须人工核对，绝不盲目重试
ST_DROPPED = "已放弃"     # 下单被交易所明确拒绝
ST_PENDING = "挂单中"     # 限价开仓单已挂出，等待成交

# 跳过原因（写入 skip_reason 字段，网页上按此分类统计）
SKIP_DUP = "重复信号"
SKIP_FILTER = "来源或方向被过滤"
SKIP_FUSED = "当日亏损熔断中"
SKIP_DAILY = "超过每日开仓上限"
SKIP_MAXPOS = "超过最大持仓数"
SKIP_SAME_SYMBOL = "同一交易对已有持仓"
SKIP_NOPRICE = "无法获取标记价"
SKIP_DEVIATION = "入场偏离过大"
SKIP_TOOSMALL = "数量或名义价值不达标"
SKIP_ORDERFAIL = "下单失败"
SKIP_POSITION_MODE = "账户不是单向持仓模式"
SKIP_NO_BITGET = "Bitget 没有该交易对"
SKIP_NO_CONTRACT = "交易所没有该交易对"

# Bitget 常见错误码 -> 说人话的中文解释
BITGET_ERR_MSG = {
    "40001": "请求参数错误",
    "40002": "请求时间戳不正确，请检查电脑时间是否准确",
    "40003": "API Key 无效，请检查是否填写正确",
    "40004": "API 签名错误，请检查 Secret 是否填写正确",
    "40005": "Passphrase（API 口令）错误，请检查后在参数区修改",
    "40006": "请求太频繁，被交易所限流",
    "40008": "IP 不在 API Key 白名单内",
    "40009": "签名校验失败（检查服务器时间，或中文参数签名编码）",
    "40012": "账户可用余额不足",
    "40762": "下单数量精度不符合交易所要求",
    "43011": "仓位不存在（可能已被止损单平掉）",
    "43012": "仓位不足",
}


def bitget_friendly(code, msg):
    """把 Bitget 的英文报错翻译成人话"""
    code = str(code)
    if code in BITGET_ERR_MSG:
        return BITGET_ERR_MSG[code]
    low = (msg or "").lower()
    if "signature" in low:
        return "API 签名错误，请检查 Secret 是否填写正确"
    if "passphrase" in low:
        return "Passphrase（API 口令）错误，请检查"
    if "apikey" in low or "api key" in low:
        return "API Key 无效，请检查"
    if "permission" in low:
        return "API Key 权限不足，请检查是否开通合约交易权限"
    if "margin" in low and "insufficient" in low:
        return "保证金不足，请检查账户余额"
    if "balance" in low or "insufficient" in low:
        return "余额不足"
    if "position" in low and ("not exist" in low or "no position" in low or "insufficient" in low):
        return "仓位不存在或已平仓"
    if "precision" in low or "decimal" in low:
        return "下单精度不符合交易所要求"
    return msg or "交易所返回未知错误"


def normalize_symbol_filter(symbol):
    """清理网页输入的币种查找词，保留未输入完整的前缀。"""
    return re.sub(r"\s+", "", str(symbol or "")).upper()


def add_symbol_filter(sql, params, symbol):
    """给历史查询增加币种条件：完整交易对精确匹配，未输完时按开头匹配。"""
    symbol = normalize_symbol_filter(symbol)
    if not symbol:
        return sql, params
    column = "UPPER(REPLACE(symbol, ' ', ''))"
    if symbol.endswith(MARGIN_COIN):
        sql += f" AND {column}=?"
        params.append(symbol)
    else:
        sql += f" AND SUBSTR({column},1,LENGTH(?))=?"
        params.extend((symbol, symbol))
    return sql, params


# ==================== 参数（网页里可填的那些） ====================

PARAM_DEFAULTS = {
    "signal_api_key": "",            # 信号平台 API Key
    "exchange_api_key": "",          # Bitget API Key（留空 = 不启用 Bitget）
    "exchange_secret": "",           # Bitget Secret Key（留空 = 不启用 Bitget）
    "exchange_passphrase": "",       # Bitget Passphrase / API 口令（留空 = 不启用 Bitget）
    "gate_api_key": "",              # Gate API Key（v4，合约权限；无 Passphrase）
    "gate_secret": "",               # Gate Secret Key
    "gate_enabled": "1",             # 1 = 启用 Gate；0 = 只用 Bitget
    "new_entry_route": "gate_first", # 新开仓路由：gate_first=Gate 优先、缺的走 Bitget；gate_only；bitget_first；bitget_only
    "gate_total_funds": "",          # Gate 账户总资金（用于 Gate 收益率；留空回退“账户总资金”）
    "notional": "100",               # 每信号名义价值 USDT（固定头寸规模）
    "head_multiplier": "1",          # 头仓仓位倍数：平台网页显示 0.5X 就填 0.5（只影响之后的新开仓）
    "leverage": "10",                # 杠杆倍数（只影响保证金，不影响数量）
    "margin_mode": "crossed",        # 新开仓保证金模式：crossed=全仓，isolated=逐仓
    "max_entry_deviation_pct": "0.5",# 最大入场偏离 %
    "max_positions": "0",            # 最大同时持仓数（0 = 不限制，2026-11-11 用户要求放开）
    "max_daily_opens": "0",          # 每日最大开仓次数（0 = 不限制，2026-11-11 用户要求放开）
    "total_funds": "",               # 账户总资金 USDT（留空时按公式自动算）
    "sources": "",                   # 来源筛选，逗号分隔；空 = 全部
    "directions": "",                # 方向筛选，逗号分隔 long/short；空 = 全部
    "telegram_token": "",            # Telegram Bot Token（平台 Webhook → 群 → 程序只读轮询）
    "telegram_chat_id": "",          # Telegram 群 Chat ID（负数，如 -1000000000000）
    "sim_mode": "1",                 # 模拟盘开关：1 = 开（默认），0 = 关（真实下单）
    "fuse_enabled": "1",             # 单日亏损熔断开关：1 = 启用（默认），0 = 关闭
    "local_stop_enabled": "1",       # 本地交易所止损：1 = 挂止损（默认安全），0 = 只听平台结束信号
}


def default_total_funds(max_positions, notional, leverage):
    """账户总资金的自动默认值：最大同时持仓数 × (名义价值 ÷ 杠杆) × 3
    最大同时持仓数填 0（不限制）时这个公式没有意义，返回 0，网页提示用户手动填写。"""
    try:
        mp = int(str(max_positions).strip() or 0)
        if mp <= 0:
            return Decimal("0")
        return (Decimal(mp) * (Decimal(notional) / Decimal(leverage)) * 3)
    except Exception:
        return Decimal("0")


# ==================== 精度计算（订单精度必须用 Decimal，绝不能用 float） ====================

def D(value, default=None):
    """把任何值安全地转成 Decimal（字符串直达，不经过 float，避免精度丢失）"""
    if value is None or value == "":
        if default is not None:
            return D(default)
        raise InvalidOperation
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(str(value))          # float 先用字符串表示，再转 Decimal
    return Decimal(str(value).strip())


def floor_to_step(value, step):
    """按 stepSize 向下取整（绝不允许向上取整，否则数量会被交易所拒绝）"""
    v, s = D(value), D(step)
    if s <= 0:
        return v
    return (v / s).to_integral_value(rounding=ROUND_DOWN) * s


def quantize(value, tick):
    """价格按 tickSize 取整（止损价挂单用）"""
    v, t = D(value), D(tick)
    if t <= 0:
        return v
    return (v / t).to_integral_value(rounding=ROUND_DOWN) * t


def pct_str(num):
    """Decimal 转成干净的字符串（去掉多余的 0）"""
    return format(D(num).normalize(), "f")


def money_str(value):
    """金额统一保留 4 位小数显示（0.0001 USDT），用于手续费 / 盈亏"""
    try:
        if value in (None, ""):
            return ""
        num = D(value)
        if not num.is_finite():
            return ""
        return format(num.quantize(Decimal("0.0001")), "f")
    except Exception:
        return ""


def now_str():
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def today_str():
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")


def time_str():
    return datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")


def ledger_time(ts):
    return datetime.fromtimestamp(float(ts), timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def day_start_ts():
    now = datetime.now(timezone(timedelta(hours=8)))
    return now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


def amount_str(value):
    """账本保留交易所原始精度，仅在页面展示时舍入。"""
    value = D(value)
    if not value.is_finite():
        raise ValueError("账本金额不是有限数值")
    return format(value, "f")


def signal_net(row):
    """全站共用同一净盈亏口径，旧记录缺净值时按已有费用补算。"""
    r = dict(row)
    if r.get("record_source") == "exchange_recovered":
        if r.get("fee_source") != "exchange" or r.get("funding_source") != "exchange":
            return None
        try:
            value = D(r["net_pnl"]) if r.get("net_pnl") not in (None, "") else None
            return value if value is not None and value.is_finite() else None
        except Exception:
            return None
    try:
        if r.get("gross_pnl") not in (None, ""):
            value = (D(r["gross_pnl"]) - D(r.get("open_fee") or "0")
                     - D(r.get("close_fee") or "0") - D(r.get("funding_fee") or "0"))
            return value if value.is_finite() else None
        if r.get("net_pnl") not in (None, ""):
            value = D(r["net_pnl"])
            return value if value.is_finite() else None
    except Exception:
        pass
    return None


def parse_bitget_closed_position(row):
    """实际历史持仓原值：费用转为正成本，净盈利保留交易所netProfit，不用相加值覆盖。"""
    required = ("positionId","symbol","marginCoin","holdSide","openAvgPrice","closeAvgPrice",
                "openTotalPos","closeTotalPos","pnl","netProfit","totalFunding","openFee","closeFee","ctime","utime")
    if any(row.get(k) in (None, "") for k in required):
        raise ValueError("Bitget历史持仓缺少实际金额、数量或时间")
    if str(row["marginCoin"]).upper() != "USDT" or row["holdSide"] not in ("long","short"):
        raise ValueError("Bitget历史持仓币种或方向不匹配")
    values = {k:D(row[k]) for k in ("openAvgPrice","closeAvgPrice","openTotalPos","closeTotalPos",
                                  "pnl","netProfit","totalFunding","openFee","closeFee","ctime","utime")}
    if not all(x.is_finite() for x in values.values()):
        raise ValueError("Bitget历史持仓存在异常数值")
    qty = values["openTotalPos"]
    if (qty <= 0 or values["closeTotalPos"] != qty or values["openAvgPrice"] <= 0
            or values["closeAvgPrice"] <= 0 or values["ctime"] <= 0 or values["utime"] < values["ctime"]):
        raise ValueError("Bitget历史持仓尚未完整平仓或实际数量不一致")
    cash = D(row.get("cashDividend") or "0")
    expected = values["pnl"]+values["openFee"]+values["closeFee"]+values["totalFunding"]+cash
    if not cash.is_finite() or abs(expected-values["netProfit"]) > Decimal("0.000001"):
        raise ValueError("Bitget历史持仓财务分项尚不能与实际净盈利核对")
    opened, closed = float(values["ctime"] / 1000), float(values["utime"] / 1000)
    return {"exchange_position_id":str(row["positionId"]),
        "exchange_history_json":json.dumps(row,ensure_ascii=False,sort_keys=True),
        "qty":amount_str(qty), "base_qty":amount_str(qty),
        "notional":amount_str(values["openAvgPrice"]*qty),
        "entry":amount_str(values["openAvgPrice"]), "avg_entry":amount_str(values["openAvgPrice"]),
        "final_price":amount_str(values["closeAvgPrice"]),
        "open_ts":opened, "close_ts":closed, "open_time":ledger_time(opened), "close_time":ledger_time(closed),
        "gross_pnl":amount_str(values["pnl"]), "net_pnl":amount_str(values["netProfit"]),
        "open_fee":amount_str(-values["openFee"]), "close_fee":amount_str(-values["closeFee"]),
        "funding_fee":amount_str(-values["totalFunding"]), "fee_source":"exchange", "funding_source":"exchange",
        "fee_note":"Bitget实际历史持仓费用及完整成交相互核对", "funding_note":"Bitget实际历史持仓累计资金费",
        "margin_mode":str(row.get("marginMode") or "")}


# ==================== 数据库 ====================

# 老版本数据库升级时自动补充的列（程序重启不丢数据）
NEW_SIGNAL_COLUMNS = {
    "exchange": "TEXT DEFAULT 'bitget'",   # 该仓位所在交易所：bitget / gate
    "open_order_id": "TEXT DEFAULT ''",    # 首次开仓订单号（兼容旧字段）
    "close_order_id": "TEXT DEFAULT ''",   # 平仓订单号
    "open_fee": "TEXT DEFAULT ''",         # 开仓手续费 USDT（马丁加仓会累加）
    "close_fee": "TEXT DEFAULT ''",        # 平仓手续费 USDT
    "gross_pnl": "TEXT DEFAULT ''",        # 毛盈亏 USDT（未扣手续费）
    "net_pnl": "TEXT DEFAULT ''",          # 净盈亏 USDT（扣除手续费后）
    "fee_note": "TEXT DEFAULT ''",         # 手续费说明（实际 / 估算）
    "avg_entry": "TEXT DEFAULT ''",        # 当前持仓均价（马丁加仓后会变化）
    "base_qty": "TEXT DEFAULT ''",         # 首次开仓数量（马丁加仓按它 × 倍数差计算）
    "martin_layers": "INTEGER DEFAULT 0",  # 已成功同步的马丁加仓层数
    "martin_multiplier": "TEXT DEFAULT '1'",  # 已成功同步的累计仓位倍数（1 = 原始仓位）
    "platform_multiplier": "TEXT DEFAULT ''", # 平台最新目标仓位倍数（加仓失败也记录）
    "platform_avg": "TEXT DEFAULT ''",        # 平台最后已知均价（用于估算实时收益）
    "platform_tracking": "TEXT DEFAULT ''",   # '1' = 我们已平仓但平台仍在跟踪（影子）
    "platform_stop": "TEXT DEFAULT ''",       # 平台最新止损（影子）
    "platform_target": "TEXT DEFAULT ''",     # 平台最新止盈（影子）
    "platform_update_time": "TEXT DEFAULT ''",# 平台最后移动止损时间（影子）
    "platform_end_reason": "TEXT DEFAULT ''", # 平台结束原因（影子）
    "platform_end_text": "TEXT DEFAULT ''",   # 平台结束文本（影子）
    "platform_end_time": "TEXT DEFAULT ''",   # 平台结束时间（影子）
    "platform_final_price": "TEXT DEFAULT ''",# 平台结束价格（影子）
    "platform_profit_percent": "TEXT DEFAULT ''",
    "open_order_ids": "TEXT DEFAULT ''",   # 所有开仓/加仓订单号，逗号分隔（查实际手续费）
    "funding_fee": "TEXT DEFAULT '0'",     # 累计资金费（正数=支付，负数=收取）
    "funding_note": "TEXT DEFAULT ''",     # 资金费说明（按公开费率估算等）
    "last_funding_ts": "REAL DEFAULT 0",   # 已核算到的资金费结算时间（epoch 秒）
    "liquidation_price": "TEXT DEFAULT ''", # Bitget 持仓接口返回的强平价格（逐仓有效）
    "margin_mode": "TEXT DEFAULT ''",      # Bitget 实际保证金模式（isolated/crossed）
    "leverage": "TEXT DEFAULT ''",         # Bitget 实际杠杆
    "follow_martingale": "TEXT DEFAULT ''",# 资金流信号新策略后入仓：'1' = 允许跟随后续马丁
    "head_multiplier": "TEXT DEFAULT ''",  # 开仓时的头仓仓位倍数（平台网页显示的 0.5X/1X）
    "pending_add_order_id": "TEXT DEFAULT ''",  # 马丁限价加仓挂单号（未成交/部分成交）
    "pending_add_qty": "TEXT DEFAULT ''",       # 该挂单的加仓数量（判断是否全部成交）
    "pending_add_price": "TEXT DEFAULT ''",     # 该挂单的限价（网页“挂单中”显示加仓挂单用）
    "pending_add_ts": "REAL DEFAULT 0",         # 该挂单的下单时间（epoch 秒）
    "pending_open_order_id": "TEXT DEFAULT ''", # 开仓限价单号（等待成交）
    "pending_open_qty": "TEXT DEFAULT ''",      # 开仓限价单目标数量
    "pending_open_price": "TEXT DEFAULT ''",    # 开仓限价单价格
    "funding_source": "TEXT DEFAULT 'estimate'", # 实际账单覆盖完成前不冒充准确值
    "fee_source": "TEXT DEFAULT 'pending'",      # 完整成交流水校准后才标为实际
    "accounting_parent": "TEXT DEFAULT ''",     # 部分平仓的原始仓位，避免重复分摊费用
    "record_source": "TEXT DEFAULT ''",         # exchange_recovered仅表示实际成交补录的已平历史
    "exchange_position_id": "TEXT DEFAULT ''",  # 实际交易所历史仓位编号
    "exchange_history_json": "TEXT DEFAULT ''", # 实际历史接口原文，不含认证信息
    "recovered_event_id": "INTEGER DEFAULT 0",  # 对应缺失补开记录的数量事件编号
}


def _dashboard_cached(fn):
    """只在本次同步只读请求内复用结果，请求结束立即丢弃。"""
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        context = getattr(self._dashboard_local, "current", None)
        if context is None:
            return fn(self, *args, **kwargs)
        key = (fn.__name__, repr(args), repr(sorted(kwargs.items())))
        if key not in context["memo"]:
            context["memo"][key] = fn(self, *args, **kwargs)
        return context["memo"][key]
    return wrapped


class Database:
    """SQLite 封装：存参数、信号全生命周期、日志。重启后数据都在。"""

    def __init__(self, path=DB_PATH, quiet=False):
        Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self.quiet = quiet          # quiet=True 时不在控制台打印日志（自检程序用）
        self.path = str(path)
        self._lock = threading.RLock()
        self._dashboard_local = threading.local()
        self._revision_epoch = str(time.time_ns())
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()
        # 老记录/加仓失败的记录：从日志里的“仓位 X.X”补回平台目标倍数，供界面显示
        try:
            self.backfill_platform_multipliers()
        except Exception:
            pass
        # 用 signals.jsonl 原始消息，给“我们已止损平仓”的记录补平台跟踪/结束状态
        try:
            self.backfill_platform_shadow_from_jsonl()
        except Exception:
            pass
        # 老影子记录的平台均价（用于平台实时收益估算）
        try:
            self.backfill_platform_avg_from_jsonl()
        except Exception:
            pass

    @contextmanager
    def dashboard_read(self):
        """同步展示请求内共享读取结果；不得跨await或用于写入流程。"""
        with self._lock:
            current = getattr(self._dashboard_local, "current", None)
            if current is not None:
                yield
                return
            self._dashboard_local.current = {"memo": {}, "rows": None, "filters": {}, "net": {}}
            try:
                yield
            finally:
                self._dashboard_local.current = None

    def _dashboard_rows(self, statuses, exchange="", date_field="", day="", source=None):
        context = getattr(self._dashboard_local, "current", None)
        if context is None:
            return None
        if context["rows"] is None:
            context["rows"] = [dict(r) for r in self._conn.execute(
                "SELECT id,status,exchange,source,open_time,close_time,profit_percent,notional,"
                "open_fee,close_fee,funding_fee,gross_pnl,net_pnl,record_source,"
                "fee_source,funding_source,end_reason "
                "FROM signals WHERE status IN (?,?,?) ORDER BY status,rowid",
                (ST_HOLDING, ST_CLOSED, ST_UNKNOWN)).fetchall()]
        key = (tuple(statuses), exchange, date_field, day, source)
        if key not in context["filters"]:
            context["filters"][key] = [r for r in context["rows"]
                if r["status"] in statuses
                and (not exchange or r.get("exchange") == exchange)
                and (not date_field or str(r.get(date_field) or "").startswith(day))
                and (source is None or (r.get("source") in (None, "", "未标注")
                     if source == "未标注" else r.get("source") == source))]
        return context["filters"][key]

    def _dashboard_net(self, row):
        context = getattr(self._dashboard_local, "current", None)
        if context is None or "id" not in row.keys():
            return signal_net(row)
        key = row["id"]
        if key in (None, ""):
            return signal_net(row)
        if key not in context["net"]:
            context["net"][key] = signal_net(row)
        return context["net"][key]

    def data_revision(self):
        """同时覆盖本连接和外部连接的写入；令牌只用于展示刷新。"""
        with self._lock:
            version = self._conn.execute("PRAGMA data_version").fetchone()[0]
            return f"{self._revision_epoch}:{self._conn.total_changes}:{version}"

    def _init_schema(self):
        with self._lock:
            self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS signals(
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                source TEXT DEFAULT '',
                qty TEXT DEFAULT '',
                notional TEXT DEFAULT '',
                entry TEXT DEFAULT '',
                stop TEXT DEFAULT '',
                target TEXT DEFAULT '',
                liquidation_price TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT '持仓中',
                skip_reason TEXT DEFAULT '',
                open_time TEXT DEFAULT '',
                open_ts REAL DEFAULT 0,
                close_time TEXT DEFAULT '',
                close_ts REAL DEFAULT 0,
                final_price TEXT DEFAULT '',
                profit_percent TEXT DEFAULT '',
                end_reason TEXT DEFAULT '',
                end_note TEXT DEFAULT '',
                note TEXT DEFAULT '',
                open_order_id TEXT DEFAULT '',
                close_order_id TEXT DEFAULT '',
                open_fee TEXT DEFAULT '',
                close_fee TEXT DEFAULT '',
                gross_pnl TEXT DEFAULT '',
                net_pnl TEXT DEFAULT '',
                fee_note TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS logs(
                ts TEXT,
                level TEXT,
                msg TEXT
            );
            CREATE TABLE IF NOT EXISTS funding_events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT NOT NULL,
                funding_ts REAL NOT NULL,
                funding_time TEXT NOT NULL,
                amount TEXT NOT NULL,
                rate TEXT DEFAULT '',
                price TEXT DEFAULT '',
                note TEXT DEFAULT '',
                exchange TEXT DEFAULT 'bitget'
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_unique
                ON funding_events(signal_id, funding_ts);
            CREATE TABLE IF NOT EXISTS fee_events(
                fill_id TEXT PRIMARY KEY,
                signal_id TEXT DEFAULT '',
                symbol TEXT DEFAULT '',
                fee_ts REAL NOT NULL,
                fee_time TEXT NOT NULL,
                fee TEXT NOT NULL,
                fee_coin TEXT DEFAULT 'USDT',
                side TEXT DEFAULT '',
                exchange TEXT DEFAULT 'bitget'
            );
            CREATE INDEX IF NOT EXISTS idx_fee_events_time ON fee_events(fee_time);
            CREATE TABLE IF NOT EXISTS ledger_sync(
                exchange TEXT NOT NULL, kind TEXT NOT NULL,
                start_ts REAL NOT NULL, end_ts REAL NOT NULL,
                updated_ts REAL NOT NULL, error TEXT DEFAULT '',
                PRIMARY KEY(exchange,kind)
            );
            CREATE TABLE IF NOT EXISTS funding_bills(
                bill_id TEXT PRIMARY KEY, exchange TEXT NOT NULL,
                symbol TEXT NOT NULL, bill_ts REAL NOT NULL,
                amount TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_funding_bills_time ON funding_bills(exchange,bill_ts);
            CREATE TABLE IF NOT EXISTS funding_allocations(
                bill_id TEXT NOT NULL, signal_id TEXT NOT NULL,
                amount TEXT NOT NULL, PRIMARY KEY(bill_id,signal_id)
            );
            CREATE TABLE IF NOT EXISTS position_qty_events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT NOT NULL,
                ts REAL NOT NULL,
                qty TEXT NOT NULL,
                action TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_qty_events
                ON position_qty_events(signal_id, ts);
            CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status);
            """)
            # 老版本数据库自动补列，避免升级后报错
            cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(signals)")}
            for name, decl in NEW_SIGNAL_COLUMNS.items():
                if name not in cols:
                    self._conn.execute(f"ALTER TABLE signals ADD COLUMN {name} {decl}")
            # 老库补交易所列：历史记录全部属于 Bitget
            fee_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(fee_events)")}
            for name in ("order_id", "price", "volume", "profit"):
                if name not in fee_cols:
                    self._conn.execute(f"ALTER TABLE fee_events ADD COLUMN {name} TEXT DEFAULT ''")
            for table in ("fee_events", "funding_events"):
                tcols = {r["name"] for r in self._conn.execute(f"PRAGMA table_info({table})")}
                if "exchange" not in tcols:
                    self._conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN exchange TEXT DEFAULT 'bitget'")
            self._conn.execute(
                "UPDATE signals SET exchange='bitget' WHERE exchange IS NULL OR exchange=''")
            # 老数据回填：均价默认用原入场价，初始数量作为基础数量
            self._conn.execute(
                "UPDATE signals SET avg_entry=entry "
                "WHERE (avg_entry IS NULL OR avg_entry='') AND entry IS NOT NULL AND entry!=''")
            self._conn.execute(
                "UPDATE signals SET base_qty=qty "
                "WHERE (base_qty IS NULL OR base_qty='') AND qty IS NOT NULL AND qty!=''")
            self._conn.execute(
                "UPDATE signals SET martin_multiplier='1' "
                "WHERE martin_multiplier IS NULL OR martin_multiplier=''")
            self._conn.execute(
                "UPDATE signals SET martin_layers=0 WHERE martin_layers IS NULL")
            self._conn.execute(
                "UPDATE signals SET open_order_ids=open_order_id "
                "WHERE (open_order_ids IS NULL OR open_order_ids='') AND open_order_id!=''")
            # 老仓位从开仓时间开始补算资金费（避免重复，只对 0 值的补）
            self._conn.execute(
                "UPDATE signals SET last_funding_ts=open_ts "
                "WHERE (last_funding_ts IS NULL OR last_funding_ts=0) AND open_ts>0")
            # 为老仓位补一条“开仓时数量”的事件，资金费按结算当时数量计算
            rows_q = self._conn.execute(
                "SELECT id, base_qty, qty, open_ts FROM signals WHERE open_ts>0").fetchall()
            for r in rows_q:
                cnt = self._conn.execute(
                    "SELECT COUNT(*) c FROM position_qty_events WHERE signal_id=?",
                    (r["id"],)).fetchone()["c"]
                if cnt:
                    continue
                q0 = r["base_qty"] or r["qty"] or "0"
                if q0 and q0 != "0":
                    self._conn.execute(
                        "INSERT INTO position_qty_events(signal_id,ts,qty,action) VALUES(?,?,?,?)",
                        (r["id"], float(r["open_ts"]), str(q0), "开仓"))
            # 先完成旧库补列与回填，再创建依赖交易所列的展示查询索引。
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_status_close ON signals(status,close_ts DESC)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_status_exchange_close ON signals(status,exchange,close_ts DESC)")
            self._conn.commit()

    def close(self):
        """关闭数据库连接（自检等一次性场景用，避免占用文件）"""
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    # ---- 参数 ----
    @_dashboard_cached
    def get_setting(self, key, default=""):
        with self._lock:
            row = self._conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    @_dashboard_cached
    def get_settings(self, defaults):
        keys = tuple(defaults)
        if not keys:
            return {}
        with self._lock:
            rows = self._conn.execute(
                "SELECT key,value FROM settings WHERE key IN (" + ",".join("?" for _ in keys) + ")",
                keys).fetchall()
        values = {r["key"]: r["value"] for r in rows}
        return {key: values.get(key, default) for key, default in defaults.items()}

    def set_setting(self, key, value):
        with self._lock:
            self._conn.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
            self._conn.commit()

    # ---- 信号 ----
    def upsert_signal(self, **fields):
        cols = list(fields.keys())
        ph = ",".join("?" * len(cols))
        sql = (f"INSERT INTO signals({','.join(cols)}) VALUES({ph}) "
               f"ON CONFLICT(id) DO UPDATE SET " +
               ",".join(f"{c}=excluded.{c}" for c in cols if c != "id"))
        with self._lock:
            self._conn.execute(sql, [fields[c] for c in cols])
            self._conn.commit()

    def update_signal(self, sig_id, **fields):
        if not fields:
            return
        # 仓位或费用发生变化后，原有核验结论失效；完整账单校准才能再次确认。
        if {"qty", "open_fee", "close_fee", "gross_pnl", "close_ts", "open_order_ids", "close_order_id"} & fields.keys():
            fields.setdefault("fee_source", "pending")
        if {"qty", "open_ts", "close_ts"} & fields.keys():
            fields.setdefault("funding_source", "pending")
        sets = ",".join(f"{k}=?" for k in fields)
        with self._lock:
            self._conn.execute(f"UPDATE signals SET {sets} WHERE id=?", [*fields.values(), sig_id])
            self._conn.commit()

    def get_signal(self, sig_id):
        with self._lock:
            row = self._conn.execute("SELECT * FROM signals WHERE id=?", (sig_id,)).fetchone()
        return dict(row) if row else None

    def closed_base_exists(self, sig_id):
        """该平台信号 id 是否已有一条本地已平仓记录（用于后入重开判断）"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) c FROM signals WHERE id=? AND status=?",
                (sig_id, ST_CLOSED)).fetchone()
        return bool(row and row["c"])

    def archive_signal_id(self, old_id, new_id):
        """把一个信号记录改名归档（保留历史），腾出原 id 供重新开仓。
        同步改资金费/数量事件的外键，避免历史数据丢失。"""
        with self._lock:
            exists = self._conn.execute(
                "SELECT id FROM signals WHERE id=?", (new_id,)).fetchone()
            if exists:
                return False
            for table, col in (("signals", "id"),
                               ("funding_events", "signal_id"),
                               ("position_qty_events", "signal_id")):
                self._conn.execute(
                    f"UPDATE {table} SET {col}=? WHERE {col}=?", (new_id, old_id))
            self._conn.commit()
        return True

    def backfill_platform_multipliers(self):
        """给持仓和历史记录补平台目标倍数：优先从日志中的“仓位 1.2346X”文本提取。
        只处理 platform_multiplier 为空的记录，不影响已有值。"""
        pat = re.compile(r"仓位\s*([0-9]+(?:\.[0-9]+)?)\s*X")
        changed = 0
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM signals WHERE status IN (?,?,?) AND "
                "(platform_multiplier IS NULL OR platform_multiplier='')",
                (ST_HOLDING, ST_UNKNOWN, ST_CLOSED)).fetchall()
            for r in rows:
                logs = self._conn.execute(
                    "SELECT msg FROM logs WHERE msg LIKE ? ORDER BY rowid DESC LIMIT 100",
                    (f"%{r['id']}%",)).fetchall()
                for log in logs:
                    m = pat.search(log["msg"] or "")
                    if m:
                        self._conn.execute(
                            "UPDATE signals SET platform_multiplier=? WHERE id=?",
                            (m.group(1), r["id"]))
                        changed += 1
                        break
            if changed:
                self._conn.commit()
        return changed

    def backfill_platform_shadow_from_jsonl(self, tail=30000):
        """用 signals.jsonl 里的原始平台消息，给“我们已止损平仓”的记录补平台状态：
        最后收到 trailed → 平台仍在跟踪；收到 ended → 平台已结束（带原因）；
        最后回执未列出且没有任何消息 → 平台已不再跟踪。只补字段为空的记录。"""
        try:
            with open(JSONL_PATH, "r", encoding="utf-8") as f:
                lines = f.readlines()[-tail:]
        except Exception:
            return 0
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, stop, target, avg_entry, entry FROM signals "
                "WHERE status=? AND end_reason='exchange_stop' AND "
                "(platform_tracking IS NULL OR platform_tracking='') AND "
                "(platform_end_reason IS NULL OR platform_end_reason='')",
                (ST_CLOSED,)).fetchall()
        recs = {r["id"]: dict(r) for r in rows}
        ids = set(recs)
        if not ids:
            return 0
        state = {}
        last_receipt_ids = set()
        saw_receipt = False
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except Exception:
                continue
            t = str(msg.get("type") or "")
            sid = str(msg.get("id") or "")
            if t == "signal.trailed" and sid in ids:
                st = state.setdefault(sid, {})
                st["platform_tracking"] = "1"
                st["platform_stop"] = str(msg.get("stop") or "")
                st["platform_target"] = str(msg.get("target") or "")
                try:
                    avg = (msg.get("martingale") or {}).get("avg")
                    if avg not in (None, ""):
                        st["platform_avg"] = str(avg)
                except Exception:
                    pass
            elif t == "signal.ended" and sid in ids:
                state[sid] = {
                    "platform_tracking": "0",
                    "platform_end_reason": str(msg.get("end_reason") or ""),
                    "platform_end_text": str(msg.get("text") or ""),
                    "platform_final_price": str(msg.get("final_price") or ""),
                    "platform_profit_percent": str(msg.get("profit_percent") or ""),
                }
            elif t == "signal.receipt":
                try:
                    last_receipt_ids = set(str(i) for i in (msg.get("ids") or []))
                except Exception:
                    last_receipt_ids = set()
                saw_receipt = True
        if saw_receipt:
            for sid in ids:
                if sid in state:
                    continue
                if sid in last_receipt_ids:
                    state[sid] = {
                        "platform_tracking": "1",
                        "platform_stop": str(recs[sid].get("stop") or ""),
                        "platform_target": str(recs[sid].get("target") or ""),
                        "platform_avg": str(recs[sid].get("avg_entry") or recs[sid].get("entry") or ""),
                    }
                else:
                    state[sid] = {
                        "platform_tracking": "0",
                        "platform_end_reason": "平台已不再跟踪（回执未列出）",
                    }
        changed = 0
        with self._lock:
            for sid, fields in state.items():
                self._conn.execute(
                    f"UPDATE signals SET {','.join(k + '=?' for k in fields)} WHERE id=?",
                    [*fields.values(), sid])
                changed += 1
            if changed:
                self._conn.commit()
        return changed

    @_dashboard_cached
    def totals(self, exchange=""):
        """累计统计：总开仓数、总手续费（含持仓中已付）、总资金费。只统计真正开过的仓位。"""
        ex_sql = " AND exchange=?" if exchange else ""
        ex_args = [exchange] if exchange else []
        rows = self._dashboard_rows((ST_HOLDING, ST_CLOSED, ST_UNKNOWN), exchange)
        if rows is None:
            with self._lock:
                opened = self._conn.execute(
                    "SELECT COUNT(*) c FROM signals WHERE status IN (?,?,?)" + ex_sql,
                    (ST_HOLDING, ST_CLOSED, ST_UNKNOWN) + tuple(ex_args)).fetchone()["c"]
                rows = self._conn.execute(
                    "SELECT open_fee, close_fee, funding_fee FROM signals "
                    "WHERE status IN (?,?,?)" + ex_sql,
                    (ST_HOLDING, ST_CLOSED, ST_UNKNOWN) + tuple(ex_args)).fetchall()
        else:
            opened = len(rows)
        fees = Decimal("0")
        funding = Decimal("0")
        for r in rows:
            try:
                fees += D(r["open_fee"] or "0") + D(r["close_fee"] or "0")
            except Exception:
                pass
            try:
                funding += D(r["funding_fee"] or "0")
            except Exception:
                pass
        names = self.accounting_exchanges(exchange)
        fees_ok = bool(names) and all(self.ledger_ready(n, "fees") for n in names)
        funding_ok = bool(names) and all(self.ledger_ready(n, "funding") for n in names)
        if fees_ok:
            fees = self.actual_fees(exchange)
        if funding_ok:
            funding = self.actual_funding(exchange)
        return {"opened": int(opened), "fees": money_str(fees), "funding": money_str(funding),
                "fees_verified": fees_ok, "funding_verified": funding_ok,
                "net_verified": self.net_quality(exchange, "closed")["verified"]}

    @_dashboard_cached
    def accounting_exchanges(self, exchange=""):
        if exchange:
            return [exchange]
        with self._lock:
            rows = self._conn.execute("SELECT DISTINCT exchange FROM signals WHERE status IN (?,?,?)",
                                       (ST_HOLDING, ST_CLOSED, ST_UNKNOWN)).fetchall()
        return list(dict.fromkeys(list(getattr(self, "active_exchange_names", [])) +
                                  [str(r[0] or "bitget") for r in rows]))

    @_dashboard_cached
    def accounting_quality(self, exchanges=None):
        names = list(exchanges) if exchanges is not None else self.accounting_exchanges()
        rows = self._dashboard_rows((ST_HOLDING, ST_CLOSED, ST_UNKNOWN))
        if rows is None:
            with self._lock:
                rows = [dict(r) for r in self._conn.execute("SELECT * FROM signals WHERE status IN (?,?,?)",
                                                           (ST_HOLDING, ST_CLOSED, ST_UNKNOWN)).fetchall()]
        rows = [r for r in rows if str(r.get("exchange") or "bitget") in names]
        with self._lock:
            unassigned = [r[0] for r in self._conn.execute(
                "SELECT DISTINCT b.exchange FROM funding_bills b LEFT JOIN funding_allocations a "
                "ON a.bill_id=b.bill_id WHERE a.bill_id IS NULL AND CAST(b.amount AS REAL)!=0").fetchall()]
        pending_fees = sum(r.get("fee_source") != "exchange" for r in rows)
        pending_funding = sum(r.get("funding_source") != "exchange" for r in rows)
        missing = sum(r["status"] == ST_CLOSED and self._dashboard_net(r) is None for r in rows)
        states = {n: {k: self.ledger_state(n, k) for k in ("fees", "funding")} for n in names}
        ready = bool(names) and all(self.ledger_ready(n, k) for n in names for k in ("fees", "funding"))
        issues = []
        if not ready:
            failures = [f"{n} {'手续费' if k == 'fees' else '资金费'}：{st['error']}"
                        for n, kinds in states.items() for k, st in kinds.items() if st.get("error")]
            failures += [f"{n}历史{'手续费' if k == 'fees' else '资金费'}：{st['error']}"
                         for n in names for k in ("fees", "funding")
                         for st in [self.ledger_state(n, k + "_history")] if st.get("error")]
            issues.append("；".join(failures) if failures else "实际费用账单同步中")
        if pending_fees:
            issues.append(f"手续费记录已确认 {len(rows)-pending_fees}/{len(rows)}，其余正在复核")
        if pending_funding:
            issues.append(f"{pending_funding} 条仓位资金费待归属")
        if missing:
            issues.append(f"{missing} 条平仓记录缺少盈亏凭证")
        if any(n in names for n in unassigned):
            issues.append("存在尚未归属到仓位的实际资金费账单")
        return {"verified": bool(ready and not issues), "issues": issues,
                "pending_fees": pending_fees, "pending_funding": pending_funding,
                "missing_net": missing, "by_exchange": states,
                "message": "；".join(issues) if issues else "已按交易所实际费用账单同步"}

    @_dashboard_cached
    def net_quality(self, exchange="", scope="closed", source=None):
        """只核验该项净值实际依赖的仓位，不与其他交易所或持仓范围联动。"""
        sql = "SELECT * FROM signals WHERE status IN (?,?)" if scope == "holding" else "SELECT * FROM signals WHERE status=?"
        args = [ST_HOLDING, ST_UNKNOWN] if scope == "holding" else [ST_CLOSED]
        if exchange:
            sql += " AND exchange=?"
            args.append(exchange)
        if scope == "today":
            sql += " AND close_time LIKE ?"
            args.append(today_str()+"%")
        if source is not None:
            if source == "未标注":
                sql += " AND (source IS NULL OR source='' OR source='未标注')"
            else:
                sql += " AND source=?"
                args.append(source)
        rows = self._dashboard_rows(
            (ST_HOLDING, ST_UNKNOWN) if scope == "holding" else (ST_CLOSED,), exchange,
            "close_time" if scope == "today" else "", today_str() if scope == "today" else "", source)
        if rows is None:
            with self._lock:
                rows = [dict(r) for r in self._conn.execute(sql, args).fetchall()]
        pending = sum(r.get("fee_source") != "exchange" or r.get("funding_source") != "exchange"
                      or r["status"] == ST_UNKNOWN or (scope != "holding" and self._dashboard_net(r) is None) for r in rows)
        return {"verified": pending == 0, "count": len(rows), "pending": pending,
                "message": f"本项 {pending}/{len(rows)} 条记录费用待核验" if pending else "本项记录实际费用已核验"}

    def backfill_platform_avg_from_jsonl(self, tail=30000):
        """老影子记录：从 signals.jsonl 的 martingale.avg 补平台最后均价。"""
        try:
            with open(JSONL_PATH, "r", encoding="utf-8") as f:
                lines = f.readlines()[-tail:]
        except Exception:
            return 0
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM signals WHERE status=? AND end_reason='exchange_stop' AND "
                "(platform_avg IS NULL OR platform_avg='')", (ST_CLOSED,)).fetchall()
        ids = {r["id"] for r in rows}
        if not ids:
            return 0
        avg_map = {}
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except Exception:
                continue
            if str(msg.get("type") or "") != "signal.trailed":
                continue
            sid = str(msg.get("id") or "")
            if sid not in ids:
                continue
            try:
                avg = (msg.get("martingale") or {}).get("avg")
            except Exception:
                avg = None
            if avg not in (None, ""):
                avg_map[sid] = str(avg)
        changed = 0
        with self._lock:
            for sid, avg in avg_map.items():
                self._conn.execute(
                    "UPDATE signals SET platform_avg=? WHERE id=?", (avg, sid))
                changed += 1
            if changed:
                self._conn.commit()
        return changed

    def platform_shadows(self, limit=100):
        """我们已平仓、平台仍在跟踪的“影子”记录"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM signals WHERE status=? AND platform_tracking='1' "
                "ORDER BY close_ts DESC LIMIT ?", (ST_CLOSED, limit)).fetchall()
        return [dict(r) for r in rows]

    def platform_ended(self, limit=100):
        """平台已结束的“影子”记录（我们此前已平仓）"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM signals WHERE status=? AND platform_end_reason!='' "
                "ORDER BY close_ts DESC LIMIT ?", (ST_CLOSED, limit)).fetchall()
        return [dict(r) for r in rows]

    def record_funding(self, signal_id, funding_ts, amount, rate="", price="", note=""):
        """记录一次资金费结算（幂等：同一信号同一结算时间只记一次）。
        amount 正数 = 支付，负数 = 收取。返回是否为新记录。"""
        ft = ledger_time(funding_ts)
        with self._lock:
            srow = self._conn.execute(
                "SELECT exchange FROM signals WHERE id=?", (signal_id,)).fetchone()
            exchange = str((srow["exchange"] if srow else "") or "bitget")
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO funding_events"
                "(signal_id,funding_ts,funding_time,amount,rate,price,note,exchange) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (signal_id, float(funding_ts), ft, str(amount), str(rate), str(price),
                 note, exchange))
            if cur.rowcount == 0:
                return False
            row = self._conn.execute(
                "SELECT funding_fee, last_funding_ts FROM signals WHERE id=?", (signal_id,)).fetchone()
            old_fee = row["funding_fee"] if row else "0"
            try:
                total = D(old_fee or "0") + D(amount or "0")
            except Exception:
                total = D(amount or "0")
            try:
                old_ts = float((row["last_funding_ts"] if row else 0) or 0)
            except Exception:
                old_ts = 0.0
            new_ts = max(old_ts, float(funding_ts))
            self._conn.execute(
                "UPDATE signals SET funding_fee=?, funding_note=?, last_funding_ts=? WHERE id=?",
                (money_str(total), note, new_ts, signal_id))
            self._conn.commit()
        return True

    def get_funding_events(self, signal_id, limit=50):
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM funding_events WHERE signal_id=? ORDER BY funding_ts DESC LIMIT ?",
                (signal_id, limit)).fetchall()
        return [dict(r) for r in rows]

    def record_qty_event(self, signal_id, ts, qty, action=""):
        """记录仓位数量变化（开仓 / 马丁加仓），资金费按结算当时数量计算"""
        with self._lock:
            self._conn.execute(
                "INSERT INTO position_qty_events(signal_id,ts,qty,action) VALUES(?,?,?,?)",
                (signal_id, float(ts), str(qty), action))
            self._conn.commit()

    def qty_at(self, signal_id, ts):
        """取某时刻的持仓数量（取该时刻之前最后一次数量变化）"""
        with self._lock:
            row = self._conn.execute(
                "SELECT qty FROM position_qty_events "
                "WHERE signal_id=? AND ts<=? ORDER BY ts DESC, id DESC LIMIT 1",
                (signal_id, float(ts))).fetchone()
        return row["qty"] if row else None

    def open_positions(self, exchange=""):
        """当前还在持仓的记录（含'待核对'：结果未知必须当持仓看待，防止重复加仓）"""
        ex_sql = " AND exchange=?" if exchange else ""
        ex_args = [exchange] if exchange else []
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM signals WHERE status IN (?,?)" + ex_sql + " ORDER BY open_ts",
                (ST_HOLDING, ST_UNKNOWN) + tuple(ex_args)).fetchall()
        return [dict(r) for r in rows]

    def pending_entries(self, exchange=""):
        """限价开仓挂单中的记录（尚未成交，不算持仓）"""
        ex_sql = " AND exchange=?" if exchange else ""
        ex_args = [exchange] if exchange else []
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM signals WHERE status=?" + ex_sql + " ORDER BY open_ts",
                (ST_PENDING,) + tuple(ex_args)).fetchall()
        return [dict(r) for r in rows]

    def closed_history(self, limit=100, source="", symbol="", exchange=""):
        """历史仓位：所有已平仓记录，最新的在前；可按来源、币种和交易所筛选。"""
        sql = "SELECT * FROM signals WHERE status=?"
        params = [ST_CLOSED]
        if source:
            sql += " AND source=?"
            params.append(source)
        if exchange:
            sql += " AND exchange=?"
            params.append(exchange)
        sql, params = add_symbol_filter(sql, params, symbol)
        sql += " ORDER BY close_ts DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        out = []
        for row in rows:
            value = dict(row)
            net = signal_net(value)
            value["net_pnl"] = money_str(net) if net is not None else ""
            value["accounting_verified"] = value.get("fee_source") == "exchange" and value.get("funding_source") == "exchange"
            out.append(value)
        return out

    def closed_count(self, source="", symbol="", exchange=""):
        """历史仓位总条数（不受网页 100 条显示上限影响）；可按来源、币种和交易所筛选。"""
        sql = "SELECT COUNT(*) c FROM signals WHERE status=?"
        params = [ST_CLOSED]
        if source:
            sql += " AND source=?"
            params.append(source)
        if exchange:
            sql += " AND exchange=?"
            params.append(exchange)
        sql, params = add_symbol_filter(sql, params, symbol)
        with self._lock:
            row = self._conn.execute(sql, tuple(params)).fetchone()
        return int(row["c"] or 0) if row else 0

    def used_sources(self):
        """当前持仓 + 历史仓位里出现过的信号来源，用于网页筛选下拉。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT source FROM signals "
                "WHERE source!='' AND status IN (?,?,?) ORDER BY source",
                (ST_HOLDING, ST_UNKNOWN, ST_CLOSED)).fetchall()
        return [str(r["source"]) for r in rows]

    def used_symbols(self):
        """当前持仓 + 历史仓位里出现过的交易对，用于币种输入联想。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT symbol FROM signals "
                "WHERE symbol!='' AND status IN (?,?,?) ORDER BY symbol",
                (ST_HOLDING, ST_UNKNOWN, ST_CLOSED)).fetchall()
        return [normalize_symbol_filter(r["symbol"]) for r in rows if r["symbol"]]

    def pnl_curve(self, scope="all", exchange=""):
        """累计已实现净盈亏曲线（按平仓时间升序）。
        scope=all 全部历史；scope=today 从今天 00:00 起算。"""
        today = today_str()
        ex_sql = " AND exchange=?" if exchange else ""
        ex_args = [exchange] if exchange else []
        date_sql = " AND close_time LIKE ?" if scope == "today" else ""
        date_args = [today + "%"] if scope == "today" else []
        with self._lock:
            rows = self._conn.execute(
                "SELECT * "
                "FROM signals WHERE status=? AND close_ts>0" + ex_sql + date_sql + " ORDER BY close_ts ASC",
                (ST_CLOSED,) + tuple(ex_args) + tuple(date_args)).fetchall()
        try:
            today_start = day_start_ts()
        except Exception:
            today_start = 0.0
        items = []
        missing = 0
        for r in rows:
            if scope == "today" and not str(r["close_time"] or "").startswith(today):
                continue
            try:
                net = self._dashboard_net(r)
                if net is None:
                    missing += 1
                    continue
            except Exception:
                net = Decimal("0")
            try:
                ts = float(r["close_ts"] or 0)
            except Exception:
                ts = 0.0
            if ts <= 0:
                continue
            items.append({"ts": ts, "net": net, "row": r})
        start_ts = today_start if scope == "today" else (
            (items[0]["ts"] - 60) if items else time.time())
        points = [{"ts": start_ts, "time": ledger_time(start_ts),
            "cum": "0", "net": "", "symbol": "", "direction": "",
            "source": ""}]
        cum = Decimal("0")
        for it in items:
            cum += it["net"]
            r = it["row"]
            points.append({
                "ts": it["ts"], "time": str(r["close_time"] or ""),
                "cum": money_str(cum), "net": money_str(it["net"]),
                "symbol": str(r["symbol"] or ""),
                "direction": str(r["direction"] or ""),
                "source": str(r["source"] or ""),
            })
        return {"scope": "today" if scope == "today" else "all",
                "points": points, "total": money_str(cum) if not missing else "",
                "missing_count": missing,
                "data_quality": self.net_quality(exchange, "today" if scope == "today" else "closed"),
                "count": len(items), "start_ts": start_ts,
                "end_ts": items[-1]["ts"] if items else time.time()}


    @_dashboard_cached
    def total_realized_pnl(self, exchange=""):
        """所有已平仓信号的净盈亏合计（历史全部，含手续费和资金费）。
        net_pnl 缺失时用毛盈亏 − 手续费 − 资金费补算，避免旧记录被当成 0。"""
        ex_sql = " AND exchange=?" if exchange else ""
        ex_args = [exchange] if exchange else []
        rows = self._dashboard_rows((ST_CLOSED,), exchange)
        if rows is None:
            with self._lock:
                rows = self._conn.execute(
                    "SELECT net_pnl, gross_pnl, open_fee, close_fee, funding_fee, record_source, fee_source, funding_source "
                    "FROM signals WHERE status=?" + ex_sql + " ORDER BY rowid",
                    (ST_CLOSED,) + tuple(ex_args)).fetchall()
        total = Decimal("0")
        for r in rows:
            value = self._dashboard_net(r)
            if value is not None:
                total += value
        return total

    def source_stats(self, exchange=""):
        """按信号来源统计累计已平仓表现（实际净盈亏，结果按平台口径分类）。"""
        ex_sql = " AND exchange=?" if exchange else ""
        ex_args = [exchange] if exchange else []
        closed = self._dashboard_rows((ST_CLOSED,), exchange)
        with self._lock:
            if closed is None:
                closed = self._conn.execute(
                    "SELECT source, net_pnl, gross_pnl, open_fee, close_fee, funding_fee, end_reason, record_source, fee_source, funding_source "
                    "FROM signals WHERE status=?" + ex_sql + " ORDER BY rowid",
                    (ST_CLOSED,) + tuple(ex_args)).fetchall()
            open_rows = self._conn.execute(
                "SELECT source, COUNT(*) c FROM signals WHERE status IN (?,?)" + ex_sql
                + " GROUP BY source",
                (ST_HOLDING, ST_UNKNOWN) + tuple(ex_args)).fetchall()
        open_counts = {str(r["source"] or "未标注"): int(r["c"] or 0) for r in open_rows}

        def blank(src):
            return {
                "source": src, "closed_count": 0, "wins": 0, "losses": 0,
                "net": Decimal("0"), "gross": Decimal("0"),
                "missing_net": 0,
                "fees": Decimal("0"), "funding": Decimal("0"),
                "target_count": 0, "trail_count": 0, "stop_count": 0,
                "receipt_count": 0, "other_count": 0,
                "open_count": open_counts.get(src, 0),
            }

        data = {name: blank(name) for name in ALL_SOURCES}
        for r in closed:
            src = str(r["source"] or "未标注")
            if src not in data:
                data[src] = blank(src)
            d = data[src]
            d["closed_count"] += 1
            try:
                net = self._dashboard_net(r)
                if net is None:
                    d["missing_net"] += 1
                    net = Decimal("0")
            except Exception:
                net = Decimal("0")
            d["net"] += net
            if net > 0:
                d["wins"] += 1
            elif net < 0:
                d["losses"] += 1
            try:
                d["gross"] += D(r["gross_pnl"] or "0")
            except Exception:
                pass
            try:
                d["fees"] += D(r["open_fee"] or "0") + D(r["close_fee"] or "0")
            except Exception:
                pass
            try:
                d["funding"] += D(r["funding_fee"] or "0")
            except Exception:
                pass
            reason = str(r["end_reason"] or "")
            if reason == "target":
                d["target_count"] += 1
            elif reason == "trail_stop":
                d["trail_count"] += 1
            elif reason == "exchange_stop":
                d["stop_count"] += 1
            elif reason in ("receipt", "对账平仓", "reconcile"):
                d["receipt_count"] += 1
            else:
                d["other_count"] += 1

        out = []
        for src, d in data.items():
            if src not in ALL_SOURCES and d["closed_count"] == 0 and d["open_count"] == 0:
                continue
            count = d["closed_count"]
            out.append({
                "source": src,
                "accounting_verified": self.net_quality(exchange, "closed", src)["verified"],
                "closed_count": count,
                "wins": d["wins"],
                "losses": d["losses"],
                "win_rate": (round(d["wins"] * 100.0 / count, 1) if count else 0.0) if not d["missing_net"] else None,
                "net_pnl": money_str(d["net"]) if not d["missing_net"] else "",
                "missing_count": d["missing_net"],
                "gross_pnl": money_str(d["gross"]),
                "fees": money_str(d["fees"]),
                "funding": money_str(d["funding"]),
                "target_count": d["target_count"],
                "trail_count": d["trail_count"],
                "stop_count": d["stop_count"],
                "receipt_count": d["receipt_count"],
                "other_count": d["other_count"],
                "open_count": d["open_count"],
            })
        return out

    def all_signals(self, limit=200):
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM signals ORDER BY open_ts DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    # ---- 日志 ----
    def add_log(self, level, msg):
        ts = now_str()
        # 控制台同步打印（方便不看网页也能看到运行情况）
        if not self.quiet:
            print(f"[{time_str()}][{level}] {msg}")
        try:
            with self._lock:
                self._conn.execute("INSERT INTO logs(ts,level,msg) VALUES(?,?,?)", (ts, level, msg))
                # 只保留最近 10000 条，防止数据库无限变大
                self._conn.execute(
                    "DELETE FROM logs WHERE ts NOT IN (SELECT ts FROM logs ORDER BY ts DESC LIMIT 10000)")
                self._conn.commit()
        except Exception:
            pass  # 日志写失败不影响主流程

    def get_logs(self, n=300):
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts,level,msg FROM logs ORDER BY rowid DESC LIMIT ?", (n,)).fetchall()
        return [dict(r) for r in reversed(rows)]

    def log_page(self, n=300, after_id=None, epoch=""):
        """初次读最近日志，后续按行号增量读取；重启或回退后自动重置。"""
        limit = min(max(int(n), 1), 1000)
        with self._lock:
            last = int(self._conn.execute("SELECT COALESCE(MAX(rowid),0) FROM logs").fetchone()[0])
            reset = after_id is not None and (
                int(after_id) < 0 or int(after_id) > last
                or bool(epoch and epoch != self._revision_epoch))
            incremental = after_id is not None and not reset
            if incremental:
                rows = self._conn.execute(
                    "SELECT rowid AS id,ts,level,msg FROM logs WHERE rowid>? ORDER BY rowid LIMIT ?",
                    (int(after_id), limit)).fetchall()
            else:
                rows = list(reversed(self._conn.execute(
                    "SELECT rowid AS id,ts,level,msg FROM logs ORDER BY rowid DESC LIMIT ?",
                    (limit,)).fetchall()))
        cursor = int(rows[-1]["id"]) if rows else (int(after_id) if incremental else 0)
        return {"logs": [dict(r) for r in rows], "cursor": cursor,
                "epoch": self._revision_epoch, "reset": reset, "incremental": incremental,
                "has_more": cursor < last}

    def get_alerts(self, n=30):
        """告警区数据：最近的重要事件（告警 / 错误级别）。
        用户在网页点“清空告警”后，只显示清空时间之后的新告警。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM settings WHERE key='alerts_cleared_at'").fetchone()
            cleared = row["value"] if row else ""
            if cleared:
                rows = self._conn.execute(
                    "SELECT ts,level,msg FROM logs WHERE level IN ('alert','error') AND ts > ? "
                    "ORDER BY rowid DESC LIMIT ?", (cleared, n)).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT ts,level,msg FROM logs WHERE level IN ('alert','error') "
                    "ORDER BY rowid DESC LIMIT ?", (n,)).fetchall()
        return [dict(r) for r in reversed(rows)]

    def clear_alerts(self):
        """把当前已有的告警标记为已读（网页“清空告警”按钮），新告警仍会显示"""
        self.set_setting("alerts_cleared_at", now_str())

    def seed_last_disconnect_from_logs(self):
        """升级后用日志里的最后一次断连初始化网页显示（只在还没有记录时执行）"""
        if self.get_setting("last_disconnect_at", ""):
            return
        with self._lock:
            row = self._conn.execute(
                "SELECT ts,msg FROM logs WHERE level='warn' AND msg LIKE '%连接断开%' "
                "ORDER BY rowid DESC LIMIT 1").fetchone()
            if not row:
                return
            ts = row["ts"]
            msg = row["msg"]
            m = re.search(r"关闭码\s*(\d+)", msg or "")
            code = m.group(1) if m else ""
            row2 = self._conn.execute(
                "SELECT ts FROM logs WHERE msg LIKE '%已连接信号服务器%' AND ts > ? "
                "ORDER BY rowid LIMIT 1", (ts,)).fetchone()
            seconds = ""
            if row2:
                try:
                    delta = (datetime.strptime(row2["ts"], "%Y-%m-%d %H:%M:%S")
                             - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")).total_seconds()
                    seconds = str(max(0, int(delta)))
                except Exception:
                    pass
            for key, value in (("last_disconnect_at", ts),
                               ("last_disconnect_code", code),
                               ("last_disconnect_seconds", seconds)):
                self._conn.execute(
                    "INSERT INTO settings(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, value))
            self._conn.commit()

    # ---- 实际账单同步：完整窗口原子替换，历史重复流水也一并修复 ----
    def backup_accounting_once(self, version="114"):
        """本版本首次改写费用前做一致性备份；失败时终止修复。"""
        version = str(version)
        if not version.isdigit():
            raise ValueError("账本备份版本号不合法")
        key = "accounting_backup_v" + version
        if self.path == ":memory:":
            return
        previous = self.get_setting(key, "")
        if previous and Path(previous).is_file():
            return
        source = Path(self.path).resolve()
        target_dir = (source.parent.parent if source.parent.name == "data" else source.parent) / "backups"
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / ("console-before-update" + version + "-" + str(time.time_ns()) + ".db")
        with self._lock:
            dest = sqlite3.connect(str(target))
            try:
                self._conn.backup(dest)
            finally:
                dest.close()
            os.chmod(target, 0o600)
            self._conn.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)",
                               (key, str(target)))
            self._conn.commit()

    def orphan_closed_candidates(self, exchange):
        """只定位有原补开事件及原归档身份、但缺跟踪记录的候选，不猜手动仓归属。"""
        if exchange != "bitget":
            return {}
        with self._lock:
            rows = self._conn.execute(
                "SELECT s.symbol,MIN(e.ts) ts FROM position_qty_events e JOIN signals s "
                "ON substr(s.id,1,length(e.signal_id)+7)=e.signal_id||'_closed' "
                "WHERE s.exchange=? AND s.status=? AND e.action='补开' AND s.close_ts<e.ts "
                "AND NOT EXISTS(SELECT 1 FROM signals x WHERE x.id=e.signal_id) "
                "AND NOT EXISTS(SELECT 1 FROM signals x WHERE x.recovered_event_id=e.id "
                "AND x.record_source='exchange_recovered') GROUP BY s.symbol", (exchange,ST_CLOSED)).fetchall()
        return {str(r["symbol"]):float(r["ts"]) for r in rows}

    def recover_orphan_closed_positions(self, exchange, open_symbols, history_records):
        """跟踪记录丢失时，只按原补开事件及完整实际成交补录已平历史，不恢复自动跟踪。"""
        if exchange != "bitget":
            return []
        state = self.ledger_state(exchange, "fees")
        if not state or state.get("end_ts", 0) <= 0:
            return []
        opened_symbols = {str(x).upper() for x in open_symbols}
        with self._lock:
            events = [dict(r) for r in self._conn.execute(
                "SELECT e.* FROM position_qty_events e LEFT JOIN signals s ON s.id=e.signal_id "
                "WHERE s.id IS NULL AND e.action='补开' ORDER BY e.ts,e.id").fetchall()]
            signals = [dict(r) for r in self._conn.execute("SELECT * FROM signals WHERE exchange=?", (exchange,)).fetchall()]
            fills = [dict(r) for r in self._conn.execute(
                "SELECT * FROM fee_events WHERE exchange=? ORDER BY fee_ts,fill_id", (exchange,)).fetchall()]
            owned_ids = set()
            for sig in signals:
                owned_ids.update(x for x in str(sig.get("open_order_ids") or sig.get("open_order_id") or "").split(",") if x)
                if sig.get("close_order_id"):
                    owned_ids.add(str(sig["close_order_id"]))
            planned, proofs = [], {}
            existing_ids = {s["id"] for s in signals}
            for event in events:
                if any(int(s.get("recovered_event_id") or 0) == int(event["id"])
                       and s.get("record_source") == "exchange_recovered" for s in signals):
                    continue
                references = [s for s in signals if s["id"].startswith(str(event["signal_id"])+"_closed")
                              and s["status"] == ST_CLOSED and 0 < float(s.get("close_ts") or 0) < float(event["ts"])]
                if not references:
                    continue
                reference = max(references, key=lambda s: float(s["close_ts"]))
                symbol, direction = str(reference["symbol"]), str(reference["direction"])
                if symbol.upper() in opened_symbols or direction not in ("long", "short"):
                    continue
                target = D(event.get("qty") or "0")
                if not target.is_finite() or target <= 0:
                    continue
                open_side, close_side = ("sell", "buy") if direction == "short" else ("buy", "sell")
                nearby = [f for f in fills if f["symbol"] == symbol and f["side"] == open_side
                          and abs(float(f["fee_ts"])-float(event["ts"])) <= 5 and f.get("order_id")]
                orders = {str(f["order_id"]) for f in nearby}
                if len(orders) != 1:
                    continue
                opening_order = next(iter(orders))
                if opening_order in owned_ids:
                    continue
                opening = [f for f in fills if str(f.get("order_id") or "") == opening_order]
                if not opening or any(f["symbol"] != symbol or f["side"] != open_side for f in opening):
                    continue
                try:
                    volume = sum((D(f.get("volume") or "0") for f in opening), Decimal("0"))
                    tolerance = max(Decimal("0.00000001"), target*Decimal("0.000001"))
                    if not volume.is_finite() or abs(volume-target) > tolerance:
                        continue
                    first = min(float(f["fee_ts"]) for f in opening)
                    if float(state["start_ts"]) > first or first <= float(reference["close_ts"]):
                        continue
                    remaining, closing = volume, []
                    valid = True
                    for fill in (f for f in fills if f["symbol"] == symbol and float(f["fee_ts"]) >= first
                                 and str(f.get("order_id") or "") != opening_order):
                        qty = D(fill.get("volume") or "0")
                        if (fill["side"] != close_side or not fill.get("order_id")
                                or str(fill["order_id"]) in owned_ids or fill.get("profit") in (None, "")
                                or not qty.is_finite() or qty <= 0 or qty-remaining > tolerance):
                            valid = False
                            break
                        closing.append(fill)
                        remaining -= qty
                        if abs(remaining) <= tolerance:
                            break
                    if not valid or not closing or abs(remaining) > tolerance:
                        continue
                    last = max(float(f["fee_ts"]) for f in closing)
                    if float(state["end_ts"]) < last+2:
                        continue
                    overlap = [s for s in signals if s["symbol"] == symbol
                               and s["status"] in (ST_HOLDING, ST_UNKNOWN, ST_CLOSED)
                               and float(s.get("open_ts") or 0) <= last
                               and (not s.get("close_ts") or float(s["close_ts"]) >= first)]
                    if overlap:
                        continue
                    for fill in opening+closing:
                        if str(fill.get("fee_coin") or "USDT").upper() != "USDT":
                            valid = False
                        numbers = [D(fill.get(k) or "0") for k in ("volume", "price", "fee")]
                        if not all(x.is_finite() for x in numbers) or numbers[0] <= 0 or numbers[1] <= 0:
                            valid = False
                    gross = sum((D(f["profit"]) for f in closing), Decimal("0"))
                    if not valid or not gross.is_finite():
                        continue
                    cost = sum((D(f["price"])*D(f["volume"]) for f in opening), Decimal("0"))
                    close_qty = sum((D(f["volume"]) for f in closing), Decimal("0"))
                    close_cost = sum((D(f["price"])*D(f["volume"]) for f in closing), Decimal("0"))
                    actual = []
                    for raw in history_records:
                        if str(raw.get("symbol") or "") != symbol or str(raw.get("holdSide") or "") != direction:
                            continue
                        try:
                            position = parse_bitget_closed_position(raw)
                        except (ValueError, ArithmeticError, TypeError):
                            continue
                        if (abs(position["open_ts"]-first) <= 2 and abs(position["close_ts"]-last) <= 2
                                and abs(D(position["qty"])-volume) <= tolerance
                                and abs(D(position["avg_entry"])-cost/volume) <= max(Decimal("0.000000000001"),abs(cost/volume)*Decimal("0.000001"))
                                and abs(D(position["final_price"])-close_cost/close_qty) <= max(Decimal("0.000000000001"),abs(close_cost/close_qty)*Decimal("0.000001"))
                                and abs(D(position["gross_pnl"])-gross) <= Decimal("0.000001")
                                and abs(D(position["open_fee"])-sum((D(f["fee"]) for f in opening),Decimal("0"))) <= Decimal("0.000001")
                                and abs(D(position["close_fee"])-sum((D(f["fee"]) for f in closing),Decimal("0"))) <= Decimal("0.000001")):
                            actual.append(position)
                    if len(actual) != 1:
                        continue
                    position = actual[0]
                    sid = "recovered_bitget_" + position["exchange_position_id"]
                    if sid in existing_ids:
                        continue
                    fields = {"id":sid, "symbol":symbol, "exchange":exchange, "direction":direction,
                        "source":str(reference.get("source") or "未标注"), "status":ST_CLOSED,
                        "record_source":"exchange_recovered",
                        "recovered_event_id":int(event["id"]),
                        "qty":amount_str(volume), "base_qty":amount_str(volume), "notional":amount_str(cost),
                        "entry":amount_str(cost/volume), "avg_entry":amount_str(cost/volume),
                        "final_price":amount_str(close_cost/close_qty), "open_ts":first, "close_ts":last,
                        "open_time":ledger_time(first), "close_time":ledger_time(last),
                        "open_order_id":opening_order, "open_order_ids":opening_order,
                        "close_order_id":str(max(closing,key=lambda f:float(f["fee_ts"]))["order_id"]),
                        "open_fee":amount_str(sum((D(f["fee"]) for f in opening),Decimal("0"))),
                        "close_fee":amount_str(sum((D(f["fee"]) for f in closing),Decimal("0"))),
                        "gross_pnl":amount_str(gross), "net_pnl":"", "funding_fee":"",
                        "fee_source":"pending", "funding_source":"pending",
                        "fee_note":"交易所实际成交手续费，等待完整数量核验", "funding_note":"等待实际资金费账单归属",
                        "end_reason":"交易所补录", "end_note":"按完整实际成交补录缺失的已平仓记录",
                        "note":"原补开事件 "+str(event["signal_id"])+"；只补录已平历史，不恢复自动交易跟踪",
                        "platform_tracking":"0", "follow_martingale":"0"}
                    # 最终财务值及持有时间直接采用已与成交相互印证的交易所历史原值。
                    fields.update(position)
                    planned.append(fields)
                    proofs[sid] = {"event":dict(event), "reference_id":reference["id"],
                                   "fills":[dict(f) for f in opening+closing]}
                    existing_ids.add(sid)
                    owned_ids.update(str(f["order_id"]) for f in opening+closing)
                    signals.append(fields)
                except (ValueError, ArithmeticError, TypeError):
                    continue
            if not planned:
                return []
        # 备份会自行获取数据库锁，必须在规划锁外执行；写入事务内再复核凭据没有变化。
        self.backup_accounting_once("115")
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                for fields in planned:
                    proof = proofs[fields["id"]]
                    event = self._conn.execute("SELECT * FROM position_qty_events WHERE id=?", (fields["recovered_event_id"],)).fetchone()
                    if event is None or dict(event) != proof["event"]:
                        raise ValueError("补录前原补开事件已变化，保留原账等待重新核对")
                    if self._conn.execute("SELECT 1 FROM signals WHERE id=?", (event["signal_id"],)).fetchone():
                        raise ValueError("补开跟踪记录已由其他流程恢复，拒绝重复补录")
                    reference = self._conn.execute("SELECT * FROM signals WHERE id=?", (proof["reference_id"],)).fetchone()
                    if (reference is None or reference["status"] != ST_CLOSED
                            or any(reference[k] != fields[k] for k in ("symbol","exchange","direction"))
                            or str(reference["source"] or "未标注") != fields["source"]
                            or float(reference["close_ts"] or 0) >= fields["open_ts"]):
                        raise ValueError("补录前原归档身份已变化，保留原账等待重新核对")
                    for fill in proof["fills"]:
                        current = self._conn.execute("SELECT * FROM fee_events WHERE fill_id=?", (fill["fill_id"],)).fetchone()
                        if current is None or any(current[k] != fill[k] for k in
                                ("order_id","symbol","side","fee_ts","fee","fee_coin","volume","price","profit","exchange")):
                            raise ValueError("补录前实际成交流水已变化，保留原账等待重新核对")
                    current_signals = self._conn.execute(
                        "SELECT * FROM signals WHERE exchange=? AND symbol=? AND status IN (?,?,?)",
                        (exchange,fields["symbol"],ST_HOLDING,ST_UNKNOWN,ST_CLOSED)).fetchall()
                    if any(float(s["open_ts"] or 0) <= fields["close_ts"]
                           and (not s["close_ts"] or float(s["close_ts"]) >= fields["open_ts"])
                           for s in current_signals):
                        raise ValueError("补录前出现重叠仓位记录，拒绝重复归属")
                    columns = list(fields)
                    self._conn.execute("INSERT INTO signals("+",".join(columns)+") VALUES("
                        +",".join("?" for _ in columns)+")", [fields[k] for k in columns])
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        self.reconcile_signal_fees(exchange)
        self.reconcile_actual_funding(exchange)
        return [self.get_signal(s["id"]) for s in planned]

    @_dashboard_cached
    def ledger_start(self, exchange):
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(open_ts) FROM signals WHERE exchange=? AND open_ts>0 "
                "AND status IN (?,?,?)", (exchange, ST_HOLDING, ST_CLOSED, ST_UNKNOWN)).fetchone()
        if row and row[0]:
            first = datetime.fromtimestamp(float(row[0]), timezone(timedelta(hours=8)))
            return first.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        return day_start_ts()

    @_dashboard_cached
    def ledger_state(self, exchange, kind):
        with self._lock:
            row = self._conn.execute("SELECT * FROM ledger_sync WHERE exchange=? AND kind=?",
                                     (exchange, kind)).fetchone()
        return dict(row) if row else {}

    def ledger_error(self, exchange, kind, message):
        with self._lock:
            self._conn.execute(
                "INSERT INTO ledger_sync(exchange,kind,start_ts,end_ts,updated_ts,error) "
                "VALUES(?,?,0,0,0,?) ON CONFLICT(exchange,kind) DO UPDATE SET error=excluded.error",
                (exchange, kind, str(message)[:200]))
            self._conn.commit()

    def _coverage_locked(self, exchange, kind, start, end):
        old = self._conn.execute("SELECT * FROM ledger_sync WHERE exchange=? AND kind=?",
                                  (exchange, kind)).fetchone()
        if old and old["end_ts"] > 0:
            if start > old["end_ts"] + 0.01 or end < old["start_ts"] - 0.01:
                raise ValueError("同步区间不连续，不能标记为全量完成")
            start, end = min(start, old["start_ts"]), max(end, old["end_ts"])
        self._conn.execute(
            "INSERT OR REPLACE INTO ledger_sync(exchange,kind,start_ts,end_ts,updated_ts,error) "
            "VALUES(?,?,?,?,?,'')", (exchange, kind, start, end, time.time()))

    @_dashboard_cached
    def ledger_ready(self, exchange, kind, since=None):
        state = self.ledger_state(exchange, kind)
        begin = self.ledger_start(exchange) if since is None else float(since)
        return bool(state and not state.get("error") and state["start_ts"] <= begin
                    and state["end_ts"] >= time.time() - 180)

    def ledger_covers(self, exchange, kind, start, end):
        """已存历史证据按持有区间确认，不因当前增量请求失败而过期。"""
        state = self.ledger_state(exchange, kind)
        return bool(state and state["end_ts"] > 0 and state["start_ts"] <= float(start)
                    and state["end_ts"] >= float(end))

    def replace_fee_window(self, exchange, start, end, fills):
        """仅完整取回并校验成功后才替换；失败保留原账本和同步游标。"""
        items = {}
        for f in fills:
            ts = float(f.get("ts") or 0)
            if ts <= 0:
                raise ValueError("成交时间缺失，不能确认完整同步")
            if not start <= ts <= end:
                continue
            fid = str(f.get("fill_id") or "")
            if not fid or f.get("fee_known") is False:
                raise ValueError("成交号或实际手续费缺失，不能确认完整同步")
            if str(f.get("fee_coin") or "USDT").upper() != "USDT":
                raise ValueError("成交手续费币种不是 USDT，不能直接与 USDT 相加")
            if exchange == "gate":
                volume, price = D(f.get("volume") or "0"), D(f.get("price") or "0")
                if not volume.is_finite() or not price.is_finite() or volume <= 0 or price <= 0:
                    raise ValueError("Gate实际成交数量或价格缺失，拒绝覆盖账本")
            key = fid if exchange == "bitget" else exchange + ":" + fid.removeprefix(exchange + ":")
            item = (key, "", str(f.get("symbol") or ""), ts, ledger_time(ts),
                    amount_str(f.get("fee") or "0"), "USDT", str(f.get("side") or ""), exchange,
                    str(f.get("order_id") or ""), amount_str(f.get("price") or "0"),
                    amount_str(f.get("volume") or "0"),
                    amount_str(f["profit"]) if "profit" in f and f.get("profit_known") is not False else "")
            if key in items and items[key] != item:
                raise ValueError("同一成交号返回了不一致的数据")
            items[key] = item
        self.backup_accounting_once()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute("DELETE FROM fee_events WHERE exchange=? AND fee_ts>=? AND fee_ts<=?",
                                   (exchange, start, end))
                self._conn.executemany(
                    "INSERT OR REPLACE INTO fee_events "
                    "(fill_id,signal_id,symbol,fee_ts,fee_time,fee,fee_coin,side,exchange,order_id,price,volume,profit) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", list(items.values()))
                self._coverage_locked(exchange, "fees", start, end)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return len(items)

    def incomplete_fee_window(self, exchange):
        """定位已覆盖但数量或价格缺失的旧流水，只用于重新读取交易所原始成交。"""
        state = self.ledger_state(exchange, "fees")
        if not state or state.get("end_ts", 0) <= 0:
            return None
        bad = "(CAST(COALESCE(volume,'0') AS REAL)<=0 OR CAST(COALESCE(price,'0') AS REAL)<=0)"
        with self._lock:
            first = self._conn.execute(
                "SELECT fee_ts FROM fee_events WHERE exchange=? AND fee_ts>=? AND fee_ts<=? AND "
                + bad + " ORDER BY fee_ts LIMIT 1",
                (exchange, state["start_ts"], state["end_ts"])).fetchone()
            if first is None:
                return None
            day = datetime.fromtimestamp(float(first[0]), timezone(timedelta(hours=8))).replace(
                hour=0, minute=0, second=0, microsecond=0).timestamp()
            start, end = max(day, state["start_ts"]), min(day+86400, state["end_ts"])
            rows = self._conn.execute(
                "SELECT fill_id,order_id FROM fee_events WHERE exchange=? AND fee_ts>=? AND fee_ts<=? AND "
                + bad, (exchange, start, end)).fetchall()
        # 已知真实成交号必须重新取得，不能用空响应删除仍有费用的凭据。
        ids = [r["fill_id"] for r in rows if r["order_id"] and "|" not in r["fill_id"]]
        return {"start": start, "end": end, "fill_ids": ids}

    def reconcile_signal_fees(self, exchange):
        """从完整账户成交校准信号费用；数量或归属不明时保留待核对。"""
        state = self.ledger_state(exchange, "fees")
        if not state or state["end_ts"] <= 0:
            return
        with self._lock:
            signals = [dict(r) for r in self._conn.execute(
                "SELECT * FROM signals WHERE exchange=? AND status IN (?,?,?) ORDER BY close_ts",
                (exchange, ST_HOLDING, ST_CLOSED, ST_UNKNOWN)).fetchall()]
            fills = [dict(r) for r in self._conn.execute("SELECT * FROM fee_events WHERE exchange=?", (exchange,)).fetchall()]
            by_id = {s["id"]: s for s in signals}
            def root_of(s):
                root = s.get("accounting_parent") or s["id"].rsplit("_part", 1)[0]
                return root if root in by_id else s["id"]
            updates = []
            for sig in signals:
                if sig.get("record_source") == "exchange_recovered":
                    continue
                # 每条历史记录只要求其自身持有期间已完整覆盖。
                # 持仓开仓费由订单号及成交数量证明，不必等所有旧记录补完。
                opened_at = float(sig.get("open_ts") or 0)
                ended_at = float(sig.get("close_ts") or opened_at)
                if not self.ledger_covers_unlocked(state, opened_at, ended_at):
                    continue
                sid, root = sig["id"], root_of(sig)
                family = [s for s in signals if root_of(s) == root]
                order_ids = {x for x in str(sig.get("open_order_ids") or sig.get("open_order_id") or "").split(",") if x}
                owned = [f for f in fills if f["order_id"] in order_ids]
                found_ids = {f["order_id"] for f in owned}
                if root == sid:
                    if not order_ids or found_ids != order_ids:
                        continue
                    open_fee = sum((D(f["fee"]) for f in owned), Decimal("0"))
                    allocated_qty = Decimal("0")
                    for part in family:
                        if part["id"] != sid:
                            part_ids = {x for x in str(part.get("open_order_ids") or "").split(",") if x}
                            if not part_ids or part_ids <= order_ids:
                                open_fee -= D(part.get("open_fee") or "0")
                                allocated_qty += D(part.get("qty") or "0")
                    opening_qty = sum((D(f["volume"] or "0") for f in owned), Decimal("0"))
                    expected_qty = D(sig.get("qty") or "0") + allocated_qty
                    if abs(opening_qty-expected_qty) > max(Decimal("0.00000001"), expected_qty*Decimal("0.000001")):
                        continue
                else:
                    parent_ids = {x for x in str(by_id[root].get("open_order_ids") or by_id[root].get("open_order_id") or "").split(",") if x}
                    if not parent_ids or not parent_ids <= {f["order_id"] for f in fills}:
                        continue
                    open_fee = D(sig.get("open_fee") or "0")
                fields = {"open_fee": amount_str(open_fee), "fee_source": "exchange",
                          "fee_note": "交易所实际成交手续费" + ("（部分平仓分摊）" if root != sid else "")}
                if sig["status"] == ST_CLOSED:
                    close = float(sig.get("close_ts") or 0)
                    lower = max([float(sig.get("open_ts") or 0)-0.001] +
                                [float(s.get("close_ts") or 0) for s in family
                                 if 0 < float(s.get("close_ts") or 0) < close])
                    side = "buy" if sig["direction"] == "short" else "sell"
                    closes = [f for f in fills if f["symbol"] == sig["symbol"] and f["side"] == side
                              and lower < f["fee_ts"] <= close + 2]
                    volume = sum((D(f["volume"] or "0") for f in closes), Decimal("0"))
                    qty = D(sig.get("qty") or "0")
                    if qty <= 0 or abs(volume - qty) > max(Decimal("0.00000001"), qty * Decimal("0.000001")):
                        continue
                    fields["close_fee"] = amount_str(sum((D(f["fee"]) for f in closes), Decimal("0")))
                    if exchange == "bitget":
                        if any(f.get("profit") in (None, "") for f in closes):
                            continue
                        gross = sum((D(f["profit"] or "0") for f in closes), Decimal("0"))
                    else:
                        avg = D(sig.get("avg_entry") or sig.get("entry") or "0")
                        if avg <= 0:
                            continue
                        sign = Decimal("-1") if sig["direction"] == "short" else Decimal("1")
                        gross = sum((D(f["volume"]) * (D(f["price"]) - avg) * sign for f in closes), Decimal("0"))
                    fields["gross_pnl"] = amount_str(gross)
                    net = signal_net(dict(sig, **fields))
                    fields["net_pnl"] = amount_str(net) if net is not None else ""
                updates.append((sid, fields))
            try:
                for sid, fields in updates:
                    self._conn.execute("UPDATE signals SET " + ",".join(k+"=?" for k in fields) + " WHERE id=?",
                                       [*fields.values(), sid])
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def replace_funding_window(self, exchange, start, end, bills):
        items = {}
        for bill in bills:
            ts = float(bill.get("ts") or 0)
            if ts <= 0:
                raise ValueError("资金费账单时间缺失")
            if not start <= ts <= end:
                continue
            bid = str(bill.get("id") or "")
            if not bid or not bill.get("symbol") or bill.get("amount") in (None, ""):
                raise ValueError("资金费账单缺少编号、交易对或金额")
            key = exchange + ":" + bid
            row = (key, exchange, str(bill["symbol"]), ts, amount_str(bill["amount"]))
            if key in items and items[key] != row:
                raise ValueError("同一资金费账单号数据不一致")
            items[key] = row
        self.backup_accounting_once()
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute("DELETE FROM funding_allocations WHERE bill_id IN "
                                   "(SELECT bill_id FROM funding_bills WHERE exchange=? AND bill_ts>=? AND bill_ts<=?)",
                                   (exchange, start, end))
                self._conn.execute("DELETE FROM funding_bills WHERE exchange=? AND bill_ts>=? AND bill_ts<=?",
                                   (exchange, start, end))
                self._conn.executemany("INSERT OR REPLACE INTO funding_bills VALUES(?,?,?,?,?)", list(items.values()))
                self._coverage_locked(exchange, "funding", start, end)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        self.reconcile_actual_funding(exchange)
        return len(items)

    def reconcile_actual_funding(self, exchange):
        """实际账单归属明确才回填；无法归属的费用保留在账户流水并标记待核对。"""
        state = self.ledger_state(exchange, "funding")
        if not state or state["end_ts"] <= 0:
            return
        with self._lock:
            signals = [dict(r) for r in self._conn.execute(
                "SELECT * FROM signals WHERE exchange=? AND status IN (?,?,?)",
                (exchange, ST_HOLDING, ST_CLOSED, ST_UNKNOWN)).fetchall()]
            bills = self._conn.execute("SELECT * FROM funding_bills WHERE exchange=? ORDER BY bill_ts,bill_id",
                                       (exchange,)).fetchall()
            ids = {s["id"] for s in signals}
            totals = {sid: Decimal("0") for sid in ids}
            uncertain, allocations = set(), []
            for bill in bills:
                ts = float(bill["bill_ts"])
                candidates = [s for s in signals if s["symbol"] == bill["symbol"]
                              and float(s.get("open_ts") or 0) <= ts
                              and (not s.get("close_ts") or ts <= float(s["close_ts"]))]
                roots = {s.get("accounting_parent") or
                         (s["id"].rsplit("_part", 1)[0] if "_part" in s["id"]
                          and s["id"].rsplit("_part", 1)[0] in ids else s["id"])
                         for s in candidates}
                if not candidates or len(roots) != 1:
                    uncertain.update(s["id"] for s in candidates)
                    continue
                weights = {s["id"]: D(s.get("qty") or "0") for s in candidates}
                root = next(iter(roots))
                if len(candidates) > 1:
                    event = self._conn.execute(
                        "SELECT qty FROM position_qty_events WHERE signal_id=? AND ts<=? ORDER BY ts DESC,id DESC LIMIT 1",
                        (root, ts)).fetchone()
                    if root not in weights or event is None:
                        uncertain.update(s["id"] for s in candidates)
                        continue
                    weights[root] = D(event[0]) - sum((v for k,v in weights.items() if k != root), Decimal("0"))
                quantity = sum(weights.values(), Decimal("0"))
                if quantity <= 0 or any(v < 0 for v in weights.values()):
                    uncertain.update(weights)
                    continue
                remaining = D(bill["amount"])
                for i, s in enumerate(candidates):
                    value = remaining if i == len(candidates)-1 else D(bill["amount"]) * weights[s["id"]] / quantity
                    remaining -= value
                    totals[s["id"]] += value
                    allocations.append((bill["bill_id"], s["id"], amount_str(value)))
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute("DELETE FROM funding_allocations WHERE bill_id IN "
                                   "(SELECT bill_id FROM funding_bills WHERE exchange=?)", (exchange,))
                self._conn.executemany("INSERT INTO funding_allocations VALUES(?,?,?)", allocations)
                for sig in signals:
                    if sig.get("record_source") == "exchange_recovered":
                        continue
                    sid = sig["id"]
                    close = float(sig.get("close_ts") or state["end_ts"])
                    covered = (float(sig.get("open_ts") or 0) >= state["start_ts"]
                               and close <= state["end_ts"] and sid not in uncertain)
                    if not covered:
                        self._conn.execute("UPDATE signals SET funding_source='pending' WHERE id=?", (sid,))
                        continue
                    funding = amount_str(totals[sid])
                    changed = dict(sig, funding_fee=funding, funding_source="exchange")
                    net = signal_net(changed) if sig.get("gross_pnl") not in (None, "") else None
                    self._conn.execute(
                        "UPDATE signals SET funding_fee=?,funding_source='exchange',funding_note=?,net_pnl=? WHERE id=?",
                        (funding, "交易所实际资金费账单", amount_str(net) if net is not None else "", sid))
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    @_dashboard_cached
    def actual_fees(self, exchange="", since=0):
        sql, args = "SELECT fee FROM fee_events WHERE fee_ts>=?", [float(since)]
        if exchange:
            sql += " AND exchange=?"
            args.append(exchange)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return sum((D(r[0]) for r in rows), Decimal("0"))

    @staticmethod
    def ledger_covers_unlocked(state, start, end):
        return bool(start > 0 and state.get("start_ts", 0) <= start and state.get("end_ts", 0) >= end)

    @_dashboard_cached
    def actual_funding(self, exchange="", since=0):
        sql, args = "SELECT amount FROM funding_bills WHERE bill_ts>=?", [float(since)]
        if exchange:
            sql += " AND exchange=?"
            args.append(exchange)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return sum((D(r[0]) for r in rows), Decimal("0"))

    # ---- 手续费流水（按成交时间归集，不在单笔入库时舍入） ----
    def record_fee_events(self, fills, signal_id="", exchange=""):
        """把成交明细里的手续费按成交时间写入流水；fill_id 幂等去重。
        返回本次新增的条数。重复查询同一批成交不会重复累加。"""
        if not fills:
            return 0
        added = 0
        with self._lock:
            if not exchange and signal_id:
                srow = self._conn.execute(
                    "SELECT exchange FROM signals WHERE id=?", (signal_id,)).fetchone()
                exchange = str((srow["exchange"] if srow else "") or "")
            ex_val = str(exchange or "bitget")
            for f in fills:
                try:
                    ts = float(f.get("ts") or 0)
                    fee_str = amount_str(f.get("fee") or "0")
                except Exception:
                    continue
                if ts <= 0:
                    continue
                fid = str(f.get("fill_id") or "")
                if not fid:
                    # 老接口/测试数据可能没有 tradeId：用订单号+时间+价格+数量拼一个稳定键
                    fid = "|".join(str(x) for x in (
                        f.get("order_id") or "", int(ts),
                        f.get("price") or "", f.get("volume") or ""))
                # 两家交易所的 tradeId 可能撞号；Gate 的成交号加前缀，Bitget 保持原样兼容老数据
                if ex_val != "bitget" and not fid.startswith(ex_val + ":"):
                    fid = f"{ex_val}:{fid}"
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO fee_events"
                    "(fill_id,signal_id,symbol,fee_ts,fee_time,fee,fee_coin,side,exchange) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (fid, str(signal_id or ""), str(f.get("symbol") or ""), ts,
                     ledger_time(ts),
                     fee_str, str(f.get("fee_coin") or "USDT"),
                     str(f.get("side") or ""), ex_val))
                if cur.rowcount and cur.rowcount > 0:
                    added += 1
            self._conn.commit()
        return added

    def today_fee_events(self, exchange=""):
        """当日按成交时间归集的手续费（成交明细写入的流水）。
        返回 (金额, 笔数)。"""
        t = today_str()
        ex_sql = " AND exchange=?" if exchange else ""
        ex_args = [exchange] if exchange else []
        with self._lock:
            rows = self._conn.execute(
                "SELECT fee FROM fee_events WHERE fee_time LIKE ?" + ex_sql,
                (t + "%",) + tuple(ex_args)).fetchall()
        total = Decimal("0")
        for r in rows:
            try:
                total += D(r["fee"] or "0")
            except Exception:
                pass
        return total, len(rows)

    # ---- 今日统计 ----
    @_dashboard_cached
    def today_stats(self, exchange=""):
        t = today_str()
        ex_sql = " AND exchange=?" if exchange else ""
        ex_args = [exchange] if exchange else []
        with self._lock:
            def one(sql, args):
                return self._conn.execute(sql, args).fetchone()[0]

            opened = one(
                "SELECT COUNT(*) FROM signals WHERE status IN (?,?,?) AND open_time LIKE ?"
                + ex_sql,
                (ST_HOLDING, ST_CLOSED, ST_UNKNOWN, t + "%") + tuple(ex_args))
            closed = one(
                "SELECT COUNT(*) FROM signals WHERE status=? AND close_time LIKE ?" + ex_sql,
                (ST_CLOSED, t + "%") + tuple(ex_args))
            skipped = one(
                "SELECT COUNT(*) FROM signals WHERE status IN (?,?) AND open_time LIKE ?"
                + ex_sql,
                (ST_SKIPPED, ST_DROPPED, t + "%") + tuple(ex_args))
            rows = self._conn.execute(
                "SELECT skip_reason, COUNT(*) c FROM signals "
                "WHERE status IN (?,?) AND open_time LIKE ? AND skip_reason!=''" + ex_sql
                + " GROUP BY skip_reason ORDER BY c DESC",
                (ST_SKIPPED, ST_DROPPED, t + "%") + tuple(ex_args)).fetchall()
            reasons = {r["skip_reason"]: r["c"] for r in rows}
            # 当日已平仓信号的收益%和净盈亏（用于毛盈亏与净盈亏统计）
            rows_closed = self._dashboard_rows((ST_CLOSED,), exchange, "close_time", t)
            if rows_closed is None:
                rows_closed = self._conn.execute(
                    "SELECT * FROM signals "
                    "WHERE status=? AND close_time LIKE ?" + ex_sql,
                    (ST_CLOSED, t + "%") + tuple(ex_args)).fetchall()
            # 今日手续费 = 今日开仓/加仓已付的手续费 + 今日平仓的平仓手续费
            rows_open_fee = self._dashboard_rows((ST_HOLDING, ST_CLOSED, ST_UNKNOWN), exchange, "open_time", t)
            if rows_open_fee is None:
                rows_open_fee = self._conn.execute(
                    "SELECT open_fee FROM signals WHERE status IN (?,?,?) AND open_time LIKE ?"
                    + ex_sql,
                    (ST_HOLDING, ST_CLOSED, ST_UNKNOWN, t + "%") + tuple(ex_args)).fetchall()
            rows_close_fee = rows_closed
            # 今日资金费（正数=支付，负数=收取）
            rows_funding = self._conn.execute(
                "SELECT amount FROM funding_events WHERE funding_time LIKE ?" + ex_sql,
                (t + "%",) + tuple(ex_args)).fetchall()
        pnl = Decimal("0")      # 毛盈亏（按服务器收益% × 名义价值换算，用于熔断）
        fees = Decimal("0")     # 当日手续费合计（含仍在持仓的已付手续费）
        net = Decimal("0")      # 当日已实现净盈亏合计（有记录才累加）
        missing_net = 0
        funding = Decimal("0")  # 当日资金费
        for r in rows_closed:
            row_net = self._dashboard_net(r)
            if row_net is not None:
                net += row_net
            else:
                missing_net += 1
            # 优先用交易所实际毛盈亏（已扣完手续费/资金费前）；没有时退回平台收益%×名义价值
            if r["gross_pnl"] not in (None, ""):
                try:
                    pnl += D(r["gross_pnl"])
                    continue
                except Exception:
                    pass
            try:
                pnl += D(r["profit_percent"] or "0") * D(r["notional"] or "0") / Decimal("100")
            except Exception:
                pass  # 拿不到收益%就按 0 计入，不凭空估算
        for r in rows_open_fee:
            try:
                fees += D(r["open_fee"] or "0")
            except Exception:
                pass
        for r in rows_close_fee:
            try:
                fees += D(r["close_fee"] or "0")
            except Exception:
                pass
        for r in rows_funding:
            try:
                funding += D(r["amount"] or "0")
            except Exception:
                pass
        # 今日手续费：只要今天已完整同步过一次该交易所的成交明细，就以「成交时间」为准。
        # 这样老仓位今天加仓的手续费也算进今天，且不会随之后加仓改动历史数字。
        # 同步标记不匹配（首次启动还没拉到成交）时，回退到上面的按开仓日估算口径。
        sync_key = "fee_events_synced_day_gate" if exchange == "gate" else "fee_events_synced_day"
        if self.get_setting(sync_key, "") == t:
            fees, _fee_count = self.today_fee_events(exchange)
        names = self.accounting_exchanges(exchange)
        fees_ok = bool(names) and all(self.ledger_ready(n, "fees", day_start_ts()) for n in names)
        funding_ok = bool(names) and all(self.ledger_ready(n, "funding", day_start_ts()) for n in names)
        if fees_ok:
            fees = self.actual_fees(exchange, day_start_ts())
        if funding_ok:
            funding = self.actual_funding(exchange, day_start_ts())
        return {"opened": opened, "closed": closed, "skipped": skipped,
                "skip_reasons": reasons, "realized_pnl": pnl,
                "fees": fees, "funding": funding, "net_pnl": net if not missing_net else None,
                "missing_count": missing_net,
                "fees_verified": fees_ok, "funding_verified": funding_ok,
                "net_verified": self.net_quality(exchange, "today")["verified"]}


# ==================== 参数管理（含校验，错误提示全部中文） ====================

class Params:
    def __init__(self, db):
        self.db = db

    def all(self):
        return self.parse(self.db.get_settings(PARAM_DEFAULTS))

    @staticmethod
    def parse(raw):
        p = dict(raw)
        p["notional"] = D(p.get("notional") or "0")
        p["head_multiplier"] = D(p.get("head_multiplier") or "1")
        p["leverage"] = int(p.get("leverage") or 0)
        mode = str(p.get("margin_mode") or "crossed").strip().lower()
        p["margin_mode"] = "isolated" if mode == "isolated" else "crossed"
        p["max_entry_deviation_pct"] = D(p.get("max_entry_deviation_pct") or "0")
        p["max_positions"] = int(p.get("max_positions") or 0)
        p["max_daily_opens"] = int(p.get("max_daily_opens") or 0)
        p["sources"] = [s.strip() for s in str(p.get("sources") or "").split(",") if s.strip()]
        p["directions"] = [d.strip() for d in str(p.get("directions") or "").split(",") if d.strip()]
        p["sim_mode"] = str(p.get("sim_mode") or "1") in ("1", "true", "True", "on", "yes")
        p["fuse_enabled"] = str(p.get("fuse_enabled") or "1") in ("1", "true", "True", "on", "yes")
        p["local_stop_enabled"] = str(p.get("local_stop_enabled") or "1") in ("1", "true", "True", "on", "yes")
        p["gate_enabled"] = str(p.get("gate_enabled") or "1") in ("1", "true", "True", "on", "yes")
        route = str(p.get("new_entry_route") or "gate_first").strip().lower()
        p["new_entry_route"] = route if route in (
            "gate_first", "gate_only", "bitget_first", "bitget_only") else "gate_first"
        return p

    def effective_total_funds(self, exchange="bitget"):
        """账户总资金：填了用填的；留空按公式自动算。
        Gate 用自己的“Gate 账户总资金”，没填时回退到“账户总资金”。"""
        p = self.all()
        if exchange == "gate":
            if p.get("gate_total_funds"):
                try:
                    return D(p["gate_total_funds"])
                except Exception:
                    pass
        if p.get("total_funds"):
            try:
                return D(p["total_funds"])
            except Exception:
                pass
        return default_total_funds(p["max_positions"], p["notional"], p["leverage"])

    def validate(self, raw):
        """校验网页提交的参数。返回错误列表（空列表 = 全部合法）。
        没有提交的字段按当前已保存的值校验（支持只改其中几个参数）。"""
        full = {}
        for k in PARAM_DEFAULTS:
            v = raw.get(k)
            full[k] = self.db.get_setting(k, PARAM_DEFAULTS[k]) if v is None else v
        raw = full
        errors = []
        try:
            n = D(raw.get("notional"))
            if n <= 0:
                errors.append("名义价值必须大于 0")
        except Exception:
            errors.append("名义价值必须是数字")
        try:
            hm = D(raw.get("head_multiplier"))
            if not (Decimal("0.01") <= hm <= Decimal("100")):
                errors.append("头仓仓位倍数必须在 0.01-100 之间")
        except Exception:
            errors.append("头仓仓位倍数必须是数字")
        try:
            lev = int(raw.get("leverage"))
            if not (1 <= lev <= 125):
                errors.append("杠杆必须在 1-125 之间")
        except Exception:
            errors.append("杠杆必须是 1-125 的整数")
        if str(raw.get("margin_mode") or "crossed").strip().lower() not in ("crossed", "isolated"):
            errors.append("保证金模式只能是全仓（crossed）或逐仓（isolated）")
        try:
            dev = D(raw.get("max_entry_deviation_pct"))
            if not (0 <= dev <= 100):
                errors.append("最大入场偏离% 必须在 0-100 之间")
        except Exception:
            errors.append("最大入场偏离% 必须是数字")
        try:
            mp = int(raw.get("max_positions"))
            if not (0 <= mp <= 1000):
                errors.append("最大同时持仓数必须在 0-1000 之间（0 = 不限制）")
        except Exception:
            errors.append("最大同时持仓数必须是整数（0 = 不限制）")
        try:
            md = int(raw.get("max_daily_opens"))
            if not (0 <= md <= 100000):
                errors.append("每日最大开仓次数必须在 0-100000 之间（0 = 不限制）")
        except Exception:
            errors.append("每日最大开仓次数必须是整数（0 = 不限制）")
        tf = str(raw.get("total_funds") or "").strip()
        if tf:
            try:
                if D(tf) <= 0:
                    errors.append("账户总资金必须大于 0（也可以留空自动计算）")
            except Exception:
                errors.append("账户总资金必须是数字")
        gtf = str(raw.get("gate_total_funds") or "").strip()
        if gtf:
            try:
                if D(gtf) <= 0:
                    errors.append("Gate 账户总资金必须大于 0（也可以留空回退账户总资金）")
            except Exception:
                errors.append("Gate 账户总资金必须是数字")
        route = str(raw.get("new_entry_route") or "gate_first").strip().lower()
        if route not in ("gate_first", "gate_only", "bitget_first", "bitget_only"):
            errors.append("新开仓路由只能是 Gate 优先/只用 Gate/Bitget 优先/只用 Bitget")
        sources = [s.strip() for s in str(raw.get("sources") or "").split(",") if s.strip()]
        bad = [s for s in sources if s not in ALL_SOURCES]
        if bad:
            errors.append(f"来源筛选包含未知选项：{'、'.join(bad)}")
        directions = [d.strip() for d in str(raw.get("directions") or "").split(",") if d.strip()]
        bad = [d for d in directions if d not in DIRECTION_NAMES]
        if bad:
            errors.append(f"方向筛选包含未知选项：{'、'.join(bad)}")
        return errors

    def save(self, raw):
        """校验并保存。返回 (是否成功, 错误列表)"""
        errors = self.validate(raw)
        if errors:
            return False, errors
        for k in PARAM_DEFAULTS:
            if k in raw:
                self.db.set_setting(k, raw[k])
        return True, []


# ==================== 交易所对接（Bitget USDT 本位永续） ====================

class OrderResult:
    """下单结果：ok=True 成功；ok=False 明确失败；ok=None 结果未知（网络超时，禁止盲目重试）"""

    def __init__(self, ok, error="", simulated=False, order_id=""):
        self.ok = ok
        self.error = error
        self.simulated = simulated
        self.order_id = order_id


class ExchangeApiError(Exception):
    """交易所明确拒绝（带错误码），不是网络问题"""

    def __init__(self, code, raw_msg, friendly):
        super().__init__(friendly)
        self.code = code
        self.raw_msg = raw_msg
        self.friendly = friendly


class ExchangeNetworkError(Exception):
    """连接交易所失败 / 超时，订单结果未知"""


def parse_contract_filters(payload):
    """从 Bitget contracts 接口解析下单需要的精度参数和费率。
    - stepSize：数量精度 = 10 的负 volumePlace 次方（如 volumePlace=4 -> 0.0001）
    - tickSize：价格精度 = 10 的负 pricePlace 次方
    - minQty：最小下单数量 minTradeNum
    - minNotional：最小名义价值 minTradeUSDT
    - takerFeeRate：吃单费率（模拟盘估算手续费、实盘兜底用）"""
    try:
        item = payload[0] if isinstance(payload, list) else payload
        price_place = int(item.get("pricePlace") or 2)
        volume_place = int(item.get("volumePlace") or 4)
        step = Decimal(1).scaleb(-volume_place)
        tick = Decimal(1).scaleb(-price_place)
        filters = {
            "stepSize": str(step),
            "tickSize": str(tick),
            "minQty": str(item.get("minTradeNum") or "0"),
            "minNotional": str(item.get("minTradeUSDT") or "0"),
            "takerFeeRate": str(item.get("takerFeeRate") or DEFAULT_TAKER_FEE),
            "symbolStatus": str(item.get("symbolStatus") or "normal"),
        }
        return filters
    except Exception:
        return None


def parse_fill(fill):
    """解析 Bitget 成交明细里的「手续费」和「已实现盈亏」。
    feeDetail 在不同接口版本里可能是对象 / 数组 / JSON 字符串，全部兼容。"""
    fee = Decimal("0")
    coins = set()
    fee_known = False
    try:
        fd = fill.get("feeDetail")
        if isinstance(fd, str):
            try:
                fd = json.loads(fd)
            except Exception:
                fd = None
        if isinstance(fd, dict):
            if "totalFee" in fd:
                fee -= D(fd["totalFee"])
                fee_known = fd["totalFee"] not in (None, "")
                coins.add(fd.get("feeCoin") or fd.get("feeCoinCode") or "USDT")
            else:
                for k, v in fd.items():
                    if isinstance(v, dict) and "totalFee" in v:
                        fee -= D(v["totalFee"])
                        fee_known = True
                        coins.add(v.get("feeCoin") or v.get("feeCoinCode") or k)
        elif isinstance(fd, list):
            for v in fd:
                if isinstance(v, dict) and v.get("totalFee") not in (None, ""):
                    fee -= D(v.get("totalFee") or 0)
                    fee_known = True
                    coins.add(v.get("feeCoin") or v.get("feeCoinCode") or "USDT")
        if fee == 0 and fill.get("fee") not in (None, ""):
            fee = -D(fill["fee"])
            fee_known = True
    except Exception:
        fee_known = False
    profit_known = fill.get("profit") not in (None, "")
    try:
        profit = D(fill.get("profit") or "0")
    except Exception:
        profit = Decimal("0")
        profit_known = False
    try:
        price = D(fill.get("price") or "0")
    except Exception:
        price = Decimal("0")
    try:
        volume = D(fill.get("baseVolume") or "0")
    except Exception:
        volume = Decimal("0")
    try:
        ts = int(fill.get("cTime") or 0) / 1000.0
    except Exception:
        ts = 0.0
    return {"fee": fee, "profit": profit, "price": price, "volume": volume,
            "fee_coin": (next(iter(coins)) if len(coins) == 1 else ("MIXED" if coins else "USDT")),
            "fee_known": fee_known and fee.is_finite(),
            "profit_known": profit_known and profit.is_finite(),
            "fill_id": str(fill.get("tradeId") or fill.get("id") or ""),
            "order_id": str(fill.get("orderId") or ""),
            "symbol": str(fill.get("symbol") or ""),
            "side": str(fill.get("side") or "").lower(),
            "ts": ts}


def gate_friendly(label, message=""):
    """Gate 错误码转中文提示。label 是 Gate 返回的机器码，message 是原始说明。"""
    label = str(label or "")
    message = str(message or "")
    mapping = {
        "INVALID_KEY": "Gate API Key 无效或未启用（确认是 gate.io 的 API v4 Key，账户类型选“经典”）",
        "SIGNATURE_MISMATCH": "Gate 签名不匹配（检查 Secret 是否复制完整）",
        "FORBIDDEN": "该 Gate API Key 没有对应权限（合约交易需要“读写”权限）",
        "USER_NOT_FOUND": "Gate 合约账户还没开通（先在网页把 USDT 划转到合约账户）",
        "CONTRACT_NOT_FOUND": "Gate 没有该交易对",
        "INSUFFICIENT_AVAILABLE": "Gate 可用保证金不足",
        "POSITION_NOT_FOUND": "Gate 没有该持仓",
        "ORDER_NOT_FOUND": "Gate 订单不存在",
        "REDUCE_ONLY": "Gate 减仓单被拒（可能已无对应持仓）",
        "TOO_BUSY": "Gate 服务繁忙，稍后重试",
        "INVALID_PARAM_VALUE": "Gate 参数不合法",
        "AMOUNT_TOO_LITTLE": "Gate 下单数量低于最小值",
    }
    friendly = mapping.get(label)
    if friendly:
        return f"{friendly}（{label}：{message}）" if message else friendly
    return f"{message or 'Gate 拒绝请求'}（{label or '未知错误'}）"


def parse_gate_contract_filters(contract):
    """把 Gate 合约详情转成和 Bitget 一样的下单精度参数。
    Gate 的 size 是“合约张数”，quanto_multiplier 是每张对应多少个基础币。
    这里把张数换算成基础币数量，让引擎现有的 notional/price 计算不用改。"""
    try:
        quanto = D(contract.get("quanto_multiplier") or "0")
        if quanto <= 0:
            return None
        decimal_size = bool(contract.get("enable_decimal"))
        # Gate 小数张合约的 order_size_min 返回 0，但实际最小下单量与成交步进都是 0.1 张：

        # 0.54/0.65/0.69 张都按 0.5/0.6/0.6 张成交。不能按 0.01 张算，
        # 否则会算出 Gate 不接受的零散张数（0.06 张这类），白下一次被拒的市价单。
        size_step = D("0.1") if decimal_size else D("1")
        min_size = max(D(contract.get("order_size_min") or "0"),
                       size_step if decimal_size else Decimal("0"))
        status = str(contract.get("status") or "")
        if contract.get("in_delisting"):
            status_label = "delisting"
        elif status == "trading":
            status_label = "normal"
        else:
            status_label = status or "off"
        return {
            "stepSize": str(quanto * size_step),
            "tickSize": str(contract.get("order_price_round") or "0.0001"),
            "minQty": str(quanto * min_size),
            # Gate 只按张数（order_size_min）限制下单量，官方没有最小名义价值；

            "minNotional": "0",
            "takerFeeRate": str(contract.get("taker_fee_rate") or GATE_DEFAULT_TAKER_FEE),
            "symbolStatus": status_label,
            "contractSize": str(quanto),
            "enableDecimal": decimal_size,
            "orderSizeMin": str(min_size),
        }
    except Exception:
        return None


def parse_gate_fill(fill, contract_size, symbol=""):
    """把 Gate 个人成交（my_trades）转成和 Bitget 统一的 fill 结构。
    Gate 的 size 是张数，按 quanto_multiplier 换成基础币数量；
    Gate 没有逐笔已实现盈亏，profit 先记 0，由引擎按平仓均价补算。"""
    try:
        size = D(fill.get("size") or "0")
        price = D(fill.get("price") or "0")
        multiplier = D(contract_size or "0")
    except Exception as exc:
        raise ValueError("Gate实际成交张数、面值或价格无法解析") from exc
    if (not all(x.is_finite() for x in (size, price, multiplier))
            or size == 0 or price <= 0 or multiplier <= 0):
        raise ValueError("Gate实际成交张数、面值或价格缺失，不能记为零数量")
    volume = abs(size) * multiplier
    fee_known = fill.get("fee") not in (None, "")
    try:
        fee = D(fill.get("fee") or "0")
    except Exception:
        fee = Decimal("0")
        fee_known = False
    try:
        ts = float(fill.get("create_time") or 0)
    except Exception:
        ts = 0.0
    return {
        "fee": fee, "profit": Decimal("0"), "profit_known": False, "price": price, "volume": volume,
        "fee_coin": "USDT", "fee_known": fee_known and fee.is_finite(),
        # Gate 两个成交接口字段不同：/my_trades 用 id，/my_trades_timerange 用 trade_id。
        # 只读 id 会让同一笔成交生成两个不同的流水键，导致手续费重复累计。
        "fill_id": str(fill.get("id") or fill.get("trade_id") or ""),
        "order_id": str(fill.get("order_id") or ""), "symbol": str(symbol or ""),
        "side": "buy" if size >= 0 else "sell", "ts": ts,
        "contract_size": str(contract_size or ""),
        "contract_size_signed": str(size),
    }


def gate_text_id(client_id):
    """Gate 自定义订单号 text：t- 开头、最多 28 字节、只能 ASCII。"""
    raw = re.sub(r"[^0-9A-Za-z_.-]", "", str(client_id or ""))
    if not raw:
        raw = "sig"
    if len(raw) <= 25:
        return "t-" + raw
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()[:6]
    return "t-" + raw[:18] + "-" + digest


def parse_platform_active(payload):
    """解析平台 /api/v1/signals/active 的返回，提取每个信号的实时收益%。
    兼容 list、{"data":[...]}、{"data":{"rows":[...]}} 等包装；有收益%就用，
    没有则用当前价和均价按方向计算。返回 {signal_id: {"percent": float|None, ...}}。"""
    rows = None
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        for key in ("data", "rows", "list", "signals"):
            v = payload.get(key)
            if isinstance(v, list):
                rows = v
                break
            if isinstance(v, dict):
                for kk in ("rows", "list", "signals"):
                    if isinstance(v.get(kk), list):
                        rows = v[kk]
                        break
                if rows is not None:
                    break
    if rows is None:
        return {}

    def num(v):
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            try:
                return float(v.replace("%", "").strip())
            except Exception:
                return None
        return None

    out = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        sid = str(r.get("id") or r.get("signal_id") or r.get("signalId") or "")
        if not sid:
            continue
        pct = None
        for key in ("profit_percent", "profitPercent", "pnl_percent", "pnlPercent",
                    "roe", "profit_rate", "profitRate"):
            pct = num(r.get(key))
            if pct is not None:
                break
        price = None
        for key in ("price", "current_price", "currentPrice", "mark_price",
                    "markPrice", "last_price", "lastPrice"):
            price = num(r.get(key))
            if price is not None:
                break
        avg = None
        for key in ("avg", "avg_price", "avgPrice", "entry", "open_price",
                    "openPrice", "entry_price"):
            avg = num(r.get(key))
            if avg is not None:
                break
        if pct is None and price and avg:
            direction = str(r.get("direction") or "").lower()
            sign = 1 if direction in ("long", "buy", "多") else -1
            pct = (price - avg) / avg * 100 * sign
        out[sid] = {"percent": pct, "price": price, "avg": avg}
    return out


class SimExchange:
    """模拟盘：所有“下单”只记日志，不连接任何真实交易所，不花一分钱。
    取标记价 / 精度参数走 Bitget 公开接口（不需要密钥），失败时由引擎兜底。"""

    name = "模拟盘"

    def __init__(self, price_fn=None, filters=None):
        self.price_fn = price_fn        # 测试时可注入固定取价函数（不联网）
        self.fixed_filters = filters    # 测试时可注入固定过滤器参数
        self._filters_cache = {}
        self.last_contract_error = ""   # 最近一次查合约信息失败的原因（模拟盘也要真实校验合约）
        self.last_price_error = ""      # 最近一次取标记价失败的原因

    async def close(self):
        pass

    async def fetch_mark_prices(self, symbols=None):
        """批量标记价：测试注入价格时直接返回；正式模拟盘没有注入，返回 None 让引擎逐个取。"""
        if self.price_fn is None:
            return None
        out = {}
        for sym in (symbols or []):
            try:
                out[str(sym).upper()] = D(self.price_fn(sym))
            except Exception:
                continue
        return out

    async def fetch_mark_price(self, symbol):
        if self.price_fn is not None:
            return D(self.price_fn(symbol))
        if httpx is None:
            return None
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(f"{BITGET_BASE}/api/v2/mix/market/ticker",
                                params={"symbol": symbol, "productType": PRODUCT_TYPE})
                if r.status_code == 200:
                    payload = r.json()
                    code = str(payload.get("code", ""))
                    if code not in ("00000", "0"):
                        self.last_price_error = f"{payload.get('msg', '')}（错误码 {code}）"
                        return None
                    data = payload.get("data")
                    if isinstance(data, list):
                        data = data[0] if data else {}
                    if isinstance(data, dict) and data.get("markPrice"):
                        return D(data["markPrice"])
                    self.last_price_error = "Bitget 未返回标记价"
                else:
                    self.last_price_error = f"HTTP {r.status_code}"
        except Exception as e:
            self.last_price_error = f"网络异常（{e.__class__.__name__}）"
        return None

    async def fetch_lot_filters(self, symbol):
        if self.fixed_filters is not None:
            return dict(self.fixed_filters)
        if httpx is None:
            return None
        now = time.time()
        hit = self._filters_cache.get(symbol)
        if hit and now - hit[0] < 3600:
            return hit[1]
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(f"{BITGET_BASE}/api/v2/mix/market/contracts",
                                params={"symbol": symbol, "productType": PRODUCT_TYPE})
                if r.status_code == 200:
                    payload = r.json()
                    code = str(payload.get("code", ""))
                    if code not in ("00000", "0"):
                        self.last_contract_error = f"{payload.get('msg', '')}（错误码 {code}）"
                        return None
                    out = parse_contract_filters(payload.get("data"))
                    if out:
                        self._filters_cache[symbol] = (now, out)
                    else:
                        self.last_contract_error = "Bitget 未返回该交易对的合约信息"
                    return out
                self.last_contract_error = f"HTTP {r.status_code}"
        except Exception as e:
            self.last_contract_error = f"网络异常（{e.__class__.__name__}）"
        return None

    async def set_leverage(self, symbol, leverage, hold_side=None):
        return True

    async def set_margin_mode(self, symbol, mode="isolated"):
        return True, ""

    async def ensure_margin_mode(self, symbol, mode="isolated", tries=3, delay=0.4):
        return True, ""

    async def fetch_account_config(self, symbol, strict=False, fresh=False):
        return {"margin_mode": "crossed", "hold_mode": "one_way_mode"}

    async def market_open(self, symbol, side, qty, client_id, margin_mode=None):
        return OrderResult(True, simulated=True)

    async def place_limit_open(self, symbol, side, qty, price, client_id, margin_mode=None):
        return OrderResult(True, simulated=True)

    async def cancel_order(self, symbol, order_id):
        return True

    async def place_stop_close_position(self, symbol, hold_side, stop_price, client_id, qty=None):
        return OrderResult(True, simulated=True)

    async def cancel_all_orders(self, symbol):
        return True

    async def cancel_plan_orders(self, symbol):
        return True

    async def fetch_open_orders(self, symbol=""):
        return []

    async def fetch_order_detail(self, symbol, order_id):
        return {}

    async def market_close(self, symbol, side, qty, client_id):
        return OrderResult(True, simulated=True)

    async def get_order_fills(self, symbol, order_id):
        return []

    async def get_symbol_fills(self, symbol, start_ts, end_ts):
        return []

    async def get_all_fills(self, start_ts, end_ts):
        return []

    async def fetch_positions(self):
        return []


class BitgetExchange:
    """真实交易所：Bitget USDT 本位永续合约（单向持仓模式）。所有下单都花真实资金。

    签名规则：sign = Base64( HMAC-SHA256( secret, 时间戳 + 方法 + 路径 + 查询串/请求体 ) )
    请求头：ACCESS-KEY / ACCESS-SIGN / ACCESS-TIMESTAMP / ACCESS-PASSPHRASE
    """

    name = "Bitget 合约"

    def __init__(self, api_key, secret, passphrase):
        if httpx is None:
            raise RuntimeError("缺少依赖 httpx，请先运行「启动.bat」或「启动.command」自动安装")
        self.api_key = api_key
        self.secret = secret
        self.passphrase = passphrase
        self.client = httpx.AsyncClient(base_url=BITGET_BASE, timeout=httpx.Timeout(10.0))
        self._filters_cache = {}      # symbol -> (缓存时间戳, 参数)。合约精度要缓存，不要每次都查
        self._account_cache = {}      # symbol -> (缓存时间戳, 保证金模式/持仓模式)
        self.last_contract_error = "" # 最近一次查合约信息失败的原因
        self.last_price_error = ""    # 最近一次取标记价失败的原因
        self._non_ascii_sign_raw = None  # 中文参数 GET：None=未确定，True=用原始查询串签名，False=用编码串

    async def close(self):
        try:
            await self.client.aclose()
        except Exception:
            pass

    # ---- 内部：签名与请求 ----
    def _signed_headers(self, method, path, payload_str):
        ts = str(int(time.time() * 1000))
        msg = ts + method.upper() + path + payload_str
        sign = base64.b64encode(
            hmac.new(self.secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
        ).decode()
        return {
            "ACCESS-KEY": self.api_key,
            "ACCESS-SIGN": sign,
            "ACCESS-TIMESTAMP": ts,
            "ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
            "locale": "zh-CN",
        }

    @staticmethod
    def _build_query(params):
        """GET 查询串：按 key 排序，签名和实际发送用同一个字符串"""
        if not params:
            return ""
        return "?" + urllib.parse.urlencode(sorted(params.items()))

    async def _signed_request(self, method, path, params=None, body=None):
        method = method.upper()
        if method == "GET":
            query = self._build_query(params)
            sign_queries = [query]
            raw_query = "?" + "&".join(
                f"{k}={v}" for k, v in sorted((params or {}).items())) if params else ""

            # 两种常见签名规范（编码串 / 原始串）都试一次；只对只读 GET 重试，POST 绝不重试。
            if raw_query and any(ord(ch) > 127 for ch in raw_query):
                if raw_query != query:
                    if self._non_ascii_sign_raw is True:
                        sign_queries = [raw_query, query]
                    else:
                        sign_queries = [query, raw_query]
            last_err = None
            for idx, sign_query in enumerate(sign_queries):
                headers = self._signed_headers(method, path, sign_query)
                try:
                    resp = await self.client.request(method, path + query, headers=headers)
                except httpx.TimeoutException:
                    raise ExchangeNetworkError("连接 Bitget 超时，结果未知")
                except httpx.HTTPError as e:
                    raise ExchangeNetworkError(f"连接 Bitget 失败（{e.__class__.__name__}）")
                try:
                    payload = resp.json()
                except Exception:
                    raise ExchangeNetworkError(f"Bitget 返回异常数据（HTTP {resp.status_code}）")
                code = str(payload.get("code", ""))
                if code in ("00000", "0"):
                    if len(sign_queries) > 1:
                        self._non_ascii_sign_raw = (sign_query == raw_query)
                    return payload.get("data")
                raw_msg = payload.get("msg", "")
                last_err = ExchangeApiError(
                    code, raw_msg,
                    f"交易所拒绝：{bitget_friendly(code, raw_msg)}（错误码 {code}）")
                if code == "40009" and idx + 1 < len(sign_queries):
                    continue
                raise last_err
            raise last_err or ExchangeApiError("-1", "", "交易所请求失败")
        else:
            body_str = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False)
            headers = self._signed_headers(method, path, body_str)
            try:
                resp = await self.client.request(method, path, headers=headers,
                                                 content=body_str.encode("utf-8"))
            except httpx.TimeoutException:
                raise ExchangeNetworkError("连接 Bitget 超时，结果未知")
            except httpx.HTTPError as e:
                raise ExchangeNetworkError(f"连接 Bitget 失败（{e.__class__.__name__}）")
            try:
                payload = resp.json()
            except Exception:
                raise ExchangeNetworkError(f"Bitget 返回异常数据（HTTP {resp.status_code}）")
            code = str(payload.get("code", ""))
            if code not in ("00000", "0"):
                raw_msg = payload.get("msg", "")
                raise ExchangeApiError(code, raw_msg,
                                       f"交易所拒绝：{bitget_friendly(code, raw_msg)}（错误码 {code}）")
            return payload.get("data")

    async def _public_get(self, path, params):
        """公开行情接口（不需要签名）：标记价、合约精度"""
        try:
            resp = await self.client.get(path, params=params)
        except httpx.TimeoutException:
            raise ExchangeNetworkError("连接 Bitget 超时")
        except httpx.HTTPError as e:
            raise ExchangeNetworkError(f"连接 Bitget 失败（{e.__class__.__name__}）")
        try:
            payload = resp.json()
        except Exception:
            raise ExchangeNetworkError(f"Bitget 返回异常数据（HTTP {resp.status_code}）")
        code = str(payload.get("code", ""))
        if code not in ("00000", "0"):
            raise ExchangeApiError(code, payload.get("msg", ""),
                                   f"交易所拒绝：{bitget_friendly(code, payload.get('msg', ''))}（错误码 {code}）")
        return payload.get("data")

    # ---- 对外接口 ----

    async def fetch_mark_price(self, symbol):
        """标记价：GET /api/v2/mix/market/ticker 的 markPrice，每次现取，不用缓存"""
        try:
            data = await self._public_get("/api/v2/mix/market/ticker",
                                          {"symbol": symbol, "productType": PRODUCT_TYPE})
            if isinstance(data, list):        # Bitget 返回的是数组，取第一条
                data = data[0] if data else {}
            if isinstance(data, dict) and data.get("markPrice"):
                return D(data["markPrice"])
            self.last_price_error = "Bitget 未返回标记价"   # 别让失败原因丢掉（2026-11-01 加仓报错查不到原因）
            return None
        except ExchangeApiError as e:
            self.last_price_error = f"{e.raw_msg}（错误码 {e.code}）"
            return None
        except Exception as e:
            self.last_price_error = f"网络异常（{e.__class__.__name__}）"
            return None

    async def fetch_mark_prices(self, symbols=None):
        """一次取全部 USDT 永续标记价，避免 30+ 个持仓同时逐个请求被 Bitget 限速。
        返回 {symbol: Decimal}；接口失败返回 None（调用方保留旧缓存并逐个兜底）。"""
        wanted = {str(s).upper() for s in (symbols or []) if s}
        try:
            data = await self._public_get("/api/v2/mix/market/tickers",
                                          {"productType": PRODUCT_TYPE})
        except Exception as e:
            self.last_price_error = f"批量取标记价失败（{e.__class__.__name__}）"
            return None
        items = data if isinstance(data, list) else ((data or {}).get("list") or [])
        out = {}
        for item in items:
            try:
                sym = str(item.get("symbol") or "").upper()
                raw = item.get("markPrice")
                if not sym or raw in (None, ""):
                    continue
                if wanted and sym not in wanted:
                    continue
                out[sym] = D(raw)
            except Exception:
                continue
        if not out:
            self.last_price_error = "Bitget 批量行情未返回标记价"
        return out

    async def fetch_lot_filters(self, symbol):
        """交易对的精度参数和费率（带 6 小时缓存）"""
        now = time.time()
        hit = self._filters_cache.get(symbol)
        if hit and now - hit[0] < 6 * 3600:
            return hit[1]
        try:
            data = await self._public_get("/api/v2/mix/market/contracts",
                                          {"symbol": symbol, "productType": PRODUCT_TYPE})
            out = parse_contract_filters(data)
            if out:
                self._filters_cache[symbol] = (now, out)
            else:
                self.last_contract_error = "Bitget 未返回该交易对的合约信息"
            return out
        except ExchangeApiError as e:
            self.last_contract_error = f"{e.raw_msg}（错误码 {e.code}）"
            return None
        except Exception as e:
            self.last_contract_error = f"网络异常（{e.__class__.__name__}）"
            return None

    async def fetch_account_config(self, symbol, strict=False, fresh=False):
        """读取账户该交易对的保证金模式 / 持仓模式（单向 or 双向），缓存 60 秒。
        fresh=True 跳过缓存（设置保证金模式后回读用）；strict=True 失败时抛异常，
        避免把“读不到”误当成全仓。"""
        now = time.time()
        if not fresh:
            hit = self._account_cache.get(symbol)
            if hit and now - hit[0] < 60:
                return hit[1]
        try:
            data = await self._signed_request(
                "GET", "/api/v2/mix/account/account",
                params={"symbol": symbol, "productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN})
            margin_mode = str((data or {}).get("marginMode") or "crossed").lower()
            # Bitget 的持仓模式字段叫 posMode（旧字段可能是 holdMode）
            hold_raw = str((data or {}).get("posMode")
                           or (data or {}).get("holdMode") or "one_way_mode").lower()
            hold_mode = "hedge_mode" if hold_raw in ("hedge_mode", "double_hold", "hedge", "double") else "one_way_mode"
            cfg = {"margin_mode": "isolated" if margin_mode.startswith("isolated") else "crossed",
                   "hold_mode": hold_mode}
            self._account_cache[symbol] = (time.time(), cfg)
            return cfg
        except Exception:
            if strict:
                raise
            return {"margin_mode": "crossed", "hold_mode": "one_way_mode"}

    async def readonly_check(self, symbol="BTCUSDT"):
        """只读连接测试：账户、持仓、挂单；绝不下单。"""
        data = await self._signed_request(
            "GET", "/api/v2/mix/account/account",
            params={"symbol": symbol, "productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN})
        info = {
            "account_equity": str((data or {}).get("accountEquity") or ""),
            "available": str((data or {}).get("available") or ""),
            "margin_mode": str((data or {}).get("marginMode") or ""),
            "pos_mode": str((data or {}).get("posMode")
                            or (data or {}).get("holdMode") or ""),
            "unrealized_pl": str((data or {}).get("unrealizedPL") or ""),
        }
        try:
            pos = await self._signed_request(
                "GET", "/api/v2/mix/position/all-position",
                params={"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN})
            items = pos if isinstance(pos, list) else (
                (pos or {}).get("list") if isinstance(pos, dict) else [])
            info["positions"] = len(items)
            info["position_symbols"] = [str(p.get("symbol")) for p in items[:10]]
        except Exception:
            info["positions"] = None
            info["position_symbols"] = []
        try:
            pend = await self._signed_request(
                "GET", "/api/v2/mix/order/orders-pending",
                params={"productType": PRODUCT_TYPE, "symbol": symbol})
            items = (pend or {}).get("entrustedList") if isinstance(pend, dict) else None
            info["pending_orders"] = len(items) if items else 0
        except Exception:
            info["pending_orders"] = None
        return info

    async def fetch_account_summary(self, symbol="BTCUSDT"):
        """只读：USDT 本位账户总权益 / 可用余额 / 未实现盈亏。
        优先查账户列表（跨仓+逐仓总额），失败再退回单交易对账户接口。"""
        item = None
        try:
            data = await self._signed_request(
                "GET", "/api/v2/mix/account/accounts",
                params={"productType": PRODUCT_TYPE})
            items = data if isinstance(data, list) else []
            if not items and isinstance(data, dict):
                for key in ("accountList", "accounts", "list"):
                    if isinstance(data.get(key), list):
                        items = data[key]
                        break
            if not items and isinstance(data, dict) and data.get("accountEquity") not in (None, ""):
                item = data
            if items and item is None:
                item = next(
                    (x for x in items
                     if str(x.get("marginCoin") or "").upper() == MARGIN_COIN),
                    items[0])
        except Exception:
            item = None
        if item is None:
            data = await self._signed_request(
                "GET", "/api/v2/mix/account/account",
                params={"symbol": symbol, "productType": PRODUCT_TYPE,
                        "marginCoin": MARGIN_COIN})
            item = data if isinstance(data, dict) else {}

        def pick(*keys):
            for k in keys:
                v = item.get(k)
                if v not in (None, ""):
                    return str(v)
            return ""

        equity = pick("accountEquity", "totalEquity", "usdtEquity")
        upnl = pick("unrealizedPL")
        if not upnl:
            try:
                crossed_upnl = D(pick("crossedUnrealizedPL") or "0")
                isolated_upnl = D(pick("isolatedUnrealizedPL") or "0")
                if crossed_upnl or isolated_upnl:
                    upnl = str(crossed_upnl + isolated_upnl)
            except Exception:
                pass
        available = pick("available")
        crossed_margin = pick("crossedMargin")
        isolated_margin = pick("isolatedMargin")
        used_margin = None
        try:
            positions = await self.fetch_positions()
            total_margin = Decimal("0")
            found_margin = False
            for pos in positions or []:
                try:
                    if D(pos.get("total") or "0") == 0:
                        continue
                except Exception:
                    continue
                raw = (pos.get("marginSize") or pos.get("positionBalance")
                       or pos.get("margin") or "")
                if raw in (None, ""):
                    continue
                try:
                    total_margin += D(str(raw))
                    found_margin = True
                except Exception:
                    pass
            if found_margin:
                used_margin = total_margin
        except Exception:
            used_margin = None
        margin = ""
        if used_margin is not None:
            margin = str(used_margin)
        else:
            try:
                if crossed_margin or isolated_margin:
                    margin = str(D(crossed_margin or "0") + D(isolated_margin or "0"))
                elif equity and available:
                    margin = str(max(D(equity) - D(available), Decimal("0")))
            except Exception:
                margin = ""
        # Bitget App 的“可用保证金”= 权益 − 持仓占用保证金 − 未实现盈亏。
        # 账户接口的 available 在混合全仓/逐仓时可能不是这个口径，所以用持仓保证金合计反算；
        # 反算值大于接口可用余额时说明保证金没取全，退回接口值，避免显示虚高。
        available_margin = available
        try:
            candidates = []
            if used_margin is not None and equity:
                candidate = D(equity) - used_margin - D(upnl or "0")
                if candidate < 0:
                    candidate = Decimal("0")
                if not available or candidate <= D(available) + Decimal("1"):
                    candidates.append(candidate)
            for raw in (pick("crossedMaxAvailable"),
                        pick("isolatedMaxAvailable", "fixedMaxAvailable")):
                if raw:
                    try:
                        value = D(raw)
                        if value > 0:
                            candidates.append(value)
                    except Exception:
                        pass
            if candidates:
                available_margin = str(min(candidates))
        except Exception:
            pass
        return {
            "equity": equity,
            "available": available_margin,
            "api_available": available,
            "unrealized_pl": upnl,
            "margin": margin,
            "margin_coin": pick("marginCoin"),
            "crossed_max_available": pick("crossedMaxAvailable", "crossedMaxAvailable"),
            "isolated_max_available": pick("isolatedMaxAvailable", "fixedMaxAvailable"),
            "max_transfer_out": pick("maxTransferOut"),
        }

    async def set_leverage(self, symbol, leverage, hold_side=None):
        """每次按请求调用交易所，不把历史设置成功当作当前状态的保证。"""
        try:
            body = {"symbol": symbol, "productType": PRODUCT_TYPE,
                    "marginCoin": MARGIN_COIN, "leverage": str(int(leverage))}
            if hold_side:
                body["holdSide"] = str(hold_side)
            await self._signed_request(
                "POST", "/api/v2/mix/account/set-leverage", body=body)
            return True
        except Exception:
            return False

    async def set_margin_mode(self, symbol, mode="isolated"):
        """设置该合约的保证金模式（逐仓/全仓）。设置后清掉账户配置缓存，开仓会重新读取。"""
        try:
            await self._signed_request(
                "POST", "/api/v2/mix/account/set-margin-mode",
                body={"symbol": symbol, "productType": PRODUCT_TYPE,
                      "marginCoin": MARGIN_COIN, "marginMode": mode})
            self._account_cache.pop(symbol, None)
            return True, ""
        except ExchangeApiError as e:
            return False, e.friendly
        except ExchangeNetworkError as e:
            return False, str(e)
        except Exception as e:
            return False, str(e)

    async def ensure_margin_mode(self, symbol, mode="isolated", tries=3, delay=0.4):
        """设置保证金模式并回读确认。Bitget 有“接口返回成功但实际没改”的情况，
        没确认目标模式就不能下单，否则会按账户原有模式开出去。"""
        ok, err = await self.set_margin_mode(symbol, mode)
        if not ok:
            return False, err
        actual = ""
        for i in range(max(1, int(tries))):
            try:
                cfg = await self.fetch_account_config(symbol, strict=True, fresh=True)
                actual = str(cfg.get("margin_mode") or "")
                if actual == mode:
                    return True, ""
            except Exception as e:
                actual = f"读取失败（{e}）"
            if i < int(tries) - 1:
                await asyncio.sleep(delay)
        return False, f"回读显示仍为 {actual or '未知'}，未下单"

    async def market_open(self, symbol, side, qty, client_id, margin_mode=None):
        """市价开仓。newClientOrderId 用信号 id 派生，网络超时重试也不会重复开仓。
        margin_mode 显式指定时直接用（开仓前已确认逐仓），不再读缓存，
        避免“刚设完逐仓、读取仍返回全仓”导致开成全仓。"""
        if margin_mode:
            mode = str(margin_mode)
        else:
            try:
                cfg = await self.fetch_account_config(symbol, strict=True)
                mode = str(cfg.get("margin_mode") or "")
            except Exception as e:
                return OrderResult(False, error=f"无法确认保证金模式，未下单：{e}")
        body = {
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "marginMode": mode,
            "marginCoin": MARGIN_COIN,
            "size": str(qty),
            "side": side,                 # buy / sell
            "orderType": "market",
            "clientOid": client_id,
        }
        try:
            data = await self._signed_request("POST", "/api/v2/mix/order/place-order", body=body)
            order_id = str((data or {}).get("orderId") or "")
            return OrderResult(True, order_id=order_id)
        except ExchangeNetworkError as e:
            return OrderResult(None, error=str(e))      # 结果未知：绝不盲目重试
        except ExchangeApiError as e:
            return OrderResult(False, error=e.friendly)

    async def place_limit_open(self, symbol, side, qty, price, client_id, margin_mode=None):
        """限价开仓（马丁加仓用）：GTC 挂单，价格按 tickSize 已对齐。"""
        if margin_mode:
            mode = str(margin_mode)
        else:
            try:
                cfg = await self.fetch_account_config(symbol, strict=True)
                mode = str(cfg.get("margin_mode") or "")
            except Exception as e:
                return OrderResult(False, error=f"无法确认保证金模式，未下单：{e}")
        body = {
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "marginMode": mode,
            "marginCoin": MARGIN_COIN,
            "size": str(qty),
            "side": side,
            "orderType": "limit",
            "price": str(price),
            "timeInForce": "GTC",
            "clientOid": client_id,
        }
        try:
            data = await self._signed_request("POST", "/api/v2/mix/order/place-order", body=body)
            order_id = str((data or {}).get("orderId") or "")
            return OrderResult(True, order_id=order_id)
        except ExchangeNetworkError as e:
            return OrderResult(None, error=str(e))
        except ExchangeApiError as e:
            return OrderResult(False, error=e.friendly)

    async def cancel_order(self, symbol, order_id):
        """按订单号撤单（马丁限价加仓替换时用）"""
        try:
            await self._signed_request(
                "POST", "/api/v2/mix/order/cancel-order",
                body={"symbol": symbol, "productType": PRODUCT_TYPE, "orderId": str(order_id)})
            return True
        except Exception:
            return False

    async def place_stop_close_position(self, symbol, hold_side, stop_price, client_id, qty=None):
        """止损保护单：用「定量 + 只减仓」的普通计划委托（normal_plan）。
        实盘验证 Bitget 的 pos_loss 接口会对正常仓位误报 43011，
        因此改用等价方案：触发价按标记价、触发后市价买入/卖出、reduceOnly 只减仓。
        qty 必须是本地记录的当前持仓数量。"""
        if not qty:
            return OrderResult(False, error="本地没有持仓数量，无法挂止损")
        try:
            cfg = await self.fetch_account_config(symbol)
            margin_mode = cfg.get("margin_mode", "crossed")
        except Exception:
            margin_mode = "crossed"
        side = "buy" if hold_side == "short" else "sell"
        body = {
            "marginCoin": MARGIN_COIN,
            "productType": PRODUCT_TYPE,
            "symbol": symbol,
            "size": str(qty),
            "side": side,
            "orderType": "market",
            "triggerPrice": str(stop_price),
            "triggerType": "mark_price",
            "executePrice": "0",
            "reduceOnly": "YES",
            "planType": "normal_plan",
            "marginMode": margin_mode,
            "clientOid": client_id,
        }
        try:
            await self._signed_request("POST", "/api/v2/mix/order/place-plan-order", body=body)
            return OrderResult(True)
        except ExchangeNetworkError as e:
            return OrderResult(None, error=str(e))
        except ExchangeApiError as e:
            return OrderResult(False, error=e.friendly)

    async def cancel_plan_orders(self, symbol):
        """只撤计划委托，不撤普通限价单；避免影响同一标的的其他订单。"""
        ok = True
        for plan_type in ("normal_plan", "track_plan"):
            try:
                data = await self._signed_request(
                    "GET", "/api/v2/mix/order/orders-plan-pending",
                    params={"productType": PRODUCT_TYPE, "symbol": symbol,
                            "planType": plan_type, "limit": "100"})
                items = (data or {}).get("entrustedList") or (data or {}).get("list") or []
                for item in items:
                    body = {"productType": PRODUCT_TYPE, "symbol": symbol, "marginCoin": MARGIN_COIN}
                    if item.get("orderId"):
                        body["orderId"] = item["orderId"]
                    if item.get("clientOid"):
                        body["clientOid"] = item["clientOid"]
                    try:
                        await self._signed_request(
                            "POST", "/api/v2/mix/order/cancel-plan-order", body=body)
                    except Exception:
                        ok = False
            except Exception:
                ok = False
        return ok

    async def cancel_all_orders(self, symbol):
        """先列出该标的的订单，再按订单号逐一撤销；不调用可能影响其他标的的批量撤单接口。"""
        ok = True
        try:
            orders = await self.fetch_open_orders(symbol)
        except Exception:
            orders = []
            ok = False            # 查不到就不撤，交给下一轮；绝不退回批量接口
        for o in orders:
            oid = str(o.get("orderId") or "")
            if not oid:
                continue
            try:
                await self.cancel_order(symbol, oid)
            except Exception:
                ok = False
        plan_ok = await self.cancel_plan_orders(symbol)
        return ok and plan_ok

    async def fetch_open_orders(self, symbol=""):
        """查该交易对当前挂着的普通委托（不含计划委托/止损单）。
        返回 list[dict]，字段同 Bitget（orderId / price / size / state / side）。"""
        params = {"productType": PRODUCT_TYPE, "limit": "100"}
        if symbol:
            params["symbol"] = symbol
        data = await self._signed_request("GET", "/api/v2/mix/order/orders-pending", params=params)
        if isinstance(data, dict):
            items = data.get("entrustedList") or data.get("list") or []
        elif isinstance(data, list):
            items = data
        else:
            items = []
        return [o for o in items if isinstance(o, dict)]

    async def fetch_order_detail(self, symbol, order_id):
        """查单笔订单：state = live / partially_filled / filled / canceled。
        订单不存在时 Bitget 会返回错误码，由调用方按“查不到”处理。"""
        data = await self._signed_request(
            "GET", "/api/v2/mix/order/detail",
            params={"productType": PRODUCT_TYPE, "symbol": symbol, "orderId": str(order_id)})
        return data if isinstance(data, dict) else {}

    async def market_close(self, symbol, side, qty, client_id):
        """市价平仓（reduceOnly=YES，只减仓不开反向仓）"""
        cfg = await self.fetch_account_config(symbol)
        body = {
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "marginMode": cfg["margin_mode"],
            "marginCoin": MARGIN_COIN,
            "size": str(qty),
            "side": side,
            "orderType": "market",
            "reduceOnly": "YES",
            "clientOid": client_id,
        }
        try:
            data = await self._signed_request("POST", "/api/v2/mix/order/place-order", body=body)
            order_id = str((data or {}).get("orderId") or "")
            return OrderResult(True, order_id=order_id)
        except ExchangeNetworkError as e:
            return OrderResult(None, error=str(e))
        except ExchangeApiError as e:
            # 平仓时“仓位不存在”是正常情况：可能已被止损单平掉
            if ("position" in (e.raw_msg or "").lower()
                    and ("not exist" in (e.raw_msg or "").lower()
                         or "insufficient" in (e.raw_msg or "").lower()
                         or "no position" in (e.raw_msg or "").lower())):
                return OrderResult(True, error="交易所返回无持仓（可能已被止损单平掉）")
            return OrderResult(False, error=e.friendly)

    async def get_order_fills(self, symbol, order_id):
        """查某笔订单的成交明细（用于拿交易所实际手续费和实际平仓盈亏）"""
        if not order_id:
            return []
        data = await self._signed_request(
            "GET", "/api/v2/mix/order/fills",
            params={"productType": PRODUCT_TYPE, "symbol": symbol,
                    "orderId": order_id, "limit": "100"})
        items = (data or {}).get("fillList") or (data or {}).get("list") or []
        return [parse_fill(f) for f in items]

    async def get_symbol_fills(self, symbol, start_ts, end_ts):
        """按交易对和时间查成交明细（止损计划单成交后没有 close_order_id，用它来对账）"""
        params = {"productType": PRODUCT_TYPE, "symbol": symbol,
                  "startTime": str(int(start_ts * 1000)),
                  "endTime": str(int(end_ts * 1000)), "limit": "100"}
        try:
            data = await self._signed_request("GET", "/api/v2/mix/order/fills", params=params)
        except ExchangeApiError as e:

            # 兜底：不带 symbol 拉时间范围内的成交，再按返回里的 symbol 过滤。
            if str(e.code) != "40009" or not any(ord(ch) > 127 for ch in str(symbol)):
                raise
            data = await self._signed_request(
                "GET", "/api/v2/mix/order/fills",
                params={"productType": PRODUCT_TYPE,
                        "startTime": str(int(start_ts * 1000)),
                        "endTime": str(int(end_ts * 1000)), "limit": "100"})
            items = (data or {}).get("fillList") or (data or {}).get("list") or []
            up = str(symbol).upper()
            return [f for f in (parse_fill(x) for x in items)
                    if str(f.get("symbol") or "").upper() == up]
        items = (data or {}).get("fillList") or (data or {}).get("list") or []
        return [parse_fill(f) for f in items]

    async def get_all_fills(self, start_ts, end_ts, chunk_seconds=3600, depth=0):
        """不指定交易对，按时间段取全部成交（用于按成交时间统计今日手续费）。
        按小时分段，避免单次 100 条上限把成交截断；某段满 100 条时再对半拆开重取。"""
        out = {}
        cur = float(start_ts)
        end = float(end_ts)
        while cur < end:
            chunk_end = min(cur + chunk_seconds, end)
            params = {"productType": PRODUCT_TYPE,
                      "startTime": str(int(cur * 1000)),
                      "endTime": str(int(chunk_end * 1000)), "limit": "100"}
            seen = set()
            while True:
                data = await self._signed_request("GET", "/api/v2/mix/order/fills", params=params)
                if not isinstance(data, dict) or not isinstance(data.get("fillList", data.get("list")), list):
                    raise ValueError("Bitget 成交响应结构不完整")
                items = data.get("fillList", data.get("list"))
                for raw in items:
                    fill = parse_fill(raw)
                    if not fill["fill_id"] or fill["ts"] <= 0:
                        raise ValueError("Bitget 成交缺少成交号或时间")
                    out[fill["fill_id"]] = fill
                if len(items) < 100:
                    break
                cursor = str(data.get("endId") or items[-1].get("tradeId") or items[-1].get("id") or "")
                if not cursor or cursor in seen:
                    raise ValueError("Bitget 成交分页没有前进，拒绝用不完整数据更新账本")
                seen.add(cursor)
                params["idLessThan"] = cursor
            cur = chunk_end
        return [f for f in out.values() if float(start_ts) <= f["ts"] <= float(end_ts)]

    async def fetch_positions(self):
        """当前所有持仓（用于止损成交后的本地对账）"""
        data = await self._signed_request(
            "GET", "/api/v2/mix/position/all-position",
            params={"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN})
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("list"), list):
            return data["list"]
        raise ValueError("Bitget实际持仓响应结构不完整，不能当作无持仓")

    async def get_closed_position_history(self, start_ts, end_ts, symbol=""):
        """只读真实历史持仓，完整翻页；不能把截断或异常响应当作完整结果。"""
        params = {"productType":PRODUCT_TYPE, "startTime":str(int(float(start_ts)*1000)),
                  "endTime":str(int(float(end_ts)*1000)), "limit":"100"}
        if symbol:
            params["symbol"] = str(symbol)
        result, seen = {}, set()
        while True:
            try:
                data = await self._signed_request("GET", "/api/v2/mix/position/history-position", params=params)
            except ExchangeApiError as exc:
                if str(exc.code) != "40009" or "symbol" not in params or not any(ord(c)>127 for c in str(symbol)):
                    raise
                params.pop("symbol")
                data = await self._signed_request("GET", "/api/v2/mix/position/history-position", params=params)
            if not isinstance(data, dict) or not isinstance(data.get("list"), list):
                raise ValueError("Bitget实际历史持仓响应结构不完整")
            rows = data["list"]
            for row in rows:
                pid = str(row.get("positionId") or "")
                if not pid:
                    raise ValueError("Bitget实际历史持仓缺少仓位编号")
                if symbol and str(row.get("symbol") or "").upper() != str(symbol).upper():
                    continue
                if pid in result and result[pid] != row:
                    raise ValueError("Bitget同一历史仓位返回了不一致的数据")
                result[pid] = row
            if len(rows) < 100:
                break
            cursor = str(data.get("endId") or "")
            if not cursor or cursor in seen:
                raise ValueError("Bitget历史持仓分页没有前进，拒绝截断记录")
            seen.add(cursor)
            params["idLessThan"] = cursor
        return list(result.values())


    async def get_funding_bills(self, start_ts, end_ts):
        """只读实际资金费账单，收支金额转为正支出、负收入。"""
        result, start = {}, float(start_ts)
        while start < end_ts:
            end = min(start + 29 * 86400, float(end_ts))
            cursor, seen = "", set()
            while True:
                params = {"productType": PRODUCT_TYPE, "businessType": "contract_settle_fee",
                          "startTime": str(int(start * 1000)), "endTime": str(int(end * 1000)), "limit": "100"}
                if cursor:
                    params["idLessThan"] = cursor
                data = await self._signed_request("GET", "/api/v2/mix/account/bill", params=params)
                if not isinstance(data, dict) or not isinstance(data.get("bills", data.get("billList")), list):
                    raise ValueError("Bitget 资金费账单响应结构不完整")
                rows = data.get("bills", data.get("billList"))
                for row in rows:
                    if row.get("businessType") not in (None, "", "contract_settle_fee"):
                        continue
                    if str(row.get("coin") or row.get("marginCoin") or "USDT").upper() != "USDT":
                        raise ValueError("资金费账单币种不是 USDT")
                    item = {"id": str(row.get("billId") or ""), "symbol": str(row.get("symbol") or ""),
                            "ts": float(row.get("cTime") or 0) / 1000, "amount": -D(row["amount"])}
                    if not item["id"] or item["ts"] <= 0:
                        raise ValueError("Bitget 资金费账单缺少编号或时间")
                    result[item["id"]] = item
                if len(rows) < 100:
                    break
                nxt = str(data.get("endId") or rows[-1].get("billId") or "")
                if not nxt or nxt in seen:
                    raise ValueError("Bitget 资金费分页没有前进")
                seen.add(nxt)
                cursor = nxt
            start = end + 0.001
        return list(result.values())


class GateExchange:
    """Gate.io USDT 本位永续合约（单向持仓模式）。
    签名：HMAC-SHA512(secret, METHOD\\nPATH\\nQUERY\\nSHA512(BODY)\\nTimestamp)
    请求头：KEY / SIGN / Timestamp。Gate 的 size 是合约张数，不是币数量。"""

    name = "Gate 合约"
    exchange_key = "gate"

    def __init__(self, api_key, secret):
        if httpx is None:
            raise RuntimeError("缺少依赖 httpx，请先运行「启动.bat」或「启动.command」自动安装")
        self.api_key = str(api_key or "").strip()
        self.secret = str(secret or "").strip()
        self.client = httpx.AsyncClient(base_url=GATE_BASE, timeout=httpx.Timeout(10.0))
        self._contracts_cache = (0.0, {})
        self._symbol_map = {}
        self._reverse_map = {}
        self._filters_cache = {}
        self._account_cache = (0.0, {})
        self._config_cache = {}
        self._quanto_cache = {}
        self.last_contract_error = ""
        self.last_price_error = ""

    async def close(self):
        try:
            await self.client.aclose()
        except Exception:
            pass

    # ---- 签名与请求 ----
    def _signed_headers(self, method, path, query="", body_str=""):
        ts = str(int(time.time()))
        body_hash = hashlib.sha512(body_str.encode("utf-8")).hexdigest()
        msg = f"{method.upper()}\n{path}\n{query}\n{body_hash}\n{ts}"
        sign = hmac.new(self.secret.encode("utf-8"), msg.encode("utf-8"),
                        hashlib.sha512).hexdigest()
        return {"KEY": self.api_key, "SIGN": sign, "Timestamp": ts,
                "Accept": "application/json", "Content-Type": "application/json",
                # 否则Gate会把不足一张的size返回成整数0，连同数量与方向一并丢失。
                "X-Gate-Size-Decimal": "1"}

    async def _request(self, method, path, params=None, body=None):
        method = method.upper()
        query = urllib.parse.urlencode(sorted((params or {}).items())) if params else ""
        body_str = ""
        if method in ("POST", "PUT"):
            body_str = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False)
        headers = self._signed_headers(method, path, query, body_str)
        url = path + (("?" + query) if query else "")
        try:
            if method == "GET":
                resp = await self.client.get(url, headers=headers)
            elif method == "DELETE":
                resp = await self.client.delete(url, headers=headers)
            else:
                resp = await self.client.request(
                    method, url, headers=headers, content=body_str.encode("utf-8"))
        except httpx.TimeoutException:
            raise ExchangeNetworkError("连接 Gate 超时，结果未知")
        except httpx.HTTPError as e:
            raise ExchangeNetworkError(f"连接 Gate 失败（{e.__class__.__name__}）")
        try:
            payload = resp.json()
        except Exception:
            raise ExchangeNetworkError(f"Gate 返回异常数据（HTTP {resp.status_code}）")
        if resp.status_code >= 400 or (isinstance(payload, dict) and payload.get("label")):
            label = str((payload or {}).get("label") or f"HTTP{resp.status_code}")
            msg = str((payload or {}).get("message") or "")
            raise ExchangeApiError(label, msg, gate_friendly(label, msg))
        return payload

    async def _public_get(self, path, params=None):
        try:
            resp = await self.client.get(path, params=params or {})
        except httpx.TimeoutException:
            raise ExchangeNetworkError("连接 Gate 超时")
        except httpx.HTTPError as e:
            raise ExchangeNetworkError(f"连接 Gate 失败（{e.__class__.__name__}）")
        try:
            payload = resp.json()
        except Exception:
            raise ExchangeNetworkError(f"Gate 返回异常数据（HTTP {resp.status_code}）")
        if resp.status_code >= 400 or (isinstance(payload, dict) and payload.get("label")):
            label = str((payload or {}).get("label") or f"HTTP{resp.status_code}")
            msg = str((payload or {}).get("message") or "")
            raise ExchangeApiError(label, msg, gate_friendly(label, msg))
        return payload

    def _path(self, tail):
        return f"{GATE_API_PREFIX}/futures/{GATE_SETTLE}{tail}"

    # ---- 合约与交易对 ----
    async def fetch_all_contracts(self, force=False):
        now = time.time()
        ts, data = self._contracts_cache
        # Gate 经常上新币；合约清单只缓存 10 分钟，避免新币信号被误判为“没有合约”
        if not force and data and now - ts < 600:
            return data
        items = await self._public_get(self._path("/contracts"))
        data = {str(c.get("name") or ""): c for c in (items or []) if c.get("name")}
        for name in data:
            self._reverse_map.setdefault(name, name.replace("_", ""))
        self._contracts_cache = (now, data)
        return data

    async def resolve_symbol(self, symbol):
        """平台/Bitget 风格交易对 → Gate 合约名（如 AIOTUSDT → AIOT_USDT）。
        兼容 1000/1M/1000000 倍币；找不到返回 None（绝不猜一个不存在的合约）。"""
        raw = str(symbol or "").strip().upper()
        if not raw:
            return None
        if raw in self._symbol_map:
            return self._symbol_map[raw]
        contracts = await self.fetch_all_contracts()
        if raw in contracts:
            self._symbol_map[raw] = raw
            self._reverse_map[raw] = raw
            return raw
        base = raw[:-4] if raw.endswith("USDT") else raw
        candidates = [f"{base}_USDT"]
        for prefix in ("1000000", "1M", "1000"):
            if base.startswith(prefix) and len(base) > len(prefix):
                candidates.append(f"{base[len(prefix):]}_USDT")
        if not base.startswith(("1000000", "1M", "1000")):
            candidates.extend([f"1000{base}_USDT", f"1M{base}_USDT",
                               f"1000000{base}_USDT"])
        seen = set()
        for c in candidates:
            if c in seen:
                continue
            seen.add(c)
            if c in contracts:
                self._symbol_map[raw] = c
                self._reverse_map[c] = raw
                return c
        self.last_contract_error = f"Gate 没有 {raw} 对应的 USDT 永续合约"
        return None

    def symbol_from_contract(self, contract):
        return self._reverse_map.get(contract) or str(contract or "").replace("_", "")

    async def fetch_contract(self, contract):
        data = await self._public_get(self._path(f"/contracts/{contract}"))
        return data if isinstance(data, dict) else {}

    async def quanto(self, contract):
        if contract in self._quanto_cache:
            return self._quanto_cache[contract]
        try:
            contracts = await self.fetch_all_contracts()
            c = contracts.get(contract)
            if c is None:
                c = await self.fetch_contract(contract)
            q = D(c.get("quanto_multiplier") or "0")
            if q > 0:
                self._quanto_cache[contract] = q
            return q
        except Exception:
            return Decimal("0")

    async def fetch_mark_price(self, symbol):
        try:
            contract = await self.resolve_symbol(symbol)
            if not contract:
                self.last_price_error = self.last_contract_error or "Gate 没有该合约"
                return None
            data = await self._public_get(self._path("/tickers"), {"contract": contract})
            item = data[0] if isinstance(data, list) and data else (
                data if isinstance(data, dict) else {})
            raw = item.get("mark_price") or item.get("last")
            if raw not in (None, ""):
                return D(raw)
            self.last_price_error = "Gate 未返回标记价"
            return None
        except Exception as e:
            self.last_price_error = f"{e.__class__.__name__}：{e}"
            return None

    async def fetch_mark_prices(self, symbols=None):
        wanted = [str(s or "").upper() for s in (symbols or []) if s]
        cmap = {}
        for s in wanted:
            try:
                c = await self.resolve_symbol(s)
                if c:
                    cmap[c] = s
            except Exception:
                continue
        try:
            items = await self._public_get(self._path("/tickers"))
        except Exception as e:
            self.last_price_error = f"批量取 Gate 标记价失败（{e.__class__.__name__}）"
            return None
        out = {}
        for item in items or []:
            try:
                c = str(item.get("contract") or "")
                raw = item.get("mark_price") or item.get("last")
                if not c or raw in (None, ""):
                    continue
                sym = cmap.get(c) or self.symbol_from_contract(c)
                if wanted and sym not in wanted:
                    continue
                out[sym] = D(raw)
            except Exception:
                continue
        return out

    async def fetch_lot_filters(self, symbol):
        now = time.time()
        hit = self._filters_cache.get(str(symbol).upper())
        if hit and now - hit[0] < 6 * 3600:
            return hit[1]
        try:
            contract = await self.resolve_symbol(symbol)
            if not contract:
                return None
            contracts = await self.fetch_all_contracts()
            info = contracts.get(contract)
            if info is None:
                info = await self.fetch_contract(contract)
            out = parse_gate_contract_filters(info)
            if out:
                self._filters_cache[str(symbol).upper()] = (now, out)
            else:
                self.last_contract_error = "Gate 合约参数不完整"
            return out
        except Exception as e:
            self.last_contract_error = f"{e.__class__.__name__}：{e}"
            return None

    # ---- 账户与持仓 ----
    async def _account_raw(self, fresh=False):
        now = time.time()
        ts, data = self._account_cache
        if not fresh and data and now - ts < 15:
            return data
        data = await self._request("GET", self._path("/accounts"))
        if not isinstance(data, dict):
            data = {}
        self._account_cache = (now, data)
        return data

    async def fetch_account_summary(self, symbol=""):
        data = await self._account_raw(fresh=True)
        try:
            total = D(data.get("total") or "0")
            upnl = D(data.get("unrealised_pnl") or "0")
            available = D(data.get("available") or "0")
            margin = D(data.get("position_margin") or "0") + D(data.get("order_margin") or "0")
        except Exception:
            total = upnl = available = margin = Decimal("0")
        return {
            "equity": str(total + upnl),
            "available": str(available),
            "api_available": str(available),
            "unrealized_pl": str(upnl),
            "margin": str(margin),
            "margin_coin": "USDT",
            "in_dual_mode": bool(data.get("in_dual_mode")),
            "position_mode": str(data.get("position_mode") or ""),
        }

    async def fetch_account_config(self, symbol, strict=False, fresh=False):
        now = time.time()
        key = str(symbol).upper()
        hit = self._config_cache.get(key)
        if hit and not fresh and now - hit[0] < 60:
            return hit[1]
        try:
            acct = await self._account_raw(fresh=fresh)
            hold = "hedge_mode" if acct.get("in_dual_mode") else "one_way_mode"
            contract = await self.resolve_symbol(symbol)
            if not contract:
                if strict:
                    raise ExchangeApiError("CONTRACT_NOT_FOUND", "",
                                           self.last_contract_error or "Gate 没有该交易对")
                return {"margin_mode": "crossed", "hold_mode": hold}
            try:
                pos = await self._request("GET", self._path(f"/positions/{contract}"))
            except ExchangeApiError as e:
                if str(e.code) == "POSITION_NOT_FOUND":
                    cfg = {"margin_mode": "crossed", "hold_mode": hold}
                    self._config_cache[key] = (time.time(), cfg)
                    return cfg
                raise
            lev = D((pos or {}).get("leverage") or "0")
            cross = lev == 0
            cfg = {
                "margin_mode": "crossed" if cross else "isolated",
                "hold_mode": hold,
                "leverage": str((pos or {}).get("cross_leverage_limit")
                                or (pos or {}).get("leverage") or ""),
            }
            self._config_cache[key] = (time.time(), cfg)
            return cfg
        except Exception:
            if strict:
                raise
            return {"margin_mode": "crossed", "hold_mode": "one_way_mode"}

    async def set_margin_mode(self, symbol, mode="isolated"):
        try:
            contract = await self.resolve_symbol(symbol)
            if not contract:
                return False, self.last_contract_error or "Gate 没有该交易对"
            body = {"contract": contract,
                    "mode": "CROSS" if mode == "crossed" else "ISOLATED"}
            data = await self._request(
                "POST", self._path("/positions/cross_mode"), body=body)
            self._config_cache.pop(str(symbol).upper(), None)
            lev = D((data or {}).get("leverage") or "0")
            actual = "crossed" if lev == 0 else "isolated"
            if isinstance(data, dict) and data.get("leverage") not in (None, ""):
                if actual != mode:
                    return False, f"Gate 回读显示仍为 {actual}"
            return True, ""
        except ExchangeApiError as e:
            return False, e.friendly
        except ExchangeNetworkError as e:
            return False, str(e)
        except Exception as e:
            return False, str(e)

    async def ensure_margin_mode(self, symbol, mode="isolated", tries=3, delay=0.4):
        ok, err = await self.set_margin_mode(symbol, mode)
        if not ok:
            return False, err
        for i in range(max(1, int(tries))):
            try:
                cfg = await self.fetch_account_config(symbol, strict=True, fresh=True)
                if str(cfg.get("margin_mode") or "") == mode:
                    return True, ""
            except Exception as e:
                err = f"读取 Gate 保证金模式失败（{e}）"
            if i < int(tries) - 1:
                await asyncio.sleep(delay)
        return False, err or f"Gate 回读未确认 {mode}，未下单"

    async def set_leverage(self, symbol, leverage, hold_side=None):
        try:
            contract = await self.resolve_symbol(symbol)
            if not contract:
                return False
            cfg = await self.fetch_account_config(symbol, fresh=True)
            if str(cfg.get("margin_mode") or "") == "isolated":
                params = {"leverage": str(int(leverage))}
            else:
                params = {"leverage": "0", "cross_leverage_limit": str(int(leverage))}
            await self._request(
                "POST", self._path(f"/positions/{contract}/leverage"), params=params)
            self._config_cache.pop(str(symbol).upper(), None)
            return True
        except Exception:
            return False

    async def fetch_positions(self):
        items = await self._request("GET", self._path("/positions"))
        out = []
        for p in items or []:
            try:
                size = D(p.get("size") or "0")
                if size == 0:
                    continue
                contract = str(p.get("contract") or "")
                q = await self.quanto(contract)
                lev = D(p.get("leverage") or "0")
                cross = lev == 0
                out.append({
                    "symbol": self.symbol_from_contract(contract),
                    "contract": contract,
                    "total": str(abs(size) * q),
                    "holdSide": "long" if size > 0 else "short",
                    "openPriceAvg": str(p.get("entry_price") or ""),
                    "markPrice": str(p.get("mark_price") or ""),
                    "marginMode": "crossed" if cross else "isolated",
                    "leverage": str(p.get("cross_leverage_limit") or lev) if cross else str(lev),
                    "liquidationPrice": str(p.get("liq_price") or ""),
                    "unrealizedPnl": str(p.get("unrealised_pnl") or ""),
                    "marginSize": str(p.get("margin") or p.get("initial_margin") or ""),
                    "cTime": int(float(p.get("open_time") or 0) * 1000),
                    "raw": p,
                })
            except Exception:
                continue
        return out

    # ---- 下单 ----
    async def market_open(self, symbol, side, qty, client_id, margin_mode=None):
        try:
            filters = await self.fetch_lot_filters(symbol)
            if not filters:
                return OrderResult(False, error=self.last_contract_error or "Gate 没有该合约")
            contract = await self.resolve_symbol(symbol)
            quanto = D(filters.get("contractSize") or "0")
            if quanto <= 0:
                return OrderResult(False, error="Gate 合约张数参数异常")
            decimal_size = bool(filters.get("enableDecimal"))
            # Gate 小数张的实际步进是 0.1 张（order_size_min 字段返回 0 不代表可以更小），
            # 这里统一向下取整到 0.1 张，避免请求量与实际成交不一致。
            step = D("0.1") if decimal_size else D("1")
            contracts = floor_to_step(D(qty) / quanto, step)
            min_size = max(D(filters.get("orderSizeMin") or "0"), step)
            if contracts < min_size:
                return OrderResult(
                    False, error=f"Gate 数量 {contracts} 张小于最小下单量 {min_size} 张")
            is_long = str(side).lower() in ("long", "buy")
            size = contracts if is_long else -contracts
            body = {"contract": contract, "price": "0", "tif": "ioc",
                    "text": gate_text_id(client_id), "reduce_only": False,
                    "size": format(size.normalize(), "f") if decimal_size else int(size)}
            data = await self._request("POST", self._path("/orders"), body=body)
            oid = str((data or {}).get("id") or "")
            return OrderResult(True, order_id=oid)
        except ExchangeNetworkError as e:
            return OrderResult(None, error=str(e))
        except ExchangeApiError as e:
            return OrderResult(False, error=e.friendly)
        except Exception as e:
            return OrderResult(False, error=f"Gate 下单失败：{e}")

    async def place_limit_open(self, symbol, side, qty, price, client_id, margin_mode=None):
        try:
            filters = await self.fetch_lot_filters(symbol)
            if not filters:
                return OrderResult(False, error=self.last_contract_error or "Gate 没有该合约")
            contract = await self.resolve_symbol(symbol)
            quanto = D(filters.get("contractSize") or "0")
            decimal_size = bool(filters.get("enableDecimal"))
            step = D("0.1") if decimal_size else D("1")
            contracts = floor_to_step(D(qty) / quanto, step)
            if contracts < max(D(filters.get("orderSizeMin") or "0"), step):
                return OrderResult(False, error="Gate 数量低于最小下单量")
            is_long = str(side).lower() in ("long", "buy")
            size = contracts if is_long else -contracts
            body = {"contract": contract, "price": str(price), "tif": "gtc",
                    "text": gate_text_id(client_id), "reduce_only": False,
                    "size": format(size.normalize(), "f") if decimal_size else int(size)}
            data = await self._request("POST", self._path("/orders"), body=body)
            return OrderResult(True, order_id=str((data or {}).get("id") or ""))
        except ExchangeNetworkError as e:
            return OrderResult(None, error=str(e))
        except ExchangeApiError as e:
            return OrderResult(False, error=e.friendly)
        except Exception as e:
            return OrderResult(False, error=f"Gate 限价开仓失败：{e}")

    async def market_close(self, symbol, side, qty, client_id):
        try:
            contract = await self.resolve_symbol(symbol)
            if not contract:
                return OrderResult(False, error=self.last_contract_error or "Gate 没有该交易对")
            quanto = await self.quanto(contract)
            try:
                pos = await self._request("GET", self._path(f"/positions/{contract}"))
            except ExchangeApiError as e:
                if str(e.code) == "POSITION_NOT_FOUND":
                    return OrderResult(True, error="Gate 无持仓（可能已平掉）")
                raise
            pos_size = D((pos or {}).get("size") or "0")
            if pos_size == 0:
                return OrderResult(True, error="Gate 无持仓（可能已平掉）")
            close_contracts = (D(qty) / quanto) if qty else abs(pos_size)
            body = {"contract": contract, "price": "0", "tif": "ioc",
                    "text": gate_text_id(client_id)}
            if close_contracts >= abs(pos_size) * Decimal("0.999"):
                body.update({"size": 0, "close": True})
            else:
                size = -close_contracts if pos_size > 0 else close_contracts
                filters = await self.fetch_lot_filters(symbol) or {}
                size_val = (format(size.normalize(), "f")
                            if filters.get("enableDecimal") else int(size))
                body.update({"size": size_val, "reduce_only": True})
            data = await self._request("POST", self._path("/orders"), body=body)
            return OrderResult(True, order_id=str((data or {}).get("id") or ""))
        except ExchangeNetworkError as e:
            return OrderResult(None, error=str(e))
        except ExchangeApiError as e:
            if str(e.code) == "POSITION_NOT_FOUND":
                return OrderResult(True, error="Gate 无持仓（可能已平掉）")
            return OrderResult(False, error=e.friendly)
        except Exception as e:
            return OrderResult(False, error=f"Gate 平仓失败：{e}")

    async def place_stop_close_position(self, symbol, hold_side, stop_price,
                                        client_id, qty=None):
        try:
            contract = await self.resolve_symbol(symbol)
            if not contract:
                return OrderResult(False, error=self.last_contract_error or "Gate 没有该交易对")
            body = {
                "initial": {
                    "contract": contract, "size": 0, "close": True, "price": "0",
                    "tif": "ioc", "reduce_only": True,
                    "text": gate_text_id(client_id),
                },
                "trigger": {
                    "strategy_type": 0, "price_type": 1,
                    "price": str(stop_price),
                    "rule": 2 if str(hold_side) == "long" else 1,
                    "expiration": 2592000,
                },
                "order_type": ("close-long-position" if str(hold_side) == "long"
                               else "close-short-position"),
            }
            data = await self._request("POST", self._path("/price_orders"), body=body)
            return OrderResult(True, order_id=str((data or {}).get("id") or ""))
        except ExchangeNetworkError as e:
            return OrderResult(None, error=str(e))
        except ExchangeApiError as e:
            return OrderResult(False, error=e.friendly)
        except Exception as e:
            return OrderResult(False, error=f"Gate 止损单失败：{e}")

    async def cancel_order(self, symbol, order_id):
        try:
            await self._request("DELETE", self._path(f"/orders/{order_id}"))
            return True
        except Exception:
            return False

    async def cancel_all_orders(self, symbol):
        """先列出该标的的订单，再按订单号逐一撤销；不调用可能影响其他标的的批量撤单接口。"""
        ok = True
        try:
            contract = await self.resolve_symbol(symbol)
            if not contract:
                return True
            await self._request("DELETE", self._path("/orders"),
                                params={"contract": contract})
        except Exception:
            ok = False
        plan_ok = await self.cancel_plan_orders(symbol)
        return ok and plan_ok

    async def cancel_plan_orders(self, symbol):
        try:
            contract = await self.resolve_symbol(symbol)
            if not contract:
                return True
            items = await self._request(
                "GET", self._path("/price_orders"),
                params={"status": "open", "contract": contract})
            for it in items or []:
                oid = it.get("id")
                if oid not in (None, ""):
                    try:
                        await self._request(
                            "DELETE", self._path(f"/price_orders/{oid}"))
                    except Exception:
                        pass
            return True
        except Exception:
            return False

    def _order_state(self, data):
        status = str((data or {}).get("status") or "").lower()
        finish = str((data or {}).get("finish_as") or "").lower()
        left = D((data or {}).get("left") or "0")
        if status == "finished":
            if finish in ("filled",):
                return "filled"
            if finish in ("cancelled", "canceled"):
                return "canceled"
            return "canceled"
        if left > 0:
            return "partially_filled"
        return "live"

    async def _order_view(self, data, contract=None):
        contract = contract or str((data or {}).get("contract") or "")
        q = await self.quanto(contract) if contract else Decimal("0")
        size = D((data or {}).get("size") or "0")
        left = D((data or {}).get("left") or "0")
        return {
            "orderId": str((data or {}).get("id") or ""),
            "symbol": self.symbol_from_contract(contract),
            "contract": contract,
            "side": "buy" if size >= 0 else "sell",
            "size": str(abs(size - left) * q) if left > 0 else str(abs(size) * q),
            "price": str((data or {}).get("price") or ""),
            "state": self._order_state(data),
            "type": "limit" if D((data or {}).get("price") or "0") > 0 else "market",
            "raw": data,
        }

    async def fetch_open_orders(self, symbol=""):
        params = {"status": "open", "limit": "100"}
        if symbol:
            contract = await self.resolve_symbol(symbol)
            if not contract:
                return []
            params["contract"] = contract
        items = await self._request("GET", self._path("/orders"), params=params)
        out = []
        for it in items or []:
            try:
                out.append(await self._order_view(it))
            except Exception:
                continue
        return out

    async def fetch_order_detail(self, symbol, order_id):
        try:
            data = await self._request("GET", self._path(f"/orders/{order_id}"))
            return await self._order_view(data) if isinstance(data, dict) else {}
        except Exception:
            return {}

    async def get_order_fills(self, symbol, order_id):
        if not order_id:
            return []
        contract = await self.resolve_symbol(symbol)
        if not contract:
            return []
        q = await self.quanto(contract)
        items = await self._request(
            "GET", self._path("/my_trades"),
            params={"contract": contract, "order": str(order_id), "limit": "1000"})
        return [parse_gate_fill(x, q, self.symbol_from_contract(contract))
                for x in (items or [])]

    async def _my_trades_range(self, contract, start_ts, end_ts):
        params = {"limit": "1000"}
        if contract:
            params["contract"] = contract
        if start_ts:
            params["from"] = str(max(0, int(float(start_ts))))
        if end_ts:
            params["to"] = str(int(float(end_ts)))
        out = []
        offset = 0
        seen = set()
        while True:
            params["offset"] = str(offset)
            items = await self._request(
                "GET", self._path("/my_trades_timerange"), params=params)
            if not isinstance(items, list):
                raise ValueError("Gate 成交响应结构不完整")
            batch = items
            fingerprint = tuple(str(x.get("id") or x.get("trade_id") or "") for x in batch)
            if batch and fingerprint in seen:
                raise ValueError("Gate 成交分页重复，拒绝截断账本")
            seen.add(fingerprint)
            out.extend(batch)
            if len(batch) < 1000:
                break
            offset += 1000
            if offset > 200000:
                raise ValueError("Gate 成交分页超过安全上限，不能确认完整数据")
        return out

    async def get_symbol_fills(self, symbol, start_ts, end_ts):
        contract = await self.resolve_symbol(symbol)
        if not contract:
            return []
        q = await self.quanto(contract)
        items = await self._my_trades_range(contract, start_ts, end_ts)
        fills = [parse_gate_fill(x, q, self.symbol_from_contract(contract))
                 for x in items]
        fills.sort(key=lambda f: float(f.get("ts") or 0))
        return fills

    async def get_all_fills(self, start_ts, end_ts):
        items = await self._my_trades_range("", start_ts, end_ts)
        contracts = await self.fetch_all_contracts()
        fills = []
        for x in items:
            contract = str((x or {}).get("contract") or "")
            c = contracts.get(contract)
            if c is None:
                c = await self.fetch_contract(contract)
            q = D((c or {}).get("quanto_multiplier") or "0")
            fills.append(parse_gate_fill(x, q, self.symbol_from_contract(contract)))
        fills.sort(key=lambda f: float(f.get("ts") or 0))
        return fills

    async def get_funding_bills(self, start_ts, end_ts):
        """只读账户实际资金费收支，分页未取完整时直接报错。"""
        result, offset, seen = {}, 0, set()
        while True:
            rows = await self._request("GET", self._path("/account_book"), params={
                "type": "fund", "from": str(int(start_ts)), "to": str(int(end_ts)),
                "limit": "100", "offset": str(offset)})
            if not isinstance(rows, list):
                raise ValueError("Gate 资金费账单响应结构不完整")
            fingerprint = tuple(str(r.get("id")) for r in rows)
            if rows and fingerprint in seen:
                raise ValueError("Gate 资金费分页重复，不能确认完整数据")
            seen.add(fingerprint)
            for row in rows:
                item = {"id": str(row.get("id") or ""),
                        "symbol": self.symbol_from_contract(str(row.get("contract") or row.get("text") or "")),
                        "ts": float(row.get("time") or 0), "amount": -D(row["change"])}
                if not item["id"] or item["ts"] <= 0:
                    raise ValueError("Gate 资金费账单缺少编号或时间")
                result[item["id"]] = item
            if len(rows) < 100:
                break
            offset += 100
            if offset > 200000:
                raise ValueError("Gate 资金费分页超过安全上限")
        return list(result.values())

    # ---- 资金费公开数据（仅模拟与旧数据诊断使用） ----
    async def fetch_funding_events(self, symbol, since_ts, now_ts):
        contract = await self.resolve_symbol(symbol)
        if not contract:
            return []
        data = await self._public_get(
            self._path("/funding_rate"), {"contract": contract, "limit": "1000"})
        out = []
        for it in data or []:
            try:
                ts = float(it.get("t") or 0)
                if not (float(since_ts) < ts <= float(now_ts)):
                    continue
                out.append({"ts": ts, "rate": D(it.get("r") or "0")})
            except Exception:
                continue
        out.sort(key=lambda x: x["ts"])
        return out

    async def fetch_funding_price(self, symbol, funding_ts):
        contract = await self.resolve_symbol(symbol)
        if not contract:
            return None
        try:
            data = await self._public_get(
                self._path("/candlesticks"),
                {"contract": contract, "interval": "1m",
                 "from": str(int(float(funding_ts) - 60)),
                 "to": str(int(float(funding_ts) + 60))})
        except Exception:
            return None
        best = None
        for row in data or []:
            try:
                if isinstance(row, dict):
                    ts = float(row.get("t") or 0)
                    close = D(row.get("c") or "0")
                else:
                    ts = float(row[0])
                    close = D(row[2])
                if best is None or abs(ts - float(funding_ts)) < abs(best[0] - float(funding_ts)):
                    best = (ts, close)
            except Exception:
                continue
        return best[1] if best else None

    async def readonly_check(self, symbol="BTC_USDT"):
        acct = await self.fetch_account_summary()
        return {
            "account_equity": acct.get("equity", ""),
            "available": acct.get("available", ""),
            "unrealized_pl": acct.get("unrealized_pl", ""),
            "hold_mode": "hedge_mode" if acct.get("in_dual_mode") else "one_way_mode",
            "position_mode": acct.get("position_mode", ""),
        }


class TelegramListener:
    """轮询 Telegram Bot API，读取平台 Webhook 推到群里的消息（只读、不依赖第三方库）。
    token 是 Bot Token（BotFather 给的），chat_id 是群 Chat ID（负数）。"""

    BASE = "https://api.telegram.org"

    def __init__(self, token, chat_id):
        self.token = str(token or "").strip()
        self.chat_id = str(chat_id or "").strip()
        self._client = None

    @property
    def ready(self):
        return bool(self.token and self.chat_id)

    def _ensure_client(self):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=45)
        return self._client

    async def close(self):
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    async def poll(self, offset=None, timeout=25):
        """长轮询一次。返回 (next_offset, [(msg_id, unix_ts, text), ...])；失败返回 (offset, [])。"""
        if not self.ready or httpx is None:
            return offset, []
        params = {"timeout": str(timeout), "allowed_updates": '["message","channel_post"]'}
        if offset:
            params["offset"] = str(offset)
        try:
            client = self._ensure_client()
            resp = await client.get(f"{self.BASE}/bot{self.token}/getUpdates", params=params)
            data = resp.json()
        except Exception:
            return offset, []
        if not isinstance(data, dict) or not data.get("ok"):
            return offset, []
        out = []
        next_off = offset or 0
        for u in data.get("result") or []:
            try:
                u_id = int(u.get("update_id") or 0)
                next_off = max(next_off, u_id + 1)
                msg = u.get("message") or u.get("channel_post") or {}
                chat = msg.get("chat") or {}
                if str(chat.get("id") or "") != self.chat_id:
                    continue
                text = msg.get("text") or msg.get("caption") or ""
                if not text:
                    continue
                out.append((u_id, int(msg.get("date") or 0), str(text)))
            except Exception:
                continue
        return next_off, out
