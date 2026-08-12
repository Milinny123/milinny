"""全市场 ETF 与支付宝 C 类基金双时段动量筛选、归因和仪表盘生成器。"""

from __future__ import annotations

import html
import base64
import gzip
import json
import math
import os
import re
import signal
from datetime import datetime, time, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable

import akshare as ak
import pandas as pd
import requests


BEIJING_TZ = timezone(timedelta(hours=8))
# Two trading years give the walk-forward check materially more observations
# than the former 150-row window while keeping GitHub Actions runtime bounded.
HISTORY_ROWS = 400
RISK_PROFILE = os.getenv("RISK_PROFILE", "aggressive").strip().lower()
if RISK_PROFILE not in {"balanced", "aggressive"}:
    RISK_PROFILE = "aggressive"
MAX_DYNAMIC_CANDIDATES = 18 if RISK_PROFILE == "aggressive" else 12
MAX_LINKED_FUNDS = 10
MAX_REPORT_ITEMS = 12
MAX_DRAWDOWN_LIMIT = -8.0
MAX_DATA_AGE_DAYS = 10
MAX_EXCHANGE_ROUND_TRIP_COST_PCT = 1.0
MIN_INTRADAY_TURNOVER = 50_000_000
HOLDINGS_PATH = Path(os.getenv("HOLDINGS_PATH", "holdings.json"))


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, "").strip() or default)
        return value if value > 0 else default
    except ValueError:
        return default


def _non_negative_float_env(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, "").strip() or default)
        return value if value >= 0 else default
    except ValueError:
        return default


def _round_to_50(value: float) -> int:
    return max(50, int(math.floor(value / 50 + 0.5) * 50))


TOTAL_CAPITAL = _positive_int_env("TOTAL_CAPITAL", 2000)
BUY_MIN = _positive_int_env("BUY_MIN", 100)
BUY_MAX = max(BUY_MIN, _positive_int_env("BUY_MAX", 250))
STANDARD_BUY = min(BUY_MAX, max(BUY_MIN, _round_to_50(TOTAL_CAPITAL * 0.10)))
HIGH_CONVICTION_BUY = min(BUY_MAX, max(BUY_MIN, _round_to_50(TOTAL_CAPITAL * 0.125)))
BROKER_MIN_COMMISSION = _non_negative_float_env("BROKER_MIN_COMMISSION", 10.0)
INTRADAY_MAX_ROUND_TRIP_COST_PCT = _non_negative_float_env(
    "INTRADAY_MAX_ROUND_TRIP_COST_PCT", 1.0
)
TARGET_DAILY_MOVE = _non_negative_float_env("TARGET_DAILY_MOVE", 3.0)
ENTRY_MAX_DAILY_MOVE = _non_negative_float_env("ENTRY_MAX_DAILY_MOVE", 2.5)
ENTRY_MIN_DAILY_MOVE = -_non_negative_float_env("ENTRY_PULLBACK_LIMIT", 2.5)
MIN_BACKTEST_SIGNALS = _positive_int_env("MIN_BACKTEST_SIGNALS", 8)
# Master prompt's intraday burst score.  est_growth is an estimate, so the
# report always exposes its source and falls back to the latest daily return.
BURST_WEIGHTS = (0.45, 0.30, 0.15, 0.10)
MOMENTUM_WEIGHTS = (0.65, 0.25, 0.10) if RISK_PROFILE == "aggressive" else (0.50, 0.30, 0.20)
SHORT_MOMENTUM_WEIGHTS = (0.45, 0.30, 0.25)
MAX_PLANNED_HOLD_DAYS = _positive_int_env("MAX_PLANNED_HOLD_DAYS", 7)
MANAGER_MIN_YEARS = _non_negative_float_env("MANAGER_MIN_YEARS", 10.0)
NEWS_LOOKBACK_DAYS = 3
NEWS_MAX_THEMES = 6
SIGNAL_PERCENTILE = 65 if RISK_PROFILE == "aggressive" else 70
SIGNAL_MIN_SCORE = 0.0 if RISK_PROFILE == "aggressive" else 0.0
REQUIRE_MA60 = RISK_PROFILE != "aggressive"
BENCHMARK = {"code": "510300", "name": "沪深300ETF", "kind": "benchmark", "data_codes": ("510300",)}

# 仅纳入已明确属于债券、黄金或跨境类别的代表性 ETF。普通股票 ETF 为 T+1，
# 不能因为名称相似而自动推断为可日内回转品种。
T0_ETF_ALLOWLIST: dict[str, dict[str, str]] = {
    "511010": {"name": "国债ETF", "category": "债券ETF"},
    "518880": {"name": "黄金ETF", "category": "黄金ETF"},
    "513050": {"name": "中概互联网ETF", "category": "跨境ETF"},
    "513180": {"name": "恒生科技指数ETF", "category": "跨境ETF"},
    "513330": {"name": "恒生互联网ETF", "category": "跨境ETF"},
    "159920": {"name": "恒生ETF", "category": "跨境ETF"},
}

_ETF_SPOT_CACHE: pd.DataFrame | None = None
_ETF_SPOT_INDEX: dict[str, dict[str, Any]] | None = None
_FUND_ESTIMATION_CACHE: dict[str, dict[str, Any]] | None = None
_FUND_CATALOG_CACHE: pd.DataFrame | None = None

# Codes are checked against AKShare's fund catalog at runtime.  The catalog is
# authoritative for the current name; an invalid or mismatched entry is skipped.
CORE_WATCHLIST: list[dict[str, Any]] = [
    {"code": "008888", "name": "华夏国证半导体芯片ETF联接C", "kind": "alipay_c", "data_codes": ("008888",)},
    {"code": "011613", "name": "华夏科创50ETF联接C", "kind": "alipay_c", "data_codes": ("011613",)},
    {"code": "008586", "name": "华夏人工智能ETF联接C", "kind": "alipay_c", "data_codes": ("008586",)},
    {"code": "024663", "name": "富国创业板人工智能ETF发起式联接C", "kind": "alipay_c", "data_codes": ("024663",)},
    {"code": "017516", "name": "易方达北证50成份指数C", "kind": "alipay_c", "data_codes": ("017516",)},
    {"code": "020973", "name": "易方达机器人ETF联接C", "kind": "alipay_c", "data_codes": ("020973",)},
    {"code": "012805", "name": "广发恒生科技ETF联接(QDII)C", "kind": "alipay_c", "data_codes": ("012805",)},
    {"code": "019892", "name": "华夏中证2000ETF发起式联接C", "kind": "alipay_c", "data_codes": ("019892",)},
    {"code": "016008", "name": "招商中证消费电子主题ETF联接C", "kind": "alipay_c", "data_codes": ("016008",)},
    {"code": "017938", "name": "易方达中证医疗ETF联接发起式C", "kind": "alipay_c", "data_codes": ("017938",)},
    {"code": "006328", "name": "易方达中证海外互联网50ETF联接(QDII)C", "kind": "alipay_c", "data_codes": ("006328",)},
    {"code": "012349", "name": "天弘恒生科技ETF联接C", "kind": "alipay_c", "data_codes": ("012349",)},
    {"code": "012729", "name": "国泰中证动漫游戏ETF联接C", "kind": "alipay_c", "data_codes": ("012729",)},
    {"code": "012637", "name": "国泰中证全指软件ETF联接C", "kind": "alipay_c", "data_codes": ("012637",)},
    {"code": "004070", "name": "南方中证全指证券公司ETF联接C", "kind": "alipay_c", "data_codes": ("004070",)},
    {"code": "007467", "name": "华泰柏瑞中证红利低波ETF联接C", "kind": "alipay_c", "data_codes": ("007467",)},
    {"code": "000217", "name": "华安黄金ETF联接C", "kind": "alipay_c", "data_codes": ("000217",)},
    {"code": "004433", "name": "南方有色金属ETF联接C", "kind": "alipay_c", "data_codes": ("004433",)},
    {"code": "017193", "name": "天弘中证工业有色金属主题ETF发起联接C", "kind": "alipay_c", "data_codes": ("017193",)},
    {"code": "018168", "name": "国泰有色矿业ETF联接C", "kind": "alipay_c", "data_codes": ("018168",)},
    {"code": "011036", "name": "嘉实中证稀土产业ETF联接C", "kind": "alipay_c", "data_codes": ("011036",)},
    {"code": "007339", "name": "易方达沪深300ETF联接C", "kind": "alipay_c", "data_codes": ("007339",)},
    {"code": "588000", "name": "科创50ETF", "kind": "etf", "data_codes": ("588000",)},
    {"code": "512480", "name": "半导体ETF", "kind": "etf", "data_codes": ("512480",)},
    {"code": "512400", "name": "有色金属ETF南方", "kind": "etf", "data_codes": ("512400",)},
    {"code": "159876", "name": "有色ETF华宝", "kind": "etf", "data_codes": ("159876",)},
    {"code": "516150", "name": "稀土ETF嘉实", "kind": "etf", "data_codes": ("516150",)},
]

METALS_CODES = {
    "004433",
    "017193",
    "018168",
    "011036",
    "512400",
    "159876",
    "516150",
}


def _is_alipay_fund(kind: str) -> bool:
    return kind in {"alipay_a", "alipay_c", "linked_c"}


def load_holdings(path: str | Path = HOLDINGS_PATH) -> list[dict[str, Any]]:
    """Load private position lots without guessing missing transaction NAVs."""
    holdings_path = Path(path)
    if not holdings_path.exists():
        return []
    payload = json.loads(holdings_path.read_text(encoding="utf-8"))
    positions = payload.get("positions", [])
    if not isinstance(positions, list):
        raise ValueError("holdings.json 的 positions 必须是数组")
    validated: list[dict[str, Any]] = []
    for index, raw in enumerate(positions, start=1):
        if not isinstance(raw, dict):
            raise ValueError(f"第 {index} 笔持仓不是对象")
        code = str(raw.get("code", "")).zfill(6)
        if not re.fullmatch(r"\d{6}", code):
            raise ValueError(f"第 {index} 笔持仓代码无效")
        name = str(raw.get("name", "")).strip()
        kind = str(raw.get("kind", "")).strip()
        if not name or not _is_alipay_fund(kind):
            raise ValueError(f"第 {index} 笔持仓缺少名称或基金类别无效")
        buy_date = datetime.strptime(str(raw.get("buy_date", "")), "%Y-%m-%d").date()
        amount = float(raw.get("amount", 0))
        if amount <= 0:
            raise ValueError(f"第 {index} 笔持仓金额必须大于 0")
        cost_nav_raw = raw.get("cost_nav")
        cost_nav = None if cost_nav_raw in (None, "") else float(cost_nav_raw)
        if cost_nav is not None and cost_nav <= 0:
            raise ValueError(f"第 {index} 笔持仓成本净值必须大于 0")
        status = str(raw.get("status", "confirmed")).strip().lower()
        if status not in {"pending", "confirmed"}:
            raise ValueError(f"第 {index} 笔持仓状态必须是 pending 或 confirmed")
        validated.append(
            {
                **raw,
                "code": code,
                "name": name,
                "kind": kind,
                "buy_date": buy_date.isoformat(),
                "amount": amount,
                "cost_nav": cost_nav,
                "status": status,
            }
        )
    return validated


def current_mode(now: datetime | None = None) -> str:
    """Return morning, evening, or off according to Beijing local time."""
    now = now or datetime.now(BEIJING_TZ)
    clock = now.astimezone(BEIJING_TZ).time()
    if time(8, 30) <= clock <= time(10, 30):
        return "morning"
    if time(14, 0) <= clock <= time(15, 30):
        return "evening"
    return "off"


def _first_existing(frame: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    for name in names:
        if name in frame.columns:
            return name
    return None


def _short_error(exc: Exception, limit: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(exc)).strip()
    return text if len(text) <= limit else f"{text[:limit - 3]}..."


class _AkshareTimeout(TimeoutError):
    """Raised when an external AkShare request exceeds the per-call limit."""


def _ak_call(func: Callable[[], Any], seconds: int = 10) -> Any:
    """Run one network-backed AkShare call with a hard timeout on Linux runners."""
    if not hasattr(signal, "SIGALRM"):
        return func()
    previous = signal.getsignal(signal.SIGALRM)

    def alarm_handler(_signum: int, _frame: Any) -> None:
        raise _AkshareTimeout(f"AkShare 请求超过 {seconds} 秒")

    signal.signal(signal.SIGALRM, alarm_handler)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        return func()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _normalise_history(frame: Any) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ValueError("接口返回空数据")
    date_col = _first_existing(frame, ("日期", "净值日期", "date", "时间"))
    value_col = _first_existing(
        frame, ("收盘", "收盘价", "close", "单位净值", "累计净值", "净值", "value")
    )
    if not date_col or not value_col:
        raise ValueError(f"无法识别日期/价格列: {list(frame.columns)}")
    result = frame[[date_col, value_col]].copy()
    result.columns = ["date", "close"]
    result["date"] = pd.to_datetime(result["date"], errors="coerce")
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result = result.dropna().drop_duplicates("date").sort_values("date")
    return result.tail(HISTORY_ROWS).reset_index(drop=True)


def _sina_symbol(code: str) -> str:
    return f"sh{code}" if code.startswith(("5", "6")) else f"sz{code}"


def _history_attempts(code: str, kind: str) -> list[Callable[[], Any]]:
    start_date = (datetime.now() - timedelta(days=650)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")
    if _is_alipay_fund(kind):
        return [
            lambda: ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势"),
            lambda: ak.fund_etf_fund_info_em(fund=code),
        ]
    if kind == "benchmark":
        return [
            lambda: ak.fund_etf_hist_em(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            ),
            lambda: ak.fund_etf_hist_sina(symbol=_sina_symbol(code)),
            lambda: ak.stock_zh_index_daily_em(symbol="sh000300"),
        ]
    return [
        lambda: ak.fund_etf_hist_em(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        ),
        # 新浪接口作为不同数据源的备用；其价格序列没有 qfq 参数，优先级低于东财。
        lambda: ak.fund_etf_hist_sina(symbol=_sina_symbol(code)),
        lambda: ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        ),
    ]


def fetch_history(code: str, kind: str) -> tuple[pd.DataFrame, str, str]:
    """Fetch one history series and return normalized data, source code, and source name."""
    errors: list[str] = []
    for attempt in _history_attempts(code, kind):
        try:
            return _normalise_history(_ak_call(attempt)), code, "akshare"
        except Exception as exc:
            errors.append(_short_error(exc))
    raise RuntimeError("；".join(errors))


def fetch_item_history(item: dict[str, Any]) -> tuple[pd.DataFrame, str, str]:
    errors: list[str] = []
    for data_code in item.get("data_codes", (item["code"],)):
        try:
            return fetch_history(data_code, item["kind"])
        except Exception as exc:
            errors.append(f"{data_code}: {_short_error(exc)}")
    raise RuntimeError("；".join(errors))


def _spot_column(frame: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    return _first_existing(frame, names)


def _etf_spot() -> pd.DataFrame:
    global _ETF_SPOT_CACHE
    if _ETF_SPOT_CACHE is None:
        frame = _ak_call(ak.fund_etf_spot_em)
        if frame is None or frame.empty:
            raise ValueError("全市场 ETF 实时行情为空")
        _ETF_SPOT_CACHE = frame
    return _ETF_SPOT_CACHE.copy()


def etf_realtime_snapshot(failures: list[str]) -> dict[str, dict[str, Any]]:
    """Normalize the current ETF quote table once for analysis and reporting."""
    global _ETF_SPOT_INDEX
    if _ETF_SPOT_INDEX is not None:
        return _ETF_SPOT_INDEX
    try:
        frame = _etf_spot()
        code_col = _spot_column(frame, ("代码", "基金代码", "code"))
        name_col = _spot_column(frame, ("名称", "基金简称", "name"))
        price_col = _spot_column(frame, ("最新价", "现价", "价格", "price"))
        change_col = _spot_column(frame, ("涨跌幅", "日涨跌幅", "change"))
        amount_col = _spot_column(frame, ("成交额", "成交金额", "amount"))
        if not code_col or not price_col:
            raise ValueError(f"ETF 实时行情字段不完整: {list(frame.columns)}")
        quotes: dict[str, dict[str, Any]] = {}
        for _, row in frame.iterrows():
            match = re.search(r"\d{6}", str(row[code_col]))
            price = _percent_number(row[price_col])
            if not match or price is None or price <= 0:
                continue
            change = _percent_number(row[change_col]) if change_col else None
            quotes[match.group(0)] = {
                "quote_price": price,
                "quote_change": change,
                "quote_name": str(row[name_col]).strip() if name_col else "",
                "quote_turnover": _percent_number(row[amount_col]) if amount_col else None,
                "quote_source": "AKShare fund_etf_spot_em 实时行情",
            }
        if not quotes:
            raise ValueError("ETF 实时行情未解析出有效记录")
        _ETF_SPOT_INDEX = quotes
        return quotes
    except Exception as exc:
        failures.append(f"ETF 实时行情：{_short_error(exc)}；场内标的使用历史行情回退")
        _ETF_SPOT_INDEX = {}
        return {}


def _percent_number(value: Any) -> float | None:
    number = pd.to_numeric(str(value).replace("%", "").replace(",", ""), errors="coerce")
    return None if pd.isna(number) else float(number)


def fund_intraday_estimations(failures: list[str]) -> dict[str, dict[str, Any]]:
    """Fetch Eastmoney intraday NAV estimates once and index them by fund code."""
    global _FUND_ESTIMATION_CACHE
    if _FUND_ESTIMATION_CACHE is not None:
        return _FUND_ESTIMATION_CACHE
    try:
        frame = _ak_call(lambda: ak.fund_value_estimation_em(symbol="全部"), seconds=20)
        if frame is None or frame.empty:
            raise ValueError("净值估算接口返回空数据")
        code_col = _first_existing(frame, ("基金代码", "代码", "fund_code"))
        name_col = _first_existing(frame, ("基金名称", "基金简称", "名称"))
        growth_col = next((col for col in frame.columns if "估算增长率" in str(col)), None)
        value_col = next((col for col in frame.columns if "估算值" in str(col)), None)
        if not code_col or not growth_col:
            raise ValueError(f"净值估算字段不完整: {list(frame.columns)}")
        estimates: dict[str, dict[str, Any]] = {}
        for _, row in frame.iterrows():
            code_match = re.search(r"\d{6}", str(row[code_col]))
            growth = _percent_number(row[growth_col])
            if not code_match or growth is None:
                continue
            code = code_match.group(0)
            estimates[code] = {
                "est_growth": growth,
                "est_value": _percent_number(row[value_col]) if value_col else None,
                "est_name": str(row[name_col]).strip() if name_col else "",
                "est_column": str(growth_col),
            }
        if not estimates:
            raise ValueError("净值估算接口未解析出有效记录")
        _FUND_ESTIMATION_CACHE = estimates
        return estimates
    except Exception as exc:
        failures.append(f"盘中净值估算：{_short_error(exc)}；本次使用最新日收益替代并明确标注")
        _FUND_ESTIMATION_CACHE = {}
        return {}


def _sector_name(name: str) -> str:
    groups = (
        (("半导体", "芯片"), "半导体"),
        (("人工智能", "AI", "算力", "软件", "机器人"), "AI / 科技"),
        (("动漫", "游戏"), "游戏传媒"),
        (("北证", "中证2000", "微盘"), "小微盘"),
        (("恒生", "港股", "互联网", "海外"), "港股 / QDII"),
        (("有色", "稀土", "矿业"), "有色金属"),
        (("科创50",), "科创50"),
        (("医疗", "医药"), "医疗"),
        (("消费电子",), "消费电子"),
        (("证券", "券商"), "证券"),
        (("黄金",), "黄金"),
    )
    for words, sector in groups:
        if any(word in name for word in words):
            return sector
    return "其他"


def scan_market_etfs() -> list[dict[str, Any]]:
    """Scan liquid full-market ETFs, then leave 20/60-day ranking to historical analysis."""
    frame = _etf_spot()
    code_col = _spot_column(frame, ("代码", "基金代码", "code"))
    name_col = _spot_column(frame, ("名称", "基金简称", "name"))
    change_col = _spot_column(frame, ("涨跌幅", "日涨跌幅", "change"))
    amount_col = _spot_column(frame, ("成交额", "成交金额", "amount"))
    if not code_col or not name_col:
        raise ValueError(f"全市场 ETF 接口缺少代码/名称列: {list(frame.columns)}")

    work = frame.copy()
    work["_code"] = work[code_col].astype(str).str.extract(r"(\d{6})", expand=False)
    work["_name"] = work[name_col].astype(str)
    work = work.dropna(subset=["_code"]).copy()
    if change_col:
        work["_change"] = pd.to_numeric(
            work[change_col].astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False),
            errors="coerce",
        )
    else:
        work["_change"] = 0.0
    if amount_col:
        work["_amount"] = pd.to_numeric(
            work[amount_col].astype(str).str.replace(",", "", regex=False), errors="coerce"
        ).fillna(0)
        liquid = work[work["_amount"] >= 50_000_000]
        if liquid.empty:
            liquid = work
    else:
        liquid = work
    # 保留高流动性、温和上涨和强势异动三组，避免动态池完全被当日涨停附近的品种占满。
    liquid_by_turnover = liquid.sort_values("_amount", ascending=False).head(8)
    moderate = liquid[
        liquid["_change"].between(ENTRY_MIN_DAILY_MOVE, ENTRY_MAX_DAILY_MOVE, inclusive="both")
    ].sort_values(["_change", "_amount"], ascending=False).head(6)
    movers = liquid.sort_values(["_change", "_amount"], ascending=False).head(4)
    liquid = (
        pd.concat([liquid_by_turnover, moderate, movers], ignore_index=True)
        .drop_duplicates("_code")
        .head(MAX_DYNAMIC_CANDIDATES)
    )
    return [
        {"code": row["_code"], "name": row["_name"], "kind": "etf", "data_codes": (row["_code"],), "dynamic": True}
        for _, row in liquid.iterrows()
    ]


def scan_intraday_t0(failures: list[str]) -> list[dict[str, Any]]:
    """Assess representative T+0 ETFs without treating feasibility as a profit forecast."""
    try:
        frame = _etf_spot()
        code_col = _spot_column(frame, ("代码", "基金代码", "code"))
        name_col = _spot_column(frame, ("名称", "基金简称", "name"))
        price_col = _spot_column(frame, ("最新价", "现价", "价格", "price"))
        change_col = _spot_column(frame, ("涨跌幅", "日涨跌幅", "change"))
        amount_col = _spot_column(frame, ("成交额", "成交金额", "amount"))
        if not code_col or not name_col or not price_col or not amount_col:
            raise ValueError(f"日内扫描缺少必要列: {list(frame.columns)}")

        work = frame.copy()
        work["_code"] = work[code_col].astype(str).str.extract(r"(\d{6})", expand=False)
        work["_price"] = pd.to_numeric(
            work[price_col].astype(str).str.replace(",", "", regex=False), errors="coerce"
        )
        work["_amount"] = pd.to_numeric(
            work[amount_col].astype(str).str.replace(",", "", regex=False), errors="coerce"
        ).fillna(0)
        if change_col:
            work["_change"] = pd.to_numeric(
                work[change_col].astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False),
                errors="coerce",
            ).fillna(0)
        else:
            work["_change"] = 0.0

        candidates: list[dict[str, Any]] = []
        for code, configured in T0_ETF_ALLOWLIST.items():
            matches = work[work["_code"] == code]
            if matches.empty:
                continue
            row = matches.iloc[0]
            price = float(row["_price"])
            if not math.isfinite(price) or price <= 0:
                continue
            lot_cost = price * 100
            affordable_lots = math.floor(TOTAL_CAPITAL / lot_cost)
            trade_value = affordable_lots * lot_cost
            round_trip_cost = BROKER_MIN_COMMISSION * 2
            cost_pct = round_trip_cost / trade_value * 100 if trade_value else math.inf
            liquid = float(row["_amount"]) >= MIN_INTRADAY_TURNOVER
            executable = (
                affordable_lots >= 1
                and liquid
                and cost_pct <= INTRADAY_MAX_ROUND_TRIP_COST_PCT
            )
            candidates.append(
                {
                    "code": code,
                    "name": str(row[name_col]) or configured["name"],
                    "category": configured["category"],
                    "price": price,
                    "change": float(row["_change"]),
                    "turnover": float(row["_amount"]),
                    "lot_cost": lot_cost,
                    "affordable_lots": affordable_lots,
                    "trade_value": trade_value,
                    "round_trip_cost": round_trip_cost,
                    "round_trip_cost_pct": cost_pct,
                    "break_even_move_pct": cost_pct,
                    "liquid": liquid,
                    "executable": executable,
                }
            )
        return sorted(
            candidates,
            key=lambda item: (item["executable"], item["change"], item["turnover"]),
            reverse=True,
        )
    except Exception as exc:
        failures.append(f"T+0 日内扫描：{_short_error(exc)}")
        return []


def _fund_catalog() -> pd.DataFrame:
    global _FUND_CATALOG_CACHE
    if _FUND_CATALOG_CACHE is not None:
        return _FUND_CATALOG_CACHE.copy()
    attempts = [
        lambda: ak.fund_name_em(),
        lambda: ak.fund_open_fund_daily_em(),
    ]
    errors: list[str] = []
    for attempt in attempts:
        try:
            frame = _ak_call(attempt)
            if frame is None or frame.empty:
                raise ValueError("基金目录为空")
            code_col = _first_existing(frame, ("基金代码", "代码", "fund_code"))
            name_col = _first_existing(frame, ("基金简称", "名称", "fund_name"))
            if not code_col or not name_col:
                raise ValueError(f"基金目录缺少代码/名称列: {list(frame.columns)}")
            catalog = frame[[code_col, name_col]].rename(
                columns={code_col: "code", name_col: "name"}
            )
            catalog["code"] = catalog["code"].astype(str).str.extract(r"(\d{6})", expand=False)
            catalog["name"] = catalog["name"].astype(str).str.strip()
            catalog = catalog.dropna().drop_duplicates("code")
            _FUND_CATALOG_CACHE = catalog.reset_index(drop=True)
            return _FUND_CATALOG_CACHE.copy()
        except Exception as exc:
            errors.append(_short_error(exc))
    raise RuntimeError("；".join(errors))


def _clean_fund_name(name: str) -> str:
    return re.sub(r"ETF|交易型|开放式|发起式|指数|证券|基金|联接|场内|场外|C类|C", "", name).lower()


def match_linked_c_fund(etf: dict[str, Any], catalog: pd.DataFrame) -> dict[str, Any] | None:
    """Best-effort fuzzy match for a dynamic ETF's Alipay C-class feeder fund."""
    target = _clean_fund_name(etf["name"])
    if len(target) < 2:
        return None
    candidates = catalog[catalog["name"].astype(str).str.contains("联接", na=False)].copy()
    candidates = candidates[candidates["name"].astype(str).str.contains(r"C(?:类)?$", regex=True, na=False)]
    best: tuple[float, str, str] | None = None
    for _, row in candidates.iterrows():
        code = str(row["code"]).zfill(6)
        name = str(row["name"])
        if not re.fullmatch(r"\d{6}", code):
            continue
        score = SequenceMatcher(None, target, _clean_fund_name(name)).ratio()
        if best is None or score > best[0]:
            best = (score, code, name)
    if best is None or best[0] < 0.38:
        return None
    return {"code": best[1], "name": best[2], "kind": "linked_c", "data_codes": (best[1],), "matched_etf": etf["code"]}


def _validate_core_funds(
    items: list[dict[str, Any]], catalog: pd.DataFrame, failures: list[str]
) -> list[dict[str, Any]]:
    names = dict(zip(catalog["code"].astype(str).str.zfill(6), catalog["name"].astype(str)))
    verified: list[dict[str, Any]] = []
    for item in items:
        if item["kind"] != "alipay_c":
            verified.append(item)
            continue
        official_name = names.get(item["code"])
        if official_name is None:
            failures.append(f"{item['code']} {item['name']}：基金目录未找到该代码，已跳过")
            continue
        similarity = SequenceMatcher(
            None, _clean_fund_name(item["name"]), _clean_fund_name(official_name)
        ).ratio()
        if similarity < 0.55:
            failures.append(
                f"{item['code']} 代码名称核验失败：配置为“{item['name']}”，官方目录为“{official_name}”，已跳过"
            )
            continue
        checked = dict(item)
        checked["name"] = official_name
        checked["name_verified"] = True
        verified.append(checked)
    return verified


def build_watchlist(
    failures: list[str], holdings: list[dict[str, Any]] | None = None
) -> tuple[list[dict[str, Any]], int]:
    items = [dict(item) for item in CORE_WATCHLIST]
    catalog: pd.DataFrame | None = None
    try:
        catalog = _fund_catalog()
        items = _validate_core_funds(items, catalog, failures)
    except Exception as exc:
        failures.append(
            f"核心基金目录在线核验失败，继续使用已配置核心池并逐只抓取净值：{_short_error(exc)}"
        )
        for item in items:
            if item["kind"] == "alipay_c":
                item["name_verified"] = False
    existing = {item["code"] for item in items}
    for position in holdings or []:
        if position["code"] not in existing:
            items.append(
                {
                    "code": position["code"],
                    "name": position["name"],
                    "kind": position["kind"],
                    "data_codes": (position["code"],),
                    "holding": True,
                }
            )
            existing.add(position["code"])
    dynamic_count = 0
    try:
        dynamic = scan_market_etfs()
        for item in dynamic:
            if item["code"] not in existing:
                items.append(item)
                existing.add(item["code"])
                dynamic_count += 1
    except Exception as exc:
        failures.append(f"全市场 ETF 扫描：{_short_error(exc)}")

    if dynamic_count:
        try:
            if catalog is None:
                catalog = _fund_catalog()
            matched = 0
            for item in items:
                if not item.get("dynamic") or matched >= MAX_LINKED_FUNDS:
                    continue
                linked = match_linked_c_fund(item, catalog)
                if linked and linked["code"] not in existing:
                    items.append(linked)
                    existing.add(linked["code"])
                    matched += 1
        except Exception as exc:
            failures.append(f"支付宝 C 类联接匹配：{_short_error(exc)}")
    return items, dynamic_count


def _return(close: pd.Series, days: int) -> float:
    return (float(close.iloc[-1]) / float(close.iloc[-1 - days]) - 1) * 100


def _merge_live_price(history: pd.DataFrame, price: float, quote_date: datetime.date) -> pd.DataFrame:
    """Merge a live quote without overwriting the previous trading day's close."""
    merged = history.copy()
    last_date = pd.Timestamp(merged["date"].iloc[-1]).date()
    if quote_date > last_date:
        merged = pd.concat(
            [merged, pd.DataFrame({"date": [pd.Timestamp(quote_date)], "close": [float(price)]})],
            ignore_index=True,
        )
    else:
        merged.loc[merged.index[-1], "close"] = float(price)
    return merged.tail(HISTORY_ROWS).reset_index(drop=True)


def _wilson_lower_bound(wins: int, observations: int, z: float = 1.2816) -> float:
    """Return a conservative binomial win-rate bound (80% two-sided interval)."""
    if observations <= 0:
        return 0.0
    probability = wins / observations
    denominator = 1 + z * z / observations
    centre = probability + z * z / (2 * observations)
    margin = z * math.sqrt(
        probability * (1 - probability) / observations + z * z / (4 * observations**2)
    )
    return max(0.0, (centre - margin) / denominator) * 100


def _rolling_entry_backtest(close: pd.Series) -> dict[str, Any]:
    """Walk forward through history and score the same non-chasing entry rule."""
    outcomes: list[tuple[float, float]] = []
    for index in range(60, len(close) - 5):
        window = close.iloc[: index + 1]
        latest = float(window.iloc[-1])
        r1 = _return(window, 1)
        r3 = _return(window, 3)
        r5 = _return(window, 5)
        r20 = _return(window, 20)
        ma20 = float(window.tail(20).mean())
        high20 = float(window.tail(20).max())
        drawdown = (latest / high20 - 1) * 100
        entry = (
            r3 > 0
            and r5 > 0
            and r20 > 0
            and latest >= ma20
            and ENTRY_MIN_DAILY_MOVE <= r1 <= ENTRY_MAX_DAILY_MOVE
            and drawdown > MAX_DRAWDOWN_LIMIT
        )
        if entry:
            outcomes.append(
                (
                    (float(close.iloc[index + 3]) / latest - 1) * 100,
                    (float(close.iloc[index + 5]) / latest - 1) * 100,
                )
            )
    if not outcomes:
        return {
            "backtest_signals": 0,
            "backtest_win3": None,
            "backtest_win5": None,
            "backtest_avg3": None,
            "backtest_avg5": None,
            "backtest_worst5": None,
            "backtest_win3_lower": 0.0,
            "backtest_win5_lower": 0.0,
        }
    three_day = [item[0] for item in outcomes]
    five_day = [item[1] for item in outcomes]
    return {
        "backtest_signals": len(outcomes),
        "backtest_win3": sum(value > 0 for value in three_day) / len(three_day) * 100,
        "backtest_win5": sum(value > 0 for value in five_day) / len(five_day) * 100,
        "backtest_avg3": sum(three_day) / len(three_day),
        "backtest_avg5": sum(five_day) / len(five_day),
        "backtest_worst5": min(five_day),
        "backtest_win3_lower": _wilson_lower_bound(sum(value > 0 for value in three_day), len(outcomes)),
        "backtest_win5_lower": _wilson_lower_bound(sum(value > 0 for value in five_day), len(outcomes)),
    }


def analyse_item(
    item: dict[str, Any],
    benchmark_returns: dict[int, float],
    as_of: datetime,
    intraday_estimations: dict[str, dict[str, Any]] | None = None,
    realtime_quotes: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    history, data_code, source = fetch_item_history(item)
    if len(history) < 61:
        raise ValueError(f"有效数据 {len(history)} 条，少于 61 条")
    official_data_date = history["date"].iloc[-1].date()
    quote = (realtime_quotes or {}).get(item["code"], {}) if item["kind"] == "etf" else {}
    quote_price = quote.get("quote_price")
    if quote_price is not None:
        history = _merge_live_price(
            history, float(quote_price), as_of.astimezone(BEIJING_TZ).date()
        )
    close = history["close"].copy()
    returns = {days: _return(close, days) for days in (1, 3, 5, 20, 60)}
    daily_returns = close.pct_change().dropna().tail(20) * 100
    max_daily_gain20 = float(daily_returns.max())
    daily_volatility20 = float(daily_returns.std(ddof=0))
    target_move_days20 = int((daily_returns >= TARGET_DAILY_MOVE).sum())
    latest = float(close.iloc[-1])
    ma20 = float(close.tail(20).mean())
    ma60 = float(close.tail(60).mean())
    high20 = float(close.tail(20).max())
    drawdown = (latest / high20 - 1) * 100
    data_age_days = (as_of.astimezone(BEIJING_TZ).date() - official_data_date).days
    effective_age_days = 0 if quote_price is not None else data_age_days
    score = (
        returns[5] * MOMENTUM_WEIGHTS[0]
        + returns[20] * MOMENTUM_WEIGHTS[1]
        + returns[60] * MOMENTUM_WEIGHTS[2]
    )
    estimation = (intraday_estimations or {}).get(item["code"], {})
    est_growth = estimation.get("est_growth")
    quote_change = quote.get("quote_change")
    if quote_change is not None:
        intraday_growth = float(quote_change)
        intraday_source = quote.get("quote_source", "AKShare ETF 实时行情")
    elif est_growth is not None:
        intraday_growth = float(est_growth)
        intraday_source = "东方财富盘中净值估算"
    else:
        intraday_growth = returns[1]
        intraday_source = "最新日收益回退"
    burst_score = (
        intraday_growth * BURST_WEIGHTS[0]
        + returns[3] * BURST_WEIGHTS[1]
        + returns[5] * BURST_WEIGHTS[2]
        + returns[20] * BURST_WEIGHTS[3]
    )
    short_score = (
        returns[1] * SHORT_MOMENTUM_WEIGHTS[0]
        + returns[3] * SHORT_MOMENTUM_WEIGHTS[1]
        + returns[5] * SHORT_MOMENTUM_WEIGHTS[2]
    )
    rs20 = returns[20] - benchmark_returns.get(20, 0.0)
    rs60 = returns[60] - benchmark_returns.get(60, 0.0)
    backtest = _rolling_entry_backtest(close)
    trend_agreement = sum((latest >= ma20, latest >= ma60, rs20 > 0, rs60 > 0)) / 4
    if backtest["backtest_signals"] >= MIN_BACKTEST_SIGNALS:
        win_quality = min(1.0, max(0.0, (backtest["backtest_win3"] - 50) / 20))
        avg_quality = min(1.0, max(0.0, backtest["backtest_avg3"] / 1.0))
        backtest_quality = win_quality * 0.6 + avg_quality * 0.4
    else:
        backtest_quality = 0.0
    freshness_quality = 1.0 if quote_price is not None or est_growth is not None else max(0.0, 1 - data_age_days / 10)
    completeness_quality = min(1.0, len(history) / HISTORY_ROWS)
    data_quality = round(100 * (freshness_quality * 0.55 + completeness_quality * 0.45), 1)
    evidence_quality = round(100 * (backtest_quality * 0.65 + trend_agreement * 0.35), 1)
    data_freshness = (
        "实时 ETF 行情" if quote_price is not None
        else ("盘中净值估算" if est_growth is not None else "历史净值/行情")
    )
    return {
        **item,
        "data_code": data_code,
        "source": source,
        "data_date": official_data_date.isoformat(),
        "data_age_days": effective_age_days,
        "stale": effective_age_days > MAX_DATA_AGE_DAYS,
        "data_freshness": data_freshness,
        "data_quality": data_quality,
        "evidence_quality": evidence_quality,
        # Retained for dashboard compatibility; this is evidence strength, not accuracy.
        "prediction_quality": evidence_quality,
        "quality_label": "高" if evidence_quality >= 70 else ("中" if evidence_quality >= 50 else "低"),
        "history_observations": len(history),
        "quote_price": quote_price,
        "quote_turnover": quote.get("quote_turnover"),
        "quote_date": as_of.astimezone(BEIJING_TZ).date().isoformat() if quote_price is not None else None,
        "latest": latest,
        "r1": returns[1],
        "r3": returns[3],
        "r5": returns[5],
        "r20": returns[20],
        "r60": returns[60],
        "score": score,
        "est_growth": est_growth,
        "est_value": estimation.get("est_value"),
        "est_source": intraday_source,
        "intraday_growth": intraday_growth,
        "burst_score": burst_score,
        "short_score": short_score,
        "selection_score": burst_score if RISK_PROFILE == "aggressive" else score,
        "sector": _sector_name(item["name"]),
        "ma20": ma20,
        "ma60": ma60,
        "above_ma20": latest >= ma20,
        "above_ma60": latest >= ma60,
        "drawdown": drawdown,
        "stop": drawdown <= MAX_DRAWDOWN_LIMIT,
        "rs20": rs20,
        "rs60": rs60,
        "rs_score": rs20 * 0.6 + rs60 * 0.4,
        "max_daily_gain20": max_daily_gain20,
        "daily_volatility20": daily_volatility20,
        "target_move_days20": target_move_days20,
        "high_move_capable": max_daily_gain20 >= TARGET_DAILY_MOVE,
        "benchmark_verified": bool(benchmark_returns),
        **backtest,
    }


def _percentile_scores(results: list[dict[str, Any]], field: str) -> dict[int, float]:
    series = pd.Series([float(item[field]) for item in results])
    ranks = series.rank(method="average", pct=True) * 100
    return {id(item): float(ranks.iloc[index]) for index, item in enumerate(results)}


def _calculate_composite_scores(results: list[dict[str, Any]]) -> None:
    """Create a cross-sectional score from independent return, risk and evidence dimensions."""
    if not results:
        return
    momentum = _percentile_scores(results, "score")
    relative_strength = _percentile_scores(results, "rs_score")
    burst = _percentile_scores(results, "burst_score")
    volatility = _percentile_scores(results, "daily_volatility20")
    drawdown = _percentile_scores(results, "drawdown")
    for item in results:
        trend = sum((item["above_ma20"], item["above_ma60"], item["rs20"] > 0, item["rs60"] > 0)) / 4 * 100
        risk = drawdown[id(item)] * 0.65 + (100 - volatility[id(item)]) * 0.35
        evidence = min(100.0, item["evidence_quality"])
        live_weight = 0.08 if RISK_PROFILE == "aggressive" else 0.04
        item["composite_score"] = round(
            momentum[id(item)] * 0.30
            + relative_strength[id(item)] * 0.22
            + trend * 0.18
            + risk * 0.12
            + evidence * (0.18 - live_weight)
            + burst[id(item)] * live_weight,
            2,
        )
        item["selection_score"] = item["composite_score"]


def _rank_results(results: list[dict[str, Any]]) -> None:
    if not results:
        return
    ordered = sorted(results, key=lambda row: row["selection_score"], reverse=True)
    total = len(ordered)
    for rank, item in enumerate(ordered, start=1):
        item["rank"] = rank
        item["pool_percentile"] = round((total - rank + 1) / total * 100, 1)
        item["pool_top_percent"] = max(1, math.ceil(rank / total * 100))
        if item["stale"]:
            item["action"] = "数据过期"
            item["amount"] = 0
        elif not item["benchmark_verified"]:
            item["action"] = "基准缺失"
            item["amount"] = 0
        elif item["stop"]:
            item["action"] = "风控止损"
            item["amount"] = 0
        else:
            trend_entry = (
                item["selection_score"] >= SIGNAL_MIN_SCORE
                and item["r3"] > 0
                and item["r5"] > 0
                and item["r20"] > 0
                and item["above_ma20"]
                and (item["above_ma60"] or not REQUIRE_MA60)
                and item["pool_percentile"] >= SIGNAL_PERCENTILE
                and item["rs20"] > 0
                and item["rs_score"] > 0
            )
            backtest_supported = (
                item["backtest_signals"] >= MIN_BACKTEST_SIGNALS
                and (
                    (
                        item["backtest_win3_lower"] >= 45
                        and item["backtest_avg3"] > 0
                    )
                    or (
                        item["backtest_win5_lower"] >= 45
                        and item["backtest_avg5"] > 0
                    )
                )
            )
            if not trend_entry:
                item["action"] = "持有/观察" if item["above_ma20"] else "暂缓/观望"
                item["amount"] = 0
                continue
            if item["intraday_growth"] > ENTRY_MAX_DAILY_MOVE:
                item["action"] = "等待回踩"
                item["amount"] = 0
                continue
            if item["intraday_growth"] < ENTRY_MIN_DAILY_MOVE:
                item["action"] = "下跌未止"
                item["amount"] = 0
                continue
            if not backtest_supported:
                item["action"] = "回测不支持"
                item["amount"] = 0
                continue
            proposed_amount = (
                HIGH_CONVICTION_BUY
                if (
                    item["pool_percentile"] >= 80
                    and item["backtest_win3_lower"] >= 52
                    and item["backtest_avg3"] >= 0.5
                )
                else STANDARD_BUY
            )
            item["suggested_amount"] = proposed_amount
            if item["kind"] == "etf" and BROKER_MIN_COMMISSION > 0:
                round_trip_cost = BROKER_MIN_COMMISSION * 2
                cost_pct = round_trip_cost / proposed_amount * 100
                item["exchange_round_trip_cost"] = round_trip_cost
                item["exchange_round_trip_cost_pct"] = cost_pct
                if cost_pct > MAX_EXCHANGE_ROUND_TRIP_COST_PCT:
                    item["action"] = "场内成本过高"
                    item["amount"] = 0
                    continue
            item["action"] = "买入观察"
            item["amount"] = proposed_amount


def _fetch_manager_frame() -> tuple[pd.DataFrame, list[int]]:
    """Fetch manager pages independently so one failed page does not discard all data."""
    from akshare.utils import demjson

    url = "https://fund.eastmoney.com/Data/FundDataPortfolio_Interface.aspx"
    base_params = {
        "dt": "14",
        "mc": "returnjson",
        "ft": "all",
        "pn": "500",
        "sc": "abbname",
        "st": "asc",
    }

    def decode(response: requests.Response) -> dict[str, Any]:
        response.raise_for_status()
        payload = re.sub(r"^\s*var\s+returnjson\s*=\s*", "", response.text).strip().rstrip(";")
        return demjson.decode(payload)

    session = requests.Session()
    pages: list[pd.DataFrame] = []
    failed_pages: list[int] = []
    try:
        first = decode(session.get(url, params={**base_params, "pi": 1}, timeout=12))
        total_pages = int(first.get("pages", 1))
        for page in range(1, total_pages + 1):
            page_data = first if page == 1 else None
            if page_data is None:
                for _attempt in range(3):
                    try:
                        page_data = decode(
                            session.get(url, params={**base_params, "pi": page}, timeout=12)
                        )
                        break
                    except (requests.RequestException, ValueError):
                        continue
            if page_data is None:
                failed_pages.append(page)
                continue
            frame = pd.DataFrame(page_data.get("data", []))
            if not frame.empty:
                pages.append(frame)
    finally:
        session.close()
    if not pages:
        raise ValueError("基金经理分页均未返回有效数据")
    combined = pd.concat(pages, ignore_index=True)
    if combined.shape[1] < 11:
        raise ValueError(f"基金经理原始字段不足: {combined.shape[1]}")
    return pd.DataFrame(
        {
            "姓名": combined.iloc[:, 1],
            "现任基金代码": combined.iloc[:, 4],
            "累计从业时间": pd.to_numeric(combined.iloc[:, 6], errors="coerce"),
        }
    ), failed_pages


def enrich_manager_experience(results: list[dict[str, Any]], failures: list[str]) -> None:
    """Attach current manager names and cumulative industry experience."""
    for item in results:
        item.update(
            {
                "manager_names": "待核验",
                "manager_years": None,
                "manager_verified": False,
                "manager_preference_met": False,
            }
    )
    try:
        frame, failed_pages = _fetch_manager_frame()
        if failed_pages:
            failures.append(f"基金经理分页 {failed_pages} 暂时失败，其余页面继续使用")
        required = {"姓名", "现任基金代码", "累计从业时间"}
        if frame is None or frame.empty or not required.issubset(frame.columns):
            raise ValueError(f"基金经理字段不完整: {list(getattr(frame, 'columns', []))}")
        by_code: dict[str, list[dict[str, Any]]] = {}
        for _, row in frame.iterrows():
            days = pd.to_numeric(row["累计从业时间"], errors="coerce")
            if pd.isna(days):
                continue
            record = {"name": str(row["姓名"]).strip(), "years": float(days) / 365.25}
            for code in set(re.findall(r"\d{6}", str(row["现任基金代码"]))):
                by_code.setdefault(code, []).append(record)
        for item in results:
            managers = by_code.get(item["code"], [])
            if not managers:
                continue
            manager_years = min(manager["years"] for manager in managers)
            item.update(
                {
                    "manager_names": " / ".join(dict.fromkeys(manager["name"] for manager in managers)),
                    "manager_years": manager_years,
                    "manager_verified": True,
                    "manager_preference_met": manager_years >= MANAGER_MIN_YEARS,
                }
            )
    except Exception as exc:
        failures.append(f"基金经理从业数据：{_short_error(exc)}")

    # 经理经验是质量提示，不是指数联接基金的硬性买入门槛；被动基金的核心风险
    # 还包括跟踪误差、费用、溢价和标的指数走势。未知数据只降低置信度，不篡改行情信号。


def _news_keyword(item: dict[str, Any]) -> str:
    name = item["name"]
    keyword_groups = (
        (("有色", "矿业"), "有色金属"),
        (("稀土",), "稀土"),
        (("半导体", "芯片"), "半导体"),
        (("人工智能", "AI"), "人工智能"),
        (("医疗", "医药"), "医疗医药"),
        (("消费电子",), "消费电子"),
        (("证券", "券商"), "证券"),
        (("黄金",), "黄金"),
        (("恒生科技", "互联网"), "互联网科技"),
        (("游戏", "动漫"), "游戏"),
        (("电力",), "电力"),
    )
    for words, keyword in keyword_groups:
        if any(word in name for word in words):
            return keyword
    return re.sub(r"(?:ETF|联接|发起式|QDII|基金|[AC]|\([^)]*\))", "", name)[:12]


def _news_sentiment(text: str) -> int:
    positive = ("增长", "上涨", "突破", "利好", "增持", "复苏", "提振", "创新高", "扩产")
    negative = ("下跌", "回落", "利空", "减持", "亏损", "调查", "处罚", "风险", "承压")
    return sum(word in text for word in positive) - sum(word in text for word in negative)


def enrich_news_and_scenarios(results: list[dict[str, Any]], failures: list[str], now: datetime) -> None:
    """Add a bounded technical scenario estimate and recent theme-news context."""
    for item in results:
        item.update(
            {"news_sentiment": 0, "news_titles": [], "news_items": [], "news_keyword": _news_keyword(item)}
        )

    candidates = sorted(
        (item for item in results if item["action"] in {"买入观察", "持有/观察"} or item["code"] in METALS_CODES),
        key=lambda item: item["selection_score"],
        reverse=True,
    )
    themes: list[str] = []
    for item in candidates:
        if item["news_keyword"] not in themes:
            themes.append(item["news_keyword"])
        if len(themes) >= NEWS_MAX_THEMES:
            break

    news_cache: dict[str, dict[str, Any]] = {}
    cutoff = now.astimezone(BEIJING_TZ) - timedelta(days=NEWS_LOOKBACK_DAYS)
    for keyword in themes:
        try:
            frame = _ak_call(lambda keyword=keyword: ak.stock_news_em(symbol=keyword), seconds=15)
            title_col = _first_existing(frame, ("新闻标题", "标题", "title"))
            time_col = _first_existing(frame, ("发布时间", "时间", "datetime"))
            source_col = _first_existing(frame, ("文章来源", "来源", "source"))
            link_col = _first_existing(frame, ("新闻链接", "链接", "url"))
            if frame is None or frame.empty or not title_col:
                news_cache[keyword] = {"score": 0, "titles": [], "items": []}
                continue
            recent: list[str] = []
            recent_items: list[dict[str, str]] = []
            for _, row in frame.iterrows():
                if time_col:
                    published = pd.to_datetime(row[time_col], errors="coerce")
                    if pd.notna(published):
                        if published.tzinfo is None:
                            published = published.tz_localize(BEIJING_TZ)
                        if published.to_pydatetime() < cutoff:
                            continue
                title = str(row[title_col]).strip()
                if title and title != "nan" and title not in recent:
                    recent.append(title)
                    recent_items.append(
                        {
                            "title": title,
                            "source": str(row[source_col]).strip() if source_col else "东方财富检索",
                            "published": str(row[time_col]).strip() if time_col else "",
                            "url": str(row[link_col]).strip() if link_col else "",
                        }
                    )
                if len(recent) >= 3:
                    break
            score = max(-2, min(2, sum(_news_sentiment(title) for title in recent)))
            news_cache[keyword] = {"score": score, "titles": recent, "items": recent_items}
        except Exception as exc:
            news_cache[keyword] = {"score": 0, "titles": [], "items": []}
            failures.append(f"{keyword}近期新闻：{_short_error(exc)}")

    for item in results:
        news = news_cache.get(item["news_keyword"], {"score": 0, "titles": [], "items": []})
        item["news_sentiment"] = news["score"]
        item["news_titles"] = news["titles"]
        item["news_items"] = news["items"]
        technical_center = item["r1"] * 0.45 + item["r3"] / 3 * 0.30 + item["r5"] / 5 * 0.25
        news_adjustment = news["score"] * 0.15
        volatility = max(0.35, item["daily_volatility20"])
        center = max(-5.0, min(5.0, technical_center + news_adjustment))
        radius = min(8.0, max(0.8, volatility * 1.28))
        item["next_day_center"] = center
        item["next_day_low"] = max(-10.0, center - radius)
        item["next_day_high"] = min(10.0, center + radius)
        if item.get("prediction_quality", 0) >= 70 and item.get("data_freshness") in {"实时 ETF 行情", "盘中净值估算"} and item["above_ma20"]:
            item["forecast_confidence"] = "中高"
        elif item.get("prediction_quality", 0) >= 50 and item["above_ma20"] and not item["stale"]:
            item["forecast_confidence"] = "中"
        else:
            item["forecast_confidence"] = "低"


def _redemption_fee_summary(frame: pd.DataFrame) -> tuple[str, int | None]:
    term_col = _first_existing(frame, ("适用期限", "持有期限", "期限"))
    rate_col = _first_existing(frame, ("赎回费率", "费率"))
    if not term_col or not rate_col or frame.empty:
        raise ValueError(f"无法识别赎回费率列: {list(frame.columns)}")
    rows: list[str] = []
    fee_free_days: list[int] = []
    for _, row in frame.iterrows():
        term = str(row[term_col]).strip()
        rate = str(row[rate_col]).strip()
        if not term or not rate or term == "nan" or rate == "nan":
            continue
        rows.append(f"{term} {rate}")
        numeric_rate = pd.to_numeric(rate.replace("%", ""), errors="coerce")
        if pd.notna(numeric_rate) and float(numeric_rate) == 0:
            match = re.search(r"(?:大于等于|不少于|满)(\d+)天", term)
            if match:
                fee_free_days.append(int(match.group(1)))
    if not rows:
        raise ValueError("赎回费率为空")
    return "；".join(rows), min(fee_free_days) if fee_free_days else None


def enrich_redemption_fees(
    results: list[dict[str, Any]], failures: list[str], holding_codes: set[str] | None = None
) -> None:
    """Verify redemption fees for buy candidates and existing Alipay holdings."""
    holding_codes = holding_codes or set()
    for item in results:
        if not _is_alipay_fund(item["kind"]):
            continue
        is_holding = item["code"] in holding_codes
        if item["action"] != "买入观察" and not is_holding:
            continue
        try:
            fee_frame = _ak_call(lambda: ak.fund_fee_em(symbol=item["code"], indicator="赎回费率"))
            summary, fee_free_days = _redemption_fee_summary(fee_frame)
            item["fee_verified"] = True
            item["redemption_fee_summary"] = summary
            item["fee_free_days"] = fee_free_days
            if fee_free_days is None and not is_holding:
                item["action"] = "费率待核验"
                item["amount"] = 0
                failures.append(f"{item['code']} {item['name']}：未识别到零赎回费持有期限，已取消买入信号")
            elif fee_free_days and fee_free_days > MAX_PLANNED_HOLD_DAYS and not is_holding:
                item["action"] = "持有期不匹配"
                item["amount"] = 0
                failures.append(
                    f"{item['code']} {item['name']}：预计免赎回费需 {fee_free_days} 天，超过计划持有 {MAX_PLANNED_HOLD_DAYS} 天，已取消买入信号"
                )
        except Exception as exc:
            item["fee_verified"] = False
            if not is_holding:
                item["action"] = "费率待核验"
                item["amount"] = 0
            failures.append(f"{item['code']} {item['name']}赎回费率：{_short_error(exc)}")


def _position_advice(position: dict[str, Any], item: dict[str, Any] | None, now: datetime) -> str:
    if position["status"] == "pending":
        return "订单待确认：今天不重复加仓，也不能赎回；确认份额和成交净值后再计算真实盈亏"
    held_days = max(0, (now.astimezone(BEIJING_TZ).date() - datetime.strptime(position["buy_date"], "%Y-%m-%d").date()).days)
    if item is None:
        return "行情未获取：保持不动，不依据缺失数据操作"
    if item["stale"] or not item["benchmark_verified"]:
        return "数据或基准不完整：保持不动，停止加仓"
    fee_days = item.get("fee_free_days")
    lock_text = (
        f"已持有约 {held_days} 天，预计免赎回费需 {fee_days} 天"
        if fee_days
        else f"已持有约 {held_days} 天，赎回费期限待支付宝页面核对"
    )
    if held_days >= MAX_PLANNED_HOLD_DAYS:
        return (
            f"已到 {MAX_PLANNED_HOLD_DAYS} 天计划复核点：停止加仓，优先核对实际确认日和赎回费；"
            f"若手续费可接受则分批止盈/退出；{lock_text}"
        )
    if item["stop"]:
        return f"停止加仓并进入减仓观察；{lock_text}，未满足免赎回费期限时先权衡手续费"
    if not item["above_ma20"]:
        return f"弱于 MA20：停止加仓、继续观察；{lock_text}"
    if item["above_ma60"] and item["score"] > 0 and item["rs20"] > 0:
        return f"趋势仍强：继续持有；{lock_text}"
    return f"趋势一般：持有但不追加；{lock_text}"


def enrich_holdings(
    holdings: list[dict[str, Any]], results: list[dict[str, Any]], now: datetime
) -> list[dict[str, Any]]:
    by_code = {item["code"]: item for item in results}
    enriched: list[dict[str, Any]] = []
    for position in holdings:
        item = by_code.get(position["code"])
        current_return = None
        if item is not None and position.get("cost_nav"):
            current_return = (item["latest"] / position["cost_nav"] - 1) * 100
        enriched.append(
            {
                **position,
                "analysis": item,
                "current_return": current_return,
                "advice": _position_advice(position, item, now),
            }
        )
    return enriched


def apply_cash_limit(
    results: list[dict[str, Any]], invested_amount: float, holding_codes: set[str]
) -> None:
    remaining_cash = max(0.0, TOTAL_CAPITAL - invested_amount)
    for item in results:
        if item.get("action") != "买入观察":
            continue
        if remaining_cash < BUY_MIN:
            item["action"] = "现金不足"
            item["amount"] = 0
        elif item["code"] in holding_codes:
            item["amount"] = min(item["amount"], int(remaining_cash))


def analyse_market(now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(BEIJING_TZ)
    mode = current_mode(now)
    if mode == "off":
        mode = "morning" if now.astimezone(BEIJING_TZ).time() < time(12) else "evening"
    failures: list[str] = []
    try:
        holdings = load_holdings()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        holdings = []
        failures.append(f"持仓文件：{_short_error(exc)}")
    watchlist, dynamic_count = build_watchlist(failures, holdings)
    intraday_estimations = fund_intraday_estimations(failures)
    realtime_quotes = etf_realtime_snapshot(failures)
    benchmark_returns: dict[int, float] = {}
    benchmark_date = ""
    try:
        benchmark_history, _, _ = fetch_item_history(BENCHMARK)
        if len(benchmark_history) >= 61:
            benchmark_date = benchmark_history["date"].iloc[-1].date().isoformat()
            benchmark_age = (now.astimezone(BEIJING_TZ).date() - benchmark_history["date"].iloc[-1].date()).days
            if benchmark_age > MAX_DATA_AGE_DAYS:
                failures.append(f"沪深300基准数据过期：最新日期 {benchmark_date}")
            else:
                benchmark_quote = realtime_quotes.get(BENCHMARK["code"], {}).get("quote_price")
                if benchmark_quote is not None:
                    benchmark_history = _merge_live_price(
                        benchmark_history,
                        float(benchmark_quote),
                        now.astimezone(BEIJING_TZ).date(),
                    )
                benchmark_returns = {
                    days: _return(benchmark_history["close"], days) for days in (5, 20, 60)
                }
    except Exception as exc:
        failures.append(f"沪深300基准：{_short_error(exc)}")

    results: list[dict[str, Any]] = []
    for item in watchlist:
        try:
            results.append(analyse_item(item, benchmark_returns, now, intraday_estimations, realtime_quotes))
        except Exception as exc:
            failures.append(f"{item['code']} {item['name']}：{_short_error(exc)}")
    _calculate_composite_scores(results)
    _rank_results(results)
    holding_codes = {position["code"] for position in holdings}
    invested_amount = sum(position["amount"] for position in holdings)
    apply_cash_limit(results, invested_amount, holding_codes)
    enrich_redemption_fees(results, failures, holding_codes)
    enrich_manager_experience(results, failures)
    results.sort(key=lambda row: row["selection_score"], reverse=True)
    enrich_news_and_scenarios(results, failures, now)
    enriched_holdings = enrich_holdings(holdings, results, now)
    intraday_t0 = scan_intraday_t0(failures)
    return {
        "now": now,
        "mode": mode,
        "results": results,
        "failures": failures,
        "dynamic_count": dynamic_count,
        "watch_count": len(watchlist),
        "benchmark_returns": benchmark_returns,
        "benchmark_date": benchmark_date,
        "intraday_estimation_count": len(intraday_estimations),
        "realtime_quote_count": len(realtime_quotes),
        "intraday_t0": intraday_t0,
        "holdings": enriched_holdings,
        "invested_amount": invested_amount,
        "remaining_cash": max(0.0, TOTAL_CAPITAL - invested_amount),
        "fund_catalog": (
            _FUND_CATALOG_CACHE.to_dict("records")
            if _FUND_CATALOG_CACHE is not None
            else []
        ),
    }


def _action_text(item: dict[str, Any]) -> str:
    if item["stale"]:
        return f"数据过期（最新 {item['data_date']}），停止给出交易信号"
    if not item["benchmark_verified"]:
        return "沪深300基准缺失，停止给出交易信号"
    if item["stop"]:
        if _is_alipay_fund(item["kind"]):
            return "风控止损；赎回前必须核对支付宝页面的实际持有天数和赎回费率"
        return "风控止损；建议减仓约 200 元"
    if item["action"] == "场内成本过高":
        return (
            f"仅观察，不下单；按 {item.get('suggested_amount', 0)} 元试仓计算，买卖最低佣金合计 "
            f"{item.get('exchange_round_trip_cost', 0):.0f} 元，往返成本 {item.get('exchange_round_trip_cost_pct', 0):.2f}%"
        )
    if item["action"] == "费率待核验":
        return "赎回费率未能实时核验，停止给出买入信号"
    if item["action"] == "持有期不匹配":
        return f"免赎回费期限超过计划持有 {MAX_PLANNED_HOLD_DAYS} 天，取消买入"
    if item["action"] == "经理待核验":
        return "基金经理累计从业年限未核验，暂不新增资金"
    if item["action"] == "经理年限未达偏好":
        return (
            f"基金经理累计从业约 {item.get('manager_years', 0):.1f} 年，未达到偏好值 "
            f"{MANAGER_MIN_YEARS:.0f} 年，暂不新增资金"
        )
    if item["action"] == "等待回踩":
        return (
            f"盘中估算已上涨 {item['intraday_growth']:.2f}%，避免追涨；回落至 MA20 附近且趋势未破坏时，"
            f"再考虑 {STANDARD_BUY} 元试仓"
        )
    if item["action"] == "下跌未止":
        return f"盘中估算 {item['intraday_growth']:.2f}% 且仍弱于入场区间，先等止跌，不接连续下跌"
    if item["action"] == "回测不支持":
        return (
            f"历史同规则样本 {item['backtest_signals']} 次，3/5日胜率或平均收益未达门槛，"
            "不把短期强势误当成买点"
        )
    if item["action"] == "现金不足":
        return "当前持仓已占用绝大部分资金，剩余现金低于单笔下限，不再新增买入"
    if item["action"] == "买入观察":
        if _is_alipay_fund(item["kind"]):
            holding = item.get("fee_free_days")
            holding_text = f"计划至少持有 {holding} 天" if holding else "赎回前再次核对费率"
            return (
                f"趋势与历史回测支持，建议在支付宝先试仓 {item['amount']} 元；{holding_text}；"
                f"当前费率规则：{item.get('redemption_fee_summary', '未获取')}"
            )
        return f"趋势与历史回测支持，建议分批试仓 {item['amount']} 元"
    if item["action"] == "持有/观察":
        return "站上 MA20，继续观察，不新增资金"
    return "低于 MA20 或相对强度不足，暂缓新增资金"


def _backtest_text(item: dict[str, Any]) -> str:
    if not item.get("backtest_signals") or item.get("backtest_win3") is None:
        return "同规则历史样本不足"
    return (
        f"同规则回测 {item['backtest_signals']} 次，3/5日胜率 "
        f"{item['backtest_win3']:.1f}% / {item['backtest_win5']:.1f}%（平均 "
        f"{item['backtest_avg3']:.2f}% / {item['backtest_avg5']:.2f}%，"
        f"保守胜率下界 {item['backtest_win3_lower']:.1f}% / {item['backtest_win5_lower']:.1f}%，"
        f"最差5日 {item['backtest_worst5']:.2f}%）"
    )


def _line(item: dict[str, Any]) -> str:
    trend = f"MA20 {'上方' if item['above_ma20'] else '下方'} / MA60 {'上方' if item['above_ma60'] else '下方'}"
    manager = (
        f"{item.get('manager_names', '待核验')}，累计从业 {item['manager_years']:.1f} 年"
        if item.get("manager_years") is not None
        else "基金经理年限待核验"
    )
    return (
        f"- **{item['code']} {item['name']}**（{item['sector']} / {item['kind']}）：盘中估算 "
        f"**{item['intraday_growth']:.2f}%**（{item['est_source']}），爆发得分 **{item['burst_score']:.2f}**；1/3/5/20/60日 "
        f"{item['r1']:.2f}% / {item['r3']:.2f}% / {item['r5']:.2f}% / {item['r20']:.2f}% / {item['r60']:.2f}%，"
        f"近20日最大单日上涨 **{item['max_daily_gain20']:.2f}%**（≥{TARGET_DAILY_MOVE:.1f}% 共 {item['target_move_days20']} 次），"
        f"日波动率 {item['daily_volatility20']:.2f}%，"
        f"综合评分 **{item['composite_score']:.2f}/100**，池内分位 **{item['pool_percentile']:.1f}%**（约前 {item['pool_top_percent']}%）；"
        f"数据日期 {item['data_date']}，{trend}，相对沪深300超额20/60日 {item['rs20']:.2f}% / {item['rs60']:.2f}%，"
        f"回撤 {item['drawdown']:.2f}%；数据质量 {item['data_quality']:.0f}/100，历史证据 {item['evidence_quality']:.0f}/100；{manager}；次日情景区间 "
        f"{item.get('next_day_low', 0):.2f}%～{item.get('next_day_high', 0):.2f}%（{item.get('forecast_confidence', '低')}置信度），"
        f"{_backtest_text(item)}，**{_action_text(item)}**。"
    )


def _holding_line(position: dict[str, Any]) -> str:
    item = position.get("analysis")
    status = "交易待确认" if position["status"] == "pending" else "已确认"
    return_text = (
        f"，按确认成本估算收益 {position['current_return']:.2f}%"
        if position.get("current_return") is not None
        else "，截图未含成交净值，暂不计算盈亏"
    )
    signal = (
        f"；最新1日 {item['r1']:.2f}%，7日短动量 {item['short_score']:.2f}，MA20 {'上方' if item['above_ma20'] else '下方'}，"
        f"20日回撤 {item['drawdown']:.2f}%"
        if item
        else "；本次行情获取失败"
    )
    return (
        f"- **{position['code']} {position['name']}**：{position['buy_date']} 买入 "
        f"{position['amount']:.0f} 元（{status}）{return_text}{signal}；**{position['advice']}**。"
    )


def _intraday_line(item: dict[str, Any]) -> str:
    if item["affordable_lots"] < 1:
        decision = f"资金不足购买 1 手（约 {item['lot_cost']:.0f} 元）"
    elif not item["liquid"]:
        decision = "成交额不足 5000 万元，只观察"
    elif not item["executable"]:
        decision = (
            f"佣金占比 {item['round_trip_cost_pct']:.2f}% 超过上限 "
            f"{INTRADAY_MAX_ROUND_TRIP_COST_PCT:.1f}%，不交易"
        )
    else:
        decision = "成本门槛通过；仍须结合盘中趋势人工确认，不代表必然盈利"
    return (
        f"- **{item['code']} {item['name']}**（{item['category']}）：现价 {item['price']:.3f}，"
        f"当日涨跌 {item['change']:.2f}%，1 手约 {item['lot_cost']:.0f} 元；"
        f"按最多 {item['affordable_lots']} 手、成交 {item['trade_value']:.0f} 元测算，"
        f"买卖最低佣金 {item['round_trip_cost']:.0f} 元，至少上涨约 "
        f"{item['break_even_move_pct']:.2f}% 才覆盖固定佣金（未计价差与滑点）；**{decision}**。"
    )


def build_report(context: dict[str, Any]) -> tuple[str, str]:
    now: datetime = context["now"]
    mode = context["mode"]
    results = context["results"]
    alipay_results = [item for item in results if _is_alipay_fund(item["kind"])]
    exchange_results = [item for item in results if item["kind"] == "etf"]
    layered_candidates = sorted(
        (
            item
            for item in results
            if item["action"] in {"买入观察", "等待回踩", "回测不支持"}
            and not item["stale"]
        ),
        key=lambda item: (item["action"] == "买入观察", item["selection_score"]),
        reverse=True,
    )
    title = "早盘 7 日高弹性动量预选" if mode == "morning" else "支付宝 14:30 七日波段实操指南"
    benchmark = context["benchmark_returns"]
    lines = [
        f"# {title}",
        f"更新时间：{now:%Y-%m-%d %H:%M}（北京时间）",
        f"风险档位：{'进攻' if RISK_PROFILE == 'aggressive' else '均衡'}；扫描 {context['watch_count']} 个候选（动态 ETF {context['dynamic_count']} 个），有效分析 {len(results)} 个。",
        "",
        "## 我的持仓复盘（仅微信报告显示明细）",
        f"已投入 {context['invested_amount']:.0f} 元 / 总资金 {TOTAL_CAPITAL} 元；剩余可用资金 {context['remaining_cash']:.0f} 元。",
    ]
    if context["holdings"]:
        lines.extend(_holding_line(position) for position in context["holdings"])
    else:
        lines.append("- 尚未录入持仓。")
    lines += [
        "",
        "## 沪深300基准",
        f"数据日期：{context['benchmark_date'] or '未获取'}；5/20/60日：{benchmark.get(5, 0):.2f}% / {benchmark.get(20, 0):.2f}% / {benchmark.get(60, 0):.2f}%",
        "",
        "## 今日分层操作候选",
    ]
    if layered_candidates:
        lines.extend(_line(item) for item in layered_candidates[:6])
    else:
        lines.append("- 今日没有通过趋势、相对强度和滚动回测的试仓候选；报告仍列出持仓动作与等待条件。")
    lines += [
        "",
        "## 支付宝场外基金优先榜",
    ]
    lines.extend(_line(item) for item in alipay_results[:MAX_REPORT_ITEMS])
    lines += ["", "## 场内 ETF 观察榜（已计入最低佣金）"]
    lines.extend(_line(item) for item in exchange_results[:6])
    intraday = context.get("intraday_t0", [])
    executable_intraday = [item for item in intraday if item["executable"]]
    lines += ["", "## T+0 日内盈利可执行性（与支付宝波段分开）"]
    if executable_intraday:
        lines.extend(_intraday_line(item) for item in executable_intraday[:3])
    elif intraday:
        lines.append(
            f"- **今日无通过成本门槛的日内标的。** 当前总资金 {TOTAL_CAPITAL} 元、买卖各最低佣金 "
            f"{BROKER_MIN_COMMISSION:.0f} 元，理论最低固定成本约 "
            f"{BROKER_MIN_COMMISSION * 2 / TOTAL_CAPITAL * 100:.2f}%；不为了追求当天盈利强行交易。"
        )
        lines.extend(_intraday_line(item) for item in intraday[:3])
    else:
        lines.append("- 实时行情未返回可核验的 T+0 候选，本次不生成日内信号。")
    if mode == "morning":
        trend_rule = "站上 MA20 且短期动量强" if RISK_PROFILE == "aggressive" else "同时站上 MA20/MA60"
        lines += ["", f"**早盘焦点：** 进攻档优先观察池内分位靠前、{trend_rule} 且相对沪深300为正的标的，开盘不追高。"]
    else:
        lines += ["", f"**尾盘纪律：** 单笔加仓严格控制在 {BUY_MIN}～{BUY_MAX} 元，{TOTAL_CAPITAL} 元总资金分批操作，不因单日波动满仓。"]
    lines += [
        "",
        f"**场内成本过滤：** 按买入、卖出各最低 {BROKER_MIN_COMMISSION:.0f} 元佣金计算；往返成本超过 {MAX_EXCHANGE_ROUND_TRIP_COST_PCT:.1f}% 时只观察、不下单。",
        "**日内规则：** 仅扫描允许当日回转的代表性债券、黄金和跨境 ETF；普通股票 ETF 不作为日内标的。日内回本涨幅尚未计入买卖价差、滑点和溢价风险。",
        "",
        "**支付宝 C 类风控：** 不同基金赎回费规则不同；脚本会实时核验出现买入信号的基金，必须按报告给出的免赎回费持有期限执行，并以支付宝购买页为最终依据。",
        "",
        f"**7日纪律：** 计划最长持有 {MAX_PLANNED_HOLD_DAYS} 天；场外基金只有免赎回费期限不超过该计划时才允许出现买入信号，到期必须复核。",
        f"**风险提示：** 当前为{'进攻' if RISK_PROFILE == 'aggressive' else '均衡'}档，爆发得分按盘中估算/3日/5日/20日 45%/30%/15%/10% 计算，可能带来更大回撤；不代表收益预测。",
        f"**数据说明：** AKShare 实时 ETF 行情覆盖 {context.get('realtime_quote_count', 0)} 只；东方财富盘中净值估算覆盖 {context.get('intraday_estimation_count', 0)} 只基金；其余标的使用最新日收益回退并明确标注。",
        f"**入场规则：** 盘中估算涨幅高于 {ENTRY_MAX_DAILY_MOVE:.1f}% 视为过热并等待回踩，不再把单日大涨当作买点。",
        "**回测说明：** 胜率和平均收益来自近150条净值/行情的滚动历史样本，不含未来数据，但仍可能过拟合且不代表未来。",
        "**免责声明：** 本报告由量化脚本自动生成，仅供研究参考，不构成投资建议。",
    ]
    if context["failures"]:
        lines += ["", "## 获取失败（已跳过，不影响其他标的）"] + [f"- {failure}" for failure in context["failures"]]
    return title, "\n".join(lines)


def _dashboard_row(item: dict[str, Any]) -> str:
    kind_label = {
        "alipay_a": "支付宝 A 类",
        "alipay_c": "支付宝 C 类",
        "linked_c": "自动匹配 C 类",
        "etf": "场内 ETF",
    }.get(item["kind"], item["kind"])
    action_class = "danger" if item["stop"] else ("positive" if item["action"] == "买入观察" else "neutral")
    return (
        f"<tr data-kind='{html.escape(item['kind'])}' data-sector='{html.escape(item['sector'])}'><td>{item['rank']}</td>"
        f"<td><strong>{html.escape(item['name'])}</strong><small>{html.escape(item['code'])} · {kind_label} · {html.escape(item['sector'])}</small></td>"
        f"<td>{html.escape(item['data_date'])}</td>"
        f"<td>{item['intraday_growth']:.2f}%<small>{html.escape(item['est_source'])} · 数据 {item.get('data_quality', 0):.0f}/100 · 证据 {item.get('evidence_quality', 0):.0f}/100</small></td>"
        f"<td>{item['r1']:.2f}%</td><td>{item['max_daily_gain20']:.2f}%<small>达标 {item['target_move_days20']} 次</small></td>"
        f"<td>{item['burst_score']:.2f}</td><td>{item['pool_percentile']:.1f}%</td>"
        f"<td>{item['r20']:.2f}%<br><small>RS {item['rs20']:.2f}%</small></td>"
        f"<td>{'是' if item['above_ma20'] else '否'} / {'是' if item['above_ma60'] else '否'}</td>"
        f"<td>{html.escape(item.get('manager_names', '待核验'))}<small>{html.escape(_manager_years_text(item))}</small></td>"
        f"<td>{html.escape(_backtest_text(item))}</td>"
        f"<td>{item.get('next_day_low', 0):.2f}% ～ {item.get('next_day_high', 0):.2f}%<small>{item.get('forecast_confidence', '低')}置信度</small></td>"
        f"<td class='{'danger' if item['stop'] else ''}'>{item['drawdown']:.2f}%</td>"
        f"<td class='{action_class}'>{html.escape(_action_text(item))}</td></tr>"
    )


def _dashboard_holding_row(position: dict[str, Any]) -> str:
    item = position.get("analysis")
    status = "待确认" if position["status"] == "pending" else "已确认"
    trend = "行情暂缺"
    latest = "--"
    drawdown = "--"
    if item:
        trend = (
            f"1日 {item['r1']:.2f}% · MA20 {'上方' if item['above_ma20'] else '下方'} / "
            f"MA60 {'上方' if item['above_ma60'] else '下方'}"
        )
        latest = f"{item['latest']:.4f}"
        drawdown = f"{item['drawdown']:.2f}%"
    current_return = position.get("current_return")
    return_text = f"{current_return:.2f}%" if current_return is not None else "待补成交净值"
    return (
        "<tr>"
        f"<td><strong>{html.escape(position['name'])}</strong><small>{html.escape(position['code'])}</small></td>"
        f"<td>{html.escape(position['buy_date'])}<small>{html.escape(position.get('buy_time', ''))}</small></td>"
        f"<td>{position['amount']:.0f} 元</td><td>{status}</td><td>{latest}</td>"
        f"<td>{html.escape(trend)}</td><td>{drawdown}</td><td>{return_text}</td>"
        f"<td class='neutral'><strong>{html.escape(position['advice'])}</strong></td></tr>"
    )


def _manager_years_text(item: dict[str, Any]) -> str:
    years = item.get("manager_years")
    return f"累计 {years:.1f} 年" if years is not None else "年限待核验"


def _dashboard_metals_row(item: dict[str, Any]) -> str:
    kind_label = "支付宝场外" if _is_alipay_fund(item["kind"]) else "场内 ETF"
    action_class = "danger" if item["stop"] else ("positive" if item["action"] == "买入观察" else "neutral")
    return (
        "<tr>"
        f"<td><strong>{html.escape(item['name'])}</strong><small>{html.escape(item['code'])} · {kind_label}</small></td>"
        f"<td>{html.escape(item['data_date'])}</td>"
        f"<td>{item['intraday_growth']:.2f}%</td><td>{item['r3']:.2f}%</td><td>{item['r5']:.2f}%</td>"
        f"<td>{item['burst_score']:.2f}</td>"
        f"<td>{'上方' if item['above_ma20'] else '下方'} / {'上方' if item['above_ma60'] else '下方'}</td>"
        f"<td>{html.escape(item.get('manager_names', '待核验'))}<small>{html.escape(_manager_years_text(item))}</small></td>"
        f"<td>{html.escape(_backtest_text(item))}</td>"
        f"<td>{item.get('next_day_low', 0):.2f}% ～ {item.get('next_day_high', 0):.2f}%<small>{item.get('forecast_confidence', '低')}置信度</small></td>"
        f"<td class='{action_class}'>{html.escape(_action_text(item))}</td></tr>"
    )


def _dashboard_decision_card(item: dict[str, Any]) -> str:
    action = _action_text(item)
    manager = (
        f"{item.get('manager_names', '待核验')} · 累计 {item['manager_years']:.1f} 年"
        if item.get("manager_years") is not None
        else "基金经理年限待核验"
    )
    holding = (
        f"至少 {item['fee_free_days']} 天"
        if _is_alipay_fund(item["kind"]) and item.get("fee_free_days")
        else ("按计划 1～7 天复核" if item["kind"] == "etf" else "赎回前核对费率")
    )
    return (
        "<article class='decision-card'>"
        f"<div class='decision-top'><span>{html.escape(item['code'])}</span><b>{html.escape(item['action'])}</b></div>"
        f"<h3>{html.escape(item['name'])}</h3>"
        f"<div class='decision-metrics'><span>盘中估算<strong>{item['intraday_growth']:.2f}%</strong></span>"
        f"<span>爆发得分<strong>{item['burst_score']:.2f}</strong></span>"
        f"<span>建议金额<strong>{item.get('amount', 0):.0f} 元</strong></span>"
        f"<span>计划持有<strong>{html.escape(holding)}</strong></span>"
        f"<span>次日情景<strong>{item.get('next_day_low', 0):.2f}% ～ {item.get('next_day_high', 0):.2f}%</strong></span></div>"
        f"<p>{html.escape(action)}</p><small>{html.escape(manager)}；{html.escape(_backtest_text(item))}；数据{html.escape(item.get('data_freshness', '待核验'))}，质量 {item.get('data_quality', 0):.0f}/100，历史证据 {item.get('evidence_quality', 0):.0f}/100；情景估计为{item.get('forecast_confidence', '低')}置信度，不是收益保证。</small>"
        "</article>"
    )


def build_dashboard(context: dict[str, Any], title: str) -> str:
    now: datetime = context["now"]
    results = context["results"]
    catalog_json = json.dumps(
        context.get("fund_catalog", []), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    catalog_gzip = base64.b64encode(gzip.compress(catalog_json, compresslevel=9)).decode("ascii")
    payload = {
        "updated": now.isoformat(),
        "title": title,
        "mode": context["mode"],
        "dynamic_count": context["dynamic_count"],
        "watch_count": context["watch_count"],
        "results": results,
        "intraday_t0": context.get("intraday_t0", []),
        "realtime_quote_count": context.get("realtime_quote_count", 0),
        "intraday_estimation_count": context.get("intraday_estimation_count", 0),
        # The generated page is published publicly. Personal positions remain
        # in the private report/repository and in browser-local storage only.
        "holdings": [],
        "invested_amount": 0,
        "remaining_cash": TOTAL_CAPITAL,
        "fund_catalog_gzip": catalog_gzip,
        "failures": context["failures"],
    }
    data_json = json.dumps(payload, ensure_ascii=False, default=str).replace("</", "<\\/")
    rows = "".join(_dashboard_row(item) for item in results)
    metals = sorted(
        (item for item in results if item["code"] in METALS_CODES),
        key=lambda item: item["short_score"],
        reverse=True,
    )
    metals_rows = "".join(_dashboard_metals_row(item) for item in metals)
    decision_items = results[:3]
    decision_cards = "".join(_dashboard_decision_card(item) for item in decision_items)
    failures = "".join(f"<li>{html.escape(failure)}</li>" for failure in context["failures"])
    intraday = context.get("intraday_t0", [])
    holdings: list[dict[str, Any]] = []
    holding_rows = "".join(_dashboard_holding_row(position) for position in holdings)
    intraday_rows = "".join(
        f"<tr><td><strong>{html.escape(item['name'])}</strong><small>{item['code']} · {item['category']}</small></td>"
        f"<td>{item['price']:.3f}</td><td>{item['change']:.2f}%</td><td>{item['lot_cost']:.0f} 元</td>"
        f"<td>{item['trade_value']:.0f} 元 / {item['affordable_lots']} 手</td>"
        f"<td>{item['round_trip_cost_pct']:.2f}%</td>"
        f"<td class='{'positive' if item['executable'] else 'danger'}'>{'成本门槛通过' if item['executable'] else '不交易'}</td></tr>"
        for item in intraday
    )
    return f'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)} · ETF Workbench</title>
<script src="https://cdn.tailwindcss.com"></script><script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
:root {{ color-scheme:dark; --ink:#f7fafc; --muted:#aeb8c5; --line:rgba(255,255,255,.13); --panel:rgba(20,27,36,.76); --bg:#090d12; --green:#5ee0a0; --red:#ff6b76; --blue:#67c5ff; --amber:#ffc857; }}
* {{ box-sizing:border-box; letter-spacing:0; }} body {{ margin:0; background:var(--bg); color:var(--ink); font:15px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC",sans-serif; }}
body::before {{ content:""; position:fixed; inset:0; pointer-events:none; background:rgba(255,255,255,.015); }}
main {{ position:relative; max-width:1320px; margin:0 auto; padding:28px 20px 56px; }} header {{ display:flex; justify-content:space-between; gap:24px; align-items:flex-end; padding:22px; margin-bottom:16px; border:1px solid var(--line); background:rgba(15,22,30,.82); backdrop-filter:blur(18px); border-radius:8px; }}
.eyebrow {{ color:var(--blue); font-size:12px; font-weight:800; text-transform:uppercase; }} h1 {{ margin:5px 0 4px; font-size:clamp(26px,4vw,42px); line-height:1.15; }} h2,h3 {{ color:var(--ink); }} p {{ margin:0; color:var(--muted); }} a {{ color:var(--blue); }}
.stamp {{ color:var(--muted); text-align:right; white-space:nowrap; }} .stats {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:10px; margin-bottom:18px; }}
.stat,.table-panel,.notes,.lookup,.calculator,.chart-panel {{ background:var(--panel); border:1px solid var(--line); backdrop-filter:blur(16px); border-radius:8px; }} .stat {{ padding:15px; }} .stat b {{ display:block; color:var(--ink); font-size:25px; }} .stat span {{ color:var(--muted); font-size:12px; }}
.metals-heading {{ border-left:4px solid var(--amber); }} .metals-heading h2 {{ color:var(--amber); }}
.section-title {{ display:flex; align-items:flex-end; justify-content:space-between; gap:16px; margin:26px 0 10px; }} .section-title h2 {{ margin:0; font-size:20px; }}
.decision-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; }} .decision-card {{ min-width:0; background:rgba(25,34,45,.84); border:1px solid var(--line); border-top:3px solid var(--blue); border-radius:8px; padding:16px; }}
.decision-card h3 {{ font-size:17px; line-height:1.35; margin:10px 0 14px; }} .decision-top {{ display:flex; justify-content:space-between; gap:10px; color:var(--muted); font-size:12px; }} .decision-top b {{ color:var(--blue); }}
.decision-metrics {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; margin-bottom:12px; }} .decision-metrics span,.query-grid span {{ min-width:0; padding:8px; background:rgba(255,255,255,.045); color:var(--muted); font-size:11px; border-radius:6px; }} .decision-metrics strong,.query-grid b {{ display:block; overflow-wrap:anywhere; color:var(--ink); font-size:13px; }} .decision-card small {{ color:var(--muted); }}
.lookup,.calculator {{ padding:18px; margin:16px 0; }} .lookup h2,.calculator h2 {{ margin:0 0 4px; font-size:20px; }} .lookup-bar,.toolbar,.calculator-grid {{ display:flex; gap:10px; margin:14px 0; }} input,select,button {{ min-height:42px; border:1px solid var(--line); background:#111922; border-radius:6px; padding:9px 11px; color:var(--ink); }} input {{ flex:1; min-width:140px; }} button {{ background:#1e8ac4; border-color:#1e8ac4; color:#fff; cursor:pointer; font-weight:700; }}
.query-result {{ display:none; border-top:1px solid var(--line); padding-top:14px; }} .query-result.visible {{ display:block; }} .query-result h3 {{ margin:0 0 8px; }} .query-grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; }} .news-list {{ margin:10px 0 0; color:var(--muted); }}
.visual-grid {{ display:grid; grid-template-columns:minmax(0,1.2fr) minmax(280px,.8fr); gap:10px; margin:16px 0; }} .chart-panel {{ min-height:300px; padding:16px; }} .chart-panel h2 {{ margin:0 0 12px; font-size:18px; }} .chart-box {{ height:245px; position:relative; }} .calc-output {{ color:var(--green); font-size:18px; font-weight:800; }}
.table-panel {{ overflow:auto; }} table {{ width:100%; border-collapse:collapse; min-width:1040px; }} th,td {{ padding:11px 12px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }} th {{ position:sticky; top:0; z-index:1; background:#111820; color:var(--muted); font-size:12px; white-space:nowrap; }} td small {{ display:block; color:var(--muted); }} tr:hover td {{ background:rgba(255,255,255,.025); }} tr:last-child td {{ border-bottom:0; }}
.positive {{ color:var(--green); font-weight:700; }} .danger {{ color:var(--red); font-weight:700; }} .neutral {{ color:var(--amber); }} .notes {{ padding:16px; margin-top:16px; }} .notes h2 {{ font-size:16px; margin:0 0 8px; }} .notes ul {{ margin:8px 0 0; padding-left:20px; color:var(--muted); }}
@media (max-width:800px) {{ main {{ padding:12px 9px 36px; }} header {{ display:block; padding:16px; }} .stamp {{ text-align:left; margin-top:10px; }} .stats {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .decision-grid,.visual-grid {{ grid-template-columns:1fr; }} .query-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .toolbar,.lookup-bar,.calculator-grid {{ flex-direction:column; }} .chart-panel {{ min-height:270px; }} }}
</style></head><body><main>
<header><div><div class="eyebrow">HIGH BURST FUND WORKBENCH · {TOTAL_CAPITAL} CNY</div><h1>{html.escape(title)}</h1><p>AKShare 实时 ETF 行情、盘中净值估算、3/5/20日动量、滚动回测与持仓风控。</p></div><div class="stamp">更新时间<br><strong>{now:%Y-%m-%d %H:%M} 北京时间</strong><br>ETF 实时 {context.get('realtime_quote_count', 0)} 只 · 基金估值 {context.get('intraday_estimation_count', 0)} 只</div></header>
<section class="stats"><div class="stat"><span>当前持仓</span><b>{len(holdings)}</b></div><div class="stat"><span>已投入</span><b>{context.get('invested_amount', 0):.0f}</b></div><div class="stat"><span>剩余资金</span><b>{context.get('remaining_cash', TOTAL_CAPITAL):.0f}</b></div><div class="stat"><span>有色/稀土</span><b>{len(metals)}</b></div><div class="stat"><span>有效标的</span><b>{len(results)}</b></div></section>
<div class="section-title"><div><h2>今日盘中动量冲刺 Top 3</h2><p>按 45% 盘中估算 + 30% 近3日 + 15% 近5日 + 10% 近20日排序；动作仍接受回测与防追高约束。</p></div></div>
<section class="decision-grid">{decision_cards or '<article class="decision-card"><h3>今天没有通过全部门槛的候选</h3><p>保留现金，不为了目标涨幅强行交易。</p></article>'}</section>
<section class="lookup"><h2>查一只基金</h2><p>输入本次已扫描的基金代码或名称，立即查看金额、计划持有期、经理经验、次日情景和风险依据。</p><div class="lookup-bar"><input id="fund-query" type="search" inputmode="search" placeholder="例如：004433 或 南方有色"><button id="query-button" type="button">查询</button></div><div id="query-result" class="query-result" aria-live="polite"></div></section>
<section class="visual-grid"><div class="chart-panel"><h2>板块爆发热度</h2><div class="chart-box"><canvas id="sector-chart"></canvas></div></div><div class="calculator"><h2>2000 元分批加仓计算器</h2><p>按剩余资金和计划批次数计算，不自动执行交易。</p><div class="calculator-grid"><input id="capital-input" type="number" min="100" step="100" value="{max(0, context.get('remaining_cash', TOTAL_CAPITAL)):.0f}" aria-label="可用资金"><input id="batch-input" type="number" min="1" max="10" value="5" aria-label="计划批次数"><button id="calc-button" type="button">计算</button></div><div id="calc-output" class="calc-output"></div></div></section>
<section class="notes"><h2>我的持仓与最新操作建议</h2><p>操作建议依据最新可用净值、均线、动量、回撤和赎回费期限生成；待确认订单不重复加仓。</p></section>
<section class="table-panel"><table><thead><tr><th>持有基金</th><th>买入时间</th><th>金额</th><th>状态</th><th>最新净值</th><th>趋势</th><th>20日回撤</th><th>持仓收益</th><th>当前操作</th></tr></thead><tbody>{holding_rows or '<tr><td colspan="9">尚未录入持仓。</td></tr>'}</tbody></table></section>
<section class="notes metals-heading" id="metals"><h2>有色金属与稀土专区</h2><p>已固定跟踪支付宝场外联接基金和场内 ETF。当前日涨幅超过 {ENTRY_MAX_DAILY_MOVE:.1f}% 会标记为“等待回踩”，避免把冲高后的价格当成买点。</p></section>
<section class="table-panel"><table><thead><tr><th>有色/稀土标的</th><th>数据日期</th><th>盘中估算</th><th>3日</th><th>5日</th><th>爆发得分</th><th>MA20 / MA60</th><th>基金经理</th><th>同规则回测</th><th>次日情景区间</th><th>当前动作</th></tr></thead><tbody>{metals_rows or '<tr><td colspan="11">本次有色金属数据获取失败，已保留标的并等待下次重试。</td></tr>'}</tbody></table></section>
<section class="notes"><h2>入场分层候选</h2><p>首选试仓只在趋势、相对强度和历史滚动回测同时支持时出现；单日冲高标记“等待回踩”，历史样本不支持则不买入。</p></section>
<div class="toolbar"><input id="search" type="search" placeholder="搜索名称或代码"><select id="kind"><option value="all">全部标的</option><option value="alipay_a">支付宝 A 类</option><option value="alipay_c">支付宝 C 类</option><option value="linked_c">自动匹配 C 类</option><option value="etf">场内 ETF</option></select><select id="sector"><option value="all">全部板块</option><option>半导体</option><option>AI / 科技</option><option>游戏传媒</option><option>小微盘</option><option>港股 / QDII</option><option>有色金属</option><option>科创50</option><option>医疗</option><option>消费电子</option><option>证券</option><option>黄金</option><option>其他</option></select></div>
<section class="table-panel"><table><thead><tr><th>排名</th><th>标的</th><th>数据日期</th><th>盘中估算</th><th>最新1日</th><th>20日最大单日涨幅</th><th>爆发得分</th><th>池内分位</th><th>20日收益 / RS</th><th>MA20 / MA60</th><th>基金经理</th><th>同规则回测</th><th>次日情景区间</th><th>20日回撤</th><th>动作与依据</th></tr></thead><tbody id="rows">{rows}</tbody></table></section>
<section class="notes"><h2>T+0 日内盈利可执行性</h2><p>仅列出规则允许当日回转的代表性债券、黄金和跨境 ETF。成本门槛通过不等于盈利预测；回本涨幅还未计买卖价差、滑点和溢价风险。</p></section>
<section class="table-panel"><table><thead><tr><th>T+0 标的</th><th>现价</th><th>当日涨跌</th><th>1 手金额</th><th>最多成交</th><th>佣金回本涨幅</th><th>结论</th></tr></thead><tbody>{intraday_rows or '<tr><td colspan="7">本次未获取到可核验的 T+0 行情。</td></tr>'}</tbody></table></section>
<section class="notes"><h2>风控与数据状态</h2><p>总资金 {TOTAL_CAPITAL} 元，单笔加仓控制在 {BUY_MIN}～{BUY_MAX} 元。场内 ETF 按买卖各最低 {BROKER_MIN_COMMISSION:.0f} 元佣金过滤，往返成本超过 {MAX_EXCHANGE_ROUND_TRIP_COST_PCT:.1f}% 时只观察；支付宝 C 类按实时赎回费率给出最低计划持有天数，最终以购买页为准。</p><p>注意：AKShare 是公开数据接口聚合层，场外/ QDII 净值不是盘中实时成交价；开放式基金日净值通常在交易日 16:00～23:00 更新。早盘看到的“关联板块”只能作为方向参考，不能当作你的实际当日收益。</p>{f'<ul>{failures}</ul>' if failures else '<p>本次扫描未记录接口失败。</p>'}</section>
<script type="application/json" id="dashboard-data">{data_json}</script><script>
const data=JSON.parse(document.getElementById('dashboard-data').textContent); const search=document.getElementById('search'); const kind=document.getElementById('kind'); const sector=document.getElementById('sector'); const fundQuery=document.getElementById('fund-query'); const queryButton=document.getElementById('query-button'); const queryResult=document.getElementById('query-result');
function filterRows(){{const q=search.value.trim().toLowerCase(), k=kind.value, s=sector.value; document.querySelectorAll('#rows tr').forEach(row=>{{const text=row.textContent.toLowerCase(); row.hidden=(q&&!text.includes(q))||(k!=='all'&&row.dataset.kind!==k)||(s!=='all'&&row.dataset.sector!==s);}});}}
search.addEventListener('input',filterRows); kind.addEventListener('change',filterRows); sector.addEventListener('change',filterRows);
function addText(parent,tag,text,className){{const node=document.createElement(tag); node.textContent=text; if(className)node.className=className; parent.appendChild(node); return node;}}
function runQuery(){{const q=fundQuery.value.trim().toLowerCase(); queryResult.replaceChildren(); queryResult.classList.add('visible'); if(!q){{addText(queryResult,'p','请输入基金代码或名称。'); return;}} const matches=data.results.filter(item=>item.code.toLowerCase()===q||item.name.toLowerCase().includes(q)); if(!matches.length){{addText(queryResult,'h3','本次扫描池中没有找到'); addText(queryResult,'p','请把基金代码发给我，我可以先核验代码、费率和数据，再决定是否加入固定扫描池。'); return;}} const item=matches[0]; addText(queryResult,'h3',item.code+' · '+item.name); const grid=addText(queryResult,'div','', 'query-grid'); const holding=item.fee_free_days?('至少 '+item.fee_free_days+' 天'):(item.kind==='etf'?'1～7 天复核':'赎回前核对费率'); const manager=item.manager_years!=null?(item.manager_names+' · '+item.manager_years.toFixed(1)+' 年'):'待核验'; const backtest=item.backtest_signals?('样本 '+item.backtest_signals+' 次；3/5日胜率 '+item.backtest_win3.toFixed(1)+'% / '+item.backtest_win5.toFixed(1)+'%'):'样本不足'; [['当前结论',item.action],['建议金额',(item.amount||0)+' 元'],['计划持有',holding],['板块',item.sector],['盘中估算',item.intraday_growth.toFixed(2)+'% · '+item.est_source],['爆发得分',item.burst_score.toFixed(2)],['1/3/5日',item.r1.toFixed(2)+'% / '+item.r3.toFixed(2)+'% / '+item.r5.toFixed(2)+'%'],['基金经理',manager],['同规则回测',backtest],['次日情景',item.next_day_low.toFixed(2)+'% ～ '+item.next_day_high.toFixed(2)+'%'],['置信度',item.forecast_confidence],['数据日期',item.data_date]].forEach(pair=>{{const box=addText(grid,'span',pair[0]); addText(box,'b',pair[1]);}}); addText(queryResult,'p','提示：盘中净值估算不是正式净值，情景区间也不是收益承诺。'); const newsItems=item.news_items||[]; if(newsItems.length){{const list=addText(queryResult,'ul','', 'news-list'); newsItems.forEach(news=>{{const li=document.createElement('li'); const link=document.createElement('a'); link.textContent=news.title; if(/^https?:\/\//.test(news.url)){{link.href=news.url; link.target='_blank'; link.rel='noopener noreferrer';}} li.appendChild(link); addText(li,'small',(news.source||'来源待核验')+(news.published?' · '+news.published:'')); list.appendChild(li);}});}}}}
queryButton.addEventListener('click',runQuery); fundQuery.addEventListener('keydown',event=>{{if(event.key==='Enter')runQuery();}});
const capitalInput=document.getElementById('capital-input'), batchInput=document.getElementById('batch-input'), calcOutput=document.getElementById('calc-output'); function calculateBatches(){{const capital=Math.max(0,Number(capitalInput.value)||0), batches=Math.max(1,Number(batchInput.value)||1); const raw=capital/batches; const each=Math.max({BUY_MIN},Math.min({BUY_MAX},Math.round(raw/50)*50)); const possible=Math.floor(capital/each); calcOutput.textContent=capital<{BUY_MIN}?'可用资金低于单笔下限。':'建议每笔 '+each+' 元，最多 '+possible+' 笔；每次只执行通过风控的信号。';}} document.getElementById('calc-button').addEventListener('click',calculateBatches); calculateBatches();
const sectorMap=new Map(); data.results.forEach(item=>{{const bucket=sectorMap.get(item.sector)||[]; bucket.push(item.burst_score); sectorMap.set(item.sector,bucket);}}); const sectorRows=[...sectorMap.entries()].map(([label,values])=>({{label,value:values.reduce((a,b)=>a+b,0)/values.length}})).sort((a,b)=>b.value-a.value).slice(0,8); const chartCanvas=document.getElementById('sector-chart'); if(window.Chart&&chartCanvas){{new Chart(chartCanvas,{{type:'bar',data:{{labels:sectorRows.map(x=>x.label),datasets:[{{label:'平均爆发得分',data:sectorRows.map(x=>x.value),backgroundColor:sectorRows.map(x=>x.value>=0?'rgba(94,224,160,.72)':'rgba(255,107,118,.72)'),borderWidth:0}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}},scales:{{x:{{grid:{{color:'rgba(255,255,255,.08)'}},ticks:{{color:'#aeb8c5'}}}},y:{{grid:{{display:false}},ticks:{{color:'#f7fafc'}}}}}}}}}});}}
</script></main></body></html>'''


def write_dashboard(context: dict[str, Any], title: str, path: str | Path = "index.html") -> None:
    generated_data_page = build_dashboard(context, title)
    mobile_template_path = Path("artifact.html")
    if mobile_template_path.exists():
        match = re.search(
            r'<script type="application/json" id="dashboard-data">([\s\S]*?)</script>',
            generated_data_page,
        )
        if match:
            mobile_page = mobile_template_path.read_text(encoding="utf-8")
            placeholder = '<script type="application/json" id="dashboard-data"></script>'
            embedded = f'<script type="application/json" id="dashboard-data">{match.group(1)}</script>'
            mobile_page = mobile_page.replace(placeholder, embedded, 1)
            Path(path).write_text(mobile_page, encoding="utf-8")
            return
    Path(path).write_text(generated_data_page, encoding="utf-8")


def push_wechat(title: str, markdown: str) -> None:
    send_key = os.getenv("SERVERCHAN_KEY", "").strip()
    if not send_key:
        print("未设置 SERVERCHAN_KEY，仅输出终端报告。")
        return
    try:
        response = requests.post(
            f"https://sctapi.ftqq.com/{send_key}.send",
            data={"title": title, "desp": markdown},
            timeout=15,
        )
        response.raise_for_status()
        result = response.json()
        if result.get("code") not in (0, "0"):
            print(f"Server酱返回失败：{result.get('message') or result.get('msg')}")
        else:
            print("Server酱推送完成。")
    except (requests.RequestException, ValueError) as exc:
        print(f"Server酱推送失败（不影响本次分析）：{_short_error(exc)}")


send_serverchan = push_wechat


def run_strategy(now: datetime | None = None) -> tuple[str, str]:
    context = analyse_market(now)
    title, report = build_report(context)
    return title, report


def main() -> None:
    context = analyse_market()
    title, report = build_report(context)
    print(report)
    try:
        write_dashboard(context, title, os.getenv("DASHBOARD_PATH", "index.html"))
        print("HTML 仪表盘已更新。")
    except OSError as exc:
        print(f"HTML 仪表盘写入失败（不影响推送）：{_short_error(exc)}")
    push_wechat(title, report)


if __name__ == "__main__":
    main()
