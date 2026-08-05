"""全市场 ETF 与支付宝 C 类基金双时段动量筛选、归因和仪表盘生成器。"""

from __future__ import annotations

import html
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
HISTORY_ROWS = 150
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
MOMENTUM_WEIGHTS = (0.65, 0.25, 0.10) if RISK_PROFILE == "aggressive" else (0.50, 0.30, 0.20)
SHORT_MOMENTUM_WEIGHTS = (0.45, 0.30, 0.25)
MAX_PLANNED_HOLD_DAYS = _positive_int_env("MAX_PLANNED_HOLD_DAYS", 7)
SIGNAL_PERCENTILE = 80 if RISK_PROFILE == "aggressive" else 70
SIGNAL_MIN_SCORE = 1.5 if RISK_PROFILE == "aggressive" else 0.0
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

# 场外基金代码已按基金管理人官网/产品资料核验；代码与名称不得分离修改。
CORE_WATCHLIST: list[dict[str, Any]] = [
    {"code": "008888", "name": "华夏国证半导体芯片ETF联接C", "kind": "alipay_c", "data_codes": ("008888",)},
    {"code": "011613", "name": "华夏科创50ETF联接C", "kind": "alipay_c", "data_codes": ("011613",)},
    {"code": "024663", "name": "富国创业板人工智能ETF发起式联接C", "kind": "alipay_c", "data_codes": ("024663",)},
    {"code": "007339", "name": "易方达沪深300ETF联接C", "kind": "alipay_c", "data_codes": ("007339",)},
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
                start_date=(datetime.now() - timedelta(days=260)).strftime("%Y%m%d"),
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust="qfq",
            ),
            lambda: ak.fund_etf_hist_sina(symbol=_sina_symbol(code)),
            lambda: ak.stock_zh_index_daily_em(symbol="sh000300"),
        ]
    return [
        lambda: ak.fund_etf_hist_em(
            symbol=code,
            period="daily",
            start_date=(datetime.now() - timedelta(days=260)).strftime("%Y%m%d"),
            end_date=datetime.now().strftime("%Y%m%d"),
            adjust="qfq",
        ),
        # 新浪接口作为不同数据源的备用；其价格序列没有 qfq 参数，优先级低于东财。
        lambda: ak.fund_etf_hist_sina(symbol=_sina_symbol(code)),
        lambda: ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=(datetime.now() - timedelta(days=260)).strftime("%Y%m%d"),
            end_date=datetime.now().strftime("%Y%m%d"),
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
    liquid = liquid.sort_values(["_change", "_amount"], ascending=False).head(MAX_DYNAMIC_CANDIDATES)
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
            return frame[[code_col, name_col]].rename(columns={code_col: "code", name_col: "name"})
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
        failures.append(f"核心基金代码名称在线核验失败，场外基金已全部跳过：{_short_error(exc)}")
        items = [item for item in items if item["kind"] != "alipay_c"]
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


def analyse_item(
    item: dict[str, Any], benchmark_returns: dict[int, float], as_of: datetime
) -> dict[str, Any]:
    history, data_code, source = fetch_item_history(item)
    if len(history) < 61:
        raise ValueError(f"有效数据 {len(history)} 条，少于 61 条")
    close = history["close"]
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
    data_date = history["date"].iloc[-1].date()
    data_age_days = (as_of.astimezone(BEIJING_TZ).date() - data_date).days
    score = (
        returns[5] * MOMENTUM_WEIGHTS[0]
        + returns[20] * MOMENTUM_WEIGHTS[1]
        + returns[60] * MOMENTUM_WEIGHTS[2]
    )
    short_score = (
        returns[1] * SHORT_MOMENTUM_WEIGHTS[0]
        + returns[3] * SHORT_MOMENTUM_WEIGHTS[1]
        + returns[5] * SHORT_MOMENTUM_WEIGHTS[2]
    )
    rs20 = returns[20] - benchmark_returns.get(20, 0.0)
    rs60 = returns[60] - benchmark_returns.get(60, 0.0)
    return {
        **item,
        "data_code": data_code,
        "source": source,
        "data_date": data_date.isoformat(),
        "data_age_days": data_age_days,
        "stale": data_age_days > MAX_DATA_AGE_DAYS,
        "latest": latest,
        "r1": returns[1],
        "r3": returns[3],
        "r5": returns[5],
        "r20": returns[20],
        "r60": returns[60],
        "score": score,
        "short_score": short_score,
        "selection_score": short_score if RISK_PROFILE == "aggressive" else score,
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
    }


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
        elif (
            item["selection_score"] >= SIGNAL_MIN_SCORE
            and item["r3"] > 0
            and item["r5"] > 0
            and item["above_ma20"]
            and (item["above_ma60"] or not REQUIRE_MA60)
            and item["pool_percentile"] >= SIGNAL_PERCENTILE
            and item["rs20"] > 0
            and item["rs_score"] > 0
            and (RISK_PROFILE != "aggressive" or item["high_move_capable"])
            and (RISK_PROFILE != "aggressive" or item["r1"] >= TARGET_DAILY_MOVE)
        ):
            proposed_amount = (
                HIGH_CONVICTION_BUY
                if item["pool_percentile"] >= 90 and item["selection_score"] >= 4
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
        elif item["above_ma20"]:
            item["action"] = "持有/观察"
            item["amount"] = 0
        else:
            item["action"] = "暂缓/观望"
            item["amount"] = 0


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
                benchmark_returns = {days: _return(benchmark_history["close"], days) for days in (5, 20, 60)}
    except Exception as exc:
        failures.append(f"沪深300基准：{_short_error(exc)}")

    results: list[dict[str, Any]] = []
    for item in watchlist:
        try:
            results.append(analyse_item(item, benchmark_returns, now))
        except Exception as exc:
            failures.append(f"{item['code']} {item['name']}：{_short_error(exc)}")
    _rank_results(results)
    holding_codes = {position["code"] for position in holdings}
    invested_amount = sum(position["amount"] for position in holdings)
    apply_cash_limit(results, invested_amount, holding_codes)
    enrich_redemption_fees(results, failures, holding_codes)
    results.sort(key=lambda row: row["selection_score"], reverse=True)
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
        "intraday_t0": intraday_t0,
        "holdings": enriched_holdings,
        "invested_amount": invested_amount,
        "remaining_cash": max(0.0, TOTAL_CAPITAL - invested_amount),
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
    if item["action"] == "现金不足":
        return "当前持仓已占用绝大部分资金，剩余现金低于单笔下限，不再新增买入"
    if item["action"] == "买入观察":
        if _is_alipay_fund(item["kind"]):
            holding = item.get("fee_free_days")
            holding_text = f"计划至少持有 {holding} 天" if holding else "赎回前再次核对费率"
            return (
                f"动量与趋势共振，建议在支付宝分批买入 {item['amount']} 元；{holding_text}；"
                f"当前费率规则：{item.get('redemption_fee_summary', '未获取')}"
            )
        return f"动量与趋势共振，建议分批买入 {item['amount']} 元"
    if item["action"] == "持有/观察":
        return "站上 MA20，继续观察，不新增资金"
    return "低于 MA20 或相对强度不足，暂缓新增资金"


def _line(item: dict[str, Any]) -> str:
    trend = f"MA20 {'上方' if item['above_ma20'] else '下方'} / MA60 {'上方' if item['above_ma60'] else '下方'}"
    return (
        f"- **{item['code']} {item['name']}**（{item['kind']}）：1/3/5/20/60日 "
        f"{item['r1']:.2f}% / {item['r3']:.2f}% / {item['r5']:.2f}% / {item['r20']:.2f}% / {item['r60']:.2f}%，"
        f"近20日最大单日上涨 **{item['max_daily_gain20']:.2f}%**（≥{TARGET_DAILY_MOVE:.1f}% 共 {item['target_move_days20']} 次），"
        f"日波动率 {item['daily_volatility20']:.2f}%，7日短动量 **{item['short_score']:.2f}**，"
        f"池内分位 **{item['pool_percentile']:.1f}%**（约前 {item['pool_top_percent']}%）；"
        f"数据日期 {item['data_date']}，{trend}，相对沪深300超额20/60日 {item['rs20']:.2f}% / {item['rs60']:.2f}%，"
        f"回撤 {item['drawdown']:.2f}%，**{_action_text(item)}**。"
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
    high_move_results = sorted(
        (
            item
            for item in results
            if item["r1"] >= TARGET_DAILY_MOVE and item["high_move_capable"] and not item["stale"]
        ),
        key=lambda item: (item["r1"], item["max_daily_gain20"], item["selection_score"]),
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
        f"## {TARGET_DAILY_MOVE:.1f}%+ 高波动进攻候选",
    ]
    if high_move_results:
        lines.extend(_line(item) for item in high_move_results[:6])
    else:
        lines.append(f"- 今日无最新1日涨幅达到 {TARGET_DAILY_MOVE:.1f}% 且通过趋势检查的候选，不强行交易。")
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
        f"**风险提示：** 当前为{'进攻' if RISK_PROFILE == 'aggressive' else '均衡'}档，1/3/5日短动量权重为 45%/30%/25%，可能带来更大回撤；不代表收益预测。",
        f"**3%目标说明：** 仅表示近20个交易日曾出现单日上涨 ≥{TARGET_DAILY_MOVE:.1f}%，不代表下一交易日或每天都能上涨 {TARGET_DAILY_MOVE:.1f}%。",
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
        f"<tr data-kind='{html.escape(item['kind'])}'><td>{item['rank']}</td>"
        f"<td><strong>{html.escape(item['name'])}</strong><small>{html.escape(item['code'])} · {kind_label}</small></td>"
        f"<td>{html.escape(item['data_date'])}</td>"
        f"<td>{item['r1']:.2f}%</td><td>{item['max_daily_gain20']:.2f}%<small>达标 {item['target_move_days20']} 次</small></td>"
        f"<td>{item['short_score']:.2f}</td><td>{item['pool_percentile']:.1f}%</td>"
        f"<td>{item['r20']:.2f}%<br><small>RS {item['rs20']:.2f}%</small></td>"
        f"<td>{'是' if item['above_ma20'] else '否'} / {'是' if item['above_ma60'] else '否'}</td>"
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


def _dashboard_metals_row(item: dict[str, Any]) -> str:
    kind_label = "支付宝场外" if _is_alipay_fund(item["kind"]) else "场内 ETF"
    action_class = "danger" if item["stop"] else ("positive" if item["action"] == "买入观察" else "neutral")
    return (
        "<tr>"
        f"<td><strong>{html.escape(item['name'])}</strong><small>{html.escape(item['code'])} · {kind_label}</small></td>"
        f"<td>{html.escape(item['data_date'])}</td>"
        f"<td>{item['r1']:.2f}%</td><td>{item['r3']:.2f}%</td><td>{item['r5']:.2f}%</td>"
        f"<td>{item['short_score']:.2f}</td>"
        f"<td>{'上方' if item['above_ma20'] else '下方'} / {'上方' if item['above_ma60'] else '下方'}</td>"
        f"<td class='{action_class}'>{html.escape(_action_text(item))}</td></tr>"
    )


def build_dashboard(context: dict[str, Any], title: str) -> str:
    now: datetime = context["now"]
    results = context["results"]
    payload = {
        "updated": now.isoformat(),
        "title": title,
        "mode": context["mode"],
        "dynamic_count": context["dynamic_count"],
        "watch_count": context["watch_count"],
        "results": results,
        "intraday_t0": context.get("intraday_t0", []),
        "holdings": context.get("holdings", []),
        "invested_amount": context.get("invested_amount", 0),
        "remaining_cash": context.get("remaining_cash", TOTAL_CAPITAL),
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
    failures = "".join(f"<li>{html.escape(failure)}</li>" for failure in context["failures"])
    intraday = context.get("intraday_t0", [])
    holdings = context.get("holdings", [])
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
<style>
:root {{ color-scheme: light; --ink:#19212b; --muted:#66717f; --line:#dfe5eb; --panel:#fff; --bg:#f4f6f8; --green:#087f5b; --red:#c92a2a; --blue:#1769aa; }}
* {{ box-sizing:border-box; }} body {{ margin:0; background:var(--bg); color:var(--ink); font:15px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC",sans-serif; }}
main {{ max-width:1240px; margin:0 auto; padding:32px 20px 56px; }} header {{ display:flex; justify-content:space-between; gap:24px; align-items:flex-end; margin-bottom:24px; }}
.eyebrow {{ color:var(--blue); font-size:12px; font-weight:700; letter-spacing:.08em; text-transform:uppercase; }} h1 {{ margin:5px 0 4px; font-size:clamp(26px,4vw,42px); line-height:1.15; }} p {{ margin:0; color:var(--muted); }}
.stamp {{ color:var(--muted); text-align:right; white-space:nowrap; }} .stats {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:12px; margin-bottom:22px; }}
.stat, .table-panel, .notes {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; }} .stat {{ padding:16px; }} .stat b {{ display:block; font-size:25px; }} .stat span {{ color:var(--muted); font-size:13px; }}
.metals-heading {{ border-left:4px solid #b7791f; background:#fffaf0; }} .metals-heading h2 {{ color:#8a5a00; }}
.toolbar {{ display:flex; gap:10px; margin:14px 0; }} input, select {{ border:1px solid var(--line); background:#fff; border-radius:6px; padding:10px 12px; color:var(--ink); }} input {{ flex:1; min-width:160px; }}
.table-panel {{ overflow:auto; }} table {{ width:100%; border-collapse:collapse; min-width:930px; }} th,td {{ padding:12px 13px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }} th {{ background:#f8fafb; color:var(--muted); font-size:12px; white-space:nowrap; }} td small {{ display:block; color:var(--muted); }} tr:last-child td {{ border-bottom:0; }}
.positive {{ color:var(--green); font-weight:700; }} .danger {{ color:var(--red); font-weight:700; }} .neutral {{ color:#8a5a00; }} .notes {{ padding:16px; margin-top:16px; }} .notes h2 {{ font-size:16px; margin:0 0 8px; }} .notes ul {{ margin:8px 0 0; padding-left:20px; color:var(--muted); }}
@media (max-width:700px) {{ main {{ padding:22px 12px 40px; }} header {{ display:block; }} .stamp {{ text-align:left; margin-top:10px; }} .stats {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .toolbar {{ flex-direction:column; }} }}
</style></head><body><main>
<header><div><div class="eyebrow">ETF WORKBENCH · {TOTAL_CAPITAL} CNY MODE</div><h1>{html.escape(title)}</h1><p>全市场动量、相对沪深300强弱与风险回撤的可视化筛选。</p></div><div class="stamp">更新时间<br><strong>{now:%Y-%m-%d %H:%M} 北京时间</strong></div></header>
<section class="stats"><div class="stat"><span>当前持仓</span><b>{len(holdings)}</b></div><div class="stat"><span>已投入</span><b>{context.get('invested_amount', 0):.0f}</b></div><div class="stat"><span>剩余资金</span><b>{context.get('remaining_cash', TOTAL_CAPITAL):.0f}</b></div><div class="stat"><span>有色/稀土</span><b>{len(metals)}</b></div><div class="stat"><span>有效标的</span><b>{len(results)}</b></div></section>
<section class="notes"><h2>我的持仓与最新操作建议</h2><p>操作建议依据最新可用净值、均线、动量、回撤和赎回费期限生成；待确认订单不重复加仓。</p></section>
<section class="table-panel"><table><thead><tr><th>持有基金</th><th>买入时间</th><th>金额</th><th>状态</th><th>最新净值</th><th>趋势</th><th>20日回撤</th><th>持仓收益</th><th>当前操作</th></tr></thead><tbody>{holding_rows or '<tr><td colspan="9">尚未录入持仓。</td></tr>'}</tbody></table></section>
<section class="notes metals-heading" id="metals"><h2>有色金属与稀土专区</h2><p>已固定跟踪支付宝场外联接基金和场内 ETF。按 1/3/5 日短动量排序，动作仍受均线、回撤、赎回费和场内佣金约束。</p></section>
<section class="table-panel"><table><thead><tr><th>有色/稀土标的</th><th>数据日期</th><th>1日</th><th>3日</th><th>5日</th><th>7日短动量</th><th>MA20 / MA60</th><th>当前动作</th></tr></thead><tbody>{metals_rows or '<tr><td colspan="8">本次有色金属数据获取失败，已保留标的并等待下次重试。</td></tr>'}</tbody></table></section>
<section class="notes"><h2>{TARGET_DAILY_MOVE:.1f}%+ 七日高弹性候选</h2><p>按 1/3/5 日短动量筛选，计划最长持有 {MAX_PLANNED_HOLD_DAYS} 天；只有最新 1 日涨幅 ≥{TARGET_DAILY_MOVE:.1f}% 且趋势通过的标的才可能进入买入观察。</p></section>
<div class="toolbar"><input id="search" type="search" placeholder="搜索名称或代码"><select id="kind"><option value="all">全部标的</option><option value="alipay_a">支付宝 A 类</option><option value="alipay_c">支付宝 C 类</option><option value="linked_c">自动匹配 C 类</option><option value="etf">场内 ETF</option></select></div>
<section class="table-panel"><table><thead><tr><th>排名</th><th>标的</th><th>数据日期</th><th>最新1日</th><th>20日最大单日涨幅</th><th>7日短动量</th><th>池内分位</th><th>20日收益 / RS</th><th>MA20 / MA60</th><th>20日回撤</th><th>动作与依据</th></tr></thead><tbody id="rows">{rows}</tbody></table></section>
<section class="notes"><h2>T+0 日内盈利可执行性</h2><p>仅列出规则允许当日回转的代表性债券、黄金和跨境 ETF。成本门槛通过不等于盈利预测；回本涨幅还未计买卖价差、滑点和溢价风险。</p></section>
<section class="table-panel"><table><thead><tr><th>T+0 标的</th><th>现价</th><th>当日涨跌</th><th>1 手金额</th><th>最多成交</th><th>佣金回本涨幅</th><th>结论</th></tr></thead><tbody>{intraday_rows or '<tr><td colspan="7">本次未获取到可核验的 T+0 行情。</td></tr>'}</tbody></table></section>
<section class="notes"><h2>风控与数据状态</h2><p>总资金 {TOTAL_CAPITAL} 元，单笔加仓控制在 {BUY_MIN}～{BUY_MAX} 元。场内 ETF 按买卖各最低 {BROKER_MIN_COMMISSION:.0f} 元佣金过滤，往返成本超过 {MAX_EXCHANGE_ROUND_TRIP_COST_PCT:.1f}% 时只观察；支付宝 C 类按实时赎回费率给出最低计划持有天数，最终以购买页为准。</p>{f'<ul>{failures}</ul>' if failures else '<p>本次扫描未记录接口失败。</p>'}</section>
<script type="application/json" id="dashboard-data">{data_json}</script><script>
const data=JSON.parse(document.getElementById('dashboard-data').textContent); const search=document.getElementById('search'); const kind=document.getElementById('kind');
function filterRows(){{const q=search.value.trim().toLowerCase(), k=kind.value; document.querySelectorAll('#rows tr').forEach(row=>{{const text=row.textContent.toLowerCase(); row.hidden=(q&&!text.includes(q))||(k!=='all'&&row.dataset.kind!==k);}});}}
search.addEventListener('input',filterRows); kind.addEventListener('change',filterRows);
</script></main></body></html>'''


def write_dashboard(context: dict[str, Any], title: str, path: str | Path = "index.html") -> None:
    Path(path).write_text(build_dashboard(context, title), encoding="utf-8")


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
