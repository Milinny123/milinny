"""全市场 ETF 与支付宝 C 类基金双时段动量筛选、归因和仪表盘生成器。"""

from __future__ import annotations

import html
import json
import math
import os
import re
from datetime import datetime, time, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable

import akshare as ak
import pandas as pd
import requests


BEIJING_TZ = timezone(timedelta(hours=8))
HISTORY_ROWS = 150
MAX_DYNAMIC_CANDIDATES = 12
MAX_LINKED_FUNDS = 5
MAX_REPORT_ITEMS = 12
MAX_DRAWDOWN_LIMIT = -8.0
BUY_MIN = 100
BUY_MAX = 250
BENCHMARK = {"code": "510300", "name": "沪深300ETF", "kind": "benchmark", "data_codes": ("510300",)}

# 000852 按用户给定的请求代码保留；该名称在部分数据源实际对应 007339，作为数据回退。
CORE_WATCHLIST: list[dict[str, Any]] = [
    {"code": "012616", "name": "华夏半导体芯片ETF联接C", "kind": "alipay_c", "data_codes": ("012616",)},
    {"code": "011613", "name": "易方达中证科创50联接C", "kind": "alipay_c", "data_codes": ("011613",)},
    {"code": "015874", "name": "富国中证人工智能ETF联接C", "kind": "alipay_c", "data_codes": ("015874",)},
    {"code": "000852", "name": "易方达沪深300联接C", "kind": "alipay_c", "data_codes": ("000852", "007339")},
    {"code": "012414", "name": "招商中证消费电子主题ETF联接C", "kind": "alipay_c", "data_codes": ("012414",)},
    {"code": "013280", "name": "易方达中证医疗ETF联接C", "kind": "alipay_c", "data_codes": ("013280",)},
    {"code": "588000", "name": "科创50ETF", "kind": "etf", "data_codes": ("588000",)},
    {"code": "512480", "name": "半导体ETF", "kind": "etf", "data_codes": ("512480",)},
]


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
    if kind in {"alipay_c", "linked_c"}:
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
            return _normalise_history(attempt()), code, "akshare"
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


def scan_market_etfs() -> list[dict[str, Any]]:
    """Scan liquid full-market ETFs, then leave 20/60-day ranking to historical analysis."""
    frame = ak.fund_etf_spot_em()
    if frame is None or frame.empty:
        return []
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


def _fund_catalog() -> pd.DataFrame:
    attempts = [
        lambda: ak.fund_name_em(),
        lambda: ak.fund_open_fund_daily_em(),
    ]
    errors: list[str] = []
    for attempt in attempts:
        try:
            frame = attempt()
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


def build_watchlist(failures: list[str]) -> tuple[list[dict[str, Any]], int]:
    items = [dict(item) for item in CORE_WATCHLIST]
    existing = {item["code"] for item in items}
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


def analyse_item(item: dict[str, Any], benchmark_returns: dict[int, float]) -> dict[str, Any]:
    history, data_code, source = fetch_item_history(item)
    if len(history) < 61:
        raise ValueError(f"有效数据 {len(history)} 条，少于 61 条")
    close = history["close"]
    returns = {days: _return(close, days) for days in (5, 20, 60)}
    latest = float(close.iloc[-1])
    ma20 = float(close.tail(20).mean())
    ma60 = float(close.tail(60).mean())
    high20 = float(close.tail(20).max())
    drawdown = (latest / high20 - 1) * 100
    score = returns[5] * 0.5 + returns[20] * 0.3 + returns[60] * 0.2
    rs20 = returns[20] - benchmark_returns.get(20, 0.0)
    rs60 = returns[60] - benchmark_returns.get(60, 0.0)
    return {
        **item,
        "data_code": data_code,
        "source": source,
        "latest": latest,
        "r5": returns[5],
        "r20": returns[20],
        "r60": returns[60],
        "score": score,
        "ma20": ma20,
        "ma60": ma60,
        "above_ma20": latest >= ma20,
        "above_ma60": latest >= ma60,
        "drawdown": drawdown,
        "stop": drawdown <= MAX_DRAWDOWN_LIMIT,
        "rs20": rs20,
        "rs60": rs60,
        "rs_score": rs20 * 0.6 + rs60 * 0.4,
    }


def _rank_results(results: list[dict[str, Any]]) -> None:
    if not results:
        return
    ordered = sorted(results, key=lambda row: row["score"], reverse=True)
    total = len(ordered)
    for rank, item in enumerate(ordered, start=1):
        item["rank"] = rank
        item["pool_percentile"] = round((total - rank + 1) / total * 100, 1)
        item["pool_top_percent"] = max(1, math.ceil(rank / total * 100))
        if item["stop"]:
            item["action"] = "风控止损"
            item["amount"] = 0
        elif item["score"] > 0 and item["above_ma20"] and item["above_ma60"] and item["pool_percentile"] >= 70:
            item["action"] = "买入观察"
            item["amount"] = 250 if item["pool_percentile"] >= 90 and item["score"] >= 4 else 200
        elif item["above_ma20"]:
            item["action"] = "持有/观察"
            item["amount"] = 0
        else:
            item["action"] = "暂缓/观望"
            item["amount"] = 0


def analyse_market(now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(BEIJING_TZ)
    mode = current_mode(now)
    if mode == "off":
        mode = "morning" if now.astimezone(BEIJING_TZ).time() < time(12) else "evening"
    failures: list[str] = []
    watchlist, dynamic_count = build_watchlist(failures)
    benchmark_returns: dict[int, float] = {}
    try:
        benchmark_history, _, _ = fetch_item_history(BENCHMARK)
        if len(benchmark_history) >= 61:
            benchmark_returns = {days: _return(benchmark_history["close"], days) for days in (5, 20, 60)}
    except Exception as exc:
        failures.append(f"沪深300基准：{_short_error(exc)}")

    results: list[dict[str, Any]] = []
    for item in watchlist:
        try:
            results.append(analyse_item(item, benchmark_returns))
        except Exception as exc:
            failures.append(f"{item['code']} {item['name']}：{_short_error(exc)}")
    _rank_results(results)
    results.sort(key=lambda row: row["score"], reverse=True)
    return {
        "now": now,
        "mode": mode,
        "results": results,
        "failures": failures,
        "dynamic_count": dynamic_count,
        "watch_count": len(watchlist),
        "benchmark_returns": benchmark_returns,
    }


def _action_text(item: dict[str, Any]) -> str:
    if item["stop"]:
        if item["kind"] in {"alipay_c", "linked_c"}:
            return "风控止损；持有未满 7 天非极端暴跌请勿赎回，满 7 天后再考虑约 200 元分批平仓"
        return "风控止损；建议减仓约 200 元"
    if item["action"] == "买入观察":
        return f"动量与趋势共振，建议分批买入 {item['amount']} 元"
    if item["action"] == "持有/观察":
        return "站上 MA20，继续观察，不新增资金"
    return "低于 MA20 或相对强度不足，暂缓新增资金"


def _line(item: dict[str, Any]) -> str:
    trend = f"MA20 {'上方' if item['above_ma20'] else '下方'} / MA60 {'上方' if item['above_ma60'] else '下方'}"
    return (
        f"- **{item['code']} {item['name']}**（{item['kind']}）：5/20/60日 "
        f"{item['r5']:.2f}% / {item['r20']:.2f}% / {item['r60']:.2f}%，动量 **{item['score']:.2f}**，"
        f"池内分位 **{item['pool_percentile']:.1f}%**（约前 {item['pool_top_percent']}%）；"
        f"{trend}，相对沪深300超额20/60日 {item['rs20']:.2f}% / {item['rs60']:.2f}%，"
        f"回撤 {item['drawdown']:.2f}%，**{_action_text(item)}**。"
    )


def build_report(context: dict[str, Any]) -> tuple[str, str]:
    now: datetime = context["now"]
    mode = context["mode"]
    results = context["results"]
    title = "早盘全市场风向与动量预选" if mode == "morning" else "支付宝 14:30 实操买卖指南"
    benchmark = context["benchmark_returns"]
    lines = [
        f"# {title}",
        f"更新时间：{now:%Y-%m-%d %H:%M}（北京时间）",
        f"扫描 {context['watch_count']} 个候选（动态 ETF {context['dynamic_count']} 个），有效分析 {len(results)} 个。",
        "",
        "## 沪深300基准",
        f"5/20/60日：{benchmark.get(5, 0):.2f}% / {benchmark.get(20, 0):.2f}% / {benchmark.get(60, 0):.2f}%",
        "",
        "## 动量与深度归因 Top 榜",
    ]
    lines.extend(_line(item) for item in results[:MAX_REPORT_ITEMS])
    if mode == "morning":
        lines += ["", "**早盘焦点：** 优先观察池内分位靠前、同时站上 MA20/MA60 且相对沪深300为正的标的，开盘不追高。"]
    else:
        lines += ["", "**尾盘纪律：** 单笔加仓严格控制在 100～250 元，2000 元总资金分批操作，不因单日波动满仓。"]
    lines += [
        "",
        "**支付宝 C 类风控：** 支付宝 C 类持有未满 7 天赎回将收取 1.5% 惩罚性手续费，非暴跌请满 7 天后再平仓。",
        "",
        "**免责声明：** 本报告由量化脚本自动生成，仅供研究参考，不构成投资建议。",
    ]
    if context["failures"]:
        lines += ["", "## 获取失败（已跳过，不影响其他标的）"] + [f"- {failure}" for failure in context["failures"]]
    return title, "\n".join(lines)


def _dashboard_row(item: dict[str, Any]) -> str:
    kind_label = {"alipay_c": "支付宝 C 类", "linked_c": "自动匹配 C 类", "etf": "场内 ETF"}.get(item["kind"], item["kind"])
    action_class = "danger" if item["stop"] else ("positive" if item["action"] == "买入观察" else "neutral")
    return (
        f"<tr data-kind='{html.escape(item['kind'])}'><td>{item['rank']}</td>"
        f"<td><strong>{html.escape(item['name'])}</strong><small>{html.escape(item['code'])} · {kind_label}</small></td>"
        f"<td>{item['score']:.2f}</td><td>{item['pool_percentile']:.1f}%</td>"
        f"<td>{item['r20']:.2f}%<br><small>RS {item['rs20']:.2f}%</small></td>"
        f"<td>{'是' if item['above_ma20'] else '否'} / {'是' if item['above_ma60'] else '否'}</td>"
        f"<td class='{'danger' if item['stop'] else ''}'>{item['drawdown']:.2f}%</td>"
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
        "failures": context["failures"],
    }
    data_json = json.dumps(payload, ensure_ascii=False, default=str).replace("</", "<\\/")
    rows = "".join(_dashboard_row(item) for item in results)
    failures = "".join(f"<li>{html.escape(failure)}</li>" for failure in context["failures"])
    return f'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)} · ETF Workbench</title>
<style>
:root {{ color-scheme: light; --ink:#19212b; --muted:#66717f; --line:#dfe5eb; --panel:#fff; --bg:#f4f6f8; --green:#087f5b; --red:#c92a2a; --blue:#1769aa; }}
* {{ box-sizing:border-box; }} body {{ margin:0; background:var(--bg); color:var(--ink); font:15px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC",sans-serif; }}
main {{ max-width:1240px; margin:0 auto; padding:32px 20px 56px; }} header {{ display:flex; justify-content:space-between; gap:24px; align-items:flex-end; margin-bottom:24px; }}
.eyebrow {{ color:var(--blue); font-size:12px; font-weight:700; letter-spacing:.08em; text-transform:uppercase; }} h1 {{ margin:5px 0 4px; font-size:clamp(26px,4vw,42px); line-height:1.15; }} p {{ margin:0; color:var(--muted); }}
.stamp {{ color:var(--muted); text-align:right; white-space:nowrap; }} .stats {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin-bottom:22px; }}
.stat, .table-panel, .notes {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; }} .stat {{ padding:16px; }} .stat b {{ display:block; font-size:25px; }} .stat span {{ color:var(--muted); font-size:13px; }}
.toolbar {{ display:flex; gap:10px; margin:14px 0; }} input, select {{ border:1px solid var(--line); background:#fff; border-radius:6px; padding:10px 12px; color:var(--ink); }} input {{ flex:1; min-width:160px; }}
.table-panel {{ overflow:auto; }} table {{ width:100%; border-collapse:collapse; min-width:930px; }} th,td {{ padding:12px 13px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }} th {{ background:#f8fafb; color:var(--muted); font-size:12px; white-space:nowrap; }} td small {{ display:block; color:var(--muted); }} tr:last-child td {{ border-bottom:0; }}
.positive {{ color:var(--green); font-weight:700; }} .danger {{ color:var(--red); font-weight:700; }} .neutral {{ color:#8a5a00; }} .notes {{ padding:16px; margin-top:16px; }} .notes h2 {{ font-size:16px; margin:0 0 8px; }} .notes ul {{ margin:8px 0 0; padding-left:20px; color:var(--muted); }}
@media (max-width:700px) {{ main {{ padding:22px 12px 40px; }} header {{ display:block; }} .stamp {{ text-align:left; margin-top:10px; }} .stats {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .toolbar {{ flex-direction:column; }} }}
</style></head><body><main>
<header><div><div class="eyebrow">ETF WORKBENCH · 2000 CNY MODE</div><h1>{html.escape(title)}</h1><p>全市场动量、相对沪深300强弱与风险回撤的可视化筛选。</p></div><div class="stamp">更新时间<br><strong>{now:%Y-%m-%d %H:%M} 北京时间</strong></div></header>
<section class="stats"><div class="stat"><span>有效标的</span><b>{len(results)}</b></div><div class="stat"><span>动态扫描 ETF</span><b>{context['dynamic_count']}</b></div><div class="stat"><span>买入观察</span><b>{sum(item['action'] == '买入观察' for item in results)}</b></div><div class="stat"><span>触发风控</span><b>{sum(item['stop'] for item in results)}</b></div></section>
<div class="toolbar"><input id="search" type="search" placeholder="搜索名称或代码"><select id="kind"><option value="all">全部标的</option><option value="alipay_c">支付宝 C 类</option><option value="linked_c">自动匹配 C 类</option><option value="etf">场内 ETF</option></select></div>
<section class="table-panel"><table><thead><tr><th>排名</th><th>标的</th><th>动量得分</th><th>池内分位</th><th>20日收益 / RS</th><th>MA20 / MA60</th><th>20日回撤</th><th>动作与依据</th></tr></thead><tbody id="rows">{rows}</tbody></table></section>
<section class="notes"><h2>风控与数据状态</h2><p>单笔加仓控制在 100～250 元；支付宝 C 类持有未满 7 天赎回将收取 1.5% 惩罚性手续费，非暴跌请满 7 天后再平仓。</p>{f'<ul>{failures}</ul>' if failures else '<p>本次扫描未记录接口失败。</p>'}</section>
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
