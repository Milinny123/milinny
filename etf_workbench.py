"""Small-capital momentum screener for Alipay funds and exchange-traded ETFs."""

from __future__ import annotations

import os
from datetime import datetime, time, timedelta, timezone
from typing import Any

import akshare as ak
import pandas as pd
import requests


BEIJING_TZ = timezone(timedelta(hours=8))
FUNDS = {
    "012616": "华夏半导体芯片ETF联接C",
    "011613": "易方达中证科创50联接C",
    "015874": "富国中证人工智能ETF联接C",
    "007339": "易方达沪深300ETF联接C",
}
ETFS = {"588000": "科创50ETF", "512480": "半导体ETF"}


def current_mode(now: datetime | None = None) -> str:
    """Return morning, evening, or off according to Beijing local time."""
    now = now or datetime.now(BEIJING_TZ)
    clock = now.time()
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


def _normalise_history(frame: Any) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ValueError("接口返回空数据")
    date_col = _first_existing(frame, ("日期", "净值日期", "date"))
    value_col = _first_existing(frame, ("收盘", "收盘价", "close", "单位净值", "累计净值", "净值", "value"))
    if not date_col or not value_col:
        raise ValueError(f"无法识别日期/价格列: {list(frame.columns)}")
    result = frame[[date_col, value_col]].copy()
    result.columns = ["date", "close"]
    result["date"] = pd.to_datetime(result["date"], errors="coerce")
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result = result.dropna().drop_duplicates("date").sort_values("date")
    return result.tail(150).reset_index(drop=True)


def fetch_history(code: str, is_fund: bool) -> pd.DataFrame:
    """Fetch daily history, accommodating AkShare naming changes across versions."""
    attempts = []
    if is_fund:
        attempts = [
            lambda: ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势"),
            lambda: ak.fund_etf_fund_info_em(fund=code),
        ]
    else:
        attempts = [
            lambda: ak.fund_etf_hist_em(symbol=code, period="daily", adjust="qfq"),
            lambda: ak.fund_etf_hist_sina(symbol=f"sh{code}"),
            lambda: ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq"),
        ]
    errors = []
    for attempt in attempts:
        try:
            return _normalise_history(attempt())
        except Exception as exc:  # one provider/signature failure should not stop the pool
            errors.append(str(exc))
    raise RuntimeError("；".join(errors))


def analyse(code: str, name: str, is_fund: bool) -> dict[str, Any]:
    history = fetch_history(code, is_fund)
    if len(history) < 61:
        raise ValueError(f"仅有 {len(history)} 条有效数据，少于 61 条")
    close = history["close"]
    latest = float(close.iloc[-1])
    returns = {days: (latest / float(close.iloc[-1 - days]) - 1) * 100 for days in (5, 20, 60)}
    ma20 = float(close.tail(20).mean())
    high20 = float(close.tail(20).max())
    drawdown = (latest / high20 - 1) * 100
    score = returns[5] * 0.5 + returns[20] * 0.3 + returns[60] * 0.2
    stop = drawdown < -8
    buy = score > 0 and latest > ma20 and not stop
    action = "买入观察" if buy else ("风控止损" if stop else "暂缓/持有")
    amount = 250 if buy and score >= 5 else (200 if buy and score >= 2 else (150 if buy else 0))
    return {"code": code, "name": name, "is_fund": is_fund, "latest": latest,
            "r5": returns[5], "r20": returns[20], "r60": returns[60], "score": score,
            "ma20": ma20, "drawdown": drawdown, "above_ma20": latest > ma20,
            "stop": stop, "action": action, "amount": amount}


def _line(item: dict[str, Any]) -> str:
    risk = "⚠️回撤止损" if item["stop"] else ("站上MA20" if item["above_ma20"] else "低于MA20")
    if item["stop"]:
        amount = "持有满7天可考虑赎回约200元" if item["is_fund"] else "建议减仓约200元"
    elif item["amount"]:
        amount = f"建议分批买入{item['amount']}元"
    else:
        amount = "本次不新增资金"
    return (f"- **{item['code']} {item['name']}**：5/20/60日 {item['r5']:.2f}% / "
            f"{item['r20']:.2f}% / {item['r60']:.2f}%，得分 **{item['score']:.2f}**；"
            f"{risk}，回撤 {item['drawdown']:.2f}%，{item['action']}，{amount}。")


def build_report(results: list[dict[str, Any]], failures: list[str], mode: str, now: datetime) -> tuple[str, str]:
    title = "早盘市场展望与动量预选" if mode == "morning" else "支付宝 14:30 尾盘实操指南"
    lines = [f"# {title}", f"更新时间：{now:%Y-%m-%d %H:%M}（北京时间）", "", "## 动量筛选"]
    lines.extend(_line(item) for item in sorted(results, key=lambda x: x["score"], reverse=True))
    if mode == "morning":
        lines += ["", "**今日焦点：** 优先观察得分靠前且站上 MA20 的标的，开盘不追高，等待趋势确认。"]
    elif mode == "evening":
        lines += ["", "**执行纪律：** 单笔买入控制在 100～250 元；分批操作，保留现金，不因单日波动满仓。"]
    lines += ["", "**支付宝 C 类基金提醒：** 若买入未满 7 天，非极端暴跌请慎重赎回，以避免 1.5% 惩罚费。"]
    if failures:
        lines += ["", "## 获取失败（已跳过）"] + [f"- {failure}" for failure in failures]
    return title, "\n".join(lines)


def push_wechat(title: str, markdown: str) -> None:
    send_key = os.getenv("SERVERCHAN_KEY", "").strip()
    if not send_key:
        print("未设置 SERVERCHAN_KEY，仅输出终端报告。")
        return
    url = f"https://sctapi.ftqq.com/{send_key}.send"
    try:
        response = requests.post(url, data={"title": title, "desp": markdown}, timeout=15)
        response.raise_for_status()
        print("Server酱推送完成。")
    except requests.RequestException as exc:
        print(f"Server酱推送失败（不影响本次分析）：{exc}")


def main() -> None:
    now = datetime.now(BEIJING_TZ)
    mode = current_mode(now)
    if mode == "off":
        print(f"当前时间 {now:%H:%M} 不在 08:30-10:30 或 14:00-15:30 推送窗口，仍执行分析。")
        mode = "morning" if now.time() < time(12) else "evening"
    results: list[dict[str, Any]] = []
    failures: list[str] = []
    for code, name in {**FUNDS, **ETFS}.items():
        try:
            results.append(analyse(code, name, code in FUNDS))
        except Exception as exc:
            failures.append(f"{code} {name}：{exc}")
            print(f"[跳过] {code} {name}: {exc}")
    title, report = build_report(results, failures, mode, now)
    print(report)
    push_wechat(title, report)


if __name__ == "__main__":
    main()
