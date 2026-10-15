"""訊號健康度:每種報告 × 市場,近期訊號照著做「真正拿得到」的報酬是否顯著為正。

機構做法是持續監控每個訊號的實盤(可交易)表現,表現不再顯著就停止背書 —— 而不是看
「預測 vs 當日」這種盤中同期推估的命中率。本模組:
  1. 以 backtest_engine 的 tradable 口徑(截單前訊號 → 次一交易日;收盤報 → 次二交易日)
     計算滾動視窗內的日聚合 t 值;
  2. 同時檢定多個 (報告, 市場) 組合,以 Bonferroni 校正單尾臨界值,避免多重比較灌水;
  3. 給出狀態與是否背書。不背書的操作建議仍保留金額,只標為「待你裁決」(與 V22.59 原則一致:
     系統可以不建議,但不剝奪選擇)。
"""
import datetime
import statistics

import backtest_engine as be

WINDOW_DAYS = 120          # 滾動視窗(日曆日)
MIN_DAYS = 30              # 有效樣本以交易日計;不足不背書
ALPHA = 0.05               # 單尾顯著水準
UNADJUSTED_T = statistics.NormalDist().inv_cdf(1 - ALPHA)   # ≈1.645
MARKETS = ("A股", "台股")

STATUS_SIGNIFICANT = "顯著"
STATUS_MARGINAL = "邊緣"
STATUS_UNPROVEN = "未證實"
STATUS_NEGATIVE = "顯著為負"
STATUS_INSUFFICIENT = "樣本不足"
ENDORSED = (STATUS_SIGNIFICANT, STATUS_MARGINAL)


def adjusted_t(m):
    """m 個同時檢定的單尾 Bonferroni 臨界值(常態近似)。"""
    return statistics.NormalDist().inv_cdf(1 - ALPHA / max(1, m))


def status_of(n_days, t, m):
    if not n_days or n_days < MIN_DAYS or t is None:
        return STATUS_INSUFFICIENT
    if t >= adjusted_t(m):
        return STATUS_SIGNIFICANT
    if t >= UNADJUSTED_T:
        return STATUS_MARGINAL
    if t <= -UNADJUSTED_T:
        return STATUS_NEGATIVE
    return STATUS_UNPROVEN


def compute(records, ref, today=None, window_days=WINDOW_DAYS):
    """回 {(report_type, market): {n, n_days, avg_pnl, daily_t, status, endorsed}}。
    records:backtest_engine.Record 清單;ref:{(code, date): 實現報酬}(returns_store.load_map)。"""
    today = today or datetime.date.today()
    cutoff = (today - datetime.timedelta(days=window_days)).isoformat()
    recent = [r for r in records if cutoff <= r.date <= today.isoformat()]
    cells = {}
    for mkt in MARKETS:
        res = be.evaluate([r for r in recent if r.market == mkt], "tradable", ref)
        for rt, m in res.items():
            cells[(rt, mkt)] = m
    tested = sum(1 for m in cells.values() if m.get("n_days", 0) >= MIN_DAYS)
    out = {}
    for key, m in cells.items():
        st = status_of(m.get("n_days", 0), m.get("daily_t"), tested)
        out[key] = {"n": m.get("n", 0), "n_days": m.get("n_days", 0), "avg_pnl": m.get("avg_pnl"),
                    "daily_t": m.get("daily_t"), "status": st, "endorsed": st in ENDORSED}
    return out


def gate_actions(actions, health, report_type, market_of):
    """對操作清單套用健康度閘門(就地修改並回傳)。不背書者:user_optional=True、操作文字加註原因。
    actions:[{code, act, user_optional, ...}];market_of:code → 市場。"""
    for a in actions:
        h = health.get((report_type, market_of(a.get("code"))))
        if h is None or h["endorsed"]:
            continue
        a["user_optional"] = True
        a["act"] = f"{a.get('act', '')}(訊號{h['status']}·系統不背書)"
    return actions


def _fmt_t(t):
    return "—" if t is None else f"{t:+.2f}"


def format_block(health, report_type, window_days=WINDOW_DAYS):
    """報告用 Markdown 區塊:本報類型在各市場的可交易優勢。"""
    rows = [(mkt, health.get((report_type, mkt))) for mkt in MARKETS]
    rows = [(mkt, h) for mkt, h in rows if h is not None]
    if not rows:
        return ""
    horizon = ("收盤報在 15:00 截單後發出,最快次一交易日下單、成交於次一交易日收盤淨值,"
               "**賺的是次二交易日(T+2)的漲跌**"
               if report_type in be.T1_TYPES else
               "截單前下單成交於當日收盤淨值,**賺的是次一交易日(T+1)的漲跌**")
    tested = sum(1 for _, h in health.items() if h["n_days"] >= MIN_DAYS)
    lines = [f"\n## 📏 {report_type}訊號的可交易優勢(近 {window_days} 日,依實際成交時點)\n",
             f"> {horizon}。以下為照本報訊號方向操作、在該時點真正拿到的報酬。",
             f"> 判定:單尾 5%,{tested} 組同時檢定經 Bonferroni 校正(臨界 t={adjusted_t(tested):.2f});"
             f"未達 {UNADJUSTED_T:.2f} 或樣本 <{MIN_DAYS} 個交易日 → **系統不背書**,操作改列「待你裁決」。\n",
             "| 市場 | 交易日 | 平均每筆 | 日聚合 t | 狀態 |", "|---|---:|---:|---:|---|"]
    for mkt, h in rows:
        avg = "—" if h["avg_pnl"] is None else f"{h['avg_pnl']:+.3%}"
        mark = "✅" if h["endorsed"] else "⚠️"
        lines.append(f"| {mkt} | {h['n_days']} | {avg} | {_fmt_t(h['daily_t'])} | {mark} {h['status']} |")
    return "\n".join(lines) + "\n"
