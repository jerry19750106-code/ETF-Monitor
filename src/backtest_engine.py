"""回測引擎:預測紀錄 + 實現報酬 → 依 report_type 分流的準確度 / 盈虧 / 回撤。

三種評估口徑(核心是依 report_type 分流):
  stored    資料庫現存的 actual_ret 原樣使用 —— 量測「目前統計被污染到什麼程度」。
  semantic  報告「宣稱要預測」的目標:盤前/早/午報 → 當日報酬(T+0);收盤報 → 次一交易日(T+1)。
  tradable  場外基金下單後「真正拿到」的報酬:15:00 截單前(盤前/早/午報)→ 次一交易日;
            收盤報在截單後發出,最快次一交易日成交 → 次二交易日。

實現報酬參考值(reference)優先取單一來源表 realized_returns(見 returns_store.py);
該表尚未建立時,退回 T+0 報告(盤前/早/午)的 actual_ret:同一 (code, 日) 在這些報告間必須一致,
不一致者剔除並回報,不猜。「交易日」以該 code 有參考值的日期序列定義。

可獨立執行:  python src/backtest_engine.py [db 路徑]
"""
import collections
import datetime
import math
import os
import sqlite3
import statistics
import sys
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import returns_store  # noqa: E402

T0_TYPES = ("盤前報", "早報", "午報")
T1_TYPES = ("收盤報",)
PRE_CUTOFF_TYPES = T0_TYPES
REPORT_TYPES = T0_TYPES + T1_TYPES
MODES = ("stored", "semantic", "tradable")
MIN_ABS_RET = 1e-4          # |報酬| 低於此視為未回填/無效(與主程式 ABS(actual_ret)>0.0001 一致)
PURE_MIN_ABS_PRED = 0.001   # 「純方向」只計 |預測| ≥ 0.1% 的明確方向訊號
MATCH_TOL = 1e-6


@dataclass(frozen=True)
class Record:
    id: int
    code: str
    date: str
    report_type: str
    market: str
    predicted: float
    actual: float
    ts: str


def load_records(db):
    with sqlite3.connect(db) as conn:
        rows = conn.execute(
            "SELECT rowid, code, date(ts), COALESCE(report_type,''), COALESCE(market,''), "
            "predicted_ret, actual_ret, ts FROM predictions").fetchall()
    return [Record(*r) for r in rows]


def reference_returns(records):
    """回 (ref, conflicts):ref[(code, 日)] = 當日實現報酬;conflicts = 參考值彼此不一致的 (code, 日)。"""
    vals = collections.defaultdict(set)
    for r in records:
        if r.report_type in T0_TYPES and r.actual is not None and abs(r.actual) > MIN_ABS_RET:
            vals[(r.code, r.date)].add(round(r.actual, 10))
    ref = {k: next(iter(v)) for k, v in vals.items() if len(v) == 1}
    conflicts = {k for k, v in vals.items() if len(v) > 1}
    return ref, conflicts


def load_reference(db):
    """單一來源的實現報酬 {(code, 日): 報酬}(realized_returns;未建表時為 T+0 共識值)。"""
    return returns_store.load(db)


def trading_days(ref):
    days = collections.defaultdict(list)
    for code, d in ref:
        days[code].append(d)
    return {c: sorted(v) for c, v in days.items()}


def shift(code, date, n, days):
    """code 在 date 之後第 n 個交易日(n≥1);無則 None。"""
    later = [d for d in days.get(code, []) if d > date]
    return later[n - 1] if len(later) >= n else None


def outcome(rec, mode, ref, days):
    """依口徑回傳該筆預測要比對的實現報酬;無法取得回 None。"""
    if mode == "stored":
        return rec.actual if rec.actual is not None and abs(rec.actual) > MIN_ABS_RET else None
    if rec.report_type in T0_TYPES:
        if mode == "semantic":
            return ref.get((rec.code, rec.date))
        nd = shift(rec.code, rec.date, 1, days)
        return ref.get((rec.code, nd)) if nd else None
    if rec.report_type in T1_TYPES:
        nd = shift(rec.code, rec.date, 1 if mode == "semantic" else 2, days)
        return ref.get((rec.code, nd)) if nd else None
    return None


def dedupe(records):
    """同 (code, 日, report_type) 只取最晚一筆;排除空白 report_type 與補跑。"""
    best = {}
    for r in records:
        if r.report_type not in REPORT_TYPES or r.predicted is None or r.predicted == 0.0:
            continue
        k = (r.code, r.date, r.report_type)
        if k not in best or (r.ts or "") > (best[k].ts or ""):
            best[k] = r
    return list(best.values())


def max_drawdown(daily_returns):
    """daily_returns:依日期排序的每日報酬。回最大回撤(正數,例如 0.12 = 12%)。"""
    peak, equity, mdd = 1.0, 1.0, 0.0
    for r in daily_returns:
        equity *= (1.0 + r)
        peak = max(peak, equity)
        mdd = max(mdd, 1.0 - equity / peak)
    return mdd


def _metrics(pairs):
    """pairs: [(date, predicted, outcome)]。每日等權平均訊號報酬作為組合曲線與 t 檢定的單位。"""
    if not pairs:
        return {"n": 0, "n_days": 0}
    pnl = [(1.0 if p > 0 else -1.0) * a for _, p, a in pairs]
    hits = [(p > 0) == (a > 0) for _, p, a in pairs]
    pure = [(p > 0) == (a > 0) for _, p, a in pairs if abs(p) >= PURE_MIN_ABS_PRED]
    by_day = collections.defaultdict(list)
    for (d, p, a), x in zip(pairs, pnl):
        by_day[d].append(x)
    daily = [statistics.mean(by_day[d]) for d in sorted(by_day)]
    t = None
    if len(daily) >= 2 and statistics.stdev(daily) > 0:
        t = statistics.mean(daily) / (statistics.stdev(daily) / math.sqrt(len(daily)))
    return {
        "n": len(pairs), "n_days": len(daily),
        "direction_acc": sum(hits) / len(hits),
        "pure_n": len(pure),
        "pure_direction_acc": (sum(pure) / len(pure)) if pure else None,
        "avg_pnl": statistics.mean(pnl),
        "daily_t": t,
        "max_drawdown": max_drawdown(daily),
    }


def evaluate(records, mode, ref=None):
    """回 {report_type: metrics};metrics 見 _metrics。只用去重後的預測。"""
    if mode not in MODES:
        raise ValueError(f"mode 必須是 {MODES}")
    if ref is None:
        ref, _ = reference_returns(records)
    days = trading_days(ref)
    grouped = collections.defaultdict(list)
    for r in dedupe(records):
        a = outcome(r, mode, ref, days)
        if a is not None and abs(a) > MIN_ABS_RET:
            grouped[r.report_type].append((r.date, r.predicted, a))
    return {rt: _metrics(grouped.get(rt, [])) for rt in REPORT_TYPES}


def classify_closing_alignment(records, ref=None):
    """收盤報 actual_ret 落位檢查。回 [(record, 類別)],類別:
       T+1正確 / T+0錯配 / 兩者皆符 / 來源不同無法判定 / 無參考 / 未回填。"""
    if ref is None:
        ref, _ = reference_returns(records)
    days = trading_days(ref)
    out = []
    for r in records:
        if r.report_type not in T1_TYPES:
            continue
        if r.actual is None or abs(r.actual) <= MIN_ABS_RET:
            out.append((r, "未回填"))
            continue
        nd = shift(r.code, r.date, 1, days)
        t0, t1 = ref.get((r.code, r.date)), (ref.get((r.code, nd)) if nd else None)
        m0 = t0 is not None and abs(r.actual - t0) < MATCH_TOL
        m1 = t1 is not None and abs(r.actual - t1) < MATCH_TOL
        if m1 and not m0:
            cat = "T+1正確"
        elif m0 and not m1:
            cat = "T+0錯配"
        elif m0 and m1:
            cat = "兩者皆符"
        elif t0 is None and t1 is None:
            cat = "無參考"
        else:
            cat = "來源不同無法判定"
        out.append((r, cat))
    return out


def _pct(x):
    return "—" if x is None else f"{x:.1%}"


def format_report(records, ref=None):
    if ref is None:
        ref, _ = reference_returns(records)
    lines = ["| 口徑 | 報告 | 筆數 | 交易日 | 方向準確度 | 純方向準確度(筆數) | 平均訊號報酬 | 日聚合 t 值 | 最大回撤 |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for mode in MODES:
        res = evaluate(records, mode, ref)
        for rt in REPORT_TYPES:
            m = res[rt]
            if not m["n"]:
                continue
            t = "—" if m["daily_t"] is None else f"{m['daily_t']:+.2f}"
            lines.append(f"| {mode} | {rt} | {m['n']} | {m['n_days']} | {_pct(m['direction_acc'])} | "
                         f"{_pct(m['pure_direction_acc'])}({m['pure_n']}) | {m['avg_pnl']:+.3%} | {t} | "
                         f"{m['max_drawdown']:.1%} |")
    return "\n".join(lines)


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else "data_sample/v20_prediction_log.db"
    print(f"# 回測報告 ({db}, 產生於 {datetime.datetime.now():%Y-%m-%d %H:%M})\n")
    print(format_report(load_records(db), load_reference(db)))
