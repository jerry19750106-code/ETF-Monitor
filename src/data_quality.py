"""預測紀錄資料品質檢查(機械驗算,非人工核對)。可獨立執行,輸出 Markdown 報告:

    python src/data_quality.py [db 路徑] [--out 報告.md]

檢查項目:
  1. 各 report_type 筆數(含空白 report_type 與「補跑」)
  2. 未回填筆數(actual_ret 為空或 |actual_ret| ≤ 0.0001)
  3. 重複膨脹:同 (code, 日, report_type) 的筆數 / 組數
  4. T+0 參考值一致性:同 (code, 日) 的盤前/早/午報 actual_ret 是否相同
  5. 收盤報 actual_ret 落位:T+1 正確 / T+0 錯配 / 來源不同無法判定 / 無參考 / 未回填(逐月)
  6. 跨表一致性:T+0 參考值 vs daily_snapshot.today_ret、capital_flow_daily.nav_ret
     (同日與 ±1 交易日平移的相關/方向一致/數值相同,判斷是否日期錯位或來源混用)
"""
import argparse
import collections
import datetime
import os
import sqlite3
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import backtest_engine as be  # noqa: E402


def count_by_type(records):
    c = collections.Counter(r.report_type or "(空白)" for r in records)
    return dict(sorted(c.items(), key=lambda kv: -kv[1]))


def unfilled_by_type(records):
    c = collections.Counter()
    for r in records:
        if r.actual is None or abs(r.actual) <= be.MIN_ABS_RET:
            c[r.report_type or "(空白)"] += 1
    return dict(c)


def duplication(records):
    groups = collections.Counter((r.code, r.date, r.report_type) for r in records if r.report_type in be.REPORT_TYPES)
    out = {}
    for rt in be.REPORT_TYPES:
        rows = sum(v for (c, d, t), v in groups.items() if t == rt)
        keys = sum(1 for (c, d, t) in groups if t == rt)
        out[rt] = (rows, keys)
    return out


def closing_alignment(records, ref):
    total = collections.Counter()
    by_month = collections.defaultdict(collections.Counter)
    misaligned_ids = []
    for rec, cat in be.classify_closing_alignment(records, ref):
        total[cat] += 1
        by_month[rec.date[:7]][cat] += 1
        if cat == "T+0錯配":
            misaligned_ids.append(rec.id)
    return dict(total), {m: dict(v) for m, v in sorted(by_month.items())}, misaligned_ids


def cross_source(db, ref):
    """回 {表名: {lag: (n, 相關, 方向一致, 數值相同)}};表不存在則略過。"""
    sources = {
        "daily_snapshot.today_ret": "SELECT code, date, today_ret FROM daily_snapshot WHERE today_ret IS NOT NULL",
        "capital_flow_daily.nav_ret": "SELECT code, date, nav_ret FROM capital_flow_daily WHERE nav_ret IS NOT NULL",
    }
    out = {}
    with sqlite3.connect(db) as conn:
        for name, q in sources.items():
            try:
                other = {(c, d): v for c, d, v in conn.execute(q) if v is not None and abs(v) > be.MIN_ABS_RET}
            except sqlite3.Error:
                continue
            if not other:
                continue
            # 單位:逐筆判斷該值與參考值是「小數」相同還是「百分比(÷100)」相同 —— 同欄混用兩種單位也抓得到
            unit = collections.Counter()
            for k in ref:
                # 只在 |報酬| ≥ 0.2% 判定單位:小值除以 100 後仍可能落在容差內,會誤判
                if k in other and abs(ref[k]) >= 0.002:
                    v = other[k]
                    if abs(v - ref[k]) < be.MIN_ABS_RET:
                        unit["小數"] += 1
                    elif abs(v / 100 - ref[k]) < be.MIN_ABS_RET:
                        unit["百分比"] += 1
            scale = 100.0 if unit["百分比"] > unit["小數"] else 1.0
            other = {k: v / scale for k, v in other.items()}
            days = collections.defaultdict(set)
            for c, d in list(ref) + list(other):
                days[c].add(d)
            days = {c: sorted(v) for c, v in days.items()}
            idx = {(c, d): i for c, v in days.items() for i, d in enumerate(v)}
            res = {}
            for lag in (-1, 0, 1):
                pairs = []
                for (c, d), a in ref.items():
                    i = idx[(c, d)] + lag
                    if 0 <= i < len(days[c]) and (c, days[c][i]) in other:
                        pairs.append((a, other[(c, days[c][i])]))
                if len(pairs) >= 3:
                    xs, ys = zip(*pairs)
                    corr = statistics.correlation(xs, ys) if len(set(xs)) > 1 and len(set(ys)) > 1 else None
                    sign = sum((x > 0) == (y > 0) for x, y in pairs) / len(pairs)
                    same = sum(abs(x - y) < be.MIN_ABS_RET for x, y in pairs) / len(pairs)
                    res[lag] = (len(pairs), corr, sign, same)
            out[name] = {"scale": scale, "lags": res, "unit": dict(unit)}
    return out


def build_report(db):
    records = be.load_records(db)
    ref, conflicts = be.reference_returns(records)
    lines = [f"# 資料品質報告", "",
             f"- 資料庫:`{db}`",
             f"- 產生時間:{datetime.datetime.now():%Y-%m-%d %H:%M}",
             f"- 總筆數:{len(records)};日期範圍:{min(r.date for r in records)} ~ {max(r.date for r in records)}"
             if records else "- 總筆數:0", ""]
    lines += ["## 1. 各 report_type 筆數", "", "| report_type | 筆數 |", "|---|---:|"]
    lines += [f"| {k} | {v} |" for k, v in count_by_type(records).items()]
    lines += ["", "## 2. 未回填筆數", "", "| report_type | 未回填 |", "|---|---:|"]
    lines += [f"| {k} | {v} |" for k, v in sorted(unfilled_by_type(records).items(), key=lambda kv: -kv[1])]
    lines += ["", "## 3. 重複膨脹(同 code×日×report_type)", "", "| report_type | 筆數 | 組數 | 膨脹 |", "|---|---:|---:|---:|"]
    for rt, (rows, keys) in duplication(records).items():
        if keys:
            lines.append(f"| {rt} | {rows} | {keys} | {rows / keys:.2f}x |")
    lines += ["", "## 4. T+0 參考值一致性", "",
              f"同 (code, 日) 的盤前/早/午報 actual_ret:可用 {len(ref)} 組,**互相矛盾 {len(conflicts)} 組**(矛盾者不作參考)。"]
    total, by_month, mis = closing_alignment(records, ref)
    cats = ["T+1正確", "T+0錯配", "兩者皆符", "來源不同無法判定", "無參考", "未回填"]
    lines += ["", "## 5. 收盤報 actual_ret 落位", "",
              "收盤報預測次一交易日(T+1)。若其 actual_ret 等於【當日】參考值 → T+0 錯配(污染回測)。", "",
              "| 月份 | " + " | ".join(cats) + " |", "|---|" + "---:|" * len(cats)]
    for m, c in by_month.items():
        lines.append(f"| {m} | " + " | ".join(str(c.get(k, 0)) for k in cats) + " |")
    lines.append("| **合計** | " + " | ".join(f"**{total.get(k, 0)}**" for k in cats) + " |")
    if mis:
        lines += ["", f"T+0 錯配的 predictions.id(前 20 筆):`{mis[:20]}`"]
    lines += ["", "## 6. 跨表一致性(同一檔基金同一天的報酬,三處來源是否相同)", "",
              "| 對照表 | 單位換算 | 平移 | 筆數 | 相關係數 | 方向一致 | 數值相同 |", "|---|---|---|---:|---:|---:|---:|"]
    xs = cross_source(db, ref)
    for name, info in xs.items():
        for lag, (n, corr, sign, same) in sorted(info["lags"].items()):
            lag_txt = {-1: "對方前一交易日", 0: "同一交易日", 1: "對方後一交易日"}[lag]
            corr_txt = "—" if corr is None else f"{corr:+.3f}"
            lines.append(f"| {name} | ÷{info['scale']:.0f} | {lag_txt} | {n} | {corr_txt} | {sign:.1%} | {same:.1%} |")
    for name, info in xs.items():
        u = info["unit"]
        if u.get("小數") and u.get("百分比"):
            lines += ["", f"⚠️ **{name} 單位混用**:與參考值相同者中,{u['小數']} 筆以小數存、{u['百分比']} 筆以百分比存。"]
    lines += ["", "> 判讀:若「同一交易日」數值相同比例遠低於 100%,代表同一天的報酬在不同表來自不同資料源"
              "(例如場外 NAV vs 場內代理 ETF);若某個平移的相關明顯高於同日,代表日期錯位。",
              "> 註:`daily_snapshot.today_ret` 存的是系統當日估值(場內代理經 RSI 截斷、±8% 夾擠),"
              "`capital_flow_daily.nav_ret` 是場內代理漲跌(百分比);兩者都不是實現報酬,不一致屬預期。", ""]
    lines += ["## 7. 實現報酬單一來源(realized_returns)", ""]
    with sqlite3.connect(db) as conn:
        try:
            src = conn.execute("SELECT src, COUNT(*) FROM realized_returns GROUP BY src ORDER BY 2 DESC").fetchall()
        except sqlite3.Error:
            src = None
    if src is None:
        lines += ["尚未建立(執行一次主程式,或 `python src/migrate_realized_returns.py <db> --apply`)。", ""]
    else:
        lines += ["| 來源 | 筆數 |", "|---|---:|"] + [f"| {s_} | {n} |" for s_, n in src] + [""]
    return "\n".join(lines)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="預測紀錄資料品質檢查")
    ap.add_argument("db", nargs="?", default="data_sample/v20_prediction_log.db")
    ap.add_argument("--out")
    args = ap.parse_args()
    report = build_report(args.db)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"已寫入 {args.out}")
    else:
        print(report)
