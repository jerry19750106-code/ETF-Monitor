"""一次性資料清洗(冪等,可重複執行;第二次執行應為 0 筆變更)。

    python src/migrate_realized_returns.py <db>                     # 預覽:只列出會改什麼,不寫入
    python src/migrate_realized_returns.py <db> --apply             # 先備份 <db>.bak-時間戳,再寫入
    python src/migrate_realized_returns.py <db> --apply --rebuild-from-nav
                                                                    # 另以官方 NAV 重建A股歷史實現報酬(需網路)

步驟(每筆變更都寫入 data_corrections 稽核表:表、主鍵、欄位、舊值、新值、原因):
  1. 建立 realized_returns(實現報酬單一來源),以遷移前一致的 T+0 實現值填入(src=legacy)。
  2. 收盤報 actual_ret 由單一來源重新推導:收盤報預測次一交易日,其 actual_ret 應等於該 code 次一交易日的
     實現報酬。已等於者不動;其餘一律改配(兩日之間有缺漏工作日 —— 漏跑/長假無法區分 —— 或無資料 → 清為未回填)。
       - T+0 錯配:actual_ret 完全等於【當日】報酬(V22.39 修復前的回填錯誤);
       - 來源不同:與當日、次日都不完全相等,無法逐筆驗證。實測這群值與【當日】報酬的方向一致率 63%、
         與次日只有 51%,8–9 月仍是如此 —— 例如舊版週一早上把週五的 NAV 報酬標成週日,
         再配給週五的收盤報。來源不同使精確比對抓不到,所以不採信,改由單一來源推導。
  3. capital_flow_daily.nav_ret 單位:現行程式寫百分比,歷史有少數以小數寫入。只轉換能與實現報酬
     機械比對確認為小數的列(|報酬| ≥ 0.2%,小數相符且百分比不符);無法確認者不動。
  4. (選用,--rebuild-from-nav)抓取A股各檔完整 NAV 歷史,以 src=nav 覆蓋較低等級的實現報酬
     (legacy/proxy),並依語意重新落位 predictions.actual_ret。這一步才能修正「來源不同無法判定」的歷史列。
"""
import argparse
import datetime
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import backtest_engine as be  # noqa: E402
import returns_store  # noqa: E402

UNIT_MIN_ABS = 0.002
TOL = 1e-4

CORRECTIONS_DDL = ("CREATE TABLE IF NOT EXISTS data_corrections("
                   "id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, tbl TEXT NOT NULL, "
                   "row_key TEXT NOT NULL, col TEXT NOT NULL, old REAL, new REAL, reason TEXT NOT NULL)")


def _weekdays_between(a, b):
    a, b = datetime.date.fromisoformat(a), datetime.date.fromisoformat(b)
    return sum(1 for i in range(1, (b - a).days) if (a + datetime.timedelta(days=i)).weekday() < 5)


def _records(conn):
    rows = conn.execute(
        "SELECT rowid, code, date(ts), COALESCE(report_type,''), COALESCE(market,''), "
        "predicted_ret, actual_ret, ts FROM predictions").fetchall()
    return [be.Record(*r) for r in rows]


KEEP = ("T+1正確", "兩者皆符")


def plan_closing_fixes(conn, ref):
    """回 [(id, 舊值, 新值或 None, 類別, 原因)]。類別沿用 backtest_engine.classify_closing_alignment。"""
    days = be.trading_days(ref)
    out = []
    for rec, cat in be.classify_closing_alignment(_records(conn), ref):
        if cat in KEEP:
            continue
        nd = be.shift(rec.code, rec.date, 1, days)
        new = None
        if nd and _weekdays_between(rec.date, nd) == 0:
            new = ref.get((rec.code, nd))
        old = rec.actual
        if (old is None or abs(old) <= be.MIN_ABS_RET) and new is None:
            continue
        target = (f"改配次一交易日({nd})" if new is not None else
                  "次一交易日無資料或中間缺漏工作日 → 清為未回填")
        out.append((rec.id, old, new, cat, f"收盤報[{cat}]:由單一來源重新推導;{target}"))
    return out


def plan_unit_fixes(conn, ref):
    """回 [((code, date), 舊值, 新值, 原因)]。"""
    try:
        rows = conn.execute("SELECT code, date, nav_ret FROM capital_flow_daily WHERE nav_ret IS NOT NULL").fetchall()
    except sqlite3.Error:
        return []
    out = []
    for code, d, v in rows:
        r = ref.get((code, d))
        if r is None or abs(r) < UNIT_MIN_ABS:
            continue
        if abs(v - r) < TOL and abs(v / 100 - r) >= TOL:
            out.append(((code, d), v, v * 100, "nav_ret 以小數寫入(與實現報酬相符);統一為百分比"))
    return out


def _log(conn, tbl, key, col, old, new, why, now):
    conn.execute("INSERT INTO data_corrections(ts, tbl, row_key, col, old, new, reason) VALUES(?,?,?,?,?,?,?)",
                 (now, tbl, str(key), col, old, new, why))


def backup(db):
    dst = f"{db}.bak-{datetime.datetime.now():%Y%m%d-%H%M%S}"
    with sqlite3.connect(db) as src, sqlite3.connect(dst) as out:
        src.backup(out)
    return dst


def rebuild_from_nav(conn, etf_module, now):
    """以官方 NAV 重建A股實現報酬。etf_module:已載入的主程式模組(提供 TARGET_ETFS、_dated_nav、
    PerformanceAttributor._apply_nav_pairs)。回 predictions 變更筆數(逐筆寫入 data_corrections)。"""
    before = dict(conn.execute("SELECT rowid, actual_ret FROM predictions"))
    today = datetime.date.today().isoformat()
    first = conn.execute("SELECT MIN(date(ts)) FROM predictions").fetchone()[0] or today
    for code, info in etf_module.TARGET_ETFS.items():
        if info.get("market") != "A股":
            continue
        dn = sorted(x for x in (etf_module._dated_nav(code, limit=None) or []) if x[0] < today)
        # 保留紀錄起始日之前的最後一筆 NAV:第一個紀錄日的報酬需要它當分母
        start = max(0, next((i for i, x in enumerate(dn) if x[0] >= first), len(dn)) - 1)
        etf_module.PerformanceAttributor._apply_nav_pairs(conn, code, info, dn[start:], verbose=False)
    n = 0
    for pid, new in conn.execute("SELECT rowid, actual_ret FROM predictions"):
        old = before.get(pid)
        if old != new:
            _log(conn, "predictions", pid, "actual_ret", old, new, "以官方 NAV 重建實現報酬(src=nav)", now)
            n += 1
    return n


def run(db, apply=False, etf_module=None, out=print):
    now = datetime.datetime.now().isoformat(timespec="seconds")
    summary = {}
    with sqlite3.connect(db) as conn:
        has_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='realized_returns'").fetchone()
        empty = (not has_table) or not conn.execute("SELECT 1 FROM realized_returns LIMIT 1").fetchone()
        seed = returns_store.legacy_t0_consensus(conn) if empty else {}
        summary["seed"] = len(seed)
        ref = returns_store.load_map(conn)     # 未建表時即 legacy 共識,與步驟 1 寫入的內容相同
        closing = plan_closing_fixes(conn, ref)
        units = plan_unit_fixes(conn, ref)
        summary["closing"] = len(closing)
        summary["closing_t0"] = sum(1 for c in closing if c[3] == "T+0錯配")
        summary["closing_to_null"] = sum(1 for c in closing if c[2] is None)
        summary["units"] = len(units)
        if apply:
            conn.execute(CORRECTIONS_DDL)
            if "actual_src" not in {r[1] for r in conn.execute("PRAGMA table_info(predictions)")}:
                conn.execute("ALTER TABLE predictions ADD COLUMN actual_src TEXT")
            returns_store.seed_if_empty(conn)
            for pid, old, new, _cat, why in closing:
                if new is None:
                    conn.execute("UPDATE predictions SET actual_ret=NULL, error=NULL, corrected=0, actual_src=NULL "
                                 "WHERE rowid=?", (pid,))
                else:
                    conn.execute("UPDATE predictions SET actual_ret=?, error=COALESCE(predicted_ret,0)-?, "
                                 "corrected=1, actual_src='legacy' WHERE rowid=?", (new, new, pid))
                _log(conn, "predictions", pid, "actual_ret", old, new, why, now)
            for (code, d), old, new, why in units:
                conn.execute("UPDATE capital_flow_daily SET nav_ret=? WHERE code=? AND date=?", (new, code, d))
                _log(conn, "capital_flow_daily", f"{code}|{d}", "nav_ret", old, new, why, now)
            if etf_module is not None:
                summary["nav_rebuild"] = rebuild_from_nav(conn, etf_module, now)
    mode = "已寫入" if apply else "預覽(未寫入)"
    out(f"[{mode}] {db}")
    out(f"  1. realized_returns 以歷史 T+0 實現值建立:{summary['seed']} 筆" +
        ("" if summary["seed"] or empty else "(已存在,略過)"))
    out(f"  2. 收盤報 actual_ret 重新推導:{summary['closing']} 筆(其中 T+0 錯配 {summary['closing_t0']});"
        f"改配次一交易日 {summary['closing'] - summary['closing_to_null']},清為未回填 {summary['closing_to_null']}")
    out(f"  3. capital_flow_daily.nav_ret 小數→百分比:{summary['units']} 筆")
    if "nav_rebuild" in summary:
        out(f"  4. 以官方 NAV 重建:predictions 變更 {summary['nav_rebuild']} 筆")
    return summary


def _load_etf_module():
    import importlib.util
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "V24_19_ETF_System.py")
    spec = importlib.util.spec_from_file_location("etf_v24_migrate", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="realized_returns 遷移與歷史清洗")
    ap.add_argument("db")
    ap.add_argument("--apply", action="store_true", help="實際寫入(預設只預覽)")
    ap.add_argument("--rebuild-from-nav", action="store_true", help="以官方 NAV 重建A股歷史(需網路,需 --apply)")
    args = ap.parse_args()
    if args.rebuild_from_nav and not args.apply:
        ap.error("--rebuild-from-nav 需搭配 --apply")
    if args.apply:
        print(f"備份:{backup(args.db)}")
    run(args.db, apply=args.apply, etf_module=_load_etf_module() if args.rebuild_from_nav else None)
