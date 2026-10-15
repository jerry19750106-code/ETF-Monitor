"""實現報酬單一來源(realized_returns)與其下游。

修正的缺陷(每項都先證明在修改前會失敗):
  ① 回填「已回填就不覆寫」:收盤時場內代理先寫入,隔天官方 NAV 永遠補不進去 → A股實際報酬幾乎全是代理估計。
  ② NAV 回填只補最新一日:漏跑幾天就永遠缺。
  ③ 台股以「收盤序列最後兩筆」標成今天:休市日跑會配到錯日期。
  ④ A股場內代理在休市日/盤中仍寫入「今天的實現報酬」。
  ⑤ 收盤計分板、真前瞻 IC、因子歸因讀 daily_snapshot.today_ret —— 那是系統當日估值,不是實現報酬。
  ⑥ ic_conviction_multiplier 用早報同期 IC(含開盤後走勢)放大倉位。
  ⑦ 遷移:收盤報 T+0 錯配 280 筆、capital_flow_daily.nav_ret 單位混用。
"""
import datetime
import importlib.util
import json
import os
import sqlite3
import sys
import types
from unittest import mock

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
MODULE_PATH = os.path.join(SRC, "V24_19_ETF_System.py")
sys.path.insert(0, SRC)


def _blocked_network_call(*args, **kwargs):
    raise AssertionError(f"測試不應發出網路請求: args={args!r}")


@pytest.fixture(scope="module")
def etf_mod(tmp_path_factory):
    env = {"ETF_OUTPUT_DIR": str(tmp_path_factory.mktemp("etf_output")),
           "EM_FORCE_IPV4": "0", "DD_AUTO_REFRESH": "0", "ETF_VERBOSE": "0"}
    with mock.patch.dict(os.environ, env), \
         mock.patch("requests.get", side_effect=_blocked_network_call), \
         mock.patch("requests.Session.get", side_effect=_blocked_network_call):
        spec = importlib.util.spec_from_file_location("etf_v24_realized_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


HHMM = {"早報": "09:59:00", "午報": "13:30:00", "收盤報": "17:37:00"}


def _db(path, rows=()):
    """rows: (日期, code, report_type[, predicted_ret[, market]])。全部未回填。"""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE predictions(id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, code TEXT, "
                 "report_type TEXT, market TEXT DEFAULT 'A股', predicted_ret REAL, actual_ret REAL, "
                 "corrected INTEGER DEFAULT 0, error REAL)")
    for r in rows:
        d, code, rt = r[:3]
        pred = r[3] if len(r) > 3 else 0.01
        mkt = r[4] if len(r) > 4 else "A股"
        conn.execute("INSERT INTO predictions(ts, code, report_type, predicted_ret, market) VALUES(?,?,?,?,?)",
                     (f"{d}T{HHMM[rt]}", code, rt, pred, mkt))
    conn.commit()
    return conn


def _actual(conn, d, rt, code="X"):
    row = conn.execute("SELECT actual_ret FROM predictions WHERE date(ts)=? AND report_type=? AND code=?",
                       (d, rt, code)).fetchone()
    return row[0] if row else None


def _code(etf_mod, market):
    return next(c for c, i in etf_mod.TARGET_ETFS.items() if i.get("market") == market)


def _put_realized(conn, code, d, r, src="nav"):
    """直接以 SQL 寫入 realized_returns(不經 returns_store),讓舊版程式也能跑到同一份資料。"""
    conn.execute("CREATE TABLE IF NOT EXISTS realized_returns(code TEXT NOT NULL, date TEXT NOT NULL, "
                 "ret REAL NOT NULL, src TEXT NOT NULL, ts TEXT NOT NULL, PRIMARY KEY(code, date))")
    conn.execute("INSERT OR REPLACE INTO realized_returns VALUES(?,?,?,?,'t')", (code, d, r, src))


# ── returns_store ────────────────────────────────────────────────

def test_store_higher_rank_overrides_lower_never_reverse(tmp_path):
    import returns_store as rs
    conn = sqlite3.connect(tmp_path / "s.db")
    assert rs.upsert(conn, "X", "2026-09-15", 0.010, "proxy")
    assert rs.upsert(conn, "X", "2026-09-15", 0.012, "nav")          # NAV 覆蓋代理
    assert not rs.upsert(conn, "X", "2026-09-15", 0.020, "proxy")    # 代理不可蓋 NAV
    assert not rs.upsert(conn, "X", "2026-09-15", 0.013, "nav")      # 同級先寫為準
    assert rs.get(conn, "X", "2026-09-15") == (0.012, "nav")


def test_store_rejects_unknown_source_and_none(tmp_path):
    import returns_store as rs
    conn = sqlite3.connect(tmp_path / "s2.db")
    with pytest.raises(ValueError):
        rs.upsert(conn, "X", "2026-09-15", 0.01, "guess")
    with pytest.raises(ValueError):
        rs.upsert(conn, "X", "2026-09-15", None, "nav")


def test_seed_only_when_empty_so_later_proxy_is_not_relabelled(tmp_path):
    import returns_store as rs
    conn = _db(tmp_path / "s3.db", [("2026-09-15", "X", "早報")])
    conn.execute("UPDATE predictions SET actual_ret=0.01, corrected=1")
    assert rs.seed_if_empty(conn) == 1
    rs.upsert(conn, "X", "2026-09-16", 0.02, "proxy")
    conn.execute("INSERT INTO predictions(ts, code, report_type, predicted_ret, actual_ret, corrected) "
                 "VALUES('2026-09-16T09:59:00','X','早報',0.01,0.02,1)")
    assert rs.seed_if_empty(conn) == 0
    assert rs.get(conn, "X", "2026-09-16") == (0.02, "proxy")


# ── ① 來源等級:NAV 覆蓋代理(T+0 與 T+1 皆然),代理不可覆蓋 NAV ──────────

def test_nav_upgrades_proxy_filled_rows(etf_mod, tmp_path):
    conn = _db(tmp_path / "a.db", [("2026-09-15", "X", "收盤報"), ("2026-09-16", "X", "早報")])
    etf_mod._apply_actual_by_semantic(conn, "X", "2026-09-16", 0.010, src="proxy")
    etf_mod._apply_actual_by_semantic(conn, "X", "2026-09-16", 0.013, src="nav")
    assert _actual(conn, "2026-09-16", "早報") == pytest.approx(0.013)
    assert _actual(conn, "2026-09-15", "收盤報") == pytest.approx(0.013)
    src = {r[0] for r in conn.execute("SELECT actual_src FROM predictions")}
    assert src == {"nav"}


def test_proxy_never_overwrites_nav(etf_mod, tmp_path):
    conn = _db(tmp_path / "b.db", [("2026-09-16", "X", "早報")])
    etf_mod._apply_actual_by_semantic(conn, "X", "2026-09-16", 0.013, src="nav")
    etf_mod._apply_actual_by_semantic(conn, "X", "2026-09-16", 0.010, src="proxy")
    assert _actual(conn, "2026-09-16", "早報") == pytest.approx(0.013)


# ── ①② NAV 回填:代理已寫入仍要升級;漏跑的多日都要補 ──────────────────

def test_backfill_nav_upgrades_proxy_and_fills_missed_days(etf_mod, tmp_path):
    code = _code(etf_mod, "A股")
    today = datetime.date.today()
    d = [(today - datetime.timedelta(days=k)).isoformat() for k in (4, 3, 2, 1)]
    db = tmp_path / "c.db"
    conn = _db(db, [(d[1], code, "早報"), (d[2], code, "早報"), (d[3], code, "早報")])
    conn.execute("UPDATE predictions SET actual_ret=0.009, corrected=1 WHERE date(ts)=?", (d[3],))  # 收盤時代理先寫
    conn.commit(); conn.close()
    dated = [(d[0], 1.00), (d[1], 1.01), (d[2], 1.0302), (d[3], 1.0405)]
    with mock.patch.object(etf_mod, "LOG_DB", str(db)), \
         mock.patch.object(etf_mod, "TARGET_ETFS", {code: etf_mod.TARGET_ETFS[code]}), \
         mock.patch.object(etf_mod, "_dated_nav", return_value=dated), \
         mock.patch.object(etf_mod, "backfill_t1_actual"):
        etf_mod.PerformanceAttributor.backfill_yesterday_actuals()
    conn = sqlite3.connect(db)
    assert _actual(conn, d[3], "早報", code) == pytest.approx(1.0405 / 1.0302 - 1)   # 代理被 NAV 取代
    assert _actual(conn, d[1], "早報", code) == pytest.approx(0.01)                  # 漏跑日也補上
    assert _actual(conn, d[2], "早報", code) == pytest.approx(1.0302 / 1.01 - 1)


# ── ③ 台股:以收盤價自身日期落位,休市日不得標成今天 ─────────────────────

def test_taiwan_backfill_uses_close_dates_not_run_date(etf_mod, tmp_path):
    code = _code(etf_mod, "台股")
    now = datetime.datetime(2026, 9, 16, 18, 0)        # 週三晚上
    db = tmp_path / "d.db"
    _db(db, [("2026-09-15", code, "早報", 0.01, "台股"), ("2026-09-16", code, "早報", 0.01, "台股")]).close()
    closes = [("2026-09-14", 100.0), ("2026-09-15", 101.0)]   # 09-16 休市:最後一筆是 09-15
    with mock.patch.object(etf_mod, "LOG_DB", str(db)), \
         mock.patch.object(etf_mod, "TARGET_ETFS", {code: etf_mod.TARGET_ETFS[code]}), \
         mock.patch.object(etf_mod, "_now_utc8", return_value=now), \
         mock.patch.object(etf_mod.DirectDataFetcher, "get_yahoo_dated_closes", return_value=closes, create=True), \
         mock.patch.object(etf_mod.DirectDataFetcher, "get_yahoo_prices", return_value=[c for _, c in closes]):
        etf_mod.PerformanceAttributor.backfill_taiwan_today_actuals()
    conn = sqlite3.connect(db)
    assert _actual(conn, "2026-09-15", "早報", code) == pytest.approx(0.01)
    assert _actual(conn, "2026-09-16", "早報", code) is None


# ── ④ A股代理:休市日/盤中不寫入 ──────────────────────────────────

@pytest.mark.parametrize("now", [datetime.datetime(2026, 9, 19, 18, 0),    # 週六
                                 datetime.datetime(2026, 9, 16, 11, 0)])   # 週三盤中
def test_a_share_proxy_not_written_on_holiday_or_intraday(etf_mod, tmp_path, now):
    code = next(c for c, i in etf_mod.TARGET_ETFS.items()
                if i.get("market") == "A股" and i.get("proxy") and not etf_mod._is_qdii_etf(i))
    day = now.date().isoformat()
    db = tmp_path / f"e{now.hour}.db"
    _db(db, [(day, code, "早報")]).close()
    class _FrozenDate(datetime.date):
        @classmethod
        def today(cls):
            return now.date()
    _clock = types.SimpleNamespace(date=_FrozenDate, datetime=datetime.datetime,
                                   timedelta=datetime.timedelta, timezone=datetime.timezone)
    with mock.patch.object(etf_mod, "LOG_DB", str(db)), \
         mock.patch.object(etf_mod, "TARGET_ETFS", {code: etf_mod.TARGET_ETFS[code]}), \
         mock.patch.object(etf_mod, "_now_utc8", return_value=now), \
         mock.patch.object(etf_mod, "datetime", _clock), \
         mock.patch.object(etf_mod.DirectDataFetcher, "get_sina_proxy_ret", return_value=0.015):
        etf_mod.PerformanceAttributor.backfill_a_stock_today_actuals()
    assert _actual(sqlite3.connect(db), day, "早報", code) is None


# ── ⑤ 下游讀實現報酬,不讀 daily_snapshot.today_ret(系統估值) ───────────────

def _premise_db(path, n_days=12):
    """前提日 D 的 t1_pred(百分比)與 D+1 的實現報酬同號;daily_snapshot.today_ret 則存反號的估值。"""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE predictions(ts TEXT, code TEXT, report_type TEXT, actual_ret REAL)")
    conn.execute("CREATE TABLE morning_premise(date TEXT PRIMARY KEY, payload TEXT, ts TEXT)")
    conn.execute("CREATE TABLE daily_snapshot(code TEXT, date TEXT, today_ret REAL, PRIMARY KEY(code, date))")
    base = datetime.date(2026, 9, 1)
    days = [base + datetime.timedelta(days=i) for i in range(40) if (base + datetime.timedelta(days=i)).weekday() < 5]
    for i, (d, nd) in enumerate(zip(days[:n_days], days[1:n_days + 1])):
        prem = {}
        for j, code in enumerate(("A", "B")):
            r = (0.01 + 0.002 * j) * (1 if (i + j) % 2 == 0 else -1)
            prem[code] = {"t1_pred": r * 100, "conf": "中"}
            _put_realized(conn, code, nd.isoformat(), r)
            conn.execute("INSERT INTO daily_snapshot VALUES(?,?,?)", (code, nd.isoformat(), -r))
        conn.execute("INSERT INTO morning_premise VALUES(?,?,?)", (d.isoformat(), json.dumps(prem), "x"))
    conn.commit()
    return conn, days


def test_close_scorecard_scores_against_realized_not_snapshot_estimate(etf_mod, tmp_path):
    conn, days = _premise_db(tmp_path / "f.db")
    with mock.patch.object(etf_mod, "load_morning_premise",
                           side_effect=lambda c, d: json.loads(c.execute(
                               "SELECT payload FROM morning_premise WHERE date=?", (d,)).fetchone()[0])):
        txt = etf_mod.format_close_scorecard(conn, days[12].isoformat())
    assert "**20/20 = 100%**" in txt         # 10 個前提日 × 2 檔,全部命中


def test_forward_ic_uses_realized_returns(etf_mod, tmp_path):
    conn, days = _premise_db(tmp_path / "g.db")
    conn.close()
    with mock.patch.object(etf_mod, "LOG_DB", str(tmp_path / "g.db")), \
         mock.patch.object(etf_mod, "load_morning_premise",
                           side_effect=lambda c, d: json.loads(c.execute(
                               "SELECT payload FROM morning_premise WHERE date=?", (d,)).fetchone()[0])):
        res = etf_mod.compute_forward_ic()
    assert res["ic"] is not None and res["ic"] > 0.99


def test_factor_attribution_reads_realized_returns(etf_mod, tmp_path):
    db = tmp_path / "h.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE daily_snapshot(code TEXT, date TEXT, today_ret REAL)")
    codes = [c for c, i in etf_mod.TARGET_ETFS.items() if i.get("market") == "A股"][:9]
    for k in range(12):
        d = (datetime.date(2026, 8, 3) + datetime.timedelta(days=k)).isoformat()
        for j, c in enumerate(codes):
            _put_realized(conn, c, d, 0.001 * ((k * 7 + j * 3) % 11 - 5))
    conn.commit(); conn.close()                          # daily_snapshot 空:舊版必回 None
    with mock.patch.object(etf_mod, "LOG_DB", str(db)):
        out = etf_mod.build_factor_attr_block(codes[0])
    assert out and "因子歸因" in out


# ── ⑥ 倉位乘子用可交易 IC ───────────────────────────────────────────

def test_conviction_multiplier_ignores_contemporaneous_ic(etf_mod, tmp_path):
    """預測與【當日】實際完全相同(同期 IC=1),但與次日實現無關 → 不得給 1.15 上限乘子。"""
    db = tmp_path / "i.db"
    conn = _db(db)
    base = datetime.date.today() - datetime.timedelta(days=80)
    days = [base + datetime.timedelta(days=i) for i in range(80) if (base + datetime.timedelta(days=i)).weekday() < 5]
    for i, d in enumerate(days):
        for j, code in enumerate(("A", "B", "C")):
            same_day = 0.002 * (((i * 5 + j * 7) % 9) - 4) + 0.0003
            conn.execute("INSERT INTO predictions(ts, code, report_type, predicted_ret, actual_ret, corrected, market) "
                         "VALUES(?,?,?,?,?,1,'A股')", (f"{d}T09:59:00", code, "早報", same_day, same_day))
            _put_realized(conn, code, d.isoformat(), same_day)
    conn.commit(); conn.close()
    with mock.patch.object(etf_mod, "LOG_DB", str(db)), \
         mock.patch.dict(etf_mod._IC_CACHE, clear=True), \
         mock.patch.dict(etf_mod.__dict__.setdefault("_TRADABLE_IC_CACHE", {}), clear=True):
        mult = etf_mod.ic_conviction_multiplier("A股")
    assert mult < 1.15


# ── 訊號健康度閘門 ─────────────────────────────────────────────────

def test_signal_health_gate_marks_unendorsed_actions():
    import signal_health as sh
    health = {("收盤報", "A股"): {"n_days": 77, "daily_t": -1.45, "avg_pnl": -0.002,
                                  "status": sh.STATUS_UNPROVEN, "endorsed": False},
              ("午報", "A股"): {"n_days": 83, "daily_t": 2.0, "avg_pnl": 0.002,
                                "status": sh.STATUS_MARGINAL, "endorsed": True}}
    acts = [{"code": "X", "act": "賣出/減倉(T+1執行)", "user_optional": False}]
    sh.gate_actions(acts, health, "收盤報", lambda c: "A股")
    assert acts[0]["user_optional"] is True and "不背書" in acts[0]["act"]
    acts2 = [{"code": "X", "act": "賣出/減倉", "user_optional": False}]
    sh.gate_actions(acts2, health, "午報", lambda c: "A股")
    assert acts2[0]["user_optional"] is False


def test_signal_health_status_thresholds():
    import signal_health as sh
    assert sh.status_of(20, 5.0, 6) == sh.STATUS_INSUFFICIENT
    assert sh.status_of(80, 2.5, 6) == sh.STATUS_SIGNIFICANT     # > Bonferroni 2.39
    assert sh.status_of(80, 2.0, 6) == sh.STATUS_MARGINAL
    assert sh.status_of(80, -1.45, 6) == sh.STATUS_UNPROVEN
    assert sh.status_of(80, -2.0, 6) == sh.STATUS_NEGATIVE


def test_signal_health_on_sample_db_does_not_endorse_closing_report():
    import backtest_engine as be
    import signal_health as sh
    db = os.path.join(ROOT, "data_sample", "v20_prediction_log.db")
    h = sh.compute(be.load_records(db), be.load_reference(db), today=datetime.date(2026, 9, 30))
    assert not h[("收盤報", "A股")]["endorsed"]
    assert h[("收盤報", "A股")]["avg_pnl"] < 0


def test_closing_report_wording_states_t_plus_2():
    src = open(MODULE_PATH, encoding="utf-8").read()
    assert "明早盤前報可下單" not in src
    assert "後天(T+2)" in src


# ── ⑦ 遷移 ──────────────────────────────────────────────────────────

def _migration_db(path):
    conn = _db(path, [("2026-09-14", "X", "早報"), ("2026-09-14", "X", "收盤報"),
                      ("2026-09-15", "X", "早報"), ("2026-09-15", "X", "收盤報"),
                      ("2026-09-17", "X", "早報")])
    for d, a in (("2026-09-14", 0.010), ("2026-09-15", 0.020), ("2026-09-17", 0.030)):
        conn.execute("UPDATE predictions SET actual_ret=?, corrected=1 WHERE date(ts)=? AND report_type='早報'", (a, d))
    # 兩筆收盤報都被配成【當日】報酬(T+0 錯配);09-15 的次一交易日 09-16 系統沒跑 → 應清空
    conn.execute("UPDATE predictions SET actual_ret=0.010, corrected=1 WHERE date(ts)='2026-09-14' AND report_type='收盤報'")
    conn.execute("UPDATE predictions SET actual_ret=0.020, corrected=1 WHERE date(ts)='2026-09-15' AND report_type='收盤報'")
    conn.execute("CREATE TABLE capital_flow_daily(code TEXT, date TEXT, nav_ret REAL, PRIMARY KEY(code, date))")
    conn.execute("INSERT INTO capital_flow_daily VALUES('X','2026-09-14',0.010)")    # 小數(錯)
    conn.execute("INSERT INTO capital_flow_daily VALUES('X','2026-09-15',2.0)")      # 百分比(對)
    conn.commit(); conn.close()


def test_migration_dry_run_changes_nothing(tmp_path):
    import migrate_realized_returns as m
    db = str(tmp_path / "m1.db")
    _migration_db(db)
    before = open(db, "rb").read()
    s = m.run(db, apply=False, out=lambda *_: None)
    assert s["closing"] == 2 and s["closing_t0"] == 2 and s["units"] == 1
    assert open(db, "rb").read() == before


def test_migration_apply_fixes_and_is_idempotent(tmp_path):
    import migrate_realized_returns as m
    db = str(tmp_path / "m2.db")
    _migration_db(db)
    m.run(db, apply=True, out=lambda *_: None)
    conn = sqlite3.connect(db)
    assert _actual(conn, "2026-09-14", "收盤報") == pytest.approx(0.020)    # 改配次一交易日
    assert _actual(conn, "2026-09-15", "收盤報") is None                   # 次日缺漏 → 清空
    assert conn.execute("SELECT nav_ret FROM capital_flow_daily WHERE date='2026-09-14'").fetchone()[0] == pytest.approx(1.0)
    assert conn.execute("SELECT nav_ret FROM capital_flow_daily WHERE date='2026-09-15'").fetchone()[0] == pytest.approx(2.0)
    assert conn.execute("SELECT COUNT(*) FROM data_corrections").fetchone()[0] == 3
    assert conn.execute("SELECT COUNT(*) FROM realized_returns").fetchone()[0] == 3
    conn.close()
    s = m.run(db, apply=True, out=lambda *_: None)
    assert (s["closing"], s["units"], s["seed"]) == (0, 0, 0)


def test_migration_rebuild_from_nav_upgrades_legacy(etf_mod, tmp_path):
    import migrate_realized_returns as m
    code = _code(etf_mod, "A股")
    db = str(tmp_path / "m3.db")
    conn = _db(db, [("2026-09-14", code, "早報"), ("2026-09-15", code, "早報")])
    conn.execute("UPDATE predictions SET actual_ret=0.0123, corrected=1")    # 代理/來源不明的舊值
    conn.commit(); conn.close()
    dated = [("2026-09-11", 1.0), ("2026-09-14", 1.01), ("2026-09-15", 1.0201)]
    with mock.patch.object(etf_mod, "TARGET_ETFS", {code: etf_mod.TARGET_ETFS[code]}), \
         mock.patch.object(etf_mod, "_dated_nav", return_value=dated), \
         mock.patch.object(etf_mod, "backfill_t1_actual"):
        s = m.run(db, apply=True, etf_module=etf_mod, out=lambda *_: None)
    conn = sqlite3.connect(db)
    assert _actual(conn, "2026-09-14", "早報", code) == pytest.approx(0.01)
    assert _actual(conn, "2026-09-15", "早報", code) == pytest.approx(0.01)
    assert s["nav_rebuild"] == 2


def test_migration_on_sample_copy_removes_contamination(tmp_path):
    """真實樣本的副本:遷移後收盤報 T+0 錯配 = 0、nav_ret 不再混用單位、stored 與 semantic 口徑一致;再跑一次 0 變更。"""
    import shutil
    import backtest_engine as be
    import data_quality as dq
    import migrate_realized_returns as m
    src = os.path.join(ROOT, "data_sample", "v20_prediction_log.db")
    db = str(tmp_path / "sample.db")
    shutil.copy(src, db)
    s = m.run(db, apply=True, out=lambda *_: None)
    assert s["closing_t0"] == 280 and s["units"] == 8
    recs, ref = be.load_records(db), be.load_reference(db)
    assert [c for _, c in be.classify_closing_alignment(recs, ref)].count("T+0錯配") == 0
    days = be.trading_days(ref)
    closing = [r for r in recs if r.report_type == "收盤報" and r.actual is not None and abs(r.actual) > be.MIN_ABS_RET]
    assert closing
    # 每一筆仍有值的收盤報,actual_ret 都等於單一來源的次一交易日實現報酬
    assert all(abs(r.actual - be.outcome(r, "semantic", ref, days)) < be.MATCH_TOL for r in closing)
    assert "單位混用" not in dq.build_report(db)
    assert m.run(db, apply=True, out=lambda *_: None)["closing"] == 0


@pytest.mark.parametrize("fn,key", [("payoff", "SIGNAL_PAYOFF"), ("tradable_ic", "TRADABLE_IC")])
def test_read_failures_are_registered_not_silent(etf_mod, tmp_path, fn, key):
    """讀取失敗時退回最小倉位/中性乘子是合理的,但必須登記到「本次資料不可用」,不能看起來像真數據。"""
    import data_availability as da
    da.reset()
    bad = str(tmp_path)            # 目錄不是 SQLite 檔 → 連線/查詢失敗
    if fn == "payoff":
        etf_mod._get_signal_payoff(db=bad)
    else:
        etf_mod.compute_tradable_ic("A股", db=bad)
    assert da.is_unavailable(key)
    da.reset()


def test_holding_overlap_returns_pairs_instead_of_nameerror(etf_mod):
    """ruff F821 找到的既有缺陷:res 未初始化,有重疊時拋 NameError。"""
    out = etf_mod.compute_holding_overlap({"A": ["台積電", "鴻海"], "B": ["台積電", "聯發科"], "C": ["寧德時代"]})
    assert out == {("A", "B"): {"jaccard": 0.33, "common": ["台積電"]}}


def test_health_and_migration_work_on_db_created_by_main_program_schema(tmp_path):
    """主程式建立的 predictions 表沒有 id 欄位(樣本庫有,是舊版遺留)。
    使用者本機實跑:訊號健康度 OperationalError: no such column: id → 一律不背書。"""
    import backtest_engine as be
    import migrate_realized_returns as m
    import signal_health as sh
    db = str(tmp_path / "noid.db")
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE predictions(ts TEXT NOT NULL, code TEXT NOT NULL, report_type TEXT, "
                 "market TEXT, predicted_ret REAL, actual_ret REAL, corrected INTEGER DEFAULT 0, error REAL)")
    conn.execute("INSERT INTO predictions(ts,code,report_type,market,predicted_ret,actual_ret,corrected) "
                 "VALUES('2026-09-14T09:59:00','X','早報','A股',0.01,0.01,1)")
    conn.execute("INSERT INTO predictions(ts,code,report_type,market,predicted_ret,actual_ret,corrected) "
                 "VALUES('2026-09-14T17:37:00','X','收盤報','A股',0.01,0.01,1)")
    conn.execute("INSERT INTO predictions(ts,code,report_type,market,predicted_ret,actual_ret,corrected) "
                 "VALUES('2026-09-15T09:59:00','X','早報','A股',0.01,0.02,1)")
    conn.commit(); conn.close()
    recs = be.load_records(db)
    assert len(recs) == 3
    sh.compute(recs, be.load_reference(db), today=datetime.date(2026, 9, 30))
    s = m.run(db, apply=True, out=lambda *_: None)
    assert s["closing_t0"] == 1
    assert _actual(sqlite3.connect(db), "2026-09-14", "收盤報") == pytest.approx(0.02)


def test_hit_table_does_not_count_missing_prediction_as_hit(etf_mod, tmp_path):
    """使用者本機實跑:當日無早報預測 → pred 退回 0.0 → (ret>0)==(0>0) 使「所有下跌」都算命中(11/13=85%)。"""
    code = next(c for c, i in etf_mod.TARGET_ETFS.items()
                if i.get("market") == "A股" and i.get("proxy") and not etf_mod._is_qdii_etf(i))
    now = datetime.datetime(2026, 10, 15, 17, 30)
    db = tmp_path / "hit.db"
    _db(db).close()                                   # 當日沒有任何早報/午報預測
    with mock.patch.object(etf_mod, "LOG_DB", str(db)), \
         mock.patch.object(etf_mod, "TARGET_ETFS", {code: etf_mod.TARGET_ETFS[code]}), \
         mock.patch.object(etf_mod, "_now_utc8", return_value=now), \
         mock.patch.object(etf_mod.DirectDataFetcher, "get_sina_proxy_ret", return_value=-0.03):
        _n, rows = etf_mod.PerformanceAttributor.backfill_a_stock_today_actuals()
    assert rows and rows[0]["hit"] == "—"
