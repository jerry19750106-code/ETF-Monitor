"""actual_ret 回填的日期落位(主程式 _apply_actual_by_semantic / backfill_yesterday_actuals)。

三個會把報酬配到錯誤日期的缺陷:
  ① backfill_yesterday_actuals 以「日曆昨天」為日期:週一的昨天是週日;
  ② 且取 NAV 序列【最後兩筆】不看日期:NAV 延遲公布(QDII)或晚上跑時配錯日;
  ③ _apply_actual_by_semantic 把報酬配給「最近一筆未回填」的收盤報:前一日漏跑時配給更早的預測。
"""
import datetime
import importlib.util
import os
import sqlite3
import sys
from unittest import mock

import pytest

MODULE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "src", "V24_19_ETF_System.py")


def _blocked_network_call(*args, **kwargs):
    raise AssertionError(f"import 階段不應發出網路請求: args={args!r}")


@pytest.fixture(scope="module")
def etf_mod(tmp_path_factory):
    env = {"ETF_OUTPUT_DIR": str(tmp_path_factory.mktemp("etf_output")),
           "EM_FORCE_IPV4": "0", "DD_AUTO_REFRESH": "0", "ETF_VERBOSE": "0"}
    with mock.patch.dict(os.environ, env), \
         mock.patch("requests.get", side_effect=_blocked_network_call), \
         mock.patch("requests.Session.get", side_effect=_blocked_network_call):
        spec = importlib.util.spec_from_file_location("etf_v24_backfill_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


def _db(path, rows):
    """rows: (日期, code, report_type)。全部未回填。"""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE predictions(ts TEXT, code TEXT, report_type TEXT, predicted_ret REAL, "
                 "actual_ret REAL, corrected INTEGER DEFAULT 0, error REAL)")
    hhmm = {"早報": "09:59:00", "午報": "13:30:00", "收盤報": "17:37:00"}
    for d, code, rt in rows:
        conn.execute("INSERT INTO predictions(ts, code, report_type, predicted_ret) VALUES(?,?,?,0.01)",
                     (f"{d}T{hhmm[rt]}", code, rt))
    conn.commit()
    return conn


def _filled(conn):
    return {(d, rt): a for d, rt, a in conn.execute(
        "SELECT date(ts), report_type, actual_ret FROM predictions WHERE actual_ret IS NOT NULL")}


# 2026-09-14 = 週一,09-15 = 週二,09-16 = 週三,09-11 = 週五
def test_closing_report_filled_by_next_run_day(etf_mod, tmp_path):
    conn = _db(tmp_path / "a.db", [("2026-09-15", "X", "收盤報"), ("2026-09-16", "X", "早報")])
    etf_mod._apply_actual_by_semantic(conn, "X", "2026-09-16", 0.012)
    assert _filled(conn) == {("2026-09-15", "收盤報"): 0.012, ("2026-09-16", "早報"): 0.012}


def test_closing_report_friday_to_monday_is_filled(etf_mod, tmp_path):
    conn = _db(tmp_path / "b.db", [("2026-09-11", "X", "收盤報"), ("2026-09-14", "X", "早報")])
    etf_mod._apply_actual_by_semantic(conn, "X", "2026-09-14", -0.007)
    assert ("2026-09-11", "收盤報") in _filled(conn)          # 週末不算缺漏


def test_skipped_day_does_not_misalign_older_closing_report(etf_mod, tmp_path):
    """週一收盤報未回填、週二系統完全沒跑、週三實現 → 週一的預測對象是週二,不得配上週三的報酬。"""
    conn = _db(tmp_path / "c.db", [("2026-09-14", "X", "收盤報"), ("2026-09-16", "X", "早報")])
    etf_mod._apply_actual_by_semantic(conn, "X", "2026-09-16", 0.02)
    assert ("2026-09-14", "收盤報") not in _filled(conn)
    assert ("2026-09-16", "早報") in _filled(conn)            # T+0 照常


def test_closing_report_skipped_on_previous_run_day_is_not_backfilled_from_older(etf_mod, tmp_path):
    """週二有跑早報但沒跑收盤報、週一的收盤報未回填 → 週三的報酬不得配給週一。"""
    conn = _db(tmp_path / "d.db", [("2026-09-14", "X", "收盤報"), ("2026-09-15", "X", "早報"),
                                    ("2026-09-16", "X", "早報")])
    etf_mod._apply_actual_by_semantic(conn, "X", "2026-09-16", 0.02)
    assert ("2026-09-14", "收盤報") not in _filled(conn)


def _a_share_code(etf_mod):
    return next(c for c, i in etf_mod.TARGET_ETFS.items() if i.get("market") == "A股")


def test_backfill_yesterday_uses_nav_own_date_when_nav_is_lagged(etf_mod, tmp_path):
    """NAV 延遲公布:最新 NAV 是三天前 → 報酬必須落在三天前,不得被標成日曆昨天。"""
    code = _a_share_code(etf_mod)
    today = datetime.date.today()
    yday, d3, d4 = (today - datetime.timedelta(days=k) for k in (1, 3, 4))
    db = tmp_path / "e.db"
    conn = _db(db, [(yday.isoformat(), code, "早報"), (d3.isoformat(), code, "早報")])
    conn.close()
    dated = [(d4.isoformat(), 1.000), (d3.isoformat(), 1.010)]          # 最新 NAV 日 = d3
    with mock.patch.object(etf_mod, "LOG_DB", str(db)), \
         mock.patch.object(etf_mod, "TARGET_ETFS", {code: etf_mod.TARGET_ETFS[code]}), \
         mock.patch.object(etf_mod, "_dated_nav", return_value=dated), \
         mock.patch.object(etf_mod.DirectDataFetcher, "get_eastmoney_nav", return_value=[v for _, v in dated]), \
         mock.patch.object(etf_mod, "backfill_t1_actual"):
        etf_mod.PerformanceAttributor.backfill_yesterday_actuals()
    got = _filled(sqlite3.connect(db))
    assert got.get((d3.isoformat(), "早報")) == pytest.approx(0.01)
    assert (yday.isoformat(), "早報") not in got


def test_backfill_yesterday_ignores_todays_nav(etf_mod, tmp_path):
    """晚上跑、今天 NAV 已公布:今天的報酬不得被標成昨天。"""
    code = _a_share_code(etf_mod)
    today = datetime.date.today()
    yday, d2 = today - datetime.timedelta(days=1), today - datetime.timedelta(days=2)
    db = tmp_path / "f.db"
    _db(db, [(yday.isoformat(), code, "早報")]).close()
    dated = [(d2.isoformat(), 1.00), (yday.isoformat(), 1.02), (today.isoformat(), 0.99)]
    with mock.patch.object(etf_mod, "LOG_DB", str(db)), \
         mock.patch.object(etf_mod, "TARGET_ETFS", {code: etf_mod.TARGET_ETFS[code]}), \
         mock.patch.object(etf_mod, "_dated_nav", return_value=dated), \
         mock.patch.object(etf_mod.DirectDataFetcher, "get_eastmoney_nav", return_value=[v for _, v in dated]), \
         mock.patch.object(etf_mod, "backfill_t1_actual"):
        etf_mod.PerformanceAttributor.backfill_yesterday_actuals()
    assert _filled(sqlite3.connect(db)).get((yday.isoformat(), "早報")) == pytest.approx(0.02)
