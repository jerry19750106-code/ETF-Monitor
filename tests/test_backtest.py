"""回測引擎與資料品質檢查(任務B)。全部使用人工構造的小型資料庫,逐條驗證規則。"""
import os
import sqlite3
import sys

import pytest

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
sys.path.insert(0, SRC)
import backtest_engine as be   # noqa: E402
import data_quality as dq      # noqa: E402

SAMPLE_DB = os.path.join(os.path.dirname(SRC), "data_sample", "v20_prediction_log.db")
HHMM = {"盤前報": "09:20:00", "早報": "09:59:00", "午報": "13:30:00", "收盤報": "17:37:00"}


def make_db(path, rows, snapshot=None, capital=None):
    """rows: (日期, code, report_type, predicted_ret, actual_ret[, 時間])"""
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE predictions(id INTEGER PRIMARY KEY, ts TEXT, code TEXT, market TEXT, "
                  "report_type TEXT, predicted_ret REAL, actual_ret REAL)")
        for r in rows:
            d, code, rt, p, a = r[:5]
            hhmm = r[5] if len(r) > 5 else HHMM.get(rt, "12:00:00")
            c.execute("INSERT INTO predictions(ts, code, market, report_type, predicted_ret, actual_ret) "
                      "VALUES(?,?,?,?,?,?)", (f"{d}T{hhmm}", code, "A股", rt, p, a))
        if snapshot is not None:
            c.execute("CREATE TABLE daily_snapshot(code TEXT, date TEXT, today_ret REAL)")
            c.executemany("INSERT INTO daily_snapshot VALUES(?,?,?)", snapshot)
        if capital is not None:
            c.execute("CREATE TABLE capital_flow_daily(code TEXT, date TEXT, nav_ret REAL)")
            c.executemany("INSERT INTO capital_flow_daily VALUES(?,?,?)", capital)
    return str(path)


# 三個交易日的 T+0 參考:9/1 +2%,9/2 −1%,9/3 +3%,9/4 −0.5%
REF_ROWS = [("2026-09-01", "X", "早報", 0.01, 0.02), ("2026-09-02", "X", "早報", 0.01, -0.01),
            ("2026-09-03", "X", "早報", 0.01, 0.03), ("2026-09-04", "X", "早報", 0.01, -0.005)]


def test_semantic_mode_splits_t0_same_day_and_t1_next_day(tmp_path):
    rows = REF_ROWS + [("2026-09-01", "X", "收盤報", -0.01, None)]   # 收盤報 9/1 看空 9/2
    recs = be.load_records(make_db(tmp_path / "a.db", rows))
    ref, _ = be.reference_returns(recs)
    days = be.trading_days(ref)
    early = next(r for r in recs if r.report_type == "早報" and r.date == "2026-09-01")
    close = next(r for r in recs if r.report_type == "收盤報")
    assert be.outcome(early, "semantic", ref, days) == pytest.approx(0.02)    # T+0:當日
    assert be.outcome(close, "semantic", ref, days) == pytest.approx(-0.01)   # T+1:次一交易日
    res = be.evaluate(recs, "semantic")
    assert res["收盤報"]["n"] == 1 and res["收盤報"]["direction_acc"] == 1.0


def test_tradable_mode_pre_cutoff_next_day_closing_two_days(tmp_path):
    rows = REF_ROWS + [("2026-09-01", "X", "收盤報", 0.01, None)]
    recs = be.load_records(make_db(tmp_path / "b.db", rows))
    ref, _ = be.reference_returns(recs)
    days = be.trading_days(ref)
    early = next(r for r in recs if r.report_type == "早報" and r.date == "2026-09-01")
    close = next(r for r in recs if r.report_type == "收盤報")
    assert be.outcome(early, "tradable", ref, days) == pytest.approx(-0.01)   # 截單前 → 次一交易日
    assert be.outcome(close, "tradable", ref, days) == pytest.approx(0.03)    # 截單後 → 次二交易日


def test_misaligned_closing_report_is_detected_and_inflates_stored_accuracy(tmp_path):
    """收盤報 actual_ret 被寫成【當日】報酬(V22.39 前的污染)→ 必須被抓出,且 stored 準確度虛高。"""
    rows = REF_ROWS + [
        ("2026-09-01", "X", "收盤報", +0.01, 0.02),    # 錯配:寫入的是 9/1 當日 +2%(預測看多 → 假命中)
        ("2026-09-02", "X", "收盤報", +0.01, 0.03),    # 正確:9/3 的 +3%
    ]
    recs = be.load_records(make_db(tmp_path / "c.db", rows))
    cats = {r.date: c for r, c in be.classify_closing_alignment(recs)}
    assert cats == {"2026-09-01": "T+0錯配", "2026-09-02": "T+1正確"}
    stored = be.evaluate(recs, "stored")["收盤報"]["direction_acc"]
    semantic = be.evaluate(recs, "semantic")["收盤報"]["direction_acc"]
    assert stored == 1.0 and semantic == 0.5        # 9/1 看多,真正的 9/2 是 −1% → 錯


def test_alignment_other_categories(tmp_path):
    rows = REF_ROWS + [
        ("2026-09-02", "X", "收盤報", 0.01, 0.0123),   # 與當日/次日參考都不同 → 來源不同
        ("2026-09-03", "X", "收盤報", 0.01, None),     # 未回填
        ("2026-09-04", "X", "收盤報", 0.01, 0.007),    # 無次日參考,當日也不同 → 來源不同(有當日參考)
        ("2026-09-04", "Y", "收盤報", 0.01, 0.007),    # Y 完全沒有參考 → 無參考
    ]
    recs = be.load_records(make_db(tmp_path / "d.db", rows))
    got = sorted((r.code, r.date, c) for r, c in be.classify_closing_alignment(recs))
    assert got == [("X", "2026-09-02", "來源不同無法判定"), ("X", "2026-09-03", "未回填"),
                   ("X", "2026-09-04", "來源不同無法判定"), ("Y", "2026-09-04", "無參考")]


def test_dedupe_excludes_blank_and_rerun_and_keeps_latest(tmp_path):
    rows = REF_ROWS + [
        ("2026-09-01", "X", "午報", -0.01, 0.02, "11:00:00"),
        ("2026-09-01", "X", "午報", +0.01, 0.02, "13:30:00"),   # 同日同報取最晚
        ("2026-09-01", "X", "", +0.01, 0.02),                   # 空白 report_type
        ("2026-09-01", "X", "早報(補跑)", -0.01, 0.02),           # 補跑
    ]
    recs = be.dedupe(be.load_records(make_db(tmp_path / "e.db", rows)))
    noon = [r for r in recs if r.report_type == "午報"]
    assert len(noon) == 1 and noon[0].predicted == 0.01
    assert all(r.report_type in be.REPORT_TYPES for r in recs)


def test_reference_conflicts_are_excluded(tmp_path):
    rows = [("2026-09-01", "X", "早報", 0.01, 0.02), ("2026-09-01", "X", "午報", 0.01, 0.025)]
    ref, conflicts = be.reference_returns(be.load_records(make_db(tmp_path / "f.db", rows)))
    assert ("X", "2026-09-01") in conflicts and ("X", "2026-09-01") not in ref


def test_max_drawdown_and_pure_direction():
    assert be.max_drawdown([0.10, -0.20, 0.05]) == pytest.approx(1 - 0.88 / 1.10)
    assert be.max_drawdown([0.01, 0.02]) == 0.0
    m = be._metrics([("d1", 0.0005, -0.01), ("d1", 0.02, 0.01), ("d2", -0.02, -0.01)])
    assert m["direction_acc"] == pytest.approx(2 / 3)
    assert m["pure_n"] == 2 and m["pure_direction_acc"] == 1.0   # |預測|<0.1% 的那筆不計入純方向
    assert m["n_days"] == 2


def test_data_quality_report_flags_misalignment_and_unit_mixing(tmp_path):
    rows = REF_ROWS + [("2026-09-01", "X", "收盤報", 0.01, 0.02)]
    capital = [("X", "2026-09-01", 2.0), ("X", "2026-09-02", -1.0), ("X", "2026-09-03", 0.03)]  # 前兩筆 %,第三筆小數
    db = make_db(tmp_path / "g.db", rows, snapshot=[("X", "2026-09-01", 0.02)], capital=capital)
    report = dq.build_report(db)
    assert "| **合計** | **0** | **1** |" in report               # 1 筆 T+0 錯配
    assert "capital_flow_daily.nav_ret 單位混用" in report
    assert "互相矛盾 0 組" in report


@pytest.mark.skipif(not os.path.exists(SAMPLE_DB), reason="無 data_sample")
def test_sample_db_baseline_contamination_visible():
    """真實樣本:收盤報 stored(含 V22.39 前污染)準確度必須明顯高於 semantic(正確落位)。"""
    recs = be.load_records(SAMPLE_DB)
    stored = be.evaluate(recs, "stored")["收盤報"]["direction_acc"]
    semantic = be.evaluate(recs, "semantic")["收盤報"]["direction_acc"]
    assert stored - semantic > 0.05
    cats = [c for _, c in be.classify_closing_alignment(recs)]
    assert cats.count("T+0錯配") > 0
