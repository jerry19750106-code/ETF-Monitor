"""全新資料庫(由主程式 _init_db 建立的 predictions 沒有 id 欄)也要能載入紀錄。
回歸:2026-10-15 新環境收盤報出現「訊號可交易優勢 計算失敗: no such column: id」。"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import backtest_engine as be  # noqa: E402


def test_load_records_without_id_column(tmp_path):
    db = str(tmp_path / "fresh.db")
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE predictions (ts TEXT, code TEXT, actual_ret REAL, predicted_ret REAL,"
                  " market TEXT, report_type TEXT)")
        c.execute("INSERT INTO predictions VALUES ('2026-10-15 17:22:00','000216',-0.0034,0.0,'A','收盤報')")
    recs = be.load_records(db)
    assert len(recs) == 1 and recs[0].code == "000216" and recs[0].id == 1
