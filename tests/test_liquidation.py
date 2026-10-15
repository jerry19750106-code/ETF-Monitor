"""全數清倉情境(2026-09 實際發生):
1) redeem_all 把部位歸零,不留「錨點值 − 估計贖回金額」的幽靈殘值;
2) 總資金為 0 時百分比計算不得 ZeroDivisionError(曾讓整份收盤報崩潰)。"""
import importlib.util
import os
import sqlite3
import sys
from unittest import mock

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODULE_PATH = os.path.join(ROOT, "src", "V24_19_ETF_System.py")


@pytest.fixture(scope="module")
def etf_mod(tmp_path_factory):
    env = {"ETF_OUTPUT_DIR": str(tmp_path_factory.mktemp("etf_output")),
           "EM_FORCE_IPV4": "0", "DD_AUTO_REFRESH": "0", "ETF_VERBOSE": "0"}
    blocked = mock.Mock(side_effect=AssertionError("測試不應發出網路請求"))
    with mock.patch.dict(os.environ, env), mock.patch("requests.get", blocked), \
         mock.patch("requests.Session.get", blocked):
        spec = importlib.util.spec_from_file_location("etf_v24_liquidation_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


def _db(path, txns):
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE position_book(code TEXT, name TEXT, buy REAL, sell REAL, current REAL,"
                  " status TEXT, anchor_date TEXT, updated_at TEXT)")
        c.execute("CREATE TABLE transactions(id INTEGER PRIMARY KEY, date TEXT, code TEXT, action TEXT,"
                  " shares REAL, nav REAL, amount REAL, created_at TEXT)")
        c.execute("INSERT INTO position_book VALUES ('AAA001','測試甲',1000,0,1200,'','2026-05-31','')")
        c.execute("INSERT INTO position_book VALUES ('BBB002','測試乙',4000,1000,3500,'','2026-05-31','')")
        c.executemany("INSERT INTO transactions(date,code,action,shares,nav,amount) VALUES (?,?,?,?,?,?)", txns)


def test_redeem_all_zeroes_position(etf_mod, tmp_path):
    db = str(tmp_path / "log.db")
    _db(db, [("2026-09-01", "AAA001", "redeem_all", 0, 0, 1111.11),   # 已知金額
             ("2026-09-11", "BBB002", "redeem_all", 0, 0, 0)])        # 金額未知 → 以帳面現值估
    with mock.patch.object(etf_mod, "LOG_DB", db):
        holdings, costs, _ = etf_mod.build_positionbook(str(tmp_path), {}, {})
    assert holdings["AAA001"] == 0 and holdings["BBB002"] == 0
    assert costs["AAA001"]["sell"] == pytest.approx(1111.11)
    assert costs["BBB002"]["sell"] == pytest.approx(1000 + 3500)


def test_plain_redeem_still_partial(etf_mod, tmp_path):
    db = str(tmp_path / "log.db")
    _db(db, [("2026-09-10", "BBB002", "redeem", 100, 2.5, 250.0)])
    with mock.patch.object(etf_mod, "LOG_DB", db):
        holdings, _, _ = etf_mod.build_positionbook(str(tmp_path), {}, {})
    assert holdings["BBB002"] == pytest.approx(3500 - 250.0)


def test_pct_of_capital_zero_capital(etf_mod):
    with mock.patch.object(etf_mod, "TOTAL_CAPITAL", 0):
        assert etf_mod._pct_of_capital(0) == 0.0
    with mock.patch.object(etf_mod, "TOTAL_CAPITAL", 200.0):
        assert etf_mod._pct_of_capital(50) == pytest.approx(25.0)
