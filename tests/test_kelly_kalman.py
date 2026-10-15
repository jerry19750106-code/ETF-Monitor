"""Kelly 倉位輸入與 KalmanLearner W 更新 —— 改用可交易口徑。

背景(data_sample/v20_prediction_log.db 實測):早/午報 predicted_ret 已含當日盤中走勢,
對「當日」報酬的方向命中率 A股 80~94%,對「次一交易日」(場外基金下單後真正拿到的報酬)
僅約 50~54%。舊 Kelly 以前者為勝率、賠率寫死 1.5;舊 Kalman 以當日誤差為準且只會單向上升。
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
SCHEMA = ("CREATE TABLE predictions(id INTEGER PRIMARY KEY, ts TEXT, code TEXT, market TEXT, "
          "report_type TEXT, predicted_ret REAL, actual_ret REAL, corrected INTEGER, error REAL)")


def _blocked_network_call(*args, **kwargs):
    raise AssertionError(f"import 階段不應發出網路請求: args={args!r}")


@pytest.fixture(scope="module")
def etf_mod(tmp_path_factory):
    env = {"ETF_OUTPUT_DIR": str(tmp_path_factory.mktemp("etf_output")),
           "EM_FORCE_IPV4": "0", "DD_AUTO_REFRESH": "0", "ETF_VERBOSE": "0"}
    with mock.patch.dict(os.environ, env), \
         mock.patch("requests.get", side_effect=_blocked_network_call), \
         mock.patch("requests.Session.get", side_effect=_blocked_network_call):
        spec = importlib.util.spec_from_file_location("etf_v24_kelly_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


def _make_db(path, rows):
    """rows: (日期, code, market, report_type, predicted_ret, actual_ret)"""
    with sqlite3.connect(path) as c:
        c.execute(SCHEMA)
        hhmm = {"早報": "09:59:00", "午報": "13:30:00", "收盤報": "17:37:00"}
        for d, code, mkt, rt, p, a in rows:
            c.execute("INSERT INTO predictions(ts, code, market, report_type, predicted_ret, actual_ret, corrected) "
                      "VALUES(?,?,?,?,?,?,1)", (f"{d}T{hhmm.get(rt, '12:00:00')}", code, mkt, rt, p, a))
    return str(path)


# ═════════════════════════════ Kelly ═════════════════════════════
def test_kelly_requires_min_trading_days(etf_mod):
    k = etf_mod.KellyEngine.compute(80, 0.70, 0.02, 0.02, n_eff=etf_mod.SIGNAL_PAYOFF_MIN_DAYS - 1)
    assert k["half_kelly"] == 0.015 and "樣本不足" in k["label"]


def test_kelly_shrinks_win_rate_toward_no_skill_prior(etf_mod):
    n = etf_mod.SIGNAL_PAYOFF_MIN_DAYS
    k = etf_mod.KellyEngine.compute(100, 0.70, 0.02, 0.02, n_eff=n)
    shrunk = (0.70 * n + 0.50 * 10) / (n + 10)
    expected_raw = max(0.0, (shrunk * 1.0 - (1 - shrunk)) / 1.0)
    assert k["kelly_pct"] == pytest.approx(expected_raw, abs=1e-4)
    assert f"{shrunk:.1%}" in k["rationale"]


def test_kelly_no_edge_gives_zero(etf_mod):
    """勝率 50%、賠率 1:無優勢,Kelly 必須為 0(舊版以賠率寫死 1.5 會給出正倉位)。"""
    k = etf_mod.KellyEngine.compute(80, 0.50, 0.015, 0.015, n_eff=60)
    assert k["half_kelly"] == 0.0


def test_signal_payoff_uses_next_trading_day_not_same_day(etf_mod, tmp_path):
    rows = [
        # X:當日看(同期推估)全錯/全對都不算數,只看「截單前訊號 → 次一交易日」
        ("2026-09-01", "X", "A股", "早報", +0.010, +0.020),
        ("2026-09-01", "X", "A股", "午報", -0.010, +0.020),   # 午報優先:訊號為「空」
        ("2026-09-01", "X", "A股", "收盤報", +0.050, +0.020),  # 收盤報在截單後,不採用
        ("2026-09-02", "X", "A股", "早報", +0.010, -0.030),   # 9/1 空 × 9/2 跌 3% → +3%
        ("2026-09-03", "X", "A股", "早報", +0.010, +0.010),   # 9/2 多 × 9/3 漲 1% → +1%
        ("2026-09-03", "X", "A股", "早報(補跑)", -0.500, +0.010),   # 補跑不計
        # Y:間隔超過 7 日不配對(系統停跑期間)
        ("2026-09-01", "Y", "A股", "午報", +0.010, +0.010),
        ("2026-09-15", "Y", "A股", "午報", +0.010, -0.050),
    ]
    db = _make_db(tmp_path / "p.db", rows)
    res = etf_mod._get_signal_payoff(window_days=90, today=datetime.date(2026, 9, 30), db=db)["A股"]
    assert res["n"] == 2 and res["n_days"] == 2
    assert res["win_rate"] == pytest.approx(1.0)
    assert res["avg_win"] == pytest.approx(0.02)
    assert res["avg_loss"] is None


def test_signal_payoff_failure_returns_empty_not_fabricated(etf_mod, tmp_path):
    res = etf_mod._get_signal_payoff(db=str(tmp_path / "no_such_dir" / "x.db"))
    assert res["A股"]["n"] == 0 and res["A股"]["win_rate"] is None
    k = etf_mod.KellyEngine.compute(80, res["A股"]["win_rate"], res["A股"]["avg_win"],
                                    res["A股"]["avg_loss"], n_eff=res["A股"]["n_days"])
    assert k["half_kelly"] == 0.015       # 退回最小倉位,而非憑空 60% 勝率


def test_main_kelly_uses_tradable_payoff():
    """防漏:main() 的 Kelly 不得再用 mae×1.5 / 預設 0.6 勝率。"""
    with open(MODULE_PATH, encoding="utf-8") as f:
        src = f.read()
    main_src = src[src.index("\ndef main():"):]
    assert "mae*1.5" not in main_src
    assert 'mkt_acc.get("accuracy", 0.6)' not in main_src
    assert "payoff = _get_signal_payoff()" in main_src
    assert 'n_eff=_po.get("n_days", 0)' in main_src


# ═════════════════════════════ Kalman ═════════════════════════════
def _kalman_rows(etf_mod, n_days, aligned):
    """建 n_days 個連續交易日資料。實際報酬正負交替;每日訊號方向 = 當日實際方向(同期推估全對,
    且數值極接近 → 舊規則誤差極小)。aligned=False:訊號對次日全錯;True:改成對次日全對。"""
    codes = [c for c, i in etf_mod.TARGET_ETFS.items() if i.get("market") == "A股"][:3]
    today = datetime.date.today()
    days = [today - datetime.timedelta(days=n_days - 1 - i) for i in range(n_days)]
    acts = [0.012 if i % 2 == 0 else -0.012 for i in range(n_days)]
    rows = []
    for i, d in enumerate(days):
        for code in codes:
            if aligned:
                pred = acts[i + 1] if i + 1 < n_days else acts[i]
                pred = pred * 0.9
            else:
                pred = acts[i] * 0.9          # 同期幾乎完全準,但次日方向相反
            rows.append((d.isoformat(), code, "A股", "午報", pred, acts[i]))
    return rows


def _run_kalman(etf_mod, db, w0=0.531):
    brain = {"kalman": {"A股": {"W": w0}, "台股": {"W": 0.481}}}
    with mock.patch.object(etf_mod, "LOG_DB", db):
        out = etf_mod.KalmanLearner.update(brain, [])
    return out["kalman"]["A股"]["W"]


def test_kalman_decreases_when_signals_wrong_for_next_day(etf_mod, tmp_path):
    """同期推估幾乎全對(舊規則誤差極小→W 上升),但對次一交易日全錯 → W 必須下降。"""
    db = _make_db(tmp_path / "k1.db", _kalman_rows(etf_mod, 6, aligned=False))
    assert _run_kalman(etf_mod, db) < 0.531


def test_kalman_increases_when_signals_right_for_next_day(etf_mod, tmp_path):
    db = _make_db(tmp_path / "k2.db", _kalman_rows(etf_mod, 6, aligned=True))
    assert _run_kalman(etf_mod, db) > 0.531


def test_kalman_no_update_below_min_samples(etf_mod, tmp_path):
    db = _make_db(tmp_path / "k3.db", _kalman_rows(etf_mod, 2, aligned=True))   # 1 個轉換 × 3 檔 = 3 對 < 10
    assert _run_kalman(etf_mod, db) == 0.531
