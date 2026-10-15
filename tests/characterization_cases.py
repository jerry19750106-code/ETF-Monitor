"""任務C 特徵測試的輸入案例(重構前後共用)。

每個案例 = (案例 id, 函式名, builder);builder 在執行時回傳 (args, kwargs)。
與日期相關的輸入一律相對「今天」產生,讓 golden 值不隨執行日期漂移。
"""
import datetime


def _d(days_ago):
    return (datetime.date.today() - datetime.timedelta(days=days_ago)).isoformat()


def _a(*args, **kwargs):
    return lambda: (args, kwargs)


PRICES_OK = [10.0 + 0.1 * i for i in range(25)]
PRICES_BAD = [10.0, 0.0, -1.0, float("nan"), "x", None] + [10.0 + 0.1 * i for i in range(18)]
DATED_A = [("2026-09-01", 1.00), ("2026-09-02", 1.02), ("2026-09-03", 0.99), ("2026-09-04", 1.01)]
DATED_US = [("2026-08-31", 100.0), ("2026-09-01", 101.0), ("2026-09-02", 99.0), ("2026-09-03", 100.5)]
BD = {"主力": 1.2, "超大單": 0.8, "大單": 0.0, "中單": 0.0, "小單": -1.1}
DIAG = [
    {"code": "008888", "name": "芯片", "style": "Tech", "market": "A股", "exp": 0.012, "act": 0.018,
     "main": 0.9, "has_flow": True},
    {"code": "016185", "name": "電力", "style": "Utility", "market": "A股", "exp": None, "act": -0.004,
     "main": -0.3, "has_flow": True},
    {"code": "00929", "name": "台股科技", "style": "Tech", "market": "台股", "exp": 0.01, "act": 0.0,
     "main": None, "has_flow": False},
]

CASES = [
    ("pb_status_pos", "_pb_status_from_pct", _a(12.5)),
    ("pb_status_neg", "_pb_status_from_pct", _a(-8.0)),
    ("pb_parse_ok", "_pb_parse_float", _a(" 1,234.5 ")),
    ("pb_parse_bad", "_pb_parse_float", _a("abc", 7.0)),
    ("pb_parse_empty", "_pb_parse_float", _a("")),
    ("pb_diff_same", "positionbook_diff_audit", _a({"A": 100.0}, {"A": 100.4})),
    ("pb_diff_diff", "positionbook_diff_audit", _a({"A": 100.0, "B": 50.0}, {"A": 120.0, "C": 9.0})),
    ("pb_diff_ok_same", "positionbook_diff_audit", _a(
        {"A": {"buy": 100.0, "sell": 0.0, "current": 110.0, "total_pnl": 10.0}},
        {"A": {"buy": 100.4, "sell": 0.0, "current": 110.2, "total_pnl": 9.8}})),
    ("pb_diff_ok_diff", "positionbook_diff_audit", _a(
        {"A": {"buy": 100.0, "sell": 0.0, "current": 110.0, "total_pnl": 10.0},
         "B": {"buy": 50.0, "sell": 5.0, "current": 48.0, "total_pnl": 3.0}},
        {"A": {"buy": 120.0, "sell": 0.0, "current": 110.0, "total_pnl": -10.0},
         "C": {"buy": 9.0, "sell": 0.0, "current": 9.0, "total_pnl": 0.0}})),
    ("sanitize_ok", "sanitize_price_series", _a(PRICES_OK, "X", 20)),
    ("sanitize_bad", "sanitize_price_series", _a(PRICES_BAD, "X", 20)),
    ("ret_by_date", "_series_to_returns_by_date", _a(DATED_A)),
    ("align_overnight", "_align_overnight", _a(DATED_A, DATED_US)),
    ("fragility_calm", "compute_fragility", _a()),
    ("fragility_hot", "compute_fragility", _a(0.12, 0.05, True, 4, 78.0, 2.4)),
    ("fragility_act_low", "fragility_action", _a(20)),
    ("fragility_act_high", "fragility_action", _a(80)),
    ("now_utc8_type", "_now_utc8", _a()),
    ("state_actor_none", "identify_state_actor", _a()),
    ("state_actor_rescue", "identify_state_actor", _a(25.0, -1.5, 3.0, -2.0)),
    ("flow_behavior_1", "interpret_flow_behavior", _a(5.0, -1.2, 1.5)),
    ("flow_behavior_2", "interpret_flow_behavior", _a(-6.0, 2.0)),
    ("predict_default", "predict_tomorrow", _a()),
    ("predict_strong", "predict_tomorrow", _a(28.0, -2.1, -0.5, 2.0, 6.0, 30, -0.03, True, 0.5)),
    ("clamp_trend_in", "_clamp_snapshot_trend", _a(0.4, "X")),
    ("clamp_trend_out", "_clamp_snapshot_trend", _a(7.5, "X")),
    ("handoff_none", "format_closing_handoff", _a(None, None)),
    ("handoff_full", "format_closing_handoff", _a(
        {"sectors": {"Tech": 0.8, "Utility": -0.4}, "top": [("008888", 0.012)], "macro": {"vix": 22.0}},
        _d(1), live_news={"avg": 0.2, "n": 3})),
    ("us_date_mon", "_expected_latest_us_date", _a("2026-09-14")),
    ("us_date_wed", "_expected_latest_us_date", _a("2026-09-16")),
    ("pinned_ret", "compute_pinned_ret", _a(DATED_US, "2026-09-02")),
    ("pinned_ret_none", "compute_pinned_ret", _a([], None)),
    ("eff_sox_decoupled", "effective_sox_for", _a(0.1, 0.02)),
    ("eff_sox_coupled", "effective_sox_for", _a(0.9, 0.02)),
    ("qdii_yes", "_is_qdii_etf", _a({"fund_type": "QDII"})),
    ("qdii_no", "_is_qdii_etf", _a({"fund_type": "passive", "style": "Tech"})),
    ("morning_diag", "format_morning_diag_block", _a(DIAG)),
    ("morning_diag_empty", "format_morning_diag_block", _a([])),
    ("secid_sh", "_proxy_to_secid", _a("sh518880")),
    ("secid_sz", "_proxy_to_secid", _a("sz159995")),
    ("secid_bad", "_proxy_to_secid", _a("xx1")),
    ("order_flow_div", "judge_order_flow", _a(BD, -0.012)),
    ("order_flow_none", "judge_order_flow", _a(None)),
    ("rvol", "compute_realized_vol", _a([0.01, -0.02, 0.015, -0.005, 0.0, 0.02] * 5, 20)),
    ("rvol_short", "compute_realized_vol", _a([0.01], 20)),
    ("vscale", "vol_scale_factor", _a(0.3, 0.2)),
    ("vscale_none", "vol_scale_factor", _a(None, 0.2)),
    ("factor_attr", "factor_attribution", _a([0.01, -0.02, 0.015, 0.003, -0.007, 0.012, 0.004, -0.01, 0.006, 0.0],
                                              [0.008, -0.015, 0.01, 0.002, -0.005, 0.01, 0.003, -0.008, 0.005, 0.001])),
    ("auto_position", "_auto_position", _a("008888", {"tech": {"RSI": 32.0, "BIAS": -0.05}}, {"位置": "低檔"})),
    ("geo_lbl_energy", "_geo_lbl_for_style", _a("Utility", False, "🟡 有條件通航")),
    ("geo_lbl_tech", "_geo_lbl_for_style", _a("Tech", False, "🟡 有條件通航")),
    ("inline_md", "_inline_md_format", _a("**主攻** 🟢 +1.25% `008888` 🔴 -0.5%")),
    ("nth_weekday", "_ae_nth_weekday", _a(2026, 9, 2, 2)),
    ("adj_weekend", "_ae_adjust_weekend", _a(datetime.date(2026, 9, 12))),
    ("events_block", "format_events_block", _a([
        {"date": "2026-09-16", "time": "09:00", "event": "LPR", "impact": "All", "severity": "high"},
        {"date": "2026-09-17", "time": "02:00", "event": "FOMC", "impact": "Tech+QDII", "severity": "critical"}])),
    ("events_block_empty", "format_events_block", _a([])),
    ("trend3_up", "analyze_3day_trend", _a([0.01, 0.012, 0.015])),
    ("trend3_cat", "analyze_3day_trend", _a([-0.03, 0.01, -0.02])),
    ("trend3_short", "analyze_3day_trend", _a([0.01])),
    ("cny_track", "analyze_cny_dual_track", _a(7.25, 7.18, 7.10)),
    ("cny_track_min", "analyze_cny_dual_track", _a(7.0)),
    ("seesaw", "analyze_gold_oil_seesaw", _a(0.02, -0.03, 24.0)),
    ("seesaw_quiet", "analyze_gold_oil_seesaw", _a(0.001, 0.002, 15.0)),
    ("tech_conc_hi", "analyze_tech_concentration", _a({"Tech": 46.0, "QDII": 10.0})),
    ("tech_conc_lo", "analyze_tech_concentration", _a({"Tech": 20.0})),
    ("today_focus", "generate_today_focus", lambda: ((
        {"vix": 27.0, "sox": -0.045, "ndx": -0.02, "cny": {"level": "warn", "text": "CNY 弱"},
         "gold_oil": {"scenario": "避險", "text": "金漲油跌"}, "sox_trend": {}, "ndx_trend": {}},
        {"tech_concentration": {"level": "hi", "pct": 46.0}},
        [{"date": datetime.date.today().isoformat(), "event": "CPI", "severity": "critical", "impact": "All"}],
        {}), {})),
    ("today_focus_empty", "generate_today_focus", _a({}, {}, [], {})),
    ("pearson", "_pearson", _a([1, 2, 3, 4, 5], [2, 4, 5, 4, 5])),
    ("pearson_flat", "_pearson", _a([1, 1, 1], [1, 2, 3])),
    ("spearman", "_spearman", _a([1, 2, 3, 4, 5], [5, 6, 7, 8, 7])),
    ("pw_fresh", "_pw_freshness_tag", lambda: ((_d(3),), {})),
    ("pw_stale", "_pw_freshness_tag", lambda: ((_d(40),), {})),
    ("pw_bad", "_pw_freshness_tag", _a("not-a-date")),
    ("pw_disp_fresh", "_pw_qualitative_display", lambda: (("強勢突破", _d(2)), {})),
    ("pw_disp_stale", "_pw_qualitative_display", lambda: (("強勢突破", _d(60)), {})),
    ("act_sym_buy", "_action_to_symbol", _a("🟢 主攻 (強信號)")),
    ("act_sym_cut", "_action_to_symbol", _a("🔴 減倉")),
    ("act_sym_other", "_action_to_symbol", _a("觀望")),
    ("judgment_hit", "_compute_judgment", _a("🟢", 1.2)),
    ("judgment_miss", "_compute_judgment", _a("🔴", 0.8)),
    ("geo_icon_low", "_geo_icon_from_cdrain", _a(0.2)),
    ("geo_icon_high", "_geo_icon_from_cdrain", _a(0.5)),
    ("cdrain_calm", "derive_c_drain", _a(16.0, 70.0, 0.0, 0.0)),
    ("cdrain_stress", "derive_c_drain", _a(32.0, 105.0, 0.06, 0.025)),
    ("cdrain_manual_fresh", "derive_c_drain", lambda: ((16.0, 70.0, 0.0, 0.0), {"manual_override": 0.45, "override_as_of": _d(1)})),
    ("cdrain_manual_stale", "derive_c_drain", lambda: ((16.0, 70.0, 0.0, 0.0), {"manual_override": 0.45, "override_as_of": _d(9)})),
    ("cdrain_manual_nodate", "derive_c_drain", _a(16.0, 70.0, 0.0, 0.0, manual_override=0.45)),
]

# classify_position_status 多個門檻值
CASES = [c for c in CASES if c[0] != "pos_status_list"] + [
    (f"pos_status_{v}", "classify_position_status", _a(v)) for v in (-25.0, -10.0, -3.0, 0.0, 5.0, 15.0, 40.0)
]

# 執行結果含當下時間 → 只比對型別與合理範圍,不比對值
TIME_DEPENDENT = {"now_utc8_type"}
