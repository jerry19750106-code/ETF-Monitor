"""盤前報「昨收前提 × 隔夜美股 × 產業新聞 → 今日盤勢預測」。

原缺陷:這段程式寫在 main() 的後段,但盤前報在前段已 return,永遠不會執行;
且引用了不存在的變數 ndx_ret / sox_ret(盤前報流程裡叫 ndx / sox),真執行也會 NameError。
"""
import ast
import importlib.util
import os
import sys
from unittest import mock

import pytest

MODULE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "src", "V24_19_ETF_System.py")


def _blocked_network_call(*args, **kwargs):
    raise AssertionError(f"測試不應發出網路請求: args={args!r}")


@pytest.fixture(scope="module")
def etf_mod(tmp_path_factory):
    env = {"ETF_OUTPUT_DIR": str(tmp_path_factory.mktemp("etf_output")),
           "EM_FORCE_IPV4": "0", "DD_AUTO_REFRESH": "0", "ETF_VERBOSE": "0"}
    with mock.patch.dict(os.environ, env), \
         mock.patch("requests.get", side_effect=_blocked_network_call), \
         mock.patch("requests.Session.get", side_effect=_blocked_network_call):
        spec = importlib.util.spec_from_file_location("etf_v24_premarket_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


def _codes(etf_mod):
    qdii = next(c for c, i in etf_mod.TARGET_ETFS.items() if etf_mod._is_qdii_etf(i))
    a = next(c for c, i in etf_mod.TARGET_ETFS.items()
             if i.get("market") == "A股" and not etf_mod._is_qdii_etf(i))
    return qdii, a


def _row(block, code, etf_mod):
    name = etf_mod.TARGET_ETFS[code]["name"][:10]
    return next(l for l in block.splitlines() if l.startswith(f"| {name} |"))


def test_forecast_combines_premise_overnight_and_news(etf_mod):
    qdii, a = _codes(etf_mod)
    premise = {qdii: {"t1_pred": 0.50, "conf": "中"}, a: {"t1_pred": -1.00, "conf": "低"}}
    with mock.patch.object(etf_mod, "compute_news_score", return_value=(0.0, "無重大新聞")):
        block = etf_mod.build_premarket_forecast(premise, "2026-10-14", ndx=0.02, sox=0.03, vix=17.5)
    # QDII:前提 +0.50 + NDX 2% 全額傳導 = +2.50
    assert "**+2.50%**" in _row(block, qdii, etf_mod)
    # A股:前提 -1.00 + SOX 3%×10%×0.3 = -0.91
    assert "**-0.91%**" in _row(block, a, etf_mod)
    assert "SOX +3.00% / NDX +2.00% / VIX 17.50" in block


def test_forecast_news_factor_applied(etf_mod):
    qdii, _ = _codes(etf_mod)
    with mock.patch.object(etf_mod, "compute_news_score", return_value=(0.5, "利多")):
        block = etf_mod.build_premarket_forecast({qdii: {"t1_pred": 0.0}}, "2026-10-14",
                                                 ndx=0.0, sox=0.0, vix=18.0, news_items=["x"])
    assert "**+0.40%**" in _row(block, qdii, etf_mod)      # 0.5 × 0.8


def test_forecast_flags_missing_overnight_data(etf_mod):
    qdii, _ = _codes(etf_mod)
    block = etf_mod.build_premarket_forecast({qdii: {"t1_pred": 0.3}}, "2026-10-14",
                                             ndx=0.0, sox=0.0, vix=18.0, unavailable=("NDX", "SOX"))
    assert "NDX" in block and "不可用" in block


def test_forecast_without_premise_is_explicit(etf_mod):
    block = etf_mod.build_premarket_forecast(None, None, ndx=0.01, sox=0.0, vix=18.0)
    assert "無昨日收盤 premise" in block


def test_premarket_report_includes_forecast_and_dead_block_removed():
    src = open(MODULE_PATH, encoding="utf-8").read()
    main_src = src[src.index("\ndef main():"):]
    pm = main_src[main_src.index('if r_type == "盤前報":\n        print("[情報官]'):]
    pm = pm[:pm.index("fn_pm = os.path.join")]
    assert "build_premarket_forecast(" in pm                 # 盤前報流程內真的呼叫
    main_fn = next(n for n in ast.parse(src).body if isinstance(n, ast.FunctionDef) and n.name == "main")
    loads = {n.id for n in ast.walk(main_fn) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
    assigned = {n.id for n in ast.walk(main_fn) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)}
    assert not ({"ndx_ret", "sox_ret"} & (loads - assigned))      # 不再讀取未定義變數
