"""IntelTeam.get_market_sentiment_v14 —— 南向端點失敗不得讓整個函式回 None。

舊行為:南向資金(kamt)端點回 None 時,函式在第一個 try 內 `return None`,
另外兩個獨立資料源(成交額、主力淨流入)完全不會嘗試;main() 隨即對 None
呼叫 `.get('summary')` → 早/午/收盤報整份崩潰(2026-10-15 沙盒斷網實跑重現)。
"""
import importlib.util
import os
import sys
from unittest import mock

import pytest

MODULE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "src", "V24_19_ETF_System.py")


def _blocked_network_call(*args, **kwargs):
    raise AssertionError(f"import 階段不應發出網路請求: args={args!r}")


@pytest.fixture(scope="module")
def etf_mod(tmp_path_factory):
    env = {
        "ETF_OUTPUT_DIR": str(tmp_path_factory.mktemp("etf_output")),
        "EM_FORCE_IPV4": "0",    # 跳過模組層級的東財即時探測
        "DD_AUTO_REFRESH": "0",  # 跳過 _ensure_dd_auto() 的批量網路抓取
        "ETF_VERBOSE": "0",
    }
    with mock.patch.dict(os.environ, env), \
         mock.patch("requests.get", side_effect=_blocked_network_call), \
         mock.patch("requests.Session.get", side_effect=_blocked_network_call):
        spec = importlib.util.spec_from_file_location("etf_v24_sentiment_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _fake_em_get(url, **kwargs):
    if "kamt/get" in url:                       # 南向:失敗(限流/熔斷/斷網)
        return None
    if "clist/get" in url:                      # 成交額:成功
        return _Resp({"data": {"diff": [{"f6": 5.0e11}, {"f6": 4.0e11}]}})
    if "fflow/kline" in url:                    # 主力淨流入:成功
        return _Resp({"data": {"klines": ["2026-10-14,1000", "2026-10-15,12340000000"]}})
    raise AssertionError(f"未預期的 URL: {url}")


def test_south_flow_failure_still_returns_dict_and_tries_other_sources(etf_mod):
    with mock.patch.object(etf_mod, "_em_throttled_get", side_effect=_fake_em_get):
        res = etf_mod.IntelTeam.get_market_sentiment_v14()

    assert isinstance(res, dict), "南向失敗不得讓整個函式回 None(main() 會對 None 呼叫 .get 崩潰)"
    assert "失敗" in res["south_flow"]
    assert "south_total" not in res["raw"]           # 不得捏造南向數值
    assert res["raw"]["amzn_total"] == pytest.approx(9000.0)   # 其餘兩源照常抓取
    assert res["raw"]["main_force"] == pytest.approx(123.4)
    assert "成交" in res["summary"] and "主力" in res["summary"]


def test_all_sources_failing_returns_honest_failure_summary(etf_mod):
    with mock.patch.object(etf_mod, "_em_throttled_get", return_value=None):
        res = etf_mod.IntelTeam.get_market_sentiment_v14()

    assert isinstance(res, dict)
    assert res["raw"] == {}
    assert "失敗" in res["summary"]   # 誠實標示採集失敗,非偽造數值
