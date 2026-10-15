"""冒煙測試:主程式在斷網環境可完整 import,且模組層級不發出任何網路請求。"""
import importlib.util
import os
import sys
from unittest import mock

MODULE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "src", "V24_19_ETF_System.py")


def test_module_imports_without_network(tmp_path):
    calls = []

    def _record(*args, **kwargs):
        calls.append(args)
        raise AssertionError("import 階段不應發出網路請求")

    env = {"ETF_OUTPUT_DIR": str(tmp_path), "EM_FORCE_IPV4": "0", "DD_AUTO_REFRESH": "0"}
    with mock.patch.dict(os.environ, env), \
         mock.patch("requests.get", side_effect=_record), \
         mock.patch("requests.Session.get", side_effect=_record), \
         mock.patch("requests.post", side_effect=_record):
        spec = importlib.util.spec_from_file_location("etf_v24_smoke", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

    assert calls == []
    assert module.__VERSION__.startswith("V24.")
    assert callable(module.main)
