"""個人持倉不寫在原始碼裡:由 personal_holdings.json 載入,缺檔時為空。"""
import importlib.util
import json
import os
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
        spec = importlib.util.spec_from_file_location("etf_v24_personal_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


def test_missing_file_gives_empty(etf_mod, tmp_path):
    assert etf_mod._load_personal_holdings([str(tmp_path)]) == ({}, {}, None)


def test_example_file_format_loads(etf_mod, tmp_path):
    ex = json.load(open(os.path.join(ROOT, "src", "personal_holdings.example.json"), encoding="utf-8"))
    (tmp_path / "personal_holdings.json").write_text(json.dumps(ex, ensure_ascii=False), encoding="utf-8")
    h, c, src = etf_mod._load_personal_holdings([str(tmp_path)])
    assert h == ex["holdings"] and c == ex["costs"] and src.endswith("personal_holdings.json")


def test_corrupt_file_gives_empty_not_crash(etf_mod, tmp_path):
    (tmp_path / "personal_holdings.json").write_text("{not json", encoding="utf-8")
    assert etf_mod._load_personal_holdings([str(tmp_path)]) == ({}, {}, None)


def test_no_personal_amounts_or_paths_in_source():
    src = open(MODULE_PATH, encoding="utf-8").read()
    assert "_LEGACY_USER_HOLDINGS = {" not in src and "_LEGACY_USER_COSTS = {" not in src
    win_users = "C:" + "\\" + "Users"            # 拆開寫,避免本測試檔自己含有該字串
    assert win_users not in src and win_users.replace("\\", "\\\\") not in src
