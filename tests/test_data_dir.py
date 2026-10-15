"""直接執行主程式時,輸出/資料夾的決定方式。

原缺陷(使用者 10/15 實跑):未設 ETF_OUTPUT_DIR 時資料夾 = 程式所在資料夾的上一層;
程式放在 ...\\ETF_Monitor_Repo\\src\\ 時,那裡沒有歷史資料庫 → 報告從空白歷史開始
(持倉 0 檔、IC 0 筆、精度 N/A),且報告中沒有任何一行說明原因。
"""
import importlib.util
import os
import sys
from unittest import mock

import pytest

MODULE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "src", "V24_19_ETF_System.py")


@pytest.fixture(scope="module")
def etf_mod(tmp_path_factory):
    env = {"ETF_OUTPUT_DIR": str(tmp_path_factory.mktemp("etf_output")),
           "EM_FORCE_IPV4": "0", "DD_AUTO_REFRESH": "0", "ETF_VERBOSE": "0"}
    blocked = mock.Mock(side_effect=AssertionError("測試不應發出網路請求"))
    with mock.patch.dict(os.environ, env), mock.patch("requests.get", blocked), \
         mock.patch("requests.Session.get", blocked):
        spec = importlib.util.spec_from_file_location("etf_v24_datadir_test", MODULE_PATH)
        m = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = m
        spec.loader.exec_module(m)
    return m


def _mk(d, with_db):
    d.mkdir(parents=True, exist_ok=True)
    if with_db:
        (d / "v20_prediction_log.db").write_bytes(b"")
    return str(d)


def test_env_var_always_wins(etf_mod, tmp_path):
    env_dir = _mk(tmp_path / "env", False)
    parent = _mk(tmp_path / "repo", True)
    d, why = etf_mod._resolve_base_dir(env=env_dir, script_parent=parent, home=str(tmp_path / "home"))
    assert d == env_dir and "ETF_OUTPUT_DIR" in why


def test_desktop_data_folder_found_when_repo_has_no_db(etf_mod, tmp_path):
    parent = _mk(tmp_path / "ETF_Monitor_Repo", False)
    desk = _mk(tmp_path / "home" / "Desktop" / "ETF_Monitor", True)
    d, why = etf_mod._resolve_base_dir(env=None, script_parent=parent, home=str(tmp_path / "home"))
    assert d == desk and "桌面" in why


def test_script_parent_preferred_when_it_has_db(etf_mod, tmp_path):
    parent = _mk(tmp_path / "ETF_Monitor", True)                    # 作者配置:ETF_Monitor\代碼\本檔
    _mk(tmp_path / "home" / "Desktop" / "ETF_Monitor", True)
    d, _ = etf_mod._resolve_base_dir(env=None, script_parent=parent, home=str(tmp_path / "home"))
    assert d == parent


def test_no_db_anywhere_falls_back_and_says_so(etf_mod, tmp_path):
    parent = _mk(tmp_path / "repo", False)
    d, why = etf_mod._resolve_base_dir(env=None, script_parent=parent, home=str(tmp_path / "home"))
    assert d == parent and "尚無歷史資料庫" in why


def test_empty_history_is_registered_in_report(etf_mod):
    import data_availability as da
    da.reset()
    etf_mod._register_history_status(existed=False, base_dir="X:/somewhere")
    assert da.is_unavailable("DATA_HISTORY") and "X:/somewhere" in da.get_reason("DATA_HISTORY")
    da.reset()
    etf_mod._register_history_status(existed=True, base_dir="X:/somewhere")
    assert not da.is_unavailable("DATA_HISTORY")


def test_main_registers_history_after_reset():
    src = open(MODULE_PATH, encoding="utf-8").read()
    main_src = src[src.index("\ndef main():"):]
    i = main_src.index("data_availability.reset()")
    assert "_register_history_status(" in main_src[i:i + 600]
