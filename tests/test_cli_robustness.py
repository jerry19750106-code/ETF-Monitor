"""下載者第一次執行時會碰到的命令列狀況(以子程序實際執行主程式)。

原缺陷(公開版 10/15 全新下載稽核):
  - 輸出導向檔案/管線且主控台為 cp950/cp936(Windows 排程、`> log.txt`)時,啟動即 UnicodeEncodeError;
  - `--date 2026/10/14` 直接丟出 Traceback,而不是使用說明;
  - `--help` 會先花 30-90 秒連網抓盡調資料。
"""
import os
import subprocess
import sys
from unittest import mock

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODULE_PATH = os.path.join(ROOT, "src", "V24_19_ETF_System.py")


def _run(args, tmp_path, **env):
    full = dict(os.environ, ETF_OUTPUT_DIR=str(tmp_path), EM_FORCE_IPV4="0",
                DD_AUTO_REFRESH="0", ETF_VERBOSE="0", **env)
    full.pop("PYTHONUTF8", None)
    return subprocess.run([sys.executable, MODULE_PATH, *args], env=full, cwd=str(tmp_path),
                          capture_output=True, timeout=120)


@pytest.mark.parametrize("enc", ["cp950", "cp936"])
def test_runs_when_output_redirected_on_non_utf8_console(tmp_path, enc):
    p = _run(["--list-positions"], tmp_path, PYTHONIOENCODING=enc)
    assert p.returncode == 0, p.stderr.decode("utf-8", "replace")[-800:]
    assert "持倉" in p.stdout.decode("utf-8")


def test_bad_date_is_a_usage_error_not_a_traceback(tmp_path):
    p = _run(["--date", "2026/10/14"], tmp_path)
    err = p.stderr.decode("utf-8", "replace")
    assert p.returncode == 2
    assert "YYYY-MM-DD" in err and "Traceback" not in err


def test_bad_set_position_is_a_usage_error(tmp_path):
    p = _run(["--set-position", "004070"], tmp_path)
    assert p.returncode == 2
    assert "Traceback" not in p.stderr.decode("utf-8", "replace")


def test_help_does_not_trigger_dd_refresh(tmp_path):
    import importlib.util
    env = {"ETF_OUTPUT_DIR": str(tmp_path), "EM_FORCE_IPV4": "0", "DD_AUTO_REFRESH": "0"}
    blocked = mock.Mock(side_effect=AssertionError("測試不應發出網路請求"))
    with mock.patch.dict(os.environ, env), mock.patch("requests.get", blocked), \
         mock.patch("requests.Session.get", blocked):
        spec = importlib.util.spec_from_file_location("etf_v24_cli_test", MODULE_PATH)
        m = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = m
        spec.loader.exec_module(m)
    with mock.patch.dict(os.environ, {"DD_AUTO_REFRESH": "1"}), \
         mock.patch.object(sys, "argv", ["prog", "--help"]), \
         mock.patch("importlib.import_module", side_effect=AssertionError("不應載入 dd_fetcher")):
        assert "略過" in m._ensure_dd_auto()
