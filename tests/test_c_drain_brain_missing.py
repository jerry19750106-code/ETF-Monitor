"""main() 主流程:brain.json 缺失/損毀時,c_drain 必須真的由 VIX/WTI/金價自動推導。

舊行為:`macro` 只在 brain.json 讀取的 try 區塊內賦值。檔案不存在(else 分支)或
損毀(except 分支)時 `macro` 從未定義 → 下方
`derive_c_drain(..., override_as_of=macro.get("c_drain_as_of"))` 必定拋
UnboundLocalError → 被 except 吞掉 → c_drain 一律是 0.20,不論當天 VIX/油價多高。
(2026-10-15 沙盒斷網實跑收盤報確認。)
"""
import importlib.util
import os
import sys
from unittest import mock

import pytest

MODULE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "src", "V24_19_ETF_System.py")
START_ANCHOR = "    # ── brain.json讀取 ───────────────────────────────\n"
END_ANCHOR = "\n    geo_status_label = {"


def _blocked_network_call(*args, **kwargs):
    raise AssertionError(f"import 階段不應發出網路請求: args={args!r}")


@pytest.fixture(scope="module")
def etf_mod(tmp_path_factory):
    env = {
        "ETF_OUTPUT_DIR": str(tmp_path_factory.mktemp("etf_output")),
        "EM_FORCE_IPV4": "0",
        "DD_AUTO_REFRESH": "0",
        "ETF_VERBOSE": "0",
    }
    with mock.patch.dict(os.environ, env), \
         mock.patch("requests.get", side_effect=_blocked_network_call), \
         mock.patch("requests.Session.get", side_effect=_blocked_network_call):
        spec = importlib.util.spec_from_file_location("etf_v24_cdrain_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


def _run_brain_and_cdrain_block(etf_mod, db_file, inputs):
    """原樣取出 main() 裡『brain.json讀取 → c_drain 推導』這一段真實原始碼執行。"""
    with open(MODULE_PATH, encoding="utf-8") as f:
        src = f.read()
    start = src.index(START_ANCHOR)
    end = src.index(END_ANCHOR, start)
    block = "\n".join(line[4:] for line in src[start:end].split("\n"))   # 去掉 main() 的一層縮排
    # 以主程式完整的模組全域環境執行(與 main() 實際執行時相同),再覆寫本測試的輸入
    ns = dict(vars(etf_mod))
    ns.update({"DB_FILE": db_file, **inputs})
    exec(compile(block, "<main:brain+c_drain>", "exec"), ns)
    return ns


# 高風險日:VIX 32、油價 +6%、金價 +2.5% —— 自動推導不可能剛好等於預設 0.20
STRESS = {"vix": 32.0, "wti": 105.0, "wti_ret": 0.06, "gold_ret": 0.025}


@pytest.mark.parametrize("case", ["missing", "corrupt"])
def test_c_drain_is_derived_when_brain_json_unusable(etf_mod, tmp_path, case):
    db_file = tmp_path / "v20_brain.json"
    if case == "corrupt":
        db_file.write_text("{not valid json", encoding="utf-8")

    expected, _ = etf_mod.derive_c_drain(**STRESS)
    assert expected != pytest.approx(0.20), "前提:這組壓力輸入的推導值必須不同於預設 0.20"

    ns = _run_brain_and_cdrain_block(etf_mod, str(db_file), STRESS)

    assert ns["c_drain"] == pytest.approx(expected), (
        f"brain.json {case} 時 c_drain 應由 VIX/WTI/金價推導為 {expected},"
        f"實得 {ns['c_drain']}(0.20 代表推導根本沒執行)"
    )
    assert ns["_cdrain_reason"], "推導成功時必須帶出推導理由"


def test_c_drain_with_valid_brain_json_is_unchanged(etf_mod, tmp_path):
    """回歸保護:brain.json 正常時,行為與修改前完全相同(含人工 c_drain 覆寫)。"""
    db_file = tmp_path / "v20_brain.json"
    db_file.write_text('{"macro": {"strait_status": "conditional_open"}, "kalman": {}}',
                       encoding="utf-8")
    expected, _ = etf_mod.derive_c_drain(**STRESS, manual_override=None, override_as_of=None)

    ns = _run_brain_and_cdrain_block(etf_mod, str(db_file), STRESS)

    assert ns["c_drain"] == pytest.approx(expected)
    assert ns["strait_status"] == "conditional_open"
