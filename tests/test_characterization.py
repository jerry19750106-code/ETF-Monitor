"""任務C 特徵測試(characterization tests):重構前以現有程式碼跑出固定輸入的輸出,
存成 tests/characterization_golden.json;重構後必須逐案例完全相同。

另外記錄每個被搬移函式的原始碼 SHA-256,證明搬移後函式本體【字元級一致】。

重新產生 golden(只能在重構前、行為正確的版本上做):
    python tests/test_characterization.py --regen
"""
import datetime
import hashlib
import importlib.util
import inspect
import json
import os
import sys
from unittest import mock

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from characterization_cases import CASES, TIME_DEPENDENT  # noqa: E402

MODULE_PATH = os.path.join(os.path.dirname(HERE), "src", "V24_19_ETF_System.py")
GOLDEN_PATH = os.path.join(HERE, "characterization_golden.json")


def _blocked_network_call(*args, **kwargs):
    raise AssertionError(f"import 階段不應發出網路請求: args={args!r}")


def load_module(out_dir):
    env = {"ETF_OUTPUT_DIR": str(out_dir), "EM_FORCE_IPV4": "0", "DD_AUTO_REFRESH": "0", "ETF_VERBOSE": "0"}
    with mock.patch.dict(os.environ, env), \
         mock.patch("requests.get", side_effect=_blocked_network_call), \
         mock.patch("requests.Session.get", side_effect=_blocked_network_call):
        spec = importlib.util.spec_from_file_location("etf_v24_characterization", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


def run_case(mod, func_name, builder):
    args, kwargs = builder()
    try:
        return {"kind": "ok", "value": repr(getattr(mod, func_name)(*args, **kwargs))}
    except Exception as e:  # 例外本身也是要固定的行為
        return {"kind": "raise", "value": type(e).__name__}


def source_sha(mod, func_name):
    return hashlib.sha256(inspect.getsource(getattr(mod, func_name)).encode("utf-8")).hexdigest()


@pytest.fixture(scope="module")
def etf_mod(tmp_path_factory):
    return load_module(tmp_path_factory.mktemp("etf_output"))


@pytest.fixture(scope="module")
def golden():
    with open(GOLDEN_PATH, encoding="utf-8") as f:
        return json.load(f)


@pytest.mark.parametrize("case_id,func_name,builder", CASES, ids=[c[0] for c in CASES])
def test_output_unchanged(etf_mod, golden, case_id, func_name, builder):
    got = run_case(etf_mod, func_name, builder)
    if case_id in TIME_DEPENDENT:
        args, kwargs = builder()
        value = getattr(etf_mod, func_name)(*args, **kwargs)
        assert isinstance(value, datetime.datetime)
        assert abs((value - datetime.datetime.now()).total_seconds()) < 86400
        return
    assert got == golden["cases"][case_id]


@pytest.mark.parametrize("func_name", sorted({c[1] for c in CASES}))
def test_function_source_byte_identical(etf_mod, golden, func_name):
    """函式本體(含 def 行與 docstring)SHA-256 與重構前完全相同。"""
    assert source_sha(etf_mod, func_name) == golden["sources"][func_name]


def test_every_moved_function_still_reachable_from_main_module(etf_mod, golden):
    """外部介面不變:所有函式仍可由主程式模組以原名取得。"""
    for name in golden["sources"]:
        assert callable(getattr(etf_mod, name)), name


if __name__ == "__main__" and "--regen" in sys.argv:
    import tempfile
    mod = load_module(tempfile.mkdtemp())
    data = {
        "generated_from": "pre-refactor V24_19_ETF_System.py",
        "cases": {cid: run_case(mod, fn, b) for cid, fn, b in CASES if cid not in TIME_DEPENDENT},
        "sources": {fn: source_sha(mod, fn) for fn in sorted({c[1] for c in CASES})},
    }
    with open(GOLDEN_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1, sort_keys=True)
    raised = [cid for cid, v in data["cases"].items() if v["kind"] == "raise"]
    print(f"golden 寫入 {GOLDEN_PATH}:{len(data['cases'])} 案例、{len(data['sources'])} 個函式;拋例外的案例:{raised}")
