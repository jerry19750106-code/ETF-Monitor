"""私人倉庫專用:產生公開版快照並做洩漏掃描,任何個人資料命中即失敗。
tools/ 不納入公開版,因此公開版中本測試自動略過。"""
import importlib.util
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOL = os.path.join(ROOT, "tools", "make_public.py")


@pytest.mark.skipif(not os.path.exists(TOOL), reason="公開版不含 tools/")
def test_public_snapshot_has_no_personal_data(tmp_path):
    spec = importlib.util.spec_from_file_location("make_public", TOOL)
    mp = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mp)
    n, hits = mp.build(str(tmp_path / "public"))
    assert n > 30
    assert hits == []
    assert not os.path.exists(tmp_path / "public" / "src" / "personal_holdings.json")
