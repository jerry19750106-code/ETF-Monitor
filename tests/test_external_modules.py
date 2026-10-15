"""外部模組:patch_reversal_criteria / news_fetcher / dd_fetcher / funddb_client(公開版介面)。
全部離線:網路呼叫一律以 monkeypatch 取代。"""
import csv
import importlib.util
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, SRC)

import dd_fetcher  # noqa: E402
import news_fetcher  # noqa: E402
import patch_reversal_criteria as prc  # noqa: E402


def _funddb_public():
    """公開版介面檔:公開倉庫在 src/,私人倉庫在 public_overrides/src/。"""
    for p in (os.path.join(ROOT, "public_overrides", "src", "funddb_client.py"),
              os.path.join(SRC, "funddb_client.py")):
        if os.path.exists(p):
            spec = importlib.util.spec_from_file_location("funddb_client_public", p)
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            return m
    pytest.skip("找不到 funddb_client 公開版介面檔")


# ── patch_reversal_criteria ──────────────────────────────────────

def test_reversal_729_case_is_rotation_bounce():
    r = prc.assess_reversal(broad_ret=0.0040, core_ret=-0.0114, core_flow5=-1.0)
    assert r["verdict"] == "輪動反抽·非反轉" and "接刀子" in r["warning"]


def test_reversal_all_three_conditions_confirm():
    r = prc.assess_reversal(broad_ret=0.01, core_ret=0.002, core_flow5=1.5,
                            idx_price=101, idx_ma10=100, today_low=95, prev_trough_low=94)
    assert r["verdict"] == "反轉確認" and r["missing"] == []


def test_reversal_missing_inputs_never_confirm():
    r = prc.assess_reversal()
    assert r["verdict"] == "無反抽訊號(大盤未翻紅)" and len(r["missing"]) == 3


# ── funddb_client 公開版 ─────────────────────────────────────────

def test_funddb_public_returns_none_not_fake_data():
    fc = _funddb_public()
    s, meta = fc.fetch_index_pe_history_funddb_meta("国证芯片")
    assert s is None and meta["status"] == "not_implemented"
    assert fc.fetch_index_pe_history_funddb("中证消费电子50") is None
    assert fc.resolve_alias("中证消费电子50") == ("消费电子", True)
    assert fc.fetch_index_pe_history_funddb_meta("不存在")[1]["status"] == "no_alias"


def test_funddb_public_has_no_network_or_signing_code():
    fc = _funddb_public()
    src = open(fc.__file__, encoding="utf-8").read()
    assert "import requests" not in src and "urllib" not in src and "hashlib" not in src


# ── news_fetcher ────────────────────────────────────────────────

def test_news_classify():
    assert news_fetcher._classify("中际旭创光模块订单大增") == "光通信"
    assert news_fetcher._classify("今天天气很好") is None


def test_news_csv_written_once_per_day(tmp_path, monkeypatch):
    out = tmp_path / "news.csv"
    monkeypatch.setattr(news_fetcher, "NEWS_CSV", str(out))
    today = news_fetcher._today()
    calls = []

    def fake_fetch(limit=80):
        calls.append(1)
        return [(today, "半導體", "晶圆厂扩产"), (today, "半導體", "晶圆厂扩产")]
    monkeypatch.setattr(news_fetcher, "fetch_eastmoney_news", fake_fetch)
    n, _ = news_fetcher.ensure_news_csv()
    assert n == 1                                    # 重複標題去重
    n2, msg = news_fetcher.ensure_news_csv()
    assert n2 == 0 and "不重抓" in msg and len(calls) == 1
    rows = list(csv.reader(open(out, encoding="utf-8")))
    assert rows[0] == ["date", "scope", "title"] and rows[1] == [today, "半導體", "晶圆厂扩产"]


def test_news_unreachable_writes_header_only(tmp_path, monkeypatch):
    out = tmp_path / "news.csv"
    monkeypatch.setattr(news_fetcher, "NEWS_CSV", str(out))
    monkeypatch.setattr(news_fetcher, "_http_get", lambda *a, **k: None)
    n, msg = news_fetcher.ensure_news_csv()
    assert n == 0 and "未編造" in msg
    assert list(csv.reader(open(out, encoding="utf-8"))) == [["date", "scope", "title"]]


# ── dd_fetcher ──────────────────────────────────────────────────

def test_dd_unreachable_returns_none(monkeypatch):
    monkeypatch.setattr(dd_fetcher, "_http_get", lambda *a, **k: None)
    assert dd_fetcher.fetch_dd("008888") is None
    assert dd_fetcher.fetch_dd("00929.TW") is None


def test_dd_to_meta_only_fills_supported_dimensions():
    d = {"code": "X", "as_of": "2026-10-15", "src": "t", "name": "某聯接A",
         "scale_yi": 1.5, "mgmt_fee": 0.5, "custody_fee": 0.1, "manager": "某人"}
    m = dd_fetcher.to_dd_meta(d)
    assert "清盤線" in m["規模流動性"] and "年費 0.60%" in m["成本"]
    assert "持股" not in m and "位置" not in m              # 沒資料的維度不填
    assert m["_auto"] is True and m["_as_of"] == "2026-10-15"
