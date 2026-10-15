"""雲端任務A：靜默吞錯稽核與修復 —— 回歸測試。

每個測試對應 docs/AUDIT_silent_errors.md 裡的一個 A 類項目：
強制讓該項目的資料來源失敗，驗證 `src/data_availability.py` 的
`_data_unavailable()` 登記機制確實被呼叫（訊號層可查到「這筆資料不可用」），
而不是讓呼叫端把預設值/陳舊快取當成真實資料悄悄往下算。

★ import 注意事項：
  src/V24_19_ETF_System.py 在 import 時會於模組層級執行：
  1. _probe_em()：對 eastmoney.com 做一次即時網路探測（除非 EM_FORCE_IPV4=0）
  2. _ensure_dd_auto()：可能呼叫 dd_fetcher 做批量網路抓取（除非 DD_AUTO_REFRESH=0）
  3. os.makedirs(BASE_DIR)：預設值是一個 Windows 路徑字串，須用 ETF_OUTPUT_DIR 覆寫成暫存目錄
  本檔的 `etf_mod` fixture 在 import 前設好這三個環境變數避開網路呼叫，
  並額外用 monkeypatch 把 requests.get / requests.Session.get 釘死成「一叫就報錯」，
  作為雙重保險 —— 若未來有人在模組層級新增了遺漏的網路呼叫，測試會立刻炸掉
  而不是安靜地在 CI 裡掛網、超時。
"""
import ast
import importlib.util
import os
import sys
import textwrap
from unittest import mock

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_ROOT, "src")
MODULE_PATH = os.path.join(SRC_DIR, "V24_19_ETF_System.py")


def _blocked_network_call(*args, **kwargs):
    raise AssertionError(
        "網路呼叫被測試攔截：import 階段不應該打任何 HTTP 請求"
        f"（args={args!r} kwargs={kwargs!r}）。若這是新增的合法呼叫，"
        "請先用 EM_FORCE_IPV4/DD_AUTO_REFRESH 等環境變數在 import 前關閉它，"
        "而不要放寬這個攔截。"
    )


@pytest.fixture(scope="session")
def etf_mod(tmp_path_factory):
    """以網路隔離的方式 import 主程式一次，供全部測試共用。"""
    out_dir = tmp_path_factory.mktemp("etf_output")
    env_overrides = {
        "ETF_OUTPUT_DIR": str(out_dir),
        "EM_FORCE_IPV4": "0",     # 跳過 L799 區塊的東財即時探測
        "DD_AUTO_REFRESH": "0",   # 跳過 _ensure_dd_auto() 的批量網路抓取
        "ETF_VERBOSE": "0",
    }
    with mock.patch.dict(os.environ, env_overrides), \
         mock.patch("requests.get", side_effect=_blocked_network_call), \
         mock.patch("requests.Session.get", side_effect=_blocked_network_call):
        spec = importlib.util.spec_from_file_location("etf_v24_under_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def _reset_registry(etf_mod):
    """每個測試前清空可用性登記表，避免互相汙染。"""
    etf_mod.data_availability.reset()
    yield
    etf_mod.data_availability.reset()


# ──────────────────────────────────────────────────────────────────
# main() 內嵌 try/except 區塊：用 AST 精準抓出該區塊原始碼後 exec，
# 不靠絕對行號（檔案其他地方增刪行不會讓這裡失真），也不用重寫main()。
# ──────────────────────────────────────────────────────────────────
def _extract_enclosing_try(anchor_text):
    """在主程式原始碼裡找出『包含 anchor_text 那一行』的最內層 try/except
    區塊，回傳 (dedent 後的原始碼字串, 起始行號)。anchor_text 必須唯一。"""
    with open(MODULE_PATH, encoding="utf-8") as f:
        source = f.read()
    lines = source.splitlines()
    matches = [i + 1 for i, line in enumerate(lines) if anchor_text in line]
    assert len(matches) == 1, (
        f"anchor {anchor_text!r} 應唯一命中一行，實際命中 {matches!r}"
    )
    target_lineno = matches[0]

    tree = ast.parse(source)
    best = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            last_end = max(h.body[-1].end_lineno for h in node.handlers) if node.handlers else node.end_lineno
            span_end = max(last_end, node.end_lineno)
            if node.lineno <= target_lineno <= span_end:
                span = span_end - node.lineno
                if best is None or span < best[0]:
                    best = (span, node)
    assert best is not None, f"找不到包住行 {target_lineno} 的 try 區塊"
    node = best[1]
    start, end = node.lineno, max(
        node.end_lineno,
        max(h.body[-1].end_lineno for h in node.handlers) if node.handlers else node.end_lineno,
    )
    snippet = "\n".join(lines[start - 1:end])
    return textwrap.dedent(snippet), start


def _exec_snippet(anchor_text, namespace, wrap_for=None):
    """wrap_for: 若該 try 區塊本身含 `continue`/`break`(只在 for/while 內合法)，
    傳入如 'for code, info in TARGET_ETFS.items():' 把片段包一層迴圈再 exec。"""
    snippet, _ = _extract_enclosing_try(anchor_text)
    if wrap_for:
        snippet = wrap_for + "\n" + textwrap.indent(snippet, "    ")
    exec(compile(snippet, filename=f"<main_snippet:{anchor_text}>", mode="exec"), namespace)
    return namespace


# ════════════════════════════════════════════════════════════════
# A1: IntelTeam.get_usd_cny —— Yahoo + AkShare 皆失敗 → 硬編碼 6.79
# ════════════════════════════════════════════════════════════════
def test_get_usd_cny_fetch_failure_marks_unavailable(etf_mod):
    with mock.patch.object(etf_mod, "_yahoo_get", side_effect=RuntimeError("yahoo down")):
        rate, tag = etf_mod.IntelTeam.get_usd_cny()

    # 修復前這裡只會驗證 tag 本身帶著警告字，驗不到「訊號層可查詢」這件事；
    # 修復後 data_availability 必須明確知道 USD_CNY 今天不可用。
    assert rate == 6.7900
    assert "HARDCODED" in tag
    assert etf_mod.data_availability.is_unavailable("USD_CNY"), (
        "get_usd_cny 在 Yahoo+AkShare 都失敗時應登記 USD_CNY 資料不可用"
    )
    assert "6.79" in etf_mod.data_availability.get_reason("USD_CNY")


# ════════════════════════════════════════════════════════════════
# A2/A3: IntelTeam.get_gold_wti_yahoo —— 黃金/WTI 各自的硬編碼回退
#   ★真正的危險案例是 A3：黃金抓到(src="🟢 LIVE")但WTI失敗 → 若只看共用的
#     src 標籤，呼叫端會誤以為WTI也是即時值。
# ════════════════════════════════════════════════════════════════
def test_get_gold_wti_yahoo_gold_failure_marks_unavailable(etf_mod):
    with mock.patch.object(etf_mod, "_yahoo_get", side_effect=RuntimeError("yahoo down")):
        with mock.patch.dict(sys.modules, {"akshare": None}):
            gold, wti, src = etf_mod.IntelTeam.get_gold_wti_yahoo()

    assert gold == 4675.0
    assert etf_mod.data_availability.is_unavailable("GOLD")


def test_get_gold_wti_yahoo_wti_failure_marks_unavailable_even_when_gold_live(etf_mod):
    """黃金 LIVE、WTI 兩源都失敗 —— 回傳的 src 仍會是 "🟢 LIVE"(舊bug)，
    必須靠獨立的 WTI 登記才能讓呼叫端知道 wti=97.0 是假的。"""
    real_yahoo_get = etf_mod._yahoo_get

    def _fake_yahoo_get(url, *a, **kw):
        if "GC%3DF" in url:  # 黃金端點照常"成功"
            class _R:
                def json(self):
                    return {"chart": {"result": [{"indicators": {"quote": [{"close": [4800.0, 4820.0]}]}}]}}
            return _R()
        raise RuntimeError("WTI endpoint down")  # WTI端點失敗

    with mock.patch.object(etf_mod, "_yahoo_get", side_effect=_fake_yahoo_get):
        with mock.patch.dict(sys.modules, {"akshare": None}):
            gold, wti, src = etf_mod.IntelTeam.get_gold_wti_yahoo()

    assert gold == 4820.0
    assert src == "🟢 LIVE"          # ← 舊bug依然存在：共用標籤誤導WTI也是live
    assert wti == 97.0               # ← 硬編碼預設值
    assert etf_mod.data_availability.is_unavailable("WTI"), (
        "WTI 獨立失敗時必須能被查到,不能只靠(會被誤導的)共用 src 標籤"
    )
    assert not etf_mod.data_availability.is_unavailable("GOLD")  # 黃金是真的live,不該被標


# ════════════════════════════════════════════════════════════════
# A4: IntelTeam.get_cny_20ma —— 失敗回退 0.0
# ════════════════════════════════════════════════════════════════
def test_get_cny_20ma_fetch_failure_marks_unavailable(etf_mod):
    with mock.patch.object(etf_mod, "_yahoo_get", side_effect=RuntimeError("yahoo down")):
        val = etf_mod.IntelTeam.get_cny_20ma()

    assert val == 0.0
    assert etf_mod.data_availability.is_unavailable("CNY_20MA")


# ════════════════════════════════════════════════════════════════
# A5/A6: main() 內嵌的 WTI 日漲跌率計算（盤前版 + 主流程版）
# ════════════════════════════════════════════════════════════════
def test_main_wti_ret_pm_failure_marks_unavailable(etf_mod):
    namespace = {
        "IntelTeam": etf_mod.IntelTeam,
        "_log_err": etf_mod._log_err,
        "_data_unavailable": etf_mod._data_unavailable,
    }
    with mock.patch.object(etf_mod.IntelTeam, "get_5day_history",
                            side_effect=RuntimeError("history fetch failed")):
        result = _exec_snippet('_log_err(_e, "wti_ret")', namespace)

    assert result["wti_ret"] == 0
    assert etf_mod.data_availability.is_unavailable("WTI_RET_PM")


def test_main_wti_ret_failure_marks_unavailable(etf_mod):
    namespace = {
        "IntelTeam": etf_mod.IntelTeam,
        "_log_err": etf_mod._log_err,
        "_data_unavailable": etf_mod._data_unavailable,
    }
    with mock.patch.object(etf_mod.IntelTeam, "get_5day_history",
                            side_effect=RuntimeError("history fetch failed")):
        result = _exec_snippet('_log_err(_e, "wti_ret_main")', namespace)

    assert result["wti_ret"] == 0.0
    assert etf_mod.data_availability.is_unavailable("WTI_RET")


# ════════════════════════════════════════════════════════════════
# A7/A8: main() 內嵌的 c_drain(地緣溫度) 自動推導（盤前版 + 主流程版）
# ════════════════════════════════════════════════════════════════
def test_main_derive_c_drain_pm_failure_marks_unavailable(etf_mod):
    namespace = {
        "vix": 18.0, "wti": 70.0, "wti_ret": 0.0, "gold_ret_pm": 0.0,
        "_macro_pm": {}, "_manual_cdrain_pm": None,
        "derive_c_drain": mock.Mock(side_effect=RuntimeError("derive failed")),
        "_log_err": etf_mod._log_err,
        "_data_unavailable": etf_mod._data_unavailable,
        "os": os, "json": etf_mod.json, "DB_FILE": "/nonexistent/path/does-not-exist.json",
        "_brain_pm": {},
    }
    result = _exec_snippet('_log_err(_e, "derive_c_drain_pm")', namespace)

    assert result["_cdrain_pm"] == 0.20
    assert etf_mod.data_availability.is_unavailable("C_DRAIN_PM")


def test_main_derive_c_drain_failure_marks_unavailable(etf_mod):
    namespace = {
        "vix": 18.0, "wti": 70.0, "wti_ret": 0.0, "gold_ret": 0.0,
        "macro": {}, "_manual_cdrain": None,
        "derive_c_drain": mock.Mock(side_effect=RuntimeError("derive failed")),
        "_log_err": etf_mod._log_err,
        "_data_unavailable": etf_mod._data_unavailable,
    }
    result = _exec_snippet('_log_err(_e, "derive_c_drain")', namespace)

    assert result["c_drain"] == 0.20
    assert etf_mod.data_availability.is_unavailable("C_DRAIN")


# ════════════════════════════════════════════════════════════════
# A9: main() 讀取 brain.json 失敗(檔案存在但損毀) → 靜默重置 macro/kalman
# ════════════════════════════════════════════════════════════════
def test_main_brain_json_corrupt_marks_unavailable(etf_mod, tmp_path):
    bad_json = tmp_path / "v20_brain.json"
    bad_json.write_text("{this is not valid json!!", encoding="utf-8")

    namespace = {
        "os": os, "json": etf_mod.json,
        "DB_FILE": str(bad_json),
        "_log_err": etf_mod._log_err,
        "_data_unavailable": etf_mod._data_unavailable,
    }
    result = _exec_snippet(
        'f"brain.json 讀取/解析失敗,strait_status/kalman權重/trade_ease"', namespace
    )

    assert result["brain"] == {"macro": {}, "kalman": {}}
    # 既有行為刻意保留:except 分支不宣告 macro(見稽核報告第五節第2點)
    assert "macro" not in result
    assert etf_mod.data_availability.is_unavailable("BRAIN_JSON")


def test_main_brain_json_pm_corrupt_marks_unavailable(etf_mod, tmp_path):
    bad_json = tmp_path / "v20_brain.json"
    bad_json.write_text("{corrupt", encoding="utf-8")

    namespace = {
        "json": etf_mod.json,
        "DB_FILE": str(bad_json),
        "_brain_pm": {},
        "_log_err": etf_mod._log_err,
        "_data_unavailable": etf_mod._data_unavailable,
    }
    result = _exec_snippet('"BRAIN_JSON_PM"', namespace)

    assert result["_brain_pm"] == {}
    assert etf_mod.data_availability.is_unavailable("BRAIN_JSON_PM")


# ════════════════════════════════════════════════════════════════
# A10: main() RS-Rank 預掃描失敗 → 靜默填中性 50 分
# ════════════════════════════════════════════════════════════════
def test_main_rs_rank_scan_failure_marks_unavailable(etf_mod):
    code = "999999"
    namespace = {
        "TARGET_ETFS": {code: {"market": "A股", "style": "Tech", "proxy": None}},
        "DirectDataFetcher": etf_mod.DirectDataFetcher,
        "np": etf_mod.np,
        "time": mock.Mock(sleep=lambda *_: None),
        "proxy_last_seen": {},
        "rs_ev_map": {},
        "strait_status": "conditional_open",
        "w_tw": 0.481, "w_a": 0.531, "c_drain": 0.20,
        "GEO_ADJ_TABLE": etf_mod.GEO_ADJ_TABLE,
        "_data_unavailable": etf_mod._data_unavailable,
    }
    with mock.patch.object(etf_mod.DirectDataFetcher, "get_eastmoney_nav",
                            side_effect=RuntimeError("nav fetch exploded")):
        result = _exec_snippet("RS-Rank預掃描失敗", namespace,
                                wrap_for="for code, info in TARGET_ETFS.items():")

    assert result["rs_ev_map"][code] == 50
    assert etf_mod.data_availability.is_unavailable(f"RS_RANK:{code}")


def test_main_rs_rank_no_prices_marks_unavailable_and_is_not_ranked(etf_mod):
    """取不到價格(回空序列,非例外)時原本靜默給 50 分,報告照印「#1/11」這種沒有依據的排名。"""
    code = "999998"
    targets = {code: {"market": "A股", "style": "Tech", "proxy": None},
               "999997": {"market": "A股", "style": "Tech", "proxy": None}}
    namespace = {
        "TARGET_ETFS": {code: targets[code]},
        "DirectDataFetcher": etf_mod.DirectDataFetcher,
        "np": etf_mod.np,
        "time": mock.Mock(sleep=lambda *_: None),
        "proxy_last_seen": {},
        "rs_ev_map": {},
        "strait_status": "conditional_open",
        "w_tw": 0.481, "w_a": 0.531, "c_drain": 0.20,
        "GEO_ADJ_TABLE": etf_mod.GEO_ADJ_TABLE,
        "_data_unavailable": etf_mod._data_unavailable,
    }
    with mock.patch.object(etf_mod.DirectDataFetcher, "get_eastmoney_nav", return_value=[]):
        result = _exec_snippet("RS-Rank預掃描失敗", namespace,
                                wrap_for="for code, info in TARGET_ETFS.items():")

    assert result["rs_ev_map"][code] == 50
    assert etf_mod.data_availability.is_unavailable(f"RS_RANK:{code}")
    groups = etf_mod._rs_style_groups({code: 50, "999997": 70}, targets)
    assert groups == {"Tech": [("999997", 70)]}


# ════════════════════════════════════════════════════════════════
# A11: main() Risk-off level 計算失敗(VIX不可用) → 靜默維持「正常」
# ════════════════════════════════════════════════════════════════
def test_main_risk_off_vix_failure_marks_unavailable(etf_mod):
    namespace = {
        "_risk_off_level": 0,
        "DirectDataFetcher": etf_mod.DirectDataFetcher,
        "_log_err": etf_mod._log_err,
        "_data_unavailable": etf_mod._data_unavailable,
    }
    with mock.patch.object(etf_mod.DirectDataFetcher, "get_vix", side_effect=RuntimeError("vix down")):
        result = _exec_snippet('_log_err(_e, "main_risk_off_calc")', namespace)

    assert result["_risk_off_level"] == 0
    assert etf_mod.data_availability.is_unavailable("VIX_RISK_OFF")


# ════════════════════════════════════════════════════════════════
# 反向保護：確認 import 階段的網路攔截機制本身有效（不是形同虛設）。
# 只在 fixture 實際 import 時的那個 with 區塊內有效，故這裡直接重現
# 同樣的 patch 並手動呼叫一次，而非倚賴 fixture 結束後殘留的 patch。
# ════════════════════════════════════════════════════════════════
def test_network_block_mechanism_itself_works(etf_mod):
    with mock.patch("requests.get", side_effect=_blocked_network_call):
        with pytest.raises(AssertionError, match="網路呼叫被測試攔截"):
            import requests
            requests.get("https://example.invalid/should-be-blocked")


# ════════════════════════════════════════════════════════════════
# 報告頂部「⚠️ 本次資料不可用」區塊
# ════════════════════════════════════════════════════════════════
_SAMPLE_REPORT = "# 收盤報 戰報 | V24.30\n* VIX=18.00🟢 | WTI=$97.00🟡\n\n## 持倉\n內容\n"


def test_unavailable_block_absent_when_nothing_registered(etf_mod):
    """零回歸:沒有任何資料不可用時,報告必須與原本逐字相同。"""
    da = etf_mod.data_availability
    assert da.format_unavailable_block() == ""
    assert da.insert_unavailable_block(_SAMPLE_REPORT) == _SAMPLE_REPORT


def test_unavailable_block_inserted_after_title_and_lists_items(etf_mod):
    da = etf_mod.data_availability
    da._data_unavailable("WTI", "get_gold_wti_yahoo: 兩源失敗 | ModuleNotFoundError")
    da._data_unavailable("VIX_RISK_OFF", "TypeError: '>' not supported")
    da._data_unavailable("RS_RANK:008888", "boom")
    da._data_unavailable("RS_RANK:011613", "boom")
    da._data_unavailable("SOMETHING_NEW", "未來新增的 key")

    out = da.insert_unavailable_block(_SAMPLE_REPORT)
    lines = out.split("\n")
    assert lines[0] == "# 收盤報 戰報 | V24.30"                  # 標題仍在第一行
    assert lines[2].startswith("## ⚠️ 本次資料不可用")           # 區塊緊接標題
    assert out.endswith(_SAMPLE_REPORT.partition("\n")[2])      # 原文其餘部分完整保留
    assert "WTI 油價" in out and "硬編碼 $97.00" in out
    assert "VIX 風控等級" in out
    assert "008888, 011613" in out                              # RS 排名合併成一列
    assert "SOMETHING_NEW" in out                               # 未知 key 不會被吞掉
    table = [l for l in lines if l.startswith("| ") and "---" not in l]
    assert all(l.count("|") == 4 for l in table), "原因文字裡的 | 必須跳脫,否則表格欄位錯位"


def test_every_report_write_point_inserts_block():
    """防漏:三個寫檔點(盤前報/早午收盤報/夜間快報)都要在寫檔前插入區塊,main() 開頭要清空登記表。"""
    with open(MODULE_PATH, encoding="utf-8") as f:
        src = f.read()
    for var in ("pm_report", "full_content", "brief"):
        assert f"{var} = data_availability.insert_unavailable_block({var})" in src, var
    assert src.count("data_availability.insert_unavailable_block(") == 3
    main_src = src[src.index("\ndef main():"):]
    assert "data_availability.reset()" in main_src[:main_src.index("if args.verbose")]


# ════════════════════════════════════════════════════════════════
# 非 except 的靜默預設:WTI 歷史不足時條件式給 0;c_drain 由預設輸入推導
# ════════════════════════════════════════════════════════════════
def _module_ns(etf_mod, **overrides):
    """以主程式完整模組全域環境為底(與 main() 實際執行一致),再覆寫本測試的輸入。"""
    ns = dict(vars(etf_mod))
    ns.update(overrides)
    return ns


@pytest.mark.parametrize("anchor,key", [
    ('_log_err(_e, "wti_ret")', "WTI_RET_PM"),
    ('_log_err(_e, "wti_ret_main")', "WTI_RET"),
])
def test_wti_ret_empty_history_marks_unavailable(etf_mod, anchor, key):
    with mock.patch.object(etf_mod.IntelTeam, "get_5day_history", return_value=[]):
        ns = _exec_snippet(anchor, _module_ns(etf_mod))
    assert ns["wti_ret"] == 0                                   # 既有行為不變
    assert etf_mod.data_availability.is_unavailable(key), (
        "歷史不足時條件式給 0,與「今日持平」無法區分,必須登記")


@pytest.mark.parametrize("anchor,key", [
    ('_log_err(_e, "wti_ret")', "WTI_RET_PM"),
    ('_log_err(_e, "wti_ret_main")', "WTI_RET"),
])
def test_wti_ret_real_history_not_marked(etf_mod, anchor, key):
    with mock.patch.object(etf_mod.IntelTeam, "get_5day_history", return_value=[70.0, 72.1]):
        ns = _exec_snippet(anchor, _module_ns(etf_mod))
    assert ns["wti_ret"] == pytest.approx(0.03)
    assert not etf_mod.data_availability.is_unavailable(key)


_CDRAIN_CASES = [  # (anchor, 本路徑的 wti_ret key, 結果變數, 理由變數, 人工值變數, macro 變數)
    ('_log_err(_e, "derive_c_drain")', "WTI_RET", "c_drain", "_cdrain_reason", "_manual_cdrain", "macro"),
    ('_log_err(_e, "derive_c_drain_pm")', "WTI_RET_PM", "_cdrain_pm", "_cdrain_pm_reason",
     "_manual_cdrain_pm", "_macro_pm"),
]


def _cdrain_ns(etf_mod, manual_var, macro_var, manual=None, as_of=None):
    return _module_ns(etf_mod, vix=18.0, wti=97.0, wti_ret=0.0, gold_ret=0.0, gold_ret_pm=0.0,
                      **{manual_var: manual, macro_var: {"c_drain_as_of": as_of} if as_of else {}})


@pytest.mark.parametrize("anchor,wti_key,out_var,reason_var,manual_var,macro_var", _CDRAIN_CASES)
@pytest.mark.parametrize("defaulted_input", ["VIX", "WTI_RET*", "GOLD_RET"])
def test_c_drain_from_defaulted_inputs_marks_unavailable(
        etf_mod, anchor, wti_key, out_var, reason_var, manual_var, macro_var, defaulted_input):
    etf_mod.data_availability._data_unavailable(
        wti_key if defaulted_input == "WTI_RET*" else defaulted_input, "測試:輸入為預設值")
    ns = _exec_snippet(anchor, _cdrain_ns(etf_mod, manual_var, macro_var))
    assert ns[out_var] == pytest.approx(0.20)                   # 數值本身不變
    assert "市場平靜" in ns[reason_var]                          # 原本看起來像真的「市場平靜」
    assert etf_mod.data_availability.is_unavailable("C_DRAIN_INPUTS")


@pytest.mark.parametrize("anchor,wti_key,out_var,reason_var,manual_var,macro_var", _CDRAIN_CASES)
def test_c_drain_from_real_inputs_not_marked(
        etf_mod, anchor, wti_key, out_var, reason_var, manual_var, macro_var):
    _exec_snippet(anchor, _cdrain_ns(etf_mod, manual_var, macro_var))
    assert not etf_mod.data_availability.is_unavailable("C_DRAIN_INPUTS")


@pytest.mark.parametrize("anchor,wti_key,out_var,reason_var,manual_var,macro_var", _CDRAIN_CASES)
def test_c_drain_fresh_manual_override_not_marked(
        etf_mod, anchor, wti_key, out_var, reason_var, manual_var, macro_var):
    """有效期內的人工加碼不依賴 VIX/油/金輸入,即使輸入不可用也不該標成不可信。"""
    import datetime
    etf_mod.data_availability._data_unavailable("VIX", "測試:輸入為預設值")
    ns = _exec_snippet(anchor, _cdrain_ns(etf_mod, manual_var, macro_var, manual=0.45,
                                          as_of=datetime.date.today().isoformat()))
    assert ns[out_var] == pytest.approx(0.45)
    assert not etf_mod.data_availability.is_unavailable("C_DRAIN_INPUTS")


# ════════════════════════════════════════════════════════════════
# A13: _get_accuracy 例外 → main() 拿到憑空 60% 勝率去算 Kelly(原稽核誤歸 B 類,已更正)
# ════════════════════════════════════════════════════════════════
def test_get_accuracy_failure_marks_unavailable(etf_mod):
    with mock.patch.object(etf_mod.sqlite3, "connect", side_effect=etf_mod.sqlite3.OperationalError("db locked")):
        acc = etf_mod._get_accuracy()

    assert acc == {"台股": {"n": 0}, "A股": {"n": 0}}               # 既有回傳不變
    mkt_acc = acc["A股"]
    win_rate, mae = mkt_acc.get("accuracy", 0.6), mkt_acc.get("mae", 0.008)   # main() L13135 原樣
    assert (win_rate, mae) == (0.6, 0.008), "前提:例外路徑確實讓 main() 拿到憑空預設值"
    kelly = etf_mod.KellyEngine.compute(60, win_rate, mae * 1.5, mae)
    assert kelly["half_kelly"] > 0, "前提:憑空預設值會產生非零的建議倉位"
    assert etf_mod.data_availability.is_unavailable("ACCURACY")


# ════════════════════════════════════════════════════════════════
# 既有 _intel_na(VIX/NDX/SOX/黃金中性預設)同步進登記表;主流程黃金原本永遠漏報
# ════════════════════════════════════════════════════════════════
def _exec_between(etf_mod, start_marker, end_marker, ns, occurrence=0):
    """取主程式中 start_marker 那一行(第 occurrence 次出現)到 end_marker 之前的真實原始碼執行。"""
    with open(MODULE_PATH, encoding="utf-8") as f:
        src = f.read()
    pos = -1
    for _ in range(occurrence + 1):
        pos = src.index(start_marker, pos + 1)
    start = src.rindex("\n", 0, pos) + 1
    end = src.index(end_marker, pos)
    block = textwrap.dedent(src[start:end])
    exec(compile(block, f"<between:{start_marker}>", "exec"), ns)
    return ns


@pytest.mark.parametrize("occurrence,names", [
    (0, {"vix": None, "ndx": None, "sox": None, "gold_ret_pm": None}),     # 盤前報 Path A
    (1, {"vix": None, "ndx_d": None, "sox_d": None}),                      # 主流程 Path B
])
def test_intel_na_defaults_are_registered(etf_mod, occurrence, names):
    ns = _module_ns(etf_mod, **names)
    _exec_between(etf_mod, "_intel_na = []", "_tw_warn = check_tracker_freshness()", ns, occurrence)
    assert ns["vix"] == 18.0                                              # 既有中性預設不變
    da = etf_mod.data_availability
    for key in ("VIX", "NDX", "SOX"):
        assert da.is_unavailable(key), key
    if occurrence == 0:
        assert da.is_unavailable("GOLD_RET")


def test_intel_na_real_values_not_registered(etf_mod):
    ns = _module_ns(etf_mod, vix=21.5, ndx_d=0.004, sox_d=-0.012)
    _exec_between(etf_mod, "_intel_na = []", "_tw_warn = check_tracker_freshness()", ns, 1)
    assert etf_mod.data_availability.unavailable_codes() == []


def test_main_gold_ret_none_registered_even_after_warning_built(etf_mod):
    """主流程的黃金在 _intel_warning 組好之後才 append,原警示永遠不含黃金;登記表必須補上。"""
    ns = _module_ns(etf_mod, gold_ret=None, _intel_na=[], _intel_warning="")
    _exec_between(etf_mod, "    if gold_ret is None:", "    # [V22.25.5] WTI漲跌率(主流程補算", ns)
    assert ns["gold_ret"] == 0.0
    assert "黃金" not in ns["_intel_warning"]                             # 證實原本漏報
    assert etf_mod.data_availability.is_unavailable("GOLD_RET")
