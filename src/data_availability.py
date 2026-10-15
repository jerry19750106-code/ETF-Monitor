"""資料可用性標記模組（雲端任務A：靜默吞錯稽核與修復）。

背景：V24_19_ETF_System.py 裡多處資料抓取函式在例外路徑會靜默代入
預設值或陳舊快取，讓訊號層無法分辨「這是真數據」還是「抓取失敗的填充值」，
因而可能產出看起來正常、實際已失效的「假訊號」。

本模組不改變既有函式的回傳值/簽名（零回歸），只新增一個旁路：
呼叫端在 except 區塊呼叫 `_data_unavailable(code, reason)` 登記本次執行
「這個 code 的資料不可用」，訊號層或報告層之後可用 `is_unavailable()` /
`get_reason()` 查詢，決定是否要在報告中明確標示、或改走保守路徑。

執行緒安全（東財節流/多執行緒抓取情境下使用 Lock）。登記是「本次程式執行」
累積的，不會跨行程持久化；每次啟動都是乾淨狀態（也可用 reset() 手動清空，
測試專用）。
"""
import threading

_lock = threading.Lock()
_unavailable = {}  # code(str) -> {"reason": str, "count": int}


def _data_unavailable(code, reason):
    """登記 code 的資料在本次執行不可用。

    code:   資料主體代號（ETF 代碼、或宏觀指標的慣用鍵如 "USD_CNY"/"WTI"）。
    reason: 人類可讀的失敗原因（建議帶原始例外的 type/message）。

    回傳 None（故意，呼叫端通常是 `except:` 區塊，不應因為登記動作本身
    又多一個要處理的回傳值）。
    """
    key = str(code)
    text = str(reason)
    with _lock:
        entry = _unavailable.get(key)
        if entry is None:
            _unavailable[key] = {"reason": text, "count": 1}
        else:
            entry["reason"] = text
            entry["count"] += 1
    return None


def is_unavailable(code):
    """code 本次執行是否曾被標記為資料不可用。"""
    with _lock:
        return str(code) in _unavailable


def get_reason(code):
    """取得 code 最近一次被標記不可用的原因；從未被標記則回 None。"""
    with _lock:
        entry = _unavailable.get(str(code))
        return entry["reason"] if entry else None


def get_count(code):
    """code 本次執行被標記不可用的次數（0 表示從未被標記）。"""
    with _lock:
        entry = _unavailable.get(str(code))
        return entry["count"] if entry else 0


def unavailable_codes():
    """目前所有被標記為不可用的 code（排序後的 list，供報告彙總用）。"""
    with _lock:
        return sorted(_unavailable.keys())


def snapshot():
    """目前整個登記表的淺拷貝（code -> {"reason", "count"}），供報告/診斷用。"""
    with _lock:
        return {k: dict(v) for k, v in _unavailable.items()}


_LABELS = {
    # key: (項目名稱, 本次實際使用的值與影響)
    "VIX":           ("VIX 恐慌指數", "改用中性預設 18.0;報告上的 VIX 與「🟢正常」燈號不是即時值"),
    "VIX_RISK_OFF":  ("VIX 風控等級", "VIX 不可用,風控等級被當成「正常」(0),倉位風控閘門未生效"),
    "NDX":           ("納指隔夜漲跌", "改用 0%;QDII 傳導與隔夜方向判斷失真"),
    "SOX":           ("費半隔夜漲跌", "改用 0%;Tech 傳導與隔夜方向判斷失真"),
    "USD_CNY":       ("USD/CNY 匯率", "改用硬編碼 6.79;QDII 預測與匯率判讀失真"),
    "CNY_20MA":      ("USD/CNY 20日均線", "改用 0.0;匯率偏離分析不可信"),
    "GOLD":          ("黃金價格", "改用硬編碼 $4675"),
    "GOLD_RET":      ("黃金日漲跌", "改用 0%;宏觀先驗失真"),
    "WTI":           ("WTI 油價", "改用硬編碼 $97.00(報告上的油價不是即時值)"),
    "WTI_RET":       ("WTI 日漲跌", "改用 0%;地緣溫度 c_drain 推導失真"),
    "WTI_RET_PM":    ("WTI 日漲跌(盤前)", "改用 0%;地緣溫度 c_drain 推導失真"),
    "C_DRAIN":       ("地緣溫度 c_drain", "改用預設 0.20/人工值;影響全部 ETF 的 EV 調整"),
    "C_DRAIN_PM":    ("地緣溫度 c_drain(盤前)", "改用預設 0.20/人工值"),
    "C_DRAIN_INPUTS": ("地緣溫度 c_drain", "由上列預設輸入推導;報告上的地緣溫度數值與燈號不可信"),
    "DATA_HISTORY":  ("歷史資料庫", "找不到;持倉、命中率、IC、訊號健康度等一切依賴歷史的數字都是空的"),
    "ACCURACY":      ("預測命中率/誤差統計", "讀取失敗;依賴此統計的顯示與倉位計算改用預設值,不可信"),
    "SIGNAL_HEALTH": ("訊號可交易優勢(健康度)", "計算失敗;本報操作建議一律不背書,改列待你裁決"),
    "SIGNAL_PAYOFF": ("訊號報酬分佈(Kelly 輸入)", "讀取失敗;Kelly 退回最小倉位"),
    "TRADABLE_IC":   ("可交易 IC(倉位乘子輸入)", "計算失敗;倉位乘子退回中性 ×1.00"),
    "BRAIN_JSON":    ("brain.json", "檔案損毀,海峽狀態/Kalman 權重/貿易順暢度已回到預設值"),
    "BRAIN_JSON_PM": ("brain.json(盤前)", "檔案損毀,海峽狀態/Kalman 權重已回到預設值"),
}
_RS_PREFIX = "RS_RANK:"


def _cell(text, limit=90):
    text = str(text).replace("|", "/").replace("\n", " ")
    return text if len(text) <= limit else text[:limit - 1] + "…"


def format_unavailable_block(snap=None):
    """把登記表轉成報告用的 Markdown 區塊。沒有任何登記時回 ""(報告與原本逐字相同)。"""
    snap = snapshot() if snap is None else snap
    if not snap:
        return ""
    rows = []
    for key in [k for k in _LABELS if k in snap] + sorted(
            k for k in snap if k not in _LABELS and not k.startswith(_RS_PREFIX)):
        name, impact = _LABELS.get(key, (key, "已登記為不可用"))
        rows.append(f"| {name} | {_cell(impact)} | {_cell(snap[key]['reason'])} |")
    rs = sorted(k[len(_RS_PREFIX):] for k in snap if k.startswith(_RS_PREFIX))
    if rs:
        rows.append(f"| RS 同風格排名 | 改用中性 50 分,排名不可信:{_cell(', '.join(rs), 120)} | 預掃描例外 |")
    return ("## ⚠️ 本次資料不可用(以下數值為預設/硬編碼,不是即時資料)\n\n"
            "| 項目 | 實際使用的值與影響 | 失敗原因 |\n"
            "|---|---|---|\n"
            + "\n".join(rows) + "\n\n"
            "> 與上列項目相關的訊號與數字請降權看待;資料源恢復後重跑本報即可。\n\n")


def insert_unavailable_block(report_text, snap=None):
    """把不可用區塊插在報告第一行標題(# 開頭)之後;無登記則原文照回。"""
    block = format_unavailable_block(snap)
    if not block:
        return report_text
    if report_text.startswith("#"):
        head, _, rest = report_text.partition("\n")
        return head + "\n\n" + block + rest.lstrip("\n")
    return block + report_text


def reset():
    """清空登記表。正式環境通常不需要呼叫（一次程式執行=一次乾淨狀態）；
    主要供測試之間互相隔離使用。"""
    with _lock:
        _unavailable.clear()
