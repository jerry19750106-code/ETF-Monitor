"""資料清洗/對齊/日曆/代碼轉換等純函式(任務C 重構 phase 1,自 V24_19_ETF_System.py 原樣搬入)。

本檔函式本體與搬移前【字元級一致】(tests/test_characterization.py 以 SHA-256 驗證),
且只依賴標準庫/numpy 與同檔其他函式,不讀取主程式任何全域狀態。
主程式以原名匯入,外部介面不變。
"""
import datetime
import numpy as np


def sanitize_price_series(prices, code="", min_len=20):
    """[V24.12] 價格序列有效性守衛 — 堵住「取數失敗回傳 0 被當成真實價格」。

    ★根因(另一線於 V19.5~V22.47 全世代發現;本線 2026-08-07 自查確認 V24 同源仍在,
      且【本線是兩處不是一處】:L7871 觀察池 + L11742 持倉池):
      原守衛只驗 len(prices),不驗值有效性 → 含 0 的序列可完整通過。PoC 實測:
        trend_5 = (arr[-1]-arr[-5]) / (arr[-5] + 1e-9)
      若 arr[-5]==0 → 除以 1e-9 → 3.007e+09,再被 min/max 夾成 **trend=1.0**
      = 【假的滿分上升趨勢】,而同時 RSI=50.01、Z=0.24 看起來完全正常、毫無異常跡象
      → 直接推高 EV 產生【買入建議】。這比 Z 極值更陰險(Z 極值至少會觸發
        abs(z)>2 加風險分讓人注意到)。
      若失敗的是最新一筆:RSI=0.48 / Z=-4.36 / bias=-1.0 / trend=-1.0
      → 全面極端看空觸發減倉。**兩個方向的假信號都會產生。**
    回傳 (clean, dropped, ok);ok=False 表示有效筆數不足,呼叫端應 skip。
    """
    if not prices:
        return [], 0, False
    clean = []
    for _p in prices:
        try:
            _v = float(_p)
        except (TypeError, ValueError):
            continue
        if _v > 0 and np.isfinite(_v):
            clean.append(_v)
    return clean, len(prices) - len(clean), (len(clean) >= min_len)


def _series_to_returns_by_date(dated):
    """[(date_str, value)] → {date_str: 日報酬}; date 升序、相鄰計算。"""
    dated = sorted(dated, key=lambda x: x[0])
    out = {}
    for i in range(1, len(dated)):
        d0, v0 = dated[i-1]; d1, v1 = dated[i]
        if v0 and v1 and v0 > 0:
            out[d1] = v1 / v0 - 1.0
    return out


def _align_overnight(asset_dated, us_dated):
    """A股[t] 對 美股[t-1](隔夜傳導)按日期對齊。回傳 (asset_rets, us_rets) 等長。"""
    import bisect
    a_ret = _series_to_returns_by_date(asset_dated)
    u_ret = _series_to_returns_by_date(us_dated)
    if not a_ret or not u_ret:
        return [], []
    u_dates = sorted(u_ret.keys())
    A, U = [], []
    for d in sorted(a_ret.keys()):
        j = bisect.bisect_left(u_dates, d) - 1   # latest US date STRICTLY < d
        if j >= 0:
            A.append(a_ret[d]); U.append(u_ret[u_dates[j]])
    return A, U


def _now_utc8():
    """回傳 UTC+8(台北/北京)當前時間。本機已在 UTC+8,直接用 now()。"""
    try:
        return datetime.datetime.now(
            datetime.timezone(datetime.timedelta(hours=8))).replace(tzinfo=None)
    except Exception:
        return datetime.datetime.now()


def _clamp_snapshot_trend(trend, code=""):
    """[V22.38 治WB查出的髒資料源頭] daily_snapshot.trend 落庫【不 clamp】→ DB 實測範圍
    -26.55~+13.66(1243筆中737筆/59%超界),而即時報告用的是 clamp 後的 trend(最多±1.0)
    → 回測讀到的是【與報告不同的另一套數字】,任何門檻校準都建立在垃圾上。
    根因:兩處計算(L5309/L8438)有 clamp,但落庫這條路徑沒套。此處統一夾擠並告警。"""
    if trend is None:
        return None
    try:
        t = float(trend)
    except Exception:
        return None
    if t < -1.0 or t > 1.0:
        print(f"   ⚠️ [資料品質] {code} trend={t:.2f} 超出 [-1,1] → 夾擠(疑上游未 clamp)")
        t = max(-1.0, min(1.0, t))
    return t


def _expected_latest_us_date(ashare_date_str):
    """A股[t] 對應應已收盤的最新美股交易日 = t-1 最近工作日(跳週末;美股假日由 >expected 丟棄機制容錯)。"""
    d = datetime.date.fromisoformat(ashare_date_str) - datetime.timedelta(days=1)
    while d.weekday() >= 5:
        d -= datetime.timedelta(days=1)
    return d.isoformat()


def compute_pinned_ret(series, expected_date=None):
    """釘選:丟掉日期 > expected 的 nascent/未來 bar,再用最後兩根算隔夜 ret。回 (ret, as_of, dropped)。"""
    if not series or len(series) < 2:
        return None, None, 0
    s = list(series)
    dropped = 0
    if expected_date:
        while s and s[-1][0] > expected_date:
            s = s[:-1]
            dropped += 1
    if len(s) < 2 or s[-2][1] == 0:   # [V22.33.3] 防 0 分母(畸形 bar close=0)→回 None 當不可用
        return None, None, dropped
    return (s[-1][1] - s[-2][1]) / s[-2][1], s[-1][0], dropped


def _is_qdii_etf(info) -> bool:
    """[V22.23.3 / CC健檢] QDII 判定單一真相來源 — 一律以 TARGET_ETFS 的 fund_type 欄位為準。

    取代散落各處的四套不一致寫法：
      - name 含「纳指/标普/Nasdaq」關鍵字法
      - 硬編碼 code-list（其中誤把台股 00982A.TW 當 QDII；另有處 "00982A" 漏 .TW 永遠不命中）
      - 從未被使用的 fund_type 欄位
    現統一：QDII = fund_type == "qdii"（目前僅 006479 / 018967）。
    """
    return bool(info) and info.get("fund_type") == "qdii"


def _proxy_to_secid(proxy):
    """場內代理碼 sh518880/sz159995 → 東財secid 1.518880/0.159995 (sh=1,sz=0)"""
    if not proxy or len(proxy) < 8:
        return None
    mkt, code = proxy[:2].lower(), proxy[2:]
    if mkt == "sh":
        return f"1.{code}"
    if mkt == "sz":
        return f"0.{code}"
    return None


def _ae_nth_weekday(year, month, weekday, n):
    """該月第n個星期幾(weekday 0=週一..6=週日)"""
    d = datetime.date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    day = 1 + offset + (n - 1) * 7
    try:
        return datetime.date(year, month, day)
    except ValueError:
        return None


def _ae_adjust_weekend(d):
    """遇週末順延到下週一"""
    while d.weekday() >= 5:
        d += datetime.timedelta(days=1)
    return d
