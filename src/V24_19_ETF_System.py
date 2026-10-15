"""
ETF Monitor V24 — A股場外聯接基金 / 台股 ETF 輪動監控系統(主程式)

每個交易日產生四份報告:盤前報 / 早報 / 午報 / 收盤報。
    執行    python V24_19_ETF_System.py --force-type 收盤報
    版本    以本檔 __VERSION__ 常量為準;檔名 V24_19 為歷史沿用,不代表版本
    說明    README.md
    變更    docs/CHANGELOG.md

不是投資建議。訊號的回測結果與限制見 docs/BACKTEST_baseline.md。
"""

ENABLE_GEMMA = False

import numpy as np
import re as _re_ver   # [V24.25] 版本自檢用
import requests
import json, datetime, time, os, re, warnings, sqlite3, traceback

# 主控台編碼:必須在任何 print 之前。輸出被導向檔案/管線(排程、> log.txt)時,Windows 預設用
# 地區編碼(cp950/cp936),無法編碼 ✅⚠️ 等字元,啟動即 UnicodeEncodeError。
import sys as _enc_sys
for _enc_stream in (_enc_sys.stdout, _enc_sys.stderr):
    try:
        _enc_stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass   # 被替換成無 reconfigure 的物件(如測試擷取)時維持原狀

# [雲端任務A] 資料可用性標記旁路(與本檔同目錄的新增模組,零改動既有回傳值)。
import sys as _das_sys
_das_here = os.path.dirname(os.path.abspath(__file__))
if _das_here not in _das_sys.path:
    _das_sys.path.insert(0, _das_here)
import data_availability
from data_availability import _data_unavailable
import returns_store     # 實現報酬單一來源(realized_returns)
import signal_health     # 各報告×市場的滾動可交易優勢
import backtest_engine as _be

# ════════════════════════════════════════════════════════════════
# [V22.18.3 新增] 數據抓取專用 no-proxy session
# ════════════════════════════════════════════════════════════════
# 設計意圖: 東財/新浪是國內CDN，走代理反而增加延遲和不穩定性。
# trust_env=False + proxies=None 確保直連國內站點。
# ⚠️ 注意: Telegram(post)和Yahoo【不用】此session(它們在牆外反而需要VPN)
#
# [V22.24.9 審計] 2026-06-16 push2his全軍覆沒根因：diag_orderflow.py 證實為
#   push2his.eastmoney.com 對本機 IP 的連線層限流（RemoteDisconnected），
#   與端點（daykline/kline）、代理（直連/走VPN）、ut 參數均無關。
#   trust_env=False + proxies=None 的設計是正確的，不動。
_DATA_SESSION = requests.Session()
_DATA_SESSION.trust_env = False

# ═══════════════════════════════════════════════════════════════════
# [V22.48] Yahoo 統一入口 —— 治「代理(GE5OX)一掛,15 個 Yahoo 請求全倒」
#   ★根因(2026-07-22 實證):_DATA_SESSION 只對【東財】設 trust_env=False 直連;
#     Yahoo 的 15 處全是裸 requests.get,繼承 env 代理 → 代理 WinError 10061 時全滅。
#     後果:VIX/NDX/SOX「數據不可用→中性預設」、條件β 全退 fallback 固定值、
#           macro_prior 假中性 +0.00 —— 而報告【看不出這些是假值】。
#   ★WB 寫了外掛模組但只覆蓋 2/15,且外掛會產生部署依賴(import 失敗全系統掛)
#     → 改為單檔內建、覆蓋全部,與 _em_throttled_get 同範式。
#   ★三道防禦:①代理 TCP 預檢(不燒 timeout) ②落盤快取回退 ③stale 標記進報告
# ═══════════════════════════════════════════════════════════════════
_YH_CACHE_FILE = os.path.join(BASE_DIR, ".yahoo_fallback_cache.json") if "BASE_DIR" in dir() else ".yahoo_fallback_cache.json"
_YH_STALE = {}                 # {url_tag: reason} 供報告層查
_YH_PROXY_DOWN = [None]        # 一次運行只探一次
_YH_CACHE_MAX_AGE_D = 7

def _yh_proxy_ok(force=False):
    """代理 TCP 預檢。無代理→True(直連)。代理沒跑→False,不燒請求 timeout。"""
    if _YH_PROXY_DOWN[0] is not None and not force:
        return not _YH_PROXY_DOWN[0]
    px = (os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
          or os.environ.get("https_proxy") or os.environ.get("http_proxy"))
    if not px:
        _YH_PROXY_DOWN[0] = False; return True
    m = re.match(r"^(?:https?|socks5h?)://([^:/]+)(?::(\d+))?", px.strip())
    if not m:
        _YH_PROXY_DOWN[0] = False; return True
    host, port = m.group(1), int(m.group(2) or 1080)
    try:
        import socket as _sk
        with _sk.create_connection((host, port), timeout=3.0):
            _YH_PROXY_DOWN[0] = False; return True
    except OSError:
        _YH_PROXY_DOWN[0] = True
        print(f"   🔴 [代理預檢] {host}:{port} 不可達 → Yahoo 將走落盤快取,報告標 ⚠️CACHE")
        return False

def _yh_cache_load():
    try:
        if os.path.exists(_YH_CACHE_FILE):
            with open(_YH_CACHE_FILE, encoding="utf-8") as f: return json.load(f)
    except Exception as _e:
        _log_err(_e, "_yh_cache_load:L709")  # [V24.16] 原為靜默 pass
        pass
    return {}

def _yh_cache_put(key, payload):
    try:
        c = _yh_cache_load(); c[key] = {"ts": time.time(), "payload": payload}
        with open(_YH_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(c, f, ensure_ascii=False)
    except Exception as _e: _log_err(_e, "yh_cache_put")

def _yh_cache_get(key):
    rec = _yh_cache_load().get(key)
    if not rec: return None, None
    age = (time.time() - rec.get("ts", 0)) / 86400.0
    if age > _YH_CACHE_MAX_AGE_D: return None, age
    return rec.get("payload"), age

def yahoo_staleness():
    """報告層呼叫:回 {tag: reason};空 = 全部 live。"""
    return dict(_YH_STALE)

class _YHResp:
    """模擬 requests.Response,只需 .json()(快取回放用)。"""
    def __init__(self, data): self._d = data
    def json(self): return self._d

def _yahoo_get(url, timeout=8, tag=None, headers=None):
    """Yahoo 統一入口:代理預檢 → live 抓 → 失敗回退落盤快取(標 stale)。
    回 Response-like(有 .json())或 None。★契約與 requests.get 相容,呼叫點只改函式名。"""
    key = tag or url.split("/chart/")[-1].split("?")[0]
    if not _yh_proxy_ok():
        pl, age = _yh_cache_get(key)
        if pl is not None:
            _YH_STALE[key] = f"代理不可達·快取{age:.1f}天前"
            return _YHResp(pl)
        _YH_STALE[key] = "代理不可達·無快取"
        return None
    try:
        r = requests.get(url, headers=headers or {"User-Agent": "Mozilla/5.0"}, timeout=timeout)
        d = r.json()
        if d and d.get("chart", {}).get("result"):
            _yh_cache_put(key, d)
        return r
    except Exception as _e:
        pl, age = _yh_cache_get(key)
        if pl is not None:
            _YH_STALE[key] = f"{type(_e).__name__}·快取{age:.1f}天前"
            print(f"   ⚠️ [Yahoo回退] {key} live失敗({type(_e).__name__}) → 用{age:.1f}天前快取")
            return _YHResp(pl)
        _YH_STALE[key] = f"{type(_e).__name__}·無快取"
        return None
_DATA_SESSION.proxies = {"http": None, "https": None}

# [V22.27.4] 強制 IPv4 — 東財 push2his 不 serve IPv6, 但本機 OS 偏好 IPv6 直連(240e:...)
#   → 連上即被 reset(RemoteDisconnected), 11檔資金流全瞎。診斷 diag_push2_ipv4.py [B] 已確認:
#   強制 IPv4 後 push2his 回 200+klines。本系統資料源(東財/新浪/Yahoo經Clash/TWSE)皆 IPv4 可達,
#   且 Clash 代理走 127.0.0.1(IPv4 localhost), 故全域強制 IPv4 安全且根治。失敗則靜默不影響啟動。
# [V22.54] ★改為【啟動自動探測】,不再寫死 —— 治「同樣 RemoteDisconnected,方向卻相反」。
#   2026-07-22 實證:diag 腳本(不強制 IPv4)對 push2his/fflow/kline 全通(lmt 1~30、連發12次);
#   系統(強制 IPv4)同一 URL 卻 RemoteDisconnected。與 V22.27.4 當年的症狀【完全相同但方向相反】
#   → IP family 的可用性會隨使用者網路環境改變(使用者的 VPN 現為 IPv6),寫死任何一邊都會壞。
#   修:啟動時實地探測 IPv4 / 不干預 兩種模式,選能通的;皆不通則不干預(讓 OS 決定)。
#   可用環境變數覆寫:EM_FORCE_IPV4=1 強制 / =0 永不強制 / 未設=自動探測
try:
    import socket as _socket
    import urllib3.util.connection as _u3conn
    _GAI_ORIG = _u3conn.allowed_gai_family
    _mode = os.environ.get("EM_FORCE_IPV4", "auto").lower()

    def _probe_em(timeout=5):
        """輕量探測東財是否可達(回 True/False)。"""
        try:
            _r = requests.get(
                "https://push2his.eastmoney.com/api/qt/stock/fflow/kline/get"
                "?secid=1.588000&fields1=f1&fields2=f51,f52&klt=101&lmt=1"
                "&ut=b2884a393a59ad64002292a3e90d46a5&_=%d" % int(time.time() * 1000),
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                         "Referer": "https://quote.eastmoney.com/"},
                timeout=timeout, proxies={"http": None, "https": None})
            return _r.status_code == 200 and bool(
                ((_r.json().get("data") or {}).get("klines")) or [])
        except Exception:
            return False

    if _mode == "1":
        _u3conn.allowed_gai_family = lambda: _socket.AF_INET
        print("   ✅ [V22.54] 強制 IPv4(EM_FORCE_IPV4=1 指定)")
    elif _mode == "0":
        print("   ✅ [V22.54] 不干預 IP family(EM_FORCE_IPV4=0 指定)")
    else:
        _u3conn.allowed_gai_family = lambda: _socket.AF_INET
        if _probe_em():
            print("   ✅ [V22.54] 自動探測:強制 IPv4 可通 → 採用")
        else:
            _u3conn.allowed_gai_family = _GAI_ORIG
            if _probe_em():
                print("   ✅ [V22.54] 自動探測:★強制 IPv4 不通、不干預可通 → 已【關閉】強制 IPv4")
                print("      (V22.27.4 當年的環境已改變;寫死 IP family 會壞,故改自動探測)")
            else:
                print("   ⚠️ [V22.54] 自動探測:兩種模式東財皆不通 → 維持不干預,資金流將落 NULL")
except Exception as _e:
    print(f"   ⚠️ [V22.54] IP family 探測失敗(不影響啟動): {_e}")

# [V22.18.4] ETF資金流 run-level 快取(同proxy一次運行只抓一次,避免持倉戰局+詳情卡重複請求)
_FLOW_CACHE = {}

# [V22.24.10] 共享 daykline 原始klines快取(secid→klines)。防限流關鍵:
#   get_etf_5day_flow(5日資金流) 與 get_order_flow_breakdown(大小單) 打完全相同
#   端點(fflow/daykline, 僅lmt不同), 原本每檔持倉重複抓2次→翻倍請求量加速限流。
#   共享後每secid一run只抓一次, 持倉路徑push2his請求砍半(Claude Code .9漏掉的防復發)。
_DAYKLINE_CACHE = {}

# ═══════════════════════════════════════════════════════════════════
# [V22.43] 東財限流防護 —— 治 flow5_yi 7/16 起 100% NULL
#   ★根因(WB 診斷 + Claude 源碼確認):get_etf_5day_flow 用單次裸請求,
#     無節流/無重試/無 429·403 處理 → 7/15 起被東財限頻 → 回 None
#     → flow5_yi 落 NULL → 報告顯示 0.0 → 進場觸發條件④【無法檢核】。
#   實測:NULL 比率 7/10 為 4/17,7/16~7/20 為 15/15(100%);最後有值 2026-07-15。
#   ★WB 的 backfill 補得了歷史,但源頭不修則【明天再斷】(同 V22.38 分工)。
# ═══════════════════════════════════════════════════════════════════
_EM_LAST_CALL = [0.0]          # 上次請求時刻(list 便於閉包寫入)
_EM_PENALTY   = [0.0]          # 自適應懲罰秒數(遇 429/403 累加)
EM_MIN_INTERVAL = 1.0          # 最小請求間隔(秒)
EM_JITTER_MAX   = 0.8          # 抖動上限,避免固定節奏被識別
EM_MAX_RETRIES  = 4
EM_BACKOFF_BASE = 1.0
EM_ADAPTIVE_PENALTY = 5.0      # 每次 429/403 額外加的間隔

# [V22.51] fflow 端點備援鏈 —— 東財已封鎖 daykline,保留備援以防 kline 也被封
#   實測序(2026-07-22 本機):
#     ① push2his /fflow/kline        → 5 筆(最佳,現用)
#     ② push2delay /fflow/daykline   → 1 筆(延遲鏡像)
#     ③ push2      /fflow/kline      → 1 筆
#   若日後 ① 也被封,把 EM_FFLOW_PATH 換成 ②/③ 的組合即可。
# [V22.52] fflow 候選鏈(host, path, lmt)—— 依 2026-07-22 本機實測結果排序
#   ★lmt=5 實測可通;系統原用 lmt=10 失敗,逐字比對後 lmt 為唯一差異 → 一律用 5。
#   任一候選成功即採用,全失敗才落 NULL(誠實留空,不冒充)。
# [V22.53] 端點熔斷:同一 host+path 連續失敗達門檻 → 本輪跳過(治跑很久)
# [V22.63] 門檻由 2 → 4:15 檔迴圈中偶發 1-2 次失敗屬正常,不該讓整輪斷炊。
#   搭配「成功即重置」後,本計數代表【連續失敗次數】,4 次連敗才判端點真的死了。
EM_DEAD_THRESHOLD = 4
_EM_DEAD_EP = {}

EM_FFLOW_CANDIDATES = [
    ("push2his.eastmoney.com",   "fflow/kline",    5),   # 實測回 5 筆(最佳)
    ("push2delay.eastmoney.com", "fflow/daykline", 5),   # 延遲鏡像
    ("push2.eastmoney.com",      "fflow/kline",    5),
    ("push2his.eastmoney.com",   "fflow/kline",   10),   # 保留原設定當最後嘗試
]

_EM_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36")
_EM_WARMED = [False]

def _em_warmup(sess):
    """[V22.98] 首次東財請求前熱身:GET 首頁取 session cookie,後續請求自動帶(治 cookie 級反爬)。
    失敗不影響主流程。★對 IP 級限流無效(IP 限流按 IP 不按 cookie),僅治 cookie/UA 級反爬。"""
    if _EM_WARMED[0]:
        return
    _EM_WARMED[0] = True
    try:
        sess.get("https://quote.eastmoney.com/", timeout=5, headers={"User-Agent": _EM_UA})
    except Exception as _e:
        _log_err(_e, "_em_warmup:L871")  # [V24.16] 原為靜默 pass
        pass

def _em_throttled_get(url, session=None, timeout=6, tag="", headers=None, retries=None):
    """[V22.43→V22.44] 東財請求節流入口:串行節流 + 抖動 + 指數退避 + 429/403 自適應降頻。
    ★V22.43 我把它命名為「統一入口」但【只接了 flow5 一條路徑】,其餘 9 處仍是裸請求
      —— WB 核證點名:命名誇大覆蓋面 = 新的誤導源。我認,這與「已入記憶/已清理」同一種範圍誇大。
    ★V22.44 已將【全部 9 處】東財請求遷入本入口,涵蓋:
      NAV序列(RSI/trend來源)/日K/資金流/大盤/估值/ETF清單 等。
      若僅 flow5 受保護,東財同源限頻時 trend/RSI 也會 NULL → 進場觸發①②同樣失準(WB 指出)。
    回 requests.Response 或 None(全部重試失敗)。"""
    import time as _t, random as _rd
    sess = session or _DATA_SESSION
    _last = ["(無紀錄)"]   # [V22.50] 保留最後一次失敗實況供診斷
    _max = retries if retries else EM_MAX_RETRIES   # [V22.52] 備援鏈用較少重試,避免拖死
    # [V22.53 熔斷器] ★治「跑一次 11 分鐘」——根因是我自己的節流設計:
    #   節流1.0s+抖動0.8s(全域) × 重試4次 × 指數退避(1+2+4+8=15s) = 單端點全敗 20.6s;
    #   ×4 條備援鏈 ×15 檔持倉 ≈ 11 分鐘。且同一死端點【每檔 ETF 都重試一遍】。
    # [V22.64 治WB指出的殘留] 熔斷 key 原為 host+path(不含 secid)→ 15 檔共用一組 key,
    #   「一檔連坐 15 檔」。V22.63 靠「成功即重置+門檻4」間接壓住,未根治。現加 secid 徹底隔離;
    #   端點真死時各檔各自累計仍會分別熔斷,保護不丟失。
    _em_warmup(sess)   # [V22.98] 熱身取 cookie(治 cookie 級反爬)
    try:
        import re as _re
        _sid = _re.search(r"secid=([^&]+)", url)
        _ep = url.split("?")[0] + ("|" + _sid.group(1) if _sid else "")
    except Exception:
        _ep = url.split("?")[0]
    if _EM_DEAD_EP.get(_ep, 0) >= EM_DEAD_THRESHOLD:
        return None
    for attempt in range(_max):
        wait = EM_MIN_INTERVAL + _EM_PENALTY[0] - (_t.time() - _EM_LAST_CALL[0])
        if wait > 0:
            _t.sleep(wait)
        _t.sleep(_rd.uniform(0, EM_JITTER_MAX))
        _EM_LAST_CALL[0] = _t.time()
        try:
            _hd = headers or {"User-Agent": _EM_UA,
                              "Referer": "https://quote.eastmoney.com/"}
            r = sess.get(url, headers=_hd, timeout=timeout)
            if r.status_code in (429, 403):
                _EM_PENALTY[0] += EM_ADAPTIVE_PENALTY
                print(f"   ⚠️ [東財限流] {tag} HTTP {r.status_code} → 降頻(累計懲罰 {_EM_PENALTY[0]:.0f}s),"
                      f"重試 {attempt+1}/{EM_MAX_RETRIES}")
                _t.sleep(EM_BACKOFF_BASE * (2 ** attempt))
                continue
            if r.status_code == 200:
                # [V22.63 治我自己的熔斷器誤傷] ★成功必須重置計數。
                #   V22.53 的熔斷器【永不重置】→ 算的是「累計失敗」不是「連續失敗」;
                #   又因 key = host+path(不含 secid),15 檔持倉【共用同一組 key】。
                #   後果實證:前 2 檔暫時性失敗 → 後 13 檔【一次請求都沒發】→ 全落 NULL。
                #   這就是 7/23 午報「⬜無資料:主力流出擴大/收斂」的真因 —— 不是東財不給,
                #   是我把門關了,而公式因此缺了最重的 +20 因子,分數 20/50 未達門檻。
                _EM_DEAD_EP[_ep] = 0
                return r
            # [V22.50] 非 200 也要留證據(原本靜默重試 → 七天不知道真因)
            _last[0] = f"HTTP {r.status_code} body[:120]={(r.text or '')[:120]!r}"
        except Exception as _e:
            _log_err(_e, f"em_throttled_get.{tag}")
            _last[0] = f"{type(_e).__name__}: {str(_e)[:160]}"
            # [V22.53] 硬失敗退避無用 —— 退避是給【限頻】的;連線被拒等再久也不通
            if isinstance(_e, requests.exceptions.ConnectionError):
                break
            _t.sleep(EM_BACKOFF_BASE * (2 ** attempt))
    # [V22.50 治我自己的診斷盲區] ★原本只印「N 次重試全失敗」,不印狀態碼/回應內容
    #   → flow5 斷線七天,始終不知道真因是限頻? token 失效? IP 被擋? DNS?
    #   這是把「不知道為什麼失敗」偽裝成「已知的限流」。現在把最後一次失敗的實況印出來。
    _EM_DEAD_EP[_ep] = _EM_DEAD_EP.get(_ep, 0) + 1
    if _EM_DEAD_EP[_ep] == EM_DEAD_THRESHOLD:
        print(f"   ⛔ [熔斷] {_ep[-46:]} 連續失敗 {EM_DEAD_THRESHOLD} 次 → 本輪不再嘗試")
    if _EM_DEAD_EP[_ep] > EM_DEAD_THRESHOLD:
        return None
    print(f"   ❌ [東財請求失敗] {tag} {_max} 次全敗 → 落 NULL(誠實留空)")
    print(f"      └ 最後一次失敗實況:{_last[0]}")
    print(f"      └ 診斷提示:HTTP 403/429=被擋或限頻 | ConnectionError=網路/IP層 | "
          f"200但 klines 空=ut token 可能失效 | Timeout=線路慢")
    return None

# [V22.17.3] 統一錯誤日誌（解決 except+pass 黑洞）
# 預設靜默（fallback 邏輯不污染終端）；--verbose 或環境變數 ETF_VERBOSE=1 開啟
VERBOSE_ERRORS = os.getenv("ETF_VERBOSE", "0") == "1"

def _log_err(exc, ctx=""):
    """統一錯誤日誌，verbose 模式才印出（取代 except: pass 黑洞）"""
    if VERBOSE_ERRORS:
        print(f"   ⚠️ [{ctx}] {type(exc).__name__}: {exc}")
warnings.filterwarnings('ignore')

__VERSION__ = "V24.30"


def _version_consistency_check():
    """[V24.25] 版本一致性自檢 — 檔名 / docstring / __VERSION__ 三者比對。

    ★使用者 2026-08-10:「抬頭寫 V24 19 etf system，版本寫 V22.39，這對嗎??」
      不對。三者角色如下,判定版本【一律看 __VERSION__】:
        __VERSION__ : 唯一權威,報告標題與輸出檔名前綴用它
        檔名        : 主版本標記,允許落後(改檔名會打斷使用者的既有指令)
        docstring   : 變更歷史堆疊,最上面是最新一筆變更標題,不是檔案版本
      另一條線(HANDOFF 2026-08-07)已實測 5 支檔案檔名與內容不符,
      **「以為在跑 V21.42、實際載入 V21.41」會讓所有修補靜默失效**——
      所以這個落差必須【每次啟動就講出來】,不能靠人記得去比對。
    """
    try:
        _fn = os.path.basename(os.path.abspath(__file__))
        _mfn = _re_ver.search(r"V(\d+)[._](\d+)", _fn)
        _fnv = f"V{_mfn.group(1)}.{_mfn.group(2)}" if _mfn else "?"
        _src_head = ""
        try:
            with open(os.path.abspath(__file__), "r", encoding="utf-8",
                      errors="replace") as _fh:
                _src_head = "".join([next(_fh) for _ in range(60)])
        except Exception:
            pass
        _mds = _re_ver.search(r"版本[：:]\s*(V[\d.]+)", _src_head)
        _dsv = _mds.group(1) if _mds else "?"
        _bad = [x for x in ((_fnv, "檔名"), (_dsv, "docstring"))
                if x[0] != "?" and not __VERSION__.startswith(x[0])]
        if _bad:
            return (f"   ℹ️ 版本標示落差(不影響執行,權威=__VERSION__ {__VERSION__}): "
                    + "、".join(f"{n}={v}" for v, n in _bad)
                    + " → 判定版本請一律看 __VERSION__ 與報告標題")
        return ""
    except Exception:
        return ""

# [V22.11+ 追蹤表] openpyxl 可選依賴，沒裝也不會崩
try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    _OPENPYXL_OK = True
except ImportError:
    _OPENPYXL_OK = False
    print("⚠️ openpyxl 未安裝，追蹤表功能不可用。安裝：pip install openpyxl")

try:
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
except Exception as _e:
    _log_err(_e, "_log_err")

print("=" * 90)
print(f"* {__VERSION__} ETF 九龍合璧版 | 直連引擎 + 邏輯護欄 + Kalman學習")
_vc = _version_consistency_check()   # [V24.25] 版本落差每次啟動就講出來
if _vc:
    print(_vc)
print("=" * 90)

# ══════════ 路徑配置 ══════════
LOG_DB_NAME = "v20_prediction_log.db"


def _resolve_base_dir(env=None, script_parent=None, home=None):
    r"""決定輸出/資料資料夾,回 (路徑, 選擇理由)。
    1. 環境變數 ETF_OUTPUT_DIR(一律優先);
    2. 程式所在資料夾的上一層 —— 若裡面有歷史資料庫(作者配置:ETF_Monitor\代碼\本檔);
    3. 桌面的 ETF_Monitor —— 若裡面有歷史資料庫(程式放在別處,例如 git 倉庫的 src\ 時);
    4. 都沒有 → 程式所在資料夾的上一層,並明說「尚無歷史資料庫」。
    原本只有 1 和 4:程式放在 ...\ETF_Monitor_Repo\src\ 時會靜默地從空白歷史開始。"""
    if env:
        return env, "環境變數 ETF_OUTPUT_DIR"
    cands = [(script_parent, "程式所在資料夾的上一層"),
             (os.path.join(home, "Desktop", "ETF_Monitor"), "桌面 ETF_Monitor 資料夾")]
    for d, why in cands:
        if d and os.path.exists(os.path.join(d, LOG_DB_NAME)):
            return d, why
    return script_parent, "程式所在資料夾的上一層(尚無歷史資料庫,將從空白歷史開始)"


def _register_history_status(existed, base_dir):
    """本次執行開始時,資料夾裡若沒有歷史資料庫,登記到報告頂部「本次資料不可用」。"""
    if not existed:
        _data_unavailable("DATA_HISTORY",
                          f"資料夾 {base_dir} 沒有 {LOG_DB_NAME},本報從空白歷史開始。"
                          f"若你的資料在別處,請設定環境變數 ETF_OUTPUT_DIR 指向該資料夾")


BASE_DIR, BASE_DIR_REASON = _resolve_base_dir(
    env=os.getenv("ETF_OUTPUT_DIR"),
    script_parent=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    home=os.path.expanduser("~"))
os.makedirs(BASE_DIR, exist_ok=True)
print(f"   [資料夾] {BASE_DIR}  ← {BASE_DIR_REASON}")
DB_FILE       = os.path.join(BASE_DIR, "v20_brain.json")
LOG_DB        = os.path.join(BASE_DIR, LOG_DB_NAME)
_LOG_DB_EXISTED_AT_START = os.path.exists(LOG_DB)   # 之後 _init_db 會建立空表,須在此之前記錄


_PRICE_SKIPPED = []   # 本次執行因價格取不到而未納入報告的持倉/標的
_WATCH_SKIPPED = []   # 同上(觀察池)


def _history_present(db_path):
    """資料庫裡是否真的有歷史(predictions 至少一筆)。
    只看檔案存在不夠:import 本檔(例如只跑 --help)就會建立空資料庫,
    之後第一次真正出報告時便不再提示「從空白歷史開始」(2026-10-15 審查實測)。"""
    if not os.path.exists(db_path):
        return False
    try:
        with sqlite3.connect(db_path, timeout=10) as _c:
            return _c.execute("SELECT 1 FROM predictions LIMIT 1").fetchone() is not None
    except Exception:
        return False
POSITION_FILE = os.path.join(BASE_DIR, "v22_positions.json")
REPORT_DIR    = os.path.join(BASE_DIR, "reports")
OVERRIDE_FILE = os.path.join(BASE_DIR, "overnight_override.json")  # [V22.25.9] 鮮數據覆寫檔


def load_overnight_override():
    """[V22.25.9] 讀鮮數據覆寫檔(WebSearch來源),TTL內才生效。
    根治盲區:A股盤前/早報跑在週末/隔夜美股休市時,Yahoo回stale值(如VIX停在週五),
    抓不到週末事件(荷莫茲再封/油急漲)。WebSearch鮮數據經此檔注入,取代stale Yahoo值。
    回傳 (overrides:dict, meta:dict|None)。overrides 只含非None欄位(null=用實時)。
    支援鍵:vix/ndx/sox/gold/wti/wti_ret/gold_ret。"""
    if not os.path.exists(OVERRIDE_FILE):
        return {}, None
    try:
        with open(OVERRIDE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        ttl = float(data.get("ttl_hours", 18))
        as_of_raw = data.get("as_of", "")
        try:
            as_of = datetime.datetime.fromisoformat(as_of_raw)
            now = datetime.datetime.now(as_of.tzinfo) if as_of.tzinfo else datetime.datetime.now()
            age_h = (now - as_of).total_seconds() / 3600.0
        except Exception:
            age_h = 0.0  # 時間解析失敗不因時效擋下,但下方記warning
            print(f"   [鮮數據覆寫] ⚠️ as_of 解析失敗('{as_of_raw}'),略過TTL檢查")
        if age_h > ttl:
            print(f"   [鮮數據覆寫] ⏰ 已過期 {age_h:.1f}h > {ttl}h,忽略(改用實時Yahoo)")
            return {}, {"expired": True, "age_h": age_h}
        # [V22.33.8 治契約斷裂·WB核證抓到] 原本【只】讀巢狀 data["overrides"];若檔案寫成頂層鍵
        #   → ov={} → 呼叫點 `if _ov:` 為 False → apply_overnight_override 根本不被呼叫
        #   → 整份 override 死(連 gold/wti 非錨鍵也死)。V33.7 出貨的範本正是頂層格式=自打臉。
        # 修法:兩種 schema 都吃(巢狀優先;無巢狀則取頂層非保留鍵),並讓 meta 帶 as_of_us。
        _RESERVED = {"as_of", "as_of_us", "ttl_hours", "source", "note", "_USAGE"}
        _raw = data.get("overrides")
        if not _raw:
            _raw = {k: v for k, v in data.items() if k not in _RESERVED}
        ov = {k: v for k, v in (_raw or {}).items() if v is not None}
        meta = {"as_of": as_of_raw, "source": data.get("source", ""),
                "note": data.get("note", ""), "age_h": age_h,
                # as_of_us 可能寫在頂層或 overrides 內,兩處都撈(治 meta 死碼)
                "as_of_us": data.get("as_of_us") or (data.get("overrides") or {}).get("as_of_us")}
        return ov, meta
    except Exception as _e:
        _log_err(_e, "load_overnight_override")
        return {}, None


_ANCHOR_OV_KEYS = {"sox", "ndx", "vix"}   # [V22.33.7] 隔夜錨鍵:覆寫前須驗證

def apply_overnight_override(vals: dict, ov: dict, meta: dict = None):
    """[V22.25.9] 原地用覆寫值替換 live 值;回傳 provenance 列(供報告透明化)。
    [V22.33.7 治連三日隔夜錨凍結] ★根因:本函式原本【無條件】覆寫,把日期釘選的 pin 直接輾過,
      且只驗「檔案新不新(18h TTL)」、不驗「值代表哪個美股交易日」→ 時間戳更新但值是舊的就照過。
      實證:7/15 午報 SOX 退格、7/16-7/17 四報 SOX 凍結在 -2.08%(真實 7/16 美盤跌逾4%),連三日污染宏觀輸入。
    修法:隔夜錨(sox/ndx/vix)覆寫前,override 必須聲明 `as_of_us`(其值代表的美股交易日),
      且須等於今日 A股對應的預期美股交易日;不符或未聲明 → 【拒絕覆寫錨,保留 pin 值】並在報告紅字標。
      (非錨鍵如 gold/wti 不受此限,維持原行為=零回歸。)
    ⚠️ 修 override 檔:在 overnight_override.json 加 "as_of_us": "YYYY-MM-DD"(該值的美股收盤日)。
    """
    prov = []
    try:
        _exp_us = _expected_latest_us_date(_now_utc8().strftime("%Y-%m-%d"))
    except Exception as _e:
        _log_err(_e, "apply_override.exp_us"); _exp_us = None
    _as_of_us = (ov or {}).get("as_of_us") or (meta or {}).get("as_of_us")
    _anchor_ok = bool(_exp_us) and (_as_of_us == _exp_us)
    for k, v in (ov or {}).items():
        if k == "as_of_us":
            continue
        if k in vals:
            if k in _ANCHOR_OV_KEYS and not _anchor_ok:
                prov.append(f"⛔ {k} 覆寫【已拒絕】:override as_of_us={_as_of_us or '未聲明'} "
                            f"≠ 預期美股交易日 {_exp_us} → 保留釘選錨值 {vals[k]}(治錨凍結)")
                continue
            prov.append(f"{k}={v}(覆寫,原{vals[k]})")
            vals[k] = v
    if (ov or {}) and not _anchor_ok:
        print(f"   [鮮數據覆寫] ⛔ 隔夜錨(sox/ndx/vix)覆寫已拒絕:as_of_us={_as_of_us or '未聲明'} "
              f"≠ {_exp_us}。請在 overnight_override.json 加正確的 as_of_us 後重跑。")
    return prov


def check_position_anchor_freshness(max_age_days: int = 14):
    """[V22.33.9 治D10] 持倉錨點時效 + 交易流水存在性檢查。
    ★根因(2026-07-19 實證):positions_opening.csv 錨點停在 2026-06-12(逾1個月),
      且 transactions.csv 【不存在】→ 7/14 買入 008888 +300、以及 004432 +300 兩筆從未進系統
      → 系統成本基準 008888=713.64 vs APP 真實 1013.64(差額正好 300),現值/佔比/加減倉全偏。
      當時報告只印「現值 1539」不印錨點日期,所以錯了一個月沒人發現(同款「不可見即不會被抓」)。
    回傳警告字串(空=正常)。純唯讀檢查,失敗不拋例外。"""
    warns = []
    try:
        base = os.path.dirname(os.path.abspath(__file__))
        op = os.path.join(base, "positions_opening.csv")
        if not os.path.exists(op):
            return "⚠️ 持倉錨點 positions_opening.csv 不存在 → 持倉數據不可信"
        anchor = None
        with open(op, encoding="utf-8-sig") as f:
            for line in f:
                if line.startswith("#"):
                    mm = __import__("re").search(r"基準日\s*(\d{4}-\d{2}-\d{2})", line)
                    if mm:
                        anchor = mm.group(1); break
                else:
                    break
        if anchor:
            age = (_now_utc8().date() - datetime.date.fromisoformat(anchor)).days
            if age > max_age_days:
                warns.append(f"🔴 持倉錨點已 {age} 天未重錨(基準日 {anchor} > {max_age_days}天)"
                             f" → 現值/成本/佔比恐失真,請用最新 APP 截圖重錨")
            else:
                warns.append(f"🕐 持倉錨點基準日 {anchor}({age}天前) ✅")
        else:
            warns.append("⚠️ positions_opening.csv 未標示『基準日 YYYY-MM-DD』→ 無法驗時效")
        _txn = os.path.join(base, "transactions.csv")
        if not os.path.exists(_txn):
            warns.append("🔴 transactions.csv 不存在 → 錨點後的申購/贖回【完全未登錄】")
        else:
            # [V22.34 治WB缺陷B] 原本只驗 os.path.exists → 【空白檔(只有表頭)照樣過關】,
            #   保護靜默、漏登交易繼續被掩蓋。實證:008888/004432 各 300 元漏登時,txn 檔是存在的。
            try:
                with open(_txn, encoding="utf-8-sig") as _f:
                    _rows = [l for l in _f if l.strip() and not l.lstrip().startswith("#")
                             and not l.lower().lstrip().startswith("date,")]
            except Exception:
                _rows = []
            if not _rows:
                warns.append("🔴 transactions.csv 存在但【無任何交易資料列】→ 錨點後申購/贖回仍未登錄"
                             "(2026-07 實證:008888/004432 各 300 元漏登,成本基準錯 600 元)")
            else:
                warns.append(f"📒 transactions.csv 有 {len(_rows)} 筆交易列")
    except Exception as _e:
        _log_err(_e, "check_position_anchor_freshness")
        return ""
    return " | ".join(warns)


def check_tracker_freshness(max_age_days: int = 3):
    """[V22.25.10] 四報追蹤表時效警報 — 防止靜默停更(脆弱真相源)。
    每報啟動時呼叫,追蹤表超過 max_age_days 未更新就在報告示警。回傳警告字串(空=正常)。"""
    if not _OPENPYXL_OK:
        print("   [追蹤表] ⚠️ openpyxl 未裝,本次收盤報未寫入追蹤表")
        return "⚠️ openpyxl 未裝,四報追蹤表停用(pip install openpyxl)"
    path = os.path.join(BASE_DIR, "V22_13_四報追蹤表.xlsx")
    if not os.path.exists(path):
        return "⚠️ 四報追蹤表檔案不存在(從未產生過)"
    try:
        wb = load_workbook(path, data_only=True, read_only=True)
        ws = wb["每日四報追蹤"] if "每日四報追蹤" in wb.sheetnames else wb[wb.sheetnames[0]]
        last_date = None
        for r in range(ws.max_row, 1, -1):
            v = ws.cell(r, 1).value
            if v and len(str(v)) >= 10 and str(v)[:4].isdigit():
                last_date = str(v)[:10]
                break
        wb.close()
        if not last_date:
            return "⚠️ 四報追蹤表無有效日期列"
        age = (datetime.date.today() - datetime.date.fromisoformat(last_date)).days
        if age > max_age_days:
            return (f"⚠️ 四報追蹤表已 {age} 天未更新(最後={last_date}) — "
                    f"只有收盤報會寫入,請確認收盤報有跑完且 openpyxl 正常")
        return ""
    except Exception as _e:
        _log_err(_e, "check_tracker_freshness")
        return f"⚠️ 四報追蹤表時效檢查失敗: {type(_e).__name__}"
os.makedirs(REPORT_DIR, exist_ok=True)

KELLY_HALF       = 0.5
KELLY_MAX_SINGLE = 0.25
KELLY_MAX_STYLE  = 0.40
# Kelly 輸入改用【可交易口徑】的訊號報酬:15:00 截單前(早/午報)的訊號 → 次一交易日實際報酬。
#   早/午報的 predicted_ret 已含當日盤中走勢,拿它對「當日」報酬算的命中率是同期推估(nowcast),
#   data_sample 實測 A股 80~94%,但對次一交易日僅約 50~54% —— 後者才是場外基金下單後真正拿到的。
SIGNAL_PAYOFF_WINDOW_DAYS = 90   # 統計視窗(日曆日)
SIGNAL_PAYOFF_MIN_DAYS    = 30   # 有效樣本以「交易日數」計(同日多檔高度連動,不以筆數計);不足→最小倉位
SIGNAL_PAYOFF_MAX_GAP     = 7    # 次一筆報酬距訊號日超過 7 個日曆日(系統停跑)則不配對

# [V22.17.0 新增] RSI 閾值統一常數（魔法數字常數化）
# 散落在 L1235/L1241/L1497/L3291/L3484 等多處的 RSI 比較統一管理
RSI_OVERSOLD     = 32   # 超賣門檻（保護減倉降級）
# [V22.36.2] 超賣護欄的趨勢豁免門檻:trend < 此值 → 視為真實下跌趨勢,不套用超賣護欄
#   (均值回歸只在震盪市有效;跌勢中超賣更超賣=接下墜的刀。008888 7/13-7/17 實證)
#   ⚠️ -0.30 為保守佔位值,待與 DCAP_REDUCE_STREAK_N 一併用 actual_returns 回測校準(⑳)
# [V22.41 回測後誠實標記] 本值【仍未校準】,且 2026-07-20 回測顯示【方向存疑】:
#   減倉信號 × trend 分桶(35交易日庫)→ trend<=-0.8 桶 T+1~5 = +4.69%(n=25)、
#   trend>=0 桶 = -3.12%(n=19) → 表面上與本豁免的假設【相反】(跌勢中反而該持有)。
#   ★但該 n=25 全部集中在 2026-05-29~06-11 的 7 個日期 × 7 檔 = 【單一六月反彈事件】,
#     真實獨立觀測 ≈1(偽重複);008888 在同桶內結果分裂(-4.07%~+11.92%)。
#   → 依㉘:**不足以支持,也不足以推翻**。維持豁免(保守方向:讓風險信號至少能被看見),
#     並在此明標「未經驗證」。待樣本涵蓋 ≥3 個獨立 regime 後重測。
OVERSOLD_GUARD_TREND_MIN = -0.30   # ⚠️未校準·方向存疑(見上)
RSI_NEUTRAL_LO   = 45   # 中性下限（觀察池進場 RSI 門檻）
RSI_NEUTRAL_HI   = 55   # 中性上限
RSI_OVERBOUGHT   = 70   # 超買門檻
RSI_EXTREME_HI   = 85   # 極端超買（早報懲罰上邊界）
RSI_EXTREME_LO   = 60   # 早報懲罰下邊界
RSI_RSI90_KILL   = 90   # 強制止盈

# [V22.17.0 新增] BIAS 閾值常數
BIAS_THRESHOLD   = 0.15  # 乖離率正常範圍上限

# [V22.18 改造] LEGACY 持倉(PositionBook 回退與對拍基準)。
#   個人持倉金額不寫在原始碼裡:改由與本檔同目錄(或 BASE_DIR)的 personal_holdings.json 提供,
#   該檔不納入公開版本。檔案不存在 → 兩者皆為空字典,PositionBook 仍以 DB / CSV 為準。
#   格式見 personal_holdings.example.json。
PERSONAL_HOLDINGS_FILE = "personal_holdings.json"


def _load_personal_holdings(search_dirs):
    """回 (holdings, costs, 來源路徑或 None)。檔案損毀時告警並回空,不拋例外。"""
    for _d in search_dirs:
        _p = os.path.join(_d, PERSONAL_HOLDINGS_FILE)
        if not os.path.exists(_p):
            continue
        try:
            with open(_p, encoding="utf-8") as _f:
                _j = json.load(_f)
            return dict(_j.get("holdings") or {}), dict(_j.get("costs") or {}), _p
        except Exception as _e:
            _log_err(_e, "load_personal_holdings")
            print(f"   ⚠️ {_p} 讀取失敗({type(_e).__name__}),LEGACY 持倉改為空")
            return {}, {}, None
    return {}, {}, None


_LEGACY_USER_HOLDINGS, _LEGACY_USER_COSTS, _PERSONAL_HOLDINGS_SRC = _load_personal_holdings(
    [os.path.dirname(os.path.abspath(__file__)), BASE_DIR])

# [V22.17.5 新增 5/30] 交易日判斷
# 排除周末 + 預設節假日（可在代碼修改 MANUAL_HOLIDAYS 補充）

# 2026 年中國 A 股節假日（國務院辦公廳通知，2026 年休市安排）
# 來源：上交所/深交所公告，用戶可在程式更新時手動補充
CN_HOLIDAYS = {
    "2026-01-01",  # 元旦
    "2026-02-16","2026-02-17","2026-02-18","2026-02-19",
    "2026-02-20","2026-02-23","2026-02-24",  # 春節
    "2026-04-03","2026-04-06",  # 清明節
    "2026-05-01","2026-05-04","2026-05-05",  # 勞動節
    "2026-06-19","2026-06-20","2026-06-21",  # 端午節（五~日）
    "2026-09-25",  # 中秋節
    "2026-10-01","2026-10-02","2026-10-05","2026-10-06",
    "2026-10-07","2026-10-08","2026-10-09",  # 國慶節
}

# 2026 年台股節假日（證交所公告）
TW_HOLIDAYS = {
    "2026-01-01",  # 元旦
    "2026-02-16","2026-02-17","2026-02-18","2026-02-19","2026-02-20","2026-02-23",  # 春節
    "2026-02-27","2026-03-02",  # 228
    "2026-04-03","2026-04-06",  # 兒童/清明
    "2026-05-01",  # 勞動節
    "2026-06-19",  # 端午
    "2026-09-25",  # 中秋
    "2026-10-09","2026-10-12",  # 國慶
}

def is_trading_day(date_obj=None, market="A股"):
    """
    [V22.17.5 新增] 判斷是否為交易日
    
    Args:
        date_obj: datetime.date / datetime.datetime / 'YYYY-MM-DD' / None=今天
        market: "A股" / "台股" / "美股"
    Returns:
        bool: True=交易日, False=非交易日
    """
    if date_obj is None:
        date_obj = datetime.date.today()
    elif isinstance(date_obj, str):
        date_obj = datetime.date.fromisoformat(date_obj)
    elif isinstance(date_obj, datetime.datetime):
        date_obj = date_obj.date()
    
    # 1. 排除週末（0=Mon, 5=Sat, 6=Sun）
    if date_obj.weekday() >= 5:
        return False
    
    # 2. 排除節假日
    iso = date_obj.isoformat()
    holidays = CN_HOLIDAYS if market == "A股" else (TW_HOLIDAYS if market == "台股" else set())
    if iso in holidays:
        return False
    return True


def get_non_trading_reason(date_obj=None):
    """[V22.17.5] 取得非交易日原因（用於用戶提示）"""
    if date_obj is None:
        date_obj = datetime.date.today()
    elif isinstance(date_obj, str):
        date_obj = datetime.date.fromisoformat(date_obj)
    elif isinstance(date_obj, datetime.datetime):
        date_obj = date_obj.date()
    
    weekday_names = ["週一","週二","週三","週四","週五","週六","週日"]
    iso = date_obj.isoformat()
    
    if date_obj.weekday() == 5:
        return f"{weekday_names[5]}（週末）"
    if date_obj.weekday() == 6:
        return f"{weekday_names[6]}（週末）"
    if iso in CN_HOLIDAYS:
        return f"A股節假日"
    if iso in TW_HOLIDAYS:
        return f"台股節假日"
    return "未知（交易日）"


# [V22.17.4] 持倉成本紀錄 _LEGACY_USER_COSTS(buy=累計買入 / sell=累計贖回 / current=現值)
#   已改由 personal_holdings.json 載入(見上方 _load_personal_holdings),不寫在原始碼裡。

# ════════════════════════════════════════════════════════════════
# [V22.18 新增] PositionBook 自動持倉引擎
# ════════════════════════════════════════════════════════════════
# 期初快照(positions_opening.csv) + 交易流水(transactions.csv)
#   → 自動生成 USER_HOLDINGS / USER_COSTS（變數名不變，下游零修改）
# 過渡：與 _LEGACY 並存對拍，數字不符告警（一週無誤後可刪 LEGACY）
# 容錯：CSV 缺失/壞行 → 回退 _LEGACY，絕不崩潰
# 每次交易只需在 transactions.csv 加一行，不再手改代碼
# ════════════════════════════════════════════════════════════════
_PB_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# [任務C 重構 phase 1] 以下 44 個純函式已原樣移至 src/ 下的模組(函式本體字元級一致,
#   見 docs/REFACTOR_plan.md);它們不讀取本檔任何全域狀態。以原名匯入,外部介面不變。
import sys as _rf_sys
_rf_here = os.path.dirname(os.path.abspath(__file__))
if _rf_here not in _rf_sys.path:
    _rf_sys.path.insert(0, _rf_here)
from data_source import (  # noqa: E402
    sanitize_price_series, _series_to_returns_by_date, _align_overnight,
    _now_utc8, _clamp_snapshot_trend, _expected_latest_us_date,
    compute_pinned_ret, _is_qdii_etf, _proxy_to_secid,
    _ae_nth_weekday, _ae_adjust_weekend,
)
from signals import (  # noqa: E402
    compute_fragility, fragility_action, identify_state_actor,
    interpret_flow_behavior, predict_tomorrow, effective_sox_for,
    judge_order_flow, compute_realized_vol, vol_scale_factor,
    factor_attribution, analyze_3day_trend, analyze_cny_dual_track,
    analyze_gold_oil_seesaw, analyze_tech_concentration, _pearson,
    _spearman, _action_to_symbol, _compute_judgment,
    derive_c_drain,
)
from position_book import (  # noqa: E402
    _pb_status_from_pct, _pb_parse_float, positionbook_diff_audit,
    classify_position_status,
)
from reporting import (  # noqa: E402
    format_closing_handoff, format_morning_diag_block, _auto_position,
    _geo_lbl_for_style, _inline_md_format, format_events_block,
    generate_today_focus, _pw_freshness_tag, _pw_qualitative_display,
    _geo_icon_from_cdrain,
)


def _pb_mark_to_market(positions, anchor_date):
    """[V24.11] lot-based mark-to-market。每筆錢從【自己的買入日】起算,不再全部套錨點日。

    ★V24.10 及之前的 bug(WB 2026-08-04 實跑暴露):
      舊版對所有部位用同一個 anchor_date 起算,但交易金額在此之前已併入 current。
      → 019072 於 7/27 才建倉 500+8/04 200,卻被套上 6/1~7/26 的報酬,
        700 元憑空變 575.3(-17.8%),而該部位真實持有期根本涵蓋不到那段跌幅。
      lots 結構:[(起算日, 金額)];錨點值為一個 lot,每筆申購/買入各為一個 lot;
      贖回按比例等比縮減所有 lot(場外贖回是按份額比例,不指定批次)。
    回傳 (套用檔數, 訊息)。失敗不拋例外, 現值維持原值。
    """
    if not anchor_date:
        return 0, "ℹ️ 無錨點日期, 跳過mark-to-market(現值維持快照)"
    applied = 0
    try:
        import sqlite3 as _sq
        with _sq.connect(LOG_DB, timeout=10) as conn:
            for code, p in positions.items():
                lots = p.get("lots") or [(anchor_date, p.get("current", 0.0))]
                rows = conn.execute(
                    "SELECT date(ts) d, actual_ret FROM predictions "
                    "WHERE code=? AND actual_ret IS NOT NULL AND date(ts) > ? "
                    "GROUP BY date(ts) ORDER BY d", (code, anchor_date)).fetchall()
                if not rows:
                    continue
                new_total, moved = 0.0, False
                for lot_date, lot_amt in lots:
                    factor = 1.0
                    for _d, ret in rows:
                        if ret is not None and _d > lot_date:
                            factor *= (1.0 + ret)
                    if factor != 1.0:
                        moved = True
                    new_total += lot_amt * factor
                if moved:
                    p["current"] = round(new_total, 2)
                    p["lots"] = [(anchor_date, p["current"])]  # 累乘後收斂成單一 lot
                    applied += 1
    except Exception as e:
        return 0, f"⚠️ mark-to-market失敗({e}), 現值維持錨點快照"
    return applied, f"✅ mark-to-market(lot-based): {applied}檔按各自買入日累乘至最新交易日"


def _pb_window_return(code, n_days):
    """[V22.23.5] 從DB取該檔最近n_days個交易日的actual_ret, 累乘算區間報酬%。
    取代成本反推%(對定投+大額申贖的churn持倉會嚴重失真, 如黃金+1256%)。
    一日多報(早/午/收)值相同→GROUP BY日期去重, 不重複累乘。
    資料不足(無actual_ret)回None, 由呼叫端回退成本法。
    """
    try:
        import sqlite3 as _sq
        with _sq.connect(LOG_DB, timeout=10) as conn:
            rows = conn.execute(
                "SELECT date(ts) d, actual_ret FROM predictions "
                "WHERE code=? AND actual_ret IS NOT NULL "
                "GROUP BY date(ts) ORDER BY d DESC LIMIT ?", (code, n_days)).fetchall()
    except Exception:
        return None
    if not rows:
        return None
    factor = 1.0
    for _d, ret in rows:
        if ret is not None:
            factor *= (1.0 + ret)
    return round((factor - 1.0) * 100, 1)

# 區間視窗(交易日): 近1月≈21, 近3月≈63
PB_WINDOW_1M = 21
PB_WINDOW_3M = 63


def _migrate_positions_to_db(base_dir):
    """[V22.27.7 FixB] 一次性將CSV持倉數據遷入SQLite。冪等: DB有資料則跳過。
    遷移內容: positions_opening.csv(錨點快照) + transactions.csv(交易流水)。
    """
    import sqlite3 as _sq
    # [V24.22] ★檔名容錯。使用者本機實際檔名為 positions_opening_reanchor_20260717.csv,
    #   系統只找 positions_opening.csv → 找不到就【回退 LEGACY 硬編碼】,
    #   帳務重建等於一直沒生效(早報某檔現倉 = LEGACY 值 + MTM,即為佐證)。
    #   處置:主檔名優先;沒有則取 positions_opening*.csv 最新一個,並【明說用了哪個】。
    import glob as _glob
    opening_path = os.path.join(base_dir, "positions_opening.csv")
    _picked_alt = None
    if not os.path.exists(opening_path):
        _alts = sorted(_glob.glob(os.path.join(base_dir, "positions_opening*.*")))
        _alts = [a for a in _alts if os.path.splitext(a)[1].lower() in (".csv", ".xlsx", ".xls")]
        if _alts:
            opening_path = _alts[-1]
            _picked_alt = os.path.basename(opening_path)
    txn_path = os.path.join(base_dir, "transactions.csv")
    try:
        with _sq.connect(LOG_DB, timeout=10) as conn:
            # 檢查是否已有資料(冪等)
            existing = conn.execute("SELECT COUNT(*) FROM position_book").fetchone()[0]
            if existing > 0:
                print("   [持倉DB] position_book 已有資料，跳過遷移")
                return "✅ 持倉DB已有資料(跳過遷移)"
            if not os.path.exists(opening_path):
                return ("⚠️ 無 positions_opening.csv/.xlsx，無法遷移 — 搜尋目錄: "
                        + base_dir)
            anchor_date = None
            count = 0
            with open(opening_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith("#"):
                        m = __import__("re").search(r"基準日\s*(\d{4}-\d{2}-\d{2})", line)
                        if m:
                            anchor_date = m.group(1)
                        continue
                    if line.startswith("code,"):
                        continue
                    parts = line.split(",")
                    if len(parts) < 6:
                        continue
                    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    conn.execute(
                        "INSERT OR REPLACE INTO position_book(code,name,buy,sell,current,status,anchor_date,updated_at)"
                        " VALUES(?,?,?,?,?,?,?,?)",
                        (parts[0].strip(), parts[1].strip(),
                         float(parts[2]) if parts[2].strip() else 0,
                         float(parts[3]) if parts[3].strip() else 0,
                         float(parts[4]) if parts[4].strip() else 0,
                         parts[5].strip(), anchor_date or "", now))
                    count += 1
            if os.path.exists(txn_path):
                txn_count = 0
                with open(txn_path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or line.startswith("date,"):
                            continue
                        parts = line.split(",")
                        if len(parts) < 6:
                            continue
                        conn.execute(
                            "INSERT INTO transactions(date,code,action,shares,nav,amount,created_at)"
                            " VALUES(?,?,?,?,?,?,?)",
                            (parts[0].strip(), parts[1].strip(), parts[2].strip(),
                             float(parts[3]) if parts[3].strip() else 0,
                             float(parts[4]) if parts[4].strip() else 0,
                             float(parts[5]) if parts[5].strip() else 0,
                             datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                        txn_count += 1
                print(f"   [持倉DB] 遷入 {count} 檔錨點 + {txn_count} 筆交易")
                return f"✅ 持倉DB遷入: {count}檔錨點 + {txn_count}筆交易"
            print(f"   [持倉DB] 遷入 {count} 檔錨點(無交易流水)")
            return f"✅ 持倉DB遷入: {count}檔錨點"
    except Exception as e:
        return f"⚠️ 持倉DB遷移失敗: {e}"


def _pb_read_rows(path):
    """[V24.23] 期初快照列讀取器 — 同時支援 .csv 與 .xlsx/.xls。

    ★根因(使用者 2026-08-10 截圖):本機 代碼資料夾內 positions_opening 的類型是【XL】,
      也就是 **positions_opening.xlsx**,不是 .csv。系統只找 .csv,
      連 V24.22 剛加的 glob `positions_opening*.csv` 也抓不到它
      → 一路回退 LEGACY 硬編碼,**帳務重建從未生效**。
      他顯然是用 Excel 在維護這份檔,所以正解是【讓系統讀得懂 xlsx】,
      而不是要求他改工作方式。
    回傳 (rows, err)。rows 為 list[list[str]],保留 # 註解列(供抓基準日)。
    """
    # [V24.27] ★用【檔頭 magic bytes】判斷真實格式,不信副檔名。
    #   根因(使用者 2026-08-10 截圖):檔案類型顯示「XLS 工作表」,
    #   而 .xls 是 OLE2 二進位,**openpyxl 完全讀不了**(它只支援 xlsx/xlsm)。
    #   且副檔名本身不可靠(Windows 可能隱藏副檔名、或關聯設定造成顯示名不符)。
    #   magic bytes:  PK\x03\x04 = xlsx(zip)  /  \xd0\xcf\x11\xe0 = xls(OLE2)  /  其餘=文字
    try:
        with open(path, "rb") as _bf:
            _magic = _bf.read(8)
    except Exception as _e:
        return None, f"檔案無法開啟:{_e}"

    if _magic.startswith(b"\xd0\xcf\x11\xe0"):        # 舊版 .xls (OLE2)
        try:
            import xlrd as _xlrd
            _wb = _xlrd.open_workbook(path)
            _ws = _wb.sheet_by_index(0)
            rows = []
            for _ri in range(_ws.nrows):
                cells = ["" if c is None else str(c).strip()
                         for c in _ws.row_values(_ri)]
                if any(cells):
                    rows.append(cells)
            return rows, None
        except ImportError:
            return None, ("此檔為舊版 .xls(OLE2)二進位,openpyxl 讀不了。"
                          "請【用 Excel 另存為 .xlsx 或 CSV(UTF-8)】,"
                          "或安裝 xlrd(pip install xlrd)")
        except Exception as _e:
            return None, f".xls 讀取失敗:{_e}"

    if _magic.startswith(b"PK\x03\x04"):               # .xlsx / .xlsm (zip)
        if not _OPENPYXL_OK:
            return None, "openpyxl 未安裝,無法讀 xlsx(pip install openpyxl)"
        try:
            wb = load_workbook(path, data_only=True, read_only=True)
            ws = wb[wb.sheetnames[0]]
            rows = []
            for r in ws.iter_rows(values_only=True):
                if r is None:
                    continue
                cells = ["" if c is None else str(c).strip() for c in r]
                if any(cells):
                    rows.append(cells)
            wb.close()
            return rows, None
        except Exception as _e:
            return None, f"xlsx 讀取失敗:{_e}"

    try:
        with open(path, encoding="utf-8-sig") as f:
            return [ln.rstrip("\n").split(",") for ln in f if ln.strip()], None
    except Exception as _e:
        return None, f"csv 讀取失敗:{_e}"



def build_positionbook(base_dir, legacy_holdings, legacy_costs):
    """[V22.27.7] 讀DB持倉重建(優先) → 失敗回退CSV → CSV也失敗回退legacy。"""
    import re as _re_pb
    msgs = []
    # [V24.22] ★檔名容錯。使用者本機實際檔名為 positions_opening_reanchor_20260717.csv,
    #   系統只找 positions_opening.csv → 找不到就【回退 LEGACY 硬編碼】,
    #   帳務重建等於一直沒生效(早報某檔現倉 = LEGACY 值 + MTM,即為佐證)。
    #   處置:主檔名優先;沒有則取 positions_opening*.csv 最新一個,並【明說用了哪個】。
    import glob as _glob
    opening_path = os.path.join(base_dir, "positions_opening.csv")
    _picked_alt = None
    if not os.path.exists(opening_path):
        _alts = sorted(_glob.glob(os.path.join(base_dir, "positions_opening*.*")))
        _alts = [a for a in _alts if os.path.splitext(a)[1].lower() in (".csv", ".xlsx", ".xls")]
        if _alts:
            opening_path = _alts[-1]
            _picked_alt = os.path.basename(opening_path)
    txn_path = os.path.join(base_dir, "transactions.csv")

    positions = {}
    anchor_date = None
    txn_count = 0
    db_mode = False

    # [V22.27.7 FixB] 優先從DB讀持倉(position_book + transactions表)
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            pb_rows = conn.execute(
                "SELECT code,name,buy,sell,current,status,anchor_date "
                "FROM position_book ORDER BY code").fetchall()
            if pb_rows:
                db_mode = True
                for r in pb_rows:
                    code, nm, buy, sell, cur, st, ad = r
                    positions[code] = {
                        "name": nm, "buy": buy or 0, "sell": sell or 0,
                        "current": cur or 0, "status": st or "⚖️",
                        "lots": [(ad or "", cur or 0)],   # [V24.11] lot-based MTM
                    }
                    if ad and not anchor_date:
                        anchor_date = ad
                # 讀交易流水
                txn_rows = conn.execute(
                    "SELECT date,code,action,shares,nav,amount "
                    "FROM transactions ORDER BY date").fetchall()
                for tr in txn_rows:
                    date, code, action, shares, nav, amount = tr
                    if anchor_date and date <= anchor_date:
                        continue
                    if code not in positions:
                        positions[code] = {"name": code, "buy": 0.0, "sell": 0.0,
                                           "current": 0.0, "status": "⚖️", "lots": []}
                    p = positions[code]
                    p.setdefault("lots", [])
                    if action in ("redeem_all", "close"):
                        # [清倉] 全額贖回:部位帳以「元」記帳,錨點值≠實際贖回金額,
                        #   用一般 redeem 相減會留下幽靈殘值 → 直接歸零。金額未知(空/0)時以帳面現值估。
                        amt = amount if amount and amount > 0 else p["current"]
                        p["sell"] += amt
                        p["current"] = 0.0
                        p["lots"] = []
                    elif action in ("redeem", "sell"):
                        amt = amount if amount and amount > 0 else (shares or 0) * (nav or 0)
                        p["sell"] += amt
                        _cur = p["current"]
                        if _cur > 0:   # [V24.11] 贖回按比例等比縮減各 lot
                            _r = max(0.0, 1.0 - amt / _cur)
                            p["lots"] = [(_d, _a * _r) for _d, _a in p["lots"]]
                        p["current"] = max(0.0, _cur - amt)
                    elif action in ("subscribe", "buy"):
                        amt = amount if amount and amount > 0 else (shares or 0) * (nav or 0)
                        p["buy"] += amt
                        p["current"] += amt
                        p["lots"].append((date, amt))   # [V24.11] 新 lot 從交易日起算
                    else:
                        msgs.append(f"⚠️ 未知 action '{action}' ({code} {date})，略過")
                        continue
                    txn_count += 1
                msgs.append(f"✅ PositionBook(DB): {len(positions)}檔，{txn_count}筆交易")
    except Exception as e:
        msgs.append(f"⚠️ DB持倉讀取失敗({e})，回退CSV")

    # [V22.27.7] DB無資料→回退CSV(保留舊邏輯)
    if not db_mode:
        if not os.path.exists(opening_path):
            msgs.append("⚠️ 無 positions_opening*.csv(已 glob 全部變體) → 回退 LEGACY 硬編碼。"
                        "★LEGACY 是【手動快照】,帳務重建不會生效。實際搜尋目錄: " + base_dir)
            return dict(legacy_holdings), dict(legacy_costs), msgs

        if _picked_alt:
            msgs.append(f"ℹ️ 未找到 positions_opening.csv → 改用 **{_picked_alt}**。"
                        f"⚠️ 請確認該檔基準日與 transactions.csv 不重疊,否則交易會被重複計算")
        try:
            _rows, _rerr = _pb_read_rows(opening_path)
            if _rerr:
                msgs.append(f"⚠️ 期初快照讀取失敗({_rerr}) → 回退 LEGACY")
                return dict(legacy_holdings), dict(legacy_costs), msgs
            for parts in (_rows or []):
                _first = (parts[0] or "").strip() if parts else ""
                if not _first:
                    continue
                if _first.startswith("#"):
                    _m = _re_pb.search(r"基準日\s*(\d{4}-\d{2}-\d{2})", ",".join(parts))
                    if _m:
                        anchor_date = _m.group(1)
                    continue
                if _first.lower() == "code":
                    continue
                if len(parts) < 6:
                    continue
                positions[_first] = {
                    "name": (parts[1] or "").strip(),
                    "buy": _pb_parse_float(parts[2]),
                    "sell": _pb_parse_float(parts[3]),
                    "current": _pb_parse_float(parts[4]),
                    "status": (parts[5] or "").strip(),
                    "lots": [(anchor_date or "", _pb_parse_float(parts[4]))],
                }
        except Exception as e:
            msgs.append(f"⚠️ 期初快照讀取失敗 ({e})，回退 LEGACY")
            return dict(legacy_holdings), dict(legacy_costs), msgs

        if not positions:
            msgs.append("⚠️ 期初快照為空，回退 LEGACY")
            return dict(legacy_holdings), dict(legacy_costs), msgs

        txn_count = 0
        if os.path.exists(txn_path):
            try:
                with open(txn_path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or line.startswith("date,"):
                            continue
                        parts = line.split(",")
                        if len(parts) < 6:
                            continue
                        date, code, action = parts[0].strip(), parts[1].strip(), parts[2].strip()
                        if anchor_date and date <= anchor_date:
                            continue
                        shares = _pb_parse_float(parts[3])
                        nav = _pb_parse_float(parts[4])
                        amount = _pb_parse_float(parts[5])
                        if code not in positions:
                            positions[code] = {"name":code,"buy":0.0,"sell":0.0,"current":0.0,
                                               "status":"⚖️","lots":[]}
                        p = positions[code]
                        p.setdefault("lots", [])
                        if action in ("redeem_all", "close"):   # [清倉] 同 DB 分支
                            amt = amount if amount > 0 else p["current"]
                            p["sell"] += amt
                            p["current"] = 0.0
                            p["lots"] = []
                        elif action in ("redeem", "sell"):
                            amt = amount if amount > 0 else shares * nav
                            p["sell"] += amt
                            _cur = p["current"]
                            if _cur > 0:   # [V24.11] 贖回按比例等比縮減各 lot
                                _r = max(0.0, 1.0 - amt / _cur)
                                p["lots"] = [(_d, _a * _r) for _d, _a in p["lots"]]
                            p["current"] = max(0.0, _cur - amt)
                        elif action in ("subscribe", "buy"):
                            amt = amount if amount > 0 else shares * nav
                            p["buy"] += amt
                            p["current"] += amt
                            p["lots"].append((date, amt))   # [V24.11] 新 lot 從交易日起算
                        else:
                            msgs.append(f"⚠️ 未知 action '{action}' ({code} {date})，略過")
                            continue
                        txn_count += 1
            except Exception as e:
                msgs.append(f"⚠️ 交易流水讀取失敗 ({e})，僅用期初快照")
        else:
            msgs.append("ℹ️ 無 transactions.csv（尚無新交易）")

    # [V22.23.4] 用DB真實actual_ret把錨點現值累乘到最新交易日(取代凍結現值)
    _mtm_n, _mtm_msg = _pb_mark_to_market(positions, anchor_date)
    msgs.append(_mtm_msg)

    holdings, costs = {}, {}
    for code, p in positions.items():
        buy, sell, current = p["buy"], p["sell"], p["current"]
        total_pnl = sell + current - buy
        # [V22.23.5] pnl_pct 主值改近1月區間報酬(DB真實actual_ret累乘), 免疫churn失真。
        #   黃金/芯片等大額申贖檔, 成本反推%失真(黃金+1256%); 近1月%反映現況該不該動。
        #   無DB資料→回退成本法(total_pnl/buy), 保持不崩。total_pnl(元)維持絕對真實不變。
        pnl_1m = _pb_window_return(code, PB_WINDOW_1M)
        pnl_3m = _pb_window_return(code, PB_WINDOW_3M)
        if pnl_1m is not None:
            pnl_pct = pnl_1m
        else:
            pnl_pct = (total_pnl / buy * 100) if buy > 0 else 0.0
        holdings[code] = round(current, 2)
        costs[code] = {
            "buy": round(buy, 2), "sell": round(sell, 2), "current": round(current, 2),
            "total_pnl": round(total_pnl, 2), "pnl_pct": round(pnl_pct, 1),
            "pnl_pct_3m": pnl_3m,  # 近3月參考(None=資料不足)
            "status": _pb_status_from_pct(pnl_pct),
        }
    msgs.append(f"✅ PositionBook 重建：{len(positions)} 檔，套用 {txn_count} 筆交易")
    return holdings, costs, msgs


# ── 生成正式 USER_HOLDINGS / USER_COSTS（下游全部引用這兩個變數，零修改）──
USER_HOLDINGS, USER_COSTS, _PB_MSGS = build_positionbook(
    _PB_BASE_DIR, _LEGACY_USER_HOLDINGS, _LEGACY_USER_COSTS)
_PB_DIFF_WARNINGS = positionbook_diff_audit(USER_COSTS, _LEGACY_USER_COSTS)


# [V22.17.4 新增] 持倉戰局摘要分組顯示

# [V22.17.2 新增] 操作指令化所需常數
# 總可用資金 = USER_HOLDINGS 合計 × 1.05（預留 5% 現金）
TOTAL_CAPITAL = sum(USER_HOLDINGS.values()) * 1.05  # 約 18,180 元


def _pct_of_capital(amt):
    """[清倉防護] 金額佔總資金百分比。全數清倉時 TOTAL_CAPITAL=0,直接除會 ZeroDivisionError
    使整份報告崩潰(2026-10-15 以真實資料庫模擬清倉實測)。此時一律回 0.0。"""
    return (amt / TOTAL_CAPITAL * 100) if TOTAL_CAPITAL > 0 else 0.0
# Style 倉位上限（百分比 of TOTAL_CAPITAL）
STYLE_CAP_PCT = {
    "Tech":       40,  # 含 008888/011613/018177/016008/019141
    "QDII":       25,  # 006479/018967
    "Defensive":  35,  # 黃金/避險 (000216)
    "Dividend":   15,  # 紅利低波 (007466)
    "Cyclical":   15,  # 有色 (004432/018168)
    "Utility":    12,  # 電力 (016185)
    # [V22.17.2 audit 補] 觀察池 5 個 style 上限
    "OptComm":    10,  # 光通信 (019072)
    "Defense":    10,  # 軍工/衛星 (005693/024195)
    "Industrial": 10,  # 電網/工業 (023639/025856電網設備)
    # [V22.27.10 補] 觀察池既有 style 未註冊(原走 .get 預設25%,顯式化避免自檢告警)
    "Robotics":   10,  # 機器人 (018345)
    "NewEnergy":  10,  # 固態電池/新能源 (013180)
}
# 單檔倉位上限（避免重倉單檔）
SINGLE_CAP_PCT = 25  # 任一 ETF 不超過 25%

# ═════════════════════════════════════════════════════════════
# [V22.33] 條件式動態上限 (Conditional Dynamic Cap)
#
# 【問題】原 STYLE_CAP_PCT 硬上限是【任意數字】(源碼無理論依據註解)。
#   使用者質疑「40% 哪來的?沒人知道」→ 質疑成立。
#   且硬上限會錯過趨勢行情(中際旭創困境:80元時因超標不讓買,結果漲到300)。
#
# 【機構真實設定值(WB 多源交叉核證 2026-07-14)】
#   - 對沖基金行業/板塊上限 15-20% (AlphaMaven/Pomegra)
#   - 單一持倉典型 2-5%;5-8% 是高信念區間;硬上限約10%
#   - Fractional Kelly 25-50%(業界近乎一致)
#   - 零售:單一10% / 行業25% (Guardfolio)
#   - ★SEC 1940 Act:sector/thematic 基金註冊為 nondiversified,
#     【依法不受分散/行業限制約束】→ 使用者是主題投資者,
#     對標基準應為 aggressive/thematic 的 25-35%,非 15-20%
#   ⚠️ Claude 原引「70%對沖基金用行業限制」= 誤引(實為「前十大持倉佔長倉70%」)
#
# 【★成敗分水嶺(WB 報告核心 — 這才是設計的靈魂)】
#   ┌─────────┬──────────────────────┬─────────────────────┐
#   │ 維度    │ ✅成功(波克夏)        │ ❌失敗(軟銀/LTCM)    │
#   ├─────────┼──────────────────────┼─────────────────────┤
#   │ 時點估值│ Apple 8-12x PE 低估買 │ WeWork 470億高點接刀 │
#   │ 槓桿    │ 零/永久資本           │ 高槓桿(LTCM 25-167x) │
#   │ 流動性  │ 大盤流動性資產        │ 非流動難變現         │
#   │ 紀律減倉│ 巴菲特把Apple 50%→22% │ 無止損,靠救援填坑    │
#   └─────────┴──────────────────────┴─────────────────────┘
#   ★波克夏 Apple 曾佔股票組合【50%】(比使用者41%還高)——集中本身不是問題。
#     關鍵是【在什麼估值買】+【有沒有紀律減倉】。
#
# 【★ARKK 血案(數據經 WB 核證,Claude 原引多處錯誤)】
#   2020 +152.8% → 2022 【-66.9%】→ 峰谷最大回撤【-80.9%~-81.6%】
#   跌67%需漲203%回本;跌80%需漲400%
#   ★★人性的量化證據(Morningstar/Amy Arnott):
#     TWR(基金本身)約 +120% vs MWR(投資人實際)約 -27%
#     → 基金賺,投資人虧 =【行為缺口】。摧毀財富 ARKK $7.1B。
#     → 【集中不害人,「在高點集中」才害人。】
#
# 【設計】不是調高數字,是改成【條件式】:
#   0-40%   自由買
#   40-50%  突破區:五條件【全滿足】才放行(五條正是ARKK投資人2020年底沒有的)
#   ≥45%    紀律減倉提示(波克夏教訓)
#   >50%    硬上限,絕對不破(50%腰斬=組合-25%,使用者能扛的邊界)
# ═════════════════════════════════════════════════════════════
DYNAMIC_CAP_ENABLED  = True    # 總開關:False → 回到原硬上限(零回歸)
DYNAMIC_CAP_BREAKOUT = {"Tech": (40, 50)}   # style: (軟上限, 硬上限)
DCAP_TREND_MIN     = 0.0       # ① 趨勢 > 0(不在下跌趨勢加碼)
DCAP_FLOW5_MIN     = 0.0       # ② 機構5日淨流入 > 0(聰明錢在買,非派發)
DCAP_RSI_MAX       = 70.0      # ③ RSI < 70(不追高)
DCAP_FRAGILITY_MAX = 50        # ④ 脆弱度 < 50(不在拋物線頂)
DCAP_VALUATION_MAX = 0.80      # ⑤ 估值分位 < 0.80(★波克夏:8-12x PE 買 Apple)
# [V22.40 回測判決] ★streak 升格【已停用】—— 回測結果與當初假設【相反】。
#   實測(v20_prediction_log_v39_fresh.db,35交易日 2026-05-29~07-20,T+0行建報酬序列):
#     減倉 streak>=2 → T+1~5 累計【+2.17%】(樣本33,負報酬僅39%)
#     減倉 streak>=1 → T+1~5 累計【+0.66%】(樣本91)
#   ★E-LOG_215 已將此結論【降級】:該 25 筆樣本全集中於 5/29~06-11 的 7 個日期
#     (一次六月反彈事件 × 7 檔 × 重疊視窗,真實獨立觀測≈1)= 偽重複,
#     依㉘【既不足以支持升格,也不足以否決升格】。
#   → 故現狀是「未獲驗證·保守停用」,不是「回測否決」。恢復與否需 ≥3 個獨立 regime 樣本。
#   ⚠️ 樣本僅 35 交易日、單一 regime、streak>=3 僅 10-14 筆(㉘:不足以反向定論),
#      故用【可逆旗標】停用,累積更多資料後重測再定,不直接刪碼。
DCAP_STREAK_ESCALATION_ENABLED = False   # [V22.40] 回測不支持 → 停用
DCAP_REDUCE_STREAK_N = 3       # 門檻值保留(旗標開啟時才生效)
DCAP_TRIM_LEVEL    = 45.0      # 紀律減倉提示線(★巴菲特 Apple 50%→22%)
# [V22.33.2→V22.34.1 誠實化] PE 估值分位資料源是否已接 —— ★這是【純資訊旗標,不是閘門】。
#   ⚠️ 舊註解說「接上後改 True,五條件突破自動生效」是【不準確的】(WB 缺陷C 抓到):
#     真正的閘門是 `val_pct` 有沒有值(見 check_dynamic_cap 的 ok 判斷);本旗標【不參與 ok】。
#     它只用來說明「⑤沒資料的原因」是【資料源沒接】還是【接了但今日無值/過期】。
#   V22.34 已建 pe_percentile 表 + load_pe_percentile;只要 DB 有未過期分位,第⑤條就會拿到值,
#     不需要改這個旗標。待 WB 填實 fetch 端點+歷史序列分位計算並每日落庫後,再改 True 以正名。
#   ⚠️ 鐵律㉒:禁止在無真數據下把此旗標改 True(它雖不放行,但會誤導讀報告的人)。
#   [V22.73] funddb(韭圈儿)真·多年 PE 分位已接(見 refresh_pe_percentile_funddb),
#     屬 live 源 → 翻 True。若 funddb 當日抓取失敗,refresh 會優雅回 None →
#     呼叫方 fallback 滾動分位(local_rolling,不驅動⑤)→ 仍安全,不假成功。
#   [V22.74] 治顯示誠實:旗標改運行時判定——pe_percentile 表【真有 funddb 來源的行】才 True。
#   funddb 從沒成功 / 只有 local_rolling → False(note 誠實說未接,不誤稱「已接但今日無值」)。
def _detect_pe_source_connected(_log_db):
    try:
        with sqlite3.connect(_log_db, timeout=10) as _c:
            _c.execute("CREATE TABLE IF NOT EXISTS pe_percentile(code TEXT, date TEXT, "
                       "pe_ttm REAL, pct_3y REAL, pct_5y REAL, pct_10y REAL, source TEXT, "
                       "ts TEXT, PRIMARY KEY(code,date))")
            return _c.execute("SELECT 1 FROM pe_percentile WHERE source LIKE 'funddb%' "
                              "LIMIT 1").fetchone() is not None
    except Exception:
        return False
DYNAMIC_CAP_PE_SOURCE_CONNECTED = _detect_pe_source_connected(LOG_DB)

def check_dynamic_cap(style_key, cur_pct, trend=None, flow5_yi=None,
                      rsi=None, fragility=None, val_pct=None):
    """條件式動態上限。回傳 (effective_cap, allowed, reason)。
    ★五條件【全滿足】才放行突破區。資料缺失 → 該條視為不滿足(保守)。
    """
    soft = STYLE_CAP_PCT.get(style_key, 25)
    if not DYNAMIC_CAP_ENABLED or style_key not in DYNAMIC_CAP_BREAKOUT:
        return (soft, cur_pct < soft, "")
    soft, hard = DYNAMIC_CAP_BREAKOUT[style_key]

    trim = ""
    if cur_pct >= DCAP_TRIM_LEVEL:
        trim = (f"\n   ⚠️ **紀律減倉提示**:{style_key} 已達 {cur_pct:.1f}%(≥{DCAP_TRIM_LEVEL}%)。"
                f"★波克夏:巴菲特 2024 主動把 Apple 從 50% 砍到 22%——"
                f"**成功者主動再平衡,失敗者無止損(軟銀/LTCM)**。")

    if cur_pct < soft:
        return (soft, True, "")
    if cur_pct >= hard:
        return (hard, False,
                f"🔴 **{style_key} {cur_pct:.1f}% 已達硬上限 {hard}%** — 絕對不破"
                f"(ARKK:>50% 進入 -80% 回撤領域){trim}")

    # ── 突破區 40-50%:五條件 ──
    checks, ok = [], True
    if trend is None or trend <= DCAP_TREND_MIN:
        ok = False; checks.append(f"❌趨勢({'無資料' if trend is None else f'{trend:+.2f}'}≤0)")
    else:
        checks.append(f"✅趨勢{trend:+.2f}")
    if flow5_yi is None or flow5_yi <= DCAP_FLOW5_MIN:
        ok = False; checks.append(f"❌機構5日({'無資料' if flow5_yi is None else f'{flow5_yi:+.1f}億'}≤0,聰明錢在跑)")
    else:
        checks.append(f"✅機構5日+{flow5_yi:.1f}億")
    if rsi is None or rsi >= DCAP_RSI_MAX:
        ok = False; checks.append(f"❌RSI({'無資料' if rsi is None else f'{rsi:.0f}'}≥70,追高)")
    else:
        checks.append(f"✅RSI {rsi:.0f}")
    _f = fragility if fragility is not None else 999
    if _f >= DCAP_FRAGILITY_MAX:
        ok = False; checks.append(f"❌脆弱度({'無資料' if fragility is None else _f}≥50)")
    else:
        checks.append(f"✅脆弱度{_f}")
    _v = val_pct if val_pct is not None else 1.0
    if _v >= DCAP_VALUATION_MAX:
        ok = False; checks.append(f"❌估值分位({'無資料' if val_pct is None else f'{_v:.2f}'}≥{DCAP_VALUATION_MAX},高點集中=軟銀式接刀)")
    else:
        checks.append(f"✅估值{_v:.2f}")

    detail = " | ".join(checks)
    # [V22.34.1 治WB缺陷C] ★原本 `_pe_gap = (val_pct is None) or (not DYNAMIC_CAP_PE_SOURCE_CONNECTED)`
    #   → 旗標恆 False 使 _pe_gap 恆 True → 【每次失敗都附「第⑤條結構性擋下」】,
    #   即使⑤其實過了(val_pct=0.62)、真正沒過的是①~④ → 給出【錯誤的失敗歸因】。
    #   與「_pa_warn 只 print」「meta 死碼」同病理:有計算 ≠ 有作用/有正確作用。
    #   修法:只有【真的沒有分位資料】才歸咎⑤;旗標退為 INFO(說明資料為何缺),不搶功、不誣賴。
    _pe_gap = (val_pct is None)
    if _pe_gap:
        _src = ("資料源【未接】(DYNAMIC_CAP_PE_SOURCE_CONNECTED=False;待填實 "
                "fetch_pe_percentile_eastmoney 端點+歷史序列分位計算)"
                if not DYNAMIC_CAP_PE_SOURCE_CONNECTED
                else "資料源已接,但【今日無值或已過期>7天】")
        _pe_note = (f"\n   ★系統狀態:PE 估值分位{_src} → 第⑤條保守擋下 → 突破區(40-50%)暫停"
                    f" → 【當前等效 {soft}% 硬上限】。此為【資料缺口】非市況判斷。")
    else:
        # [V22.34.2 採納WB補丁] 有分位值但抓取端點未接 → 明示「此值為外部/手動填入,非live抓取」,
        #   讓讀報告的人知道資料來源層級;⑤已用實值判斷,不歸咎旗標(不誣賴)。
        _pe_note = ("\n   ⚠️ INFO:PE 分位為【外部/手動填入】(DYNAMIC_CAP_PE_SOURCE_CONNECTED=False,"
                    "live 抓取端點待 WB 接);第⑤條已用實值判斷,不歸咎本旗標。"
                    ) if not DYNAMIC_CAP_PE_SOURCE_CONNECTED else ""
    if ok:
        return (hard, True,
                f"🟢 **{style_key} {cur_pct:.1f}% 突破區放行**(五條件全滿足) → {soft}%→{hard}%\n"
                f"   {detail}{trim}")
    return (soft, False,
            f"🟡 **{style_key} {cur_pct:.1f}% 超軟上限 {soft}%,突破條件未滿足** → 維持 {soft}%\n"
            f"   {detail}{trim}{_pe_note}\n"
            f"   ★ARKK 教訓:TWR(基金)+120% vs MWR(投資人)-27% = 行為缺口\n"
            f"     → 集中不害人,【在高點集中】才害人")
# 試水位（未持倉但出主攻信號時的起手量）
# ═════════════════════════════════════════════════════════════
# [V24.10] 曝險狀態監控 — 既有持倉的上限狀態(第4題:使用者明令必須顯示)
#
# 【為何需要】grep 證據:`SINGLE_CAP_PCT` 全檔僅 2 處——定義行 + compute_order
#   買入試算的 min() 保護。既有持倉因【市價變動】被動突破上限時,四報完全靜默。
#
# 【機構標準做法】CFA Institute《Active Equity Investing: Portfolio Construction》:
#   對證券/板塊/產業/地域的集中度限制屬「啟發式約束(heuristic constraints)」,
#   且明載【必須持續監控,以判斷市價演變是否已使約束被突破或逼近突破】。
#   → 只管新買、不回頭看 = 只做了機構做法的一半。
#   https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/
#     active-equity-investing-portfolio-construction
#
# 【表述原則(交接手冊§七)】只陳述客觀狀態 + 換算成【具體金額/回撤數字】,
#   不使用「持倉過高/集中度太高」等短期交易者語言,不擋人、不剝奪選擇權。
# ═════════════════════════════════════════════════════════════

def compute_max_drawdown_from_db(code, days=252):
    """從DB actual_ret序列算最大回撤。回傳 (mdd_pct, n_days) 或 (None, 0)。

    ★V24.11 修正(WB 2026-08-04 實跑暴露):舊版只回 mdd,呼叫端寫死「近252日」,
      但 DB 實際去重後每檔僅 ~55-56 個交易日 → 顯示「近252日最大回撤」是【假精確】(鐵律㉘)。
      現一併回傳【實際窗長】,由呼叫端如實顯示,窗長不足時明確標註可能低估。
    無資料或樣本<20回 (None, 0)——不猜、不用寫死值。
    """
    try:
        import sqlite3 as _sq
        with _sq.connect(LOG_DB, timeout=10) as conn:
            rows = conn.execute(
                "SELECT date(ts) d, actual_ret FROM predictions "
                "WHERE code=? AND actual_ret IS NOT NULL "
                "GROUP BY date(ts) ORDER BY d DESC LIMIT ?", (code, days)).fetchall()
    except Exception:
        return (None, 0)
    if not rows or len(rows) < 20:
        return (None, len(rows) if rows else 0)
    rets = [r[1] for r in reversed(rows) if r[1] is not None]
    if len(rets) < 20:
        return (None, len(rets))
    nav, peak, mdd = 1.0, 1.0, 0.0
    for r in rets:
        nav *= (1.0 + r)
        peak = max(peak, nav)
        mdd = min(mdd, nav / peak - 1.0)
    return (round(mdd * 100, 1), len(rets))




def compute_max_drawdown_from_nav(code, min_days=120):
    """[V24.11] 用【天天基金 NAV 序列】算最大回撤——優於 DB actual_ret。

    ★為何新增(WB 2026-08-04 誠實揭露):predictions.actual_ret 去重後每檔僅 ~56 個
      交易日(2026-05-12起),用它算「最大回撤」必然低估——沒涵蓋更早的高低點。
      而 `_dated_nav()` 讀 fund.eastmoney pingzhongdata,可取【數年】完整淨值序列
      ——★但 V24.14 前它寫死 `out[-120:]`,實際只回 120 點,本句斷言曾為【假】,
        V24.15 起以 limit=None 呼叫才成立(WB 2026-08-07 揭露,E-LOG_287),
      這才是回撤的正確資料源。
    回傳 (mdd_pct, n_days, "nav") 或 (None, n, "nav")。
    ⚠️ 沙盒東財不通 → 本函式【未經實跑驗證】,需 WB 本機驗(鐹律㉖)。
    """
    try:
        series = _dated_nav(code, limit=None)   # [V24.15] MDD 需全序列,不可截斷
    except Exception:
        return (None, 0, "nav")
    if not series or len(series) < min_days:
        return (None, len(series) if series else 0, "nav")
    navs = [v for _d, v in sorted(series) if v and v > 0]
    if len(navs) < min_days:
        return (None, len(navs), "nav")
    peak, mdd = navs[0], 0.0
    for v in navs:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1.0)
    return (round(mdd * 100, 1), len(navs), "nav")


_DD_CACHE = {}   # [V24.12] {(code, YYYY-MM-DD): (mdd, n, src)} — 當日只算一次

def compute_max_drawdown_best(code, _force=False):
    """優先 NAV 長序列,失敗回退 DB actual_ret 短窗。回傳 (mdd, n_days, source)。

    ★[V24.12] 加當日快取。根因:render_exposure_status 與 render_drawdown_budget
      都會呼叫,且後者對【每一檔持倉】各叫一次;NAV 走網路,東財不通時每檔都要等
      retry/timeout → 四報產出被拖垮(沙盒實測直接超時)。快取後每檔每日最多一次。
    """
    _k = (code, datetime.datetime.now().strftime("%Y-%m-%d"))
    if not _force and _k in _DD_CACHE:
        return _DD_CACHE[_k]
    mdd, n, _src = compute_max_drawdown_from_nav(code)
    if mdd is not None:
        _DD_CACHE[_k] = (mdd, n, "NAV")
    else:
        mdd2, n2 = compute_max_drawdown_from_db(code)
        _DD_CACHE[_k] = (mdd2, n2, "DB")
    return _DD_CACHE[_k]


def compute_drawdown_position(code):
    """[V24.17] 當前回撤位置 — 把回撤資料【翻過來用:找機會,不是設限制】。

    ★使用者 2026-08-07 明令後的方向修正:
      我先前把「具體回撤數字」做成 D_max 上限,但那條哲學原本是【取代「風險高」
      這種空話的表述方式】,不是要設新天花板。同一批 NAV 資料應該反過來問:
        不是「你可能虧多少」,而是「現在跌到歷史回撤的哪個位置了」。
      008888 歷史最深 -59.94%;若它現在已跌 45%,那是【機會】不是風險。
      這也是 E-LOG_281 說的「估值分位是唯一能區分波克夏式集中與ARKK式集中的變數」
      ——在 PE 分位鏈接通前,**回撤位置就是可用的估值代理**。

    回傳 dict 或 None:
      cur_dd    : 現價距【歷史最高淨值】的回撤%(負值)
      max_dd    : 歷史最大回撤%(負值)
      pct_of_max: 已走完歷史級回撤的幾成(0~100+,>100 表示比史上任何一次都深)
      to_peak   : 回到歷史高點需要漲幅%
      n         : 樣本淨值點數
    ⚠️ 沙盒東財不通,本函式【未在真實 pingzhongdata 上驗證】,需 WB 本機驗。
    """
    try:
        series = _dated_nav(code, limit=None)
    except Exception as _e:
        _log_err(_e, f"compute_drawdown_position:{code}")
        return None
    if not series or len(series) < 60:
        return None
    navs = [v for _d, v in sorted(series) if v and v > 0]
    if len(navs) < 60:
        return None
    peak_all = max(navs)
    cur = navs[-1]
    # [V24.18] 動能:近20/60日報酬 + 距最近低點反彈幅度 → 判「已止穩」還是「還在跌」
    _r20 = (cur / navs[-21] - 1.0) * 100.0 if len(navs) > 21 and navs[-21] else None
    _r60 = (cur / navs[-61] - 1.0) * 100.0 if len(navs) > 61 and navs[-61] else None
    _recent_low = min(navs[-61:]) if len(navs) >= 61 else min(navs)
    _off_low = (cur / _recent_low - 1.0) * 100.0 if _recent_low else None
    cur_dd = (cur / peak_all - 1.0) * 100.0
    # 歷史最大回撤
    peak, mdd = navs[0], 0.0
    for v in navs:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1.0)
    max_dd = mdd * 100.0
    pct_of_max = (cur_dd / max_dd * 100.0) if max_dd < -1e-9 else 0.0
    to_peak = (peak_all / cur - 1.0) * 100.0 if cur > 0 else None
    return {"cur_dd": round(cur_dd, 1), "max_dd": round(max_dd, 1),
            "pct_of_max": round(pct_of_max, 0), "to_peak": round(to_peak, 1),
            "r20": None if _r20 is None else round(_r20, 1),
            "r60": None if _r60 is None else round(_r60, 1),
            "off_low": None if _off_low is None else round(_off_low, 1),
            "n": len(navs)}


def render_factor_matrix(results):
    """[V24.30] 統一因子矩陣 — 把【每個因子對每檔的實際 pp 貢獻】攤開。

    ★使用者 2026-08-11:「依樣只有一個新聞分,其他的呢??已經說了很多次了」。
      根因:報告只有「產業鏈新聞因子」一張表,其餘因子(主力/RSI/趨勢/脆弱)
      **從來沒有以「每檔貢獻多少 pp」的形式呈現過**——
      我前幾輪一直在【解釋為什麼是0】,卻沒有把它做出來。這次做出來。
    """
    try:
        rows = []
        for r in (results or []):
            c = r.get("code"); cb = r.get("t1_contrib") or {}
            if not c or not cb:
                continue
            tot = sum(cb.values())
            top = max(cb.items(), key=lambda kv: abs(kv[1])) if cb else ("—", 0)
            nm = TARGET_ETFS.get(c, {}).get("name", c)[:12]
            rows.append((abs(tot), f"| {c} {nm} | {cb.get('主力當日',0):+.2f} | "
                         f"{cb.get('主力5日',0):+.2f} | {cb.get('RSI反轉',0):+.2f} | "
                         f"{cb.get('趨勢動能',0):+.2f} | {cb.get('脆弱阻尼',0):+.2f} | "
                         f"{cb.get('產業新聞',0):+.2f} | **{tot:+.2f}** | "
                         f"{top[0]}({top[1]:+.2f}) |"))
        if not rows:
            return ""
        rows.sort(key=lambda x: -x[0])
        _dead = [k for k in ("主力當日", "主力5日", "RSI反轉", "趨勢動能", "脆弱阻尼", "產業新聞")
                 if all(abs((r.get("t1_contrib") or {}).get(k, 0)) < 0.005
                        for r in results if r.get("t1_contrib"))]
        out = ["\n### 🧮 因子貢獻矩陣（每檔 T+1 預測的成分拆解，單位 pp）",
               "> 這張表回答「這個預測數字是怎麼來的」。加總 = T+1 預測值。",
               "| 標的 | 主力當日 | 主力5日 | RSI反轉 | 趨勢動能 | 脆弱阻尼 | 產業新聞 | 合計 | 主導因子 |",
               "|---|---:|---:|---:|---:|---:|---:|---:|---|"]
        out += [r[1] for r in rows]
        out.append(f"> 權重:主力當日28% / 主力5日12% / RSI25% / 趨勢12% / 脆弱8% / 新聞15%"
                   f"(★**未經真數據校準**,見下方校準區塊)")
        if _dead:
            out.append(f"> ⚠️ **全檔皆為 0 的因子:{'、'.join(_dead)}** — "
                       f"這些因子目前【對預測毫無影響】,等於權重被浪費。"
                       f"若因資料源不可得,應把其權重重分配給有效因子,而非留著稀釋。")
        return "\n".join(out) + "\n"
    except Exception as _e:
        _log_err(_e, "render_factor_matrix")
        return ""


def render_forecast_bias_audit(chain_rows=None):
    """[V24.30] 早報系統性偏誤稽核 — 不只記錄失準,要算出【偏誤方向】並歸因。

    ★使用者 2026-08-11:「你應該要在不準的 etf 上找原因為什麼不准?
      早報明明預測上漲,為什麼到了午報和收盤報就下跌??這不應該要深度探討嗎?」
      ——對。四報校正鏈原本只列「早報/午報/收盤/誤差/✅❌」,是【記錄】不是【診斷】。

    2026-08-11 實測(15檔)算出來的東西:
      早報平均絕對誤差 1.52pp,午報 0.19pp(僅早報的 13%)
      早報平均【帶符號】偏誤 **+1.50pp** ← 正值 = 系統性高估,不是隨機誤差
      早報預測看漲 12/15,實際收漲僅 5/15
      早報方向錯 7 檔,**午報 100% 全部修正**
    → 結論:早報的錯不是「不準」,是【固定方向的看多偏誤】;
      而午報一看到開盤實況就修得回來 → **偏誤來源必然是早報唯一的資訊源:隔夜美股**。
      這正是 regime decoupling(A股科技 vs 美股隔夜)在真實數據上的證據。

    chain_rows: [(name, 早報pred, 午報pred, 收盤actual), ...]
    """
    try:
        rows = [r for r in (chain_rows or []) if r and len(r) >= 4
                and all(x is not None for x in r[1:4])]
        if len(rows) < 3:
            return ""
        e_m = [abs(r[1] - r[3]) for r in rows]
        e_n = [abs(r[2] - r[3]) for r in rows]
        b_m = sum(r[1] - r[3] for r in rows) / len(rows)
        b_n = sum(r[2] - r[3] for r in rows) / len(rows)
        wrong = [r for r in rows if (r[1] > 0) != (r[3] > 0)]
        fixed = [r for r in wrong if (r[2] > 0) == (r[3] > 0)]
        mae_m, mae_n = sum(e_m) / len(e_m), sum(e_n) / len(e_n)
        out = ["\n### 🔬 早報偏誤稽核（為什麼早報看漲、收盤卻跌）",
               f"| 指標 | 早報 | 午報 |", "|---|---:|---:|",
               f"| 平均絕對誤差 | {mae_m:.2f}pp | **{mae_n:.2f}pp** |",
               f"| 平均帶符號偏誤 | **{b_m:+.2f}pp** | {b_n:+.2f}pp |",
               f"| 方向錯誤檔數 | {len(wrong)}/{len(rows)} | "
               f"{sum(1 for r in rows if (r[2]>0)!=(r[3]>0))}/{len(rows)} |"]
        if abs(b_m) >= 0.5:
            _d = "高估(系統性看多)" if b_m > 0 else "低估(系統性看空)"
            out.append(f"\n**🚨 早報存在【系統性{_d}】,平均 {b_m:+.2f}pp。**")
            out.append(f"> 這【不是隨機誤差】——隨機誤差的帶符號平均會趨近 0。"
                       f"固定方向的偏誤代表某個輸入被系統性地過度採信。")
            if wrong and len(fixed) / max(len(wrong), 1) >= 0.7:
                out.append(f"> **午報修正了 {len(fixed)}/{len(wrong)} 檔早報方向錯誤"
                           f"({len(fixed)/len(wrong)*100:.0f}%)** → 偏誤來源是"
                           f"【早報有、午報沒有】的那個輸入:**隔夜美股**。"
                           f"午報一看到 A 股開盤實況就修得回來。")
                out.append(f"> **可行的修法**:早報對 A 股標的的美股傳導係數應下調,"
                           f"或引入 regime decoupling(A股本地催化週期中降低美股 β)。"
                           f"建議累積 20 個交易日的偏誤序列後再定係數,勿用單日校準。")
        if wrong:
            out.append("\n**早報方向錯誤明細（午報是否已修正）**:")
            out.append("| 標的 | 早報 | 午報 | 收盤實際 | 午報修正 |")
            out.append("|---|---:|---:|---:|---|")
            for r in wrong[:10]:
                ok = "✅ 已修正" if (r[2] > 0) == (r[3] > 0) else "❌ 仍錯"
                out.append(f"| {r[0]} | {r[1]:+.2f}% | {r[2]:+.2f}% | {r[3]:+.2f}% | {ok} |")
        return "\n".join(out) + "\n"
    except Exception as _e:
        _log_err(_e, "render_forecast_bias_audit")
        return ""


def render_signal_conflicts(results, layout_calls=None, flow_calls=None):
    """[V24.21] 訊號衝突揭示 — 同一檔被不同模組給出相反建議時,明說。

    ★使用者 2026-08-10 指出 019141 在【同一份報告】出現三種建議:
        操作清單  : 賣出/減倉(待午報確認) -352元
        布局探測  : 🔴 逆市場異常吸籌→該檔可考慮加/持有
        資金流建議: 🟡 維持現倉,觀察
      三個模組互不知道對方存在,使用者拿到三個相反指令、無從判斷。
      **這不是預測不準,是系統沒有把自己的分歧講出來。**
    處置:不強行統一(各模組看的維度本來就不同,分歧本身是資訊),
      但**必須攤在同一張表上並標明主從**——操作清單為執行依據,其餘為觀點。
    """
    try:
        rows = []
        for r in (results or []):
            _c = r.get("code")
            if not _c:
                continue
            _act = str(r.get("action", "") or "")
            _lay = str((layout_calls or {}).get(_c, "") or "")
            _flw = str((flow_calls or {}).get(_c, "") or "")
            def _dir(txt):
                if any(k in txt for k in ("減倉", "賣出", "清倉")):
                    return -1
                if any(k in txt for k in ("加碼", "買入", "可考慮加", "主攻", "吸籌")):
                    return 1
                return 0
            ds = [(_dir(_act), "操作清單", _act), (_dir(_lay), "布局探測", _lay),
                  (_dir(_flw), "資金流", _flw)]
            ds = [(d, n, t) for d, n, t in ds if t]
            dirs = {d for d, _, _ in ds if d != 0}
            if len(dirs) > 1:
                nm = TARGET_ETFS.get(_c, {}).get("name", _c)
                detail = " ｜ ".join(f"{n}:{'📈加' if d>0 else '📉減' if d<0 else '➖持'}"
                                    for d, n, t in ds)
                rows.append(f"| {_c} {nm} | {detail} |")
        if not rows:
            return ""
        out = ["\n### ⚔️ 訊號衝突揭示",
               "> 同一檔被不同模組給出**相反**建議。分歧本身是資訊,不強行統一——",
               "> 但**執行依據以「操作清單」為準**,其餘為不同維度的觀點。",
               "> 衝突越多代表該檔越處於轉折點,**若不確定就等午報/收盤確認,不必今日決定**。",
               "| 標的 | 各模組方向 |", "|---|---|"]
        return "\n".join(out + rows) + "\n"
    except Exception as _e:
        _log_err(_e, "render_signal_conflicts")
        return ""


def render_drawdown_position(holdings=None, extra_codes=None):
    """[V24.17] 回撤位置表 — 機會視角。不設限制、不擋任何操作。"""
    try:
        H = dict(holdings if holdings is not None else USER_HOLDINGS)
        for _c in (extra_codes or []):
            H.setdefault(_c, 0.0)
        if not H:
            return ""
        _WL = globals().get("WATCHLIST_ETFS", {}) or {}
        _held = set(holdings if holdings is not None else USER_HOLDINGS)
        rows, miss = [], []
        for _c in H:
            d = compute_drawdown_position(_c)
            _meta = TARGET_ETFS.get(_c) or _WL.get(_c) or {}
            nm = _meta.get("name", _c)
            # 觀察池標記:這些是【還沒買的機會】,與已持倉分開看
            tag = "持倉" if _c in _held else "👀觀察"
            if not d:
                miss.append(_c); continue
            # 判讀:純描述位置,不下買賣指令
            p = d["pct_of_max"]
            if   p >= 90: zone = "🟢 已達/超越史上最深區"
            elif p >= 70: zone = "🟢 深回撤區"
            elif p >= 40: zone = "🟡 中段"
            elif p >= 15: zone = "🟠 淺回撤"
            else:         zone = "🔴 貼近歷史高點"
            # [V24.18 使用者明令] 只給位置不夠——位置 × 動能 才是【有沒有機會】。
            #   深回撤 + 已止穩回升 = 機會浮現;深回撤 + 還在跌 = 接刀子區(位置便宜不代表現在該接)。
            _r20, _off = d.get("r20"), d.get("off_low")
            # [V24.18 修正] 門檻由 70 降為 50。根因:標的一旦【從深回撤回升】,
            #   pct_of_max 會自然變淺(例:曾跌-50%,回升後現距前高-32.7% → 65%),
            #   用 70 當門檻反而吃不到「曾深跌+剛起來」這個**最想抓的機會型態**。
            _deep = p >= 50
            if _r20 is None:
                call = "⚪ 動能資料不足"
            elif _deep and _r20 > 2:
                call = (f"🟢 **曾深跌+已止穩回升**(史上最深{d['max_dd']:+.0f}%、"
                        f"近20日{_r20:+.1f}%、離低點{_off:+.1f}%)→機會浮現")
            elif _deep and _r20 < -2:
                call = f"⚠️ **深回撤但仍在跌**(近20日{_r20:+.1f}%)→接刀子區,便宜≠現在該接"
            elif _deep:
                call = f"🟡 深回撤+橫盤築底(近20日{_r20:+.1f}%)→等一根確認"
            elif p < 20 and _r20 > 3:
                call = f"🔴 貼近前高+仍在衝(近20日{_r20:+.1f}%)→追高風險最大處"
            elif _r20 < -5:
                call = f"⚠️ 中段但跌勢未止(近20日{_r20:+.1f}%)→回撤可能還沒走完"
            else:
                call = f"➖ 位置中性(近20日{_r20:+.1f}%)"
            rows.append((p, f"| {_c} {nm} | {tag} | {d['cur_dd']:+.1f}% | {d['max_dd']:+.1f}% "
                            f"| {p:.0f}% | {d['to_peak']:+.1f}% | {zone} | {call} |"))
        if not rows:
            return ""
        rows.sort(key=lambda x: -x[0])
        out = ["\n### 🎯 回撤位置(機會視角)",
               "> 判準是【現在在歷史回撤的哪一段】,不是持倉多寡、不是價格高低。",
               "> 純位置描述,不含買賣建議;系統不背書也不擋你的選擇。",
               "| 標的 | | 現距前高 | 史上最深 | 已走完 | 回前高需漲 | 位置 | 判讀(位置×動能) |",
               "|---|---|---:|---:|---:|---:|---|---|"]
        out += [r[1] for r in rows]
        out.append("> **白話說明**(使用者 2026-08-10 要求):")
        out.append("> ．**現距前高** = 現在比它自己史上最貴的時候便宜了多少。")
        out.append("> ．**史上最深** = 這檔歷史上最慘的一次,從高點跌掉多少。")
        out.append("> ．**已走完 84%** = 「這次已經跌到史上最慘那次的 84% 深度了」。"
                   "數字越大代表跌得越接近歷史極限、往下空間相對越有限;"
                   "**但不保證跌不下去**(84% 也可能變 120%)。")
        out.append("> ．**回前高需漲** = 從現在漲回史上最高點要漲幾 %"
                   "(跌 50% 要漲 100% 才回本,跌越深回本越吃力)。")
        out.append("> ⚠️ **歷史最大回撤不是地板**——「已走完100%」不代表跌不下去,"
                   "只代表已達過去樣本內最深處。樣本長度見各檔 n(未涵蓋更早年份)。")
        out.append("> ⚠️ 位置便宜 ≠ 該買。仍須看產業週期在哪一段(訂單/資本支出/國產替代)"
                   "與曝險結構,回撤位置只是【估值代理】,不是理由本身。")
        if miss:
            out.append(f"> ⓘ {len(miss)} 檔 NAV 樣本不足未列入:{', '.join(miss)}")
        return "\n".join(out) + "\n"
    except Exception as _e:
        _log_err(_e, "render_drawdown_position")
        return ""


def render_drawdown_budget(holdings=None):
    """[V24.12] 回撤預算現況表 — 把「資本%上限」翻譯成【具體回撤金額】。

    為何:E-LOG_281 已查證,頂級機構主體風控是風險預算,資本%只是次級啟發式約束
      (CFA)。且等資本 != 等風險——Tech 與 Utility 波動差一個量級,用平行的資本%
      上限等於假裝兩者風險同質。本表不設限制、不擋人,只把曝險換算成使用者能判斷的
      單位:「若歷史級回撤重演,我會掉多少錢」。
    口徑誠實化:
      - 加總為【上界】(假設所有持倉同時重演各自MDD、忽略相關性<1的分散效果)
      - MDD 取不到的檔【排除在加總外】並明列少算幾檔,不以 0 充數(鐵律㉘)
      - 每檔標示 MDD 來源(NAV長序列/DB短窗)與實際窗長
    """
    try:
        H = holdings if holdings is not None else USER_HOLDINGS
        if not H or TOTAL_CAPITAL <= 0:
            return ""
        rows, total_contrib, missing = [], 0.0, []
        for _c, _a in sorted(H.items(), key=lambda x: -x[1]):
            w = _a / TOTAL_CAPITAL * 100
            mdd, ndays, src = compute_max_drawdown_best(_c)
            nm = TARGET_ETFS.get(_c, {}).get("name", _c)
            if mdd is None:
                missing.append(_c)
                rows.append(f"| {_c} {nm} | {w:.2f}% | 未校準({ndays}筆) | — |")
                continue
            contrib = w / 100.0 * mdd
            total_contrib += contrib
            flag = "⚠️窗短" if ndays < 120 else ""
            rows.append(f"| {_c} {nm} | {w:.2f}% | {mdd:.1f}% [{src}/{ndays}日]{flag} "
                        f"| {contrib:.2f}% |")
        if not rows:
            return ""
        out = ["\n### 📉 回撤預算現況",
               "> 不是限制,是換算:把曝險翻成「若歷史級回撤重演會掉多少錢」。決定權在你。",
               "> **注意:「歷史最大回撤」是這檔【史上曾經跌過的最深幅度】,不是它現在跌了多少。**",
               "> 現在跌到哪請看上面的「🎯 回撤位置」表。本表回答的是【最壞情況會賠多少錢】。",
               "| 標的 | 權重 | 歷史最大回撤(非現況) | 若重演的組合損失 |", "|---|---:|---:|---:|"]
        out += rows
        amt = abs(total_contrib) / 100.0 * TOTAL_CAPITAL
        out.append(f"\n**若所有持倉同時重演各自史上最深回撤:組合約 {total_contrib:.1f}% ≈ {amt:,.0f} 元**")
        out.append("> ⚠️ 此為【上界】:假設所有持倉同時重演各自最大回撤且忽略相關性<1 的"
                   "分散效果。真實相關性下會低於此數。")
        if missing:
            out.append(f"> ⚠️ 另有 {len(missing)} 檔 MDD 未校準({', '.join(missing)}),"
                       f"**未計入上述加總**——真實上界高於此數。")
        _dmax = globals().get("DRAWDOWN_BUDGET_MAX")
        if _dmax:
            out.append(f"> 回撤預算 D_max = {_dmax:.1f}%;目前占用 "
                       f"{abs(total_contrib)/abs(_dmax)*100:.0f}%。")
        else:
            out.append("> ⓘ 尚未設定回撤預算 D_max(需由你決定能承受多少回撤),"
                       "故本表僅陳述現況,不做任何判定。")
        return "\n".join(out) + "\n"
    except Exception as _e:
        return f"\n### 📉 回撤預算現況\n⚠️ 計算失敗({_e}),不影響其餘報告。\n"


# [V24.12→V24.17 降級] 回撤預算上限。None = 未設定,且【永遠可以不設定】。
# ★V24.17 方向修正(使用者 2026-08-07 明令):
#   我先前把它講成「唯一需要人定的數字」「不定就無法從陳述進到判定」——那是錯的框架。
#   D_max 本質仍是【上限】,只是把單位從資本%換成回撤%。而系統的判準應是
#   【產業週期在哪一段 + 現在在歷史回撤的哪個位置】,不是一個天花板。
#   本常數保留為【純選填的參考線】:設了只多印一行占用比例,不觸發任何阻擋或降級。
#   真正的主表是 render_drawdown_position()(機會視角)。
DRAWDOWN_BUDGET_MAX = None


def render_exposure_status(holdings=None, styles_pct=None):
    """既有持倉曝險狀態表。回傳 markdown 字串(四報共用)。
    永不拋例外——失敗回空字串,不擋報告產出。
    """
    try:
        H = holdings if holdings is not None else USER_HOLDINGS
        if not H or TOTAL_CAPITAL <= 0:
            return ""
        # style 佔比(呼叫端沒給就自己算,路由與 compute_order 一致)
        if styles_pct is None:
            styles_pct = {}
            for _c, _a in H.items():
                _i = TARGET_ETFS.get(_c, {})
                if not _i:
                    continue
                _sk = "QDII" if _is_qdii_etf(_i) else _i.get("style", "?")
                styles_pct[_sk] = styles_pct.get(_sk, 0) + _pct_of_capital(_a)

        single_breach, style_breach = [], []
        for _c, _a in sorted(H.items(), key=lambda x: -x[1]):
            pct = _pct_of_capital(_a)
            if pct > SINGLE_CAP_PCT:
                over_amt = _a - TOTAL_CAPITAL * SINGLE_CAP_PCT / 100.0
                single_breach.append((_c, pct, over_amt))
        for _sk, _p in sorted(styles_pct.items(), key=lambda x: -x[1]):
            cap = STYLE_CAP_PCT.get(_sk, 25)
            if _p > cap:
                over_amt = (_p - cap) / 100.0 * TOTAL_CAPITAL
                style_breach.append((_sk, _p, cap, over_amt))

        if not single_breach and not style_breach:
            return ("\n### 📐 曝險狀態(既有持倉)\n"
                    f"✅ 無超標——單檔上限 {SINGLE_CAP_PCT}%、各 style 上限均未觸及。\n")

        out = ["\n### 📐 曝險狀態(既有持倉)",
               "> 客觀狀態陳述,非減倉建議。上限是啟發式約束,會因市價變動被動觸發(CFA:須持續監控)。"]

        for _c, pct, over_amt in single_breach:
            nm = TARGET_ETFS.get(_c, {}).get("name", _c)
            mdd, ndays, src = compute_max_drawdown_best(_c)
            if mdd is not None:
                impact = pct / 100.0 * mdd
                _short = ("  ⚠️窗長不足一年,未涵蓋更早高低點,**極可能低估**"
                          if ndays < 120 else "")
                dd_txt = (f"近**{ndays}交易日**最大回撤 **{mdd:.1f}%**[源:{src}];若重演,"
                          f"對組合影響約 **{impact:.1f}%**"
                          f"({abs(impact)/100*TOTAL_CAPITAL:,.0f}元){_short}")
            else:
                dd_txt = f"回撤參考:**未校準**(NAV與DB皆不足,可用{ndays}筆,需≥20,待本機補)"
            out.append(f"⚠️ **{_c} {nm}** 單檔 **{pct:.2f}%** > 上限 {SINGLE_CAP_PCT}% "
                       f"→ 超出 **{over_amt:,.0f}元**\n   └ {dd_txt}")

        for _sk, _p, cap, over_amt in style_breach:
            out.append(f"⚠️ **{_sk}** 板塊 **{_p:.2f}%** > 上限 {cap}% → 超出 **{over_amt:,.0f}元**")

        out.append("> ⓘ 上限數值的理論依據見 E-LOG_281;未標註依據者為待校準值(鐵律㉘)。")
        return "\n".join(out) + "\n"
    except Exception as _e:
        return f"\n### 📐 曝險狀態\n⚠️ 計算失敗({_e}),不影響其餘報告。\n"


PILOT_AMT = 500.0

# ════════════════════════════════════════════════════════════════════
# [V22.23 L2因果鏈] 每檔「美股連動係數」β_US — 用於盤前報顯性後驗
#   為何按代碼不按 style: 019141電信(0.10)與008888芯片(0.65)同屬Tech,
#     但隔夜SOX對芯片是大事、對電信幾乎無關。用style會把不連動的硬連動
#     (=6/10 QDII高估的同類錯誤)。
#   用途: 後驗推力 = β_US × 隔夜美股衝擊; 判定昨日結論是否被隔夜推翻。
#   ⚠️ 起始為「分桶估計」非實測, 標未校準; 待隔夜美股數列入庫後回歸校準。
#   範圍 [-1,1]: 正=隨美股同向, 負=反向(如黃金避險), 0=無關。
BETA_US = {
    "006479": 0.90,   # 廣發納指 QDII — 直接追納指
    "018967": 0.90,   # 匯添富納指 QDII — 直接追納指
    "008888": 0.65,   # 芯片 — A股半導體 vs SOX
    "011613": 0.40,   # 科創50 — A股科技,部分連動
    "018177": 0.40,   # 科創增強 — 同科創50
    "016008": 0.35,   # 消費電子 — 中度連動
    "004432": 0.20,   # 有色 — 商品全球但非美股權益
    "019141": 0.10,   # 電信 — 內需,低連動
    "007466": 0.05,   # 紅利 — 防禦低波
    "016185": 0.05,   # 電力 — 內需公用
    "000216": -0.20,  # 黃金 — 避險,風險ON時常被壓(負連動)
}
BETA_US_CALIBRATED = False  # 校準後改 True; 報告會據此標註「估計/實測」

# ══════════════════════════════════════════════════════════════════════
# [V22.27 條件 beta 引擎] Engle Dynamic Conditional Beta — 取代固定 β_US
#   機構作法(Engle 2016 / DCC): β = Cov(標的, 美股) / Var(美股), 時變、滾動。
#   理由(web實證): A股與全球相關性僅~0.34(MSCI/Allianz), 固定0.65過度傳導;
#   危機/脫鉤期 Kalman/條件beta 勝固定beta。脫鉤是副產品, 非另寫if-else。
#   隔夜傳導: A股[t] 對應 美股[t-1](latest US date < A股date), 按「日期」對齊
#   (中美交易日曆不同, 不可位置對齊)。資料不足/抓取失敗 → fallback 固定 BETA_US。
# ══════════════════════════════════════════════════════════════════════
_COND_BETA_CACHE = {}          # {code: float}  本次執行算出的條件beta
COND_BETA_MIN_PAIRS = 20       # 對齊樣本 < 20 → 不可靠, fallback
COND_BETA_CLAMP = (-0.50, 1.30)  # 合理區間(A股不該>1.3 對美股; 黃金可負)



def compute_conditional_beta(asset_rets, us_rets):
    """Engle 條件 beta = Cov(asset,us)/Var(us)。樣本不足/退化 → None。"""
    n = min(len(asset_rets), len(us_rets))
    if n < COND_BETA_MIN_PAIRS:
        return None
    a = asset_rets[-n:]; u = us_rets[-n:]
    ma = sum(a)/n; mu = sum(u)/n
    cov = sum((a[i]-ma)*(u[i]-mu) for i in range(n)) / (n-1)
    var = sum((u[i]-mu)**2 for i in range(n)) / (n-1)
    if var <= 1e-12:
        return None
    beta = cov / var
    lo, hi = COND_BETA_CLAMP
    return max(lo, min(hi, beta))

def get_beta_us(code, default=None):
    """條件beta優先(本次算出且可靠), 否則 fallback 固定 BETA_US。"""
    cb = _COND_BETA_CACHE.get(code)
    if cb is not None:
        return cb
    return BETA_US.get(code, 0.0) if default is None else default

# [V22.27.9] macro_prior Tech 支弱連動縮放 — 見 compute_macro_prior 與 UPGRADE_CHECKLIST P1-1
#   總開關: 設 False → 兩呼叫點傳 None → 完全回舊行為(零回歸驗證用)。
MACRO_TECH_COUPLING_ENABLED = True
_MACRO_TECH_CODES = {"008888", "011613", "018177", "016008"}

# ═══════════════════════════════════════════════════════════════════
# [V22.45] 代理共用偵測 —— 治「018177 吃 011613 的數據」
#   ★根因(2026-07-21 實證):011613(科創50聯接C)與 018177(科創50【增強】A)
#     設定同一 proxy = sh588000 → 018177 的 RSI/trend/actual_ret/預測【全部是 011613 的】。
#     實測:34 個交易日中 23 日(67%)兩檔日報酬【完全相同】——指數增強基金設計上
#     就該偏離指數,不可能三分之二交易日分毫不差。
#   ★影響三層:①018177 全部信號失真(WB 查到 +9~11pp 偏差的真因,非除息)
#             ②兩檔被當獨立持倉計入 Tech 上限,實為同一數據
#             ③回測把同一組資料算兩次 = 偽重複(Claude 先前未抓到的第二個來源)
#   ★正解:主動/增強型基金應走【自身 NAV 序列】(fund.eastmoney pingzhongdata),
#     而非指數 ETF 代理。此處先加【偵測+告警】使問題可見,NAV 接線待 WB live 驗證後上。
# ═══════════════════════════════════════════════════════════════════
ENHANCED_FUND_CODES = {"018177"}   # 增強/主動型:代理≠自身NAV,信號須標註失真風險

def check_proxy_collisions(holdings: dict):
    """偵測多檔持倉共用同一 proxy → 回警告字串列(空=正常)。純唯讀,不拋例外。"""
    warns = []
    try:
        from collections import defaultdict
        m = defaultdict(list)
        for c, info in (holdings or {}).items():
            px = (info or {}).get("proxy")
            if px:
                m[px].append(c)
        for px, codes in m.items():
            if len(codes) > 1:
                warns.append(f"🔴 代理共用:{'/'.join(sorted(codes))} 同用 proxy={px}"
                             f" → 這些持倉的 RSI/trend/報酬【是同一組數據】,"
                             f"不可視為獨立樣本(上限計算與回測都會失真)")
        for c in sorted(ENHANCED_FUND_CODES & set(holdings or {})):
            warns.append(f"⚠️ {c} 為增強/主動型基金,現用指數ETF代理 → 信號無法反映其 alpha,"
                         f"幅度偏差屬【未解釋差異】(已排除除息:2026 無分紅紀錄)")
    except Exception as _e:
        _log_err(_e, "check_proxy_collisions")
    return warns
_MACRO_TECH_COUPLING_DEFAULT = 0.20   # 抓取失敗日(cache空)先驗;弱連動實證,標observe校準

def compute_tech_coupling(default=_MACRO_TECH_COUPLING_DEFAULT):
    """A股科技群平均|條件β|對舊固定假設0.65正規化 → tech_coupling∈[0,1]。
    條件β≈0 → ≈0(關閉SOX對Tech的EV微調)。cache空(抓取失敗)→ 用弱連動先驗default。
    停用(MACRO_TECH_COUPLING_ENABLED=False)→ None(呼叫端據此不傳,回舊行為)。"""
    if not MACRO_TECH_COUPLING_ENABLED:
        return None
    betas = [abs(_COND_BETA_CACHE[c]) for c in _MACRO_TECH_CODES
             if _COND_BETA_CACHE.get(c) is not None]
    if not betas:
        print(f"   [⚠️V22.33.1] 條件β快取空,tech_coupling回退弱連動先驗{default}"
              f"(可能高估美股對Tech的EV影響,待實測β校準)")
        return default
    return max(0.0, min(1.0, (sum(betas) / len(betas)) / 0.65))

# ─────────────────────────────────────────────────────────────
# [V22.28 L2] A股內生崩盤脆弱度分數 — 對應 7/10、7/13 兩次崩盤系統無預警
#   根因:predicted_ret = intraday_proxy_ret(場內即時價)= 觀測非預測,
#         對「盤中反轉/內生崩盤」無預警能力(7/13 早報+0.11% → 收盤-5.02%)。
#   設計:純用系統【既有數據】(主力資金流/RSI/Z/NAV序列),不需新 API。
#   回測(7/6-7/13 真實數據):崩盤預警 2/2=100%、平穩期誤報 0 次。
#   ★v1 曾漏警 7/13(拋物線因子被 7/9+8.22% 與 7/10-5.48% 互相抵銷),
#     v2 加入【波動聚集】因子(前一日大跌 + 近5日有崩盤)後修正。
# ─────────────────────────────────────────────────────────────
FRAGILITY_ENABLED = True          # 總開關:False → 完全不影響 EV(零回歸驗證用)
FRAGILITY_CRASH_TH = -0.04        # 單日 -4% 視為崩盤日



def _fragility_inputs_from(prices, flow5, rsi, z_score):
    """從既有變數萃取 compute_fragility 的輸入。全防禦,任何缺失→安全預設。"""
    p3 = pd_ = 0.0
    crash5 = False
    try:
        if prices and len(prices) >= 4:
            if prices[-4]: p3 = (prices[-1] - prices[-4]) / prices[-4]
            if prices[-2]: pd_ = (prices[-1] - prices[-2]) / prices[-2]
            # 近5日是否有崩盤日
            for i in range(1, min(5, len(prices))):
                if prices[-i-1] and (prices[-i] - prices[-i-1]) / prices[-i-1] <= FRAGILITY_CRASH_TH:
                    crash5 = True
                    break
    except Exception as _e:
        _log_err(_e, "fragility_price_inputs")
    neg_days = 0
    try:
        if flow5 and isinstance(flow5, dict):
            for v in (flow5.get("flow5_list") or []):
                if isinstance(v, (int, float)) and v < 0:
                    neg_days += 1
    except Exception as _e:
        _log_err(_e, "fragility_flow_inputs")
    return dict(prev3_ret=p3, prev_day_ret=pd_, crash_in_5d=crash5,
                main_flow_neg_days=neg_days, rsi=rsi, z_score=z_score)

# ═════════════════════════════════════════════════════════════
# [V22.29] 四報因果鏈修復 — T+1 預測引擎
#
# 【問題】四報初衷=「今天的報告預測明天的盤勢」,但:
#   - predicted_ret = intraday_proxy_ret(場內代理【當日】價)= 觀測非預測
#   - OutlookTeam「明日前瞻」只複述【今日】的 action/EV,不產出明日預測
#   - morning_premise 表標「待做」(L372),跨報因果鏈從未實作
#   → 收盤報不產出 T+1 預測 → 盤前報無前提可讀 → 早報無法驗證 → 因果鏈斷裂
#
# 【設計】以【資金流】為核心——它是唯一有實證支持的【領先指標】;
#   RSI/趨勢是同步/滯後指標,只做均值回歸修正。
#
# 【⚠️ 誠實標記(E-LOG #258 鐵律⑳)】
#   本引擎【未經真實數據驗證】。DB 目前無 ETF 歷史 RSI/資金流/Z,
#   故【無法回測】。部署後先 observe-only 落庫,兩週後才有真數據可校準權重。
#   ★ 不得在無真數據時宣稱任何命中率。
# ═════════════════════════════════════════════════════════════
T1_PREDICT_ENABLED = True      # 總開關:False → 不產出T+1預測(零回歸)
T1_OBSERVE_ONLY   = True       # ★校準期:True → 只落庫+顯示,不驅動任何操作建議

# ═════════════════════════════════════════════════════════════
# [V22.30] 四報時序修正 — 決策時點對齊(Decision Point Alignment)
#
# 【使用者的真實流程】
#   盤前報 08:40 : 隔夜美股 + 產業新聞 + 昨日收盤premise → 【今日盤勢預測】
#   早報   10:30 : 開盤已穩 → 驗證盤前預測(果因診斷) → 初步買賣判斷
#   午報   12:00 : 沉澱期 → 更新
#   ★午報 14:00 : 【最終決策點】← 場外基金15:00前下單=當日NAV,這是最後行動機會
#   收盤報 17:30 : 完整檢討 + 寫入明日premise
#
# 【V22.29 的架構錯誤】把 T+1 預測放在收盤報(17:30)——
#   預測出來時使用者【已經不能下單了】。決策無用的預測=廢的。
#
# 【機構做法(Decision Point Alignment)】所有信號必須在【決策時點】前收斂。
#   → T+1 預測必須在【午報決策窗口(≥13:30)】產出,驅動「今天15:00前買不買」。
#   → 收盤報的 T+1 只是寫給明日盤前報的 premise(輸入),不是決策用。
# ═════════════════════════════════════════════════════════════
# [V22.32 HOTFIX] _now_utc8 未定義 → NameError(WB 實環境回報)
#   根因:Claude 在 V22.30 寫 is_decision_window 時【憑空假設】此函數存在,
#         未 grep 確認 → 違反自身鐵律「呼叫函數前必 grep」。
#   系統本機在台北(UTC+8),datetime.now() 即為 UTC+8;此處顯式化避免歧義。

DECISION_WINDOW_HOUR = 13      # 午報 ≥13:30 視為決策窗口(場外15:00截單前)
DECISION_WINDOW_MIN  = 30

def is_decision_window(r_type):
    """午報且 ≥13:30 → 最終決策窗口(場外15:00截單前最後行動機會)"""
    if r_type != "午報":
        return False
    n = _now_utc8()
    return (n.hour > DECISION_WINDOW_HOUR or
            (n.hour == DECISION_WINDOW_HOUR and n.minute >= DECISION_WINDOW_MIN))

# ═════════════════════════════════════════════════════════════
# [V22.31] 收盤報歸因引擎 — 「今天為什麼漲/跌?」
#
# 【使用者的定義】收盤報不是決策引擎,是【歸因/解釋引擎】:
#   是利空新聞?利多新聞?是買家布局多日收網?還是賣家絞殺成功?
#   三星獲利成長19倍卻慘跌10% —— 這種現象只有歸因能解釋,理解它才能預測下次。
#
# 【四報是學習閉環(OODA)】
#   盤前=預測(先驗) → 早報=驗證(證據) → 午報=行動(決策) → 收盤=【解釋(學習)】
#   沒有收盤報的歸因,系統永遠不會進步——它從不知道自己為什麼對、為什麼錯。
#
# 【★機構核心:殘差(unexplained residual)才是最重要的警報】
#   殘差小 → 我們理解今天發生了什麼(模型有效)
#   殘差大 → 【有我們不知道的事在發生】← 這是最有價值的信號
#   三星案例:基本面大利多,實際暴跌 → 巨大負殘差 → 「利好出盡/預期已 price in」
# ═════════════════════════════════════════════════════════════
ATTRIBUTION_ENABLED = True
RESIDUAL_ALERT_TH   = 2.0     # 殘差 >2pp → 有未知因素主導

# ═════════════════════════════════════════════════════════════
# [V22.32] 早報核心:資金流【主體識別】— 誰在買?誰在賣?
#
# 【使用者的定義】早報(開盤1.5-2h,第一波廝殺後漸穩)要看的是:
#   散戶、機構、主權基金、國家隊的走向 → 判斷上半場走勢。
#
# 【源碼事實】系統的 get_order_flow_breakdown 已把「機構」與「散戶」分開
#   (超大單欄=機構合計, 小單欄=散戶合計),但從未做成判讀 → 最大的浪費。
#
# 【★核心:聰明錢 vs 笨錢的【背離】才是信號】
#   機構買 + 散戶賣  = 吸籌(最強看多)
#   機構賣 + 散戶買  = 派發給散戶(經典出貨,最強看空)
#
# 【★★機構級洞察(Claude 主動提醒):單日是噪音,【多日累積背離】才是 alpha】
#   - 機構連續淨流入 + 股價【不漲】 = 吸籌(最強,籌碼在換手但價未動)
#   - 機構連續淨流出 + 股價【不跌】 = 派發(最強警告,主力在出貨但撐住價)
#   ⚠️ 局限:東財分類按【成交金額】非帳戶類型;機構可用算法拆單偽裝成中小單。
#      量化高頻拆單會污染此信號 → 不可盲信,必須結合價格行為交叉驗證。
# ═════════════════════════════════════════════════════════════
FLOW_ACTOR_ENABLED = True
FLOW_SIG_TH = 0.3      # 單邊 >0.3億 視為顯著



def identify_flow_actors(inst_yi=0.0, retail_yi=0.0, price_ret_pct=0.0,
                         inst_5d_yi=None):
    """資金流主體識別。回傳 (signal, strength, reason)。
    inst_yi   : 今日機構淨流入(億) ← bd["超大單"]
    retail_yi : 今日散戶淨流入(億) ← bd["小單"]
    price_ret_pct : 今日漲跌(%)
    inst_5d_yi: 5日機構累積(億, 可選) ← 多日背離才是真信號
    ⚠️ 係數未經真數據校準(E-LOG #258),observe 期僅供判讀。
    """
    # [V22.68 治「NULL→0 產生結論」] ★原本 `inst_yi or 0`:資料缺失時被當成「機構零流入」,
    #   於是輸出「散戶主導」「無明顯主力」等【結論】—— 但真相是【不知道】。與 V22.49 修 flow5 同族。
    #   判準:0 作為「中性預設」可接受;但這裡 0 會被拿去比大小【產生判讀】,故缺失須提前返回。
    if inst_yi is None and retail_yi is None:
        return ("⬜ **資金主體資料不可用**", 0,
                "機構/散戶淨額【未取得】→ 無法判斷誰在買賣,**請勿當作『無主力動作』**")
    i = inst_yi or 0
    rt = retail_yi or 0
    p = price_ret_pct or 0
    i5 = inst_5d_yi

    # ① 機構 vs 散戶背離(最核心)
    inst_sig = abs(i) >= FLOW_SIG_TH
    ret_sig = abs(rt) >= FLOW_SIG_TH

    if inst_sig and ret_sig and i > 0 and rt < 0:
        base = ("🟢🟢 **機構吸籌·散戶離場**", 2,
                f"機構+{i:.2f}億 / 散戶{rt:.2f}億 → 聰明錢在收,笨錢在跑(**最強看多背離**)")
    elif inst_sig and ret_sig and i < 0 and rt > 0:
        base = ("🔴🔴 **機構派發·散戶接盤**", -2,
                f"機構{i:.2f}億 / 散戶+{rt:.2f}億 → **經典出貨**(主力賣給散戶,最強看空)")
    elif inst_sig and i > 0:
        base = ("🟢 機構淨買", 1, f"機構+{i:.2f}億(散戶{rt:+.2f}億,無明顯背離)")
    elif inst_sig and i < 0:
        base = ("🔴 機構淨賣", -1, f"機構{i:.2f}億(散戶{rt:+.2f}億)")
    elif ret_sig and rt > 0:
        base = ("🟡 僅散戶買(機構未動)", 0,
                f"散戶+{rt:.2f}億但機構{i:+.2f}億 → 散戶自嗨,無機構背書,慎")
    else:
        base = ("⚪ 主體不明", 0, f"機構{i:+.2f}億 / 散戶{rt:+.2f}億,皆不顯著")

    sig, strength, reason = base

    # ② ★多日累積 × 價格行為 = 真 alpha(單日是噪音)
    if i5 is not None and abs(i5) >= 1.0:
        if i5 > 0 and abs(p) < 1.0:
            sig = "🟢🟢🟢 **吸籌(多日)**"
            strength = 3
            reason += f" | ★機構5日累積+{i5:.1f}億但股價僅{p:+.1f}% → **籌碼在換手、價未動 = 吸籌**"
        elif i5 < 0 and abs(p) < 1.0:
            sig = "🔴🔴🔴 **派發(多日)**"
            strength = -3
            reason += f" | ★機構5日累積{i5:.1f}億但股價撐住({p:+.1f}%) → **主力在出貨卻護價 = 派發**"
        elif i5 > 0 and p > 2.0:
            reason += f" | 機構5日+{i5:.1f}億且今日拉升{p:+.1f}% → 收網中(追高風險)"
        elif i5 < 0 and p < -2.0:
            reason += f" | 機構5日{i5:.1f}億且今日殺跌{p:+.1f}% → 派發完成"

    return (sig, strength, reason)

def flow5_outage_days():
    """[V22.49] 回 flow5 已連續 NULL 的交易日數(0=正常)。供報告升級告警。
    ★WB 建議:目前斷線只『誠實留空』,不會提醒已卡幾天 → 容易被遺忘。"""
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            rows = conn.execute(
                "SELECT date, SUM(flow5_yi IS NOT NULL) FROM daily_snapshot "
                "GROUP BY date ORDER BY date DESC LIMIT 30").fetchall()
        n = 0
        for _d, _has in rows:
            if (_has or 0) > 0:
                break
            n += 1
        return n
    except Exception as _e:
        _log_err(_e, "flow5_outage_days")
        return 0



def attribute_today(actual_ret_pct=0.0, macro_prior=0.0, main_flow_yi=0.0,
                    flow5_yi=0.0, rsi_prev=50.0, news_score=0.0,
                    fragility=0, is_qdii=False):
    """收盤報歸因:今天的漲跌,拆解成各因子的貢獻 + 殘差。
    回傳 dict(contributions, explained, residual, alert, behavior)。

    ⚠️ 係數★未經真數據校準(E-LOG #258)。observe 期僅供解釋,不驅動決策。
    ★殘差才是重點:殘差大 = 有我們不知道的因素(如三星「利好出盡」)。
    """
    a = actual_ret_pct or 0
    c = {}
    # ① 美股傳導(#170:QDII真連動;A股科技弱連動β≈0)
    c["🌎 美股傳導"] = round((macro_prior or 0) * (2.0 if is_qdii else 0.3), 2)
    # ② 主力資金流(今日)
    c["💰 主力資金"] = round(max(-1.5, min(1.5, (main_flow_yi or 0) / 2.0)), 2)
    # ③ 技術回歸(前日RSI極端 → 今日反向)
    _r = rsi_prev if rsi_prev is not None else 50
    tech_c = 0.0
    if   _r >= 80: tech_c = -1.2
    elif _r >= 70: tech_c = -0.6
    elif _r <= 20: tech_c = +1.2
    elif _r <= 30: tech_c = +0.6
    c["📉 技術回歸"] = tech_c
    # ④ 產業新聞
    c["📰 產業新聞"] = round((news_score or 0) * 1.2, 2)
    # ⑤ 內生脆弱度(拋物線/波動聚集的下行壓力)
    c["⚠️ 內生脆弱"] = round(-0.02 * (fragility or 0), 2) if (fragility or 0) >= 50 else 0.0

    explained = round(sum(c.values()), 2)
    residual  = round(a - explained, 2)

    # ★殘差警報 — 機構最看重的信號
    # ★三星悖論優先判定:新聞【大利多】卻【暴跌】= 利好出盡(不看 explained 正負,
    #   因為資金流/技術回歸常會把 explained 壓負,反而遮蔽了「基本面利多卻跌」這個矛盾)
    alert = ""
    _ns = news_score or 0
    if _ns >= 0.3 and a <= -2.0:
        alert = (f"🚨 **利好出盡型(三星悖論)**:產業新聞【利多 {_ns:+.2f}】卻暴跌 {a:+.2f}% "
                 f"(殘差 {residual:+.2f}pp)→ 疑「**預期已 price in / 獲利了結 / "
                 f"forward guidance 不佳**」。三星獲利成長19倍卻跌10%即此類。"
                 f"**利多兌現=賣點,不是買點。**")
    elif _ns <= -0.3 and a >= 2.0:
        alert = (f"🚨 **利空出盡型**:產業新聞【利空 {_ns:+.2f}】卻大漲 {a:+.2f}% "
                 f"(殘差 {residual:+.2f}pp)→ 疑「**利空已 price in / 超跌反彈 / 空頭回補**」。"
                 f"**壞消息不再跌 = 底部信號。**")
    elif abs(residual) >= RESIDUAL_ALERT_TH:
        _dir = "跌" if a < 0 else "漲"
        alert = (f"⚠️ **殘差 {residual:+.2f}pp 偏高**:已知因子(美股/資金/技術/新聞)"
                 f"只解釋了 {explained:+.2f}pp,實際{_dir} {a:+.2f}% → "
                 f"**有我們不知道的因素在主導**(未捕捉的催化/政策/情緒/跨市場傳導)。"
                 f"**這是最該追查的信號。**")

    behavior, behavior_why = interpret_flow_behavior(flow5_yi, a, main_flow_yi)
    return dict(contributions=c, explained=explained, residual=residual,
                alert=alert, behavior=behavior, behavior_why=behavior_why)


def build_volume_monitor():
    """[V23.4 item6] 板塊成交額監測(東財限流替代)——用 sina 板塊指數量判操盤手放量/縮量意圖。
    場外聯接本身無成交額,量在對應板塊指數上。★板塊代碼待擴充驗證(現含上證/光通信/半導體,勿猜)。"""
    _monmap = [
        ("上證大盤", "sh000001"), ("黃金", "sh518880"), ("有色", "sh512400"),
        ("紅利", "sh512890"), ("芯片", "sz159995"), ("科創50", "sh588000"),
        ("消費電子", "sz159779"), ("電力", "sz159611"), ("通信(019072)", "sz159695"),
        ("電信", "sh563010"), ("納指QDII", "sh513100"),
        ("軍工(觀)", "sh512680"), ("機器人(觀)", "sh562500"), ("電池(觀)", "sz159755"),
        ("半導體設備(觀)", "sh562590"), ("電網(觀)", "sz159326"),
    ]   # [V23.4→擴充] 複用各ETF已驗證的場內proxy碼(有成交額),覆蓋全持倉+觀察池,不猜碼
    _rows = []
    for _nm, _code in _monmap:
        if not _code:
            continue
        try:
            _k = DirectDataFetcher.get_index_kline_sina(_code, datalen=8)
            if not _k or len(_k) < 6:
                continue
            _vols = [x.get("volume", 0) for x in _k]
            if sum(1 for v in _vols if v) < 6:
                continue
            _tv = _vols[-1]; _avg5 = sum(_vols[-6:-1]) / 5.0
            _vr = (_tv / _avg5) if _avg5 else 1.0
            _pr = ((_k[-1]["close"] - _k[-2]["close"]) / _k[-2]["close"] * 100) if _k[-2]["close"] else 0.0
            if _vr > 3.0:
                _sig = "⚠️ 暴量異常(變盤)"
            elif _vr > 1.0 and _pr > 1.0:
                _sig = "✅ 溫和放量上漲(機構掃貨)"
            elif _vr > 1.0 and _pr < -1.0:
                _sig = "🔴 放量下跌(主力出貨/恐慌)"
            elif _vr < 1.0 and abs(_pr) < 1.0:
                _sig = "🟢 縮量橫盤(拋壓衰竭·可能見底)"
            elif _vr < 1.0 and _pr < -1.0:
                _sig = "⚠️ 縮量破位(無承接)"
            # [V24.13] 補齊落空分支(原 else 直接吐「—」,兩種組合永遠無判讀):
            #   (a) _vr<1.0 且 _pr>1.0  → 縮量上漲(量價背離,patch_v24_11 修補②指出)
            #   (b) _vr>1.0 且 |_pr|<=1.0 → 放量橫盤(本線額外發現,patch 亦未涵蓋)
            elif _vr < 1.0 and _pr > 1.0:
                _sig = "🟡 縮量上漲(無量空漲·量價背離·續航存疑)"
            elif _vr >= 1.0 and abs(_pr) <= 1.0:
                _sig = "🟠 放量橫盤(多空分歧·變盤前兆)"
            else:
                _sig = f"⚪ 無明確意圖(量比{_vr:.2f}x/漲跌{_pr:+.2f}%)"
            _rows.append(f"| {_nm} | {_pr:+.2f}% | {_vr:.2f}x | {_sig} |")
        except Exception as _e:
            _log_err(_e, "build_volume_monitor:L2680")  # [V24.16] 原為靜默 continue
            continue
    if not _rows:
        return ["\n## 📊 板塊成交額監測(東財限流替代)\n", "> ⚠️ sina 板塊量今日未取得(可能限流),待恢復。"]
    return (["\n## 📊 板塊成交額監測(東財限流替代·sina量)\n",
             "| 板塊 | 今日漲跌 | 量/5日均 | 操盤手意圖 |", "|---|---|---|---|"] + _rows +
            ["> 規則:放量下跌=出貨｜縮量橫盤=拋壓衰竭可能見底🟢｜溫和放量上漲=掃貨✅｜暴量>3x=變盤⚠️"
             "｜縮量上漲=量價背離🟡｜放量橫盤=分歧🟠 [V24.13補齊,原有兩組合落空吐「—」]",
             "> ★板塊代碼待擴充(現上證/光通信/半導體);場外聯接無成交額,量取對應板塊指數。"])


def build_daily_conclusion(results, att_cache):
    """[V23.9 修用戶投訴] 收盤報「今日結論總結」——用戶兩次指出「依舊沒有給出結論總結」。
    根因:歸因/殘差的 actual 用場外聯接NAV(T+1 才結算,收盤時是 T-1 滯後值,封頂±0.07%),
    → 殘差全被壓成噪音,系統判「全可解釋」,最大的未知因子(板塊崩跌)反而看不見。
    本節改用 live 板塊K線當真實基準,算「真實殘差」,並合成一句主線結論。"""
    out = ["\n## 🧭 今日結論總結（今天到底發生什麼·一句話版）\n"]
    _reals = []
    try:
        for _nm, _code in [("大盤", REVERSAL_BROAD_IDX)] + list(REVERSAL_CORE_IDX.items()):
            if not _code:
                continue
            _k = DirectDataFetcher.get_index_kline_sina(_code, datalen=3)
            if _k and len(_k) >= 2 and _k[-2].get("close"):
                _reals.append((_nm, (_k[-1]["close"] - _k[-2]["close"]) / _k[-2]["close"] * 100))
    except Exception as _e:
        _log_err(_e, "build_daily_conclusion:L2705")  # [V24.16] 原為靜默 pass
        pass
    if not _reals:
        return out + ["> ⚠️ live 板塊指數今日未取得,無法合成結論(不編造)。"]

    _broad = dict(_reals).get("大盤", 0.0)
    _sectors = [(n, r) for n, r in _reals if n != "大盤"]
    _worst = min(_sectors, key=lambda x: x[1]) if _sectors else None
    _best = max(_sectors, key=lambda x: x[1]) if _sectors else None

    # [V24.19 使用者指正] 原模板寫死「最弱/領跌/跌幅」,不分辨方向:
    #   板塊 +1.45% 也會被寫成「最弱」,第③句更直接說「明日NAV才會反映今日跌幅」
    #   ——**漲了卻在示警**。改為隨實際方向敘述。
    _all_up = all(r > 0 for _, r in _sectors) if _sectors else False
    _all_dn = all(r < 0 for _, r in _sectors) if _sectors else False
    _line = f"> **① 主線**：大盤 {_broad:+.2f}%"
    if _worst and _best and abs(_worst[1] - _best[1]) > 2.0:
        _tag_w = "領跌" if _worst[1] < 0 else "漲最少"
        _line += (f"，但**極度分化**——{_best[0]} {_best[1]:+.2f}% 最強 / "
                  f"{_worst[0]} {_worst[1]:+.2f}% {_tag_w}(價差 {abs(_best[1]-_worst[1]):.2f}pp)。")
    elif _worst and _all_up:
        _line += f"，**板塊全數收紅**，其中 {_worst[0]} {_worst[1]:+.2f}% 漲最少。"
    elif _worst and _all_dn:
        _line += f"，**板塊全面收綠**，其中 {_worst[0]} {_worst[1]:+.2f}% 跌最深。"
    elif _worst:
        _line += f"，{_worst[0]} {_worst[1]:+.2f}% 相對最弱、{_best[0]} {_best[1]:+.2f}% 相對最強。"
    out.append(_line)

    # 真實殘差:live 板塊 vs 場外聯接NAV(滯後)
    _navs = {r.get("name", "")[:8]: (r.get("predicted_ret", 0) or 0) for r in (results or [])}
    _nav_avg = (sum(_navs.values()) / len(_navs)) if _navs else 0.0
    if _worst:
        _resid = _worst[1] - _nav_avg
        out.append(f"> **② 最大真實殘差**：{_worst[0]} live {_worst[1]:+.2f}% vs 場外NAV均 {_nav_avg:+.2f}% "
                   f"= **{_resid:+.2f}pp**。★這才是今天最大的未知因子;歸因表用滯後NAV當 actual,"
                   f"會把它壓成 ±0.0x 噪音而誤判「全可解釋」→ 待 T+1 NAV 結算後複核。")
    # [V24.19] ③讀法隨方向;④補「對持倉的意涵」(使用者:總結要回答對我的持倉意味什麼)
    _dir_word = "漲幅" if (_worst and _worst[1] > 0) or _all_up else "跌幅"
    if _dir_word == "漲幅":
        out.append("> **③ 讀法**：板塊今日的**漲幅**才是真實發生的事;場外聯接NAV 因 T+1 結算而滯後,"
                   "**你的帳面現在還沒體現這段漲幅**——明日 NAV 才會補上。"
                   "反過來說,今天看到的帳面數字不是今天的成果。")
    else:
        out.append("> **③ 讀法**：板塊今日的**跌幅**才是真實發生的事;場外聯接NAV 因 T+1 結算而滯後,"
                   "不代表你的持倉沒事——明日 NAV 才會反映今日板塊跌幅。")
    try:
        _hold_map = {}
        for _r in (results or []):
            _sec = (_r.get("sector") or _r.get("style") or "")
            _hold_map.setdefault(_sec, []).append(_r.get("name", "")[:8])
        _impact = []
        for _nm, _rv in ((_best, "最強"), (_worst, "最弱")):
            if not _nm:
                continue
            _hits = [h for k, v in _hold_map.items() if _nm[0] in k or k in _nm[0] for h in v]
            if _hits:
                _impact.append(f"{_nm[0]}({_nm[1]:+.2f}%,{_rv})對應持倉:{'、'.join(_hits[:4])}")
        if _impact:
            out.append("> **④ 對你的持倉**：" + ";".join(_impact)
                       + "。板塊方向會在明日 NAV 上體現,今日帳面尚未反映。")
        else:
            out.append("> **④ 對你的持倉**：今日最強/最弱板塊**均無對應持倉**"
                       "→ 今日板塊分化與你的部位關聯低,不必據此調整。")
    except Exception as _e:
        _log_err(_e, "conclusion_holding_impact")
    out.append("> ⚠️ 本結論由 live 指數合成,歸因權重仍 observe-only 未校準(E-LOG #258)。")
    return out


def _build_market_summary(results, att_cache):
    """[V22.92] 收盤報「今日盤面總結」——填補「為什麼漲跌」的空白 blockquote。
    合成：漲跌分布 / 均值回報 / 主導因子 / 異常信號 / 反轉偵測。
    回傳 [string, ...] 可直接塞入 _ab 陣列。"""
    if not results:
        return ["> ⚠️ 本日無標的資料，無法生成盤面總結。"]
    rets = []
    for r in results:
        _act = r.get("predicted_ret", 0) or 0
        rets.append(_act)
    n = len(rets)
    n_up = sum(1 for x in rets if x > 0)
    n_down = sum(1 for x in rets if x < 0)
    n_flat = sum(1 for x in rets if x == 0)
    avg_ret = sum(rets) / n if n else 0
    max_ret = max(rets) if rets else 0
    min_ret = min(rets) if rets else 0

    # --- 方向判斷 ---
    if avg_ret > 0.5:
        _dir_word = "偏多"
        _dir_emoji = "🟢"
    elif avg_ret > 0:
        _dir_word = "微幅收紅"
        _dir_emoji = "🟢"
    elif avg_ret > -0.5:
        _dir_word = "微幅收綠"
        _dir_emoji = "🔴"
    else:
        _dir_word = "偏空"
        _dir_emoji = "🔴"

    # --- 全域因子主導度（跨 ETF 加總）---
    _factor_totals = {"🌎 美股傳導": 0, "💰 主力資金": 0, "📉 技術回歸": 0, "📰 產業新聞": 0}
    for _c, (_nm, _act, _att) in att_cache.items():
        for _fk in _factor_totals:
            _factor_totals[_fk] += _att["contributions"].get(_fk, 0)
    # 找絕對值最大的全域因子
    _global_dom = sorted(_factor_totals.items(), key=lambda x: abs(x[1]), reverse=True)
    _dom_name, _dom_val = _global_dom[0] if _global_dom else ("?", 0)

    # --- 反轉/異常信號 ---
    _signals = []
    # 超過半數下跌但均值收紅 → 可能「權重股拉指數、個股普跌」
    if n_down > n_up and avg_ret > 0:
        _signals.append(f"⚠️ **分化行情**: {n_down}隻下跌 vs {n_up}隻上漲，但均值{avg_ret:+.2f}%收紅 → 權重股/紅利撐指數，個股弱")
    # 超過半數上漲且均值明顯正 → 「由黑翻紅」候選
    if n_up > n_down and avg_ret > 0.3:
        _signals.append(f"✨ **多方占優**: {n_up}/{n} 隻收紅，均值{avg_ret:+.2f}%")
    # 最大跌幅很深
    if min_ret < -3:
        _signals.append(f"🔴 **最深跌幅**: {min_ret:+.2f}%（個別標的重創區）")
    # 最大涨幅很猛
    if max_ret > 3:
        _signals.append(f"🟢 **最強反彈**: {max_ret:+.2f}%")

    # [V22.99 治「場外NAV說微幅收綠、實際崩盤」] 先報真實指數級盤面(sina 源,東財限流下仍可用);
    #   根因:原本只用 results.predicted_ret(場外聯接NAV,結算滯後→幅度極小)→ 崩盤日誤顯「微幅收綠」。
    _real_lead = None
    try:
        _idxmap = [("大盤", REVERSAL_BROAD_IDX)] + list(REVERSAL_CORE_IDX.items())
        _reals = []
        for _inm, _icode in _idxmap:
            if not _icode:
                continue
            _k = DirectDataFetcher.get_index_kline_sina(_icode, datalen=3)
            if _k and len(_k) >= 2 and _k[-2].get("close"):
                _reals.append((_inm, (_k[-1]["close"] - _k[-2]["close"]) / _k[-2]["close"] * 100))
        if _reals:
            _txt = "｜".join(f"{_n} {_r:+.2f}%" for _n, _r in _reals)
            _cd = [n for n, r in _reals if n != "大盤" and r < -1.0]
            _tag = ("→ " + "/".join(_cd) + " 領跌(AI硬件被砸)") if _cd else ""
            _real_lead = f"> 🌐 **真實盤面(指數級·sina)**：{_txt} {_tag}"
    except Exception:
        _real_lead = None
    # --- 組裝輸出 ---
    out = [_real_lead] if _real_lead else []
    out.append(
        f"> {_dir_emoji} **場外聯接NAV盤面**（結算滯後、幅度偏小,非板塊真實漲跌）: {_dir_word}（均價{avg_ret:+.2f}% / "
        f"{n_up}漲 {n_down}跌 {n_flat}平 / "
        f"最强{max_ret:+.2f}% 最弱{min_ret:+.2f}%）。"
        f"全域主導因子：{_dom_name}(合計{_dom_val:+.2f}pp)。")
    if _signals:
        out.append("> ")
        for s in _signals:
            out.append(f"> {s}")
    return out


# [V22.94] 輪動反抽 vs 反轉確認 — live 板塊/指數資料來源(經聯網+沙盒雙驗證,見 E-LOG)
#   光通信 = 國證通信  sz399389  ✅   |   半導體 = 國證芯片  sz980017  ✅
#   ⚠️ sz399417 是【新能源車】,非半導體 → 不可用(Claude 原猜錯,WB 聯網核證修正)
#   大盤   = 上證指數  sh000001
# 數據源鐵律(繼承 patch_reversal_criteria.py):【絕不使用 results.predicted_ret】(場外聯接NAV,7/29最大才-0.07%,
#   根本不反映板塊 -1.14%);一律用 live 板塊/指數 K線。observe-only 偵測,不進 EV/決策。
REVERSAL_BROAD_IDX = "sh000001"
REVERSAL_CORE_IDX = {"光通信": "sz399389", "半導體": "sz980017"}


def _load_assess_reversal():
    """匯入用戶交付的量化判定式(patch_reversal_criteria.assess_reversal),失敗回 None(誠實標註,不靜默)。"""
    try:
        from patch_reversal_criteria import assess_reversal
        return assess_reversal
    except Exception as _e:
        _log_err(_e, "load_assess_reversal")
        return None


def _build_reversal_judgment(results, att_cache, flow_map=None):
    """[V22.94] 收盤報「由黑翻紅研判：反抽 or 誘敵深入？」專區。
    [V22.93→V22.94] 重大修正:拋棄舊版用 results.predicted_ret(場外聯接NAV)算廣義/核心漲跌的模糊邏輯,
    改呼叫用戶交付的量化判定式 assess_reversal(),輸入全部來自 live 板塊/指數 K線(國證通信/國證芯片/上證):
      - broad_ret     ← 上證指數 K線(今收-昨收)/昨收
      - core_ret      ← min(光通信,半導體 兩板塊 K線報酬) → 任一核心板塊續跌即計續跌
      - core_flow5    ← 持倉核心ETF(OptComm/Tech,排除QDII) _flow_map 5日淨流合計(億,觀察用代理)
      - idx_price/ma10← 上證 K線收盤 / ma_price10(sina直接給)
      - today_low/prev_trough_low ← 上證 K線今日最低 / 前段窗口最低
    回傳 [string,...] 塞入 _ab。"""
    out = ["\n### 🔄 由黑翻紅研判：反抽 or 誘敵深入？（量化判定式 V22.94）\n"]
    out.append("> 📌 數據源 = live 板塊/指數 K線(國證通信399389 + 國證芯片980017 + 上證000001),"
                "**非**聯接NAV。⚠️ observe-only 偵測,不進決策。")
    _assess = _load_assess_reversal()
    if _assess is None:
        out.append("> ⚠️ 判定式模組(patch_reversal_criteria.assess_reversal)未載入,本區暫停。")
        return out
    if not results:
        return out

    # ── 1) live 大盤 + 核心板塊 K線 ──
    _broad_ret = _idx_price = _idx_ma10 = _today_low = _prev_trough = None
    try:
        _bk = DirectDataFetcher.get_index_kline_sina(REVERSAL_BROAD_IDX, datalen=25)
        if _bk and len(_bk) >= 2 and _bk[-2]["close"]:
            _idx_price = _bk[-1]["close"]
            _idx_ma10 = _bk[-1]["ma_price10"]
            _today_low = _bk[-1]["low"]
            _prev_trough = min(r["low"] for r in _bk[:-1]) if len(_bk) > 2 else None
            _broad_ret = (_bk[-1]["close"] - _bk[-2]["close"]) / _bk[-2]["close"]
    except Exception as _e:
        _log_err(_e, "reversal_broad_kline")
    _core_ret = None
    _core_ret_detail = {}
    try:
        for _nm, _code in REVERSAL_CORE_IDX.items():
            _ck = DirectDataFetcher.get_index_kline_sina(_code, datalen=25)
            if _ck and len(_ck) >= 2 and _ck[-2]["close"]:
                _core_ret_detail[_nm] = (_ck[-1]["close"] - _ck[-2]["close"]) / _ck[-2]["close"]
        if _core_ret_detail:
            _core_ret = min(_core_ret_detail.values())   # 任一核心板塊續跌即計續跌
    except Exception as _e:
        _log_err(_e, "reversal_core_kline")

    # ── 2) 核心板塊機構5日淨流(持倉核心ETF合計,觀察用代理) ──
    _core_flow5 = None
    try:
        _core_styles = {"OptComm", "Tech"}
        _cf = 0.0
        _cf_n = 0
        for r in results:
            if r.get("style") in _core_styles and not _is_qdii_etf(TARGET_ETFS.get(r.get("code", ""), {})):
                _cf += (flow_map or {}).get(r.get("code", ""), 0) or 0
                _cf_n += 1
        _core_flow5 = _cf if _cf_n else None
    except Exception as _e:
        _log_err(_e, "reversal_core_flow")

    # ── 3) 呼叫量化判定式 ──
    _res = _assess(broad_ret=_broad_ret, core_ret=_core_ret, core_flow5=_core_flow5,
                   idx_price=_idx_price, idx_ma10=_idx_ma10,
                   today_low=_today_low, prev_trough_low=_prev_trough)
    out.append(f"> \n> **判定**: **{_res['verdict']}**")
    if _res.get("warning"):
        out.append(f"> {_res['warning']}")
    # 數據來源透明標註
    _src = (f"大盤{_broad_ret*100:+.2f}%" if _broad_ret is not None else "大盤?")
    _csrc = "、".join(f"{k}{v*100:+.2f}%" for k, v in _core_ret_detail.items()) or "核心?"
    _cflow = (f"核心ETF5日淨流{_core_flow5:+.2f}億" if _core_flow5 is not None else "核心ETF5日淨流?")
    out.append(f"> 📡 來源: {_src} ｜ 核心板塊[{_csrc}] ｜ {_cflow}")

    # ── 4) 反轉確認三條件(區分「真False」vs「數據缺失⏳」) ──
    out.append("> \n> **🔍 反轉確認三條件（齊備才視為真反轉，否則誘敵 / 反抽）**：")
    _c1_missing = (_core_ret is None or _core_flow5 is None)
    _c2_missing = (_idx_price is None or _idx_ma10 is None)
    _c3_missing = (_today_low is None or _prev_trough is None)
    _miss = [_c1_missing, _c2_missing, _c3_missing]
    for (_k, _v), _m in zip(_res["conditions"].items(), _miss):
        if _m:
            _mk = "⏳"
            _note = "（數據缺失,保守計不入反轉）"
        elif _v:
            _mk = "✅"
            _note = ""
        else:
            _mk = "❌"
            _note = ""
        out.append(f"> {_mk} {_k}{_note}")

    # ── 5) 持倉 019072(嘉實通信/光通信)專屬提醒 — 用 live 光通信板塊報酬判接刀子區 ──
    _h = att_cache.get("019072")
    if _h:
        _nm, _act, _att = _h
        _flow019 = (flow_map or {}).get("019072", 0) if flow_map else 0
        _telecom_ret = _core_ret_detail.get("光通信")
        if _telecom_ret is not None and _telecom_ret < -0.005:
            _warn = "仍處接刀子區(光通信板塊續跌)"
        elif _telecom_ret is not None:
            _warn = "光通信板塊已止跌、但反轉三條件未齊"
        else:
            _warn = "仍處接刀子區" if _act < 0 else "已止跌、但反轉未確"
        out.append(f"\n> 📌 **持倉 019072（嘉實通信/光通信）專屬**：聯接NAV今日{_act:+.2f}%、"
                    f"5日資金{_flow019:+.2f}億、live光通信板塊{_telecom_ret*100:+.2f}%若可得"
                    f"→ **{_warn}**。廣義翻紅≠個股/板塊翻紅，勿被指數紅包誤導（V22.87 資金閘）。")
    return out


def _build_attribution_narrative(att_cache):
    """[V22.92] 每個 ETF 都給敘事歸因(不只高殘差)。
    入參 att_cache: {code: (name, actual_ret, att_dict)}。
    高殘差(alert): 詳細 breakdown + 追查路徑。
    低殘差: 精簡 1-liner（主導因子 / 正常歸因）。"""
    out = ["\n### 📝 敘事歸因 — 每個標的為什麼漲跌\n"]
    # [V24.0 修② 治本] 殘差原本用場外聯接NAV當 actual(T+1結算→收盤時是T-1滯後、封頂±0.07%),
    #   → 殘差全成 ±0.0x 噪音、系統誤判「全可解釋」,最大的未知因子(板塊崩跌)反而看不見。
    #   此處補「live 板塊真實殘差」:用該檔已驗證的場內 proxy 即時報酬當真實基準。
    _real_rows = []
    try:
        for _c, (_nm, _act, _att) in att_cache.items():
            _px = (TARGET_ETFS.get(_c) or {}).get("proxy")
            if not _px or (TARGET_ETFS.get(_c) or {}).get("market") == "台股":
                continue
            _lr = DirectDataFetcher.get_sina_proxy_ret(_px)
            if _lr is None:
                continue
            _lr_pct = _lr * 100.0
            _resid = _lr_pct - (_act or 0.0)
            if abs(_resid) >= 2.0:
                _real_rows.append((_nm, _lr_pct, _act or 0.0, _resid))
    except Exception as _e:
        _log_err(_e, "_build_attribution_narrative:L2974")  # [V24.16] 原為靜默 pass
        pass
    if _real_rows:
        _real_rows.sort(key=lambda x: abs(x[3]), reverse=True)
        out.append("#### 🚨 真實殘差（live 板塊 vs 場外NAV）— 這才是今天真正發生的事\n")
        out.append("| 標的 | live板塊(真實) | 場外NAV(滯後) | 真實殘差 |")
        out.append("|---|---|---|---|")
        for _nm, _lr_pct, _a, _rd in _real_rows[:8]:
            out.append(f"| {_nm} | {_lr_pct:+.2f}% | {_a:+.2f}% | **{_rd:+.2f}pp** |")
        out.append("> ★下表的「實際」是場外聯接NAV(T+1結算·滯後),殘差會被壓成噪音;"
                   "上表才是板塊真實跌幅,待 T+1 NAV 結算後複核。\n")

    # --- 高殘差（alert）先出，詳細版 ---
    high_residuals = []
    normal_items = []
    for _c, (_nm, _act, _att) in att_cache.items():
        _cc = _att["contributions"]
        if _att.get("alert"):
            high_residuals.append((_c, _nm, _act, _att, _cc))
        else:
            normal_items.append((_c, _nm, _act, _att, _cc))

    if high_residuals:
        out.append("#### ⚠️ 高殘差標的（已知因子解釋不了，需追查未知）\n")
        for _c, _nm, _act, _att, _cc in high_residuals:
            _excl = []
            if abs(_cc['🌎 美股傳導']) < 0.05: _excl.append("美股傳導可忽略")
            if abs(_cc['💰 主力資金']) < 0.05: _excl.append("主力無明顯方向")
            if abs(_cc['📉 技術回歸']) < 0.05: _excl.append("技術無均值回歸")
            if abs(_cc['📰 產業新聞']) < 0.05: _excl.append("新聞因子中性")
            _excl_s = "；".join(_excl) if _excl else "（無因子可排除,未知更純）"
            # 找最大絕對值因子當「主導因子」
            _factors = sorted(_cc.items(), key=lambda x: abs(x[1]), reverse=True)
            _dom = _factors[0] if _factors else ("?", 0)
            out.append(
                f"- **{_nm}** 實際 {_act:+.2f}%，已知因子合計僅解釋 "
                f"**{_att['explained']:+.2f}pp**（美股{_cc['🌎 美股傳導']:+.2f}／"
                f"主力{_cc['💰 主力資金']:+.2f}／技術{_cc['📉 技術回歸']:+.2f}／"
                f"新聞{_cc['📰 產業新聞']:+.2f}），"
                f"**殘差 {_att['residual']:+.2f}pp 無法由已知因子解釋** → "
                f"推測為未捕捉的（政策托底／大單／情緒／跨市場傳導／聯接基金申贖與跟蹤誤差）。"
                f"已排除：{_excl_s}。")

    # --- 正常殘差（全量精簡版）---
    if normal_items:
        out.append(f"\n#### 📊 其他 {len(normal_items)} 隻標的（因子可解釋，無異常殘差）\n")
        for _c, _nm, _act, _att, _cc in normal_items:
            _factors = sorted(_cc.items(), key=lambda x: abs(x[1]), reverse=True)
            _dom_name, _dom_val = _factors[0] if _factors else ("?", 0)
            _dir = "🟢漲" if _act > 0 else "🔴跌" if _act < 0 else "⚪平"
            if abs(_att['residual']) < 0.3:
                _res_note = "因子解釋充分"
            elif abs(_att['residual']) < 1.0:
                _res_note = f"小殘差{_att['residual']:+.2f}pp( minor)"
            else:
                _res_note = f"殘差{_att['residual']:+.2f}pp"
            out.append(
                f"- **{_nm}** {_dir}{abs(_act):.2f}% → "
                f"{_dom_name}{_dom_val:+.2f}pp 主導 / "
                f" {_res_note}")

    # --- 追查路徑（僅當有高殘差時才顯示）---
    if high_residuals:
        out.append("\n### 🔎 殘差追查路徑(下次盤前/盤中具體執行)\n")
        out.append("- 查 `news.csv` 同 scope 當日新聞(利多/利空關鍵詞,含盤後突發)")
        out.append("- 查 龍虎榜 / 大單淨買(判斷 買家收網 or 賣家絞殺)")
        out.append("- 查 同業/同板塊異動(個股特例 or 板塊共性)")
        out.append("- 查 政策面/監管動向(國家隊/產業政策/關稅)")
        out.append("- 聯接基金特例:申贖盤 + 跟蹤誤差會放大殘差,需拆 NAV vs 市價")
    return out


# [V22.74] 新聞掃描審計計數(每輪 reset;不影響分數,只供靜默失效偵測)
_NEWS_AUDIT = {"scored": 0, "chars": 0, "hits": 0}
def reset_news_audit():
    _NEWS_AUDIT.update(scored=0, chars=0, hits=0)

def compute_news_score(news_items, style=None):
    """[V22.30] 產業新聞 → 量化因子(使用者:「新聞是公式的一部分,有占比」)
    回傳 (score∈[-1,1], reason)。★機構做法:有 alpha 的是【意外程度】,
    不是新聞本身——已被 price in 的利多不再是利多。故只計【當日新增】且【高影響力】。
    ⚠️ 權重未經真數據校準(E-LOG #258),observe 期僅顯示。"""
    if not news_items:
        return (0.0, "無重大新聞")
    # [V22.68 治簡繁不匹配] ★關鍵詞表原為【繁體】,但東財公告/新聞來源回傳【簡體】
    #   → "中标" 永遠比不中 "中標" → 公告掃描會天天回「無高影響公告」而且【不報錯】。
    #   典型靜默失效(同 _direct_session 族)。人工 news.csv 是繁體、自動源是簡體,兩種都要收。
    # [V22.83 治「消息面盲區」] 原詞集只收【事件型】關鍵詞(漲價/減產/制裁…),
    #   東財即時頭條多為【方向型】(走高/大跌/震盪走低)→ 全漏抓→ 分數永遠0→ 系統盲。
    #   補方向型詞集(簡繁雙收),讓「產業新聞」真正進因子;仍屬 observe 期僅顯示(權重未校準)。
    POS = ("漲價", "涨价", "供不應求", "供不应求", "擴產", "扩产", "訂單", "订单",
           "突破", "國產替代", "国产替代", "禁止出口", "超預期", "超预期",
           "上調", "上调", "利好", "中標", "中标", "量產", "量产",
           "業績預增", "业绩预增", "預增", "预增", "增持", "回購", "回购",
           "走高", "大漲", "大涨", "上漲", "上涨", "反彈", "反弹", "創新高", "创新高",
           "走強", "走强", "獲批", "获批", "簽約", "签约", "需求旺盛", "滿產", "满产",
           "開盤大漲", "开盘大涨", "集採中標", "集采中标")
    NEG = ("下調", "下调", "減產", "减产", "跌價", "跌价", "產能過剩", "产能过剩",
           "制裁", "限制", "低於預期", "低于预期", "虧損", "亏损",
           "解禁", "減持", "减持", "風險提示", "风险提示",
           "業績預減", "业绩预减", "預減", "预减", "問詢函", "问询函",
           "走低", "大跌", "下挫", "重挫", "暴跌", "閃崩", "闪崩", "利空", "看空",
           "承壓", "承压", "回調", "回调", "殺跌", "杀跌", "重挫", "下探",
           "開盤大跌", "开盘大跌", "跳水", "重創", "重创", "破位", "走弱", "走軟", "走软")
    pos = neg = 0
    hits = []
    for it in (news_items or [])[:20]:
        t = str(it if isinstance(it, str) else it.get("title", ""))
        _NEWS_AUDIT["scored"] += 1; _NEWS_AUDIT["chars"] += len(t)   # [V22.74 審計計數,不影響分數]
        for k in POS:
            if k in t:
                pos += 1; hits.append(f"+{k}"); _NEWS_AUDIT["hits"] += 1; break
        else:
            for k in NEG:
                if k in t:
                    neg += 1; hits.append(f"-{k}"); _NEWS_AUDIT["hits"] += 1; break
    if pos == neg == 0:
        return (0.0, "新聞中性")
    score = max(-1.0, min(1.0, (pos - neg) / max(3, pos + neg)))
    return (round(score, 2), " ".join(hits[:3]) or "新聞中性")

# ── [V22.74] 新聞計分防靜默失效三閘(比對法/詞集一字不改,分數零漂移)──
_NEWS_SELFTEST_PAIRS = [
    ("公司拟回购股份", "公司擬回購股份", "回購"),
    ("股东拟大幅减持", "股東擬大幅減持", "減持"),
    ("公司收到监管问询函", "公司收到監管問詢函", "問詢函"),
    ("公司发布业绩预减公告", "公司發佈業績預減公告", "業績預減"),
    ("中标国家电网订单", "中標國家電網訂單", "中標訂單"),
]
def news_score_selftest():
    """閘①(啟動/preflight):簡繁同詞必同分且非零。原坑(繁體單列)在此測必 FAIL。回 (ok, fails)。"""
    fails = []
    for _s, _t, _lb in _NEWS_SELFTEST_PAIRS:
        _ss = compute_news_score([{"title": _s}])[0]
        _st = compute_news_score([{"title": _t}])[0]
        if _ss != _st or _ss == 0:
            fails.append(f"{_lb}: 簡={_ss} 繁={_st}")
    return (len(fails) == 0, fails)

def record_news_scan(conn, date_str):
    """閘③-a:落庫當日掃描(僅 scored>0);同日多報取 max(任一報有命中→該日非0)。"""
    _a = _NEWS_AUDIT
    if _a["scored"] <= 0:
        return
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS news_scan_daily("
                     "date TEXT PRIMARY KEY, scored INT, hits INT, ts TEXT)")
        _r = conn.execute("SELECT scored, hits FROM news_scan_daily WHERE date=?",
                          (date_str,)).fetchone()
        _sc, _ht = _a["scored"], _a["hits"]
        if _r:
            _sc = max(_sc, _r[0] or 0); _ht = max(_ht, _r[1] or 0)
        conn.execute("INSERT OR REPLACE INTO news_scan_daily VALUES(?,?,?,?)",
                     (date_str, _sc, _ht, datetime.datetime.now().isoformat(timespec="seconds")))
    except Exception as _e:
        _log_err(_e, "record_news_scan")

def check_news_silent_streak(conn, k=3, min_scored=3):
    """閘③-b:最近 k 個【掃到≥min_scored 則】的日子皆 0 命中 → (streak, dates),否則 (0,[])。"""
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS news_scan_daily("
                     "date TEXT PRIMARY KEY, scored INT, hits INT, ts TEXT)")
        _rows = conn.execute("SELECT date, scored, hits FROM news_scan_daily "
                             "WHERE scored >= ? ORDER BY date DESC LIMIT ?",
                             (min_scored, k)).fetchall()
        if len(_rows) < k:
            return (0, [])
        if all((_r[2] or 0) == 0 for _r in _rows):
            return (len(_rows), [_r[0] for _r in _rows])
        return (0, [])
    except Exception as _e:
        _log_err(_e, "check_news_silent_streak")
        return (0, [])

def news_audit_report_line(date_str=None):
    """閘②/③:報告尾端一行——記錄今日掃描 + 回覆蓋率提示(⚠️連續K日 / ℹ️單日 / '')。
    自帶連線,自我隔離;優先級 ⚠️連續 > ℹ️單日 > 空。"""
    try:
        _ds = date_str or _now_utc8().strftime("%Y-%m-%d")
        with sqlite3.connect(LOG_DB, timeout=10) as _c:
            record_news_scan(_c, _ds)
            _streak, _dates = check_news_silent_streak(_c, k=3)
        if _streak >= 3:
            return (f"\n\n⚠️ **新聞掃描疑靜默失效**:連續 {_streak} 個掃描日"
                    f"({_dates[-1]}~{_dates[0]})皆【0 命中】→ 疑來源格式/編碼變動或管道斷裂,"
                    f"請查來源(勿當『無重大新聞』採用)。\n")
        _a = _NEWS_AUDIT
        if _a["scored"] >= 5 and _a["hits"] == 0:
            return (f"\n\nℹ️ 新聞掃描 {_a['scored']} 則 0 命中(單日,可能安靜日;"
                    f"連續多日 0 命中才是確定訊號)。\n")
        return ""
    except Exception as _e:
        _log_err(_e, "news_audit_report_line")
        return ""

# 啟動自測(loud-if-fail,非致命):抓簡繁匹配回歸——原坑若重現,此處會大聲報
try:
    _NS_OK, _NS_FAILS = news_score_selftest()
    if not _NS_OK:
        print(f"⚠️⚠️ [V22.74 新聞計分自測失敗] 簡繁匹配疑回歸:{_NS_FAILS}")
except Exception as _e:
    _log_err(_e, "news_audit_report_line:L3174")  # [V24.16] 原為靜默 pass
    pass

# ═══════════════════════════════════════════════════════════════════
# [V22.68] ★公告自動掃描 —— 情報層第三條線
#   7/23 最刺痛的事實:中國西電【7/22 晚間】就公告中標 41.29 億(占 2025 營收 17.38%),
#   平高電氣公告 18.18 億 → 隔天電網設備 29 股漲停、主力淨流入 39 億。
#   **消息前一晚就公開了,而系統當晚跑的夜間快報只看美股。**
#
#   ★端點與坑位【全部來自 WB 2026-07-24 實測】,非憑記憶(㉒):
#     可用:https://np-anotice-stock.eastmoney.com/api/security/ann
#           ?sr=-1&page_size=N&page_index=1&client_source=web
#     坑①:帶 ann_type=0 會把結果濾成 0 筆 → **不可帶 ann_type**
#     坑②:個股參數 stock_list=601179.SH 實測回 0 筆 → **必須抓全量流 + 客戶端過濾**
#     坑③:樣本中 display_time=2026-01-07 但 notice_date=2026-12-26 互相矛盾
#           → **必須用 sort_date 真排序,並驗證取到的是近期公告**,否則情報是陳舊的
#   回傳結構:data.list[] 每筆含 codes[]{stock_code,short_name}、title、sort_date
# ═══════════════════════════════════════════════════════════════════
ANNOUNCE_URL = ("https://np-anotice-stock.eastmoney.com/api/security/ann"
                "?sr=-1&page_size={n}&page_index=1&client_source=web")
ANNOUNCE_MAX_AGE_DAYS = 3      # 超過此天數視為陳舊,不計入情報
# 持倉/觀察池 → 底層代表個股(用於在全量公告流中比對)。⚠️ 非窮舉,只放權重高者。
HOLDING_UNDERLYING = {
    "016185": ["600900", "601985", "600886"],          # 電力
    "025856": ["601179", "600312", "002498", "000533"], # 電網設備(中國西電/平高/漢纜/順鈉)
    "008888": ["688981", "002049", "603986"],           # 芯片
    "011613": ["688981", "688012", "688008"],           # 科創50
    "018177": ["688981", "688012", "688008"],
    "004432": ["600219", "601600", "603993"],           # 有色
    "020356": ["688146", "688268", "688556"],           # 半導體材料設備
    "013180": ["300750", "002240", "603399"],           # 電池
}


def fetch_announcements(n=100):
    """抓最新 n 筆全量公告流。回 [{codes,title,date}];失敗回 []。"""
    r = _em_throttled_get(ANNOUNCE_URL.format(n=int(n)), timeout=12,
                          tag="announce", retries=2)
    if r is None:
        return []
    try:
        lst = ((r.json() or {}).get("data") or {}).get("list") or []
    except Exception as _e:
        _log_err(_e, "fetch_announcements_parse")
        return []
    out = []
    for it in lst:
        try:
            codes = [str(c.get("stock_code") or "") for c in (it.get("codes") or [])]
            names = [str(c.get("short_name") or "") for c in (it.get("codes") or [])]
            # 坑③:sort_date 才是真排序鍵;display_time/notice_date 實測互相矛盾
            d = str(it.get("sort_date") or it.get("notice_date") or "")[:10]
            out.append({"codes": codes, "names": names,
                        "title": str(it.get("title") or it.get("title_ch") or ""),
                        "date": d})
        except Exception as _e:
            _log_err(_e, "fetch_announcements:L3229")  # [V24.16] 原為靜默 continue
            continue
    return out


def scan_announcements_for_holdings(anns, holdings=None):
    """在全量流中比對持倉底層個股。回 {etf_code: [(個股名, 標題, 日期, 分數)]}。
    ★依 WB 坑②:只能全量流+客戶端過濾,不能按個股拉。"""
    res = {}
    if not anns:
        return res
    today = _now_utc8().date()
    for etf, unders in HOLDING_UNDERLYING.items():
        if holdings and etf not in holdings:
            continue
        hits = []
        for a in anns:
            if not any(u in a["codes"] for u in unders):
                continue
            # 坑③:驗證是近期公告,陳舊者不計入情報
            try:
                age = (today - datetime.date.fromisoformat(a["date"])).days
            except Exception:
                age = None
            if age is None or age < 0 or age > ANNOUNCE_MAX_AGE_DAYS:
                continue
            sc, why = compute_news_score([{"title": a["title"], "scope": "ALL"}], "")
            if sc:
                nm = a["names"][a["codes"].index(next(u for u in unders if u in a["codes"]))] \
                     if a["names"] else ""
                hits.append((nm, a["title"][:44], a["date"], sc))
        if hits:
            hits.sort(key=lambda x: -abs(x[3]))
            res[etf] = hits[:4]
    return res


def format_announcements(hitmap, total_scanned=0):
    """公告情報區塊。無命中時誠實說明掃了幾筆、為何無命中。"""
    if not hitmap:
        return (f"> ⬜ **公告掃描**:已掃最新 {total_scanned} 筆全量公告,"
                f"持倉底層個股【近{ANNOUNCE_MAX_AGE_DAYS}日無高影響公告】"
                f"(或端點失敗)\n" if total_scanned else
                "> ⬜ **公告掃描**:端點失敗,本維度不可用(**非『無公告』**)\n")
    L = [f"\n### 📢 公告情報(掃描最新 {total_scanned} 筆全量流)\n",
         "> 依 WB 實測:個股參數回 0 筆,故採【全量流+客戶端過濾】;"
         f"僅計近 {ANNOUNCE_MAX_AGE_DAYS} 日\n\n",
         "| 持倉 | 個股 | 公告 | 日期 | 情報分 |\n|---|---|---|---|---|\n"]
    for etf, hits in hitmap.items():
        for nm, title, d, sc in hits:
            icon = "🟢" if sc > 0 else "🔴"
            L.append(f"| {etf} | {nm} | {title} | {d} | {icon} {sc:+.2f} |\n")
    L.append("> ⚠️ 公告為【個股層級】,對 ETF 的影響取決於權重;"
             "本區塊為**情報提示**,未直接進 EV 公式\n")
    return "".join(L)


NEWS_WINDOW_DAYS = 3   # [V22.64] 情報有效視窗(日);當日1.0,每過1日×0.6衰減

def _news_diag():
    """[V24.18] 新聞因子=0 的【根因診斷】——分辨三種完全不同的情況。

    ★使用者 2026-08-07:「依舊沒有新聞,說過很多次了依舊沒改善」。
      查明根因:load_news_csv 在 news.csv 缺失時會 importlib.import_module("news_fetcher")
      自動補抓,但 **news_fetcher 是外部模組;本機若無這支檔案,import 直接失敗**
      → except 優雅降級 → 報告只寫「無今日新聞」。
      使用者看到的是「今天沒新聞」,實際是「抓取器根本不存在,新聞永遠不會有」。
      **這是靜默失效偽裝成沒資料**——與 _direct_session 同類的問題。
    """
    import importlib.util as _iu
    _dir = os.path.dirname(os.path.abspath(__file__))
    _csv = os.path.join(_dir, "news.csv")
    try:
        _has_mod = _iu.find_spec("news_fetcher") is not None
    except Exception:
        _has_mod = False
    if not _has_mod and not os.path.exists(_csv):
        return ("> ❌ **新聞因子失效,不是「今天沒新聞」**:`news_fetcher.py` 不存在於代碼目錄,"
                "且 `news.csv` 也不存在 → 自動補抓無法執行,**新聞因子將永遠=0**。\n"
                "> **解法二選一**:①放一支 `news_fetcher.py`(需含 `ensure_news_csv()`)到代碼目錄;"
                "②手動建 `news.csv`,格式 `date,scope,title`"
                "(例:`2026-08-07,光通信,中際旭創獲大單`)。\n")
    if not _has_mod:
        return ("> ⚠️ `news_fetcher.py` 不存在 → 無法自動補抓;僅能讀既有 `news.csv`,"
                "其中無今日資料 → 因子=0。請補當日列或放入抓取模組。\n")
    if not os.path.exists(_csv):
        return ("> ⚠️ `news.csv` 不存在且自動補抓未產出檔案 → 因子=0。"
                "請以 `ETF_VERBOSE=1` 重跑看 news_fetcher 的錯誤。\n")
    return ("> ⚠️ `news.csv` 存在但無今日日期的列 → 因子=0"
            "(**這才是真的「今天沒收到新聞」**)。格式:`date,scope,title`。\n")



def load_news_csv():
    """[V22.33.5·治使用者原則①] 讀 news.csv(與代碼同目錄,WB/用戶策展,仿 events.csv)→ 產業新聞 news_items。
       格式: date,scope,title   (scope=style名或 ALL;title=新聞標題,含關鍵詞如「漲價/減產/國產替代」)
       機構作法:只計【當日新增】高影響新聞(已 price in 的不算)。回 [{"title":.., "scope":..}]。缺檔/空→[]。
       [V22.83 治「消息面盲區」] 若 news.csv 缺失或今日尚未抓取,自動呼叫 news_fetcher 補抓
       (東財產業新聞→寫入 news.csv)。失敗優雅降級(留空,系統照舊跑)。每日僅自動抓一次(見 ensure_news_csv)。"""
    import csv
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "news.csv")
    today = _now_utc8().date().isoformat()
    # ── [V22.83] 自動補抓:缺失或今日未抓→觸發 news_fetcher(優雅) ──
    try:
        _have_today = False
        if os.path.exists(path):
            with open(path, encoding="utf-8-sig") as _f0:
                for _r0 in csv.DictReader(_f0):
                    if (_r0.get("date") or "")[:10] == today:
                        _have_today = True
                        break
        if not os.path.exists(path) or not _have_today:
            import importlib
            try:
                _nf = importlib.import_module("news_fetcher")
                _nf.ensure_news_csv()
            except Exception as _ne:
                _log_err(_ne, "load_news_csv_auto_fetch")
    except Exception as _e:
        _log_err(_e, "load_news_csv_prefetch")
    items = []
    try:
        if os.path.exists(path):
            with open(path, encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    row = {k.strip(): (v.strip() if v else "") for k, v in row.items()}
                    # [V22.64 治WB指出的限制] 原本只載 date==今日 → 情報種子【隔日即失效】。
                    #   但「只計當日新增(已price in的不算)」的原則是對的,全載會讓舊聞永遠加分。
                    #   折衷:近 NEWS_WINDOW_DAYS 日內都載,權重【隨天數衰減】(當日1.0,每過1日×0.6)。
                    #   → 結構性催化(如十五五規劃)不會隔天歸零,也不會永遠滿分。
                    try:
                        _age = (_now_utc8().date()
                                - datetime.date.fromisoformat(row.get("date", "")[:10])).days
                    except Exception:
                        _age = None
                    if _age is not None and 0 <= _age <= NEWS_WINDOW_DAYS and row.get("title"):
                        items.append({"title": row["title"],
                                      "scope": row.get("scope", "ALL"),
                                      "age_days": _age,
                                      "decay": round(0.6 ** _age, 3)})
    except Exception as _e:
        _log_err(_e, "load_news_csv")
    return items

def _sector_kw_map():
    """[V24.29] 取 HOLDING_SECTOR_KEYWORDS(定義在本函式之後,故延遲取用)。"""
    return globals().get("HOLDING_SECTOR_KEYWORDS", {}) or {}


def build_news_score_map(news_items=None):
    """[V22.33.5·治原則①「產業鏈新聞四報都要有、是公式一部分有占比」] 逐檔算產業新聞因子。
       回 {code:(score,reason)}。四報統一呼叫此→新聞才『四報都有、進 EV/T+1 公式(占比15%)』。
       scope 過濾:ALL 或符合該檔 style 的新聞才計入(無相符則退回全部)。"""
    if news_items is None:
        news_items = load_news_csv()
    m = {}
    for _code, _info in TARGET_ETFS.items():
        _style = _info.get("style", "")
        # [V24.29] ★根因(使用者 2026-08-10 收盤報:17 檔全部 +0.67 +反弹 +量产):
        #   原式比對的 `_style` 是 Tech/Defensive/Cyclical 這類【風格代碼】,
        #   而 news.csv 的 scope 是【中文板塊名】(光通信/半導體/黃金…) → 永遠對不上,
        #   於是每檔都觸發尾巴的 `or news_items` **退回全部新聞** → 17 檔拿到同一個分數。
        #   ★更糟的是這個 fallback 把「沒有相關新聞」偽裝成「有新聞且分數相同」:
        #     一個 17 檔都一樣的因子占 EV 權重 15%,不但零區分度,還【稀釋了真正有區分度的因子】。
        #   修法:①接上系統既有的 HOLDING_SECTOR_KEYWORDS(代碼→中文板塊)與 ANCHOR_MAP(錨股名)
        #         ②移除 `or news_items`,無相符就給 0 並明說,不再拿全市場新聞冒充。
        _kw = list(_sector_kw_map().get(_code, [])) + list(ANCHOR_MAP.get(_code, []))
        def _hit(_it):
            _sp = str(_it.get("scope", "ALL") or "ALL")
            if _sp == "ALL":
                return True                      # 全市場新聞:所有檔都吃,合理
            if _sp == _style:
                return True
            if any(k and (k in _sp or _sp in k) for k in _kw):
                return True
            _ti = str(_it.get("title", "") or "")
            return any(k and k in _ti for k in _kw)   # 標題直接命中錨股/板塊名
        _rel = [it for it in news_items if _hit(it)]
        if not _rel:
            m[_code] = (0.0, "無本板塊今日新聞")
            continue
        _sc, _rs = compute_news_score(_rel, _style)
        # [V22.66 治WB指出的「有算未用」] ★V22.64 算了 decay(0.6^age)卻【沒人讀】。
        #   設計意圖是「十五五規劃在 D+2 應打 0.36 而非 1.0」,但打分函式只看 title 關鍵詞
        #   → 舊聞與當日新聞同權。此處按【相關新聞的最大 decay】折算,並在理由標明天數。
        try:
            _dc = max([float(it.get("decay", 1.0)) for it in _rel] or [1.0])
            _ag = min([int(it.get("age_days", 0)) for it in _rel] or [0])
            if _dc < 1.0 and _sc:
                _sc = round(_sc * _dc, 3)
                _rs = f"{_rs}(D+{_ag}衰減×{_dc})"
        except Exception as _e:
            _log_err(_e, "build_news_score_map:L3359")  # [V24.16] 原為靜默 pass
            pass
        m[_code] = (_sc, _rs)
    return m


# ═══════════════════════════════════════════════════════════════════
# [V22.34] PE 估值分位子系統 — DYNAMIC_CAP 第⑤條的【唯一解鎖鍵】
#   背景:突破區(40-50%)五條件的⑤「估值分位<0.80」因系統無 PE 欄位 → val_pct 恆 None
#         → 結構性擋下 → Tech 等效 40% 硬上限(不是設計選擇,是資料缺口)。
#   ★鐵律㉓:估值分位【必須標窗口】——同一標的 3年=96% / 5年=76% / 10年=38%(019072 實證),
#     挑窗口就能挑出想要的結論。故本表【強制存三個窗口 + 明示採用哪個】,預設採最嚴(最短窗口=最保守)。
#   沙盒無法連東財(IP被擋)→ fetcher 寫好由 WB 本機跑;無資料時系統照舊保守擋下(誠實,不假放行)。
# ═══════════════════════════════════════════════════════════════════
PE_PCT_WINDOW_DEFAULT = "3y"     # 預設採用窗口(最短=最保守;可改 5y/10y)
PE_PCT_MAX_AGE_DAYS   = 7        # 分位資料超過幾天視為過期(不採用→保守擋下)

def save_pe_percentile(conn, code, date_str, pe_ttm=None,
                       pct_3y=None, pct_5y=None, pct_10y=None, source=""):
    """落庫 PE 估值分位(三窗口全存,治『挑窗口挑結論』)。"""
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS pe_percentile(
            code TEXT, date TEXT, pe_ttm REAL,
            pct_3y REAL, pct_5y REAL, pct_10y REAL,
            source TEXT, ts TEXT, PRIMARY KEY(code, date))""")
        conn.execute("INSERT OR REPLACE INTO pe_percentile VALUES(?,?,?,?,?,?,?,?)",
                     (code, date_str, pe_ttm, pct_3y, pct_5y, pct_10y, source,
                      datetime.datetime.now().isoformat(timespec="seconds")))
        return True
    except Exception as _e:
        _log_err(_e, f"save_pe_percentile_{code}")
        return False

def load_pe_percentile(code, window=None, max_age_days=None):
    """讀最新 PE 分位。回 (pct|None, meta:dict)。
    ★無資料/過期 → 回 (None, meta) → 第⑤條保守擋下(誠實,不假放行)。"""
    window = window or PE_PCT_WINDOW_DEFAULT
    max_age = PE_PCT_MAX_AGE_DAYS if max_age_days is None else max_age_days
    meta = {"code": code, "window": window, "status": "no_data", "date": None, "pe_ttm": None}
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS pe_percentile(
                code TEXT, date TEXT, pe_ttm REAL,
                pct_3y REAL, pct_5y REAL, pct_10y REAL,
                source TEXT, ts TEXT, PRIMARY KEY(code, date))""")
            row = conn.execute(
                "SELECT date, pe_ttm, pct_3y, pct_5y, pct_10y, source FROM pe_percentile "
                "WHERE code=? ORDER BY date DESC LIMIT 1", (code,)).fetchone()
        if not row:
            return None, meta
        _d, _pe, _p3, _p5, _p10 = row[0], row[1], row[2], row[3], row[4]
        meta.update({"date": _d, "pe_ttm": _pe, "source": row[5],
                     "pct_3y": _p3, "pct_5y": _p5, "pct_10y": _p10})
        try:
            age = (_now_utc8().date() - datetime.date.fromisoformat(_d)).days
        except Exception:
            age = 999
        meta["age_days"] = age
        if age > max_age:
            meta["status"] = f"stale({age}天>{max_age}天)"
            return None, meta          # 過期不採用 → 保守擋下
        pct = {"3y": _p3, "5y": _p5, "10y": _p10}.get(window)
        if pct is None:
            meta["status"] = f"窗口{window}無值"
            return None, meta
        meta["status"] = "ok"
        return float(pct), meta
    except Exception as _e:
        _log_err(_e, f"load_pe_percentile_{code}")
        meta["status"] = "error"
        return None, meta

def fetch_pe_percentile_eastmoney(proxy_secid, timeout=8):
    """抓 proxy ETF 當前 PE-TTM(東財 push2 stock/get f162)。
    ★設計(V22.71): 投資組合是「基金/ETF 联接」,東財【無基金 PE 分位端點】
      (RPT_VALUATIONSTATUS 經實測為個股專用,指數 000300.SH 回空;push2 對基金代碼限流)。
      正解=用基金 proxy 欄位指向的【真實場內 ETF】(如 008888→sz159995 芯片ETF),
      其 PE-TTM 在 push2 可得 → 分位由本地 pe_history 滾動計算(見 compute_pe_percentile_local)。
    proxy_secid 例:'0.159995'(sz159995)。回 dict(pe_ttm) 或 None(限流/無資料→誠實回 None)。
    ⚠️ 沙盒當下 push2 被東財限流(RemoteDisconnected),但用戶 live 端 _em_throttled_get 含重試可通。"""
    try:
        if not proxy_secid or "." not in proxy_secid:
            return None
        url = ("https://push2.eastmoney.com/api/qt/stock/get"
               f"?secid={proxy_secid}&fields=f57,f58,f162,f167&_={int(time.time()*1000)}")
        r = _em_throttled_get(url, timeout=timeout, tag="pe_pct")
        if r is None:
            return None
        d = (r.json() or {}).get("data") or {}
        if not d:
            return None
        pe = d.get("f162")
        try:
            pe = float(pe) if pe is not None else None
        except Exception:
            pe = None
        return {"pe_ttm": pe, "source": f"eastmoney/push2(f162);secid={proxy_secid}"}
    except Exception as _e:
        _log_err(_e, "fetch_pe_percentile_eastmoney")
        return None


# ── [V22.71] PE 歷史滾動分位(本地累積,不造假多年分位) ──────────
CROWD_MIN_DAYS_PE = 20   # PE 分位最低樣本日數(㉘:不足即不生分位→保守擋下)

def update_pe_history(conn, code, date_str, pe_ttm):
    """每日把 proxy ETF 當前 PE-TTM 追加進 pe_history,供滾動分位累積。"""
    try:
        if pe_ttm is None:
            return False
        conn.execute("""CREATE TABLE IF NOT EXISTS pe_history(
            code TEXT, date TEXT, pe_ttm REAL, ts TEXT, PRIMARY KEY(code, date))""")
        conn.execute("INSERT OR REPLACE INTO pe_history VALUES(?,?,?,?)",
                     (code, date_str, float(pe_ttm),
                      datetime.datetime.now().isoformat(timespec="seconds")))
        return True
    except Exception as _e:
        _log_err(_e, f"update_pe_history_{code}")
        return False

def compute_pe_percentile_local(conn, code):
    """從 pe_history 算『當前 PE 在滾動歷史中的分位』(0~1)。
    ★語義:這是【滾動窗口分位】(樣本日數=歷史累積),非東財 3y/5y/10y 真·多年分位——
      因東財無基金 PE 分位端點(已實測)。樣本 < CROWD_MIN_DAYS_PE → 回 None(誠實,
      不假放行,維持 40% 上限)。回 (pct|None, n_samples)。"""
    try:
        rows = conn.execute(
            "SELECT pe_ttm FROM pe_history WHERE code=? ORDER BY date ASC", (code,)).fetchall()
        if len(rows) < CROWD_MIN_DAYS_PE:
            return None, len(rows)
        vals = [float(r[0]) for r in rows if r[0] is not None]
        if len(vals) < CROWD_MIN_DAYS_PE:
            return None, len(vals)
        cur = vals[-1]
        below = sum(1 for v in vals if v <= cur)   # 分位 = 歷史中 ≤ 當前PE 的比例(含自身)
        pct = below / len(vals)
        return pct, len(vals)
    except Exception as _e:
        _log_err(_e, f"compute_pe_percentile_local_{code}")
        return None, 0

def refresh_pe_percentile(code, info, date_str=None):
    """[V22.71 每日呼叫] 抓 proxy ETF PE → 累積歷史 → 算滾動分位 → 落庫 pe_percentile。
    回 (pct|None, meta)。失敗靜默回 None,不拖累主報告。"""
    meta = {"code": code, "status": "skip"}
    try:
        date_str = date_str or _now_utc8().strftime("%Y-%m-%d")
        # proxy → secid(主 proxy 優先, proxy2 備援)
        secid = _proxy_to_secid(info.get("proxy")) or _proxy_to_secid(info.get("proxy2"))
        if not secid:
            meta["status"] = "no_proxy_secid"
            return None, meta
        res = fetch_pe_percentile_eastmoney(secid)
        pe = res.get("pe_ttm") if isinstance(res, dict) else None
        if pe is None:
            meta["status"] = "fetch_none(throttled?)"
            return None, meta
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            update_pe_history(conn, code, date_str, pe)
            pct, n = compute_pe_percentile_local(conn, code)
        if pct is None:
            meta.update({"status": f"insufficient_samples({n}<{CROWD_MIN_DAYS_PE})",
                         "pe_ttm": pe, "n": n})
            with sqlite3.connect(LOG_DB, timeout=10) as conn:
                save_pe_percentile(conn, code, date_str, pe_ttm=pe,
                                   source="local_rolling(insufficient)")
            return None, meta
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            save_pe_percentile(conn, code, date_str, pe_ttm=pe,
                               pct_3y=pct, pct_5y=pct, pct_10y=pct,
                               source=f"local_rolling({n}d)")
        meta.update({"status": "ok", "pct": round(pct, 4), "pe_ttm": pe, "n": n,
                     "note": "滾動窗口分位(非東財多年分位)"})
        return pct, meta
    except Exception as _e:
        _log_err(_e, f"refresh_pe_percentile_{code}")
        meta["status"] = "error"
        return None, meta


# ── [V22.73] 真·多年 PE 分位(韭圈儿 funddb 底層指數估值) ──────────────
# 設計(V22.72 安全修正的接榫):V22.72 的 source-check 只擋 "local_rolling",
#   其他來源自動驅動⑤。本段用 funddb 多年指數 PE 序列算【真 3y/5y/10y 分位】,
#   存 source="funddb_index_3y" → 消費端(L10422 elif)自動驅動⑤,消費端零修改。
#
# ★ ETF→追蹤指數→funddb 名稱映射(最佳推測,待 live 用 akshare.index_value_name_funddb() 核對):
#   funddb 命名可能微差(如「中證芯片」vs「國證芯片」)。若名稱對不上 → akshare 回空/拋 →
#   本段優雅回 None → refresh 呼叫方 fallback 滾動分位(安全,不假成功)。故錯名=不動作,非假放行。
FUND_TO_FUNDBB_INDEX = {
    # ── Tech 桶(⑤ 只 gate Tech,優先對準) ──
    "008888": "国证芯片",          # proxy sz159995 芯片ETF→國證芯片(980017)
    "011613": "科创50",            # proxy sh588000→上證科創板50(000688)
    "018177": "科创50",            # 增強型,同底層
    "016008": "中证消费电子50",     # proxy sz159779→中證消費電子50
    "018135": "中证大数据产业",     # proxy sh515400→中證大數據產業
    "019141": "中证电信主题",       # proxy sh563010→中證電信主題
    "019072": "国证通信",          # WATCHLIST OptComm, proxy sz159695→國證通信
    # ── 其餘次要桶 ──
    "016185": "中证全指电力",       # Utility, proxy sz159611→中證全指電力
    "004432": "中证有色金属",       # Cyclical, proxy sh512400→有色金屬
    "007466": "中证红利",           # Dividend, proxy sh512890→中證紅利
}
# 跳過:000216(黃金·無PE)/006479+018967(納指QDII·非A股)/00982A.TW+00929.TW(台股,proxy=None)

# akshare 懶載入(避免每檔重 import;本沙盒 1.18.79 未導出 funddb 函式→回 None→安全)
_AK_FUNDBB_FN = None
_AK_FUNDBB_TRIED = False
def _get_ak_funddb_fn():
    global _AK_FUNDBB_FN, _AK_FUNDBB_TRIED
    if _AK_FUNDBB_TRIED:
        return _AK_FUNDBB_FN
    _AK_FUNDBB_TRIED = True
    try:
        import akshare as _ak
        _AK_FUNDBB_FN = getattr(_ak, "index_value_hist_funddb", None)
    except Exception:
        _AK_FUNDBB_FN = None
    return _AK_FUNDBB_FN

def fetch_index_pe_history_funddb(index_name, indicator="市盈率"):
    """抓底層指數【多年 PE 序列】(韭圈说 funddb 直連)。
    [V22.77 折入] akshare 1.18.79 已移除 index_value_hist_funddb(函式被拔,非版本落後)，
      故改直連 api.jiucaishuo.com + 複製混淆簽章(funddb_client.py,2026-07-27 實測 code:0)。
      funddb_client 負責:名稱別名(FUND_NAME_ALIAS,9 檔 ETF→gu_name,含 is_proxy 標註)、
      簽章、抓取、過濾哨兵,回 [(date_str, pe), ...] 升序(同本函式契約)。
    ★缺失/名稱錯/網路擋/簽章失效 → 客戶端回 None → 呼叫方 fallback 滾動分位(安全,不假成功)。
    回 [(date_str, pe), ...] 升序,或 None。"""
    try:
        import os as _os, sys as _sys
        _here = _os.path.dirname(_os.path.abspath(__file__))
        if _here not in _sys.path:
            _sys.path.insert(0, _here)
        import funddb_client as _fc
        return _fc.fetch_index_pe_history_funddb(index_name, indicator)
    except Exception as _e:
        _log_err(_e, "fetch_index_pe_history_funddb")
        return None

def fetch_index_pe_history_funddb_meta(index_name, indicator="市盈率"):
    """[V22.78] 同 fetch_index_pe_history_funddb,但回 (series|None, meta)。
    meta 含 is_proxy/gu_name/status → 供誠實區分「真底層指數 vs 同板塊代理」。"""
    try:
        import os as _os, sys as _sys
        _here = _os.path.dirname(_os.path.abspath(__file__))
        if _here not in _sys.path:
            _sys.path.insert(0, _here)
        import funddb_client as _fc
        return _fc.fetch_index_pe_history_funddb_meta(index_name, indicator)
    except Exception as _e:
        _log_err(_e, "fetch_index_pe_history_funddb_meta")
        return None, {"is_proxy": False, "status": "error"}

def _parse_pe_date(s):
    """寬鬆解析 funddb 日期(支援 2023-02-26 / 2023/02/26 / 20230226)。失敗回 None。"""
    for _f in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.datetime.strptime(str(s)[:10], _f).date()
        except Exception as _e:
            _log_err(_e, "_parse_pe_date:L3679")  # [V24.16] 原為靜默 continue
            continue
    return None

def compute_pe_pct_from_series(series, window_years=None):
    """series=[(date_str, pe), ...] 升序。回 (pct|None, current_pe, n_used)。
    pct = 當前 PE 在【window_years 內】歷史 PE 的分位(0~1,低=便宜→放行⑤,高=貴→擋⑤)。
    日期無法解析 → 退為用全序列(多年序列本身即長期分位)。樣本<20 → 回 None(誠實,不假放行)。"""
    if not series or len(series) < 2:
        return None, (series[-1][1] if series else None), len(series)
    _cur_d, _cur_pe = series[-1]
    _cd = _parse_pe_date(_cur_d)
    _win = []
    for _d, _pe in series:
        if window_years and _cd:
            _dd = _parse_pe_date(_d)
            if _dd is None:
                continue
            if (_cd - _dd).days > window_years * 365:
                continue
        _win.append(_pe)
    if not _win:
        _win = [p for _, p in series]   # 日期不可解析→用全序列
    if len(_win) < 20:
        return None, _cur_pe, len(_win)
    _below = sum(1 for v in _win if v <= _cur_pe)
    return _below / len(_win), _cur_pe, len(_win)

def refresh_pe_percentile_funddb(code, info):
    """[V22.73 每日呼叫·優先於滾動] 抓底層指數多年 PE → 算真 3y/5y/10y 分位 → 落庫。
    成功 → source="funddb_index_3y"(非 local_rolling)→ 消費端自動驅動⑤。
    失敗(無映射/取不到/樣本不足)→ 回 (None, meta)→ 呼叫方 fallback 滾動(安全)。
    回 (pct_3y|None, meta)。"""
    meta = {"code": code, "status": "skip"}
    try:
        _idx = FUND_TO_FUNDBB_INDEX.get(code)
        if not _idx:
            meta["status"] = "no_funddb_index_mapping"
            return None, meta
        _series, _fmeta = fetch_index_pe_history_funddb_meta(_idx)
        if not _series or len(_series) < 30:
            meta["status"] = "funddb_empty_or_short"
            return None, meta
        # [V22.78 治代理不透明] ★區分「真底層指數」vs「同板塊代理」:
        #   funddb 無精確底層時用同板塊近似指數,其分位仍是真·多年估值(非 rolling),
        #   故【仍驅動⑤】(比擋掉更有用),但存 funddb_proxy_3y 讓報告誠實標「代理近似」,
        #   不把近似當精確估值(同 V22.71 rolling 那次語義誠實原則)。
        _is_proxy = bool((_fmeta or {}).get("is_proxy"))
        _gu = (_fmeta or {}).get("gu_name")
        _src = "funddb_proxy_3y" if _is_proxy else "funddb_index_3y"
        _p3, _cur, _n3 = compute_pe_pct_from_series(_series, 3)
        _p5, _, _n5 = compute_pe_pct_from_series(_series, 5)
        _p10, _, _n10 = compute_pe_pct_from_series(_series, 10)
        if _p3 is None:
            meta["status"] = "insufficient_for_window"
            return None, meta
        _ds = _now_utc8().strftime("%Y-%m-%d")
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            save_pe_percentile(conn, code, _ds, pe_ttm=_cur,
                               pct_3y=_p3, pct_5y=_p5, pct_10y=_p10,
                               source=_src)
        meta.update({"status": "funddb_ok", "pct_3y": round(_p3, 4),
                     "pct_5y": round(_p5, 4), "pct_10y": round(_p10, 4),
                     "pe_ttm": _cur, "n": len(_series), "source": _src,
                     "is_proxy": _is_proxy, "gu_name": _gu})
        return _p3, meta
    except Exception as _e:
        _log_err(_e, f"refresh_pe_funddb_{code}")
        meta["status"] = "error"
        return None, meta




def save_watchlist_snapshot(results, date_str=None):
    """[V22.55] 觀察池每日落庫 —— 治「算完就丟,只看得到今天、看不到趨勢」。
    ★根因(2026-07-23 實證):daily_snapshot 裡觀察池紀錄停在 2026-07-15 且 rsi=0,
      代表 scan_watchlist 的結果【從未持久化】。後果:
        020356(材料/特氣)、025856(電網/AI電力)這些題材標的,每天算完即丟,
        永遠只有當日快照 → 無法看資金流方向(而 008888 正是靠五日 -11.8→+0.7 判定起飛)。
      使用者的核心方法是「起飛前抓住」,那靠的就是【趨勢】,不是單日值。
    寫入與持倉相同的 daily_snapshot 表,欄位對齊,故 analyze_entry_v2 等工具零修改即可讀。
    回 (寫入筆數, 訊息)。純寫入,失敗不拋例外。"""
    if not results:
        return 0, "觀察池無結果,未落庫"
    date_str = date_str or _now_utc8().strftime("%Y-%m-%d")
    n = 0
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS daily_snapshot(
                code TEXT, date TEXT, rsi REAL, z_score REAL, trend REAL,
                main_flow_yi REAL, flow5_yi REAL, fragility INTEGER,
                today_ret REAL, t1_pred REAL, t1_conf TEXT,
                t1_actual REAL, ts TEXT, PRIMARY KEY(code, date))""")
            for r in results:
                code = r.get("code")
                if not code:
                    continue
                _f5 = None
                _fi = r.get("flow5")
                if isinstance(_fi, dict):
                    _f5 = _fi.get("flow5_yi")
                # trend 走與持倉相同的夾擠(V22.38),避免又出現 -26 這種超界值
                _tr = _clamp_snapshot_trend(r.get("trend"), code)
                # [V22.56 修 V22.55 兩個 bug]
                #  ① 欄位名:V22.55 寫 r.get("fragility") —— scan_watchlist 【沒有這個鍵】,
                #     恆得 None。觀察池不算脆弱度,故改存 None 並在註解說明,不假裝有值。
                #     trend 則是【早就算好但沒帶出來】,已於 append 補上(見上)。
                #  ② 覆蓋:原用 INSERT OR REPLACE 無條件覆蓋 → 同日稍後一次跑若沒抓到 flow5,
                #     會把稍早成功的值【洗成 NULL】(2026-07-23 實證:WB 核證時見 +0.19,
                #     午報跑完變無資料)。改為 COALESCE(新值, 既有值):新值為 None 時保留舊值。
                conn.execute(
                    "INSERT OR REPLACE INTO daily_snapshot"
                    "(code,date,rsi,z_score,trend,main_flow_yi,flow5_yi,fragility,"
                    " today_ret,t1_pred,t1_conf,t1_actual,ts) "
                    "VALUES(?,?,"
                    " COALESCE(?,(SELECT rsi      FROM daily_snapshot WHERE code=? AND date=?)),"
                    " (SELECT z_score FROM daily_snapshot WHERE code=? AND date=?),"
                    " COALESCE(?,(SELECT trend    FROM daily_snapshot WHERE code=? AND date=?)),"
                    " (SELECT main_flow_yi FROM daily_snapshot WHERE code=? AND date=?),"
                    " COALESCE(?,(SELECT flow5_yi FROM daily_snapshot WHERE code=? AND date=?)),"
                    " (SELECT fragility FROM daily_snapshot WHERE code=? AND date=?),"
                    " COALESCE(?,(SELECT today_ret FROM daily_snapshot WHERE code=? AND date=?)),"
                    " COALESCE(?,(SELECT t1_pred  FROM daily_snapshot WHERE code=? AND date=?)),"
                    " COALESCE(?,(SELECT t1_conf  FROM daily_snapshot WHERE code=? AND date=?)),"
                    " (SELECT t1_actual FROM daily_snapshot WHERE code=? AND date=?),?)",
                    (code, date_str,
                     r.get("rsi"), code, date_str,
                     code, date_str,
                     _tr, code, date_str,
                     code, date_str,
                     _f5, code, date_str,
                     code, date_str,
                     r.get("ret_5d"), code, date_str,
                     r.get("pred_5d"), code, date_str,
                     r.get("entry_signal"), code, date_str,
                     code, date_str,
                     datetime.datetime.now().isoformat(timespec="seconds")))
                n += 1
        return n, f"✅ 觀察池已落庫 {n} 檔({date_str})"
    except Exception as _e:
        _log_err(_e, "save_watchlist_snapshot")
        return 0, f"⚠️ 觀察池落庫失敗(不影響報告): {type(_e).__name__}"


def save_daily_snapshot(conn, code, date_str, rsi, z_score, trend,
                        main_flow_yi, flow5_yi, fragility, today_ret,
                        t1_pred, t1_conf):
    """★落庫:沒有真實歷史數據,T+1/脆弱度就永遠只能靠估計值 = 造假溫床(E-LOG #258)。
    每日落庫,兩週後才有真數據可校準權重、可真回測。"""
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS daily_snapshot(
            code TEXT, date TEXT, rsi REAL, z_score REAL, trend REAL,
            main_flow_yi REAL, flow5_yi REAL, fragility INTEGER,
            today_ret REAL, t1_pred REAL, t1_conf TEXT,
            t1_actual REAL, ts TEXT, PRIMARY KEY(code, date))""")
        conn.execute("""INSERT OR REPLACE INTO daily_snapshot
            (code,date,rsi,z_score,trend,main_flow_yi,flow5_yi,fragility,
             today_ret,t1_pred,t1_conf,t1_actual,ts)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,
                   (SELECT t1_actual FROM daily_snapshot WHERE code=? AND date=?),?)""",
            (code, date_str, rsi, z_score, _clamp_snapshot_trend(trend, code), main_flow_yi, flow5_yi,
             fragility, today_ret, t1_pred, t1_conf, code, date_str,
             datetime.datetime.now().isoformat(timespec="seconds")))
    except Exception as _e:
        _log_err(_e, f"save_daily_snapshot_{code}")

def backfill_t1_actual(conn, code, date_str, actual_ret):
    """回填 T+1 實際值 → 這才能算真命中率(不是我估的)"""
    try:
        conn.execute("UPDATE daily_snapshot SET t1_actual=? WHERE code=? AND date=?",
                     (actual_ret, code, date_str))
    except Exception as _e:
        _log_err(_e, f"backfill_t1_{code}")


def _weekdays_between(d_start, d_end):
    """d_start 與 d_end(ISO 日期字串)之間【不含兩端】的週一至週五天數。"""
    a = datetime.date.fromisoformat(str(d_start)[:10])
    b = datetime.date.fromisoformat(str(d_end)[:10])
    return sum(1 for i in range(1, (b - a).days) if (a + datetime.timedelta(days=i)).weekday() < 5)


def _ensure_actual_src_col(conn):
    """舊資料庫(或測試用的最小資料庫)可能還沒有 predictions.actual_src。"""
    if "actual_src" not in {r[1] for r in conn.execute("PRAGMA table_info(predictions)")}:
        conn.execute("ALTER TABLE predictions ADD COLUMN actual_src TEXT")


def _apply_actual_by_semantic(conn, code, realized_date, ret, src="legacy"):
    """[V22.39] 按 report_type 語義分離回填 predictions.actual_ret（治回測阻塞點）。

    背景缺陷（V22.38 三處回填共犯）：backfill_yesterday/taiwan_today/a_stock_today
      的 UPDATE 都用 `WHERE code=? AND date(ts)=?`，不分 report_type，把單一
      same-day 實現收益寫進當天【所有】預測行。但 L407 權威定義：
        盤前/早/午 = T+0（預測當日收盤）；收盤報 = T+1（預測次日）。
      → 收盤報行拿到 same-day 收益 = 與自己的 T+1 預測【錯配】→ 汙染回測樣本。

    本函式把「某實現交易日 realized_date 的收益 ret」按語義正確落位：
      (a) T+0：盤前/早/午 且 date(ts)=realized_date → 直接配 ret。
      (b) T+1：緊鄰 realized_date 的前一個運行日的收盤報 —— 它當初預測的正是
          realized_date 的收益 —— 配 ret。原本取「最近一筆未回填」並宣稱容錯漏跑,
          實際上前一日漏跑時會把更早那天的預測配錯;現改為兩日之間有缺漏工作日就不配
          (週末可跨,長假與漏跑無法區分,一律留 NULL,寧缺勿錯)。
    空白 report_type（僅 2026-05-27 前歷史遺留，之後不再產生）不在此動，屬
      clean.db 歷史清洗線（見 E-LOG_211 待辦），避免 going-forward 邏輯碰歷史死資料。

    來源等級(src,見 returns_store):先寫入單一來源表 realized_returns;被同級或更高級的既有值
      擋下就整筆不動。predictions 已回填的行,只有新來源等級【更高】時才覆寫 —— 例如收盤時先以
      場內代理(proxy)回填,隔天官方 NAV(nav)公布後覆蓋;反過來代理值永遠不能蓋掉 NAV。
      (原本「已回填就不覆寫」,哪條路徑先跑就用哪個來源,A股幾乎全是代理值。)
    回傳 (n_t0, n_t1)。
    """
    n0 = n1 = 0
    try:
        if not returns_store.upsert(conn, code, realized_date, ret, src):
            return 0, 0
        _ensure_actual_src_col(conn)
        _rk = returns_store.rank(src)
        _upgradable = (f"(corrected=0 OR actual_ret IS NULL "
                       f"OR {returns_store.rank_sql('actual_src')} < {_rk})")
        n0 = conn.execute(f"""
            UPDATE predictions
            SET actual_ret=?, corrected=1, error=COALESCE(predicted_ret,0)-?, actual_src=?
            WHERE code=? AND date(ts)=?
              AND report_type IN ('盤前報','早報','午報')
              AND {_upgradable}
        """, (ret, ret, src, code, realized_date)).rowcount
        # 只配給「緊鄰的前一個運行日」的收盤報:原本取「最近一筆未回填」,一旦前一日漏跑
        #   就會把更早那天的預測(它預測的是更早的日子)配上 realized_date 的報酬 → 錯配。
        #   前一運行日 = 任一 code 有預測紀錄、早於 realized_date 的最大日期;兩者之間若隔著
        #   工作日(漏跑或長假無法區分)則不配,寧可留 NULL 也不錯配。
        prev = conn.execute("SELECT MAX(date(ts)) FROM predictions WHERE date(ts) < ?",
                            (realized_date,)).fetchone()[0]
        row = None
        if prev and _weekdays_between(prev, realized_date) == 0:
            row = conn.execute(f"""
                SELECT rowid FROM predictions
                WHERE code=? AND report_type='收盤報'
                  AND date(ts) = ?
                  AND {_upgradable}
                ORDER BY ts DESC LIMIT 1
            """, (code, prev)).fetchone()
        if row:
            n1 = conn.execute("""
                UPDATE predictions
                SET actual_ret=?, corrected=1, error=COALESCE(predicted_ret,0)-?, actual_src=?
                WHERE rowid=?
            """, (ret, ret, src, row[0])).rowcount
    except Exception as _e:
        _log_err(_e, f"apply_actual_semantic_{code}")
    return n0, n1


def save_morning_premise(conn, date_str, payload):
    """[V22.29] morning_premise 表 — 跨報因果鏈(L372 標「待做」,今日實作)
    收盤報寫入 T+1 前提 → 盤前報讀取 → 早報驗證。因果鏈的錨。"""
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS morning_premise(
            date TEXT PRIMARY KEY, payload TEXT, ts TEXT)""")
        conn.execute("INSERT OR REPLACE INTO morning_premise VALUES(?,?,?)",
                     (date_str, json.dumps(payload, ensure_ascii=False),
                      datetime.datetime.now().isoformat(timespec="seconds")))
    except Exception as _e:
        _log_err(_e, "save_morning_premise")

def load_morning_premise(conn, date_str):
    """盤前/早報讀取昨日收盤報寫的 T+1 前提"""
    try:
        r = conn.execute("SELECT payload FROM morning_premise WHERE date=?",
                         (date_str,)).fetchone()
        return json.loads(r[0]) if r and r[0] else None
    except Exception as _e:
        _log_err(_e, "load_morning_premise")
        return None

def build_premarket_forecast(premise, prem_date, ndx, sox, vix, news_items=None,
                             unavailable=(), max_rows=12):
    """[V22.30 設計,本版接線] 盤前報【今日盤勢預測】= 昨收 T+1 前提 + 隔夜美股傳導 + 產業新聞。
    單位:前提 t1_pred 為百分比數;ndx/sox 為小數報酬(0.02 = +2%)。
      QDII:NDX 全額傳導(真連動);A股:SOX×10%×0.3(條件 β≈0,E-LOG #170 弱連動)。
      新聞:compute_news_score ∈ [-1,1] × 0.8。
    原本這段寫在 main() 後段,盤前報在前段就 return,永遠不會執行,且引用了不存在的
    ndx_ret/sox_ret。改成獨立函式由盤前報流程呼叫。unavailable:本次取不到的隔夜指標(如 "NDX")。
    ⚠️ 權重未經真實數據校準(E-LOG #258),observe-only。"""
    if not premise:
        return ("\n## 🌅 今日盤勢預測\n\n"
                "> ⚠️ 無昨日收盤 premise(首次執行或昨日收盤報未跑)。\n"
                "> 因果鏈需昨日收盤報先寫入 `morning_premise`。\n\n")
    ndx, sox = (ndx or 0.0), (sox or 0.0)
    rows = []
    for code, pv in premise.items():
        if not isinstance(pv, dict):
            continue
        info = TARGET_ETFS.get(code, {})
        is_q = _is_qdii_etf(info)
        base = pv.get("t1_pred", 0) or 0
        us = ndx * 100 if is_q else sox * 100 * 0.10
        nsc, nrs = compute_news_score(news_items or [], info.get("style", ""))
        news_adj = round(nsc * 0.8, 2)
        upd = round(base + us * (1.0 if is_q else 0.3) + news_adj, 2)
        rows.append((info.get("name", code)[:10], base, us, news_adj, upd, pv.get("conf", "低"), nrs))
    rows.sort(key=lambda x: -abs(x[4]))
    na = [k for k in ("NDX", "SOX") if k in (unavailable or ())]
    na_line = (f"> ⚠️ **隔夜 {'/'.join(na)} 本次不可用**,以 0 代入 → 該部分傳導未計入,今日預測只反映昨收前提與新聞。\n"
               if na else "")
    out = (f"\n## 🌅 今日盤勢預測 — 昨收前提({prem_date}) × 隔夜美股\n\n"
           "> 📌 **四報因果鏈起點**:昨日收盤報寫入的 T+1 前提,"
           "此刻用**隔夜美股**更新 → 產出**今日預測** → 早報驗證(果因診斷)。\n"
           f"> 🌎 隔夜:SOX {sox*100:+.2f}% / NDX {ndx*100:+.2f}% / VIX {vix:.2f}\n"
           + na_line +
           "> ⚠️ **A股科技對美股弱連動**(條件β≈0, E-LOG #170)→ SOX 只給 10%×0.3 權重;"
           "**QDII 真連動** → NDX 全額傳導。\n"
           "> ⚠️ 校準期(observe-only),權重未經真數據驗證(E-LOG #258)。\n\n"
           "| ETF | 昨收前提 | 隔夜美股 | 📰產業新聞 | **今日預測** | 信心 |\n|---|---|---|---|---|---|\n")
    for nm, b, u, na_, up, cf, _nrs in rows[:max_rows]:
        ic = "🔴" if up < -0.8 else ("🟢" if up > 0.8 else "⚪")
        out += f"| {nm} | {b:+.2f}% | {u:+.2f}% | {na_:+.2f}% | {ic} **{up:+.2f}%** | {cf} |\n"
    out += ("\n> ➡️ **下一步**:早報(10:30)驗證實際開盤 vs 本預測 → "
            "分離「美股傳導(果)」vs「提前布局(因)」→ 午報14:00 決策。\n\n")
    return out


def format_morning_premise(premise, prem_date, max_rows=8):
    """[V22.69] 盤前報區塊 — 渲染昨日收盤報寫入的 T+1 前提(因果鏈的錨)。
    premise: {code: {t1_pred, conf, reason, rsi, frag}} 或 None。
    ★誠實處理:premise 為 None → 明說「收盤報未寫入」,不假裝有前提(缺陷A家族教訓)。
    ★過期警示:前提日與今日相差 > CROWD_STALE_WARN_DAYS 個交易日跨度時提示,避免拿舊前提當今日基準。"""
    hdr = ("\n## 🔗 昨日收盤報 → 今日前提(四報因果鏈)\n\n"
           "> 📌 收盤報 17:30 寫入的 T+1 方向前提。盤前報結合隔夜美股 + 產業新聞後,\n"
           "> 據此產出【今日預測】;9:30 開盤後由**早報驗證**(傳導 vs 提前布局)。\n\n")
    if not premise:
        return (hdr + "> ⬜ **無昨日前提** — `morning_premise` 表無記錄。\n"
                "> 因果鏈斷點:昨日收盤報未寫入 T+1 前提(可能昨日未跑收盤報,或 T1_PREDICT 未啟用)。\n"
                "> 今日盤前預測退化為【純隔夜美股 + 新聞】,無昨日收盤基準可對照。\n\n")
    try:
        _age = (_now_utc8().date() - datetime.date.fromisoformat(prem_date)).days
    except Exception:
        _age = None
    stale = ""
    if _age is not None and _age > 4:
        stale = (f"> ⚠️ **前提偏舊**:此前提寫於 {prem_date}(距今 {_age} 日)。\n"
                 f"> 中間交易日的收盤報可能未寫入 → 對照基準非「昨日」,判讀請降權。\n\n")
    rows = []
    for _c, _p in premise.items():
        if not isinstance(_p, dict):
            continue
        _tp = _p.get("t1_pred", 0) or 0
        _nm = TARGET_ETFS.get(_c, {}).get("name", _c)[:10]
        rows.append((_nm, _c, _tp, _p.get("conf", "低"), _p.get("reason", "")))
    rows.sort(key=lambda x: -abs(x[2]))
    body = (f"> 🗓️ 前提來源日:**{prem_date}**（收盤報寫入）\n\n"
            "| ETF | 昨收預測T+1 | 信心 | 依據 |\n|---|---|---|---|\n")
    for _nm, _c, _tp, _tc, _tr in rows[:max_rows]:
        _ic = "🔴" if _tp < -0.8 else ("🟢" if _tp > 0.8 else "⚪")
        body += f"| {_nm} | {_ic} {_tp:+.2f}% | {_tc} | {_tr} |\n"
    body += ("\n> ⚠️ **校準期(observe-only)**:T+1 權重未經真實數據驗證(E-LOG #258),\n"
             "> 此前提僅供對照,不自動下單。早報將以開盤實況驗證此前提之真偽。\n\n")
    return hdr + stale + body

def save_closing_handoff(conn, date_str, results, news_map=None, flow_map=None, extra=None):
    """[V22.80/81 富交棒] 收盤把『今日一切情報綜合』結構化交棒給明日盤前:
    分板塊方向偏向(action)+ 最強方向預測(t1_pred)+ 宏觀傾向(macro_prior)
    + 市場regime(tech.Regime聚合)+ 資金流方向(flow_map 機構5日)+ 新聞面(news_map)。
    盤前讀它做今日起手情境,非從零重推。★只用【確實可取】的來源,不硬塞。"""
    try:
        import json as _json
        style_dir = {}
        for r in results:
            st = r.get("style", "?"); act = str(r.get("action", ""))
            d = style_dir.setdefault(st, {"buy": 0, "reduce": 0, "hold": 0})
            if ("主攻" in act) or ("加碼" in act) or ("買" in act):
                d["buy"] += 1
            elif ("減倉" in act) or ("清倉" in act) or ("賣" in act):
                d["reduce"] += 1
            else:
                d["hold"] += 1
        style_bias = {}
        for st, d in style_dir.items():
            if d["buy"] > d["reduce"] and d["buy"] >= d["hold"]:
                b = "主攻"
            elif d["reduce"] > d["buy"]:
                b = "減倉"
            elif d["buy"] > 0 and d["reduce"] > 0:
                b = "分歧"
            else:
                b = "靜默"
            style_bias[st] = {"bias": b, **d}
        _top = sorted(
            [(r.get("code"), (r.get("name", "") or "")[:8], r.get("t1_pred", 0) or 0,
              r.get("t1_conf", "低")) for r in results],
            key=lambda x: -abs(x[2]))[:5]
        _macro = [r.get("macro_prior") for r in results if r.get("macro_prior") is not None]
        # [V22.81] 市場 regime(tech.Regime 聚合)
        _reg = {}
        for r in results:
            _rg = (r.get("tech") or {}).get("Regime")
            if _rg:
                _reg[_rg] = _reg.get(_rg, 0) + 1
        _regime = max(_reg, key=_reg.get) if _reg else None
        # [V22.81] 資金流方向(flow_map = {code: 機構5日億})
        _fnet = 0.0; _fin = _fout = 0
        for _v in (flow_map or {}).values():
            if _v is None:
                continue
            try:
                _v = float(_v)
            except Exception as _e:
                _log_err(_e, "save_closing_handoff:L4023")  # [V24.16] 原為靜默 continue
                continue
            _fnet += _v; _fin += (_v > 0); _fout += (_v < 0)
        # [V22.81] 新聞面(news_map = {code:(score,reason)})
        _ns = []
        for _v in (news_map or {}).values():
            _s = _v[0] if isinstance(_v, (list, tuple)) and _v else _v
            if _s not in (None, 0):
                try:
                    _ns.append(float(_s))
                except Exception as _e:
                    _log_err(_e, "save_closing_handoff:L4033")  # [V24.16] 原為靜默 pass
                    pass
        payload = {
            "date": date_str,
            "style_bias": style_bias,
            "top_pred": [{"code": c, "name": n, "t1": round(p, 4), "conf": cf}
                         for c, n, p, cf in _top],
            "macro_avg": round(sum(_macro) / len(_macro), 3) if _macro else 0.0,
            "regime": _regime, "regime_dist": _reg,
            "flow_net_yi": round(_fnet, 2), "flow_in_n": _fin, "flow_out_n": _fout,
            "news_avg": round(sum(_ns) / len(_ns), 3) if _ns else 0.0, "news_n": len(_ns),
            # [V22.85] news_valid: 區分「抓失敗凍的0」(False) vs「真無方向新聞」(True)。
            # 收盤當下 news_map 空→疑抓取失敗,消費端勿盲信此0(WB 定位 + Claude 補強)。
            "news_valid": bool(news_map),
        }
        if extra:
            payload.update(extra)
        conn.execute("CREATE TABLE IF NOT EXISTS closing_handoff("
                     "date TEXT PRIMARY KEY, payload TEXT, ts TEXT)")
        conn.execute("INSERT OR REPLACE INTO closing_handoff VALUES(?,?,?)",
                     (date_str, _json.dumps(payload, ensure_ascii=False),
                      datetime.datetime.now().isoformat(timespec="seconds")))
    except Exception as _e:
        _log_err(_e, "save_closing_handoff")

def load_closing_handoff(conn):
    """[V22.80] 讀最新收盤交棒(盤前用)。回 (payload|None, date|None)。"""
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS closing_handoff("
                     "date TEXT PRIMARY KEY, payload TEXT, ts TEXT)")
        _r = conn.execute("SELECT date, payload FROM closing_handoff "
                          "ORDER BY date DESC LIMIT 1").fetchone()
        if not _r:
            return None, None
        import json as _json
        return _json.loads(_r[1]), _r[0]
    except Exception as _e:
        _log_err(_e, "load_closing_handoff")
        return None, None


REALIZED_MAX_GAP_DAYS = 7   # 前提日到目標日超過 7 個日曆日(系統停跑/長假)不配對


def _realized_next_by_code(rmap_dates, rmap, after, max_gap_days=REALIZED_MAX_GAP_DAYS):
    """{code: (目標日, 實現報酬)}:每檔嚴格晚於 after 的第一個實現報酬日。
    各檔分別找(台股與A股休市日不同,不可用全表的同一個「下一日」)。"""
    out = {}
    a = datetime.date.fromisoformat(after)
    for code, ds in rmap_dates.items():
        nd = next((d for d in ds if d > after), None)
        if nd and (datetime.date.fromisoformat(nd) - a).days <= max_gap_days:
            out[code] = (nd, rmap[(code, nd)])
    return out


def format_close_scorecard(conn, today_str, roll_n=10):
    """[V22.79] 收盤級預測命中計分(場外按收盤NAV成交→這才是真計分,早報9:59版為盤中預覽)。
    前驗 = 收盤報寫入的 T+1 前提(morning_premise.t1_pred);
    後驗 = 該前提【目標日】的實現報酬,讀單一來源 realized_returns。
      原本讀 daily_snapshot.today_ret,但該欄存的是【系統當日估值】(場內代理經 RSI 截斷、±8% 夾擠),
      不是實際收盤 —— 等於拿自己的估計當答案計分。
    逐檔方向命中 + 近 roll_n 個前提日滾動命中率。★observe-only,不回寫權重(㉘假精確防禦)。"""
    hdr = ("\n## 🎯 收盤級預測命中計分(前驗 T+1 vs 實際收盤)\n\n"
           "> 場外 ETF 按收盤 NAV 成交 → **收盤級方向命中才是真計分**(早報 9:59 版為盤中預覽,盤中會反轉)。\n"
           "> ⚠️ 校準期 observe-only:樣本小,僅供觀察,**不自動調權**。\n\n")
    _DIR = 0.0015
    def _d(v):
        return 0 if v is None else (1 if v > _DIR else (-1 if v < -_DIR else 0))
    try:
        _pdates = [r[0] for r in conn.execute(
            "SELECT date FROM morning_premise WHERE date <= ? ORDER BY date DESC LIMIT ?",
            (today_str, roll_n)).fetchall()]
    except Exception as _e:
        _log_err(_e, "scorecard_pdates")
        return hdr + "> ⬜ morning_premise 無記錄 → 無法計分。\n\n"
    if not _pdates:
        return hdr + "> ⬜ 無前提記錄(需先跑收盤報寫 T+1 前提)。\n\n"
    _roll_h = _roll_t = 0
    _latest = None
    _rmap = returns_store.load_map(conn)
    _rdates = returns_store.dates_by_code(_rmap)
    for _pd in _pdates:
        _prem = load_morning_premise(conn, _pd)
        if not _prem:
            continue
        _nxt = _realized_next_by_code(_rdates, _rmap, _pd)
        if not _nxt:
            continue
        _tdate = min(d for d, _ in _nxt.values())
        _act = {c: r for c, (_d, r) in _nxt.items()}
        _rows = []
        for _c, _p in _prem.items():
            if not isinstance(_p, dict):
                continue
            _a = _act.get(_c)
            if _a is None:
                continue
            _tp = (_p.get("t1_pred", 0) or 0) / 100.0   # [V22.95 合併×100修] t1_pred 是百分比數(-1.48)→ 轉小數,與 today_ret(decimal)+_DIR 一致
            _ok = (_d(_tp) != 0 and _d(_tp) == _d(_a)) or (_d(_tp) == 0 and _d(_a) == 0)
            _rows.append((TARGET_ETFS.get(_c, {}).get("name", _c)[:10], _c, _tp, _a, _ok,
                          _p.get("conf", "低")))
        _roll_h += sum(1 for r in _rows if r[4])
        _roll_t += len(_rows)
        if _latest is None and _rows:
            _latest = (_pd, _tdate, _rows)
    if _roll_t == 0:
        return hdr + ("> ⬜ 前提已寫,但尚無配對的實現報酬(realized_returns 待累積)"
                      "→ 下個交易日起自動計分。\n\n")
    _pd, _tdate, _rows = _latest
    body = (f"> 🗓️ 最近可計分:前提日 **{_pd}** → 目標日 **{_tdate}** 實際收盤\n\n"
            "| ETF | 前驗T+1 | 實際收盤 | 命中 | 信心 |\n|---|---|---|---|---|\n")
    for _nm, _c, _tp, _a, _ok, _cf in sorted(_rows, key=lambda x: -abs(x[3]))[:12]:
        body += f"| {_nm} | {_tp*100:+.2f}% | {_a*100:+.2f}% | {'🎯' if _ok else '⚠️'} | {_cf} |\n"
    _dh = sum(1 for r in _rows if r[4]); _dt = len(_rows)
    body += (f"\n> 📊 該日方向命中:**{_dh}/{_dt} = {100*_dh/_dt:.0f}%**\n"
             f"> 📈 **滾動命中率**(近 {len(_pdates)} 個前提日,{_roll_t} 檔次):"
             f"**{_roll_h}/{_roll_t} = {100*_roll_h/_roll_t:.0f}%**\n\n")
    # [V22.82] 重大失準檢討:方向錯 + 單日大幅移動(|實際|≥2%)→ 打臉,逐檔追原因(對應「暴跌不檢討」)
    _busts = [(nm, c, tp, a, cf) for nm, c, tp, a, ok, cf in _rows if (not ok) and abs(a) >= 0.02]
    if _busts:
        body += "> 🔍 **重大失準檢討**(昨日預測方向錯 + 單日≥2%,信號失效需追因):\n"
        for nm, c, tp, a, cf in sorted(_busts, key=lambda x: -abs(x[3])):
            _blind = "(⚠️系統無新聞源→消息面盲區,需接新聞才能解釋)" if abs(a) >= 0.05 else "(查資金派發/技術破位)"
            body += (f"> - **{nm}**:昨預測 {tp*100:+.2f}%(信心{cf})→ 實際 **{a*100:+.2f}%** = **打臉**,"
                     f"幅度 {abs(a)*100:.1f}% {_blind}\n")
        body += "> ➡️ 失準檢討才能修正下次——非命中率數字,是【為什麼錯】。\n\n"
    return hdr + body

def format_premise_verification(premise, actual_map, prem_date, max_rows=8):
    """[V22.70] 早報專屬 — 前驗×後驗驗證(四報因果鏈的【驗證閉環】)。
    前驗 = 收盤報寫入 morning_premise 的 T+1 方向預測(t1_pred)
    後驗 = 早報當下抓到的實際走勢(同日場內漲跌 intraday_proxy_ret)
    比對方向 → 🎯命中 / ⚠️偏離；並給命中率(校準期 observe-only)。
    premise   : {code:{t1_pred,conf,reason,rsi,frag}} 或 None
    actual_map: {code:{"ret":float,"src":str,"market":str}}  ← 早報迴圈收集
    ★誠實處理: premise 為 None → 明說「無昨日前提·無法驗證」(缺陷A家族教訓);
               後驗不可得(台股無場內代理/早報未取得) → 顯「— 待取」,不用 0 假裝沒動。
    ★命中率非校準結論:樣本極小,只標 observe-only,不回寫權重(㉘假精確防禦)。"""
    hdr = ("\n## 🔬 前驗×後驗驗證 — 收盤報 T+1 預測 vs 今日實際\n\n"
           "> 📌 **前驗**=昨日收盤報寫入的 T+1 方向前提；**後驗**=今日早報抓到的實際走勢(日內)。\n"
           "> 這是四報因果鏈的【驗證閉環】:收盤報說「明天會漲/跌」→ 早報看「今天到底漲/跌了」。\n"
           "> ⚠️ 後驗為**日內實況**(開盤至早報時點),非收盤;盤中仍可能反轉,判讀請保守。\n"
           "> ⚠️ 校準期(observe-only):樣本極小,命中率僅供觀察,**不自動調權**。\n\n")
    if not premise:
        return (hdr + "> ⬜ **無昨日前提** — `morning_premise` 表無記錄,無法做前驗×後驗。\n"
                "> 因果鏈斷點:昨日收盤報未寫入 T+1 前提(可能未跑收盤報,或 T1_PREDICT 未啟用)。\n\n")
    # 過期警示(前提日 vs 今日)
    try:
        _age = (_now_utc8().date() - datetime.date.fromisoformat(prem_date)).days
    except Exception:
        _age = None
    stale = ""
    if _age is not None and _age > 4:
        stale = (f"> ⚠️ **前提偏舊**:寫於 {prem_date}(距今 {_age} 日),中間交易日收盤報可能漏寫 → "
                 f"對照基準非「昨日」,判讀降權。\n\n")
    # 方向判斷: |x|>=0.15% 視為有方向,否則中性(避免盤中微小噪動誤判命中;
    #   0.15% 與專案 RSI 反轉護欄/T+1 門檻同量級,確保 ±0.15% 內才算噪動)
    _DIR = 0.0015
    def _dir(v):
        if v is None: return 0
        if v > _DIR: return 1
        if v < -_DIR: return -1
        return 0
    rows, _hit, _miss = [], 0, 0
    for _c, _p in premise.items():
        if not isinstance(_p, dict):
            continue
        _tp = _p.get("t1_pred", 0) or 0                       # 前驗:收盤報 T+1 預測
        _ai = actual_map.get(_c)                             # 後驗:早報實際走勢
        _act = _ai.get("ret") if _ai else None
        _asrc = _ai.get("src") if _ai else None
        # 後驗可用性: 台股(src=TW_Direct,ret=0.0)或未取得 → 不可用,顯「—」(不用 0 假裝)
        _unavail = (_act is None) or (abs(_act) < 1e-9 and _asrc in ("TW_Direct", None))
        _act_dir = None if _unavail else _dir(_act)
        _act_disp = "—" if _unavail else f"{_act:+.2%}"
        _pred_dir = _dir((_tp or 0) / 100.0)   # [V23.0 修單位] t1_pred 是percent→轉decimal,與 _act(decimal)+_DIR 一致(修近平預測中性帶)
        if _pred_dir != 0 and _act_dir is not None and _act_dir != 0:
            if _pred_dir == _act_dir:
                _st = "🎯 命中"; _hit += 1
            else:
                _st = "⚠️ 偏離"; _miss += 1
        elif _pred_dir == 0:
            _st = "⚪ 前驗中性"
        else:
            _st = "⏳ 後驗待取"
        _nm = TARGET_ETFS.get(_c, {}).get("name", _c)[:10]
        rows.append((_nm, f"{_tp:+.2f}%", _act_disp, _st, _p.get("conf", "低"), _p.get("reason", "")))
    # 偏離優先排序(最該警惕的領先信號擺最前)
    rows.sort(key=lambda x: 0 if "偏離" in x[3] else (1 if "命中" in x[3] else 2))
    body = (f"> 🗓️ 前提來源日:**{prem_date}**(收盤報寫入) · 後驗=今日早報日內實況\n\n"
            "| ETF | 前驗 T+1 | 後驗 實際 | 狀態 | 信心 | 依據 |\n"
            "|---|---|---|---|---|---|\n")
    for _nm, _pd, _ad, _st, _tc, _tr in rows[:max_rows]:
        body += f"| {_nm} | {_pd} | {_ad} | {_st} | {_tc} | {_tr} |\n"
    _tot = _hit + _miss
    if _tot > 0:
        _rate = _hit / _tot
        body += (f"\n> **前驗×後驗 命中率 {_hit}/{_tot} = {_rate:.0%}** — "
                 f"{'🎯 方向預測初步有效' if _rate >= 0.6 else '⚠️ 方向預測偏離偏高,權重待校準'}。\n"
                 f"> 校準期(observe-only):樣本 {_tot} 檔,未達統計顯著,僅供觀察,不回寫權重。\n")
    else:
        body += ("\n> 本輪無可對照的「前驗有方向 + 後驗有方向」樣本,命中率暫不計"
                 "(可能:前驗皆中性,或後驗皆未取得)。\n")
    return hdr + stale + body

# [V22.27.9] 數據管道離群值防呆 — 對應 #165(SOX 803.87 壞值教訓)
#   指數單日 |ret|>20% 或收盤量級差>100x 皆屬抓取錯位,不靜默餵入 macro_prior/回測。
_US_RET_OUTLIER = 0.20   # 任何指數單日 ±20% 皆異常

def _us_ret_outlier_guard(ret, name=""):
    """單日報酬 ±20% 防呆: 超標→log+回None(當不可用),不把 -89% 餵進 EV 算式。"""
    if ret is not None and abs(ret) > _US_RET_OUTLIER:
        try:
            _log_err(ValueError(f"{name} 單日報酬 {ret:+.1%} 超±{_US_RET_OUTLIER:.0%}(疑抓取錯位)"),
                     "us_ret_outlier_guard")
        except Exception as _e:
            _log_err(_e, "_us_ret_outlier_guard:L4274")  # [V24.16] 原為靜默 pass
            pass
        return None
    return ret

def _series_outlier_filter(closes, name=""):
    """5日收盤序列量級離群過濾: 剔除 <0.5x 或 >2x 鄰均值者(指數單日不可能±50%;
    如 SOX 803.87 少一位數 vs 鄰均8213→比率0.098,必抓)。用鄰均插值替代。"""
    if not closes or len(closes) < 3:
        return closes
    out = list(closes)
    for i in range(1, len(out) - 1):
        neigh = (out[i - 1] + out[i + 1]) / 2.0
        if neigh > 0 and (out[i] / neigh < 0.5 or out[i] / neigh > 2.0):
            try:
                _log_err(ValueError(f"{name} {out[i]} vs 鄰均{neigh:.0f} 量級離群,插值"),
                         "series_outlier_filter")
            except Exception as _e:
                _log_err(_e, "_series_outlier_filter:L4291")  # [V24.16] 原為靜默 pass
                pass
            out[i] = round(neigh, 2)
    return out


# ═══════════════════════════════════════════════════════════════════
# [V22.33.3] MacroAnchor — 隔夜錨單一真值源 + 日期釘選 + 跨進程落盤
#   治 D1/D2/D4/D12/D13 + Gemini G1-G3(module-global跨進程失效/假釘選/單路徑):
#   ① 落盤 JSON(跨進程) → 盤前/早/午/收四報讀同一 pin,錨全天固定(治退格)
#   ② 帶【日期】抓取 + 釘選(丟 nascent bar) → 治「退格一天」根因
#   ③ 兩支 get_sox/get_ndx 皆路由此處 → 消除實作分岔(D12/D13)
#   ④ effective_sox_for:|β|<0.3 脫鉤【不引用】,非乘負β當反向(治 F1/G4)
# ═══════════════════════════════════════════════════════════════════
_ANCHOR_CACHE_FILE = os.path.join(BASE_DIR, ".macro_anchor_cache.json")


def fetch_dated_us_series(symbol, days=7, _series_fn=None):
    """回傳 [(date_str, close)] 排序去None。★帶日期(治『只回 closes 無法釘選』)。失敗回 []。"""
    try:
        if _series_fn is not None:
            return sorted(_series_fn(symbol), key=lambda x: x[0])
        r = _yahoo_get(
            f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?range={days}d&interval=1d",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
        res = r.json()['chart']['result'][0]
        ts, closes = res['timestamp'], res['indicators']['quote'][0]['close']
        out = [(datetime.datetime.utcfromtimestamp(t).strftime("%Y-%m-%d"), float(c))
               for t, c in zip(ts, closes) if c is not None]
        return sorted(out, key=lambda x: x[0])
    except Exception as _e:
        _log_err(_e, "fetch_dated_us_series")
        return []


def _load_anchor_cache():
    try:
        if os.path.exists(_ANCHOR_CACHE_FILE):
            with open(_ANCHOR_CACHE_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception as _e:
        _log_err(_e, "_load_anchor_cache")
    return {}

def _save_anchor_cache(cache):
    try:
        with open(_ANCHOR_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as _e:
        _log_err(_e, "_save_anchor_cache")

def get_pinned_anchor(symbol, ashare_date=None, _series_fn=None):
    """★單一真值源。key=ashare_date|symbol。cache-first(落盤跨進程共享)。
    已有 pin → 直接回(盤前/早/午/收四報全天一致);無 pin → 抓+釘+落盤。回 dict(ret 可能 None)。"""
    if ashare_date is None:
        ashare_date = _now_utc8().strftime("%Y-%m-%d")
    key = f"{ashare_date}|{symbol}"
    cache = _load_anchor_cache()
    if key in cache and cache[key].get("ret") is not None:
        c = dict(cache[key])
        c["source"] = "pinned_cache"
        return c
    exp = _expected_latest_us_date(ashare_date)
    series = fetch_dated_us_series(symbol, _series_fn=_series_fn)
    ret, as_of, dropped = compute_pinned_ret(series, exp)
    rec = {"ret": ret, "as_of": as_of, "symbol": symbol, "ashare_date": ashare_date,
           "expected_us": exp, "dropped_nascent": dropped, "stale": False,
           "source": "fresh_pin" if ret is not None else "fetch_fail"}
    if ret is not None:
        cache[key] = rec
        _save_anchor_cache(cache)
    return rec

def get_pinned_trajectory(symbol, n=3, ashare_date=None):
    """[V22.35 治D2顯示層] 由【與當前錨同一份釘選序列】派生 n 日報酬軌跡。
    ★根因:報告的「當前錨」走 get_pinned_anchor(已釘選),但「3日軌跡」走 get_5day_history
      (純位置索引、未釘選)→ 兩者各抓各的,軌跡末位可能 ≠ 當前錨(7/15-7/17 錨凍結時期尤明顯)。
    回 (rets:list[float], asof:list[str])。失敗回 ([], [])。
    ★不變式:rets[-1] 必等於 get_pinned_anchor(symbol) 的 ret(同源同釘選)。"""
    try:
        if ashare_date is None:
            ashare_date = _now_utc8().strftime("%Y-%m-%d")
        exp = _expected_latest_us_date(ashare_date)
        series = fetch_dated_us_series(symbol, days=max(10, n * 3))
        if not series:
            return [], []
        s = [x for x in series if x[0] <= exp]      # 同一把釘選尺:丟掉 nascent/未來 bar
        if len(s) < 2:
            return [], []
        rets, asof = [], []
        for i in range(max(1, len(s) - n), len(s)):
            if s[i - 1][1]:
                rets.append((s[i][1] - s[i - 1][1]) / s[i - 1][1])
                asof.append(s[i][0])
        return rets, asof
    except Exception as _e:
        _log_err(_e, f"get_pinned_trajectory({symbol})")
        return [], []



# [V22.33.6] 移除死碼 reconcile_macro_holding(V33.5 零呼叫點,WB 查驗抓到)。
#   其唯一用途(呼叫 effective_sox_for)已由夜間/盤前 F1 活接線取代 → effective_sox_for 不再是死碼。
#   D11「宏觀 vs 逐檔誰蓋誰」的顯示層調和,待 report-gen 端整合(WB live-confirm),不留死函式佔位。

def _dated_yahoo(code, days=90):
    """Yahoo chart → [(YYYY-MM-DD, close)]。失敗回 []。"""
    try:
        import datetime as _dt
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{code}?range={days}d&interval=1d"
        r = _yahoo_get(url, timeout=8)  # [V22.48] 代理預檢+快取回退
        if r is None: return []
        res = r.json()['chart']['result'][0]
        ts = res['timestamp']; cl = res['indicators']['quote'][0]['close']
        return [(_dt.datetime.utcfromtimestamp(t).strftime("%Y-%m-%d"), float(c))
                for t, c in zip(ts, cl) if c is not None]
    except Exception as e:
        print(f"   ⚠️ 條件beta美股序列抓取失敗 ({code}): {e}")
        return []

def _dated_nav(code, limit=120):
    """天天基金 NAV → [(YYYY-MM-DD, nav)]。失敗回 []。"""
    try:
        import datetime as _dt
        url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
        headers = {"User-Agent":"Mozilla/5.0", "Referer":f"http://fund.eastmoney.com/{code}.html"}
        r = _em_throttled_get(url, timeout=8, tag="em1", headers=headers)  # [V22.44]
        if r is None: return None
        m = re.search(r'Data_netWorthTrend\s*=\s*(\[.*?\]);', r.text)
        if not m:
            return []
        data = json.loads(m.group(1))
        out = []
        for item in data:
            if 'x' in item and 'y' in item:
                d = _dt.datetime.utcfromtimestamp(item['x']/1000).strftime("%Y-%m-%d")
                out.append((d, float(item['y'])))
        # [V24.15] 原寫死 out[-120:]。WB 2026-08-07 實測揭露:此截斷使
        #   compute_max_drawdown_from_nav 實際只看 ~6 個月,**與其 docstring 宣稱的
        #   「可取數年完整序列」直接矛盾**(我 E-LOG_282 那句斷言是錯的,見 E-LOG_287)。
        #   實證低估幅度:008888 全序列 -59.94% vs 近120 -36.78%(差23pp)、011613 差29pp。
        #   改為參數化而非全域去截斷:技術指標消費端維持 120(零回歸),
        #   MDD 專用呼叫傳 limit=None 取全序列。
        #   註:pingzhongdata 本來就整包下載後才截斷 → 去截斷【不增加網路成本】。
        return out[-limit:] if limit else out
    except Exception as e:
        print(f"   ⚠️ 條件beta NAV序列抓取失敗 ({code}): {e}")
        return []

def prime_conditional_betas(codes):
    """為 codes 計算條件 beta 並寫入 _COND_BETA_CACHE。全程防禦, 失敗不影響主流程。
    QDII→對NDX, 其餘→對SOX。資料不足或抓取失敗的代碼留空(由 get_beta_us fallback 固定值)。"""
    try:
        us_cache = {}
        ok, fb, skip_qdii = [], [], []
        for code in codes:
            try:
                info = TARGET_ETFS.get(code, {})
                # [V22.27.3] QDII(納指)直接追指數,β結構性≈固定值;其NAV走海外託管結算滯後,
                #   與A股式「對美股t-1」隔夜對齊不符→會算出負β假象(追指數卻負β=物理不可能)。
                #   故QDII不估計,維持結構性固定β;條件β僅用於A股(連動時變且NAV當日對齊正確)。
                if _is_qdii_etf(info):
                    skip_qdii.append(code); continue
                us_idx = "%5ESOX"   # 走到此處皆A股(QDII已skip),對SOX
                if us_idx not in us_cache:
                    us_cache[us_idx] = _dated_yahoo(us_idx, 90)
                us_dated = us_cache[us_idx]
                is_tw = (info.get("market") == "台股")
                asset_dated = _dated_yahoo(code, 90) if is_tw else _dated_nav(code)
                if not asset_dated or not us_dated:
                    fb.append(code); continue
                A, U = _align_overnight(asset_dated, us_dated)
                cb = compute_conditional_beta(A, U)
                if cb is None:
                    fb.append(code); continue
                _COND_BETA_CACHE[code] = round(cb, 3)
                ok.append(f"{code}={cb:+.2f}(固定{BETA_US.get(code,0):+.2f})")
            except Exception as e:
                fb.append(code); print(f"   ⚠️ 條件beta({code})計算失敗→fallback固定: {e}")
        if ok:
            print(f"   ✅ 條件beta實算 {len(ok)}檔: " + " | ".join(ok))
        if fb:
            print(f"   ↩️ 條件beta fallback固定 {len(fb)}檔: {','.join(fb)}")
        if skip_qdii:
            print(f"   ⏭️ QDII維持結構性固定β {len(skip_qdii)}檔: {','.join(skip_qdii)}")
        return bool(ok)
    except Exception as e:
        print(f"   ⚠️ prime_conditional_betas整體失敗→全用固定BETA_US: {e}")
        return False

def render_conditional_beta_block(held_codes=None):
    """[V22.27.1] 條件β脫鉤監測顯示塊 — 讓引擎每跑可見(firing值 vs 固定fallback)。
    [V22.27.2] 可傳入 held_codes(收盤報走USER_COSTS);預設USER_HOLDINGS。"""
    try:
        if held_codes is None:
            held_codes = list(USER_HOLDINGS.keys())
        held = [c for c in held_codes if c in BETA_US]
        if not held:
            return ""
        ashare = [c for c in held if not _is_qdii_etf(TARGET_ETFS.get(c, {}))]
        qdii   = [c for c in held if _is_qdii_etf(TARGET_ETFS.get(c, {}))]
        fired = sum(1 for c in ashare if _COND_BETA_CACHE.get(c) is not None)
        head = (f"  A股實算 {fired}/{len(ashare)} 檔（滾動60日 Cov/Var, A股[t]對美股[t-1]）"
                + (f"｜QDII {len(qdii)} 檔維持結構性固定β（直接追指數）" if qdii else "")
                if (fired or qdii) else
                f"  ⚠️ 全 fallback 固定值（{len(held)}檔 序列不足/抓取失敗）")
        lines = ["\n### 📡 條件β（脫鉤監測）", head]
        for c in held:
            info = TARGET_ETFS.get(c, {})
            nm = info.get("name", c)[:9]
            fixed = BETA_US.get(c, 0.0)
            cb = _COND_BETA_CACHE.get(c)
            if cb is None:
                if _is_qdii_etf(info):
                    lines.append(f"* {c} {nm}: 固定{fixed:+.2f}（QDII結構β·直接追指數）🔗")
                else:
                    lines.append(f"* {c} {nm}: 固定{fixed:+.2f}（fallback·序列不足/抓取失敗）")
                continue
            if   cb < 0:     tag = "🛡️ 避險反向"
            elif cb < 0.25:  tag = "🏠 內生主導（US連動弱）"
            elif cb < 0.50:  tag = "〰️ 中度連動"
            else:            tag = "🔗 強US連動"
            drift = "  📉 實測連動低於假設" if (fixed >= 0.30 and cb <= fixed - 0.20) else ""
            lines.append(f"* {c} {nm}: 條件β={cb:+.2f}（固定{fixed:+.2f}）{tag}{drift}")
        return "\n".join(lines) + "\n"
    except Exception as e:
        return f"\n### 📡 條件β（脫鉤監測）\n  （顯示失敗，不影響預測: {e}）\n"

# 後驗翻轉門檻(後驗推力 ÷ |先驗| 的倍數):
POSTERIOR_FLIP_RATIO   = 1.0   # ≥1.0倍 且方向相反 → 🔄前提翻轉
POSTERIOR_LOOSEN_RATIO = 0.3   # 0.3~1.0倍 → 🟡鬆動(待早報確認); <0.3 → ➡️維持
# ════════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════════
# [V22.23.3 預測偏差自校正] 數據先於預期的反饋迴路 (merge自V22.23.2分支)
#   病根: 系統會「預測」卻從不「回頭看自己準不準並自我修正」→ 無反饋迴路。
#         電力016185被系統性高估一個月(6/1主攻錯)系統渾然不覺繼續高估;
#         6/12分裂盤同理(美股漲→A股科技全喊漲, 實際跌)。
#   解法: 用DB真實 actual_ret 量測每檔系統性偏差 mean(pred-actual), 保守修正。
#   安全: 樣本<8不動作; 只修一半; 封頂±1%; 全程透明print。
PRED_BIAS_LOOKBACK_RECS   = 90    # 回看最近筆數(≈30交易日×3報)
PRED_BIAS_MIN_SAMPLES     = 8     # 樣本<此值不修正(資料不足→安全靜默)
PRED_BIAS_CORRECTION_FRAC = 0.5   # 只修正量測偏差的一半(保守)
PRED_BIAS_CAP             = 0.01  # 修正上限±1%(防過度修正)

def compute_pred_bias(code):
    """[V22.23.3] 每檔預測偏差自校正。
    從DB撈該檔最近配對(predicted_ret, actual_ret), 量測系統性偏差。
    bias = mean(pred − actual): >0=系統性高估(像電力), <0=低估。
    回傳 (correction, info|None): 樣本不足回 (0.0, None) 不修正。
    """
    try:
        import sqlite3 as _sq
        with _sq.connect(LOG_DB, timeout=10) as conn:
            rows = conn.execute(
                "SELECT predicted_ret, actual_ret FROM predictions "
                "WHERE code=? AND actual_ret IS NOT NULL AND predicted_ret IS NOT NULL "
                "ORDER BY ts DESC LIMIT ?", (code, PRED_BIAS_LOOKBACK_RECS)).fetchall()
    except Exception:
        return 0.0, None
    pairs = [(p, a) for p, a in rows if p is not None and a is not None]
    if len(pairs) < PRED_BIAS_MIN_SAMPLES:
        return 0.0, None
    bias = sum(p - a for p, a in pairs) / len(pairs)      # >0=高估
    corr = -PRED_BIAS_CORRECTION_FRAC * bias              # 反向修正
    corr = max(-PRED_BIAS_CAP, min(PRED_BIAS_CAP, corr))  # 封頂
    return corr, {"n": len(pairs), "bias": bias, "corr": corr}
# ════════════════════════════════════════════════════════════════════


# [V22.17.3 新增 5/29] 同質性群組（相關性 ≥85% 的 ETF 應同步建議）
# 根因：5/29 011613 科創50 vs 018177 科創增強 同質性 95%
#       我建議「11613 留 / 018177 賣」差別處理 = 邏輯失誤
#       兩檔走勢幾乎完全同步（5/27 跌幅 -2.65% vs -2.58%）
# 規則：同群組內 ETF 出現「方向衝突信號」時，自動警示用戶
SIMILARITY_GROUPS = {
    "科創50": ["011613", "018177"],         # 科創 50 指數 vs 增強，同底層
    "半導體": ["008888", "016008"],         # 半導體 vs 消費電子（部分重疊）
    "納指QDII": ["006479", "018967"],       # 廣發納指 vs 匯添富納指 同指數
    "電力": ["016185"],                     # 暫時單檔
}

# [V22.76] find_similarity_group 已移除(零呼叫死碼;SIMILARITY_GROUPS 於他處直接迭代使用)

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID",   "YOUR_CHAT_ID_HERE")

TARGET_ETFS = {
    # [V22.75] 019072 嘉實通信ETF聯接(光模組/CPO)— 2026-07-27 買入 ¥500,自觀察池升入追蹤名單
    "019072":    {"name":"嘉實通信ETF聯接","market":"A股","style":"OptComm",
                  "proxy":"sz159695","proxy2":None,
                  "proxy_name":"通信ETF嘉實(159695)★同公司同指數國證通信","te":"0.03%","fund_type":"passive"},
    "00982A.TW": {"name":"群益台灣精選強棒","market":"台股","style":"Tech",
                  "proxy":None,"proxy2":None,"proxy_name":"Yahoo直連","te":"0.00%","fund_type":"passive"},
    "00929.TW":  {"name":"復華台灣科技優息","market":"台股","style":"Tech",
                  "proxy":None,"proxy2":None,"proxy_name":"Yahoo直連","te":"0.00%","fund_type":"passive"},
    "000216":    {"name":"华安黄金易联接A","market":"A股","style":"Defensive",
                  "proxy":"sh518880","proxy2":None,
                  "proxy_name":"华安黄金ETF(518880)★同公司同标的","te":"0.01%","fund_type":"passive"},
    "007466":    {"name":"华泰柏瑞红利联接","market":"A股","style":"Dividend",
                  "proxy":"sh512890","proxy2":"sh515080",
                  "proxy_name":"华泰柏瑞中证红利ETF(512890)★同公司同指数","te":"0.02%","fund_type":"passive"},
    "019141":    {"name":"易方达电信联接A","market":"A股","style":"Tech",
                  "proxy":"sh563010","proxy2":None,
                  "proxy_name":"易方达中证电信主题ETF(563010)★同公司同指数","te":"0.02%","fund_type":"passive"},
    "004432":    {"name":"南方有色金属联接","market":"A股","style":"Cyclical",
                  "proxy":"sh512400","proxy2":None,
                  "proxy_name":"有色金属ETF南方(512400)★同公司同指数[修正:旧sh515220实为国泰煤炭ETF]","te":"0.03%","fund_type":"passive"},
    "016185":    {"name":"广发电力全指联接","market":"A股","style":"Utility",
                  "proxy":"sz159611","proxy2":None,
                  "proxy_name":"广发中证全指电力ETF(159611)★同公司同指数","te":"0.02%","fund_type":"passive"},
    "020356":    {"name":"华夏半导体材料设备联接A","market":"A股","style":"Tech",
                  "proxy":"sh562590","proxy2":None,
                  "proxy_name":"半导体材料设备ETF(562590)","te":"0.10%","fund_type":"passive"},
    "008888":    {"name":"华夏芯片联接A","market":"A股","style":"Tech",
                  "proxy":"sz159995","proxy2":"sh588200",
                  "proxy_name":"芯片ETF(159995)+科创芯片ETF(588200)双验证","te":"0.04%","fund_type":"passive"},
    "018135":    {"name":"富国大数据联接C","market":"A股","style":"Tech",
                  "proxy":"sh515400","proxy2":None,
                  "proxy_name":"富国大数据产业ETF(515400)★同公司同指数","te":"0.02%","fund_type":"passive"},
    "011613":    {"name":"华夏科创50联接C","market":"A股","style":"Tech",
                  "proxy":"sh588000","proxy2":"sh588080",
                  "proxy_name":"华夏科创50ETF(588000)★同公司同指数","te":"0.01%","fund_type":"passive"},
    "018177":    {"name":"华夏科创50增强A","market":"A股","style":"Tech",
                  "proxy":"sh588000","proxy2":"sh588080",
                  "proxy_name":"科创50ETF双均值(588000+588080)[增强型·代理误差较大]","te":"0.15%","fund_type":"enhanced"},
    "016008":    {"name":"招商消费电子联接","market":"A股","style":"Tech",
                  "proxy":"sz159779","proxy2":None,
                  "proxy_name":"招商中证消费电子50ETF(159779)★同公司同指数","te":"0.02%","fund_type":"passive"},
    "006479":    {"name":"广发纳指100联接","market":"A股","style":"Tech",
                  "proxy":"sz159941","proxy2":"sh513100",
                  "proxy_name":"纳指100ETF(159941)+汇添富纳指(513100)双验证","te":"0.03%","fund_type":"qdii"},
    # [V24.9] 018966→018967 更正：用戶截圖持倉代碼為 018967(人民幣C類)。
    #   出處:汇添富纳斯达克100ETF发起式联接(QDII)C类基金产品资料概要(2025-08-15)
    #   —— 基金主代码 018966,下属基金代码 A=018966 / C=018967 / 美元现钞=018969 / 现汇=018968。
    #   影響:_dated_nav / get_eastmoney_nav 以 code 組 URL,原本抓到A類淨值序列。
    #   ⚠️ DB 歷史 predictions 仍以 018966 記錄 → 需一次性遷移(見 E-LOG_279,WB本機執行)。
    "018967":    {"name":"汇添富纳指100C","market":"A股","style":"Tech",
                  "proxy":"sh513100","proxy2":"sz159941",
                  "proxy_name":"国泰纳指100ETF(513100)·同纳指100指数非同公司[修正]","te":"0.02%","fund_type":"qdii"},
    "004070":    {"name":"南方证券ETF联接C","market":"A股","style":"Cyclical",
                  "proxy":"sz159842","proxy2":"sh512880",
                  "proxy_name":"银华证券ETF(159842)·同中证全指证券公司指数非同公司[修正]","te":"0.03%","fund_type":"passive"},
}




# ═══════════════════════════════════════════════════════════════════════════
# [V22.16 新增] 獨立觀察池 WATCHLIST_ETFS
# ═══════════════════════════════════════════════════════════════════════════
# 設計原則：
#   1. 不參與 TARGET_ETFS 的 RS-Rank / EV / Kelly 計算
#   2. 只追蹤每日 RSI / Regime / pred / 進場條件
#   3. 在午報新增「觀察池摘要」區塊單獨顯示
#   4. 進場條件：RSI ≤ 45 + Regime trending_up + EV ≥ 50（三條件同時滿足）
#
# 加入方式：完成 8D 研究 (web_search 驗證代碼 + 個股四維打分) 後加入
# ═══════════════════════════════════════════════════════════════════════════
WATCHLIST_ETFS = {
    # [V22.75] 019072 嘉實通信ETF聯接 已於 2026-07-27 買入 ¥500 → 移入 TARGET_ETFS(追蹤名單),
    #          不再列為觀察池標的(避免與持倉重複處理)。
    # ─── 軍工/國防 族群（2026-05-26 加入） ─────────────────────────────
    "005693": {
        "name":"廣發中證軍工聯接C",
        "market":"A股",
        "style":"Defense",
        "theme":"主流軍工",
        "proxy":"sh512680",
        "proxy2":None,
        "proxy_name":"廣發中證軍工ETF(512680)★同公司同指數中證軍工",
        "te":"0.03%",
        "fund_type":"passive",
        "anchors":["中航沈飛 600760", "中國船舶 600150", "航發動力 600893", "中航光電 002179"],
        "added_date":"2026-05-26",
        "add_reason":"地緣政治+十五五規劃，補非Tech族群",
        "index_top10_weight":"37.24%（中證軍工399967前10大）",
        "warning":"估值已升新階段，但軍貿+商業航天有結構性機會",
    },

    # ─── 人形機器人 族群（2026-06-02 加入，Computex催化） ─────────────────
    "018345": {
        "name":"華夏中證機器人ETF聯接C",
        "market":"A股",
        "style":"Robotics",
        "theme":"人形機器人/具身智能",
        "proxy":"sh562500",
        "proxy2":None,
        "proxy_name":"華夏中證機器人ETF(562500)★同公司同指數中證機器人[web驗證]",
        "te":"0.05%",
        "fund_type":"passive",
        "anchors":["匯川技術", "三花智控", "綠的諧波", "鳴志電器", "拓普集團"],
        "added_date":"2026-06-02",
        "add_reason":"Computex 2026 Physical AI/機器人主軸+黃仁勳機器人平台落地，補觀察池機器人族群空缺",
        "index_top10_weight":"追蹤中證機器人指數(932049)，工業機器人+減速器+伺服電機產業鏈",
        "warning":"近一年+38%，2026年內+17%，估值偏中段；WorkBuddy 6/1建議#2，web驗證為華夏基金管理(非WorkBuddy標的混淆)，方向正確",
    },

    # ─── 固態電池 族群（2026-06-02 加入，零重疊分散） ─────────────────
    "013180": {
        "name":"廣發國證新能源車電池聯接C",
        "market":"A股",
        "style":"NewEnergy",
        "theme":"固態電池/新能源車電池",
        "proxy":"sz159755",
        "proxy2":None,
        "proxy_name":"廣發新能源車電池ETF(159755)★同公司同指數國證新能源車電池980032[V22.18 web驗證]",
        "te":"0.05%",
        "fund_type":"passive",
        "anchors":["寧德時代", "比亞迪", "億緯鋰能", "陽光電源", "國軒高科"],
        "added_date":"2026-06-02",
        "add_reason":"2026固態電池量產元年(半固態已裝車/全固態2027示範)；與持倉零重疊；國金證券深度研報背書",
        "index_top10_weight":"追蹤國證新能源車電池指數(980032)，寧德/比亞迪為核心，電芯+材料+設備全鏈",
        "warning":"電池板塊已從底部反彈非最低位；估值中性(中證電池PE約34倍近10年34.7%分位)",
        "priority_watch":"2026-06-15 Claude web初判:估值分位偏低(約34%)+2026固態電池量產元年催化,位置次佳。待盤前報用真實RSI/EV/三共振複核(閉環對照)",
    },

    # ─── 台股高息 族群（2026-06-16 加入，用戶交代 / 試水場內台股追蹤）─────────
    "00919.TW": {
        "name":"群益台灣精選高息ETF",
        "market":"台股",
        "style":"Dividend",
        "theme":"台股高息/金融權值",
        "proxy":"00919.TW",
        "proxy2":None,
        "proxy_name":"群益台灣精選高息00919(自身即場內, Yahoo直連)",
        "te":"—",
        "fund_type":"tw",
        "anchors":["國泰金", "富邦金", "中信金", "廣達", "華碩"],
        "added_date":"2026-06-16",
        "add_reason":"用戶交代加入追蹤;追蹤臺灣指數公司特選臺灣上市上櫃精選高息指數,前10大成分股71.23%(金融為主+廣達);季配高息(2025全年2.52元殖利率約11%)。定位=長期存股候選,非現在追高",
        "index_top10_weight":"前10:國泰金/富邦金/中信金/廣達/台新新光金/華碩/長榮/聯詠/聯電/凱基金=71.23%(金融權值主導)",
        "warning":"⚠️今日(6/16)除息(配息1元), 股價已扣息;年初至今+34.78%但跑輸大盤12.24%(高息今年輸市值型);金融股高度集中;配息來源可能含收益平準金。台股經Yahoo掃描需VPN(沙盒測不出, WorkBuddy真環境驗證)",
        "priority_watch":"2026-06-16 Claude查證:歷史相對高位+今日除息,非現在追高時機;若長期存股可待除息後填息缺口分批。放觀察池追蹤填息與位置,不急進。",
    },

    # ─── 台股AI收益族群（2026-07-31 加入, 用戶交代）─────────────────────────
    "00406A.TW": {
        "name":"主動中信台灣收益",
        "market":"台股",
        "style":"Tech",
        "theme":"台股AI+月配+covered call",
        "proxy":"00406A.TW",
        "proxy2":None,
        "proxy_name":"主動中信台灣收益00406A(自身即場內, Yahoo直連)",
        "te":"—",
        "fund_type":"tw",
        "anchors":["台積電 2330", "鴻海 2317", "廣達 2382", "聯發科 2454"],
        "added_date":"2026-07-31",
        "add_reason":"用戶交代加入。台股AI供應鏈(半導體佔比30%+,前十大晶圓代工/伺服器/網通/被動元件龍頭)+月配+掩護性買權下檔緩衝;2026/6/11發行價10元,遇7月科技崩跌至8.06(-19%),用戶視為跌破發行價入場機會。",
        "index_top10_weight":"半導體30%+;前十大=台積電/鴻海/廣達/伺服器供應鏈AI龍頭",
        "warning":"⚠️台股AI+月配+covered call,適震盪不適急漲(強漲時上檔被賣出買權鎖住,漲不贏純AI);主動式經理費0.75%(比被動高)、新基金無真實績效(回測必樂觀)、首配0.128含收益平準金、RR4。台股經Yahoo掃描需VPN(沙盒測不出,WB真環境驗)。",
        "dd": {
            "結構":"主動選股+covered call(上檔封鎖)+月配;RR4;標的指數:無",
            "成本":"經理費0.75%+保管0.035%=0.785%/年(被動約0.3-0.4%,主動溢價約一倍)",
            "規模流動性":"7月中112.5億/4.5萬人 → 7/31 151.74億/7.12萬人(淨值跌期間規模+35%、受益人+58%=逢低申購承接);折價0.66~1.x%無溢價風險",
            "經理人":"張書廷;track record代理=00995A(2026/1掛牌,成立以來累積>60%,優於同期大盤與0050)",
            "持股":"64檔/22產業;半導體30%+;最大持股臺股期貨(covered call部位);前十大台積電/鴻海/廣達/聯發科",
            "配息":"月配,首配0.128(公告日收盤9.76→月化1.31%);含收益平準金→部分可能為本金;權利金部分免納綜所稅",
            "位置":"發行10元→8.81(8/3,-11.9%);8/1-8/3連3漲+11.33%(已非7/30的8.06)",
            "結構vs能力歸因":"★落後主因是結構(covered call封頂)+發行時點(6/11掛牌即遇7月科技崩),非經理人選股差——同經理人00995A同期>60%可證",
            "單點風險":"與00995A同一經理人同一投信→同時持有=重複押注經理人風險,非分散",
        },
        "priority_watch":"2026-07-31 Claude查證:底層台股AI供應鏈基本面硬;covered call適震盪不適急漲——賭AI強勢續漲不如純台股科技(00982A/00929漲更多),要月配現金流+抗震則其設計正好。放觀察池追蹤,是否進場看你對後市預期。",
    },

    "00995A.TW": {
        "name":"主動中信台灣卓越",
        "market":"台股",
        "style":"Tech",
        "theme":"台股主動選股(無上檔封鎖)·同經理人張書廷",
        "proxy":"00995A.TW",
        "proxy2":None,
        "proxy_name":"主動中信台灣卓越00995A(自身即場內, Yahoo直連)",
        "te":"—",
        "fund_type":"tw",
        "anchors":["台積電 2330", "鴻海 2317", "台達電 2308", "聯發科 2454"],
        "added_date":"2026-08-04",
        "add_reason":"用戶交代加入。與00406A同經理人(張書廷)但結構相反:純主動選股、無covered call封頂,成立以來累積>60%且優於同期大盤與0050——是張書廷 alpha 的直接證據,亦為00406A的對照組。",
        "index_top10_weight":"台股大型權值+AI供應鏈(主動選股,無追蹤指數)",
        "warning":"⚠️買點在高基期:成立半年累積>60%,追高風險高於00406A(後者-11.9%在低基期)。與00406A同經理人同投信→同時持有=重複押注經理人風險,非分散。主動式無指數可退守,經理人離職為單點風險。台股經Yahoo掃描需VPN(沙盒測不出,須本機驗)。",
        "priority_watch":"2026-08-04 Claude查證:張書廷 track record 的實證(>60%勝0050);要賺AI上漲其結構優於00406A(不封頂),但你買在+60%之後的高基期。要現金流選00406A、要成長選00995A,兩者擇一勿並持。",
        "dd": {
            "結構":"主動選股/因子投資,無covered call封頂,無配息",
            "成本":"待查(中信主動式ETF約0.75%級距,未逐項核實→未驗證·待查)",
            "規模流動性":"未逐項核實→未驗證·待查",
            "經理人":"張書廷(同00406A);成立以來累積>60%,優於同期大盤與0050(掛牌2026/1,約半年,樣本短且處AI多頭)",
            "持股":"台股大型權值+AI供應鏈(主動,無指數)",
            "配息":"無配息(近期未配發現金股利)",
            "位置":"18.41(6/18);成立以來>60%=高基期",
            "結構vs能力歸因":"★其優異表現部分來自「無封頂」結構+掛牌時點好(1月掛牌吃到整段AI行情),非純選股alpha;需與00406A對照才能拆開",
            "單點風險":"同00406A經理人→重複押注;主動式無指數退守",
        },
    },

    # ─── AI供應鏈「賣鏟人」族群（2026-07-13 加入, AI材料矩陣重刷版第一/第二順位）─────

    "025856": {
        "name":"華夏電網設備聯接A",
        "market":"A股",
        "style":"Industrial",
        "theme":"AI電力/電網設備",
        "proxy":"sz159326",
        "proxy2":None,
        "proxy_name":"電網設備ETF華夏(159326)★同公司同指數 中證電網設備主題931994",
        "te":"0.03%",
        "fund_type":"passive",
        "anchors":["中國西電 601179", "亨通光電 600487", "中天科技 600522", "特銳德 300001"],
        "anchor_pe":"(2026-07-13 未核個股PE, 交WB live補)",
        "added_date":"2026-07-13",
        "add_reason":"AI材料矩陣重刷版【第二順位】。智能電網權重89%、特高壓65-67%(全市場最高),成分為輸變電/智能配電設備商(非發電運營商,與持倉016185不同群)。AI資料中心用電buildout+電網升級+設備出海。主力:回調中連3日淨流入13.92億(新浪/Wind)。已從高位回調約一個月,7/10企穩+2%。",
        "index_top10_weight":"杭電/中國西電/東材/亨通/中天/特銳德/白雲電器/智洋創新/金利華電/明陽電氣",
        "warning":"⚠️近一年漲幅四源歧異(60-95%),★不可靠指標不採為決策依據(E-LOG#178);改用可驗證事實:已回調+主力流入。7/10單日+2%非止跌確認,需觀察2-3日。",
        "priority_watch":"2026-07-13 重刷版第二順位。與持倉016185(發電運營/Utility)為不同sleeve,非重複。",
    },
}
#   用戶用國金證券買場內ETF/個股。這裡記錄「場內實時 vs 場外收盤NAV」對照,
#   為後續場內引擎攢數據。每對都是同底層資產(同公司/同指數), 物理已web驗證。
#   ⚠️ 電力修正: 用戶原寫159146(華寶),經查證實為華宝中證全指電力公用事業,
#      非其016185廣發。改用159611(廣發, 同H30199指數, 016185即其聯接)為精確兄弟。
#   注意: 此dict僅供場內對照觀察, 不進compute_order、不動11檔場外持倉。
INTRADAY_PAIRS = {
    # 場內代碼   : {場外對應, 名稱, 是否持有, 標的物理驗證}
    "sh518880": {"otc": "000216", "name": "黃金ETF華安", "held": True,
                 "verify": "華安, 同SGE黃金, 000216為其聯接 [web驗證2026-06-15]"},
    "sz159995": {"otc": "008888", "name": "芯片ETF華夏", "held": True,
                 "verify": "華夏, 國證芯片指數30成分股, 008888為其聯接 [web驗證2026-06-15]"},
    "sz159732": {"otc": "016008", "name": "消費電子ETF華夏", "held": True,
                 "verify": "華夏, 國證消費電子指數50成分股, 016008為其聯接 [web驗證2026-06-15]"},
    "sz159611": {"otc": "016185", "name": "廣發電力ETF", "held": True,
                 "verify": "廣發, 中證全指電力公用事業H30199, 016185為其聯接;"
                           "✅修正用戶誤填的159146(華寶,不同公司) [web驗證2026-06-15]"},
}

def fetch_intraday_quote(proxy):
    """[V22.23.9→V23.7] 抓場內ETF實時報價。源鏈: 新浪hq(主) → 騰訊qt(備援,直連繞EM節流)。
    東財f62資金流已結構性封鎖(L5125),但即時報價走新浪/騰訊不受影響,故價格維度不因東財封鎖而空白。
    回傳 {name, price, prev_close, ret} 或 None(失敗不拋例外)。
    """
    # ① 新浪(主源)
    try:
        import re as _re_iq
        url = f"https://hq.sinajs.cn/list={proxy}"
        headers = {"User-Agent": "Mozilla/5.0",
                   "Referer": "https://finance.sina.com.cn"}
        r = _em_throttled_get(url, timeout=5, tag="em2", headers=headers)  # [V22.44]
        if r is not None:
            raw = r.content.decode("gbk", errors="replace")
            m = _re_iq.search(r'"([^"]+)"', raw)
            if m:
                parts = m.group(1).split(",")
                if len(parts) > 3:
                    prev_close = float(parts[2]); price = float(parts[3])
                    if prev_close > 0.01 and price > 0.01:
                        return {"name": parts[0], "price": price,
                                "prev_close": prev_close,
                                "ret": (price - prev_close) / prev_close}
    except Exception as _e:
        _log_err(_e, "fetch_intraday_quote_sina")
    # ② 騰訊(備援,直連繞開EM節流wrapper)
    try:
        import urllib.request as _uq
        req = _uq.Request(
            f"https://qt.gtimg.cn/q={proxy}",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com"})
        resp = _uq.urlopen(req, timeout=5)
        raw = resp.read().decode("gbk", errors="replace")
        inner = raw.split('"', 2)[1] if raw.count('"') >= 2 else ""
        parts = inner.split("~")
        if len(parts) >= 5:
            price = float(parts[3]); prev_close = float(parts[4])
            if prev_close > 0.01 and price > 0.01:
                return {"name": parts[1], "price": price,
                        "prev_close": prev_close,
                        "ret": (price - prev_close) / prev_close}
    except Exception as _e:
        _log_err(_e, "fetch_intraday_quote_tencent")
    return None

def _volume_flow_proxy(proxy):
    """[V23.8] 東財主力大單(fflow)結構性封鎖時的『量價資金流代理』降級源。
    用可達的新浪hq(主)/騰訊qt(備援)抓場內代理ETF即時量價，
    以(漲跌% × 成交額)近似主力淨流向傾向。
    回傳 (proxy_str, ok, signed_yi)；ok=False 表示雙源皆不可達,signed_yi=None。
    [V24.19] 第三元素為【可計算數值】,供 _flow5_with_manual 回退鏈使用。
    誠實化：明確標註『代理』，絕不與東財主力大單淨額混淆。
    """
    try:
        import re as _re_vp
        raw = None
        try:  # ① 新浪(主源)
            url = f"https://hq.sinajs.cn/list={proxy}"
            headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"}
            r = _em_throttled_get(url, timeout=5, tag="em2", headers=headers)
            if r is not None:
                raw = r.content.decode("gbk", errors="replace")
        except Exception as _e:
            _log_err(_e, "_volume_flow_proxy_sina")
        if not raw:  # ② 騰訊(備援)
            try:
                import urllib.request as _uq
                req = _uq.Request(f"https://qt.gtimg.cn/q={proxy}",
                                  headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com"})
                resp = _uq.urlopen(req, timeout=5)
                raw = resp.read().decode("gbk", errors="replace")
            except Exception as _e:
                _log_err(_e, "_volume_flow_proxy_tencent")
        if not raw:
            return None, False, None
        m = _re_vp.search(r'="([^"]+)"', raw)
        if not m:
            return None, False, None
        parts = m.group(1).split(",") if "," in raw else m.group(1).split("~")
        if len(parts) < 10:
            return None, False, None
        prev = float(parts[2]); price = float(parts[3])
        amt = float(parts[9])
        if prev <= 0 or price <= 0:
            return None, False, None
        chg = (price - prev) / prev * 100
        amt_yi = amt / 1e8
        # [V24.19.2] ★盤前/停牌守衛。根因(使用者 2026-08-10 09:06 盤前報暴露):
        #   09:30 前新浪即時報價尚無當日成交 → amt≈0、chg≈0 → 代理算出 +0.0億,
        #   而報告顯示「💰5日+0.0億⚪」會被讀成【主力持平】,實際是【還沒開盤沒有數據】。
        #   **0.0 又一次同時代表「真持平」與「無數據」——與 get_sina_proxy_ret 同一種
        #   語意重疊,我在 V24.14 才修過一次,這裡自己又犯。**
        #   處置:成交額趨近 0 視為無效,回 None(讓下游顯示「未取得」而非偽裝的 0)。
        if amt_yi < 0.01:
            return None, False, None
        if chg > 0.3 and amt_yi > 3:
            icon, dirn, note = "🔴", "淨流入傾向", "漲+放量"
        elif chg > 0.3:
            icon, dirn, note = "🟠", "價漲量平", "漲但量未放大"
        elif chg < -0.3 and amt_yi > 3:
            icon, dirn, note = "🟢", "淨流出傾向", "跌+放量"
        elif chg < -0.3:
            icon, dirn, note = "🟡", "價跌量平", "跌但量未放大"
        else:
            icon, dirn, note = "⚪", "窄幅觀望", "波動小"
        # [V24.19] 額外回傳【可計算的數值】。根因:本函式原本只回顯示字串,
        #   所以量價代理只能出現在畫面上,凡是吃數值的地方(歸因表主力欄、
        #   反轉三條件①機構回補、⑤資金流閘門)全都拿不到 → 東財封鎖後那些永遠是 0/❌。
        #   ★誠實化:這【不是】主力淨額,是「方向×量能×幅度」的傾向強度,單位借用億但語意不同,
        #   所以 src 一律標「📊量價代理」,且 is_proxy=True 供下游決定要不要用。
        _mag = min(1.0, abs(chg) / 2.0)          # 漲跌幅強度,±2% 封頂
        _signed = (1 if chg > 0 else -1 if chg < 0 else 0) * amt_yi * _mag
        return (f"{icon}量價代理【{dirn}】{chg:+.2f}%·成交{amt_yi:.2f}億({note})", True,
                round(_signed, 2))
    except Exception as _e:
        _log_err(_e, "_volume_flow_proxy")
        return None, False, None


def record_intraday_compare(report_type="收盤"):
    """[V22.23.9 場內對照] 抓 INTRADAY_PAIRS 各場內實時報價, 寫 intraday_compare 表。
    轉場內鋪路: 攢「場內實時 vs 場外」同底層對照數據, 量測時點差。
    場外當日真實報酬日後由 predictions.actual_ret 按(日期,otc)join進來分析。
    僅記錄不進compute_order、不動持倉。失敗靜默不影響主報告。
    回傳套用檔數。
    """
    n = 0
    try:
        import sqlite3 as _sq
        from datetime import datetime as _dt
        with _sq.connect(LOG_DB, timeout=10) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS intraday_compare("
                "ts TEXT, intraday_code TEXT, otc_code TEXT, name TEXT, "
                "intraday_price REAL, intraday_ret REAL, held INTEGER, report_type TEXT)")
            for icode, pair in INTRADAY_PAIRS.items():
                q = fetch_intraday_quote(icode)
                if not q:
                    print(f"   [場內對照] {icode} 抓取失敗(可能VPN/休市), 跳過")
                    continue
                conn.execute(
                    "INSERT INTO intraday_compare VALUES(?,?,?,?,?,?,?,?)",
                    (_dt.now().strftime("%Y-%m-%d %H:%M:%S"), icode, pair["otc"],
                     q["name"], round(q["price"], 4), round(q["ret"], 4),
                     1 if pair.get("held") else 0, report_type))
                _hmark = "持倉" if pair.get("held") else "觀察"
                print(f"   [場內對照] {icode}({q['name']}) 實時{q['price']:.3f} "
                      f"{q['ret']*100:+.2f}% ↔ 場外{pair['otc']}({_hmark})")
                n += 1
            conn.commit()
    except Exception as _e:
        _log_err(_e, "record_intraday_compare")
    return n




def format_order_flow_block():
    """[V22.24.4] 資金流大小單鏈路狀態區塊(顯性化到HTML, 不再只console)。
    遍歷持倉proxy, 取大小單判定(緩存復用不重複抓), 輸出三態:
      🔴機構吃貨/🚩誘多派發/➡️中性(接口通) / ❌抓取失敗(接口/VPN未通)。
    讓防誘多核心武器在報告可見、可驗證接口狀態。失敗靜默。
    """
    lines = ["\n## 💴 資金流情報維度（V22.25 鯨吞蠶食探測 · 場內代理ETF主力大單趨勢）\n"]
    ok = fail = 0
    ok_proxy = 0     # [V23.8] 量價代理降級成功檔數
    ok_fallback = 0   # [V22.53] DB 回退與即時成功分開計
    try:
        for code in USER_HOLDINGS:
            info = TARGET_ETFS.get(code)
            if not info:
                continue
            proxy = info.get("proxy")
            if not proxy:
                continue  # 台股無push2大小單
            nm = info.get("name", code)[:10]
            bd = get_order_flow_breakdown(proxy)
            if bd is not None:
                ok += 1
                persist_capital_flow(code, nm, bd)   # [V22.25.0] 抓到就存DB→沉澱歷史
                主力 = bd.get("主力") or 0
                _icon = "🔴" if 主力 > 0.05 else "🚩" if 主力 < -0.05 else "➡️"
                _dir = "主力淨流入" if 主力 > 0.05 else "主力淨流出" if 主力 < -0.05 else "主力中性"
                today_str = f"{_icon} 今日{_dir} {主力:+.2f}億"
            else:
                # [V22.27.7 FixA] 盤後API限流→從DB讀今日數據(午報已存)
                bd_db = _read_today_flow_from_db(code)
                if bd_db is not None:
                    ok_fallback += 1   # [V22.53 治WB指出的謊] DB舊值不得計入即時成功
                    主力 = bd_db.get("主力") or 0
                    _icon = "🔴" if 主力 > 0.05 else "🚩" if 主力 < -0.05 else "➡️"
                    _dir = "主力淨流入" if 主力 > 0.05 else "主力淨流出" if 主力 < -0.05 else "主力中性"
                    today_str = f"{_icon} 今日{_dir} {主力:+.2f}億 (DB fallback)"
                else:
                    # [V23.8] 東財主力大單結構性封鎖→量價代理降級(新浪/騰訊可達)
                    _px, _ok, _pv = _volume_flow_proxy(proxy)   # [V24.19] _pv=可計算數值
                    if _ok:
                        ok_proxy += 1
                        today_str = f"📊 {_px}（東財主力大單不可得·量價代理替代）"
                    else:
                        fail += 1
                        today_str = "❌ 今日抓取失敗(東財限流+DB無今日+量價代理亦失敗)"
            # [V22.25.0] 多日趨勢從DB讀——限流時今日缺一格, 歷史趨勢照在(斷不掉的情報)
            tr = read_capital_flow_trend(code, days=10)
            if tr:
                _lag = " ⭐量增價平(代理ETF量增·NAV未跟上;場內需求領先參考,非板塊吸籌驗證)" if tr["price_lag"] else ""
                lines.append(
                    f"- {code} {nm}：{today_str}\n"
                    f"  └ 📈{tr['n']}日趨勢：{tr['verdict']}（連續淨流入{tr['days_in']}日，"
                    f"累計主力{tr['cum']:+.2f}億"
                    + (f"，累計NAV{tr['cum_nav']:+.2f}%" if tr['cum_nav'] is not None else "")
                    + f"）{_lag}")
            else:
                lines.append(f"- {code} {nm}：{today_str}　（趨勢需≥3日DB歷史，積累中）")
        if ok == 0 and ok_proxy > 0 and fail == 0:
            lines.append(f"\n> 📊 東財主力大單(fflow)結構性封鎖, 已用量價代理替代 {ok_proxy} 檔"
                         f"(場內代理ETF即時量價→淨流向傾向)。此為『量價近似』非『主力大單』,"
                         f"僅示方向, 不得等同東財主力淨額。\n")
        elif ok == 0 and fail > 0:
            lines.append(f"\n> ⚠️ push2大小單接口抓取失敗（{fail}檔）"
                         f"——已帶ut權杖; 疑東財限流或VPN中斷, 需WorkBuddy確認。"
                         f"其中 {ok_proxy} 檔已用量價代理替代, 其餘待恢復。目前防誘多武器未生效。\n")
        elif ok == 0 and ok_fallback > 0:
            # [V22.53 治WB指出的謊] 原本 DB 回退也計入 ok → 斷線期報告仍印「✅接口已通」。
            #   實證(0722收盤報):11檔全 (DB fallback) 卻寫「✅ 接口已通(11檔成功)」。
            lines.append(f"\n> ⬜ **即時抓取 0 檔 ｜ {ok_fallback} 檔為 DB 舊值回退(非即時)**"
                         f" → 資金流維度**待確認**;下列方向僅供趨勢參考,"
                         f"**不可作為今日決策依據**。\n")
        elif ok > 0:
            _fb = f"(即時{ok}檔" + (f", 另{ok_fallback}檔DB回退⚠️非即時" if ok_fallback else "") + ")"
            lines.append(f"\n> ✅ 接口已通{_fb}。本區僅示主力『方向』"
                         f"(機構≡主力為東財定義, 散戶為其補數·非獨立信號)。\n"
                         f"> 🧭 **口徑誠實化(V22.25.14)**：本資金流為『場內代理ETF的大單淨額』"
                         f"(機構/大戶買賣該ETF份額), **非**板塊成分股主力淨買入之驗證值——"
                         f"代理ETF流向與成分股主力流向可背離(004432教訓)。故『🐋鯨吞蠶食』僅為"
                         f"場內需求/情緒之**領先參考**, 非板塊吸籌鐵證。\n"
                         f"> ⚖️ **權重紀律(E-LOG#134)**：資金流(多日尺度)用於『方向偏移』, "
                         f"**不得**否決同日macro/技術的下行風險(單日尺度)——兩者時間尺度不同。\n"
                         f"> ⚠️ 單日淨額≡主力方向、已被EV其他管道反映; EV調整只在【價量背離】"
                         f"(價漲主力撤/價跌主力吸)時觸發, 見各ETF明細。"
                         f"真·日內誘多(早盤砸大單→午後派發)需分鐘級時序, 留V22.19+。\n")
        return "".join(f"{ln}\n" for ln in lines) if (ok or fail) else ""
    except Exception as _e:
        _log_err(_e, "format_order_flow_block")
        return ""


# ══════════════════════════════════════════════════════════════════════
# [V22.24.0→.9 因果鏈 L2] 資金流大小單拆解 — Gemini盲點①: 防「誘多假強流入」
#   病根: 只看「淨流入總額」會被主力「左手倒右手」騙(早盤砸大單→午後派發殺豬盤)。
#   認知收尾(E-LOG#121): 單日大小單淨額 ≡ 主力方向——機構(超大+大)≡主力(東財定義恆等),
#     散戶(中+小)為其補數, 非獨立信號。故純『方向』調EV=重複計算(主力已在5日流入/EV)。
#   [V22.24.9 改] judge_order_flow 只在【價量背離】動EV(價漲主力撤/價跌主力吸),
#     此為單日資料能合法提供的唯一新資訊; 價量同向則EV不動。
#   接口[V22.24.5根治]: fflow/daykline f52主力/f53小/f54中/f55大/f56超大 淨流入(元);
#     非舊stock/get的f62/f66/f72/f78/f84(那批字段在資金流語境全錯, 見get_order_flow_breakdown)。
#   真·日內誘多(分鐘級時序)留V22.19+。抓不到靜默回None不拖累主報告。
_ORDERFLOW_CACHE = {}


def get_order_flow_breakdown(proxy):
    """[V22.24.5 根治] 抓場內代理的大小單拆解。
    🐛修(V22.24.4午報揪出): 原用 stock/get 接口 + f62/f66/f72/f78/f84 字段全錯——
       f62在資金流語境是「收盤價」非主力;stock/get是行情快照非資金流接口,
       取到的是成交量額(大單全0+中小單幾百億荒謬)。
    改: 復用已驗證可通的 fflow/daykline 接口(同get_etf_5day_flow),
       klines每行=日期,主力(f52),小單(f53),中單(f54),大單(f55),超大單(f56) [元]。
    sanity防呆: 單ETF日資金流不可能超±50億, 超過判數據異常回None(自動抓未來字段錯配)。
    回傳 {主力, 超大單, 大單, 中單, 小單}(億元) 或 None(失敗/異常靜默)。
    """
    secid = _proxy_to_secid(proxy)
    if not secid:
        return None
    if proxy in _ORDERFLOW_CACHE:
        return _ORDERFLOW_CACHE[proxy]
    try:
        import time as _t_of
        import json as _json_of
        # [V22.24.10] 先查共享daykline快取(5日資金流可能已抓過同secid)→命中則不重抓
        klines = _DAYKLINE_CACHE.get(secid)
        if klines is None:
            ts = int(_t_of.time() * 1000)  # 防快取時間戳
            url = ("https://push2his.eastmoney.com/api/qt/stock/fflow/kline/get"
                   f"?secid={secid}&fields1=f1,f2,f3,f7"
                   f"&fields2=f51,f52,f53,f54,f55,f56,f57"
                   f"&klt=101&lmt=10&ut=b2884a393a59ad64002292a3e90d46a5&_={ts}")
            headers = {"User-Agent": "Mozilla/5.0",
                       "Referer": "https://data.eastmoney.com"}
            r = _em_throttled_get(url, timeout=6, tag="em3", headers=headers)  # [V22.44]
            if r is None: return None
            data = _json_of.loads(r.text).get("data") or {}
            klines = data.get("klines", [])
            _DAYKLINE_CACHE[secid] = klines  # 存快取供5日資金流復用(失敗存[]避免重試)
        if not klines:
            # [V22.24.9修🟡] 區分「連到但回空」vs「連線失敗(except)」——後者才疑VPN。
            print(f"   [大小單拆解] {proxy} 接口回空(已帶ut; 疑東財限流/該票無資金流數據, 非VPN)")
            return None
        parts = klines[-1].split(",")  # 最新一日
        if len(parts) < 6:
            return None
        # [V22.24.7 診斷] 打印原始klines各列, 供確認「散戶」是真數據還是-機構artifact
        #   (午報發現大單=中小單精確鏡像, 疑散戶半為artifact, 需raw值定真偽)
        try:
            _raw = [round(float(parts[i]) / 1e8, 3) for i in range(1, 6)]
            print(f"   [大小單raw] {proxy} parts[1..5](億)={_raw} "
                  f"| sum[2..5]={sum(_raw[1:]):+.3f}(若≈0則四列和為零)")
        except (ValueError, IndexError):
            pass
        # [V22.24.6 列序自校驗] klines子列順序有歧義(字段定義 vs 實測"順序錯位"相反),
        #   若搞反, 機構/散戶判定會完全顛倒(防誘多信號反向=比沒有更危險)。
        #   解法: 用已驗證的 主力净流入(parts[1], 5日資金流已證對)當ground truth,
        #   東財定義 主力 ≡ 超大單+大單(機構)。比對哪兩列之和≈主力, 自動定真實列序。
        # ⚠️[V22.24.9 誠實註記] 因主力≡大+超大為東財定義, 候選A(c4+c5)幾乎恒勝、機構≡主力
        #   近乎恆等——本校驗主要防「列序顛倒」, 非「散戶獨立性」之驗證(散戶為主力補數)。
        主力 = float(parts[1]) / 1e8
        c2, c3, c4, c5 = (float(parts[i]) / 1e8 for i in (2, 3, 4, 5))
        # 候選A(我原映射): [4]大+[5]超大=機構, [2]小+[3]中=散戶
        # 候選B(實測順序錯位): [2]超大+[3]大=機構, [4]中+[5]小=散戶
        errA = abs(主力 - (c4 + c5))
        errB = abs(主力 - (c2 + c3))
        if errA <= errB:
            機構, 散戶 = c4 + c5, c2 + c3   # 候選A
        else:
            機構, 散戶 = c2 + c3, c4 + c5   # 候選B(順序錯位)
        # 一致性兜底: 若兩候選都對不上主力(誤差>主力一半且>0.05), 數據可疑→拒用
        if min(errA, errB) > max(abs(主力) * 0.5, 0.05):
            print(f"   [大小單拆解] {proxy} 主力({主力:+.2f})≠任一機構候選, 列序存疑拒用")
            return None
        # 以「機構/散戶」為準回傳(超大單欄裝機構, 小單欄裝散戶, judge沿用不變)
        bd = {"主力": round(主力, 3),
              "超大單": round(機構, 3), "大單": 0.0,   # 機構合計放超大單欄
              "中單": 0.0, "小單": round(散戶, 3)}     # 散戶合計放小單欄
        # [sanity] 單ETF日資金流超±50億必是數據異常(字段錯配/單位錯)→拒用
        if any(abs(v) > 50 for v in (主力, 機構, 散戶)):
            print(f"   [大小單拆解] {proxy} 數據異常(|淨額|>50億), 拒用防GIGO")
            return None
        _ORDERFLOW_CACHE[proxy] = bd
        return bd
    except Exception as _e:
        _log_err(_e, "get_order_flow_breakdown")
    return None


def _read_today_flow_from_db(code):
    """[V22.27.7 FixA] 從DB讀取今日資金流(盤後API失敗時的fallback)
    回傳bd等價dict(主力/超大單/大單/中單/小單)或None。
    數據來自午報時段persist_capital_flow寫入的capital_flow_daily表。
    """
    try:
        import sqlite3 as _sq
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        with _sq.connect(LOG_DB, timeout=10) as conn:
            row = conn.execute(
                "SELECT main_net,super_net,big_net,mid_net,small_net "
                "FROM capital_flow_daily WHERE code=? AND date=?",
                (code, today)).fetchone()
        if row and any(v is not None for v in row):
            return {"主力": row[0] or 0, "超大單": row[1] or 0,
                    "大單": row[2] or 0, "中單": row[3] or 0,
                    "小單": row[4] or 0}
    except Exception as _e:
        _log_err(_e, "_read_today_flow_from_db:L5327")  # [V24.16] 原為靜默 pass
        pass
    return None


NAV_RET_SANE_ABS = 25.0   # [V22.38] 單日 NAV 報酬合理上限(%);超過視為喂數錯誤

def persist_capital_flow(code, name, bd, nav_ret=None):
    """[V22.38 治WB查出的 nav_ret 污染] DB 實測 min=-695.5% / max=+427%,
    76筆(6%) |nav_ret|>25% 且對應 main_net 極小(如 016008 main_net=0.001 卻 nav_ret=-695%)
    → 典型 proxy 喂數單位/除零錯誤,且在特定日期【聚類】(07-16/07-17/06-30/06-18)非隨機。
    修法:入口攔截,超出合理範圍→寫 None(誠實留空)而非污染值,並告警。"""
    if nav_ret is not None:
        try:
            if abs(float(nav_ret)) > NAV_RET_SANE_ABS:
                print(f"   ⚠️ [資料品質] {code} nav_ret={float(nav_ret):+.1f}% 超出±{NAV_RET_SANE_ABS}% "
                      f"→ 判為喂數錯誤,改存 None(不污染回測樣本)")
                nav_ret = None
        except Exception:
            nav_ret = None
    """[V22.25.0] 把單日資金流五檔淨額存DB(capital_flow_daily)——沉澱成歷史。
    bd: get_order_flow_breakdown回傳(主力/超大單/大單/中單/小單, 億元);
    nav_ret: 當日NAV漲跌%(收盤回填, 用於量價背離)。(code,date)唯一→同日覆蓋。
    限流抓不到時不寫(保留既有歷史), 故歷史趨勢斷不掉。
    """
    if not bd:
        return
    try:
        import sqlite3 as _sq
        from datetime import datetime as _dt
        today = _dt.now().strftime("%Y-%m-%d")
        now = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
        with _sq.connect(LOG_DB, timeout=10) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO capital_flow_daily"
                "(code,date,name,main_net,super_net,big_net,mid_net,small_net,nav_ret,ts)"
                " VALUES(?,?,?,?,?,?,?,?,"
                "COALESCE(?,(SELECT nav_ret FROM capital_flow_daily WHERE code=? AND date=?)),?)",
                (code, today, name, bd.get("主力"), bd.get("超大單"), bd.get("大單"),
                 bd.get("中單"), bd.get("小單"), nav_ret, code, today, now))
    except Exception as _e:
        _log_err(_e, "persist_capital_flow")


def read_capital_flow_trend(code, days=10):
    """[V22.25.0] 從DB讀某檔近N日主力淨流入趨勢——鯨吞蠶食探測器。
    不依賴當下連得上東財; 限流時用歷史照樣看趨勢。
    回傳 {days_in:連續淨流入天數, cum:累計主力淨流入, series:[(date,main,nav_ret)],
          verdict:鯨吞蠶食/溫和流入/派發/震盪, price_lag:量增價平真領先?} 或 None。
    """
    try:
        import sqlite3 as _sq
        with _sq.connect(LOG_DB, timeout=10) as conn:
            rows = conn.execute(
                "SELECT date,main_net,nav_ret FROM capital_flow_daily "
                "WHERE code=? ORDER BY date DESC LIMIT ?", (code, days)).fetchall()
        if not rows or len(rows) < 3:
            return None
        rows = rows[::-1]  # 轉成時間正序
        series = [(d, m, nr) for d, m, nr in rows]
        mains = [m for _, m, _ in series if m is not None]
        if not mains:
            return None
        cum = round(sum(mains), 2)
        # 連續淨流入天數(從最近往回數)
        days_in = 0
        for _, m, _ in reversed(series):
            if m and m > 0:
                days_in += 1
            else:
                break
        # 量價背離: 累計(代理ETF)主力淨流入為正, 但累計NAV漲幅很小 → 量增價平
        #   = 場內需求領先參考(非板塊吸籌鐵證); 權重見 #134: 定方向偏移,不否決同日macro
        navs = [nr for _, _, nr in series if nr is not None]
        cum_nav = round(sum(navs), 2) if navs else None
        price_lag = (cum > 0 and cum_nav is not None and abs(cum_nav) < 1.0)
        # 判定
        pos_days = sum(1 for _, m, _ in series if m and m > 0)
        if days_in >= 3 and cum > 0:
            verdict = "🐋鯨吞蠶食(場內代理連續淨流入)"
        elif pos_days >= len(series) * 0.6 and cum > 0:
            verdict = "🟢溫和流入(多數日淨流入)"
        elif cum < 0 and pos_days <= len(series) * 0.4:
            verdict = "🚩派發(多數日淨流出)"
        else:
            verdict = "➡️震盪(無明顯方向)"
        return {"days_in": days_in, "cum": cum, "cum_nav": cum_nav,
                "series": series, "verdict": verdict, "price_lag": price_lag,
                "n": len(series)}
    except Exception as _e:
        _log_err(_e, "read_capital_flow_trend")
        return None


# 觀察池進場條件閾值
WATCHLIST_ENTRY_RSI_MAX = 45.0        # RSI ≤ 45 (相對冷卻區)
WATCHLIST_ENTRY_EV_MIN = 50           # EV ≥ 50 (系統最低信心)
WATCHLIST_ENTRY_REGIME = "trending_up" # 必須為上行趨勢市

# ══ 地緣調整完整表（V22.8修復：補全Dividend/Cyclical/Utility） ══
GEO_ADJ_TABLE = {
    "Defensive": {"closed_hostile":1.25,"conditional_open":1.05,"full_open":1.0},
    "Dividend":  {"closed_hostile":1.05,"conditional_open":1.02,"full_open":1.0},
    "Cyclical":  {"closed_hostile":0.90,"conditional_open":1.03,"full_open":1.05},
    "Utility":   {"closed_hostile":0.92,"conditional_open":0.95,"full_open":1.0},
    "Tech":      {"closed_hostile":0.88,"conditional_open":0.95,"full_open":1.0},
}

# ══ Style敘事庫 ══
GEO_NARRATIVES = {
    "Defensive":  "地緣風險邊際降溫，避險資金邊際流出黃金，但實物需求仍構成底部支撐。",
    "Dividend":   "風險邊際降溫，紅利溢價收窄，但無風險利率下行支撐高股息策略持續。",
    "Cyclical":   "地緣邊際緩解，有色金屬受益於需求恢復預期，但美元走向構成不確定性。",
    "Utility":    "能源壓力邊際緩解，電力成本壓力減輕，公用事業估值修復窗口開啟。",
    "Tech":       "地緣邊際降溫，霍爾木茲溢價收縮，A股科技景氣修復預期升溫，但出口管制壓力仍構成外部約束。",
    "Tech_QDII":  "地緣邊際降溫，科技風險偏好回升，美股科技估值修復窗口重啟，NASDAQ100領漲動能蓄積中。",
    "Tech_TW":    "地緣邊際降溫，科技股情緒修復，台積電供應鏈恢復邏輯重啟，但終端需求仍待驗證。",
}
RSI_NARRATIVES = {
    "overbought": "RSI進入超買區，高位震盪風險加劇；乖離率偏高，估值溢價已部分定價。",
    "hot":        "RSI向上延伸至偏熱區，上方空間逐步縮窄；均線上方運行，技術格局偏多。",
    "neutral":    "RSI運行於合理區間，方向尚待確認；均線上方運行，技術格局偏多。",
    "cool":       "RSI偏離中性，價格防線正在重建；輕微低於均線，短期支撐仍有效。",
    "oversold":   "RSI急劇收縮至超賣區，技術性修復動能集結；輕微低於均線，短期支撐仍有效。",
    "z_warn":     "Z-Score穿透顯示統計顯著偏離，均值回歸壓力上升。",
}

# ══════════════════════════════════════════════════════════════════════
# 🌐 DirectDataFetcher — Gemini V22.7直連引擎（保留，修復Sina bug）
# ══════════════════════════════════════════════════════════════════════
class DirectDataFetcher:

    @staticmethod
    def get_yahoo_prices(code: str) -> list:
        """台股/美股直連Yahoo Finance API"""
        try:
            url = f"https://query2.finance.yahoo.com/v8/finance/chart/{code}?range=90d&interval=1d"
            r = _yahoo_get(url, timeout=8)  # [V22.48] 代理預檢+快取回退
            if r is None: return []
            closes = r.json()['chart']['result'][0]['indicators']['quote'][0]['close']
            return [float(c) for c in closes if c is not None]
        except Exception as e:
            print(f"   ⚠️ Yahoo直連失敗 ({code}): {e}")
            return []

    @staticmethod
    def get_yahoo_dated_closes(code: str) -> list:
        """[(交易日 ISO, 收盤價)] 升序。日期取自 Yahoo 的 timestamp + 交易所時區偏移,
        不靠執行日推算(休市日跑時,最後一筆是前一交易日,不能標成今天)。失敗回 []。"""
        try:
            url = f"https://query2.finance.yahoo.com/v8/finance/chart/{code}?range=1mo&interval=1d"
            r = _yahoo_get(url, timeout=8)
            if r is None: return []
            res = r.json()['chart']['result'][0]
            off = int(res.get('meta', {}).get('gmtoffset') or 0)
            closes = res['indicators']['quote'][0]['close']
            out = []
            for t, c in zip(res.get('timestamp') or [], closes):
                if c is None: continue
                d = datetime.datetime.fromtimestamp(int(t) + off, datetime.timezone.utc).date().isoformat()
                out.append((d, float(c)))
            return out
        except Exception as e:
            _log_err(e, f"get_yahoo_dated_closes_{code}")
            return []

    @staticmethod
    def get_eastmoney_nav(code: str) -> list:
        """
        A股場外NAV直連天天基金底層JS
        免疫AkShare套件欄位變更（V22.7核心創新）
        """
        try:
            url = f"http://fund.eastmoney.com/pingzhongdata/{code}.js"
            headers = {"User-Agent":"Mozilla/5.0",
                       "Referer":f"http://fund.eastmoney.com/{code}.html"}
            r = _em_throttled_get(url, timeout=8, tag="em4", headers=headers)  # [V22.44]
            if r is None: return None
            m = re.search(r'Data_netWorthTrend\s*=\s*(\[.*?\]);', r.text)
            if m:
                data = json.loads(m.group(1))
                return [float(item['y']) for item in data if 'y' in item]
        except Exception as e:
            print(f"   ⚠️ 天天基金直連失敗 ({code}): {e}")
        return []

    @staticmethod
    def get_sina_dual_proxy_ret(proxy1: str, proxy2: str) -> tuple[float, str]:
        """
        [V22.10] 雙代理取平均值（用於018177增強型基金）
        同時取兩個ETF即時漲跌幅，取均值降低誤差
        """
        r1 = DirectDataFetcher.get_sina_proxy_ret(proxy1)
        r2 = DirectDataFetcher.get_sina_proxy_ret(proxy2) if proxy2 else None

        # [V24.14] 失效判定改用 is None,不再用 abs()<1e-6。
        #   原判定把【板塊真實收平(0.00%)】誤判成「代理失效」——0.0 語意重疊。
        if r1 is None and r2 is None:
            return None, "雙代理均失效"
        elif r1 is None:
            return r2, f"主代理失效→備用{proxy2}"
        elif r2 is None:
            return r1, f"備用失效→主代理{proxy1}"
        else:
            avg = (r1 + r2) / 2
            diverge = abs(r1 - r2)
            if diverge > 0.005:  # 兩代理差>0.5%時警示
                return avg, f"⚠️雙代理分歧{diverge:.2%}→取均值"
            return avg, f"雙代理均值(分歧{diverge:.2%})"

    @staticmethod
    def get_sina_proxy_ret(proxy: str) -> float:
        """
        [V22.11 BUGFIX] 單代理 Sina 實時漲跌幅
        舊版重構時遺漏此方法，導致所有 A股 ETF 代理崩潰
        修復：從新浪財經 hq.sinajs.cn 抓即時報價，計算 (今價-昨收)/昨收
        """
        try:
            url = f"https://hq.sinajs.cn/list={proxy}"
            headers = {"User-Agent": "Mozilla/5.0",
                       "Referer": "https://finance.sina.com.cn"}
            r = _em_throttled_get(url, timeout=5, tag="em5", headers=headers)  # [V22.44]
            if r is None: return None
            raw = r.content.decode("gbk", errors="replace")
            m = re.search(r'"([^"]+)"', raw)
            if m:
                parts = m.group(1).split(",")
                if len(parts) > 3:
                    close_y = float(parts[2])  # 昨收
                    close_t = float(parts[3])  # 今價
                    if close_y > 0.01 and close_t > 0.01:
                        return (close_t - close_y) / close_y
        except Exception as _e:
            _log_err(_e, "get_sina_proxy_ret")
        # [V24.14] 失敗語意單一化:解析失敗/欄位不足/價格<=0.01 一律 None。
        #   原本 return 0.0 → 被消費端當成「今日 0% 波動」(取數失敗偽裝成真實收平)。
        #   0.0 從此【只】由 (close_t-close_y)/close_y 真的算出 0 時產生。
        return None

    @staticmethod
    def get_index_kline_sina(symbol: str, datalen: int = 30):
        """[V22.94] 抓指數日K(收盤報「由黑翻紅研判」用:算 ma10 / 今日最低 / 前低)。
        源: money.finance.sina.com.cn getKLineData。回 [{"day","open","high","low","close","ma_price10"},...]。
        ⚠️ 沙盒外網測過可連(2026-07-30);用戶 live 端 _em_throttled_get 含重試,失敗回 None(保守不誤判)。"""
        try:
            url = (f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
                   f"CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=10&datalen={datalen}")
            r = _em_throttled_get(url, timeout=8, tag="em5")
            if r is None:
                return None
            arr = r.json()
            if not isinstance(arr, list):
                return None
            out = []
            for d in arr:
                _ma = d.get("ma_price10")
                out.append({
                    "day": d.get("day"),
                    "open": float(d.get("open", 0) or 0),
                    "high": float(d.get("high", 0) or 0),
                    "low": float(d.get("low", 0) or 0),
                    "close": float(d.get("close", 0) or 0),
                    "volume": float(d.get("volume", 0) or 0),   # [V23.4 item6] 抽出量(sina本就回,原碼漏抽)
                    "ma_price10": (float(_ma) if _ma not in (None, "", 0) else None),
                })
            return out
        except Exception as _e:
            _log_err(_e, "get_index_kline_sina")
            return None

    @staticmethod
    def get_etf_5day_flow(proxy: str) -> dict | None:
        """
        [V22.18.2 修訂] 抓 ETF 代理近5日主力淨流入（情報輸入層）
        數據源：東方財富 push2his **fflow/kline**（[V22.51] 由 daykline 遷移）
        ★2026-07-22 使用者本機分層實測(diag_eastmoney3/4):東財【單獨封鎖 fflow/daykline 這一支】,
          同一台主機的 kline/stock/get/quote首頁/天天基金 全部 HTTP 200 正常。
          → 不是 IP 被封、不是 TLS 指紋、不是 header、不是 token、不是限頻(六個假設全被證偽)。
          實測可用:push2his /fflow/kline(5筆) > push2delay /fflow/daykline(1筆) > push2 /fflow/kline(1筆)
          回傳格式與 daykline 完全相同 → 解析算法不需更動。
        關鍵修訂(基於真實返回校準):
          - 加 _= 時間戳參數(東財防快取,缺它可能返回空→收盤報1911顯示'—'根因)
          - 自適應數量級:回傳 scale 標記('etf'億級/'index'百億級)供背離閾值校準
          - 欄位確認:klines每行 日期,主力淨流入(元),...,當日漲跌% (實測驗證)
        回傳 {flow5_yi, flow5_list, signal, trend, scale} 或 None（失敗）
        """
        if not proxy or len(proxy) < 3:
            return None
        if proxy in _FLOW_CACHE:   # [V22.18.4] run-level快取
            return _FLOW_CACHE[proxy]
        prefix = proxy[:2].lower()
        digits = proxy[2:]
        if prefix == "sh":
            secid = f"1.{digits}"
        elif prefix == "sz":
            secid = f"0.{digits}"
        else:
            return None
        try:
            import time as _t
            ts = int(_t.time() * 1000)  # [修訂]13位毫秒時間戳,東財防快取必需
            # [V22.24.10] 先查共享daykline快取(大小單可能已抓同secid)→命中不重抓
            klines = _DAYKLINE_CACHE.get(secid)
            if klines is None:
                # [V22.52] ★多端點備援鏈 —— 原本只試一條,失敗就放棄。
                #   2026-07-22 實測:daykline 被封,但下列三條可通(diag_eastmoney4):
                #     push2his/fflow/kline(5筆) / push2delay/fflow/daykline(1筆) / push2/fflow/kline(1筆)
                #   且 lmt=5 實測可通、系統原用 lmt=10 失敗 → 逐字比對後【lmt 是唯一差異】,
                #   故 lmt 改 5 並逐一嘗試候選,任一成功即用(不再單押一條)。
                klines = []
                for _h, _pth, _lmt in EM_FFLOW_CANDIDATES:
                    _u = (f"https://{_h}/api/qt/stock/{_pth}/get"
                          f"?secid={secid}&fields1=f1,f2,f3,f7"
                          f"&fields2=f51,f52,f53,f54,f55,f56,f57"
                          f"&klt=101&lmt={_lmt}&ut=b2884a393a59ad64002292a3e90d46a5&_={ts}")
                    _r = _em_throttled_get(_u, timeout=6,
                                           tag=f"flow5:{secid}@{_h.split('.')[0]}/{_pth.split('/')[-1]}",
                                           retries=2)
                    if _r is None:
                        continue
                    try:
                        _d = _r.json().get("data", {}) or {}
                        _k = _d.get("klines", []) or []
                    except Exception:
                        _k = []
                    if _k:
                        klines = _k
                        print(f"   ✅ [flow5] {secid} 由 {_h.split('.')[0]}/{_pth} lmt={_lmt} 取得 {len(_k)} 筆")
                        break
                if not klines:
                    print(f"   ❌ [flow5] {secid} 全部 {len(EM_FFLOW_CANDIDATES)} 個備援端點皆失敗 → 落 NULL")
                _DAYKLINE_CACHE[secid] = klines  # 存快取供大小單復用
            if not klines:
                _FLOW_CACHE[proxy] = None   # [V22.18.5]失敗也快取,避免同run重試
                return None
            recent = klines[-5:]
            flow_list = []
            for line in recent:
                parts = line.split(",")
                if len(parts) >= 2:
                    try:
                        flow_list.append(float(parts[1]) / 1e8)  # 元→億
                    except (ValueError, IndexError):
                        continue
            if not flow_list:
                _FLOW_CACHE[proxy] = None
                return None
            flow5 = sum(flow_list)
            # [V22.18.2] 自適應數量級:百億級=指數/板塊, 億級=ETF自身
            abs_max = max(abs(x) for x in flow_list)
            scale = "index" if abs_max >= 10 else "etf"
            # 燈號閾值隨數量級調整
            sig_thr = 20 if scale == "index" else 2
            if flow5 > sig_thr:      signal = "🟢 5日主力強流入"
            elif flow5 > 0:          signal = "🟡 5日主力小流入"
            elif flow5 > -sig_thr:   signal = "🟠 5日主力小流出"
            else:                    signal = "🔴 5日主力強流出"
            pos_days = sum(1 for x in flow_list if x > 0)
            neg_days = sum(1 for x in flow_list if x < 0)
            if pos_days >= 4:   trend = f"連流入{pos_days}/5日"
            elif neg_days >= 4: trend = f"連流出{neg_days}/5日"
            else:               trend = f"流入{pos_days}/流出{neg_days}日"
            _result = {
                "flow5_yi": round(flow5, 2),
                "flow5_list": [round(x, 2) for x in flow_list],
                "signal": signal,
                "trend": trend,
                "scale": scale,
            }
            _FLOW_CACHE[proxy] = _result   # [V22.18.4] 存快取
            return _result
        except Exception as _e:
            _log_err(_e, "get_etf_5day_flow")
            _FLOW_CACHE[proxy] = None   # [V22.18.5]失敗也快取,避免同run重試
            return None

    @staticmethod
    def get_vix() -> float | None:
        """[V22.25.5fix P0] VIX 直連 — 失敗返回 None 而非硬編碼 18.0"""
        try:
            r = _yahoo_get(
                "https://query2.finance.yahoo.com/v8/finance/chart/%5EVIX?range=1d&interval=1d",
                headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
            closes = r.json()['chart']['result'][0]['indicators']['quote'][0]['close']
            return round(closes[-1], 2) if closes else None
        except Exception as _e:
            _log_err(_e, "DirectDataFetcher.get_vix")
            return None

    @staticmethod
    def get_ndx(days: int = 5) -> float | None:
        """[V22.33.3] 先走單一真值源(釘選+落盤,四報一致);失敗回退原直抓(不regress)。"""
        try:
            _rec = get_pinned_anchor("%5ENDX")
            if _rec.get("ret") is not None:
                return _us_ret_outlier_guard(_rec["ret"], "NDX")
        except Exception as _e:
            _log_err(_e, "DirectDataFetcher.get_ndx.pinned")
        try:
            r = _yahoo_get(
                f"https://query2.finance.yahoo.com/v8/finance/chart/%5ENDX?range={days}d&interval=1d",
                headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
            closes = [c for c in
                      r.json()['chart']['result'][0]['indicators']['quote'][0]['close']
                      if c is not None]
            _ret = (closes[-1] - closes[-2]) / closes[-2] if len(closes) >= 2 else None
            return _us_ret_outlier_guard(_ret, "NDX")   # [V22.27.9] ±20%離群防呆
        except Exception as _e:
            _log_err(_e, "DirectDataFetcher.get_ndx")
            return None

    @staticmethod
    def get_sox() -> float | None:
        """[V22.33.3] 先走單一真值源(釘選+落盤,四報一致);失敗回退原直抓(不regress)。"""
        try:
            _rec = get_pinned_anchor("%5ESOX")
            if _rec.get("ret") is not None:
                return _us_ret_outlier_guard(_rec["ret"], "SOX")
        except Exception as _e:
            _log_err(_e, "DirectDataFetcher.get_sox.pinned")
        try:
            r = _yahoo_get(
                "https://query2.finance.yahoo.com/v8/finance/chart/%5ESOX?range=5d&interval=1d",
                headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
            closes = [c for c in
                      r.json()['chart']['result'][0]['indicators']['quote'][0]['close']
                      if c is not None]
            _ret = (closes[-1] - closes[-2]) / closes[-2] if len(closes) >= 2 else None
            return _us_ret_outlier_guard(_ret, "SOX")   # [V22.27.9] ±20%離群防呆
        except Exception as _e:
            _log_err(_e, "DirectDataFetcher.get_sox")
            return None


# ══════════════════════════════════════════════════════════════════════
# 🎯 PrecisionProxyEngine — 精準代理引擎
# [V22.10] 核心新功能：溢折價監控 + 追蹤誤差計算 + 代理品質分級
# ══════════════════════════════════════════════════════════════════════
class PrecisionProxyEngine:
    """
    精準代理引擎 — 把場外基金誤差降到最小

    三大功能：
    1. 溢折價監控：今日ETF市價 vs 昨日NAV
    2. 雙代理驗證：主備代理漲跌幅一致性檢查
    3. 追蹤誤差警示：TE超閾值時降低EV信心
    """

    # 溢折價歷史緩存（當日）
    _premium_cache: dict = {}

    @classmethod
    def get_premium_discount(cls, code: str, nav_prices: list,
                             proxy: str) -> dict:
        """
        計算場內ETF相對場外NAV的溢折價
        溢折價 = (場內即時價 - 昨日NAV) / 昨日NAV

        意義：
        - 溢價>1%：市場情緒亢奮，ETF被高估，NAV次日可能跟漲
        - 折價>1%：市場情緒悲觀，ETF被低估，NAV次日可能補跌
        - 大幅溢折價通常在收盤後1-2天回歸
        """
        if code in cls._premium_cache:
            return cls._premium_cache[code]

        result = {"premium": None, "signal": "", "reliability": ""}

        if not nav_prices or len(nav_prices) < 2 or not proxy:
            return result

        last_nav = nav_prices[-1]  # 昨日NAV（場外）

        # 取場內即時價
        try:
            url = f"https://hq.sinajs.cn/list={proxy}"
            headers = {"User-Agent":"Mozilla/5.0","Referer":"https://finance.sina.com.cn"}
            r = _em_throttled_get(url, timeout=5, tag="em6", headers=headers)  # [V22.44]
            if r is None: return None
            raw = r.content.decode("gbk", errors="replace")
            m = re.search(r'"([^"]+)"', raw)
            if m:
                parts = m.group(1).split(",")
                if len(parts) > 3:
                    etf_price = float(parts[3])  # 場內最新價
                    if etf_price > 0.01 and last_nav > 0.01:
                        premium = (etf_price - last_nav) / last_nav

                        # [V22.24.3 防呆] 場內ETF與場外聯接「份額基準不同」GIGO防護:
                        #   場內ETF價(如518880≈8.9)與場外聯接NAV(如000216≈3.14)是不同
                        #   基金/不同成立面值, 直接相減算溢折價無意義(會得+184%荒謬值)。
                        #   真溢折價需場內價 vs 該ETF自身NAV。基準不同時抑制錯誤信號。
                        #   套利使真ETF溢折價幾乎不超過±5%, |premium|>10%必是基準不匹配。
                        if abs(premium) > 0.10:
                            signal = (f"⚪ 溢折價不可比（場內價{etf_price:.3f} vs "
                                      f"場外聯接NAV{last_nav:.3f}份額基準不同，"
                                      f"需該ETF自身NAV才能算）")
                            result = {"premium": None, "signal": signal,
                                      "etf_price": etf_price, "last_nav": last_nav,
                                      "reliability": "⚪ 基準不匹配,不採用"}
                            cls._premium_cache[code] = result
                            return result

                        # 信號解讀
                        if premium > 0.02:
                            signal = f"🔴 溢價{premium:.1%}（ETF被高估，NAV次日可能補漲）"
                        elif premium > 0.005:
                            signal = f"🟡 輕微溢價{premium:.1%}"
                        elif premium < -0.02:
                            signal = f"🔵 折價{premium:.1%}（ETF被低估，NAV次日可能補跌）"
                        elif premium < -0.005:
                            signal = f"🟡 輕微折價{premium:.1%}"
                        else:
                            signal = f"✅ 溢折價{premium:.2%}（正常範圍）"

                        result = {
                            "premium": round(premium, 5),
                            "signal": signal,
                            "etf_price": etf_price,
                            "last_nav": last_nav,
                            "reliability": "🟢 可信"
                        }
        except Exception as _e:
            _log_err(_e, "unknown")

        cls._premium_cache[code] = result
        return result

    @classmethod
    def validate_dual_proxy(cls, proxy1: str, proxy2: str) -> dict:
        """
        雙代理一致性驗證
        若兩代理漲跌方向相反，說明數據有問題
        """
        if not proxy2:
            return {"valid": True, "note": "單代理（無備用）"}

        r1 = DirectDataFetcher.get_sina_proxy_ret(proxy1)
        r2 = DirectDataFetcher.get_sina_proxy_ret(proxy2)

        if r1 is None or r2 is None:   # [V24.14] 原用 abs()<1e-6,真收平會被誤判無數據
            return {"valid": True, "note": "一個代理無數據", "r1": r1, "r2": r2}

        diverge = abs(r1 - r2)
        same_dir = (r1 > 0) == (r2 > 0)

        if not same_dir:
            return {
                "valid": False,
                "note": f"⚠️ 雙代理方向相反！{proxy1}={r1:+.2%} vs {proxy2}={r2:+.2%}，數據存疑",
                "r1": r1, "r2": r2, "diverge": diverge
            }
        elif diverge > 0.01:
            return {
                "valid": True,
                "note": f"⚠️ 雙代理分歧{diverge:.2%}（較大），取均值",
                "r1": r1, "r2": r2, "diverge": diverge
            }
        else:
            return {
                "valid": True,
                "note": f"✅ 雙代理一致（分歧{diverge:.2%}）",
                "r1": r1, "r2": r2, "diverge": diverge
            }

    @classmethod
    def get_best_proxy_ret(cls, code: str, info: dict,
                           nav_prices: list) -> tuple[float, str, dict]:
        """
        [V22.10 核心] 取最佳代理漲跌幅

        優先級：
        1. 雙代理驗證通過 → 均值
        2. 雙代理方向相反 → 降低信心，取主代理並警示
        3. 主代理失效 → 備用代理
        4. 全部失效 → NAV日間變化（T+1）

        特殊處理：
        - 增強型基金(enhanced) → 強制雙代理取均值，不用NAV
        - QDII → 優先NDX估算
        """
        proxy1 = info.get("proxy")
        proxy2 = info.get("proxy2")
        fund_type = info.get("fund_type", "passive")

        proxy_meta = {
            "proxy_used": proxy1,
            "method": "",
            "warning": "",
            "premium_info": {}
        }

        # 溢折價計算（無論哪種方案都計算）
        if proxy1 and nav_prices:
            prem = cls.get_premium_discount(code, nav_prices, proxy1)
            proxy_meta["premium_info"] = prem

        if not proxy1:
            # 台股：由上層處理
            return 0.0, "TW_Direct", proxy_meta

        # 雙代理驗證
        validation = cls.validate_dual_proxy(proxy1, proxy2)
        proxy_meta["validation"] = validation

        if not validation["valid"]:
            # 方向相反：警示但仍使用主代理
            proxy_meta["warning"] = validation["note"]
            r1 = validation.get("r1", 0.0)
            if abs(r1) > 1e-6:
                proxy_meta["method"] = f"主代理(雙代理異常警示)"
                return r1, "Proxy⚠️", proxy_meta

        # [V22.47 治 018177 代理錯配] ★增強/主動型【優先用自身 NAV】,代理僅作 fallback。
        #   根因實證(2026-07-21):018177(科創50增強A)與 011613(科創50聯接C)設定同一
        #   proxy=sh588000 → 34 交易日中 23 日(67%)日報酬【完全相同】;WB 查到 pred vs 真實
        #   偏差 +9~11pp。下方舊註解寫「增強型代理誤差約 0.1~0.3%/日」——實測差 30~100 倍,
        #   該假設本身就是錯的:增強型有主動 alpha,任何指數代理都無法反映。
        #   ⚠️ 沙盒無法 live 驗證天天基金連線(需 VPN),此處只驗【路徑選擇邏輯】;
        #      實際取數成敗待 WB live 確認(㉖:不宣稱已驗證取數)。
        if code in ENHANCED_FUND_CODES:
            try:
                _nav = _dated_nav(code)
                if _nav and len(_nav) >= 2:
                    _p0, _p1 = _nav[-2][1], _nav[-1][1]
                    if _p0:
                        _r = (_p1 - _p0) / _p0
                        proxy_meta["method"] = f"自身NAV({_nav[-1][0]})"
                        proxy_meta["warning"] = ""
                        return _r, "OwnNAV[Enhanced]", proxy_meta
                print(f"   ⚠️ [增強型] {code} 自身NAV取得失敗 → 退回代理(信號將失真,幅度偏差屬未解釋差異)")
            except Exception as _e:
                _log_err(_e, f"enhanced_own_nav_{code}")

        # 增強型：強制雙代理均值(fallback;⚠️ 對增強型僅為次選,見上)
        if fund_type == "enhanced" and proxy2:
            avg_ret, avg_note = DirectDataFetcher.get_sina_dual_proxy_ret(proxy1, proxy2)
            # [V24.14] 本行原為 abs(avg_ret)>1e-6。dual_proxy 改回 None 後會 abs(None) 崩潰
            #   ——此崩潰點是【本次修改自己引入的】,掃下游時抓到,記錄備查。
            if avg_ret is not None:
                proxy_meta["method"] = f"增強型雙均值: {avg_note}"
                proxy_meta["warning"] = "增強型基金，代理誤差約0.1~0.3%/日"
                return avg_ret, "DualProxy[Enhanced]", proxy_meta

        # 標準：主代理
        r1 = DirectDataFetcher.get_sina_proxy_ret(proxy1)
        if r1 is not None:   # [V24.14] 原 abs(r1)>1e-6:主代理真收平會被當失效而跳到備用
            proxy_meta["method"] = "主代理"
            src = "Proxy+Nav"
            # 若有proxy2且主代理成功，順帶取proxy2做驗證
            if proxy2 and abs(validation.get("diverge", 0)) > 0.005:
                proxy_meta["warning"] = validation.get("note", "")
            return r1, src, proxy_meta

        # 備用代理
        if proxy2:
            r2 = DirectDataFetcher.get_sina_proxy_ret(proxy2)
            if r2 is not None:   # [V24.14] 同上
                proxy_meta["method"] = f"備用代理{proxy2}（主代理{proxy1}失效）"
                return r2, "Proxy2+Nav", proxy_meta

        # NAV歷史（最後手段）
        if nav_prices and len(nav_prices) >= 2:
            ret = (nav_prices[-1] - nav_prices[-2]) / nav_prices[-2]
            proxy_meta["method"] = "NAV歷史（場內代理全部失效）"
            proxy_meta["warning"] = "⚠️ 使用T+1 NAV，數據滯後1天"
            return ret, "NAV_Only", proxy_meta

        return 0.0, "NoData", proxy_meta

    @staticmethod
    def get_sina_proxy_ret(proxy: str) -> float:
        """[V22.25.5fix P1] 恢復死代碼 — Sina實時代理ETF漲跌幅"""
        try:
            url = f"https://hq.sinajs.cn/list={proxy}"
            headers = {"User-Agent":"Mozilla/5.0",
                       "Referer":"https://finance.sina.com.cn"}
            r = _em_throttled_get(url, timeout=5, tag="em7", headers=headers)  # [V22.44]
            if r is None: return None
            raw = r.content.decode("gbk", errors="replace")
            m = re.search(r'"([^"]+)"', raw)
            if m:
                parts = m.group(1).split(",")
                if len(parts) > 3:
                    close_y = float(parts[2])
                    close_t = float(parts[3])
                    if close_y > 0.01 and close_t > 0.01:
                        return (close_t - close_y) / close_y
        except Exception as _e:
            _log_err(_e, "get_sina_proxy_ret")
        # [V24.14] 失敗語意單一化:解析失敗/欄位不足/價格<=0.01 一律 None。
        #   原本 return 0.0 → 被消費端當成「今日 0% 波動」(取數失敗偽裝成真實收平)。
        #   0.0 從此【只】由 (close_t-close_y)/close_y 真的算出 0 時產生。
        return None

    @staticmethod
    def get_ndx(days: int = 5) -> float | None:
        """[V22.33.3] 路由同一 pin(與DirectDataFetcher消除分岔)+補上原缺的±20%離群防呆;失敗回退。"""
        try:
            _rec = get_pinned_anchor("%5ENDX")
            if _rec.get("ret") is not None:
                return _us_ret_outlier_guard(_rec["ret"], "NDX")
        except Exception as _e:
            _log_err(_e, "PrecisionProxyEngine.get_ndx.pinned")
        try:
            r = _yahoo_get(
                f"https://query2.finance.yahoo.com/v8/finance/chart/%5ENDX?range={days}d&interval=1d",
                headers={"User-Agent":"Mozilla/5.0"}, timeout=6)
            closes = [c for c in
                      r.json()['chart']['result'][0]['indicators']['quote'][0]['close']
                      if c is not None]
            _ret = (closes[-1]-closes[-2])/closes[-2] if len(closes)>=2 else None
            return _us_ret_outlier_guard(_ret, "NDX")   # [V22.33.3] 補上原缺的離群防呆
        except Exception as _e:
            _log_err(_e, "PrecisionProxyEngine.get_ndx")
            return None

    @staticmethod
    def get_sox() -> float | None:
        """[V22.33.3] 路由同一 pin(★這就是午報 -4.78% 的來源函式)+補上原缺的±20%離群防呆;失敗回退。"""
        try:
            _rec = get_pinned_anchor("%5ESOX")
            if _rec.get("ret") is not None:
                return _us_ret_outlier_guard(_rec["ret"], "SOX")
        except Exception as _e:
            _log_err(_e, "PrecisionProxyEngine.get_sox.pinned")
        try:
            r = _yahoo_get(
                "https://query2.finance.yahoo.com/v8/finance/chart/%5ESOX?range=5d&interval=1d",
                headers={"User-Agent":"Mozilla/5.0"}, timeout=6)
            closes = [c for c in
                      r.json()['chart']['result'][0]['indicators']['quote'][0]['close']
                      if c is not None]
            _ret = (closes[-1]-closes[-2])/closes[-2] if len(closes)>=2 else None
            return _us_ret_outlier_guard(_ret, "SOX")   # [V22.33.3] 補上原缺的離群防呆
        except Exception as _e:
            _log_err(_e, "PrecisionProxyEngine.get_sox")
            return None

    @staticmethod
    def get_vix() -> float | None:
        """[V22.25.5fix P0] VIX直連 — 失敗返回 None 而非硬編碼 18.0"""
        try:
            r = _yahoo_get(
                "https://query2.finance.yahoo.com/v8/finance/chart/%5EVIX?range=1d&interval=1d",
                headers={"User-Agent":"Mozilla/5.0"}, timeout=6)
            closes = r.json()['chart']['result'][0]['indicators']['quote'][0]['close']
            return round(closes[-1], 2) if closes else None
        except Exception as _e:
            _log_err(_e, "PrecisionProxyEngine.get_vix")
            return None


# ══════════════════════════════════════════════════════════════════════
# 💼 PositionBook — 持倉成本本
# ══════════════════════════════════════════════════════════════════════
class PositionBook:
    _data: dict = {}

    @classmethod
    def load(cls):
        if os.path.exists(POSITION_FILE):
            try:
                with open(POSITION_FILE, encoding='utf-8') as f:
                    cls._data = json.load(f)
            except Exception:
                cls._data = {}

    @classmethod
    def save(cls):
        with open(POSITION_FILE, 'w', encoding='utf-8') as f:
            json.dump(cls._data, f, indent=2, ensure_ascii=False)

    @classmethod
    def get(cls, code): return cls._data.get(code, {})

    @classmethod
    def set_position(cls, code, cost, date_in=None, note=""):
        cls._data[code] = {"cost": cost,
                           "date_in": date_in or datetime.date.today().isoformat(),
                           "note": note}
        cls.save()

    @classmethod
    def remove_position(cls, code):
        """[V22.12新增] 移除持倉（清理幽靈持倉）"""
        if code in cls._data:
            del cls._data[code]
            cls.save()
            return True
        return False

    @classmethod
    def pnl(cls, code, current_price):
        pos = cls.get(code)
        if not pos or not pos.get("cost"):
            return {"pnl_pct": None, "days_held": None}
        cost = pos["cost"]
        pnl_pct = (current_price - cost) / cost
        try:
            days = (datetime.date.today() -
                    datetime.date.fromisoformat(pos["date_in"])).days
        except Exception:
            days = None
        return {"pnl_pct": pnl_pct, "days_held": days, "cost": cost}

    @classmethod
    def stop_profit_check(cls, code, price, profit_thr=0.20, days_max=90):
        info = cls.pnl(code, price)
        if info["pnl_pct"] is None: return ""
        if info["pnl_pct"] >= profit_thr:
            return f"🔔 浮盈{info['pnl_pct']:.1%}超{profit_thr:.0%}，建議分批止盈"
        if info["days_held"] and info["days_held"] >= days_max:
            return f"⏰ 持倉{info['days_held']}天，建議重新評估"
        return ""






# ══════════════════════════════════════════════════════════════════════
# 🛡️ KellyEngine — 倉位定規
# ══════════════════════════════════════════════════════════════════════
class KellyEngine:

    @staticmethod
    def compute(ev, win_rate, avg_win, avg_loss, event_near=False,
                vol_factor=1.0, hard_brake=False, n_eff=None):
        """
        Kelly 公式計算（含貝葉斯先驗平滑、半凱利風控）
        [V22.22] event_near=True 時觸發事件波動率懲罰(兩檔制):
          重大事件臨近 → half_kelly × 0.83 (等效σ²放大1.2倍, f*=EV/σ²精神)
        [V22.26] vol-scaled Kelly(模式B相對基準): half_kelly × vol_factor(連續波動率縮放,
          froth→自動縮; 預設1.0=不變,完全向後相容)。hard_brake=True(過熱AND宏觀逆風雙條件)
          再 ×0.5。僅調建議倉位/加碼size,絕不強制平倉。
        """
        if not win_rate or win_rate<=0 or not avg_win or avg_win<=0 or not avg_loss or avg_loss<=0:
            return {"kelly_pct":0.03,"half_kelly":0.015,
                    "label":"🔸 最小倉位(數據不足)","rationale":"歷史數據不足"}
        # 貝葉斯先驗收縮(原 docstring 有寫、程式從未實作):勝率往「無技巧 50%」收縮,
        #   權重以【交易日數】計。原先驗 0.65 本身是樂觀假設,改為 0.50。
        PRIOR_WIN_RATE = 0.50
        PRIOR_WEIGHT   = 10
        shrink_note = ""
        if n_eff is not None:
            if n_eff < SIGNAL_PAYOFF_MIN_DAYS:
                return {"kelly_pct":0.03,"half_kelly":0.015,
                        "label":"🔸 最小倉位(樣本不足)",
                        "rationale":f"可交易樣本僅 {n_eff} 個交易日(<{SIGNAL_PAYOFF_MIN_DAYS})"}
            _raw_w = win_rate
            win_rate = (win_rate * n_eff + PRIOR_WIN_RATE * PRIOR_WEIGHT) / (n_eff + PRIOR_WEIGHT)
            shrink_note = f"(勝率{_raw_w:.1%}→收縮{win_rate:.1%},{n_eff}日)"
        b = avg_win / avg_loss
        q = 1 - win_rate
        kelly_raw = max(0.0, (win_rate*b - q) / b)
        if kelly_raw > 1.0 or win_rate >= 0.95:
            kelly_raw = min(kelly_raw, 0.40)
            rationale_note = "(精度過高→保守截斷)"
        else:
            rationale_note = ""

        ev_factor = (ev/100)**1.5
        half_kelly = min(kelly_raw * ev_factor * KELLY_HALF, KELLY_MAX_SINGLE)
        # [V22.22] 事件波動率懲罰(兩檔制) — 等效Kelly分母σ²放大
        event_note = ""
        if event_near:
            EVENT_VOL_PENALTY = 0.83  # =1/1.2, 保守預設待A股歷史校準
            half_kelly = half_kelly * EVENT_VOL_PENALTY
            event_note = "×事件懲罰0.83(σ²+20%預設)"
        # [V22.26] vol-scaled Kelly: 連續波動率縮放(模式B) + 條件硬煞
        vol_note = ""
        if vol_factor != 1.0:
            half_kelly = half_kelly * vol_factor
            vol_note += f"×vol{vol_factor:.2f}"
        if hard_brake:
            half_kelly = half_kelly * 0.5
            vol_note += "×硬煞0.5(過熱且宏觀逆風)"
        if half_kelly >= 0.15: label = "🟢 重倉（15-25%）"
        elif half_kelly >= 0.08: label = "🟡 標準倉（8-15%）"
        elif half_kelly >= 0.03: label = "🟠 輕倉（3-8%）"
        else: label = "🔴 觀察倉（<3%）"
        return {"kelly_pct": round(kelly_raw*ev_factor, 4),
                "half_kelly": round(half_kelly, 4),
                "label": label,
                "rationale": f"Kelly={kelly_raw:.1%}{shrink_note}{rationale_note}×EV因子={ev_factor:.2f}{event_note}{vol_note}→{half_kelly:.1%}"}

    @staticmethod
    def style_concentration_check(results):
        style_map = {}
        for r in results:
            _ci = TARGET_ETFS.get(r.get("code",""), {})
            style = "QDII" if _is_qdii_etf(_ci) else _ci.get("style","?")  # [V22.26.3] QDII另計,與全系統一致(_is_qdii_etf),不混入Tech
            kelly = r.get("kelly", {}).get("half_kelly", 0.0)
            if "主攻" in r.get("action","") or "靜默" in r.get("action",""):
                style_map.setdefault(style, []).append(kelly)
        alerts = []
        for style, kellys in style_map.items():
            total = sum(kellys); count = len(kellys)
            # [V22.26.3] 口徑釐清:total=主攻/靜默信號的建議Kelly倉位「加總」=加倉上限參考,非實際持倉(實際持倉見📊板塊集中度)
            alert = (f"⚠️ {style}信號建議倉位合計{total:.1%}超單style上限{KELLY_MAX_STYLE:.0%}(此為加倉上限參考,非實際持倉)"
                     if total > KELLY_MAX_STYLE else
                     (f"📌 {style}信號{count}檔，相關性風險高" if count >= 4 else ""))
            alerts.append({"style":style,"count":count,
                           "total_kelly":round(total,4),"alert":alert})
        return alerts


# ══════════════════════════════════════════════════════════════════════
# 🌡️ TrendRegimeDetector — [V22.13 核心架構升級] 市場狀態檢測器
# ══════════════════════════════════════════════════════════════════════
class TrendRegimeDetector:
    """
    [V22.13] 8D根因分析後的架構修復

    背景：5/18-5/20連續錯失+348元 A股科技強動量
    根因：EV公式內建均值回歸假設（rsi_s/bias_s對超買懲罰）
          不適應「連續上漲5天+乖離>10%」的趨勢市
    修復：先判斷市場狀態，再用對應的EV公式

    三種狀態：
    - trending_up:   連續上漲趨勢市（超買是正常的）
    - trending_down: 連續下跌趨勢市（超賣保護不可信）
    - ranging:       震盪市（均值回歸邏輯有效）

    判斷指標：
    1. 連續天數方向（recent 5 days）
    2. 5日累計漲跌
    3. 20日均線斜率
    """

    @staticmethod
    def detect(prices):
        """
        返回: dict {regime, strength, evidence, up_days, ret_5d, ma_slope}
        """
        if not prices or len(prices) < 25:
            return {
                "regime": "ranging", "strength": 0.0,
                # [V23.9] 這裡的 0.0 是「資料不足」哨兵,非真震盪強度。標到不可能與真讀數混淆
                #   (疑為 live 報告 11 檔恆0.0 的真根因:NAV序列<25日 → 全落此分支,待 live 佐證)
                "evidence": "⚠️資料不足(<25日·NAV序列不足)→強度不可用,非真震盪讀數",
                "up_days": 0, "ret_5d": 0, "ma_slope": 0
            }

        arr = [float(p) for p in prices[-25:]]

        # 1. 連續方向性（最近5日，看相鄰日漲跌）
        recent_6 = arr[-6:]
        up_days = sum(1 for i in range(5) if recent_6[i+1] > recent_6[i])
        down_days = 5 - up_days

        # 2. 5日累計漲跌
        ret_5d = (arr[-1] - arr[-5]) / arr[-5] if arr[-5] > 0.01 else 0.0

        # 3. 20日均線斜率
        ma20_now = sum(arr[-20:]) / 20
        ma20_old = sum(arr[-25:-5]) / 20
        ma_slope = (ma20_now - ma20_old) / ma20_old if ma20_old > 0.01 else 0.0

        # ── 趨勢上判斷 ──────────────────────────────────
        # [V22.14] 雙路徑觸發（基於 5/21 實盤暴露的閾值過嚴問題）
        # 5/21 008888/011613/018177/016008 都是「連漲3天+5日漲幅5-9%+MA5%」
        # 因連漲天數=3<4，被誤判 ranging → 錯失主升
        # 路徑A（標準）：連漲≥4 + 5日>+2.5% + MA>+0.3% — 適合慢速大盤
        # 路徑B（強動量）：連漲≥3 + 5日>+4.0% + MA>+1.0% — 適合快速個股
        path_a = up_days >= 4 and ret_5d > 0.025 and ma_slope > 0.003
        path_b = up_days >= 3 and ret_5d > 0.040 and ma_slope > 0.010
        if path_a or path_b:
            # 強度 0~1：取兩個指標的正規化均值
            strength = min(1.0, (min(ret_5d / 0.05, 1) + min(ma_slope / 0.015, 1)) / 2)
            path_tag = "標準路徑" if path_a else "強動量路徑"
            return {
                "regime": "trending_up", "strength": round(strength, 2),
                "evidence": f"連漲{up_days}/5日+5日漲幅{ret_5d:+.1%}+MA20斜率{ma_slope:+.2%}[{path_tag}]",
                "up_days": up_days, "ret_5d": ret_5d, "ma_slope": ma_slope
            }

        # ── 趨勢下判斷 ──────────────────────────────────
        path_a_d = down_days >= 4 and ret_5d < -0.025 and ma_slope < -0.003
        path_b_d = down_days >= 3 and ret_5d < -0.040 and ma_slope < -0.010
        if path_a_d or path_b_d:
            strength = min(1.0, (min(abs(ret_5d) / 0.05, 1) + min(abs(ma_slope) / 0.015, 1)) / 2)
            path_tag = "標準路徑" if path_a_d else "強動量路徑"
            return {
                "regime": "trending_down", "strength": round(strength, 2),
                "evidence": f"連跌{down_days}/5日+5日跌幅{ret_5d:+.1%}+MA20斜率{ma_slope:+.2%}[{path_tag}]",
                "up_days": up_days, "ret_5d": ret_5d, "ma_slope": ma_slope
            }

        # ── 震盪市 ──────────────────────────────────
        # [V23.9 修B] 原寫死 strength=0.0 → 所有震盪標的印「強度0.0」零資訊量。
        #   改算「盤整緊密度」:偏離越小=震盪越紮實(強度高);接近趨勢邊緣=強度低 → 恢復區分度。
        _rng_strength = round(max(0.0, 1.0 - max(min(abs(ret_5d) / 0.05, 1.0),
                                                 min(abs(ma_slope) / 0.015, 1.0))), 2)
        return {
            "regime": "ranging", "strength": _rng_strength,
            "evidence": f"連漲{up_days}/5日+5日{ret_5d:+.1%}+MA{ma_slope:+.2%}",
            "up_days": up_days, "ret_5d": ret_5d, "ma_slope": ma_slope
        }

    @staticmethod
    def compute_macro_prior(sox_ret, ndx_ret, vix_val, cny_val, style,
                            gold_ret=0.0, is_qdii=False, tech_coupling=None):
        """
        [V22.16 5/27 完整版] 跨報宏觀因子繼承機制
        
        將盤前報的隔夜宏觀數據作為「先驗權重」注入早報的 EV 算式。
        盤前 = 先驗（Prior），早盤 = 觀測（Observation）
        
        返回宏觀先驗分數 -1.0 ~ +1.0
        正分 = 利多（推升 EV）/ 負分 = 利空（壓制 EV）
        
        各 Style 的傳導權重設計（基於 5 日實證，5/25 同向 / 5/26 反向）:
          Tech (A股科技 / 半導體):
            SOX 0.40 + VIX 0.20 = 0.60 主導
          QDII (006479/018967 納指相關):
            NDX 0.60 + CNY 0.20 + VIX 0.20 = 1.00 主導
          Defensive (000216 黃金 / 紅利低波):
            黃金漲跌 0.50 反向：黃金升 = QDII 緊張 = 避險買盤 = Defensive 利多
            VIX 0.30 同向：VIX 升 = 恐慌 = 避險買盤 = Defensive 利多
          Cyclical / Industrial / Dividend / Utility:
            僅 VIX 0.20 影響（反向：VIX 升 = 風險資產壓力）
        
        保守係數：完整版 0.20（影響 ±20% EV）
        """
        def clip(x, lo, hi):
            return max(lo, min(hi, x))
        
        # [V22.17.3 防禦 5/29] None 參數正規化（取代 sox/ndx 的 `or 0` 模式）
        # 根因：邊界測試發現 vix_val=None / cny_val=None 會炸 TypeError
        sox_ret  = sox_ret  if sox_ret  is not None else 0.0
        ndx_ret  = ndx_ret  if ndx_ret  is not None else 0.0
        gold_ret = gold_ret if gold_ret is not None else 0.0
        vix_val  = vix_val  if vix_val  is not None else 18.0  # 中性預設
        cny_val  = cny_val  if cny_val  is not None else 7.0   # 中性預設
        
        score = 0.0
        
        # 優先處理 QDII（雖 style 標 Tech 但 _is_qdii=True）
        if is_qdii:
            ndx_score = clip((ndx_ret or 0) / 0.02, -1, 1)
            score += ndx_score * 0.60
            # CNY 反向：USD/CNY 升（人民幣弱）對 QDII 利多（用人民幣買美股資產便宜）
            # 但 QDII ETF 用人民幣計價：CNY 升 → 美股資產換成 CNY 變多 → 利多 ✅
            cny_score = clip((cny_val - 7.0) / 0.1, -1, 1)
            score += cny_score * 0.20
            vix_score = -clip((vix_val - 18) / 8, -1, 1)
            score += vix_score * 0.20
            return clip(score, -1.0, 1.0)
        
        # Tech Style：SOX 主導 + VIX 壓制
        if style == "Tech":
            sox_score = clip((sox_ret or 0) / 0.03, -1, 1)
            # [V22.17.1 誘多過濾 5/28 → V22.33.4 對稱化] SOX 隔夜【極端】時降權
            # 原:只擋正向暴漲(>+4% 誘多)。V22.33.4 治 D3:暴跌(如 7/15 −4.78%)同樣是
            #   極端值,過去【未降權】→ 直接餵 −0.40 進 Tech prior(WB harness 揪出)。
            # 機構作法(Renaissance/AQR 一脈):極端值穩健【降權】非歸零(歸零丟真訊號、
            #   生餵過度信任壞資料)→ 對稱 ×0.3。real 崩盤留 30% 方向、壞資料傷害限 30%。
            # ⚠️ 門檻 ±4% / 係數 0.3 待實盤數據校準(鐵律⑳)。
            if abs(sox_ret or 0) > 0.04:
                sox_score *= 0.30  # [V22.33.4] 對稱降權:暴漲(誘多)+暴跌(誘空/壞資料)皆打 3 折
            # [V22.27.9 弱連動縮放] #165-170 源碼+實證: A股科技對美股 sign-agreement~50%、
            #   系統自算條件β(Cov/Var)≈0。用實測條件β正規化的 tech_coupling∈[0,1] 縮放 SOX 貢獻;
            #   ≈0 時 SOX 對 Tech EV 微調趨近 0。tech_coupling=None(未傳/停用)→完全等於舊行為(向後相容)。
            if tech_coupling is not None:
                sox_score *= tech_coupling
            score += sox_score * 0.40
            vix_score = -clip((vix_val - 18) / 8, -1, 1)
            score += vix_score * 0.20
            return clip(score, -1.0, 1.0)
        
        # Defensive Style：黃金 + VIX（避險屬性反向）
        if style == "Defensive":
            # [V22.22.2 調權重] 黃金漲跌主導權0.50→0.30, VIX避險權0.30→0.50
            # 根因(6/10查證): 舊版黃金漲跌權0.50主導, 但恐慌日'流動性提款'時
            #   黃金被迫拋售下跌, 公式誤判'避險降溫→利空', 壓過VIX避險加分
            #   → 黃金被建議減倉(在最該避險時卸掉避險盾)
            # 修正: VIX是更純粹的避險需求度量, 讓VIX主導Defensive定價
            # ⚠️ 0.30/0.50 為邏輯調整, 非回測最優, 待A股歷史數據校準
            gold_score = clip((gold_ret or 0) / 0.015, -1, 1)  # 黃金 ±1.5% → ±1.0
            score += gold_score * 0.30  # 降權: 恐慌日黃金跌會失真
            # VIX 高（恐慌）= 避險買盤 = Defensive 利多（同向, 升權為主導）
            vix_score = clip((vix_val - 18) / 8, -1, 1)  # VIX 升 → 正分
            score += vix_score * 0.50  # 升權: VIX是純避險需求度量
            return clip(score, -1.0, 1.0)
        
        # 其他 Style (Cyclical/Industrial/Dividend/Utility)：僅 VIX 反向
        vix_score = -clip((vix_val - 18) / 8, -1, 1)
        score += vix_score * 0.20
        return clip(score, -1.0, 1.0)

    @staticmethod
    def compute_ev_regime_aware(rsi, bias, trend, geo_adj, W, c_drain,
                                  regime_info):
        """
        [V22.13] 市場狀態感知的EV計算
        三套公式，根據regime切換
        """
        regime = regime_info["regime"]
        strength = regime_info["strength"]

        if regime == "trending_up":
            # 趨勢上行EV：超買=順勢=高分
            # rsi_s 反轉：RSI 50 → 0分，RSI 80 → +1分（順勢）
            rsi_s = max(-1.0, min(1.0, (rsi - 50) / 30))
            # bias_s 反轉：乖離正向→順勢→正分
            bias_s = max(-1.0, min(1.0, bias / 0.10))
            # trend 直接用（趨勢市中 trend 多為正）
            f_micro = (rsi_s + bias_s + trend) / 3
            # 趨勢市加成：strength=1.0 時 EV ×1.2
            boost = 1.0 + strength * 0.2
            ev = (W * f_micro * geo_adj * boost + (1-W) * 0.5) * 100 * max(0.6, 1-c_drain)

        elif regime == "trending_down":
            # 趨勢下行EV：超賣保護減弱（持續下跌中反彈不可期）
            # rsi_s：RSI極低時不再給高分，避免在趨勢下行中誤觸發超賣保護
            rsi_s = -max(-1.0, min(1.0, abs(rsi - 50) / 30)) if rsi < 50 else max(-1.0, min(1.0, (50 - rsi) / 30))
            bias_s = max(-1.0, min(1.0, bias / 0.10))  # 乖離負向→負分
            f_micro = (rsi_s + bias_s + trend) / 3
            ev = (W * f_micro * geo_adj + (1-W) * 0.5) * 100 * max(0.6, 1-c_drain)

        else:  # ranging
            # 震盪市：原V22.12均值回歸邏輯（保留）
            rsi_s = (50 - abs(rsi - 50)) / 50
            bias_s = max(-1, min(1, -bias / 0.15))
            f_micro = (rsi_s + trend + bias_s) / 3
            ev = (W * f_micro * geo_adj + (1-W) * 0.5) * 100 * max(0.6, 1-c_drain)

        return max(0, min(100, int(round(ev))))


# ════════════════════════════════════════════════════════════════
# [V22.18 新增] 資金流向背離示警引擎 (實證加固版)
# ════════════════════════════════════════════════════════════════
# 三重驗證地基: 長江證券研究 + 6/2收盤報案例(科创50涨+22%卻流出21億)
#              + WorkBuddy實證(逆向約1/3,一致約2/3,噪音比3:7)
# 設計: 只在【極端背離】時輕調EV±5並示警, 避開2/3一致區噪音
#   顶背離: RSI≥65 + 連流出≥4天 + 流出顯著 → EV-5 + ⚠️
#   底背離: RSI≤35 + 連流入≥4天 + 流入顯著 → EV+5 + 💡
#   其餘/缺數據: EV不動(歸零安全)
# ════════════════════════════════════════════════════════════════
FLOW_DIVERGENCE_MIN_YI = 1.0   # 流出/流入"顯著"閾值(億), 過濾微量噪音
FLOW_DIVERGENCE_EV_ADJ = 5     # 背離EV調整幅度(實證噪音比3:7,故輕調)


def apply_flow_divergence(ev, rsi, flow5):
    """
    [V22.18.1/.2] 資金流向背離示警 — 在主EV計算外層疊加(不動核心公式)
    [V22.18.2修訂] 顯著閾值隨數量級自適應(etf億級/index百億級)
    參數: ev(原始EV 0-100), rsi(當前RSI), flow5(get_etf_5day_flow回傳dict或None)
    回傳: (adjusted_ev, divergence_tag)  無背離時 tag=""
    """
    if not flow5 or not isinstance(flow5, dict):
        return ev, ""
    flow_list = flow5.get("flow5_list")
    if not flow_list or len(flow_list) < 4:
        return ev, ""
    flow5_sum = sum(flow_list)
    pos_days = sum(1 for x in flow_list if x > 0)
    neg_days = sum(1 for x in flow_list if x < 0)
    # [V22.18.2] 顯著閾值隨數量級:指數/板塊百億級→20億, ETF億級→1億
    scale = flow5.get("scale", "etf")
    min_yi = 20.0 if scale == "index" else FLOW_DIVERGENCE_MIN_YI
    # 顶背離: 高位+連流出+顯著
    if rsi >= 65 and neg_days >= 4 and flow5_sum <= -min_yi:
        adj_ev = max(0, ev - FLOW_DIVERGENCE_EV_ADJ)
        tag = f"⚠️顶背離(主力高位派發:RSI={rsi:.0f}+5日流出{flow5_sum:.1f}億,涨势或见顶)"
        return adj_ev, tag
    # 底背離: 低位+連流入+顯著
    if rsi <= 35 and pos_days >= 4 and flow5_sum >= min_yi:
        adj_ev = min(100, ev + FLOW_DIVERGENCE_EV_ADJ)
        tag = f"💡底背離(主力低位吸籌:RSI={rsi:.0f}+5日流入{flow5_sum:.1f}億,或近底部)"
        return adj_ev, tag
    return ev, ""

# ══════════════════════════════════════════════════════════════════════
# 🌑 [V22.23.6 影子] 低點買入三共振 — 只記錄不觸發實際操作(觀察期驗證)
#   痛點: 系統強守弱攻, 只在高點說「別追」, 不在低點給可信買入信號。
#   解法: 三個獨立維度同時確認才算真低點(對抗單一指標噪音):
#     共振1 技術超賣  : RSI≤35 且 乖離bias≤-5%        (價格維度)
#     共振2 資金底背離: 近5日連流入≥4天且顯著(主力吸籌)(資金維度, 復用現有引擎)
#     共振3 β/隔夜催化: β_US×隔夜美股 ≥ +0.3%          (催化維度)
#   假信號率粗估 0.33³≈3.6%。影子模式: 觸發只寫DB+報告標註, 絕不進compute_order。
#   跑一週後用真實T+1/T+3結果驗證有效, 再決定是否啟用。
RESONANCE_RSI_MAX       = 35      # 共振1 技術超賣 RSI上限
RESONANCE_BIAS_MAX      = -0.05   # 共振1 乖離上限(-5%)
RESONANCE_BETA_PUSH_MIN = 0.003   # 共振3 β推力最小門檻(+0.3%)

def detect_bottom_resonance(code, rsi, bias, divergence_tag, sox_ret, ndx_ret, is_qdii):
    """[V22.23.6 影子] 低點買入三共振偵測。回傳 (count, hits, beta_push)。
    count==3 → 影子買入信號(未啟用); count==2 → 近觸發(記錄供觀察)。
    """
    hits = []
    if rsi is not None and bias is not None and rsi <= RESONANCE_RSI_MAX and bias <= RESONANCE_BIAS_MAX:
        hits.append("技術超賣")
    if divergence_tag and "底背離" in divergence_tag:
        hits.append("資金底背離")
    beta = get_beta_us(code)  # [V22.27] 條件beta優先(滾動Cov/Var),無則fallback固定BETA_US
    overnight = (ndx_ret or 0.0) if is_qdii else (sox_ret or 0.0)
    beta_push = beta * overnight
    if beta_push >= RESONANCE_BETA_PUSH_MIN:
        hits.append(f"β催化{beta_push*100:+.1f}%")
    return len(hits), hits, beta_push

def record_shadow_signal(code, name, report_type, rsi, bias, count, hits, beta_push):
    """[V22.23.6 影子] 寫入shadow_signals表(供日後T+1/T+3驗證)。
    CREATE IF NOT EXISTS→無需手動建表。失敗靜默不影響主流程。
    """
    try:
        import sqlite3 as _sq
        from datetime import datetime as _dt
        with _sq.connect(LOG_DB, timeout=10) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS shadow_signals("
                "ts TEXT, code TEXT, name TEXT, report_type TEXT, rsi REAL, "
                "bias REAL, resonance_count INTEGER, hits TEXT, beta_push REAL)")
            conn.execute(
                "INSERT INTO shadow_signals VALUES(?,?,?,?,?,?,?,?,?)",
                (_dt.now().strftime("%Y-%m-%d %H:%M:%S"), code, name, report_type,
                 round(rsi, 2) if rsi is not None else None,
                 round(bias, 4) if bias is not None else None,
                 count, "+".join(hits), round(beta_push, 4)))
            conn.commit()
    except Exception:
        pass  # 影子記錄失敗絕不影響主報告

# ══════════════════════════════════════════════════════════════════════
# 📐 QuantEnhanced — 量化增強（RSI反轉/動量/風險評分）
# ══════════════════════════════════════════════════════════════════════
def _flow5_with_manual(code, proxy, live_flow):
    """[V24.0 修③] 東財 fflow 結構性封鎖時的手工補值通道(治「資金流永遠空白」)。
    用法:在 BASE_DIR 放 manual_flow.json = {"008888": {"flow5_yi": -7.3, "as_of": "2026-08-04"}, ...}
    (你從券商/東財PC端看一眼 5 日主力淨額填入)。live 抓到則優先 live;手工值一律標來源+日期,
    且超過 3 個交易日自動失效(不讓舊值冒充今日,㉘)。"""
    if live_flow and isinstance(live_flow, dict) and live_flow.get("flow5_yi") is not None:
        return live_flow
    try:
        _p = os.path.join(BASE_DIR, "manual_flow.json")
        if not os.path.exists(_p):
            return _flow5_proxy_fallback(code, proxy, live_flow)
        with open(_p, "r", encoding="utf-8") as _fh:
            _mf = json.load(_fh) or {}
        _e = _mf.get(str(code)) or {}
        _v = _e.get("flow5_yi")
        if _v is None:
            return _flow5_proxy_fallback(code, proxy, live_flow)
        _asof = str(_e.get("as_of", ""))[:10]
        _age = None
        if _asof:
            try:
                _age = (datetime.date.today() - datetime.date.fromisoformat(_asof)).days
            except Exception:
                _age = None
        if _age is None or _age > 3:
            return _flow5_proxy_fallback(code, proxy, live_flow)   # 過期→改走量價代理
        # [V24.19.1] 補齊 schema:此 dict 原本也缺 signal/trend,
        #   只因手工檔通常不存在才未觸發 KeyError——同一顆地雷,一併拆掉。
        _mi = "🔴" if float(_v) > 0.3 else "🟢" if float(_v) < -0.3 else "⚪"
        return {"flow5_yi": float(_v), "flow5_list": _e.get("flow5_list"),
                "signal": f"{_mi} 手工補5日主力",
                "trend": _e.get("trend") or "手工值(無逐日序列)",
                "scale": _e.get("scale"),
                "src": f"✍️手工補({_asof},{_age}日前)"}
    except Exception as _e2:
        _log_err(_e2, "_flow5_with_manual")
        return _flow5_proxy_fallback(code, proxy, live_flow)


def _flow5_proxy_fallback(code, proxy, live_flow=None):
    """[V24.19] 回退鏈第三層:量價代理數值(東財封鎖 + 無手工值時)。

    ★為何新增(使用者 2026-08-07 指出「WB不是已經有給過替代方案,怎麼沒用」):
      量價代理(V23.8 `_volume_flow_proxy`)【確實存在且可用】,但它原本只回傳
      **顯示字串**,所以只出現在資金流區塊的畫面上;凡是吃數值的下游——
      歸因表💰主力欄、反轉三條件①機構回補、⑤資金流閘門——全都拿不到,
      於是東財封鎖後那些位置永遠是 0 / ❌。**替代方案有做,但沒接線。**

    ⚠️ 誠實邊界:回傳值【不是主力淨額】,是「方向×量能×幅度」的傾向強度。
      故 src 標「📊量價代理(非主力淨額)」、`is_proxy=True`,
      下游若需要真實主力淨額應檢查此旗標並拒用。
    """
    try:
        if not proxy:
            return live_flow
        _txt, _ok, _val = _volume_flow_proxy(proxy)
        if not _ok or _val is None:
            return live_flow
        # [V24.19.1 修我自己引入的 KeyError] 下游有 5 處【直接索引】flow5 dict
        #   (L8527 trend / L8562 signal / L8603 signal+trend / L11729 signal / L13139 signal),
        #   原本只回 flow5_yi+flow5_list 少了 signal/trend/scale → 代理一生效就 KeyError:'signal'。
        #   **教訓:新增回退層必須回【與主路徑完全相同的 schema】,少一個鍵就是崩潰。**
        _ic = "🔴" if _val > 0.3 else "🟢" if _val < -0.3 else "⚪"
        _dr = "代理淨流入傾向" if _val > 0.3 else "代理淨流出傾向" if _val < -0.3 else "代理窄幅觀望"
        return {"flow5_yi": float(_val), "flow5_list": None, "is_proxy": True,
                "signal": f"{_ic} 量價{_dr}",
                "trend": "單日量價(非5日累計)",
                "scale": None,
                "src": "📊量價代理(非主力淨額·單日量價傾向)"}
    except Exception as _e:
        _log_err(_e, "_flow5_proxy_fallback")
        return live_flow


# ── [V24.4] 盡調公式引擎(WB審計:原dd只是手打展示,無計算/無閘門/無自動偵測)──────
DD_DIMS = ["結構", "成本", "規模流動性", "經理人", "持股", "配息", "位置",
           "結構vs能力歸因", "單點風險"]
_DD_WARN_KW = ("⚠️", "封頂", "重複押注", "高基期", "無實績", "接刀", "本金", "單點", "溢價")


def _dd_has_warn(text):
    """[V24.4] 警示關鍵字偵測,排除否定式假陽性:關鍵字【前 20 字內】出現否定詞則不算警示。
    例:「無covered call封頂」的『封頂』被『無』否定 → 不標警示。"""
    _neg = ("無", "不", "非", "免", "未有")
    for _w in _DD_WARN_KW:
        _i = text.find(_w)
        while _i >= 0:
            _win = text[max(0, _i - 20):_i]
            if not any(_n in _win for _n in _neg):
                return True
            _i = text.find(_w, _i + 1)
    return False


DD_META_TTL_DAYS = 30   # [V24.6] dd 研究值保鮮期(人工web研究,非即時)

def _dd_freshness(text):
    """[V24.6] dd 研究值保鮮檢查——治「DD_META 有標日期卻無TTL,60天後仍冒充今日值」。
    與觀察池 _pw_qualitative_display 同源病(凍結判斷天天照印),此處一併防。"""
    import re as _re_f
    _m = _re_f.search(r"web\s*(\d{4}-\d{2}-\d{2})", str(text or ""))
    if not _m:
        return text
    try:
        _age = (datetime.date.today() - datetime.date.fromisoformat(_m.group(1))).days
    except Exception:
        return text
    if _age > DD_META_TTL_DAYS:
        return f"🔴 研究值已過期{_age}日(>{DD_META_TTL_DAYS}日)·勿依賴,需重查 ← 原:{text}"
    return f"{text} ⏱️{_age}日前"


def build_dd_profile(info, code=None):
    """[V24.4→V24.5] 全池套用 9 維骨架 + 自動補值。
    自動補值優先級: 已填(raw) > DD_META(web實證,標來源日期) > 計算(重疊/經理人/位置) > 未驗證·待查。
    目的:消滅「東財封鎖就拿不到數據」的死結——成本/規模/經理人用已研究實證庫,
          位置/單點風險/持股用零外部依賴的計算(錨股重疊+同經理人掃描)。"""
    code = code or info.get("code")
    raw = (info.get("dd") or {}).copy()
    _meta = {_k: _dd_freshness(_v) for _k, _v in (DD_META.get(code, {}) if code else {}).items()}   # [V24.6] 套保鮮
    # ① 成本/規模流動性/經理人/配息/結構/結構vs能力歸因: web實證(標來源); 位置/單點風險/持股 由計算helper處理
    for _d in DD_DIMS:
        if _d in ("位置", "單點風險", "持股"):
            continue
        _cur = str(raw.get(_d, ""))
        if _d not in raw or not raw.get(_d) or "未驗證" in _cur or "待查" in _cur:
            if _meta.get(_d):
                raw[_d] = _meta[_d]
    # ② 位置: 發行價/高點(web) + RSI(掃描)
    _cur = str(raw.get("位置", ""))
    if ("位置" not in raw) or ("未驗證" in _cur) or ("待查" in _cur):
        _pos = _auto_position(code, info, _meta)
        if _pos:
            raw["位置"] = _pos
    # ③ 單點風險: 同經理人 + 持股重疊
    _cur = str(raw.get("單點風險", ""))
    if ("單點風險" not in raw) or ("未驗證" in _cur) or ("待查" in _cur):
        _sp = _auto_single_point_risk(code) if code else None
        if _sp:
            raw["單點風險"] = _sp
    # ④ 持股: 重疊對手
    _cur = str(raw.get("持股", ""))
    if ("持股" not in raw) or ("未驗證" in _cur) or ("待查" in _cur):
        _hold = _auto_holding_overlap(code) if code else None
        if _hold:
            raw["持股"] = _hold
    # ⑤ 結構vs能力歸因: web實證註記
    _cur = str(raw.get("結構vs能力歸因", ""))
    if ("結構vs能力歸因" not in raw) or ("未驗證" in _cur) or ("待查" in _cur):
        if _meta.get("結構vs能力歸因"):
            raw["結構vs能力歸因"] = _meta["結構vs能力歸因"]
    # ── 分類裁決(原邏輯) ──
    full, n_ok, n_todo, n_warn = {}, 0, 0, 0
    for _d in DD_DIMS:
        _v = raw.get(_d)
        if not _v:
            full[_d] = "❓ 未驗證·待查(未填)"; n_todo += 1
        elif ("未驗證" in _v) or ("待查" in _v):
            full[_d] = _v; n_todo += 1
        elif _v.startswith("⚠️"):          # 計算/實證已附權威前綴,不再二重判斷
            full[_d] = _v; n_warn += 1
        elif _v.startswith("✅"):
            full[_d] = _v; n_ok += 1
        elif _dd_has_warn(_v):
            full[_d] = "⚠️ " + _v; n_warn += 1
        else:
            full[_d] = "✅ " + _v; n_ok += 1
    for _k, _v in raw.items():          # 保留骨架外的自訂維度
        if _k not in full:
            full[_k] = _v
    if n_todo == 0 and n_warn == 0:
        verdict = "✅ 盡調完備·可進入技術面評估"
    elif n_warn and n_todo:
        verdict = f"⛔ 盡調未過:{n_warn} 警示 + {n_todo} 待查 → 補查並評估警示後再議"
    elif n_warn:
        verdict = f"⚠️ 盡調有警示({n_warn} 項) → 可進場但需接受該風險"
    else:
        verdict = f"🔍 盡調待補({n_todo} 項未驗證) → 資料不足,不宜下重手"
    return full, {"通過": n_ok, "待查": n_todo, "警示": n_warn}, verdict


def scan_duplicate_manager(pool=None, holdings_pool=None):
    """[V24.4 WB建議②] 自動掃描全池同經理人/同投信 → 重複押注偵測(原本只是手寫字串)。
    回傳 {經理人: [代碼,...]},僅列 >=2 檔者。加第3檔同經理人基金時會自動報警。"""
    import re as _re_dm
    _pool = pool if pool is not None else WATCHLIST_ETFS
    _mgr = {}
    for _code, _info in (_pool or {}).items():
        _t = (_info.get("dd") or {}).get("經理人", "") or ""
        _m = _re_dm.match(r"\s*([\u4e00-\u9fff]{2,4})", _t)
        if _m:
            _mgr.setdefault(_m.group(1), []).append(_code)
    return {k: v for k, v in _mgr.items() if len(v) >= 2}


# ── [V24.5 ⑩] 簡化版因子歸因(單因子 OLS,純Python無numpy依賴) ───────────────


def build_factor_attr_block(code):
    """[V24.5 ⑩] 從 LOG_DB 的實現報酬(realized_returns 單一來源)對大盤/板塊做單因子歸因。
    零外部依賴(只讀本機DB);自動剔除污染值(|日報酬|>25%)與最新一日(防半日/髒值);
    大盤proxy=DB內A股ETF等權籃;半導體板塊=008888/011613/020356 等權籃。
    (原本讀 daily_snapshot.today_ret,那是系統當日估值,不是實現報酬。)"""
    try:
        import sqlite3 as _sq
        _con = _sq.connect(LOG_DB)
        _raw = [(c, d, r) for (c, d), r in returns_store.load_map(_con).items()]
        _con.close()
        _data = {}
        for _cc, _dd, _rr in _raw:
            if abs(_rr) > 0.25:        # ㉙ 污染剔除: 日報酬正常±15%內,>25%必為GIGO
                continue
            _data.setdefault(_cc, {})[_dd] = _rr
        _alld = sorted({d for c in _data for d in _data[c]})
        if _alld:                       # 排除最新一日(可能不完整/半日/髒值)
            _last = _alld[-1]
            for c in _data:
                _data[c].pop(_last, None)
        if code not in _data or len(_data[code]) < 5:
            return None
        _ash = [c for c in _data if not c.endswith(".TW")]
        _mkt = {}
        for d in sorted({x for c in _data for x in _data[c]}):
            _v = [_data[c][d] for c in _ash if d in _data[c]]
            if len(_v) >= 8:
                _mkt[d] = sum(_v) / len(_v)
        _dates = sorted(d for d in _data[code] if d in _mkt)
        if len(_dates) < 5:
            return None
        _ar = [_data[code][d] for d in _dates]
        _mr = [_mkt[d] for d in _dates]
        _r = factor_attribution(_ar, _mr, "大盤")
        if not _r["ok"]:
            return f"⑩ 因子歸因: {_r.get('reason', '樣本不足')}"
        _s = (f"⑩ **因子歸因(對大盤單因子OLS)**: α年化={_r['alpha_pct_ann']:+.1f}% | "
              f"β={_r['beta']} | R²={_r['r2']} | t(α)={_r['t_alpha']} | N={_r['n']}交易日")
        _semi = {"008888", "011613", "020356"}
        if code in _semi:
            _sb = {}
            for d in sorted({x for c in _data for x in _data[c]}):
                _v = [_data[c][d] for c in _semi if c in _data and d in _data[c]]
                if len(_v) >= 2:
                    _sb[d] = sum(_v) / len(_v)
            _d2 = sorted(d for d in _data[code] if d in _sb)
            if len(_d2) >= 5:
                _r2 = factor_attribution([_data[code][d] for d in _d2],
                                         [_sb[d] for d in _d2], "半導體")
                if _r2["ok"]:
                    _s += (f"　| 對半導體板塊: β={_r2['beta']} R²={_r2['r2']} "
                           f"α={_r2['alpha_pct_ann']:+.1f}%")
        _s += f"　⚠️{_r['caveat']}"
        return _s
    except Exception as _e:
        _log_err(_e, "build_factor_attr_block")
        return f"⑩ 因子歸因: 數據讀取失敗({type(_e).__name__})"


# ── [V24.5 ⑤] 持股重疊度自動計算(零外部依賴,用錨股集合算 Jaccard) ─────────
ANCHOR_MAP = {
    "008888":    ["中芯國際", "北方華創", "中微公司", "寒武紀", "海光信息", "兆易創新", "滬矽產業"],
    "011613":    ["中芯國際", "海光信息", "中微公司", "寒武紀", "瀾起科技", "滬矽產業"],
    "018177":    ["中芯國際", "海光信息", "中微公司", "寒武紀", "瀾起科技"],
    "019072":    ["新易盛", "中際旭創", "天孚通信", "亨通光電", "中天科技", "中興通訊", "中國聯通"],
    "00406A.TW": ["台積電", "聯發科", "鴻海", "廣達", "緯穎", "欣興", "南亞電路板"],
    "00995A.TW": ["台積電", "聯發科", "鴻海", "緯穎", "奇鋐", "台光電", "欣興", "南亞電路板"],
    "00919.TW":  ["國泰金", "富邦金", "中信金", "廣達", "華碩"],
    "00982A.TW": ["台積電", "鴻海", "聯發科", "廣達", "中華電"],
    "00929.TW":  ["台積電", "鴻海", "聯發科", "聯電", "日月光"],
    "013180":    ["寧德時代", "比亞迪", "億緯鋰能", "陽光電源", "國軒高科"],
}


def compute_holding_overlap(pool=None):
    """[V24.5 ⑤] 用 ANCHOR_MAP 各檔錨股算 Jaccard 重疊,回傳重疊對 { (a,b):{jaccard,common} }。
    零外部依賴:只吃已在程式內的錨股集合,標出重複押注對(防你以為分散其實加重)。"""
    _pool = pool if pool is not None else ANCHOR_MAP
    codes = [c for c in _pool if _pool[c]]
    res = {}   # 原本未初始化:只要有任何重疊對就拋 NameError,盡調「單點風險/持股」欄位整段失效
    for i in range(len(codes)):
        for j in range(i + 1, len(codes)):
            a = set(_pool[codes[i]]); b = set(_pool[codes[j]])
            inter = a & b
            if not inter:
                continue
            union = a | b
            res[(codes[i], codes[j])] = {"jaccard": round(len(inter) / len(union), 2),
                                         "common": sorted(inter)}
    return res


# ── [V24.5] 已研究標的盡調實證庫(web 2026-08-04,前期研究,待本機複核) ─────────
def _load_dd_auto():
    """[V24.20] 讀 dd_fetcher 產出的 dd_meta_auto.json,自動併入 DD_META。

    ★使用者 2026-08-10 指正:「資料為什麼是人工填??找你就是要你抓資料自動填上」。
      我寫了 dd_fetcher 卻【沒有把它接進主系統】——抓到的資料躺在 json 裡,
      報告照樣顯示❓未驗證。**這正是我前幾輪批評的「功能存在 ≠ 功能接線」,自己又犯。**
    規則:自動值【覆蓋】人工值(自動的較新且有 as_of);人工獨有的維度保留。
    """
    # [V24.24] ★多路徑搜尋。根因:系統有【兩個】目錄常數——
    #   BASE_DIR     = ...\ETF_Monitor(上一層)  /  _PB_BASE_DIR = 主程式所在 = ...\代碼
    #   dd_fetcher 寫在【代碼】,原用 BASE_DIR 去上一層找 → 永遠「未找到」。
    #   ⚠️ V24.22 這段曾因 patch 行號位移【插進 compute_holding_overlap】而完全沒生效,
    #      改用字串替換重做(教訓:同一腳本多處插入必須由後往前,或改用字串定位)。
    _cands = [os.path.join(_d, "dd_meta_auto.json") for _d in
              (_PB_BASE_DIR, BASE_DIR, os.path.dirname(os.path.abspath(__file__)))]
    _p = next((x for x in _cands if os.path.exists(x)), None)
    if _p is None:
        return {}, ("⚠️ 未找到 dd_meta_auto.json — 已搜尋: "
                    + " | ".join(_cands) + " (請先跑 dd_fetcher.py)")
    try:
        with open(_p, "r", encoding="utf-8") as _fh:
            _raw = json.load(_fh) or {}
    except Exception as _e:
        _log_err(_e, "_load_dd_auto")
        return {}, f"⚠️ dd_meta_auto.json 解析失敗({_e})"
    _out, _n = {}, 0
    for _c, _m in _raw.items():
        _clean = {_k: _v for _k, _v in (_m or {}).items() if not _k.startswith("_")}
        if _clean:
            _asof = (_m or {}).get("_as_of", "")
            _out[_c] = {_k: (f"{_v}（自動 {_asof}）" if _asof and "自動" not in str(_v) else _v)
                        for _k, _v in _clean.items()}
            _n += len(_clean)
    return _out, f"✅ dd_meta_auto.json 併入 {len(_out)} 檔 / {_n} 維(自動值覆蓋人工值)"


DD_META = {
    "00406A.TW": {
        "結構": "主動型+covered call封頂(收益上限),月配(web 2026-08-04)",
        "成本": "主動型,經理費0.75%+保管0.035%=0.785%/年(web 2026-08-04,前期研究待複核)",
        "規模流動性": "AUM約151.74億台幣,月配+收益平準金,折溢價待查(web 2026-08-04)",
        "配息": "月配息+收益平準金(web 2026-08-04)",
        "經理人": "張書廷(中信投信,同管00995A)",
        "位置": "發行10.00(6/11)→高10.60(6/22)→低8.06(7/30)→~9.00(8/4),距高點約-15%(web 2026-08-04)",
        "結構vs能力歸因": "成立-11.9%主因covered-call封頂+發行時點差(7月科技崩),非選股爛(web 2026-08-04)",
    },
    "00995A.TW": {
        "結構": "主動型,季配,無收益平準金(web 2026-08-04)",
        "成本": "主動型,經理費0.75%+保管0.035%=0.785%/年,無收益平準金(web 2026-08-04)",
        "規模流動性": "AUM快速成長中,季配,折溢價待查(web 2026-08-04)",
        "配息": "季配息(web 2026-08-04)",
        "經理人": "張書廷(中信投信,同管00406A)——同經理人重複押注需計",
        "位置": "發行10→高~19(上半年)→低13.77(7/30)→~15(8/4),高基期(web 2026-08-04)",
        "結構vs能力歸因": "成立以來>60%優於大盤/0050,是張書廷alpha直接證據;但含高基期與單邊牛市樣本偏誤(web 2026-08-04)",
    },
    "00919.TW": {
        "結構": "被動指數型(高息策略),季/月配(web 2026-08-04)",
        "成本": "經理費0.35%+保管0.04%(web 2026-08-04,前期研究待複核)",
        "規模流動性": "高息ETF龍頭之一,AUM千億級,季/月配(web 2026-08-04)",
        "配息": "季配/月配高息(web 2026-08-04)",
        "經理人": "群益投信(web 2026-08-04)",
        "位置": "追特選台灣高息指數,前10大71.23%金融主導(web 2026-08-04)",
        "結構vs能力歸因": "高息策略,與AI成長β低相關,防禦/现金流屬性(web 2026-08-04)",
    },
    "00982A.TW": {
        "結構": "主動/指數型科技,月配(web 2026-08-04)",
        "成本": "經理費約0.6-0.8%(web 2026-08-04,待複核)",
        "規模流動性": "台股科技主動/指數型,流動性佳(web 2026-08-04)",
        "配息": "月配息(群益台灣精選高科技,web 2026-08-04)",
        "經理人": "群益投信(web 2026-08-04)",
        "位置": "台股AI供應鏈純β,漲多高基期(web 2026-08-04)",
        "結構vs能力歸因": "純科技成長,無call封頂,與00929/00406A部分重疊(web 2026-08-04)",
    },
    "00929.TW": {
        "結構": "被動指數型科技優息,月配(web 2026-08-04)",
        "成本": "經理費約0.6-0.8%(web 2026-08-04,待複核)",
        "規模流動性": "台股科技優息,流動性佳(web 2026-08-04)",
        "配息": "月配息(復華台灣科技優息,web 2026-08-04)",
        "經理人": "復華投信(web 2026-08-04)",
        "位置": "台股科技+高息,與00982A/00406A部分重疊(web 2026-08-04)",
        "結構vs能力歸因": "科技優息,含部分call或高息結構,與純科技β有差(web 2026-08-04)",
    },
    "013180": {
        "結構": "被動指數聯接(國證新能電池159755),不配息(web 2026-08-04)",
        "成本": "指數聯接C類,申購0費+管理0.5%+託管0.15%(web 2026-08-04)",
        "規模流動性": "AUM約17.3億(2026-08-03),被動指數聯接(國證新能電池),web 2026-08-04",
        "配息": "不配息(資本增益型,web 2026-08-04)",
        "經理人": "羅國慶(廣發基金,2021-08-16上任至今,web 2026-08-04)",
        "位置": "國證新能電池(159755)聯接,跟蹤新能電池指數,與持倉零重疊分散(web 2026-08-04)",
        "結構vs能力歸因": "被動指數聯接,2026新能車/儲能景氣修復,與持倉零重疊(web 2026-08-04)",
    },
    "008888": {
        "結構": "被動指數聯接C(投資華夏國證半導體芯片ETF),追蹤國證半導體芯片指數;力爭日均跟蹤偏離度<0.35%、年跟蹤誤差<4%(web 2026-08-04·天天基金/同花順)",
        "成本": "管理費0.50%+託管費0.10%=0.60%/年;C類無申購費,持有<7日贖回費1.50%(web 2026-08-04·同花順)",
        "規模流動性": "淨資產89.19億元(2026-03-31),份額66.71億份(2025-12-31),規模大流動性佳(web 2026-08-04·天天基金)",
        "經理人": "趙宗庭(華夏基金,17年證券從業,2016/11加入,2020/01起管理本ETF);被動型,經理人alpha非關鍵(web 2026-08-04)",
        "配息": "指數型不配息,收益滾入淨值(web 2026-08-04)",
        "結構vs能力歸因": "被動追蹤,漲跌幾乎全由國證半導體芯片指數決定;跑輸贏大盤是板塊beta非選股(web 2026-08-04)",
    },
    "019072": {
        "結構": "被動指數聯接(國證通信),不配息(web 2026-08-04)",
        "成本": "場外聯接,經理費約0.5-0.8%(web 2026-08-04,待複核)",
        "規模流動性": "嘉實國證通信聯接,規模中(web 2026-08-04)",
        "配息": "不配息(指數型,web 2026-08-04)",
        "經理人": "嘉實基金(web 2026-08-04)",
        "位置": "國證通信=純光通信+運營商,與ABF/載板零重疊(web 2026-08-04)",
        "結構vs能力歸因": "光通信β,與PCB/載板鏈不重疊(web 2026-08-04)",
    },
}

# [V24.20] ★自動盡調資料併入(dd_fetcher 產出) — 放在 DD_META 定義之後才能覆蓋
DD_AUTO_TTL_DAYS = int(os.getenv("DD_AUTO_TTL_DAYS", "7"))   # [V24.28] 自動盡調保鮮期
# 不產生報告的指令:不必為它們先花 30-90 秒連網抓盡調
_NO_REPORT_ARGS = {"-h", "--help", "--list-positions", "--set-position", "--remove-position"}


def _ensure_dd_auto():
    """[V24.28] dd_meta_auto.json 過期/不存在時【自動呼叫 dd_fetcher 重抓】。

    ★使用者 2026-08-10:「以後跑4報前,我就非得要先自己跑一次 dd_fetcher 嗎?」
      不該。`news_fetcher` 早就有「缺了就自動補抓」的模式(load_news_csv),
      dd_fetcher 我卻只寫了模組【沒接觸發器】——又是「功能存在 ≠ 功能接線」。
    設計:
      - TTL 預設 7 天(可用環境變數 DD_AUTO_TTL_DAYS 調整)。
        盡調多為慢變資料(費率/經理人/持股),但「位置/近一年報酬」是快變的,故不用 30 天。
      - 代碼清單【從實際持倉 + 觀察池自動取】,不再寫死 7 檔。
      - 設 DD_AUTO_REFRESH=0 可完全關閉自動重抓。
      - 抓取需數十秒 → 只在過期時跑,並事先告知預估耗時。
      - 任何失敗都優雅降級:沿用舊 json,絕不阻斷四報。
    """
    if os.getenv("DD_AUTO_REFRESH", "1") == "0":
        return "ℹ️ DD_AUTO_REFRESH=0,略過盡調自動更新"
    if _NO_REPORT_ARGS.intersection(sys.argv[1:]):
        return "ℹ️ 未執行報告(說明/持倉指令),略過盡調自動更新"
    _cands = [os.path.join(_d, "dd_meta_auto.json") for _d in
              (_PB_BASE_DIR, BASE_DIR)]
    _p = next((x for x in _cands if os.path.exists(x)), None)
    _age = None
    if _p:
        try:
            _age = (datetime.datetime.now()
                    - datetime.datetime.fromtimestamp(os.path.getmtime(_p))).days
        except Exception:
            _age = None
        if _age is not None and _age <= DD_AUTO_TTL_DAYS:
            return f"ℹ️ 盡調資料 {_age} 日前更新(TTL {DD_AUTO_TTL_DAYS} 日),不重抓"
    _why = "不存在" if not _p else f"已 {_age} 日未更新"
    try:
        import importlib as _il
        _sys_path_added = False
        if _PB_BASE_DIR not in sys.path:
            sys.path.insert(0, _PB_BASE_DIR); _sys_path_added = True
        _dd = _il.import_module("dd_fetcher")
        _codes = sorted(set(list(USER_HOLDINGS.keys())
                            + list(WATCHLIST_ETFS.keys())))
        _codes = [c for c in _codes if c.isdigit() and len(c) == 6]   # 台股走別的源
        print(f"   ⏳ 盡調資料{_why} → 自動更新 {len(_codes)} 檔(約需 30-90 秒,"
              f"設 DD_AUTO_REFRESH=0 可關閉)...")
        _meta, _fail = _dd.build_dd_meta(_codes)
        if _sys_path_added:
            sys.path.remove(_PB_BASE_DIR)
        if not _meta:
            return f"⚠️ 盡調自動更新未取得任何資料(失敗 {len(_fail)} 檔),沿用既有值"
        _out = os.path.join(_PB_BASE_DIR, "dd_meta_auto.json")
        with open(_out, "w", encoding="utf-8") as _fh:
            json.dump(_meta, _fh, ensure_ascii=False, indent=2)
        return (f"✅ 盡調自動更新完成:{len(_meta)} 檔已寫入"
                + (f"(另 {len(_fail)} 檔失敗)" if _fail else ""))
    except ImportError:
        return ("⚠️ 找不到 dd_fetcher.py(請放在與主程式同一資料夾) → "
                "盡調資料不會自動更新,沿用既有 json")
    except Exception as _e:
        _log_err(_e, "_ensure_dd_auto")
        return f"⚠️ 盡調自動更新失敗({_e}),沿用既有 json"


_DD_REFRESH_MSG = _ensure_dd_auto()
if _DD_REFRESH_MSG:
    print(f"   {_DD_REFRESH_MSG}")
_DD_AUTO, _DD_AUTO_MSG = _load_dd_auto()
for _ac, _am in (_DD_AUTO or {}).items():
    DD_META.setdefault(_ac, {}).update(_am)
if _DD_AUTO_MSG:
    print(f"   {_DD_AUTO_MSG}")





def _auto_single_point_risk(code):
    _risk = []
    for _m, _codes in scan_duplicate_manager().items():
        if code in _codes and len(_codes) >= 2:
            _risk.append(f"同經理人重複押注:{_m}({'/'.join(_codes)})")
    for (_a, _b), _v in compute_holding_overlap().items():
        if code in (_a, _b) and _v["jaccard"] >= 0.3:
            _other = _b if code == _a else _a
            _risk.append(f"持股重疊{_v['jaccard']*100:.0f}%與{_other}(共:{','.join(_v['common'][:4])})")
    # [V24.5 Q5] 掃描無異常 → 明確回傳「零重疊」而非 None,消滅❓未驗證死結
    if _risk:
        return "⚠️ " + "; ".join(_risk)
    return "✅ 無同經理人重複 / 錨股重疊<30% → 單點風險分散"


def _auto_holding_overlap(code):
    _pairs = []
    for (_a, _b), _v in compute_holding_overlap().items():
        if code in (_a, _b):
            _other = _b if code == _a else _a
            _pairs.append(f"{_other}({_v['jaccard']*100:.0f}%:{','.join(_v['common'][:3])})")
    # [V24.5 Q5] 無重疊對手 → 明確回傳「零重疊」,不留❓
    if _pairs:
        return "⚠️ 重疊對手:" + "; ".join(_pairs)
    return "✅ 與觀察池錨股零重疊 → 無重複押注"




class QuantEnhanced:

    @staticmethod
    def rsi_reversal_prob(rsi):
        """[V24.13] 桶區間化 — 消除假精確但不剝奪資訊。

        ★根因(patch_v24_11 修補⑤指出,本線採納其洞見但不採其做法):
          原輸出「RSI=73→回調概率45%」會讓人誤以為 45% 是【對 RSI=73 這個值】算出來的,
          但它是【分桶常數】——RSI 71 與 75 同樣吐 45%,同桶內完全不隨 RSI 變動。
          patch 主張「未回測就完全不輸出機率」;本線改為【明示桶邊界】,理由:
          完全不輸出會損失資訊,而系統原則是「可以不背書,但不得剝奪用戶的選擇權」。
          明示桶邊界後,用戶自己看得出這是查表不是模型輸出,假精確即消除。
        回測校準後應改為輸出「次日正報酬頻率 X%(樣本n=,回測期=)」並移除本註記。
        """
        _NC = "⚠️分桶常數,同桶內不隨RSI變動·未回測"
        if rsi > 85:   return 0.78, f"RSI={rsi:.0f}→極度超買(桶:RSI>85)，查表值78%·{_NC}"
        elif rsi > 75: return 0.58, f"RSI={rsi:.0f}→超買(桶:75<RSI≤85)，查表值58%·{_NC}"
        elif rsi > 70: return 0.45, f"RSI={rsi:.0f}→偏熱(桶:70<RSI≤75)，查表值45%·{_NC}"
        elif rsi < 25: return 0.81, f"RSI={rsi:.0f}→極度超賣(桶:RSI<25)，查表值81%·{_NC}"
        elif rsi < 30: return 0.72, (f"RSI={rsi:.0f}→超賣(桶:25≤RSI<30)，查表值72%·{_NC}"
                                     f"(寬基統計,高波動下跌趨勢中高估)")
        elif rsi < 35: return 0.55, f"RSI={rsi:.0f}→偏冷(桶:30≤RSI<35)，查表值55%·{_NC}"
        return 0.0, ""

    @staticmethod
    def momentum_persistence(trend, z_score, bias):
        if trend >= 0.8 and z_score > 0 and bias > 0.05:
            return "📈 強趨勢持續（動能充足）"
        elif trend >= 0.5 and z_score < -1.0:
            return "⚡ 趨勢遭Z-Score壓制，潛在技術反彈"
        elif trend < 0.2 and abs(bias) > 0.10:
            return "↩️ 弱趨勢+大乖離，均值回歸概率高"
        elif trend < 0 and z_score < -1.5:
            return "🔽 趨勢轉空+Z偏低，下行風險上升"
        return ""

    @staticmethod
    def composite_risk_score(rsi, z_score, bias, trend, ev):
        risk, reasons = 0, []
        if rsi > 85: risk += 3; reasons.append("RSI極度超買")
        elif rsi > 75: risk += 2; reasons.append("RSI超買")
        if abs(bias) > 0.15: risk += 2; reasons.append(f"乖離{bias:.0%}過大")
        elif abs(bias) > 0.10: risk += 1; reasons.append("乖離偏高")
        if abs(z_score) > 2.0: risk += 2; reasons.append(f"Z={z_score:.1f}極端")
        elif abs(z_score) > 1.5: risk += 1; reasons.append(f"Z={z_score:.1f}偏離")
        if ev < 25: risk += 2; reasons.append("EV極低")
        elif ev < 35: risk += 1; reasons.append("EV偏低")
        if risk >= 6: return "🔴 高風險", " | ".join(reasons)
        elif risk >= 4: return "🟠 中高風險", " | ".join(reasons)
        elif risk >= 2: return "🟡 中風險", " | ".join(reasons)
        return "🟢 低風險", " | ".join(reasons)

    @staticmethod
    def build_narrative(info: dict, tech: dict, ndx_d: float,
                        vix: float, c_drain: float, c_drain_reason: str = "") -> str:
        """
        [V22.9] 差異化敘事 + RSI動量一致性修復

        修復：
        1. 非科技ETF不再出現「科技股小幅走高」
        2. 超賣保護時動量描述與信號一致（不再矛盾）
        """
        style = info["style"]
        name = info["name"]
        rsi = tech.get("RSI", 50)
        trend = tech.get("Trend_Strength", 0)
        z = tech.get("Z_Score", 0)
        is_qdii = _is_qdii_etf(info)
        is_tw   = info["market"] == "台股"

        # 地緣敘事（按Style）
        geo_key = "Tech_QDII" if is_qdii else ("Tech_TW" if is_tw else style)
        # [V22.96 ③動態] 地緣敘述跟實際 c_drain 走,不再永遠「降溫」(今天 WTI 暴漲卻寫降溫的硬傷)
        _cdr = c_drain_reason or "能源/避險溢價升溫"
        if c_drain >= 0.4:
            geo_text = (f"地緣/能源風險升溫(c_drain={c_drain:.2f}:{_cdr}) "
                        f"→ 壓科技風險偏好,外部約束增強,防禦優先。")
        elif c_drain >= 0.25:
            geo_text = f"地緣/能源風險中性偏高(c_drain={c_drain:.2f}:{_cdr}),方向待觀察。"
        else:
            # [V23.1 殺靜態fallback] 不再回退寫死的「降溫」死文字,改跟實際低 c_drain 動態
            geo_text = (f"地緣/能源風險偏低(c_drain={c_drain:.2f}:{_cdr}),"
                        f"外部約束緩和,科技風險偏好有回升空間。")

        # [V24.0 修A] 地緣敘述原本只跟全局 c_drain → 16 檔逐字相同。此處補「該資產的傳導路徑」,
        #   讓同一個 c_drain 對不同資產講出不同含意(治「模板逐字複製」)。
        _geo_ch = {
            "Tech":      "對本檔的傳導:油價→製造成本/海運→半導體設備交期,且風險偏好下降時科技最先被減。",
            "Defensive": "對本檔的傳導:地緣升溫時黃金受益(避險買盤),與科技反向,c_drain 高對本檔偏多。",
            "Dividend":  "對本檔的傳導:紅利低波受地緣影響小,主要看無風險利率與資金避風港需求。",
            "Utility":   "對本檔的傳導:內需公用事業與海運/地緣脫鉤,主要看電價與用電量,非地緣敏感。",
            "Power":     "對本檔的傳導:內需電力與地緣脫鉤,主要看迎峰度夏用電與電網投資。",
            "Cyclical":  "對本檔的傳導:有色/週期直接吃能源與運費,地緣升溫時成本與價格同向,雙面影響。",
            "Tech_QDII": "對本檔的傳導:QDII 走美股定價,地緣經 VIX/美債殖利率傳導,匯率亦影響換匯後報酬。",
            "Tech_TW":   "對本檔的傳導:台股AI供應鏈受 SOX 與海運/地緣情緒連動,外資流向為關鍵中介。",
        }.get(geo_key, "")
        if _geo_ch:
            geo_text = geo_text.rstrip("。") + "。" + _geo_ch

        # 宏觀情緒 [V23.1 修矛盾] c_drain 高(risk-off)時 VIX 不得說「回升」,與 geo_text 協調
        if c_drain >= 0.4:
            vix_text = f"VIX={vix:.1f},惟地緣/能源風險升溫壓過情緒 → 避險優先。"
        else:
            vix_text = ("市場恐慌情緒升溫，避險優先。" if vix > 22 else
                        "風險偏好回升，避險資金成本降至低點。" if vix < 19 else
                        "市場情緒中性，震盪等待方向。")

        # [V22.9修復] 僅科技/QDII/台股 才提及美股方向
        if style in ("Tech", "OptComm") or is_qdii or is_tw:   # [V23.1] OptComm(019072通信)也走科技敘事
            mkt_text = ("科技股小幅走高，隔夜信心持穩。" if ndx_d > 0.003 else
                        "科技股小幅回調，隔夜獲利了結壓力。" if ndx_d < -0.003 else
                        "美股整體橫向，方向待確認。")
        else:
            # 非科技類：Defensive/Dividend/Cyclical/Utility 用資產自身敘事
            style_mkt = {
                "Defensive": "黃金避險需求邊際變化，注意美元走勢對貴金屬的壓制效應。",
                "Dividend":  "無風險利率走勢影響紅利溢價，關注國債收益率動向。",
                "Cyclical":  "商品需求端信號待確認，關注中國製造業PMI數據。",
                "Utility":   "電力需求季節性因素及新能源裝機進度為核心驅動。",
            }
            mkt_text = style_mkt.get(style, "宏觀整體待觀察。")

        # 地緣額外說明
        drain_text = f" 地緣/能源複合風險 c_drain={c_drain:.2f}（{c_drain_reason}；非單一事件，VIX+油價+金價滾動合成）。" if c_drain >= 0.3 else ""

        # RSI技術敘事
        if rsi > 75:   rsi_key = "overbought"
        elif rsi > 60: rsi_key = "hot"
        elif rsi > 45: rsi_key = "neutral"
        elif rsi > RSI_OVERSOLD: rsi_key = "cool"
        else:          rsi_key = "oversold"
        rsi_text = RSI_NARRATIVES[rsi_key]

        # [V22.9修復] RSI超賣時 Z-Score警示改為反彈提示（不矛盾）
        if abs(z) > 1.5:
            if rsi < RSI_OVERSOLD:
                # 超賣 + Z偏低：強化反彈信號，而非警示下行
                rsi_text += "Z-Score負偏疊加超賣，技術性反彈動能進一步累積。"
            else:
                rsi_text += RSI_NARRATIVES["z_warn"]

        return (
            f"### 🌍 宏觀資金流向與地緣推演\n"
            f"{geo_text.rstrip(chr(12290))}；{vix_text} {mkt_text}{drain_text}\n"   # [V23.1] 去尾。防「。；」
            f"### 📊 量化指標與Z-Score穿透解析\n"
            f"{rsi_text}\n"
        )


# ══════════════════════════════════════════════════════════════════════
# 📊 PerformanceAttributor — 績效歸因+T+1補寫
# ══════════════════════════════════════════════════════════════════════
class PerformanceAttributor:

    @classmethod
    def backfill_yesterday_actuals(cls):
        """早報前補寫最近一個已公布 NAV 交易日的A股 actual_ret。

        原本以「日曆昨天」為日期、取 NAV 序列【最後兩筆】算報酬,有兩個錯配:
          ① 週一的「昨天」是週日,週五的報酬永遠補不進去,且最後兩筆被標成週日;
          ② NAV 延遲公布(QDII 常晚 1~2 天)或晚上跑(今天 NAV 已公布)時,
             最後兩筆不是昨天的報酬,卻被標成昨天。
        改用帶日期的 NAV(_dated_nav):取今天以前的 NAV 日 d1 與其前一筆 d0,
        報酬落在 d1 —— NAV 自己的日期,不靠日曆推算。

        官方 NAV 是場外基金的實現報酬(src=nav,最高等級):
          - 原本「當日已有 actual_ret 就跳過」,而收盤時場內代理已先寫入,NAV 幾乎永遠補不進去;
            現改為只在 realized_returns 已有 NAV 時跳過,代理值會被 NAV 覆蓋。
          - 原本只補最新一個 NAV 日,漏跑幾天就永遠缺;現對近 10 筆 NAV 的每一對相鄰日都檢查。"""
        today = datetime.date.today().isoformat()
        n = 0
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            for code, info in TARGET_ETFS.items():
                if info["market"] != "A股": continue
                try:
                    _dn = [x for x in (_dated_nav(code, limit=10) or []) if x[0] < today]
                    n += cls._apply_nav_pairs(conn, code, info, _dn)
                except Exception as _e:
                    _log_err(_e, "backfill_yesterday_actuals")
        print(f"   [績效師] T+1補寫完成：{n}檔")
        return n

    @classmethod
    def _apply_nav_pairs(cls, conn, code, info, dated_navs, verbose=True):
        """dated_navs:[(日期, NAV)] 升序。對每一對相鄰 NAV 日 (d0, d1) 把 d1 的報酬以 src=nav 落位。
        realized_returns 已有該日 NAV 者跳過(冪等)。回寫入的 predictions 筆數。"""
        n = 0
        for (_d0, _v0), (_d1, _v1) in zip(dated_navs, dated_navs[1:]):
            _have = returns_store.get(conn, code, _d1)
            if _have and returns_store.rank(_have[1]) >= returns_store.AUTHORITATIVE:
                continue
            if not _v0:
                continue
            ret = (_v1 - _v0) / _v0
            if abs(ret) <= 1e-6:
                continue
            # [V22.39] 語義分離：ret = NAV 日 _d1 的實現收益。
            #   T+0(盤前/早/午 dated=_d1)配 ret；
            #   T+1(緊鄰前一運行日的收盤報,它預測 _d1)配 ret。
            _n0, _n1 = _apply_actual_by_semantic(conn, code, _d1, ret, src="nav")
            n += (_n0 + _n1)
            # [V22.33.7 治D8死碼] backfill_t1_actual 定義後【零呼叫】→ daily_snapshot.t1_actual
            #   永遠是空 → T+1 預測無實際值配對 → 精度校準鏈斷(只能靠估計)。
            #   此處已有現成的真實 ret,零額外網路成本,直接回填。
            backfill_t1_actual(conn, code, _d1, ret)
            if verbose:
                print(f"   ✅ NAV實現報酬 {info['name'][:8]} {_d1}: {ret:+.3%}")
        return n

    @classmethod
    def backfill_taiwan_today_actuals(cls):
        """
        [V22.17.1 新增 5/29] 台股當日收盤即時補寫 actual_ret
        
        根因：台股 ETF（00982A/00929）當日收盤就有價，不需 T+1
        效益：收盤報生成時，台股的 actual_ret 已就位
              → daily_pnl_report 能立即顯示「實際 vs 預測」
              → 四報校正鏈台股欄不再「T+1待確認」
              → 學習速度從 T+1 → T+0
        
        使用：在收盤報流程 daily_pnl_report 之前調用
        資料源：DirectDataFetcher.get_yahoo_dated_closes(code)

        日期修正:原本取收盤序列最後兩筆並標為「今天」。台股休市日(或盤中)執行時,最後一筆
        是前一交易日(或未收盤的半根K棒),報酬被配到錯的日期。現改用帶日期的收盤價:
        每一對相鄰交易日 (d0, d1) 的報酬落在 d1;今天的K棒只在 14:00(台北)後、確定收盤才採用。
        交易所收盤價就是該上市 ETF 本身的成交價 → src=exch_close(最高等級)。
        """
        now = _now_utc8()
        today = now.date().isoformat()
        n = 0
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            for code, info in TARGET_ETFS.items():
                if info["market"] != "台股": continue
                try:
                    dated = [(d, c) for d, c in DirectDataFetcher.get_yahoo_dated_closes(code)
                             if d < today or (d == today and now.hour >= 14)]
                    if len(dated) < 2:
                        print(f"   ⚠️ {code} Yahoo 資料不足（{len(dated)} 點）")
                        continue
                    _tail = dated[-10:]
                    for (_d0, _c0), (_d1, _c1) in zip(_tail, _tail[1:]):
                        _have = returns_store.get(conn, code, _d1)
                        if _have and returns_store.rank(_have[1]) >= returns_store.AUTHORITATIVE:
                            continue
                        if not _c0:
                            continue
                        ret = (_c1 - _c0) / _c0
                        if abs(ret) <= 1e-6:
                            continue
                        # [V22.39] 語義分離：ret=台股 _d1 實現收益。
                        #   T+0(盤前/早/午 dated=_d1)配 ret；T+1(緊鄰前一運行日的收盤報)配 ret。
                        _n0, _n1 = _apply_actual_by_semantic(conn, code, _d1, ret, src="exch_close")
                        n += (_n0 + _n1)
                        print(f"   ✅ 台股實現報酬 {info['name'][:8]}({code}) {_d1}: "
                              f"收盤={_c1:.2f} ret={ret:+.3%}")
                except Exception as e:
                    print(f"   ⚠️ {code} 台股當日補寫失敗: {e}")
        print(f"   [績效師 V22.17.1] 台股當日 actual 補寫完成：{n}檔")
        return n

    @classmethod
    def backfill_a_stock_today_actuals(cls):
        """
        [V22.17.3 新增 5/29] A股當日 actual 即時補寫（用場內代理）
        
        根因：5/29 用戶提案 — 場內 ETF 16:00 收盤就有價
                場外聯接基金 NAV 要等晚上，但場內代理當日就準
        
        效益：
          - 收盤報生成時 actual_ret 即時就位
          - 不再 T+1 backfill
          - 四報校正鏈當天就有「實際 %」
          - 系統 pred vs 真實 命中度當天就能評估
        
        資料源：DirectDataFetcher.get_sina_proxy_ret(proxy) — 已有
        條件：只在 A股 ETF 且 proxy 存在 + sina 抓到合理值（abs > 0.0001）

        來源等級:場內代理只是場外基金的【估計】(有追蹤誤差,增強型更大)→ src=proxy(最低級)。
          隔天官方 NAV 公布後,backfill_yesterday_actuals 會以 src=nav 覆蓋。
        日期守衛:新浪報價在休市日仍顯示上一交易日的漲跌,盤中則是未收盤的值;
          兩種情況都不是「今天的實現報酬」,一律不寫(原本會照寫並標成今天)。
        """
        now = _now_utc8()
        today = now.date().isoformat()
        n = 0
        report_rows = []  # 給驗證區塊用
        if not is_trading_day(now.date(), "A股") or now.hour < 15:
            print(f"   [績效師] {today} 非A股交易日或未收盤,不以場內代理寫入當日實現報酬")
            return n, report_rows
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            for code, info in TARGET_ETFS.items():
                if info.get("market") != "A股": continue
                if _is_qdii_etf(info): continue   # [V22.27.9 #162] QDII場內代理≠場外NAV,不覆寫;留NULL等場外T+2回填(backfill_yesterday_actuals)
                proxy = info.get("proxy")
                if not proxy: continue
                # 跳過已有實現報酬者(任何來源;代理是最低級,不可能升級既有值)
                if returns_store.get(conn, code, today):
                    continue
                try:
                    ret = DirectDataFetcher.get_sina_proxy_ret(proxy)
                    if ret is not None and abs(ret) > 0.0001:   # [V24.14] 防 abs(None) TypeError
                        # [V22.27.5 Fix B] 命中表用「早報 pre-move」預測,不用最後一筆
                        #   (收盤報19:37生成,趨勢component已含當日收盤=循環;早報10:03用T-1 NAV=pre-move)
                        pred_row = conn.execute(
                            "SELECT predicted_ret FROM predictions "
                            "WHERE code=? AND date(ts)=? AND report_type='早報' "
                            "ORDER BY ts DESC LIMIT 1",
                            (code, today)).fetchone()
                        if not (pred_row and pred_row[0]):
                            # fallback: 最早一筆非收盤報(盤前/午報)
                            pred_row = conn.execute(
                                "SELECT predicted_ret FROM predictions "
                                "WHERE code=? AND date(ts)=? "
                                "AND (report_type IS NULL OR report_type='' OR report_type!='收盤報') "
                                "ORDER BY ts ASC LIMIT 1",
                                (code, today)).fetchone()
                        has_pred = bool(pred_row and pred_row[0])
                        pred = pred_row[0] if has_pred else 0.0
                        # [V22.39] 語義分離：ret=A股當日(today)場內代理實現收益。
                        #   T+0(盤前/早/午 dated=today)配 ret；
                        #   T+1(收盤報 dated<today 最近未回填,它預測 today)配 ret。
                        _n0, _n1 = _apply_actual_by_semantic(conn, code, today, ret, src="proxy")
                        n += (_n0 + _n1)
                        # 命中判定
                        # 當日沒有盤前預測時不判命中:原本 pred 退回 0.0,(ret>0)==(0>0) 讓所有下跌都算命中
                        dir_hit = (ret > 0) == (pred > 0) or (abs(ret) < 0.003 and abs(pred) < 0.005)
                        report_rows.append({
                            "code": code, "name": info["name"][:8],
                            "pred": pred, "actual": ret,
                            "hit": ("✅" if dir_hit else "❌") if has_pred else "—",
                            "proxy": proxy,
                        })
                        print(f"   ✅ A股當日補寫 {info['name'][:8]}({code}) "
                              f"via {proxy}: pred={pred*100:+.2f}% / 真實={ret*100:+.2f}%")
                except Exception as e:
                    print(f"   ⚠️ {code} A股當日補寫失敗: {e}")
        print(f"   [績效師 V22.17.3] A股當日 actual 補寫完成：{n}檔")
        return n, report_rows

    @classmethod
    def daily_pnl_report(cls, results, date_str=None):
        """P&L歸因報告"""
        if not date_str:
            date_str = datetime.date.today().isoformat()
        lines = [
            "\n## 📊 今日信號績效歸因\n",
            "| ETF | 早報信號 | 早報預測 | 實際收益 | 有效 | 評論 |",
            "|-----|---------|---------|---------|------|------|",
        ]
        avoided = missed = ok = total = 0
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            for r in results:
                code = r.get("code","")
                name = TARGET_ETFS.get(code,{}).get("name",code)[:8]
                mrow = conn.execute(
                    "SELECT ROUND(predicted_ret*100,2), action FROM predictions "
                    "WHERE code=? AND date(ts)=? ORDER BY ts ASC LIMIT 1",
                    (code, date_str)).fetchone()
                if not mrow: continue
                m_pred, m_action = mrow
                arow = conn.execute(
                    "SELECT ROUND(actual_ret*100,2) FROM predictions "
                    "WHERE code=? AND date(ts)=? AND actual_ret IS NOT NULL "
                    "AND ABS(actual_ret)>0.0001 ORDER BY corrected DESC LIMIT 1",
                    (code, date_str)).fetchone()
                actual = arow[0] if arow else None
                if actual is None:
                    lines.append(f"| {name} | {(m_action or '')[:6]} | {m_pred or 0:+.2f}% | T+1 | — | — |")
                    continue
                total += 1
                p_dir = 1 if (m_pred or 0)>0.1 else (-1 if (m_pred or 0)<-0.1 else 0)
                a_dir = 1 if actual>0.1 else (-1 if actual<-0.1 else 0)
                correct = (p_dir == a_dir) if p_dir != 0 else True
                if correct: ok += 1
                comment = ""
                if "減倉" in (m_action or ""):
                    if actual < -0.1: avoided += abs(actual); comment = f"🛡️ 避開{abs(actual):.2f}%"
                    elif actual > 0.5: missed += actual; comment = f"😢 錯失{actual:.2f}%"
                elif "主攻" in (m_action or ""):
                    if actual > 0.1: comment = f"🎯 命中+{actual:.2f}%"
                    elif actual < -0.5: comment = f"⚠️ 失準{actual:.2f}%"
                lines.append(f"| {name} | {(m_action or '')[:6]} | {m_pred or 0:+.2f}% "
                             f"| {actual:+.2f}% | {'✅' if correct else '❌'} | {comment} |")
        acc = f"{ok/total:.1%}" if total else "N/A"
        lines.append(f"\n**今日方向精度：{ok}/{total} = {acc}**")
        if avoided > 0: lines.append(f"🛡️ 信號避開損失：{avoided:.2f}%")
        if missed  > 0: lines.append(f"😢 減倉錯失漲幅：{missed:.2f}%")
        return "\n".join(lines) + "\n"


# ══════════════════════════════════════════════════════════════════════
# 📐 Kalman 簡化學習（收盤報使用）
# ══════════════════════════════════════════════════════════════════════
class KalmanLearner:
    """
    收盤報W值自適應更新
    [V22.8新增] V22.7完全缺少此功能，導致W值永不更新

    更新規則(可交易口徑、雙向):
      原規則 ΔW = 0.005×(0.5 − 10×當日平均誤差),平衡點在「日均誤差 5%」;ETF 日誤差幾乎都 <5%,
      W 因此只會單向上升(日誤差 1% 時約 185 個收盤報升到上限 0.9),與預測好壞無關。且所用的
      「當日預測 vs 當日實際」是盤中同期推估(nowcast),不是可交易的預測。
      現改為:最近 LOOKBACK_DAYS 個交易日內,每個交易日截單前(早/午報)的訊號方向 vs 次一交易日實際報酬 → 技巧分數
        s = (命中數 − 未命中數) / 樣本數 ∈ [−1, 1],ΔW = LEARNING_SCALE × 0.01 × s。
      命中率 50%(無技巧)時 ΔW 的期望為 0;優於擲硬幣才升、劣於則降。樣本 < MIN_SAMPLES 不更新。
    """
    LEARNING_SCALE = 0.5
    W_MIN, W_MAX = 0.2, 0.9
    MIN_SAMPLES = 10     # 視窗內「訊號→次日」配對數下限
    LOOKBACK_DAYS = 20   # 滾動視窗(交易日);台股僅 2 檔,逐日樣本永遠不足,故改視窗

    @classmethod
    def _pairs(cls, conn, today):
        """回 {市場: [(前一交易日訊號, 該日實際報酬), ...]},涵蓋截至 today 的最近 LOOKBACK_DAYS 個交易日。"""
        out = {"台股": [], "A股": []}
        days = [r[0] for r in conn.execute(
            "SELECT DISTINCT date(ts) FROM predictions WHERE report_type IN ('早報','午報') "
            "AND date(ts) <= ? ORDER BY 1 DESC LIMIT ?", (today, cls.LOOKBACK_DAYS + 1))][::-1]
        if len(days) < 2:
            return out
        sig = {}
        act = {k: v for k, v in returns_store.load_map(conn).items() if abs(v) > 0.0001}   # 實現報酬單一來源
        for code, d, rt, pred, a, ts in conn.execute(
                "SELECT code, date(ts), report_type, predicted_ret, actual_ret, ts FROM predictions "
                "WHERE report_type IN ('早報','午報') AND date(ts) BETWEEN ? AND ?", (days[0], days[-1])):
            if pred is not None and pred != 0.0:
                rank = (rt == "午報", ts or "")
                if (code, d) not in sig or rank > sig[(code, d)][0]:
                    sig[(code, d)] = (rank, pred)
        for prev, d in zip(days, days[1:]):
            for code, info in TARGET_ETFS.items():
                if (code, prev) in sig and (code, d) in act and info.get("market") in out:
                    out[info["market"]].append((sig[(code, prev)][1], act[(code, d)]))
        return out

    @classmethod
    def update(cls, brain: dict, results: list) -> dict:
        today = datetime.date.today().isoformat()
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            pairs = cls._pairs(conn, today)

        kal = brain.setdefault("kalman", {})

        def _update_w(state, prs, label):
            if len(prs) < cls.MIN_SAMPLES:
                return state
            W = state.get("W", 0.481)
            hits = sum(1 for p, a in prs if (p > 0) == (a > 0))
            s = (2 * hits - len(prs)) / len(prs)
            delta = cls.LEARNING_SCALE * 0.01 * s
            W_new = max(cls.W_MIN, min(cls.W_MAX, W + delta))
            print(f"   [Kalman] {label}: W {W:.3f}→{W_new:.3f} (近{cls.LOOKBACK_DAYS}交易日 訊號→次日命中 {hits}/{len(prs)},技巧分數 {s:+.2f})")
            return {"W": round(W_new, 4), "last_update": datetime.date.today().isoformat()}

        kal["台股"] = _update_w(kal.get("台股", {"W": 0.481}), pairs["台股"], "台股")
        kal["A股"]  = _update_w(kal.get("A股",  {"W": 0.531}), pairs["A股"],  "A股")
        brain["kalman"] = kal
        return brain


# ══════════════════════════════════════════════════════════════════════
# 🧭 OutlookTeam — 明日前瞻+夜間快報
# ══════════════════════════════════════════════════════════════════════
class OutlookTeam:

    @staticmethod
    def build_tomorrow_outlook(results, vix, strait_status, c_drain, gold, wti):
        lines = ["\n## 🌅 明日作戰指引\n"]
        high_risk, attack_list, sell_list = [], [], []

        for r in results:
            code = r.get("code","")
            info = TARGET_ETFS.get(code, {})
            tech = r.get("tech", {})
            action, ev = r.get("action",""), r.get("ev",50)
            name = info.get("name","")[:8]
            rsi = tech.get("RSI", 50) if tech else 50
            bias = tech.get("BIAS", 0) if tech else 0

            if rsi > 80 or abs(bias) > 0.13:
                prob, prob_text = QuantEnhanced.rsi_reversal_prob(rsi)
                if prob > 0.55:
                    high_risk.append(f"  - **{name}** → {prob_text}")
            if "主攻" in action: attack_list.append(f"  - {name}（EV={ev}%）")
            if "減倉" in action: sell_list.append(f"  - {name}（EV={ev}%）")

        lines.append("### 🔍 明早關鍵觀察點\n"
                     "- **09:30** 北向資金前15分鐘 → A股科技方向確認\n"
                     f"- **VIX={vix:.2f}** {'⚠️若>20主攻全降為靜默' if vix>18 else '✅市場穩定'}")
        if c_drain >= 0.3:
            # [V22.89 治死文字] 原「能源供應存在不確定性」永恆靜態;改用在 scope 的 wti/gold 動態說明真因。
            _energy = []
            try:
                if wti and float(wti) > 0:
                    _energy.append(f"WTI ${float(wti):.0f}")
                if gold and float(gold) > 0:
                    _energy.append(f"金價 ${float(gold):.0f}")
            except Exception as _e:
                _log_err(_e, "build_tomorrow_outlook:L7416")  # [V24.16] 原為靜默 pass
                pass
            _etxt = ("（" + " / ".join(_energy) + " → 通脹/避險壓力,壓 risk 偏好）") if _energy else "（能源/避險溢價升溫）"
            lines.append(f"- ⛽ c_drain={c_drain:.2f} 偏高 {_etxt}")

        if high_risk:
            lines.append("\n### ⚠️ 明日高風險持倉\n" + "\n".join(high_risk[:5]))
            lines.append("> 若明日繼續上漲建議觀望；-1%以上可考慮止盈")
        if attack_list:
            lines.append("\n### 🟢 主攻持倉（明日追蹤）\n" + "\n".join(attack_list))
        if sell_list:
            lines.append("\n### 🔴 減倉信號（確認執行）\n" + "\n".join(sell_list))

        lines.append("\n### 📋 明早執行清單\n```\n"
                     "□ VIX確認 < 20（若超過 → 主攻全降靜默）\n"
                     "□ 北向資金方向（09:30前15分鐘）\n"
                     "□ NDX隔夜漲跌（影響006479/018967開盤）\n"
                     "□ trade_ease_signal評估（有貿易新進展？）\n"
                     "```")
        return "\n".join(lines) + "\n"

    @staticmethod
    def build_night_brief(ndx_ret, sox_ret, vix, gold, wti, usd_cny):
        """[V22.8修復] 夜間快報現在會寫入文件"""
        now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        # [V22.27.8] 修正單位災難:原式 (usd_cny-7.0)*(-0.5) 把「匯率對7.0的絕對偏離」
        #   當小數→6.785貢獻+10.75%,壓過納指項;且符號錯(人民幣強→美元資產折CNY變少→QDII應降)。
        #   QDII開盤主驅動=隔夜納指×結構β(QDII結構β≈0.90);匯率為次級且此處只有水平、無隔夜報酬→略去。
        qdii_pred = (ndx_ret or 0) * 0.92
        qdii_dir = "📈偏多" if qdii_pred > 0.003 else ("📉偏空" if qdii_pred < -0.003 else "🔄中性")
        # [V22.33.6 F1活接線·治F1/G4] 008888 對SOX脫鉤(|β|<0.3)則不引用SOX,不拿美股硬推A股芯片(7/15實盤背書)
        _s888 = effective_sox_for(get_beta_us("008888"), sox_ret)
        _s888_txt = (f"SOX{_s888*100:+.1f}%信號，A股芯片偏{'多' if _s888>0 else '空'}" if _s888 is not None
                     else "條件β脫鉤·SOX不引用（β≈-0.10；7/15實盤:美股半導體強、A股半導體崩背離）")

        brief = "\n".join([
            f"# 🌙 夜間快報 | {__VERSION__} | {now_str}",
            "",
            "## 🇺🇸 美股收盤快照",
            f"| 指標 | 數值 | 信號 |",
            f"|------|------|------|",
            f"| NDX  | {ndx_ret*100 if ndx_ret else 0:+.2f}% | {'📈' if (ndx_ret or 0)>0 else '📉'} |",
            f"| SOX  | {sox_ret*100 if sox_ret else 0:+.2f}% | {'📈' if (sox_ret or 0)>0 else '📉'} |",
            f"| VIX  | {vix:.2f} | {'🔴恐慌' if vix>25 else '🟡偏高' if vix>20 else '🟢正常'} |",
            f"| 黃金 | ${gold:.0f} | {'🟢避險強' if gold>4700 else '🔄中性'} |",
            f"| WTI  | ${wti:.2f} | {'🔴高油價' if wti>100 else '🟡中性'} |",
            f"| CNY  | {usd_cny:.4f} | {'🔴人民幣弱' if usd_cny>7.1 else '🟢偏強'} |",
            "",
            "## 🔮 明日QDII開盤預判",
            f"* 006479/018967：預計{qdii_pred*100:+.2f}% → **{qdii_dir}**",
            f"* 008888/016008：{_s888_txt}",
            "",
            "> ⚠️ 本報告純信息，不產生交易指令。早報09:00有完整EV+信號。",
        ])

        # [V22.8修復] 存檔（V22.7只生成不存）
        brief = data_availability.insert_unavailable_block(brief)   # [雲端任務A]
        ts_str = datetime.datetime.now().strftime('%m%d_%H%M')
        fn = os.path.join(REPORT_DIR,
             f"{__VERSION__}_夜間快報_{ts_str}.txt")
        with open(fn, 'w', encoding='utf-8') as f:
            f.write(brief)
        print(f"   夜間快報已存：{fn}")

        # [V22.25.5-fix] 同步生成 HTML 版（與早/午/收盤報一致）
        fn_html = os.path.join(REPORT_DIR,
                   f"{__VERSION__}_夜間快報_{ts_str}.html")
        html_content = markdown_to_html(brief,
                                     title=f"{__VERSION__} 夜間快報",
                                     subtitle=f"{datetime.datetime.now():%Y-%m-%d %H:%M}")
        with open(fn_html, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"   夜間快報HTML已存：{fn_html}")

        return brief



# ══════════════════════════════════════════════════════════════════════
# ✍️ ReportFormatter + IntelTeam
# ══════════════════════════════════════════════════════════════════════
class ReportFormatter:

    @staticmethod
    def format_etf_section(code, info, tech, ev, action, predicted_ret,
                           geo_adj, ret_src, strait_status,
                           geo_status_label, narrative,
                           kelly=None, pnl=None,
                           noon_note="", risk_level="", stop_tip="",
                           rs_label="—", proxy_quality="", premium_signal=""):
        pred_str = f"{predicted_ret:+.2%}" if predicted_ret is not None else "N/A"
        rsi = tech.get("RSI", 50)

        rev_prob, rev_text = QuantEnhanced.rsi_reversal_prob(rsi)
        rev_line = f"> ⚡ 反轉概率：{rev_text}\n" if rev_text else ""

        # [V22.9] 動量與RSI一致性：超賣時不顯示「下行風險上升」（矛盾）
        raw_mom = QuantEnhanced.momentum_persistence(
            tech.get("Trend_Strength",0), tech.get("Z_Score",0), tech.get("BIAS",0))
        if rsi < RSI_OVERSOLD and "下行" in (raw_mom or ""):
            mom_text = "↩️ 超賣疊加下行動量，技術反彈蓄力（短線反彈概率>60%）"
        else:
            mom_text = raw_mom
        mom_line = f"> 📈 動量：{mom_text}\n" if mom_text else ""

        # [V22.16 P0-1 修復 5/26] Kelly 用語根據 action 區分
        # 主攻 → 「建議倉位 🟢 重倉」（label 不變）
        # 靜默 → 「持有上限 🟢 上限 15-25%」（label 中性化，避免誤導）
        # 減倉 → 「目標降至 0%」
        if kelly and kelly.get("half_kelly", 0) > 0:
            kelly_pct = kelly['half_kelly']
            raw_label = kelly.get('label', '')

            if "減倉" in action:
                kelly_line = f"> 💼 目標倉位：🔴 降至 0%（信號要求出場，原 Kelly 上限 {kelly_pct:.1%}）\n"
            elif "主攻" in action:
                kelly_line = f"> 💼 建議倉位：{raw_label} ({kelly_pct:.1%}) — {kelly['rationale']}\n"
            else:  # 靜默
                # 中性化 label：「🟢 重倉（15-25%）」→「🟢 上限 15-25%」
                range_match = re.search(r'（(.+?)）', raw_label)
                icon_match = re.search(r'^([🟢🟡🟠])', raw_label)
                if range_match and icon_match:
                    neutral_label = f"{icon_match.group(1)} 上限 {range_match.group(1)}"
                else:
                    neutral_label = f"上限 {kelly_pct:.1%}"
                kelly_line = f"> 💼 持有上限：{neutral_label}（{kelly_pct:.1%}）— 持有不加碼，不超過此倉位\n"
        else:
            kelly_line = ""

        pnl_line = ""
        if pnl and pnl.get("pnl_pct") is not None:
            icon = "🟢" if pnl["pnl_pct"] > 0 else "🔴"
            pnl_line = (f"> 📋 持倉：{icon}{pnl['pnl_pct']:+.1%}"
                        f"（持{pnl.get('days_held','?')}天 / 成本={pnl.get('cost','?'):.4f}）\n")

        stop_line = f"> 🔔 {stop_tip}\n" if stop_tip else ""

        # [V22.10] 代理品質 + 溢折價
        proxy_line = f"> 📍 {proxy_quality}\n" if proxy_quality else ""
        prem_line  = f"> 💹 溢折價：{premium_signal}\n" if premium_signal else ""

        # [V22.9] 風險一致性注解（更清晰說明）
        risk_note = ""
        if "減倉" in action and "低風險" in risk_level:
            risk_note = " ℹ️(風險評分低但預測方向向下，以方向信號為準)"
        elif "主攻" in action and "高風險" in risk_level:
            risk_note = " ⚠️(高風險主攻，建議縮減Kelly倉位至1/3)"

        # [V22.13] 市場狀態顯示
        regime = tech.get("Regime", "ranging")
        regime_str = tech.get("Regime_Strength", 0)
        regime_evd = tech.get("Regime_Evidence", "")
        regime_icon = {"trending_up":"📈 趨勢上",
                       "trending_down":"📉 趨勢下",
                       "ranging":"🔄 震盪市"}.get(regime, "🔄 震盪市")
        regime_line = f"> 🌡️ Regime: {regime_icon} (強度{regime_str:.1f}) | {regime_evd}\n"

        # [V22.25.8] 決策理由拆解：方向(EV/action) + 時機(技術過熱)，讓「主攻≠買進指令」一目了然
        # 結構維度(倉位上限超標)需 style 聚合，由 main 流程注入 reason_struct（見 spec），預設空字串。
        _bias_v = tech.get("BIAS", 0) or 0
        _z_v    = tech.get("Z_Score", 0) or 0
        _hot = []
        if rsi >= 62:            _hot.append(f"RSI {rsi}")
        if abs(_z_v) >= 2.0:     _hot.append(f"Z {_z_v}")
        if abs(_bias_v) >= 0.10: _hot.append(f"乖離 {_bias_v:.0%}")
        if "主攻" in action:
            if _hot:
                _reason = f"方向偏多(EV {ev}%)，但技術偏熱({'/'.join(_hot)}) → 屬延伸非起漲，追高風險，宜續抱不追"
            else:
                _reason = f"方向偏多(EV {ev}%)，技術未過熱，具進場條件"
        elif "減倉" in action:
            _reason = f"方向轉弱(EV {ev}%)，信號要求出場"
        else:  # 靜默
            _reason = f"中性觀望(EV {ev}%)，未達主攻門檻" + (f"；技術偏熱({'/'.join(_hot)})" if _hot else "")
        reason_struct = locals().get("reason_struct", "")  # [V22.25.8] 結構/上限維度由 main 注入
        reason_line = f"> 🧭 **決策理由**：{_reason}{reason_struct}\n"

        matrix = (
            "\n> **🧮 核心矩陣**: "
            f"價格=`{tech.get('price',0):.4f}` | RSI=`{rsi}` | "
            f"乖離=`{tech.get('BIAS',0):.2%}` | Z=`{tech.get('Z_Score',0)}` | "
            f"趨勢=`{tech.get('Trend_Strength',0):+.2f}` | GeoAdj=`{geo_adj:.2f}`\n"
            f"> 🎯 **預測:{pred_str}** | **EV=`{ev}%`** | 風險={risk_level} ➜ **{action}**{risk_note}{noon_note}\n"
            f"{reason_line}"
            f"{regime_line}"
            f"> 🏆 RS={rs_label} | 源:[{ret_src}] | 地緣:[{_geo_lbl_for_style(info.get('style'), info.get('is_qdii'), geo_status_label)}]\n"
            f"{rev_line}{mom_line}{kelly_line}{pnl_line}{stop_line}{proxy_line}{prem_line}"
        )
        return f"\n## 🎯 {info['name']} ({code})\n{narrative}{matrix}---\n"

    @staticmethod
    def format_concentration_alert(style_alerts):
        if not any(a["alert"] for a in style_alerts):
            return ""
        lines = ["\n## 🛡️ 組合風險儀表板\n",
                 "| Style | 持倉數 | 建議倉位合計 | 警示 |",
                 "|-------|--------|------------|------|"]
        for a in style_alerts:
            if a["total_kelly"] > 0:
                lines.append(
                    f"| {a['style']} | {a['count']}檔 "
                    f"| {a['total_kelly']:.1%} | {a['alert'] or '✅正常'} |")
        return "\n".join(lines) + "\n"

    # ═══════════════════════════════════════════════════════════════════
    # [V22.16 新增] 信號統計表（按Style分組）
    # ═══════════════════════════════════════════════════════════════════
    @staticmethod
    def format_signal_statistics(results):
        """
        V22.16 新增：按Style分組統計信號分佈
        輸出格式：
        | Style | 主攻 | 靜默 | 減倉 | 主導方向 |
        放在午報/收盤報的「市場資金面」之後，「組合風險儀表板」之前
        """
        if not results:
            return ""

        from collections import defaultdict
        style_stats = defaultdict(lambda: {"attack":0, "silent":0, "reduce":0, "names":[]})

        for r in results:
            style = r.get("style", "其他")
            action = r.get("action", "")
            name = r.get("name", "")[:6]
            if "主攻" in action:
                style_stats[style]["attack"] += 1
                style_stats[style]["names"].append(("🟢", name))
            elif "減倉" in action:
                style_stats[style]["reduce"] += 1
                style_stats[style]["names"].append(("🔴", name))
            else:
                style_stats[style]["silent"] += 1

        # 計算總計
        total_attack = sum(s["attack"] for s in style_stats.values())
        total_silent = sum(s["silent"] for s in style_stats.values())
        total_reduce = sum(s["reduce"] for s in style_stats.values())
        total_etfs = total_attack + total_silent + total_reduce

        lines = [
            "\n## 📊 信號統計表（按Style分組）\n",
            "| Style | 🟢主攻 | 🟡靜默 | 🔴減倉 | 主導方向 | 主攻檔 |",
            "|-------|--------|--------|--------|---------|--------|",
        ]

        # 排序：主攻多的 Style 排前面
        for style, s in sorted(style_stats.items(), key=lambda x: -x[1]["attack"]):
            attack_names = [n for icon, n in s["names"] if icon == "🟢"]
            attack_str = " / ".join(attack_names) if attack_names else "—"
            # 主導方向判斷
            if s["attack"] >= 3:
                lead = "🟢🟢 強勢"
            elif s["attack"] >= 1 and s["attack"] > s["reduce"]:
                lead = "🟢 偏多"
            elif s["reduce"] > s["attack"]:
                lead = "🔴 偏弱"
            else:
                lead = "🟡 平穩"
            lines.append(
                f"| {style} | {s['attack']} | {s['silent']} | {s['reduce']} "
                f"| {lead} | {attack_str} |"
            )

        # 加上總計
        lines.append(
            f"| **總計** | **{total_attack}** | **{total_silent}** | **{total_reduce}** "
            f"| ({total_attack/total_etfs*100:.0f}%/{total_silent/total_etfs*100:.0f}%/"
            f"{total_reduce/total_etfs*100:.0f}%) | — |"
        )

        return "\n".join(lines) + "\n"


# ═══════════════════════════════════════════════════════════════════════════
# [V22.16 新增 5/27] Markdown → HTML 互動式報告（雙輸出）
# ═══════════════════════════════════════════════════════════════════════════


def markdown_to_html(md_content, title="V22.16 戰報", subtitle="", chart_data=None):
    """
    [V22.16 5/27 新增] 把 markdown 報告轉成自包含 HTML
    支援：標題（#/##/###）/ 表格 / 列表 / 引用 / 粗體 / 代碼塊 / 信號 emoji
    [V22.16 5/27 圖表升級] chart_data 可選 dict：
        signal_pie: {"主攻": N, "靜默": N, "減倉": N}
        watchlist_radar: [{"code": ..., "name": ..., "rsi": ...}, ...]
        pred_bar: [{"code": ..., "name": ..., "pred": ..., "action": ...}, ...]
    """
    import html as html_lib
    import json as _json
    lines = md_content.split('\n')
    html_blocks = []
    i = 0
    in_code = False
    code_buffer = []
    # [V22.17.1 P3] ETF 折疊區塊狀態追蹤
    in_etf_section = False

    while i < len(lines):
        line = lines[i]

        # 代碼塊處理
        if line.startswith('```'):
            if in_code:
                escaped = html_lib.escape('\n'.join(code_buffer))
                html_blocks.append(
                    f'<pre class="bg-slate-900 text-emerald-300 p-3 rounded-lg my-3 text-xs font-mono overflow-x-auto">{escaped}</pre>'
                )
                code_buffer = []
                in_code = False
            else:
                in_code = True
            i += 1
            continue
        if in_code:
            code_buffer.append(line)
            i += 1
            continue

        # 表格處理
        if line.startswith('|') and i + 1 < len(lines) and re.match(r'\s*\|[\s|:-]+\|', lines[i + 1]):
            header_cells = [c.strip() for c in line.split('|')[1:-1]]
            i += 2  # 跳過 header 和分隔線
            rows = []
            while i < len(lines) and lines[i].strip().startswith('|'):
                cells = [c.strip() for c in lines[i].split('|')[1:-1]]
                rows.append(cells)
                i += 1
            tbl = '<div class="overflow-x-auto my-4"><table class="min-w-full border border-slate-200 rounded-lg shadow-sm"><thead class="bg-gradient-to-r from-indigo-50 to-purple-50">'
            tbl += '<tr>' + ''.join(
                f'<th class="px-3 py-2 text-left font-semibold text-sm border-b border-slate-300">{_inline_md_format(c)}</th>'
                for c in header_cells
            ) + '</tr></thead><tbody>'
            for row in rows:
                # 填補不足 cells
                while len(row) < len(header_cells):
                    row.append('')
                tbl += '<tr class="hover:bg-slate-50 border-t border-slate-100">' + ''.join(
                    f'<td class="px-3 py-2 text-sm">{_inline_md_format(c)}</td>' for c in row
                ) + '</tr>'
            tbl += '</tbody></table></div>'
            html_blocks.append(tbl)
            continue

        # 標題
        if line.startswith('### '):
            html_blocks.append(f'<h3 class="text-lg font-semibold mt-4 mb-2 text-slate-700">{_inline_md_format(line[4:].strip())}</h3>')
        elif line.startswith('## '):
            # [V22.17.1 P3] ETF 區塊折疊式
            heading_text = line[3:].strip()
            # ETF section 特徵：## 🎯 + 含 (代碼) 括號（排除「今日QDII開盤方向預判」等）
            is_etf = (heading_text.startswith('🎯 ')
                      and '(' in heading_text and ')' in heading_text)
            if is_etf:
                # 關閉前一個 ETF section
                if in_etf_section:
                    html_blocks.append('</div></details>')
                # 開啟新 ETF section（折疊式，預設收起）
                html_blocks.append(
                    '<details class="my-3 border border-slate-200 rounded-lg overflow-hidden shadow-sm">'
                    '<summary class="cursor-pointer bg-gradient-to-r from-indigo-50 to-purple-50 px-4 py-3 font-bold text-indigo-700 hover:from-indigo-100 hover:to-purple-100 transition-colors select-none">'
                    f'{_inline_md_format(heading_text)}'
                    '</summary>'
                    '<div class="p-4 bg-white">'
                )
                in_etf_section = True
            else:
                # 非 ETF 的 ## — 關閉前一個 ETF section（如有）
                if in_etf_section:
                    html_blocks.append('</div></details>')
                    in_etf_section = False
                html_blocks.append(f'<h2 class="text-xl font-bold mt-5 mb-2 text-indigo-700 border-b border-indigo-200 pb-1">{_inline_md_format(heading_text)}</h2>')
        elif line.startswith('# '):
            # 主標題前關閉 ETF section（如有）
            if in_etf_section:
                html_blocks.append('</div></details>')
                in_etf_section = False
            html_blocks.append(f'<h1 class="text-2xl font-bold mt-6 mb-3 gradient-text">{_inline_md_format(line[2:].strip())}</h1>')
        # 引用
        elif line.startswith('> '):
            html_blocks.append(f'<blockquote class="border-l-4 border-indigo-400 bg-indigo-50 p-2 my-2 text-slate-700">{_inline_md_format(line[2:].strip())}</blockquote>')
        # 列表
        elif line.startswith('- ') or line.startswith('* '):
            items = []
            while i < len(lines) and (lines[i].startswith('- ') or lines[i].startswith('* ')):
                items.append(f'<li class="ml-4 my-1">{_inline_md_format(lines[i][2:].strip())}</li>')
                i += 1
            html_blocks.append(f'<ul class="list-disc list-inside my-2 space-y-1">{chr(10).join(items)}</ul>')
            continue
        # 水平線
        elif line.startswith('---') or line.startswith('═══'):
            html_blocks.append('<hr class="my-4 border-slate-300">')
        elif not line.strip():
            pass  # 空行跳過
        else:
            html_blocks.append(f'<p class="my-2 leading-relaxed text-slate-800">{_inline_md_format(line)}</p>')
        i += 1

    # [V22.17.1 P3] 收尾：關閉殘留 ETF section（如有）
    if in_etf_section:
        html_blocks.append('</div></details>')

    body = '\n'.join(html_blocks)
    gen_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── 圖表區塊組裝（如果有 chart_data）─────────────
    chart_section = ""
    chart_script = ""
    if chart_data:
        pie = chart_data.get('signal_pie') or {}
        radar = chart_data.get('watchlist_radar') or []
        bar = chart_data.get('pred_bar') or []
        # 只有當至少有一個非空才生成圖表區塊
        if pie or radar or bar:
            pie_json = _json.dumps(pie, ensure_ascii=False)
            radar_json = _json.dumps(radar, ensure_ascii=False)
            bar_json = _json.dumps(bar, ensure_ascii=False)
            chart_section = (
                '<div class="bg-white shadow-xl rounded-xl p-6 mb-6">\n'
                '  <h2 class="text-xl font-bold mb-4 gradient-text">📊 視覺化儀表板</h2>\n'
                '  <div class="grid md:grid-cols-2 gap-4">\n'
                '    <div class="border border-slate-200 rounded-lg p-3">\n'
                '      <h3 class="text-sm font-semibold mb-2 text-slate-700">信號統計</h3>\n'
                '      <canvas id="signalPie" height="180"></canvas>\n'
                '    </div>\n'
                '    <div class="border border-slate-200 rounded-lg p-3">\n'
                '      <h3 class="text-sm font-semibold mb-2 text-slate-700">觀察池 RSI 雷達</h3>\n'
                '      <canvas id="watchRadar" height="180"></canvas>\n'
                '    </div>\n'
                '    <div class="border border-slate-200 rounded-lg p-3 md:col-span-2">\n'
                '      <h3 class="text-sm font-semibold mb-2 text-slate-700">ETF 預測 (%) 排序</h3>\n'
                '      <canvas id="predBar" height="80"></canvas>\n'
                '    </div>\n'
                '  </div>\n'
                '</div>\n'
            )
            chart_script = (
                '<script>\n'
                'window.addEventListener("load", function() {\n'
                '  if (typeof Chart === "undefined") return;\n'
                f'  const pieData = {pie_json};\n'
                f'  const radarData = {radar_json};\n'
                f'  const barData = {bar_json};\n'
                # 圓餅圖
                '  if (Object.keys(pieData).length > 0) {\n'
                '    new Chart(document.getElementById("signalPie"), {\n'
                '      type: "doughnut",\n'
                '      data: {\n'
                '        labels: Object.keys(pieData),\n'
                '        datasets: [{\n'
                '          data: Object.values(pieData),\n'
                '          backgroundColor: Object.keys(pieData).map(k => '
                'k.includes("主攻") ? "#10b981" : (k.includes("減倉") ? "#ef4444" : "#f59e0b")),\n'
                '          borderWidth: 0\n'
                '        }]\n'
                '      },\n'
                '      options: { responsive: true, plugins: { legend: { position: "bottom" } } }\n'
                '    });\n'
                '  }\n'
                # 雷達圖
                '  if (radarData.length > 0) {\n'
                '    new Chart(document.getElementById("watchRadar"), {\n'
                '      type: "radar",\n'
                '      data: {\n'
                '        labels: radarData.map(d => d.code + "\\n" + (d.name || "").slice(0,8)),\n'
                '        datasets: [{\n'
                '          label: "RSI（≤45 達標）",\n'
                '          data: radarData.map(d => d.rsi),\n'
                '          backgroundColor: "rgba(99, 102, 241, 0.2)",\n'
                '          borderColor: "rgba(99, 102, 241, 1)",\n'
                '          pointBackgroundColor: radarData.map(d => d.rsi <= 45 ? "#10b981" : "#ef4444"),\n'
                '          pointRadius: 5\n'
                '        }, {\n'
                '          label: "進場門檻 RSI=45",\n'
                '          data: radarData.map(() => 45),\n'
                '          borderColor: "rgba(16, 185, 129, 0.6)",\n'
                '          borderDash: [4, 4], pointRadius: 0, fill: false\n'
                '        }]\n'
                '      },\n'
                '      options: { responsive: true, scales: { r: { min: 0, max: 100, ticks: { stepSize: 20 } } },\n'
                '        plugins: { legend: { position: "bottom", labels: { font: { size: 10 } } } } }\n'
                '    });\n'
                '  }\n'
                # 柱狀圖
                '  if (barData.length > 0) {\n'
                '    new Chart(document.getElementById("predBar"), {\n'
                '      type: "bar",\n'
                '      data: {\n'
                '        labels: barData.map(d => d.code),\n'
                '        datasets: [{\n'
                '          label: "預測 (%)",\n'
                '          data: barData.map(d => d.pred),\n'
                '          backgroundColor: barData.map(d => d.pred > 0 ? "rgba(16, 185, 129, 0.7)" : "rgba(239, 68, 68, 0.7)"),\n'
                '          borderWidth: 0\n'
                '        }]\n'
                '      },\n'
                '      options: { responsive: true, plugins: { legend: { display: false }, '
                'tooltip: { callbacks: { afterLabel: function(ctx) { return barData[ctx.dataIndex].name + " | " + barData[ctx.dataIndex].action; } } } } }\n'
                '    });\n'
                '  }\n'
                '});\n'
                '</script>\n'
            )

    return (
        '<!DOCTYPE html>\n'
        '<html lang="zh-Hant">\n<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'<title>{html_lib.escape(title)}</title>\n'
        '<script src="https://cdn.tailwindcss.com"></script>\n'
        '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>\n'
        '<style>\n'
        '  body { font-family: -apple-system, "PingFang TC", "Microsoft JhengHei", "Segoe UI", sans-serif; }\n'
        '  .gradient-text { background: linear-gradient(135deg, #667eea, #764ba2); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }\n'
        '  table { border-collapse: collapse; }\n'
        '  code { font-size: 0.875em; }\n'
        '</style>\n'
        '</head>\n'
        '<body class="bg-gradient-to-br from-slate-50 to-slate-100 text-slate-900 min-h-screen">\n'
        '<div class="bg-gradient-to-r from-indigo-900 to-purple-900 text-white py-5 shadow-lg">\n'
        '  <div class="max-w-6xl mx-auto px-6">\n'
        f'    <h1 class="text-3xl font-bold">⚡ {html_lib.escape(title)}</h1>\n'
        f'    <p class="text-indigo-200 text-sm mt-1">{html_lib.escape(subtitle)}</p>\n'
        '  </div>\n'
        '</div>\n'
        '<main class="max-w-6xl mx-auto px-6 py-6">\n'
        f'{chart_section}'
        '<div class="bg-white shadow-xl rounded-xl p-6">\n'
        f'{body}\n'
        '</div>\n'
        '</main>\n'
        '<footer class="bg-slate-900 text-slate-400 py-4 mt-8">\n'
        '  <div class="max-w-6xl mx-auto px-6 text-center text-xs">\n'
        f'    <p>{__VERSION__} 互動式報告 · 自動生成於 {gen_time}</p>\n'
        '  </div>\n'
        '</footer>\n'
        f'{chart_script}'
        '</body>\n</html>\n'
    )


# ═══════════════════════════════════════════════════════════════════════════
# [V22.16 新增] 觀察池掃描器（簡化版，獨立於 TARGET_ETFS 主邏輯）
# ═══════════════════════════════════════════════════════════════════════════
class WatchlistScanner:
    """
    觀察池掃描器：
      - 不參與 TARGET_ETFS 的 RS-Rank / Kelly / EV / Style 集中度計算
      - 只追蹤每日 RSI / Regime / pred / 進場條件
      - 在午報新增「觀察池摘要」區塊單獨顯示
      - 進場條件：RSI ≤ WATCHLIST_ENTRY_RSI_MAX
                + Regime trending_up
                + EV ≥ WATCHLIST_ENTRY_EV_MIN
    """

    @staticmethod
    def scan_watchlist():
        """
        [V22.16 Bug 修復 5/26] 用 DirectDataFetcher 取代不存在的方法
        掃描 WATCHLIST_ETFS 中所有觀察池 ETF
        返回每檔的簡化信號狀態
        失敗時返回空列表，不影響主報告生成
        """
        if not WATCHLIST_ETFS:
            return []

        watchlist_results = []
        for code, info in WATCHLIST_ETFS.items():
            try:
                # ─ 數據採集（與主流程相同邏輯） ───────────────
                # [V22.24.2] 台股走Yahoo(.TW), A股/QDII場外走天天基金NAV
                if info.get("market") == "台股":
                    _tw_code = info.get("proxy") or (code if code.endswith(".TW") else f"{code}.TW")
                    prices = DirectDataFetcher.get_yahoo_prices(_tw_code)
                else:
                    # code 是場外聯接代碼，用 eastmoney_nav 抓 NAV 序列
                    prices = DirectDataFetcher.get_eastmoney_nav(code)

                # [V24.12] 價格有效性守衛(與主循環同款缺陷,一併堵)
                prices, _pdrop, _pok = sanitize_price_series(prices, code, 20)
                if _pdrop:
                    print(f"   ⚠️ [觀察池] {code} 價格序列剔除 {_pdrop} 筆無效值"
                          f"(0/負/NaN,取數失敗) [剩 {len(prices)} 筆]")
                if not _pok:
                    print(f"   [觀察池] {code} {info['name']} 有效價格不足20天"
                          f"(剔除{_pdrop}筆後剩{len(prices)})，跳過")
                    _WATCH_SKIPPED.append(code)
                    _data_unavailable("WATCH_PRICES", f"{len(_WATCH_SKIPPED)} 檔觀察池價格取不到:"
                                      f"{', '.join(_WATCH_SKIPPED)}")
                    continue

                # ─ 量化計算（與主循環相同） ─────────────────
                arr = np.array(prices[-60:], dtype=float)
                if len(arr) < 20:
                    continue

                delta = np.diff(arr)
                avg_g = np.mean(np.where(delta>0, delta, 0)[-14:]) or 0.001
                avg_l = np.mean(np.where(delta<0, -delta, 0)[-14:]) or 0.001
                rsi = 100 - 100 / (1 + avg_g / max(avg_l, 1e-9))

                cur, ma20 = arr[-1], np.mean(arr[-20:])
                bias = (cur - ma20) / ma20 if ma20 > 0 else 0.0
                trend_5 = ((arr[-1] - arr[-5]) / (arr[-5] + 1e-9)) if len(arr)>=5 else 0.0
                trend = round(min(1.0, max(-1.0, trend_5 * 10)), 2)
                ret_5d = (arr[-1] - arr[-5]) / arr[-5] * 100 if len(arr) >= 5 else 0.0

                # 連漲天數
                consecutive_up = 0
                for i in range(len(arr)-1, 0, -1):
                    if arr[i] > arr[i-1]:
                        consecutive_up += 1
                    else:
                        break

                # ─ Regime 識別（用 RegimeIdentifier） ──────
                tech_dict = {
                    "rsi": rsi, "bias": bias, "trend": trend,
                    "ret_5d": ret_5d, "consecutive_up": consecutive_up,
                    "ma20_diff": bias,
                }
                try:
                    regime = TrendRegimeDetector.detect(prices)   # [V22.89 修F13] 原 RegimeIdentifier 未定義→NameError被吞→觀察池Regime永遠ranging
                    regime_type = regime.get("regime", "ranging")
                    regime_strength = regime.get("strength", 0.0)
                except Exception:
                    regime_type = "ranging"
                    regime_strength = 0.0

                # ─ [V22.16 P0-2 修復] 多因子 EV 估算 ──────────────
                # 原邏輯：ranging 永遠 = 40，5 檔無區分度
                # 新邏輯：Regime基礎 + RSI + ret_5d + bias + 連漲調整
                #        範圍 10-85，讓 ranging 也有區分度

                # 1. Regime 基礎 EV
                if regime_type == "trending_up":
                    base_ev = 55
                elif regime_type == "trending_down":
                    base_ev = 25
                else:  # ranging
                    base_ev = 40

                # 2. RSI 調整（超賣加分，超買扣分）
                if rsi <= 30:    rsi_adj = +15
                elif rsi <= 40:  rsi_adj = +8
                elif rsi <= 50:  rsi_adj = +3
                elif rsi <= 60:  rsi_adj = 0
                elif rsi <= 70:  rsi_adj = -5
                elif rsi <= 80:  rsi_adj = -10
                else:            rsi_adj = -15

                # 3. ret_5d 動量調整（過熱扣分，超跌【溫和】加分）
                #    [V22.87 修] 原 ret_5d<=-5 → +10 等於「跌越多越推薦買」=接刀子誘因,
                #       改為只給溫和加分且門檻下移,避免崩盤型標的被高估 EV(019072 實證)。
                if ret_5d <= -8:   ret_adj = +5
                elif ret_5d <= -5: ret_adj = +3
                elif ret_5d <= -2: ret_adj = +2
                elif ret_5d <= 2:  ret_adj = 0
                elif ret_5d <= 5:  ret_adj = -3
                elif ret_5d <= 10: ret_adj = -8
                else:              ret_adj = -12

                # 4. bias MA20 偏離調整
                if bias <= -0.05:   bias_adj = +5
                elif bias <= -0.02: bias_adj = +2
                elif bias >= 0.10:  bias_adj = -8
                elif bias >= 0.05:  bias_adj = -4
                else:               bias_adj = 0

                # 5. 連漲天數調整（連漲過多可能高點）
                if consecutive_up >= 7:   cu_adj = -10
                elif consecutive_up >= 5: cu_adj = -5
                elif consecutive_up >= 3: cu_adj = -2
                else:                     cu_adj = 0

                # 加總並限制範圍 10-85
                ev_est = max(10, min(85,
                    base_ev + rsi_adj + ret_adj + bias_adj + cu_adj))

                # ─ [V22.18 新增] 資金流向 + 背離示警(在進場判斷前調整EV) ──
                flow5_info = None
                divergence_tag = ""
                try:
                    _proxy_flow = info.get("proxy")
                    if _proxy_flow:
                        flow5_info = _flow5_with_manual(code, _proxy_flow, DirectDataFetcher.get_etf_5day_flow(_proxy_flow))
                    ev_est, divergence_tag = apply_flow_divergence(ev_est, rsi, flow5_info)
                except Exception as _e:
                    _log_err(_e, "watchlist_flow5")

                # 簡化 pred：5 日漲幅日均
                pred_5d = ret_5d / 5 / 100  # 轉為小數

                # ─ [V22.16 P2-B 修 5/27] 進場條件階層化 ─────────────
                # 原邏輯：三條件同時 (RSI≤45 + trending_up + EV≥50)
                #         5/27 5 檔全 ranging，3 檔達 2/3 卻被卡死
                # 新邏輯：階層式
                #   ✨✨ 強勢進場：三條件齊備
                #   ✨   弱勢進場：RSI 強超賣（≤40）+ EV 強（≥55），無視 Regime
                #   🟢   可考慮進場：2/3 條件達標 + RSI ≤ 50
                #   🟡   觀察中：其他狀況
                entry_signal = "🟡 觀察中"
                entry_reasons = []
                # 計算條件達標數
                cond_rsi = rsi <= WATCHLIST_ENTRY_RSI_MAX
                cond_regime = regime_type == WATCHLIST_ENTRY_REGIME
                cond_ev = ev_est >= WATCHLIST_ENTRY_EV_MIN
                hits = sum([cond_rsi, cond_regime, cond_ev])

                if cond_rsi and cond_regime and cond_ev:
                    entry_signal = "✨✨ 強勢進場（三條件齊備）"
                    entry_reasons.append(f"RSI={rsi:.1f}≤{WATCHLIST_ENTRY_RSI_MAX}")
                    entry_reasons.append(f"Regime={regime_type}")
                    entry_reasons.append(f"EV={ev_est}≥{WATCHLIST_ENTRY_EV_MIN}")
                elif rsi <= 40 and ev_est >= 55:
                    # 弱勢進場：強超賣 + 強 EV，無視 Regime
                    # [V22.17.3 P0 5/29] 板塊熱度過濾（接刀子警示）
                    # [V22.87 修] 防呆 + 多維接刀子閘（019072 實證虧 16% 教訓）
                    #   原 bug：sector_check_ok=False（無 proxy/抓取失敗）→ 整個過濾跳過 → 乾淨買進信號
                    #   現改：① 板塊數據缺失 → 不給買進,降為觀察
                    #        ② 板塊當日<-2% / 自身5日<=-5% / 5日淨流出 → 接刀子,不給買進
                    sector_today_ret = 0.0
                    sector_check_ok = False
                    try:
                        _proxy = info.get("proxy")
                        if _proxy:
                            sector_today_ret = DirectDataFetcher.get_sina_proxy_ret(_proxy)
                            sector_check_ok = True
                    except Exception as _e:
                        _log_err(_e, "watchlist_sector_filter")
                    _flow5_yi = (flow5_info or {}).get("flow5_yi")
                    _knife_flow = (_flow5_yi is not None and _flow5_yi < -0.3)
                    _knife_ret5 = (ret_5d is not None and ret_5d <= -5 and not (sector_today_ret is not None and sector_today_ret > 0.03))  # [V23.3 修缺陷1] proxy今日板塊漲>3%=已反轉,滯後NAV ret_5d不再誤觸接刀子

                    if sector_check_ok and sector_today_ret is not None and sector_today_ret < -0.02:  # [V24.14] 防 None<float TypeError
                        # 板塊當日跌幅 >2% → 接刀子風險
                        entry_signal = f"⚠️ 接刀子風險（板塊當日{sector_today_ret*100:+.2f}%）"
                        entry_reasons.append(f"RSI={rsi:.1f}≤40（強超賣）")
                        entry_reasons.append(f"❌ 板塊當日跌{sector_today_ret*100:+.2f}%")
                        entry_reasons.append("⚠️ V22.17.3 過濾：強超賣 + 板塊續跌 = 高機率接刀子")
                        entry_reasons.append("建議：等板塊止跌（連續 2 日收漲）再評估")
                    elif not sector_check_ok:
                        # [V22.87] 板塊數據缺失 → 不給買進信號(原 bug: 直接給弱勢進場)
                        entry_signal = "🟡 觀察中（板塊數據缺失,無法排除接刀子風險）"
                        entry_reasons.append(f"RSI={rsi:.1f}≤40（強超賣）")
                        entry_reasons.append(f"EV={ev_est}≥55")
                        entry_reasons.append("⚠️ V22.87 防呆：板塊抓取失敗/無 proxy → 不給弱勢進場,等數據齊")
                    elif _knife_ret5:
                        # [V22.87] 自身近5日大跌 → 接刀子(不只看單日板塊)
                        entry_signal = f"⚠️ 接刀子風險（近5日{ret_5d:+.1f}%）"
                        entry_reasons.append(f"RSI={rsi:.1f}≤40（強超賣）")
                        entry_reasons.append(f"❌ 近5日跌{ret_5d:+.1f}%（多日下跌=接刀子）")
                        entry_reasons.append("建議：等止跌訊號再評估")
                    elif _knife_flow:
                        # [V22.87] 5日淨流出 → 資金不認同
                        entry_signal = f"⚠️ 接刀子風險（5日淨流出{_flow5_yi:+.1f}億,資金背離）"
                        entry_reasons.append(f"RSI={rsi:.1f}≤40（強超賣）")
                        entry_reasons.append(f"EV={ev_est}≥55")
                        entry_reasons.append(f"❌ 5日資金淨流出{_flow5_yi:+.1f}億=資金不認同")
                        entry_reasons.append("建議：等資金回流(5日轉正)再評估")
                    else:
                        if _flow5_yi is not None and _flow5_yi < 0:
                            # [V22.87] 任何淨流出都不給買進措辭(資金不認同),降為觀察
                            #   這正是 019072 教訓:flow5=-0.09 仍給進場→跌16%
                            entry_signal = f"🟡 觀察中（5日淨流出{_flow5_yi:+.1f}億,資金不認同）"
                            entry_reasons.append(f"RSI={rsi:.1f}≤40（強超賣）")
                            entry_reasons.append(f"EV={ev_est}≥55")
                            entry_reasons.append(f"❌ 5日資金淨流出{_flow5_yi:+.1f}億")
                            entry_reasons.append("⚠️ V22.87：資金撤離下暫緩進場")
                        else:
                            entry_signal = "✨ 弱勢進場（超賣+EV強，無視 Regime）"
                            entry_reasons.append(f"RSI={rsi:.1f}≤40（強超賣）")
                            entry_reasons.append(f"EV={ev_est}≥55")
                            if sector_check_ok:
                                entry_reasons.append(f"✅ 板塊當日{sector_today_ret*100:+.2f}%（非接刀子）")
                            entry_reasons.append("⚠️ 建議倉位上限 5%（橫盤期試水）")
                elif hits >= 2 and rsi <= 50:
                    entry_signal = "🟢 可考慮進場（2/3 條件達標）"
                    if cond_rsi:
                        entry_reasons.append(f"✅RSI={rsi:.1f}")
                    if cond_regime:
                        entry_reasons.append(f"✅Regime={regime_type}")
                    if cond_ev:
                        entry_reasons.append(f"✅EV={ev_est}")
                    if not cond_rsi:
                        entry_reasons.append(f"❌RSI={rsi:.1f}>{WATCHLIST_ENTRY_RSI_MAX}")
                    if not cond_regime:
                        entry_reasons.append(f"❌Regime={regime_type}")
                    if not cond_ev:
                        entry_reasons.append(f"❌EV={ev_est}")
                    # [V22.87 修] 進場信號也要尊重資金/接刀子（019072 實證:淨流出仍給可考慮進場→跌16%）
                    _flow5_yi = (flow5_info or {}).get("flow5_yi")
                    _knife_ret5 = (ret_5d is not None and ret_5d <= -5 and not (sector_today_ret is not None and sector_today_ret > 0.03))  # [V23.3 修缺陷1] proxy今日板塊漲>3%=已反轉,滯後NAV ret_5d不再誤觸接刀子
                    if _knife_ret5:
                        entry_signal = f"⚠️ 接刀子風險（近5日{ret_5d:+.1f}%）"
                        entry_reasons.append(f"❌ 近5日跌{ret_5d:+.1f}%（多日下跌=接刀子）")
                        entry_reasons.append("建議：等止跌訊號再評估")
                    elif _flow5_yi is not None and _flow5_yi < 0:
                        # 資金淨流出 → 降級為觀察(不給買進措辭)
                        #   任何淨流出都不給進場(019072 教訓:flow5=-0.09 仍給可考慮進場→跌16%)
                        entry_signal = f"🟡 觀察中（5日淨流出{_flow5_yi:+.1f}億,資金不認同）"
                        entry_reasons.append(f"❌ 5日資金淨流出{_flow5_yi:+.1f}億")
                        entry_reasons.append("⚠️ V22.87：資金撤離下暫緩進場")
                    else:
                        entry_reasons.append("⚠️ 建議倉位上限 3%（保守試水）")
                else:
                    if rsi > WATCHLIST_ENTRY_RSI_MAX:
                        entry_reasons.append(f"❌RSI={rsi:.1f}>{WATCHLIST_ENTRY_RSI_MAX}")
                    if regime_type != WATCHLIST_ENTRY_REGIME:
                        entry_reasons.append(f"❌Regime={regime_type}")
                    if ev_est < WATCHLIST_ENTRY_EV_MIN:
                        entry_reasons.append(f"❌EV={ev_est}<{WATCHLIST_ENTRY_EV_MIN}")

                watchlist_results.append({
                    "code": code,
                    "name": info.get("name", "?"),
                    "theme": info.get("theme", "?"),
                    "rsi": rsi,
                    # [V22.56] ★trend/bias 早就算好(L5812/L5814)卻沒帶出來 →
                    #   V22.55 的 save_watchlist_snapshot 寫 r.get("trend") 恆得 None,
                    #   導致觀察池落庫的 trend/fragility 全是 NULL。補上。
                    "trend": trend,
                    "bias": bias,
                    "regime": regime_type,
                    "regime_strength": regime_strength,
                    "ev_est": ev_est,
                    "pred_5d": pred_5d,
                    "ret_5d": ret_5d,
                    "entry_signal": entry_signal,
                    "entry_reasons": " | ".join(entry_reasons),
                    "anchors": info.get("anchors", []),
                    "flow5": flow5_info,
                    "divergence": divergence_tag,
                    "priority_watch": info.get("priority_watch", ""),  # [V22.23.8] 重點觀察標記
                    "added_date": info.get("added_date", ""),          # [V22.84] 供渲染動態算保鮮期(F6)
                    "dd": info.get("dd", {}),                          # [V24.3 真因修] 原本沒帶dd→渲染端r.get("dd")恆None,盡調永遠印不出來
                })
                _flow_txt = ""
                if flow5_info:
                    _flow_txt = f" 資金{flow5_info.get('flow5_yi', 0):+.1f}億 {flow5_info.get('trend','')}"
                if divergence_tag:
                    _flow_txt += f" {divergence_tag[:20]}"
                print(f"   [觀察池] {code} {info['name']}: RSI={rsi:.1f} Regime={regime_type} EV={ev_est} → {entry_signal}{_flow_txt}")
            except Exception as e:
                print(f"   [觀察池] {code} 異常: {type(e).__name__}: {e}")
                continue

        return watchlist_results

    @staticmethod
    def format_watchlist_block(watchlist_results):
        """格式化觀察池信號摘要為 Markdown 區塊"""
        if not watchlist_results:
            if WATCHLIST_ETFS:
                return "\n## 👁️ 觀察池信號摘要\n\n*（本次掃描全部失敗，請檢查網路或代理）*\n"
            return ""

        lines = [
            "\n## 👁️ 觀察池信號摘要\n",
            "| 代碼 | 名稱 | 主題 | RSI | Regime | EV估 | 5日資金 | 進場條件 |",
            "|------|------|------|-----|--------|------|--------|---------|",
        ]

        # [V22.23.8] 重點觀察(priority_watch)排最前, 供盤前報優先複核
        watchlist_results = sorted(
            watchlist_results, key=lambda r: (0 if r.get("priority_watch") else 1))

        for r in watchlist_results:
            regime_icon = "📈" if r["regime"] == "trending_up" else (
                "📉" if r["regime"] == "trending_down" else "🔄")
            regime_str = f"{regime_icon}{r['regime']}({r['regime_strength']:.1f})"
            # [V22.18] 5日資金流向欄
            flow5 = r.get("flow5")
            if flow5:
                _sg = flow5.get('signal', '') or ''
                flow_str = f"{flow5.get('flow5_yi', 0):+.1f}億 {_sg[:1]}{_sg[1:4]}"
            else:
                flow_str = "—"
            _star = "🎯" if r.get("priority_watch") else ""  # 重點觀察標記
            lines.append(
                f"| {_star}{r['code']} | {r['name'][:12]} | {r['theme']} "
                f"| {r['rsi']:.1f} | {regime_str} | {r['ev_est']} | {flow_str} | "
                f"{r['entry_signal']} |"
            )

        # 詳細條件分析
        lines.append("\n**進場條件詳情**：")
        for r in watchlist_results:
            _pstar = "🎯重點觀察 " if r.get("priority_watch") else ""
            lines.append(f"- **{_pstar}{r['code']} {r['name']}**: {r['entry_reasons']}")
            if r.get("priority_watch"):
                lines.append(f"  - 🎯 {_pw_qualitative_display(r['priority_watch'], r.get('added_date'))}{_pw_freshness_tag(r.get('added_date'))}")
            # [V24.4] 盡調改用公式引擎,且【脫離 priority_watch 綁定】(WB缺口③:原本沒設priority_watch就不顯示)
            try:
                _ddf, _ddc, _ddv = build_dd_profile(WATCHLIST_ETFS.get(r["code"], {}), code=r["code"])
                lines.append(f"  - 🔍 **盡調裁決**：{_ddv}　(通過{_ddc['通過']}/待查{_ddc['待查']}/警示{_ddc['警示']} · 共{len(DD_DIMS)}維)")
                for _k, _v in _ddf.items():
                    lines.append(f"      · {_k}：{_v}")
                _dupm = scan_duplicate_manager()
                for _mg, _cs in _dupm.items():
                    if r["code"] in _cs:
                        lines.append(f"      · 🚨 **重複押注自動偵測**：經理人「{_mg}」在觀察池有 {len(_cs)} 檔({'/'.join(_cs)}) → 同時持有非分散")
            except Exception as _e:
                _log_err(_e, "dd_profile")
                lines.append(f"  - 🔄 閉環對照：系統實算 RSI={r['rsi']:.1f}/EV={r['ev_est']}/{r['entry_signal']} ← 對比上方Claude web初判")
            # [V24.5 ⑩] 因子歸因(對大盤/板塊單因子OLS,真實DB日報酬)
            try:
                _fa = build_factor_attr_block(r["code"])
                if _fa:
                    lines.append(f"  - {_fa}")
            except Exception as _e:
                _log_err(_e, "factor_attr_report")
            # [V22.18] 5日資金流向明細（情報輸入層）
            flow5 = r.get("flow5")
            if flow5:
                # [V24.26] ★flow5_list 可能為 None(量價代理/手工補值只有單值,無5日逐日序列)。
                #   原寫法 `for x in flow5["flow5_list"]` → 'NoneType' object is not iterable
                #   → 觀察池整體失敗(使用者 2026-08-10 實測)。
                #   ★這是我 V24.19 加回退層時引入的:上一輪才因 KeyError:'signal' 補齊 key,
                #     卻【只補了 key 沒檢查 value 型別是否符合下游期待】——同一個坑踩兩次。
                _fl = flow5.get("flow5_list") or []
                _daily = ", ".join(f"{x:+.1f}" for x in _fl) if _fl else "無逐日序列(代理/手工值)"
                lines.append(f"  - 💰 5日主力淨流入: {flow5.get('flow5_yi', 0):+.1f}億 ({flow5.get('signal','—')}, {flow5.get('trend','—')}) | 逐日[{_daily}]億")
            # [V22.18] 資金背離示警
            _div = r.get("divergence")
            if _div:
                lines.append(f"  - {_div}")
            if r["anchors"]:
                lines.append(f"  - 錨股: {', '.join(r['anchors'])}")

        # 達標統計
        ready = sum(1 for r in watchlist_results if r["entry_signal"] == "✨ 達進場條件")
        if ready > 0:
            lines.append(f"\n🎯 **本次掃描有 {ready} 檔達進場條件**，請評估加入持倉。")
        else:
            lines.append(f"\n📌 本次掃描 {len(watchlist_results)} 檔，**無達進場條件**，繼續觀察。")

        return "\n".join(lines) + "\n"


class IntelTeam:

    @staticmethod
    def get_vix_ndx_sox() -> tuple[float | None, float | None, float | None]:
        """
        [V22.25.5fix P0] 統一情報採集介面 — None 透傳，下游用 None 防禦正規化
        舊版返回 0.0/18.0 偽裝成正常值，現返回 None 讓下游可區分「無漲跌」和「數據不可用」
        """
        vix = PrecisionProxyEngine.get_vix()
        ndx = PrecisionProxyEngine.get_ndx()
        sox = PrecisionProxyEngine.get_sox()
        # 數據不可用時打印警告（不靜默）
        if vix is None or ndx is None or sox is None:
            missing = []
            if vix is None: missing.append("VIX")
            if ndx is None: missing.append("NDX")
            if sox is None: missing.append("SOX")
            print(f"   [情報官] ⚠️ Yahoo數據不可用: {', '.join(missing)} — 下游將用中性預設")
        return vix, ndx, sox

    @staticmethod
    def get_usd_cny() -> tuple[float, str]:
        """
        [V22.9 P0修復] USD/CNY實時匯率
        舊版硬編碼6.79，影響所有QDII預測
        多源fallback：Yahoo → 備用固定值 + 警示
        """
        # 源1：Yahoo Finance USDCNY=X
        try:
            r = _yahoo_get(
                "https://query2.finance.yahoo.com/v8/finance/chart/USDCNY%3DX?range=1d&interval=1d",
                headers={"User-Agent":"Mozilla/5.0"}, timeout=6)
            closes = [c for c in r.json()['chart']['result'][0]['indicators']['quote'][0]['close'] if c]
            if closes:
                return round(closes[-1], 4), "🟢 LIVE"
        except Exception as _e:
            _log_err(_e, "get_usd_cny")
        # 源2：AkShare
        try:
            import akshare as ak
            df = ak.currency_boc_safe()
            if df is not None and not df.empty:
                row = df[df.iloc[:,0].astype(str).str.contains("美元", na=False)]
                if not row.empty:
                    return round(float(row.iloc[0, 3]) / 100, 4), "🟡 AkShare"
        except Exception as _e:
            _log_err(_e, "get_usd_cny")
        # Fallback：固定值+警示
        # [雲端任務A] Yahoo+AkShare 均失敗 → 登記資料不可用,避免呼叫端把
        #   硬編碼6.79當成即時匯率往下算(QDII預測依賴此值)。
        _data_unavailable("USD_CNY", "get_usd_cny: Yahoo與AkShare均失敗,改用硬編碼6.79")
        return 6.7900, "🔴 HARDCODED(警告)"

    @staticmethod
    def get_gold_wti_yahoo() -> tuple[float, float, str]:
        """
        [V22.9] 黃金/WTI Yahoo Finance直連
        舊版：AkShare崩潰時使用固定預設($4675/$97)
        修復：Yahoo Finance作為第一選擇
        """
        gold, wti, src = 4675.0, 97.0, "🔴 預設值(警告)"
        # 黃金（V22.16 5/27 改 range=5d 以便計算漲跌率）
        try:
            r = _yahoo_get(
                "https://query2.finance.yahoo.com/v8/finance/chart/GC%3DF?range=5d&interval=1d",
                headers={"User-Agent":"Mozilla/5.0"}, timeout=6)
            closes = [c for c in r.json()['chart']['result'][0]['indicators']['quote'][0]['close'] if c]
            if closes:
                gold = round(closes[-1], 1)
                src = "🟢 LIVE"
        except Exception as _e:
            _log_err(_e, "get_gold_wti_yahoo")
        # WTI
        try:
            r = _yahoo_get(
                "https://query2.finance.yahoo.com/v8/finance/chart/CL%3DF?range=1d&interval=1d",
                headers={"User-Agent":"Mozilla/5.0"}, timeout=6)
            closes = [c for c in r.json()['chart']['result'][0]['indicators']['quote'][0]['close'] if c]
            if closes:
                wti = round(closes[-1], 2)
        except Exception:
            # AkShare fallback
            try:
                import akshare as ak
                df = ak.futures_foreign_commodity_realtime(symbol="CL")
                if df is not None and not df.empty:
                    wti = float(df["最新价"].iloc[0])
            except Exception as _e:
                _log_err(_e, "get_gold_wti_yahoo")
                # [雲端任務A] ★WTI 與黃金共用同一個 src 標籤,但只有黃金成功時才會被設成
                #   "🟢 LIVE"——若黃金抓到而WTI兩源都失敗,回傳值會是(真黃金, 假97.0, "🟢 LIVE"),
                #   呼叫端無法從 src 分辨 WTI 其實是硬編碼預設值。獨立登記,不依賴共用標籤。
                _data_unavailable("WTI", "get_gold_wti_yahoo: Yahoo與AkShare均失敗,改用硬編碼97.0"
                                  f"({type(_e).__name__})")
        if src != "🟢 LIVE":
            # 黃金本身也沒拿到真值(src 停在預設/未更新) → 一併登記,供訊號層查詢。
            _data_unavailable("GOLD", "get_gold_wti_yahoo: 黃金抓取失敗,改用硬編碼4675.0")
        return gold, wti, src

    @staticmethod
    def get_gold_ret() -> float:
        """
        [V22.16 5/27 新增] 黃金日漲跌率（給 compute_macro_prior 用）
        返回最近 1 日漲跌率 (e.g. +0.005 = +0.5%)
        失敗返回 0.0
        """
        try:
            r = _yahoo_get(
                "https://query2.finance.yahoo.com/v8/finance/chart/GC%3DF?range=5d&interval=1d",
                headers={"User-Agent":"Mozilla/5.0"}, timeout=6)
            closes = [c for c in r.json()['chart']['result'][0]['indicators']['quote'][0]['close'] if c]
            if len(closes) >= 2:
                return round((closes[-1] - closes[-2]) / closes[-2], 4)
        except Exception as _e:
            _log_err(_e, "get_gold_ret")
        return None  # [V22.25.5fix] 失敗返回 None, 下游有 None 防禦

    @staticmethod
    def get_5day_history(symbol: str) -> list:
        """
        [V22.17.4 新增] 抓取 Yahoo Finance 5 日歷史收盤價
        
        symbol 範例: "^VIX" / "^SOX" / "^NDX" / "GC=F" / "CL=F" / "CNY=X"
        失敗返回 [] （盤前報自適應降級為單日數據）
        """
        try:
            url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?range=5d&interval=1d"
            r = _yahoo_get(url, timeout=6)  # [V22.48] 代理預檢+快取回退
            if r is None: return []
            data = r.json()
            closes = data['chart']['result'][0]['indicators']['quote'][0]['close']
            return _series_outlier_filter([c for c in closes if c is not None], symbol)   # [V22.27.9] 量級離群過濾
        except Exception as _e:
            _log_err(_e, f"get_5day_history({symbol})")
            return []

    @staticmethod
    def get_cny_20ma() -> float:
        """
        [V22.17.4 新增] 計算 USD/CNY 20 日均線
        
        失敗返回 0.0（盤前報 20MA 偏離分析會略過）
        """
        try:
            url = "https://query2.finance.yahoo.com/v8/finance/chart/CNY%3DX?range=1mo&interval=1d"
            r = _yahoo_get(url, timeout=6)  # [V22.48] 代理預檢+快取回退
            if r is None: return []
            closes = [c for c in r.json()['chart']['result'][0]['indicators']['quote'][0]['close'] if c]
            if len(closes) >= 15:
                return round(sum(closes[-20:]) / len(closes[-20:]), 4)
            return round(sum(closes) / len(closes), 4) if closes else 0.0
        except Exception as _e:
            _log_err(_e, "get_cny_20ma")
            # [雲端任務A] 0.0 在「USD/CNY 20日均線偏離」計算裡是看起來合理的數字
            # (會被當成真的均線值往下算偏離百分比),登記不可用供訊號層查詢。
            _data_unavailable("CNY_20MA", f"get_cny_20ma: 抓取失敗,改用0.0 ({type(_e).__name__})")
            return 0.0

    @staticmethod
    def get_north_flow_direct() -> tuple[str, float]:
        """
        [V22.14] 北向實時已停止披露（2024-08-19監管變更）
        API 結構變化：s2nDay → hk2sh/hk2sz/sh2hk/sz2hk
        新欄位 hk2sh/hk2sz 永遠返回 0（無數據可披露）
        保留此函數為向下相容，永遠返回「實時已停止披露」
        """
        return "實時已停止披露(2024-08-19監管)", 0.0

    @staticmethod
    def get_market_sentiment_v14() -> dict:
        """
        [V22.14] 北向資金替代方案：市場資金面三合一
        替代指標：
        1. 滬深兩市成交額（市場活躍度）
        2. 主力資金淨流入（聰明錢方向）
        3. 南向資金（境內資金對港股態度，間接反映A股獲利了結意願）
        """
        result = {
            "amzn_total":  "待確認",   # 滬深成交額
            "main_force":  "待確認",   # 主力淨流入
            "south_flow":  "待確認",   # 南向資金
            "summary":     "市場資金面待確認",
            "raw":         {}
        }

        # 1. 南向資金（從原 push2 API 改新結構）
        try:
            url = "https://push2.eastmoney.com/api/qt/kamt/get?fields1=f1,f2,f3,f4&fields2=f51,f52&ut=b2884a393a59ad64002292a3e90d46a5"
            r = _em_throttled_get(url, timeout=6, tag="em_a",
                                  headers={"User-Agent":"Mozilla/5.0",
                                           "Referer":"https://data.eastmoney.com"})
            if r is None:
                # 原為 `return None`:南向一失敗就放棄另外兩個獨立資料源,且 main() 對 None
                # 呼叫 .get 導致整份報告崩潰。改交給下方 except 標示「南向採集失敗」後續跑。
                raise RuntimeError("南向端點無回應(限流/熔斷/網路)")
            data = r.json().get("data", {})
            # 新結構：sh2hk + sz2hk = 南向資金合計
            sh2hk = float(data.get("sh2hk", {}).get("dayNetAmtIn", 0) or 0)
            sz2hk = float(data.get("sz2hk", {}).get("dayNetAmtIn", 0) or 0)
            south_total = (sh2hk + sz2hk) / 1e8  # 萬元→億
            if abs(south_total) > 0.01:
                if south_total > 50:   sig = "🔴 南向強流入(A股獲利了結)"
                elif south_total > 10: sig = "🟠 南向溫和流入"
                elif south_total < -10:sig = "🟢 南向流出(A股回流)"
                else:                  sig = "⚪ 南向中性"
                result["south_flow"] = f"{south_total:+.1f}億 {sig}"
                result["raw"]["south_total"] = south_total
        except Exception as e:
            result["south_flow"] = f"南向採集失敗:{type(e).__name__}"

        # 2. 滬深兩市成交額（東方財富）
        try:
            # 上證指數成交額（sh000001）
            url2 = "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=10&po=1&fields=f1,f2,f3,f4,f5,f6,f12,f13,f14&fs=m:1+s:2,m:0+t:6,m:0+t:80"
            r2 = _em_throttled_get(url2, timeout=6, tag="em_b",
                                   headers={"User-Agent":"Mozilla/5.0",
                                            "Referer":"https://quote.eastmoney.com"})
            data2 = (r2.json().get("data", {}) if r2 is not None else {})
            # 累加上滬深主要指數成交額
            if data2 and data2.get("diff"):
                # 簡化：用全市場總成交額（粗略）
                total_amount = 0
                for item in data2.get("diff", [])[:5]:
                    try:
                        total_amount += float(item.get("f6", 0) or 0)
                    except Exception as _e:
                        _log_err(_e, "unknown")
                if total_amount > 0:
                    amzn_bn = total_amount / 1e8  # 元→億
                    if amzn_bn > 12000: sig = "🟢 極度活躍"
                    elif amzn_bn > 8000: sig = "🟢 活躍"
                    elif amzn_bn > 5000: sig = "🟡 中性"
                    else: sig = "🔴 清淡"
                    result["amzn_total"] = f"{amzn_bn:.0f}億 {sig}"
                    result["raw"]["amzn_total"] = amzn_bn
        except Exception as _e:
            _log_err(_e, "unknown")

        # 3. 主力資金淨流入（[V22.35] 已遷 fflow/daykline,見下)
        try:
            # [V22.35 治D12] ★f62 遷移:原用 stock/get(行情快照)+f62 —— f62 在資金流語境是「收盤價」
            #   非主力淨流入(同 V22.24.5 已為 get_order_flow_breakdown 查證並遷移的根因),
            #   導致全市場主力恆顯示「≈0億」(物理不合理,正常數百億)。
            #   改用已驗證可通的 fflow/daykline(同 get_etf_5day_flow):
            #     klines 每行 = 日期,主力淨流入(元),小單,中單,大單,超大單,...  取最後一行=今日。
            _mkt_main_bn = None
            try:
                _ts = int(time.time() * 1000)
                url3 = ("https://push2his.eastmoney.com/api/qt/stock/fflow/kline/get"
                        "?secid=1.000001&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56"
                        f"&klt=101&lmt=2&ut=fa5fd1943c7b386f172d6893dbfba10b&_={_ts}")
                # [V22.46] 同 F-03:此處自 V22.35 起亦為 NameError → 大盤主力【從未成功抓取】,
                #   報告一直顯示「— (fflow 採集失敗)」而我的 E-LOG 卻聲稱遷移成功。
                _r3 = _em_throttled_get(url3, timeout=8, tag="mkt_main")
                if _r3 is None:
                    raise RuntimeError("mkt_main fflow 取得失敗")
                _kl = ((_r3.json() or {}).get("data") or {}).get("klines") or []
                if _kl:
                    _parts = _kl[-1].split(",")
                    if len(_parts) >= 2:
                        _mkt_main_bn = float(_parts[1]) / 1e8   # f52 主力淨流入(元)→億
            except Exception as _e:
                _log_err(_e, "market_main_fflow")
            if _mkt_main_bn is not None:
                main_bn = _mkt_main_bn
                if abs(main_bn) < 1:   # 全市場≈0億仍屬異常,誠實標(不偽裝成信號)
                    sig = "⚠️ 數值異常(全市場≈0億不合理,疑接口變更)"
                elif main_bn > 100:  sig = "🟢 主力強流入"
                elif main_bn > 0:    sig = "🟡 主力小流入"
                elif main_bn > -100: sig = "🟠 主力小流出"
                else:                sig = "🔴 主力強流出"
                result["main_force"] = f"{main_bn:+.0f}億 {sig}"
                result["raw"]["main_force"] = main_bn
            else:
                result["main_force"] = "— (fflow 採集失敗)"   # 誠實:不填假值
        except Exception as _e:
            _log_err(_e, "unknown")

        # 綜合摘要
        parts = []
        if "億" in result["amzn_total"]: parts.append(f"成交{result['amzn_total']}")
        if "億" in result["main_force"]: parts.append(f"主力{result['main_force']}")
        if "億" in result["south_flow"]: parts.append(f"南向{result['south_flow']}")
        if parts:
            result["summary"] = " | ".join(parts)
        else:
            result["summary"] = "市場資金面採集失敗（北向實時2024-08已停止披露）"

        return result

    @staticmethod
    def format_header(vix, ndx_d, sox_d, gold, wti, usd_cny,
                      c_drain, strait_status, w_tw, w_a,
                      acc_tw, acc_a, r_type, style_groups=None,
                      usd_cny_src="🔴", gold_src="🔴", north_flow_str="待確認",
                      market_sentiment=None):
        """
        [V22.14] 北向資金行升級為「市場資金面」三合一
        market_sentiment = {amzn_total, main_force, south_flow, summary}
        """
        def _fmt(a):
            if not a or a.get("accuracy") is None:
                return "N/A (積累中)"
            return f"{a['accuracy']:.1%}({a['n']}筆)"
        geo_icon = "🔴" if strait_status=="closed_hostile" else _geo_icon_from_cdrain(c_drain)

        # [V22.14] 市場資金面行：替代「北向資金」
        if market_sentiment:
            sentiment_line = f"* 市場資金面：{market_sentiment.get('summary','—')}\n"
            sentiment_line += f"  ℹ️ 北向實時已停止披露(2024-08-19監管變更)，改用滬深成交+主力+南向\n"
        else:
            sentiment_line = f"* 北向資金：{north_flow_str}\n"

        header = (
            f"# {r_type} 戰報 | {__VERSION__} 九龍合璧版\n"
            f"* VIX={vix:.2f}🟢 | 納指={ndx_d:+.2%}🟢 | SOX={sox_d:+.2%}🟢 | "
            f"W台股={w_tw:.3f} | W_A股={w_a:.3f}\n"
            f"* 黃金=${gold:.0f}{gold_src} | WTI=${wti:.2f}🟡 | USD/CNY={usd_cny:.4f}{usd_cny_src}\n"
            f"{sentiment_line}"
            f"* 地緣/能源複合風險：{geo_icon} | c_drain={c_drain:.2f}（VIX+油價+金價滾動合成,非單一事件）\n"
            f"* 精度：台股={_fmt(acc_tw)} | A股={_fmt(acc_a)}\n"
        )
        if usd_cny_src.startswith("🔴"):
            header += f"> ⚠️ USD/CNY固定值，QDII預測可能有系統性偏差\n"

        # RS-Rank 摘要區塊
        if style_groups:
            header += "\n## 🏆 RS-Rank 同Style排名\n"
            for style, etfs in sorted(style_groups.items()):
                etf_strs = " | ".join(
                    f"**{TARGET_ETFS[c]['name'][:6]}** #{i+1}/{len(etfs)}"
                    for i, (c, ev) in enumerate(etfs))
                header += f"**{style}：** {etf_strs}\n"

        return header


# ══════════════════════════════════════════════════════════════════════
# 🛠️ DB工具
# ══════════════════════════════════════════════════════════════════════
def _init_db():
    with sqlite3.connect(LOG_DB, timeout=10) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                ts           TEXT NOT NULL,
                code         TEXT NOT NULL,
                name         TEXT NOT NULL,
                style        TEXT NOT NULL,
                ev           INTEGER NOT NULL,
                action       TEXT NOT NULL,
                geo_status   TEXT NOT NULL,
                c_drain      REAL NOT NULL,
                geo_adj      REAL NOT NULL,
                rsi          REAL,
                bias         REAL,
                z_score      REAL,
                actual_ret   REAL,
                error        REAL,
                corrected    INTEGER DEFAULT 0
            )""")
        # [V22.16 5/27 完整版 P1] DB schema 擴展（向下相容舊資料）
        for col, typ in [("predicted_ret","REAL DEFAULT 0.0"),
                          ("market","TEXT DEFAULT ''"),
                          ("report_type","TEXT DEFAULT ''"),
                          ("macro_prior","REAL DEFAULT 0.0"),
                          # [V22.24.0 因果鏈] 記錄每次預測「基於什麼因」, 積累因→果歷史(正/負樣本)
                          ("cause_tag","TEXT DEFAULT ''"),
                          # actual_ret 的來源(nav/exch_close/proxy/legacy),見 returns_store
                          ("actual_src","TEXT")]:
            # [V22.17.4] 改為 PRAGMA 預檢，避免 except+pass 違規
            existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(predictions)").fetchall()}
            if col not in existing_cols:
                try:
                    conn.execute(f"ALTER TABLE predictions ADD COLUMN {col} {typ}")
                except sqlite3.OperationalError as _e:
                    _log_err(_e, f"alter_table_{col}")

        # [V22.25.0 資金流情報底座] 每日資金流持久化 — 把單日沉澱成歷史。
        #   設計定調: 資金流是獨立情報維度(非±5輔助), 鯨吞蠶食=連續多日主力淨流入趨勢。
        #   有此底座, 限流只少一個數據點, 歷史趨勢斷不掉; 量價背離(量增價平=真領先)可算。
        #   (code,date)唯一: 同日重跑覆蓋(INSERT OR REPLACE)。nav_ret留待收盤回填。
        conn.execute("""
            CREATE TABLE IF NOT EXISTS capital_flow_daily (
                code        TEXT NOT NULL,
                date        TEXT NOT NULL,
                name        TEXT DEFAULT '',
                main_net    REAL,
                super_net   REAL,
                big_net     REAL,
                mid_net     REAL,
                small_net   REAL,
                nav_ret     REAL,
                ts          TEXT,
                PRIMARY KEY (code, date)
            )""")
        # ⚠️ 欄位名 nav_ret 是歷史遺留:實際存的是【場內代理 ETF 的當日漲跌,單位為百分比】
        #   (persist_capital_flow 傳入 intraday_proxy_ret×100),不是場外 NAV,也不是實現報酬。
        #   實現報酬一律讀 realized_returns(returns_store)。

        # 實現報酬單一來源。首次建表時以遷移前的 T+0 實現值(src=legacy)填入,之後由三條回填路徑
        #   依來源等級寫入(nav/exch_close > legacy > proxy)。
        _seeded = returns_store.seed_if_empty(conn)
        if _seeded:
            print(f"   [realized_returns] 首次建立,已由歷史 T+0 實現值填入 {_seeded} 筆(src=legacy)")

        # [V22.27.7 FixB] 持倉大本簿: positions_opening.csv + transactions.csv → DB
        #   錨點快照 + 交易流水從CSV遷入DB，消除「脆弱真相源」檔案依賴。
        conn.execute("""
            CREATE TABLE IF NOT EXISTS position_book (
                code        TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                buy         REAL DEFAULT 0,
                sell        REAL DEFAULT 0,
                current     REAL DEFAULT 0,
                status      TEXT DEFAULT '',
                anchor_date TEXT DEFAULT '',
                updated_at  TEXT DEFAULT ''
            )""")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT NOT NULL,
                code        TEXT NOT NULL,
                action      TEXT NOT NULL,
                shares      REAL DEFAULT 0,
                nav         REAL DEFAULT 0,
                amount      REAL DEFAULT 0,
                created_at  TEXT DEFAULT ''
            )""")






# FOMC 2026 官方排程(web驗證 federalreserve.gov), 決議日=會議第2天
_FOMC_2026 = ["2026-01-28","2026-03-18","2026-04-29","2026-06-17",
              "2026-07-29","2026-09-16","2026-10-28","2026-12-09"]
_FOMC_DOTPLOT = {"2026-03-18","2026-06-17","2026-09-16","2026-12-09"}


def generate_auto_events(today=None, horizon_days=45):
    """
    [V22.18 新增] 生成未來 horizon_days 天的規律事件(LPR/FOMC/CPI/非農/財報季)
    回傳 list of dict。失敗返回空 list(不影響手動事件)
    """
    try:
        if today is None:
            today = datetime.date.today()
        end = today + datetime.timedelta(days=horizon_days)
        events = []
        months = set()
        d = today
        while d <= end:
            months.add((d.year, d.month))
            d += datetime.timedelta(days=28)
        months.add((end.year, end.month))

        for (y, m) in sorted(months):
            # LPR 每月20(週末順延)
            lpr = _ae_adjust_weekend(datetime.date(y, m, 20))
            events.append({"date":lpr.isoformat(),"time":"09:15",
                "event":f"{y}年{m}月 LPR 利率發布","impact":"All",
                "severity":"critical","source":"auto"})
            # 美CPI: BLS公布日每月浮動(10-14日無固定規律), 寫死任何一天都會錯
            # [V22.22.3修正] 保守設每月10日(寧早提醒不晚), 明確標'必須手動核實'
            cpi = _ae_adjust_weekend(datetime.date(y, m, 10))
            events.append({"date":cpi.isoformat(),"time":"21:30",
                "event":f"美國 {m}月 CPI 公布(預估日,需手動核實BLS官網)","impact":"QDII+Defensive",
                "severity":"high","source":"auto-tentative"})
            # 美非農 每月第1個週五
            nfp = _ae_nth_weekday(y, m, 4, 1)
            if nfp:
                events.append({"date":nfp.isoformat(),"time":"20:30",
                    "event":f"美國 {m}月 非農就業數據","impact":"QDII",
                    "severity":"high","source":"auto-tentative"})
        # FOMC 官方排程
        for fdate in _FOMC_2026:
            fd = datetime.date.fromisoformat(fdate)
            if today <= fd <= end:
                dot = "(含點陣圖)" if fdate in _FOMC_DOTPLOT else ""
                events.append({"date":fdate,"time":"次日02:00",
                    "event":f"美聯儲 FOMC 利率決議{dot}","impact":"All",
                    "severity":"critical","source":"auto"})
        # A股財報季
        ew = {4:"A股年報+Q1季報密集披露期(4月)",8:"A股中報密集披露期(8月)",
              10:"A股Q3季報密集披露期(10月)"}
        for (y, m) in sorted(months):
            if m in ew:
                wd = datetime.date(y, m, 1)
                events.append({"date":wd.isoformat(),"time":"全月",
                    "event":ew[m],"impact":"All","severity":"medium","source":"auto"})

        events = [e for e in events if today.isoformat() <= e["date"] <= end.isoformat()]
        events.sort(key=lambda x: (x["date"], x["time"]))
        return events
    except Exception as _e:
        _log_err(_e, "generate_auto_events")
        return []


def load_events_csv():
    """
    [V22.17.4 新增 / V22.18 升級半自動] 讀取 events.csv + 合併自動規律事件

    格式（events.csv 與代碼同目錄）:
        date,time,event,impact,severity
        2026-06-02,09:00,Computex 2026 展期,Tech+QDII,high

    impact: All / Tech / QDII / Defensive / Cyclical / Dividend / Utility
    severity: critical / high / medium / low

    [V22.18] 自動合併 LPR/FOMC/CPI/非農/財報季規律事件(source=auto)
             手動事件優先,同日同類不重複。手動 events.csv 仍可手填突發事件
    返回: list of dict（依日期排序，未來事件優先）
    """
    import csv
    today = datetime.date.today().isoformat()
    manual = []
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "events.csv")
    if os.path.exists(csv_path):
        try:
            with open(csv_path, encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row = {k.strip(): (v.strip() if v else "") for k, v in row.items()}
                    if not row.get("date"):
                        continue
                    row["source"] = "manual"
                    manual.append(row)
        except Exception as _e:
            _log_err(_e, "load_events_csv")

    # [V22.18] 自動規律事件
    auto = generate_auto_events()
    # 合併: 手動優先, 去重邏輯:
    #   (1) 同日同類關鍵字 (2) 同月同類事件(LPR/CPI/FOMC/非農 每月唯一)
    _monthly_kw = ["LPR", "CPI", "FOMC", "非農"]
    merged = list(manual)
    for e in auto:
        e_month = e["date"][:7]  # YYYY-MM
        # 找出此自動事件屬於哪個月度關鍵字
        e_kw = next((kw for kw in _monthly_kw if kw in e["event"]), None)
        dup = False
        for me in manual:
            # 同日同類
            if e["date"] == me.get("date") and \
               (e["event"][:4] in me.get("event","") or me.get("event","")[:4] in e["event"]):
                dup = True
                break
            # 同月同類月度事件(手動已填則自動跳過)
            if e_kw and e_kw in me.get("event","") and me.get("date","")[:7] == e_month:
                dup = True
                break
        if not dup:
            merged.append(e)

    merged = [e for e in merged if e.get("date","") >= today]
    merged.sort(key=lambda x: (x.get("date",""), x.get("time","")))
    return merged




# ════════════════════════════════════════════════════════════════
# [V22.19 新增] 場外ETF 操作建議三分級引擎
# ════════════════════════════════════════════════════════════════
# 病根(用戶+WorkBuddy雙洞察): 系統用'單日技術信號'驅動場外ETF進出,
#   既沒考慮短線噪音(場外無時點優勢), 也沒考慮明天有催化事件
# 解法: 系統【主動判斷】每個操作信號等級, 寫進4報:
#   🔴可操作: 資金背離=中線信號, 凌駕短期事件, 建議執行
#   ⏸️暫緩:   減倉 + 該板塊1-2天有催化事件 → 等事件落地
#   🟡觀望:   僅單日技術信號(短線噪音) → 場外ETF不建議據此進出
# ════════════════════════════════════════════════════════════════
_EVENT_IMPACT_STYLE = {
    "Tech": ["Tech"], "QDII": ["Tech"],
    "Defensive": ["Defensive", "Dividend"], "Cyclical": ["Cyclical"],
    "Utility": ["Utility"], "NewEnergy": ["NewEnergy"], "Pharma": ["Pharma"],
    "All": ["Tech", "Cyclical", "Defensive", "Dividend", "Utility", "NewEnergy", "Pharma"],
}


def _styles_affected_by_event(impact_str):
    affected = set()
    for kw, styles in _EVENT_IMPACT_STYLE.items():
        if kw in (impact_str or ""):
            affected.update(styles)
    return affected


def get_upcoming_event_styles(events, today=None, lookahead_days=2):
    """找未來 lookahead_days 天內 high/critical 事件 → {style:[(事件名,日期)]}"""
    if today is None:
        today = datetime.date.today()
    horizon = today + datetime.timedelta(days=lookahead_days)
    result = {}
    for e in events or []:
        try:
            edate = datetime.date.fromisoformat(e.get("date", ""))
        except (ValueError, TypeError):
            continue
        if not (today <= edate <= horizon):
            continue
        if e.get("severity") not in ("critical", "high"):
            continue
        for s in _styles_affected_by_event(e.get("impact", "")):
            result.setdefault(s, []).append((e.get("event", "?")[:20], e.get("date", "")))
    return result


# ═══════════════════════════════════════════════════════════════════
# [V22.36] 原始信號跨日管線 — persistence(連續N日)的【前置基礎】
#   ★WB盲點A:若在 grade_action_signal【之後】數「連續N日減倉」,因減倉恆被降為 watch,
#     graded 輸出裡根本不存在「減倉」→ 計數恆 0 → 閘門永不開 → 修了等於沒修。
#     故必須落庫【分級之前】的原始 action,並把 streak 當【第三把鑰匙】傳進分級。
#   ★WB盲點B:現有 shadow_signals 無 action 欄、format_signal_statistics 只做當次快照不跨日
#     → 系統原本算不出「連續N日」,必須新建本表。
# ═══════════════════════════════════════════════════════════════════
def save_raw_signal(code, name, report_type, action_raw, trade_date=None):
    """落庫【分級前】的原始 action(每交易日每檔每報一筆,UPSERT)。"""
    try:
        trade_date = trade_date or _now_utc8().strftime("%Y-%m-%d")
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS signal_raw_daily(
                trade_date TEXT, code TEXT, name TEXT, report_type TEXT,
                action_raw TEXT, ts TEXT,
                PRIMARY KEY(trade_date, code, report_type))""")
            conn.execute("INSERT OR REPLACE INTO signal_raw_daily VALUES(?,?,?,?,?,?)",
                         (trade_date, code, name, report_type, action_raw or "",
                          datetime.datetime.now().isoformat(timespec="seconds")))
        return True
    except Exception as _e:
        _log_err(_e, f"save_raw_signal_{code}")
        return False

def read_reduce_streak(code, n=3):
    """讀近 n 個【有紀錄的交易日】中,該 code 原始信號含「減倉/賣」的天數。
    ★注意:回傳的是 n 日內的減倉【天數】(非嚴格連續),作為 persistence 代理指標;
      資料不足 n 日 → 回實際天數(不外推、不假裝)。無資料 → 0(保守:不放行)。"""
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS signal_raw_daily(
                trade_date TEXT, code TEXT, name TEXT, report_type TEXT,
                action_raw TEXT, ts TEXT,
                PRIMARY KEY(trade_date, code, report_type))""")
            rows = conn.execute(
                "SELECT trade_date, GROUP_CONCAT(action_raw) FROM signal_raw_daily "
                "WHERE code=? GROUP BY trade_date ORDER BY trade_date DESC LIMIT ?",
                (code, int(n))).fetchall()
        return sum(1 for _d, _a in rows
                   if _a and ("減倉" in _a or "賣" in _a))
    except Exception as _e:
        _log_err(_e, f"read_reduce_streak_{code}")
        return 0


# ═══════════════════════════════════════════════════════════════════
# [V22.58] ★減倉裁決【公式化】—— 取代原本的 if-else 三鑰匙否決閘
#   使用者原則:「公式驅動邏輯,不是 if-else 鏈」。原閘一是純查表:
#     有事件? 有背離? 有streak? → 三個都沒有就否決。
#   兩個根本問題:
#     ① 它不計算,只查表 → 無法反映證據【強度】,只反映證據【有無】。
#     ② 它把「沒有證據」當成「證據顯示不該賣」—— 這兩件事完全不同。
#   而證據正在變多:flow5 自 7/16 真、背離活了、脆弱度活了、EV 可歸因。
#   → 改為:各因子加權計分 → 分數裁決 → 並印出完整歸因鏈(同 EV 歸因鏈範式)。
#
#   ★誠實設計(㉘):
#     - 每個因子標【資料狀態】:有值才計分;NULL 記為「未知」而非 0 分。
#     - 回傳 coverage(可用因子/總因子)。覆蓋率低 → 分數本身不可信,報告要標。
#     - 權重目前為【未校準佔位值】,源碼與報告都明標,不假裝校準過。
#       校準需 ≥3 個獨立 regime 的樣本(㉘),目前 flow5 真實樣本僅數日。
# ═══════════════════════════════════════════════════════════════════
REDUCE_CONVICTION_THRESHOLD = 50      # ⚠️ 未校準佔位值
REDUCE_FACTOR_WEIGHTS = {             # ⚠️ 全部未校準,待 ≥3 regime 樣本後校準
    "事件臨近":     25,
    "資金頂背離":   30,
    "主力流出擴大": 20,
    "主力流出收斂": 5,
    "趨勢向下":     15,
    "RSI超買":      15,
    "脆弱度偏高":   15,
    "EV偏低":       10,
    "連續偏空":     10,
}

def read_prev_flow5(code, before_date=None):
    """[V22.61] 讀該檔【前一個有值交易日】的 flow5_yi,供公式判斷「流出擴大 vs 收斂」。
    ★根因(WB 核證,2026-07-23):V22.58 我寫 `locals().get("_dc_flow5_prev")`,
      但**該變數全檔從未被賦值** → 恆 None → `_worse` 恆 False →
      「主力流出擴大(+20)」永不觸發、「主力流出收斂(+5)」只要負值就觸發。
      實證:011613 五日主力 **-21.4 億強流出**,公式卻判「收斂(賣壓減輕)」——**決策誤導**。
      這是第四次「寫了一個永遠是 None 的名字」(前三次:_direct_session / POSITIONS_INFO / r.get('trend'))。
    回 float 或 None(無資料 → 公式記⬜無資料,不假裝「賣壓減輕」)。"""
    try:
        before_date = before_date or _now_utc8().strftime("%Y-%m-%d")
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            r = conn.execute(
                "SELECT flow5_yi FROM daily_snapshot "
                "WHERE code=? AND date<? AND flow5_yi IS NOT NULL "
                "ORDER BY date DESC LIMIT 1", (code, before_date)).fetchone()
        return float(r[0]) if r and r[0] is not None else None
    except Exception as _e:
        _log_err(_e, f"read_prev_flow5_{code}")
        return None


# ═══════════════════════════════════════════════════════════════════
# [V22.65] ★板塊資金流輪動 —— 治 7/23 最大盲區:看不到錢從哪個板塊流到哪個板塊
#   7/23 實況:三大指數收紅、4200檔上漲,但科創50 -4.09%、半導體 -4.10%;
#             錢從半導體流向電網設備(主力淨流入39億)/鋰礦/有色。
#             系統只看個別 ETF 的 flow5,【完全看不到這個輪動】→ 四報全天判「中性震盪」。
#   ★端點與欄位【全部來自 2026-07-24 使用者本機探針實測】,非憑記憶(㉒):
#     可用:push2delay.eastmoney.com/api/qt/clist/get?fs=m:90+t:2 (行業板塊)
#     ⚠️ 同次探針中 push2/push2his 全部 RemoteDisconnected,含對照組個股資金流
#        → 故 push2delay 排第一,其餘留作備援(端點可用性會變,不寫死單一條)
#   實測回傳欄位:f12=板塊代碼 f14=板塊名稱 f3=漲跌% f62=主力淨額(元)
#                f66=超大單淨額 f69=超大單淨佔比 f72=大單淨額 f75=大單淨佔比 f184=主力淨佔比
# ═══════════════════════════════════════════════════════════════════
SECTOR_FLOW_CANDIDATES = [
    ("push2delay.eastmoney.com", "m:90+t:2"),   # 2026-07-24 實測唯一可通
    ("push2.eastmoney.com",      "m:90+t:2"),
    ("push2his.eastmoney.com",   "m:90+t:2"),
    ("push2delay.eastmoney.com", "m:90+t:3"),   # 概念板塊(備援)
]
# [V22.67] 加請 f6(成交額)/f8(換手率)供擁擠度使用。★但【不假設它們一定回得來】——
#   WB 2026-07-24 實測確認的只有 f12/f14/f3/f62/f184/f66/f69/f72/f75;
#   f6/f8 是我加的候選,若東財不回,擁擠度自動降級用已驗證的 f62 算,不會壞。
SECTOR_FLOW_FIELDS = "f12,f14,f3,f6,f8,f62,f184,f66,f69,f72,f75"

# 持倉/觀察池 → 行業板塊關鍵詞(用於在板塊榜中定位自己的曝險)
#   ⚠️ 關鍵詞比對,非官方對照;無相符者誠實標「無對應板塊」,不硬湊。
HOLDING_SECTOR_KEYWORDS = {
    "008888": ["半导体", "集成电路"],          "011613": ["半导体", "集成电路"],
    "018177": ["半导体", "集成电路"],          "016008": ["消费电子"],
    "016185": ["电力", "电网设备"],            "004432": ["有色", "工业金属", "小金属"],
    "000216": ["贵金属"],                      "019141": ["通信"],
    "018135": ["软件", "互联网"],              "004070": ["证券"],
    "025856": ["电网设备"],                    "020356": ["半导体材料", "半导体设备"],
    "013180": ["电池", "能源金属"],            "018345": ["自动化设备", "通用设备"],
    "019072": ["通信"],                        "005693": ["航天装备", "地面兵装"],
}


def fetch_sector_flow(top_n=60):
    """抓行業板塊資金流排行。回 [{code,name,chg,net_yi,ratio}] 依主力淨額降序;失敗回 []。"""
    for host, fs in SECTOR_FLOW_CANDIDATES:
        url = (f"https://{host}/api/qt/clist/get?pn=1&pz={int(top_n)}&po=1&np=1"
               f"&fltt=2&invt=2&fid=f62&fs={fs}&fields={SECTOR_FLOW_FIELDS}"
               f"&ut=b2884a393a59ad64002292a3e90d46a5&_={int(time.time()*1000)}")
        r = _em_throttled_get(url, timeout=10, tag=f"sector:{host.split('.')[0]}", retries=2)
        if r is None:
            continue
        try:
            diff = ((r.json() or {}).get("data") or {}).get("diff") or []
        except Exception:
            diff = []
        if not diff:
            continue
        out = []
        for it in diff:
            try:
                net = it.get("f62")
                _amt = it.get("f6")      # 成交額(元),未驗證欄位 → 可能為 None/'-'
                _tor = it.get("f8")      # 換手率(%),同上
                out.append({"code": it.get("f12"), "name": str(it.get("f14") or ""),
                            "chg": it.get("f3"),
                            "net_yi": (float(net) / 1e8) if isinstance(net, (int, float)) else None,
                            "ratio": it.get("f184"),
                            "amount_yi": (float(_amt) / 1e8) if isinstance(_amt, (int, float)) else None,
                            "turnover": _tor if isinstance(_tor, (int, float)) else None})
            except Exception as _e:
                _log_err(_e, "fetch_sector_flow:L9134")  # [V24.16] 原為靜默 continue
                continue
        out = [x for x in out if x["net_yi"] is not None]
        if out:
            out.sort(key=lambda x: x["net_yi"], reverse=True)
            print(f"   ✅ [板塊輪動] 由 {host.split('.')[0]}/{fs} 取得 {len(out)} 個板塊")
            return out
    print("   ❌ [板塊輪動] 全部候選端點失敗 → 本區塊留空(誠實,不冒充)")
    return []


# ═══════════════════════════════════════════════════════════════════
# [V22.67] ★板塊擁擠度 —— 直擊 7/23 崩跌的【根本原因】
#   7/23 實況:科創50 -4.09%、半導體 -4.10%,而三大指數收紅、4200 檔上漲。
#   東吳證券研報:二季度公募基金雙創配置比例 **56.35%,首次超過主板**,
#   創業板/科創板配置均為 **2012 年以來歷史最高**;主板配置 43.55% 為 2012 年以來最低。
#   → **擁擠度到歷史極值** = 任何風吹草動都會踩踏。這是崩跌主因,
#     而系統有 RSI、有 trend、有資金流,【唯獨沒有「這個賽道多擁擠」】。
#
#   ★誠實設計(㉘):
#     - 主指標用【已驗證欄位 f62】算「資金集中度」= 該板塊主力淨額 / 全市場淨額絕對值總和
#     - 成交額佔比(需 f6)為【加分項】,回不來就不算,不假裝
#     - **分位需歷史**:今日起累積,樣本 < CROWD_MIN_DAYS 一律標「樣本不足·僅記錄」
#       絕不用 5 天資料生一個「歷史分位 87%」這種假精確
# ═══════════════════════════════════════════════════════════════════
CROWD_MIN_DAYS = 20     # 分位計算的最低樣本日數(㉘:不足即不生分位)

def save_sector_snapshot(sectors, date_str=None):
    """落庫每日板塊資金/成交快照,供擁擠度分位累積。回寫入筆數。"""
    if not sectors:
        return 0
    date_str = date_str or _now_utc8().strftime("%Y-%m-%d")
    n = 0
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS sector_flow_daily(
                date TEXT, code TEXT, name TEXT, chg REAL,
                net_yi REAL, ratio REAL, amount_yi REAL, turnover REAL,
                concentration REAL, ts TEXT, PRIMARY KEY(date, code))""")
            tot = sum(abs(x["net_yi"]) for x in sectors if x.get("net_yi") is not None) or 1.0
            for x in sectors:
                if x.get("net_yi") is None:
                    continue
                conn.execute("INSERT OR REPLACE INTO sector_flow_daily VALUES(?,?,?,?,?,?,?,?,?,?)",
                             (date_str, x.get("code"), x.get("name"), x.get("chg"),
                              x.get("net_yi"), x.get("ratio"), x.get("amount_yi"),
                              x.get("turnover"), round(abs(x["net_yi"]) / tot, 5),
                              datetime.datetime.now().isoformat(timespec="seconds")))
                n += 1
        return n
    except Exception as _e:
        _log_err(_e, "save_sector_snapshot")
        return 0


def compute_crowdedness(sector_name):
    """回 (今日集中度, 分位或None, 樣本日數, 說明)。樣本不足時分位回 None(不生假精確)。"""
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            rows = conn.execute(
                "SELECT date, concentration FROM sector_flow_daily "
                "WHERE name=? AND concentration IS NOT NULL ORDER BY date", (sector_name,)).fetchall()
    except Exception as _e:
        _log_err(_e, f"compute_crowdedness_{sector_name}")
        return None, None, 0, "查詢失敗"
    if not rows:
        return None, None, 0, "⬜ 無資料"
    cur = rows[-1][1]
    ndays = len(rows)
    if ndays < CROWD_MIN_DAYS:
        return cur, None, ndays, (f"樣本 {ndays}/{CROWD_MIN_DAYS} 日·**不足以生分位**"
                                  f"(㉘:僅記錄,不判讀)")
    hist = sorted(r[1] for r in rows)
    pct = sum(1 for v in hist if v <= cur) / len(hist)
    if pct >= 0.9:
        lab = "🔴 極度擁擠(歷史前10%)·踩踏風險高"
    elif pct >= 0.7:
        lab = "🟠 偏擁擠"
    elif pct <= 0.2:
        lab = "🟢 冷門(歷史後20%)·起飛空間大"
    else:
        lab = "⚪ 中性"
    return cur, pct, ndays, f"{lab}(分位 {pct*100:.0f}%,樣本 {ndays} 日)"


def format_crowdedness(sectors, codes=None):
    """擁擠度報告區塊。無資料/樣本不足時誠實標示,不冒充。"""
    if not sectors:
        return ""
    loc = locate_holdings_in_sectors(sectors, codes or [])
    names = sorted({v[0] for v in loc.values() if v[0]})
    if not names:
        return ""
    L = ["\n### 🌡️ 板塊擁擠度(踩踏風險)\n",
         "> 擁擠度 = 該板塊主力淨額佔全市場淨額絕對值總和之比重;分位需 "
         f"{CROWD_MIN_DAYS} 日以上樣本\n\n",
         "| 板塊 | 今日集中度 | 歷史分位 | 判讀 |\n|---|---|---|---|\n"]
    for nm in names:
        cur, pct, nd, note = compute_crowdedness(nm)
        L.append(f"| {nm} | {(f'{cur*100:.2f}%' if cur is not None else '—')} | "
                 f"{(f'{pct*100:.0f}%' if pct is not None else '⬜ 樣本不足')} | {note} |\n")
    L.append("> ⚠️ 7/23 教訓:公募雙創配置 56.35% 創 2012 年來最高 → 擁擠度極值,"
             "指數收紅仍可科技獨跌。**擁擠度高≠立刻跌,但下跌時跌得更兇**\n")
    return "".join(L)


def locate_holdings_in_sectors(sectors, codes):
    """在板塊榜中定位持倉曝險。回 {code: (板塊名, 排名, 淨額億, 全體板塊數)} 或 (None,...)。"""
    res = {}
    if not sectors:
        return res
    n = len(sectors)
    for c in codes:
        kws = HOLDING_SECTOR_KEYWORDS.get(c)
        if not kws:
            res[c] = (None, None, None, n)
            continue
        best = None
        for i, sec in enumerate(sectors, 1):
            if any(k in sec["name"] for k in kws):
                if best is None or sec["net_yi"] > best[2]:
                    best = (sec["name"], i, sec["net_yi"])
        res[c] = (best[0], best[1], best[2], n) if best else (None, None, None, n)
    return res


def format_sector_rotation(sectors, codes=None, top=5):
    """產生「錢往哪流」報告區塊。無資料回空字串(不冒充)。"""
    if not sectors:
        return ("> ⬜ **板塊輪動:資料不可用**(端點失敗)→ 本維度無法判讀,"
                "**請勿當作『無輪動』**\n")
    n = len(sectors)
    inflow = sectors[:top]
    outflow = sectors[-top:][::-1]
    L = ["\n### 🔄 板塊資金輪動(錢從哪來、往哪去)\n",
         f"> 依主力淨額排序,共 {n} 個行業板塊\n\n",
         "| 方向 | 板塊 | 漲跌 | 主力淨額 | 淨佔比 |\n|---|---|---|---|---|\n"]
    for x in inflow:
        L.append(f"| 🟢 流入 | {x['name']} | {x['chg']:+.2f}% | **{x['net_yi']:+.2f}億** | "
                 f"{x['ratio']:+.2f}% |\n" if isinstance(x['chg'], (int, float)) else
                 f"| 🟢 流入 | {x['name']} | — | **{x['net_yi']:+.2f}億** | — |\n")
    for x in outflow:
        # [修正] 方向標記依【實際正負】,不依排名位置 —— 否則「末段但淨額為正」會被誤標流出
        _d = "🔴 流出" if x["net_yi"] < 0 else "🟡 末段(仍淨流入)"
        L.append(f"| {_d} | {x['name']} | {x['chg']:+.2f}% | **{x['net_yi']:+.2f}億** | "
                 f"{x['ratio']:+.2f}% |\n" if isinstance(x['chg'], (int, float)) else
                 f"| {_d} | {x['name']} | — | **{x['net_yi']:+.2f}億** | — |\n")
    if codes:
        loc = locate_holdings_in_sectors(sectors, codes)
        hit = [(c, v) for c, v in loc.items() if v[1] is not None]
        if hit:
            hit.sort(key=lambda kv: kv[1][1])
            L.append("\n> **你的持倉在輪動中的位置**\n")
            for c, (nm, rank, net, tot) in hit:
                pct = rank / tot
                # [修正] 排名+實際正負【雙條件】,避免「排名末段但仍淨流入」被誤判
                if pct <= 0.2 and net > 0:
                    tag = "🟢 資金流入前段(被輪入)"
                elif pct >= 0.8 and net < 0:
                    tag = "🔴 資金流出後段(被輪出)"
                elif net < 0:
                    tag = "🟠 淨流出(中段)"
                else:
                    tag = "⚪ 中段"
                L.append(f"> - {c} → 板塊「{nm}」排名 **{rank}/{tot}**,"
                         f"主力 {net:+.2f}億 {tag}\n")
            miss = [c for c, v in loc.items() if v[1] is None]
            if miss:
                L.append(f"> - ⬜ 無對應板塊(不硬湊):{'、'.join(miss)}\n")
    L.append("> ⚠️ 板塊資金流為【全市場行業維度】,與個股 flow5 尺度不同,兩者互補非替代\n")
    return "".join(L)


def compute_reduce_conviction(upcoming=None, divergence_tag=None, flow5_yi=None,
                              flow5_prev=None, trend=None, rsi=None,
                              fragility=None, ev=None, reduce_streak=0):
    """減倉信心分數。回 (score, coverage, trace)。
    trace = [(因子, 得分, 說明)];未知因子以 score=None 記錄,不當 0 分。"""
    W = REDUCE_FACTOR_WEIGHTS
    trace, score, known, total = [], 0, 0, 0

    def add(name, cond, pts, desc, available=True):
        nonlocal score, known, total
        total += 1
        if not available:
            trace.append((name, None, "⬜ 無資料")); return
        known += 1
        if cond:
            score += pts; trace.append((name, pts, f"✅ {desc}"))
        else:
            trace.append((name, 0, f"— {desc}"))

    add("事件臨近", bool(upcoming), W["事件臨近"],
        (f"臨近「{upcoming[0][0]}」" if upcoming else "無臨近事件"))
    add("資金頂背離", bool(divergence_tag and "頂背離" in str(divergence_tag)),
        W["資金頂背離"], (str(divergence_tag)[:26] if divergence_tag else "無背離"),
        available=(divergence_tag is not None))
    _f_avail = flow5_yi is not None
    _worse = (_f_avail and flow5_prev is not None and flow5_yi < flow5_prev)
    add("主力流出擴大", (_f_avail and flow5_yi < 0 and _worse), W["主力流出擴大"],
        (f"5日主力{flow5_yi:+.1f}億" +
         (f"·**流出擴大**(前值{flow5_prev:+.1f}億)" if _worse else "")
         if _f_avail else ""),
        available=(_f_avail and flow5_prev is not None))
    # [V22.61] ★「收斂」是【比較】結論,沒有前值就【不能下結論】。
    #   原本 available=_f_avail → 只要有今日值就判定,前值缺失時預設「收斂」= 無證據卻說賣壓減輕。
    #   改為 available 需【兩個值都有】,否則記⬜無資料。
    add("主力流出收斂", (_f_avail and flow5_prev is not None and flow5_yi < 0 and not _worse),
        W["主力流出收斂"],
        (f"5日主力{flow5_yi:+.1f}億·流出收斂(前值{flow5_prev:+.1f}億→賣壓減輕)"
         if (_f_avail and flow5_prev is not None) else ""),
        available=(_f_avail and flow5_prev is not None))
    add("趨勢向下", (trend is not None and trend < -0.30), W["趨勢向下"],
        (f"trend={trend:+.2f}" if trend is not None else ""), available=(trend is not None))
    add("RSI超買", (rsi is not None and rsi >= 70), W["RSI超買"],
        (f"RSI={rsi:.0f}" if rsi is not None else ""), available=(rsi is not None))
    add("脆弱度偏高", (fragility is not None and fragility >= 50), W["脆弱度偏高"],
        (f"脆弱度={fragility}" if fragility is not None else ""),
        available=(fragility is not None))
    add("EV偏低", (ev is not None and ev < 30), W["EV偏低"],
        (f"EV={ev}" if ev is not None else ""), available=(ev is not None))
    add("連續偏空", (reduce_streak or 0) >= 2, W["連續偏空"],
        f"近{DCAP_REDUCE_STREAK_N}日{reduce_streak}日偏空(⚠️未校準)")

    coverage = known / total if total else 0.0
    return score, coverage, trace


def format_reduce_trace(score, coverage, trace):
    """把裁決過程印成可讀歸因鏈(同 EV 歸因鏈範式:讓用戶看得到憑什麼)。"""
    hit = [f"{n}+{p}" for n, p, _d in trace if p]
    unk = [n for n, p, _d in trace if p is None]
    txt = (f"減倉信心 **{score}/{REDUCE_CONVICTION_THRESHOLD}**"
           f"({'/'.join(hit) if hit else '無正向因子'})"
           f" | 因子覆蓋 {coverage*100:.0f}%")
    if unk:
        txt += f" | ⬜無資料:{'、'.join(unk)}"
    txt += " | ⚠️權重未校準(待≥3獨立regime樣本)"
    return txt


def grade_action_signal(action, style, divergence_tag, upcoming_event_styles,
                        reduce_streak=0, flow5_yi=None, flow5_prev=None,
                        trend=None, rsi=None, fragility=None, ev=None):
    """[V22.19/V22.22] 場外ETF操作信號分級 → (grade, icon, note)
    [V22.22 非對稱事件過濾] 重大事件臨近時:
      減倉/賣出(降低風險) → 放行(防禦絕對優先,不綁住逃命的手)
      買入/主攻(增加風險) → 暫緩(防追高)
    病根修復: 舊版'減倉+事件→hold'會在CPI前夕鎖住減倉,讓用戶抱著風險過事件
    """
    is_reduce = "減倉" in (action or "") or "賣" in (action or "")
    is_attack = "主攻" in (action or "") or "買" in (action or "")
    has_divergence = bool(divergence_tag)
    upcoming = upcoming_event_styles.get(style, []) if upcoming_event_styles else []
    # 🔴可操作(最高優先): 資金背離=中線信號, 凌駕短期事件
    if has_divergence:
        return ("actionable", "🔴", "可操作：資金背離=中線信號(主力動向)，凌駕短期事件，建議執行")
    # [V22.22 非對稱] 減倉=降低風險, 事件臨近也放行(防禦優先)
    if is_reduce:
        if upcoming:
            return ("actionable", "🔴",
                    f"可操作：臨近「{upcoming[0][0]}」事件，但減倉=降低風險，防禦優先放行")
        # [V22.36 第三把鑰匙] 連續N日偏空 ≠ 單日噪音 → 升格可操作。
        #   ★治「008888 上週天天喊減倉、天天輸出觀望,利潤蒸發35%」:原邏輯【寫死】
        #     假設每個減倉都是單日噪音,系統無跨日記憶,連5天與只1天完全同權。
        #   ⚠️ N=3 為【保守佔位值,待回測校準】(鐵律⑳):須用 actual_returns 回測
        #     N=2/3/5 的命中率與盈虧後才定案;現在不假裝它校準過。
        # [V22.58] ★公式化裁決取代 if-else 三鑰匙(使用者:「公式驅動邏輯,不是 if-else 鏈」)
        _sc, _cov, _tr = compute_reduce_conviction(
            upcoming=upcoming, divergence_tag=divergence_tag,
            flow5_yi=flow5_yi, flow5_prev=flow5_prev, trend=trend,
            rsi=rsi, fragility=fragility, ev=ev, reduce_streak=reduce_streak)
        _tx = format_reduce_trace(_sc, _cov, _tr)
        if _sc >= REDUCE_CONVICTION_THRESHOLD:
            # ★不自動執行:升格為可操作 + 明標依據未校準,決定權交回用戶(使用者拍板 C+公式)
            return ("actionable", "🔴",
                    f"可操作·**建議減倉**:{_tx}"
                    + ("  ⚠️ **因子覆蓋不足 50%,分數可信度低,請人工判斷**"
                       if _cov < 0.5 else "")
                    + "  ➡️ 依據未經校準,**最終由你裁決**")
        # 未達門檻:仍把分數與缺口攤開,讓用戶看得見「差多少、差在哪」
        return ("watch", "🟡",
                f"觀望(未達減倉門檻):{_tx}"
                f"  ➡️ 差 {REDUCE_CONVICTION_THRESHOLD - _sc} 分;"
                f"若你判斷該賣,原始信號仍在上方【原始:】欄可執行")

    # [V22.22 非對稱] 買入=增加風險, 事件臨近則暫緩(防追高)
    if is_attack:
        if upcoming:
            return ("hold", "⏸️",
                    f"暫緩：臨近「{upcoming[0][0]}」事件，禁止建倉追高(防禦優先)")
        return ("watch", "🟡", "觀望：僅單日技術信號，場外ETF無時點優勢，不建議據此短線進出")
    return ("none", "", "")












def compute_order(code, info, action, ev, kelly_pct, current_styles_pct=None,
                  risk_off_level=0,
                  _dcap_trend=None, _dcap_flow5=None, _dcap_rsi=None,
                  _dcap_frag=None, _dcap_val=None):
    """
    [V22.17.2] 操作指令化
    [V22.17.4 新增] risk_off_level 參數
        0 = 正常 / 1 = 輕度避險（Kelly 減半）/ 2 = 重度避險（強制靜默）
    [V22.17.5 新增] kelly_pct 類型自動防呆
        若意外傳入 dict，自動取 'half_kelly' × 100；否則用 0
    """
    # [V22.17.5] 雙保險：防止 KellyEngine.compute() dict 直接傳入
    if isinstance(kelly_pct, dict):
        kelly_pct = kelly_pct.get("half_kelly", 0.0) * 100  # 0.114 → 11.4%
    elif kelly_pct is None:
        kelly_pct = 0.0
    elif not isinstance(kelly_pct, (int, float)):
        kelly_pct = 0.0  # 未知類型保守處理
    """
    [V22.17.2 新增] 操作指令化 — 把信號轉成具體金額指令
    
    輸入:
        code, info: ETF 代碼/資訊
        action: 信號文字（含「主攻」/「減倉」/「清倉」/「靜默」）
        ev: 系統 EV 評分 (0-100)
        kelly_pct: Kelly 建議倉位百分比 (0-25)
        current_styles_pct: dict {style: 當前百分比}（給 Style cap 用）
    
    返回:
        (order_str, delta_amount, warning_flag)
        order_str: 「💡 建議：賣出 1,350 元（減倉 50%）」
        delta_amount: -1350 (負=賣, 正=買, 0=持有)
        warning_flag: 是否大額操作（>30%）
    
    決策樹:
        💥 清倉 → 全賣 100%
        🔴 減倉:
          EV < 25 → 賣 70%（重度，接近清倉）
          EV < 35 → 賣 50%（中度）
          EV ≥ 35 → 賣 30%（輕度）
        🟢 主攻:
          未持倉 → 試水 500 元
          已持倉 → 加碼至 Kelly 目標（受 Style cap + Single cap）
        🟡 靜默 → 維持
    """
    current = USER_HOLDINGS.get(code, 0.0)
    style = info.get("style", "?")
    is_qdii = _is_qdii_etf(info)
    style_key = "QDII" if is_qdii else style
    
    # ── 清倉 ──
    if "清倉" in action or "💥" in action:
        if current < 50:
            return ("🟡 未持倉，無需操作", 0, False)
        return (f"💥 **全部賣出 {current:.0f} 元**（清倉）", -current, True)
    
    # ── 減倉 ──
    if "減倉" in action:
        if current < 50:
            return ("🟡 未持倉，減倉信號不適用", 0, False)
        if ev < 25:
            ratio, label = 0.70, "重度減倉 70%"
        elif ev < 35:
            ratio, label = 0.50, "中度減倉 50%"
        else:
            ratio, label = 0.30, "輕度減倉 30%"
        sell = round(current * ratio, 0)
        warn = ratio >= 0.50
        return (f"🔴 **賣出 {sell:.0f} 元**（{label}，剩 {current-sell:.0f} 元）",
                -sell, warn)
    
    # ── 主攻 ──
    if "主攻" in action or "✨" in action:
        # [V22.17.4] Risk-off 護欄
        if risk_off_level >= 2:
            return (f"🟡 **重度 Risk-off，主攻降級為靜默（維持現倉 {current:.0f} 元）**",
                    0, True)
        
        # 計算 Kelly 目標金額
        effective_kelly = (kelly_pct or 0)
        if risk_off_level == 1:
            # 輕度避險：Kelly 減半
            effective_kelly = effective_kelly * 0.5
        target_amt = TOTAL_CAPITAL * effective_kelly / 100.0
        # 單檔上限保護
        single_cap = TOTAL_CAPITAL * SINGLE_CAP_PCT / 100.0
        target_amt = min(target_amt, single_cap)
        # Style 上限保護 — [V22.33] 條件式動態上限
        if current_styles_pct:
            cur_pct = current_styles_pct.get(style_key, 0)
            cap, _allowed, _dmsg = check_dynamic_cap(
                style_key, cur_pct, trend=_dcap_trend, flow5_yi=_dcap_flow5,
                rsi=_dcap_rsi, fragility=_dcap_frag, val_pct=_dcap_val)  # [V22.33.2] 修:val_pctl→val_pct(原TypeError,買路徑崩)
            if not _allowed:
                return (_dmsg or f"⚠️ **{style_key} 已達 {cur_pct:.1f}% (上限 {cap}%)，建議維持現倉**",
                        0, False)
            if _dmsg:
                print(f"   [V22.33動態上限] {_dmsg.splitlines()[0]}")
            # Style 剩餘額度
            remaining_pct = cap - cur_pct
            remaining_amt = TOTAL_CAPITAL * remaining_pct / 100.0
            target_amt = min(target_amt, current + remaining_amt)
        delta = target_amt - current
        # 未持倉的試水位邏輯
        if current < 50 and delta > 0:
            buy = min(PILOT_AMT, delta)
            return (f"🟢 **買入 {buy:.0f} 元**（試水起手，未持倉）", buy, False)
        # 已持倉
        if delta < 100:
            return ("🟢 已達 Kelly 目標，維持現倉", 0, False)
        buy = round(delta, 0)
        # ── [V24.21] 進場時機分批(使用者 2026-08-10:「004432 加碼+2133元,認真的??什麼底氣??」) ──
        #   查明:Style 上限是【容量約束】不是【時機判斷】——它只回答「最多能放多少」,
        #   不回答「現在是不是好時機」。004432 RSI=76.88、乖離=11.34%、系統自己寫
        #   「均值回歸壓力上升」,買入路徑卻【完全沒有讀 RSI/乖離/Z】→ 一次把 3.4% 填到 15%。
        #   ★這正是投資哲學說的「集中不害人,在高點集中才害人」的教科書案例。
        #   處置:**不禁止、不減額度上限**,只把「一次買滿」改成「分批」並寫明理由。
        #   金額仍全額保留在建議裡,使用者要一次買進系統不擋(不剝奪選擇權)。
        _tranche, _why = 1.0, []
        try:
            _r = _dcap_rsi
            if _r is not None and _r >= 75:
                _tranche = min(_tranche, 0.34); _why.append(f"RSI {_r:.0f}≥75(超買)")
            elif _r is not None and _r >= 70:
                _tranche = min(_tranche, 0.50); _why.append(f"RSI {_r:.0f}≥70(偏熱)")
            _b = info.get("_bias_pct") if isinstance(info, dict) else None
            if _b is not None and abs(_b) >= 10:
                _tranche = min(_tranche, 0.34); _why.append(f"乖離 {_b:+.1f}%≥10")
        except Exception as _e:
            _log_err(_e, "compute_order_tranche")
        if _tranche < 1.0 and buy >= 300:
            _first = round(buy * _tranche, 0)
            warn = buy > TOTAL_CAPITAL * 0.10
            return (f"🟢 **建議分批買入:首批 {_first:.0f} 元**"
                    f"（總額度 {buy:.0f} 元仍保留,分 {round(1/_tranche)} 批）\n"
                    f"   └ 分批理由:{' / '.join(_why)}——**額度沒問題,是進場時機偏熱**。"
                    f"系統不擋你一次買滿,但一次填滿等於把成本鎖在高位;"
                    f"回落或量能確認後再補後續批次,平均成本會更好。",
                    _first, warn)
        warn = buy > TOTAL_CAPITAL * 0.10  # 單筆 >10% 總資金警示
        risk_label = "（Kelly 減半，輕度避險）" if risk_off_level == 1 else ""
        return (f"🟢 **買入 {buy:.0f} 元**（達 Kelly {effective_kelly:.1f}% 目標{risk_label}）",
                buy, warn)
    
    # ── 靜默 ──
    return ("🟡 維持現倉，觀察", 0, False)


def _db_write(conn, code, info, ev, action, geo_status, c_drain,
              geo_adj, tech, final_pred, report_type="", macro_prior=0.0,
              cause_tag=""):
    """
    [V22.16 5/27 P1 重構] 加 report_type 區分四報
    
    原邏輯：每天每 code 只保留 1 筆（UPDATE 模式 / 最後一次跑覆蓋）
    新邏輯：每天每 code 每 report_type 保留 1 筆（最多 4 筆/天）
      - 盤前/早報/午報/收盤 各 1 筆
      - 同 report_type 重跑會 UPDATE（保護重跑）
      - 不同 report_type 自動 INSERT 新行
    
    向下相容：
      report_type 預設 "" 與舊資料一致
      如果不傳 report_type，行為跟 V22.15 完全一樣
    """
    ts = datetime.datetime.now().isoformat()
    today = datetime.date.today()
    # [V22.17.0 防禦 5/28] 確保新欄位存在（冪等，不依賴 _init_db 是否被呼叫）
    # 根因：用戶舊 DB 缺 report_type/macro_prior，_init_db 可能沒跑到
    # 此處每次寫入前確保欄位
    # [V22.17.4] 改為 PRAGMA 預檢，避免 except+pass 違規
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(predictions)").fetchall()}
    for _col, _typ in [("report_type", "TEXT DEFAULT ''"),
                       ("macro_prior", "REAL DEFAULT 0.0"),
                       ("predicted_ret", "REAL DEFAULT 0.0"),
                       ("market", "TEXT DEFAULT ''"),
                       ("cause_tag", "TEXT DEFAULT ''")]:
        if _col not in existing_cols:
            try:
                conn.execute(f"ALTER TABLE predictions ADD COLUMN {_col} {_typ}")
            except sqlite3.OperationalError as _e:
                _log_err(_e, f"alter_table_{_col}")
    # UPDATE 條件加 report_type
    up = conn.execute("""
        UPDATE predictions
        SET ts=?,predicted_ret=?,ev=?,action=?,geo_status=?,
            c_drain=?,geo_adj=?,rsi=?,bias=?,z_score=?,
            corrected=0,market=?,macro_prior=?,cause_tag=?
        WHERE code=? AND date(ts)=? AND report_type=?
          AND rowid=(SELECT MAX(rowid) FROM predictions
                     WHERE code=? AND date(ts)=? AND report_type=?)
    """, (ts, final_pred, int(ev), action, geo_status, c_drain, geo_adj,
          tech.get("RSI"), tech.get("BIAS"), tech.get("Z_Score"),
          info["market"], macro_prior, cause_tag,
          code, today, report_type,
          code, today, report_type)).rowcount
    if up == 0:
        conn.execute("""
            INSERT INTO predictions
            (ts,code,name,style,ev,action,geo_status,c_drain,geo_adj,
             rsi,bias,z_score,predicted_ret,market,report_type,macro_prior,cause_tag)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (ts, code, info["name"], info["style"], int(ev), action,
              geo_status, c_drain, geo_adj,
              tech.get("RSI"), tech.get("BIAS"), tech.get("Z_Score"),
              final_pred, info["market"], report_type, macro_prior, cause_tag))


# ═══════════════════════════════════════════════════════════════════
# 📐 資訊係數引擎 (Information Coefficient) — Grinold-Kahn 主動管理基本法則
#   IR = TC × IC × √Breadth。IC = 預測報酬與實際報酬的相關係數,衡量「幅度技巧」
#   (非僅方向命中)。只用早報 pre-move 預測;⚠️但早報 09:59 已見開盤後走勢 → 屬【同期相關】,
#   非純前瞻,高 IC 恐高估技巧(V22.89 已於計分卡加 caveat 顯示;真前瞻 IC 須 T+1 分離樣本另審)。
#   小樣本往零收縮 + TC<1(場外/不可空/集中度) + breadth 去同質化。
# ═══════════════════════════════════════════════════════════════════
_IC_CACHE = {}
_HOMOGENEOUS_CLUSTERS = [
    {"011613", "018177"},   # 科創50 ≈ 科創增強(≥95%重疊)
    {"006479", "018967"},   # 納指QDII ×2
]



def compute_information_coefficient(market="A股", window_days=60):
    """從早報 pre-move 預測算 IC。回傳 dict 或 None(防禦)。"""
    try:
        conn = sqlite3.connect(LOG_DB)
        rows = conn.execute("""
            SELECT predicted_ret, actual_ret FROM predictions
            WHERE report_type='早報' AND corrected=1
              AND actual_ret IS NOT NULL AND predicted_ret IS NOT NULL
              AND predicted_ret!=0.0 AND ABS(actual_ret)>0.0001
              AND market=? AND ts>=date('now', ?)
        """, (market, f"-{int(window_days)} days")).fetchall()
        conn.close()
    except Exception:
        return None
    pairs = [(float(p), float(a)) for p, a in rows if p is not None and a is not None]
    n = len(pairs)
    if n < 10:
        return {"n": n, "ic": None, "rank_ic": None, "ic_shrunk": None, "note": "樣本不足(<10)"}
    xs = [p for p, _ in pairs]; ys = [a for _, a in pairs]
    ic = _pearson(xs, ys); rank_ic = _spearman(xs, ys)
    if ic is None:
        return {"n": n, "ic": None, "rank_ic": rank_ic, "ic_shrunk": None, "note": "預測無變異"}
    k = 20.0                              # 小樣本往零收縮:IC × N/(N+k)
    ic_shrunk = ic * n / (n + k)
    se = 1.0 / ((n - 1) ** 0.5)           # IC 標準誤近似
    # [V22.89] 誠實 caveat:早報 09:59 預測已含當日開盤後觀察 → 此 IC 為【同期相關】非純前瞻,
    #   高值(>0.3)恐高估技巧。真前瞻 IC 須用 T+1 分離樣本另審(非本函式一行可修,標註為先)。
    _ic_caveat = ("⚠️此為早報同期相關(預測時已見開盤後走勢),非純前瞻IC,高值恐高估技巧"
                  if abs(ic) > 0.3 else None)
    return {"n": n, "ic": ic, "rank_ic": rank_ic, "ic_shrunk": ic_shrunk,
            "se": se, "significant": bool(abs(ic) > 2 * se), "caveat": _ic_caveat}

def compute_forward_ic(roll_n=60):
    """[V22.90] 真前瞻 IC(治早報同期 IC=0.69 虛高的方法論根因)。
    早報 09:59 的 pre-move 預測已含當日開盤後走勢 → 與當日 actual 算的是【同期相關】,虛高。
    真前瞻應用【目標日之前】寫入的預測:morning_premise.t1_pred 由收盤報(~17:30,已收盤)寫入,
    對 T+1;配對各檔 T 之後第一個實現報酬(realized_returns)→ 預測時不可能看到 T+1 走勢,
    零 look-ahead。配對邏輯與收盤計分板相同。來源:Claude patch_forward_ic.py,
    WB 折入(自開 LOG_DB、吃模組級 _pearson/_spearman/load_morning_premise)+ 行為測試。
    (原本配對 daily_snapshot.today_ret —— 那是系統當日估值,不是實現報酬。)"""
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            pdates = [r[0] for r in conn.execute(
                "SELECT date FROM morning_premise ORDER BY date DESC LIMIT ?", (roll_n,)).fetchall()]
            preds, acts = [], []
            rmap = returns_store.load_map(conn)
            rdates = returns_store.dates_by_code(rmap)
            for pd_ in pdates:
                prem = load_morning_premise(conn, pd_)
                if not prem:
                    continue
                act = {c: r for c, (_d, r) in _realized_next_by_code(rdates, rmap, pd_).items()}
                if not act:
                    continue
                for c, p in prem.items():
                    if not isinstance(p, dict):
                        continue
                    a = act.get(c); tp = p.get("t1_pred")
                    if a is None or tp is None or tp == 0:
                        continue
                    preds.append(tp); acts.append(a)
    except Exception:
        return {"n": 0, "ic": None, "rank_ic": None, "note": "無 morning_premise 表/紀錄"}
    n = len(preds)
    if n < 10:
        return {"n": n, "ic": None, "rank_ic": None, "note": f"前瞻樣本不足({n})"}
    ic_fwd = _pearson(preds, acts)
    if ic_fwd is None:
        return {"n": n, "ic": None, "rank_ic": _spearman(preds, acts), "note": "預測無變異"}
    return {"n": n, "ic": ic_fwd, "rank_ic": _spearman(preds, acts), "note": "真前瞻(零look-ahead)"}



def _effective_breadth(held_codes):
    """去同質化的獨立 bet 數(科創群/QDII群各併為1)。"""
    remaining = set(held_codes); count = 0
    for cluster in _HOMOGENEOUS_CLUSTERS:
        if remaining & cluster:
            count += 1; remaining -= cluster
    return max(count + len(remaining), 1)

_TRADABLE_IC_CACHE = {}
TRADABLE_IC_MIN_DAYS = 30   # 有效樣本以交易日計(同日各檔高度連動,不以筆數計)


def compute_tradable_ic(market="A股", window_days=90, today=None, db=None):
    """可交易口徑的 IC:截單前訊號(早/午報,每 (code, 日) 午報優先)的 predicted_ret
    vs 同 code 次一交易日的實現報酬(realized_returns)—— 15:00 前下單真正拿到的那一天。

    與 compute_information_coefficient 的差別:後者配對【同日】實際,而早報 09:59 已見開盤後走勢,
    屬同期推估(nowcast),IC 必然虛高;拿它放大倉位等於用答案估自己的能力。
    收縮與標準誤以【交易日數】為有效樣本:IC×n_days/(n_days+20)、SE≈1/√(n_days−1)。"""
    today = today or datetime.date.today()
    cutoff = (today - datetime.timedelta(days=window_days)).isoformat()
    try:
        with sqlite3.connect(db or LOG_DB, timeout=10) as conn:
            rows = conn.execute(
                "SELECT code, date(ts), report_type, predicted_ret, ts FROM predictions "
                "WHERE report_type IN ('早報','午報') AND market=? AND date(ts) BETWEEN ? AND ?",
                (market, cutoff, today.isoformat())).fetchall()
            rmap = returns_store.load_map(conn)
    except Exception as _e:
        _log_err(_e, "compute_tradable_ic")
        _data_unavailable("TRADABLE_IC", f"{type(_e).__name__}: {_e}")
        return None
    sig = {}
    for code, d, rt, pred, ts in rows:
        if pred is None or pred == 0.0:
            continue
        rk = (rt == "午報", ts or "")
        if (code, d) not in sig or rk > sig[(code, d)][0]:
            sig[(code, d)] = (rk, pred)
    rdates = returns_store.dates_by_code(rmap)
    xs, ys, days = [], [], set()
    for (code, d), (_rk, pred) in sig.items():
        nd = next((x for x in rdates.get(code, []) if x > d), None)
        if not nd or (datetime.date.fromisoformat(nd) - datetime.date.fromisoformat(d)).days > REALIZED_MAX_GAP_DAYS:
            continue
        a = rmap[(code, nd)]
        if abs(a) <= 0.0001:
            continue
        xs.append(pred); ys.append(a); days.add(d)
    n, n_days = len(xs), len(days)
    base = {"n": n, "n_days": n_days, "ic": None, "rank_ic": None, "ic_shrunk": None,
            "se": None, "significant": False}
    if n_days < 2 or n < 10:
        return {**base, "note": f"可交易樣本不足({n_days} 個交易日)"}
    ic = _pearson(xs, ys)
    if ic is None:
        return {**base, "rank_ic": _spearman(xs, ys), "note": "預測無變異"}
    se = 1.0 / ((n_days - 1) ** 0.5)
    return {**base, "ic": ic, "rank_ic": _spearman(xs, ys), "ic_shrunk": ic * n_days / (n_days + 20.0),
            "se": se, "significant": bool(abs(ic) > 2 * se), "note": "可交易口徑(訊號→次一交易日實現)"}


def ic_conviction_multiplier(market="A股"):
    """技巧型倉位乘子:量到的幅度技巧低→收縮倉位。防禦:不確定/樣本<30 個交易日→1.0。
    參考 IC=0.05(機構「正常技巧」基準);窄區間 0.70~1.15,scale-not-gate,不暴衝。
    改用可交易口徑 IC(compute_tradable_ic):原本用早報同期 IC(含開盤後走勢),
    數值普遍 >0.3,乘子幾乎恆為上限 1.15 —— 放大倉位的依據其實是 nowcast,不是預測技巧。"""
    try:
        ic = _TRADABLE_IC_CACHE.get(market) or compute_tradable_ic(market=market)
        if not ic or ic.get("ic_shrunk") is None or ic.get("n_days", 0) < TRADABLE_IC_MIN_DAYS:
            return 1.0
        return max(0.70, min(1.15, ic["ic_shrunk"] / 0.05))
    except Exception:
        return 1.0

def _tradable_ic_line(market, mult):
    t = _TRADABLE_IC_CACHE.get(market) or compute_tradable_ic(market=market) or {}
    if t.get("ic") is None:
        return (f"　• 🔍 可交易IC(訊號→次一交易日實現):{t.get('note', '無資料')}"
                f" → 倉位乘子 ×{mult:.2f}(中性)")
    sig = "✅顯著" if t.get("significant") else "⚠️未達顯著"
    return (f"　• 🔍 <b>可交易IC</b>(訊號→次一交易日實現)= <b>{t['ic']:+.3f}</b> {sig}"
            f"　收縮後 {t['ic_shrunk']:+.3f}　[{t['n_days']} 個交易日/{t['n']} 筆] → <b>倉位乘子 ×{mult:.2f}</b>")


def render_ic_scorecard(held_codes=None, market="A股"):
    """機構級技巧計分卡:IC / rank-IC / 收縮IC / breadth / TC / 隱含IR。
    [V22.90] 加「真前瞻IC vs 早報同期IC」對照行,把同期虛高常設可見。"""
    ic = _IC_CACHE.get(market) or compute_information_coefficient(market=market)
    fwd = compute_forward_ic()          # [V22.90] 真前瞻(零look-ahead),自開 LOG_DB
    if not ic or ic.get("ic") is None:
        n = ic.get("n", 0) if ic else 0
        _msg = (f"📐 <b>資訊係數(IC)計分卡</b>:早報 pre-move 樣本不足({n}筆),"
                f"需累積≥10筆才能評估幅度技巧(非方向命中)。")
        if fwd and fwd.get("ic") is not None:
            _msg += (f"<br>　• 🔍 真前瞻IC(零look-ahead, t1_pred×T+1收盤)= "
                     f"<b>{fwd['ic']:+.3f}</b> [n={fwd['n']}]")
        else:
            _msg += (f"<br>　• 🔍 真前瞻IC 亦待累積(morning_premise×T+1 樣本 "
                     f"{fwd.get('n',0) if fwd else 0} 筆)— 收盤報寫前提+隔日快照即可填")
        return _msg
    breadth = _effective_breadth(held_codes or [])
    TC = 0.60                             # 場外NAV結算(無盤中)+不可放空+集中度→保守
    ic_s = ic["ic_shrunk"]
    # 隱含 IR 用可交易 IC:同期 IC 含開盤後走勢,代入會把 IR 灌大數倍
    _tic = (_TRADABLE_IC_CACHE.get(market) or compute_tradable_ic(market=market) or {}).get("ic_shrunk")
    implied_ir = TC * _tic * (breadth ** 0.5) if _tic is not None else None
    a = abs(ic["ic"])
    grade = ("強(機構頂段)" if a >= 0.10 else "正常(有效技巧)" if a >= 0.05
             else "弱(勉強有訊號)" if a >= 0.03 else "近噪音(慎用)")
    sig = "✅顯著" if ic.get("significant") else "⚠️未達顯著(在樣本噪音內)"
    mult = ic_conviction_multiplier(market)
    _rows = [
        "📐 <b>資訊係數(IC)計分卡</b> — Grinold-Kahn 主動管理基本法則(衡量幅度技巧,非方向命中)",
        f"　• IC(幅度相關)= <b>{ic['ic']:+.3f}</b> [{grade}] {sig}",
        f"　• rank-IC(排序技巧)= {ic['rank_ic']:+.3f}　樣本 {ic['n']} 筆(早報09:59,含開盤後觀察)",
        f"　• 收縮IC(小樣本往零)= {ic_s:+.3f}(同期口徑,僅供對照,不驅動倉位)",
        _tradable_ic_line(market, mult),
        f"　• 獨立breadth(去同質)= {breadth}　TC(實作摩擦)≈ {TC:.2f}",
        (f"　• 隱含IR = TC×可交易IC×√Breadth = <b>{implied_ir:+.3f}</b>(每橫斷面決策,非年化承諾)"
         if implied_ir is not None else "　• 隱含IR:可交易 IC 樣本不足,暫不計算(不以同期 IC 代入)"),
    ]
    if ic.get("caveat"):   # [V22.89] 高值誠實標註,不假裝是純前瞻技巧
        _rows.append(f"　• {ic['caveat']}")
    # [V22.90] 真前瞻IC 對照行:把同期虛高「常設可見」
    if fwd and fwd.get("ic") is not None:
        if fwd["ic"] not in (0, None) and ic["ic"] not in (0, None):
            _ratio = abs(ic["ic"]) / abs(fwd["ic"])
            _cmp = (f"→ 早報同期較真前瞻高估≈{_ratio:.1f}x"
                    if _ratio >= 1.0 else
                    f"→ 真前瞻未低於同期(比值 {_ratio:.1f}x,同期未高估)")
            _rows.append(
                f"　• 🔍 真前瞻IC(零look-ahead)= <b>{fwd['ic']:+.3f}</b> [n={fwd['n']}]"
                f"　vs 早報同期IC= {ic['ic']:+.3f} {_cmp}")
        else:
            _rows.append(
                f"　• 🔍 真前瞻IC(零look-ahead)= <b>{fwd['ic']:+.3f}</b> [n={fwd['n']}]"
                f"　vs 早報同期IC= {ic['ic']:+.3f}")
    else:
        _rows.append(
            f"　• 🔍 真前瞻IC 待累積(morning_premise×T+1 樣本 "
            f"{fwd.get('n',0) if fwd else 0} 筆)— 跑幾次收盤報+隔日快照即自動出值,讓同期虛高常設可見")
    return "<br>".join(_rows)

def _get_accuracy():
    result = {}
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            for mkt in ["台股","A股"]:
                row = conn.execute("""
                    SELECT COUNT(*),
                           SUM(CASE WHEN (predicted_ret>0 AND actual_ret>0)
                                      OR (predicted_ret<0 AND actual_ret<0)
                               THEN 1 ELSE 0 END),
                           AVG(ABS(error))
                    FROM predictions
                    WHERE corrected=1 AND ts>=date('now','-20 days')
                      AND predicted_ret!=0.0
                      AND actual_ret IS NOT NULL
                      AND ABS(COALESCE(actual_ret,0))>0.0001
                      AND market=?
                      AND (report_type IS NULL OR report_type='' OR report_type!='收盤報')
                """, (mkt,)).fetchone()
                if row and row[0]:
                    result[mkt] = {"n": row[0],
                                   "accuracy": round((row[1] or 0)/row[0],4),
                                   "mae": round(row[2] or 0, 6)}
                else:
                    result[mkt] = {"n":0, "accuracy":None, "mae":None}
    except Exception as _ae:
        # [雲端任務A] 此回傳不含 "accuracy" 鍵 → main() 的 mkt_acc.get("accuracy", 0.6)
        # 會拿到憑空的 60% 勝率與 0.8% 誤差去算 Kelly 倉位。登記不可用供報告列出。
        _data_unavailable("ACCURACY", f"_get_accuracy 讀取預測紀錄失敗,Kelly 改用預設勝率60%/誤差0.8% "
                          f"({type(_ae).__name__}: {_ae})")
        result = {"台股":{"n":0},"A股":{"n":0}}
    return result


def _get_signal_payoff(window_days=None, today=None, db=None):
    """可交易口徑的訊號報酬分佈(供 Kelly)。回 {市場: {n, n_days, win_rate, avg_win, avg_loss}}。

    訊號:15:00 截單前發出的早報/午報,每 (code, 日) 取一筆(午報優先,其次最晚)的 predicted_ret 方向。
    結果:同 code 次一交易日的實現報酬(realized_returns 單一來源)。
    單筆訊號報酬 = sign(predicted_ret) × 次日報酬。失敗或無資料 → n=0(Kelly 退回最小倉位,不捏造)。
    """
    window_days = SIGNAL_PAYOFF_WINDOW_DAYS if window_days is None else window_days
    today = today or datetime.date.today()
    cutoff = (today - datetime.timedelta(days=window_days)).isoformat()
    empty = {m: {"n": 0, "n_days": 0, "win_rate": None, "avg_win": None, "avg_loss": None}
             for m in ("台股", "A股")}
    try:
        with sqlite3.connect(db or LOG_DB, timeout=10) as conn:
            rows = conn.execute(
                "SELECT code, date(ts), market, report_type, predicted_ret, actual_ret, ts "
                "FROM predictions WHERE report_type IN ('早報','午報') AND date(ts) >= ? "
                "AND date(ts) <= ?", (cutoff, today.isoformat())).fetchall()
            t0 = {k: v for k, v in returns_store.load_map(conn).items() if abs(v) > 0.0001}
    except Exception as _e:
        _log_err(_e, "_get_signal_payoff")
        _data_unavailable("SIGNAL_PAYOFF", f"{type(_e).__name__}: {_e}")
        return empty
    signal = {}
    for code, d, mkt, rt, pred, act, ts in rows:
        if pred is not None and pred != 0.0:
            rank = (rt == "午報", ts or "")
            if (code, d) not in signal or rank > signal[(code, d)][0]:
                signal[(code, d)] = (rank, pred, mkt)
    days_by_code = {}
    for (code, d) in t0:
        days_by_code.setdefault(code, []).append(d)
    for v in days_by_code.values():
        v.sort()
    out = {m: [] for m in ("台股", "A股")}
    for (code, d), (_, pred, mkt) in signal.items():
        nxt = next((d2 for d2 in days_by_code.get(code, []) if d2 > d), None)
        if nxt is None or mkt not in out:
            continue
        if (datetime.date.fromisoformat(nxt) - datetime.date.fromisoformat(d)).days > SIGNAL_PAYOFF_MAX_GAP:
            continue
        out[mkt].append((d, (1.0 if pred > 0 else -1.0) * t0[(code, nxt)]))
    result = {}
    for mkt, items in out.items():
        rets = [r for _, r in items]
        wins = [r for r in rets if r > 0]
        losses = [-r for r in rets if r < 0]
        result[mkt] = {
            "n": len(rets), "n_days": len({d for d, _ in items}),
            "win_rate": (len(wins) / len(rets)) if rets else None,
            "avg_win": (sum(wins) / len(wins)) if wins else None,
            "avg_loss": (sum(losses) / len(losses)) if losses else None,
        }
    return result


def _send_telegram(msg, silent=False):
    if not TG_BOT_TOKEN or TG_BOT_TOKEN.startswith("YOUR"):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id":TG_CHAT_ID,"text":msg,
                  "parse_mode":"Markdown","disable_notification":silent},
            timeout=5)
    except Exception as _e:
        _log_err(_e, "_send_telegram")


# ══════════════════════════════════════════════════════════════════════
# 📊 四報追蹤表自動更新（V22.11+）
# ══════════════════════════════════════════════════════════════════════




def _mirror_four_report_db(date_str, early, morning, noon, closing):
    """[V22.76] 四報追蹤 DB 鏡射(加性·非破壞)。把當日合併四報決策鏡寫進
    four_report_daily 表,使 DB 成為穩健真相源(治『xlsx 脆弱真相源』架構債第一階段)。
    xlsx 寫入完全不動,保留為人可視儀表板。冪等:date 主鍵 INSERT OR REPLACE。
    失敗靜默(不拖累收盤報主流程)。"""
    try:
        import json as _json
        with sqlite3.connect(LOG_DB, timeout=10) as _conn:
            _conn.execute("CREATE TABLE IF NOT EXISTS four_report_daily("
                          "date TEXT PRIMARY KEY, early TEXT, morning TEXT, "
                          "noon TEXT, closing TEXT, ts TEXT)")
            _conn.execute("INSERT OR REPLACE INTO four_report_daily VALUES(?,?,?,?,?,?)",
                          (date_str,
                           _json.dumps(early or {}, ensure_ascii=False),
                           _json.dumps(morning or {}, ensure_ascii=False),
                           _json.dumps(noon or {}, ensure_ascii=False),
                           _json.dumps(closing or {}, ensure_ascii=False),
                           datetime.datetime.now().isoformat(timespec="seconds")))
    except Exception as _e:
        _log_err(_e, "mirror_four_report_db")

def update_four_report_tracker(report_type: str, results: list,
                                tracker_path: str = None):
    """
    四報追蹤表自動更新
    - 收盤報跑完時呼叫，把今日 4 報決策合併寫入 Excel
    - 從 SQLite predictions 表讀取前 3 報（盤前/早/午）
    - 收盤報當下用 results 參數
    - 不會覆寫歷史資料，僅新增當日列或更新今日列
    """
    if not _OPENPYXL_OK:
        return
    if report_type != "收盤報":
        return  # 只在收盤報觸發
    
    if tracker_path is None:
        tracker_path = os.path.join(BASE_DIR, "V22_13_四報追蹤表.xlsx")
    
    today = datetime.date.today()
    today_str = today.isoformat()
    
    # ── 從 DB 取今日盤前/早報/午報的決策 ─────────────────────
    early_decisions = {}  # {code: (action_symbol)}
    morning_decisions = {}
    noon_decisions = {}
    
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            rows = conn.execute("""
                SELECT code, ts, action FROM predictions
                WHERE date(ts)=? ORDER BY ts ASC
            """, (today_str,)).fetchall()
        
        # 用時間區段分類
        for code, ts, action in rows:
            hour = int(ts.split("T")[1][:2]) if "T" in ts else 0
            symbol = _action_to_symbol(action)
            if hour < 9:
                early_decisions[code] = symbol
            elif 9 <= hour < 12:
                morning_decisions[code] = symbol
            elif 12 <= hour < 15:
                noon_decisions[code] = symbol
    except Exception as e:
        print(f"   ⚠️ 追蹤表：讀 DB 失敗 {e}")
    
    # 收盤報結果直接用 results
    closing_decisions = {r.get("code",""): _action_to_symbol(r.get("action","")) for r in results}

    # [V22.76 xlsx→DB 遷移·第一階段:加性鏡射] ★把當日合併四報決策鏡寫進 DB(穩健真相源),
    #   xlsx 照舊寫、保留為人可視儀表板 → 非破壞。冪等(date 主鍵)。第二階段(讀改走 DB、
    #   xlsx 降為視圖)待鏡射累積驗證後再切,不在本輪強行 cut-over(避免毀 6 分頁儀表板)。
    _mirror_four_report_db(today_str, early_decisions, morning_decisions,
                           noon_decisions, closing_decisions)

    # ── 創建或載入 Excel ───────────────────────────────────────
    if os.path.exists(tracker_path):
        wb = load_workbook(tracker_path)
        ws = wb["每日四報追蹤"] if "每日四報追蹤" in wb.sheetnames else wb.active
    else:
        wb = Workbook()
        ws = wb.active
        ws.title = "每日四報追蹤"
        # 標題
        ws["A1"] = "📊 V22.11 ETF 系統 — 四報決策追蹤表"
        ws["A1"].font = Font(name="Arial", size=14, bold=True)
        ws.merge_cells("A1:K1")
        ws["A1"].alignment = Alignment(horizontal="center")
        # 欄位
        headers = ["日期", "ETF代碼", "ETF名稱", "Style",
                   "盤前", "早報", "午報", "收盤",
                   "一致性", "實際走勢%", "判定"]
        for col, h in enumerate(headers, 1):
            c = ws.cell(row=3, column=col, value=h)
            c.font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
            c.fill = PatternFill("solid", start_color="1F4E78")
            c.alignment = Alignment(horizontal="center")
        # 欄寬
        for i, w in enumerate([12, 11, 22, 12, 8, 8, 8, 8, 11, 12, 11], 1):
            ws.column_dimensions[get_column_letter(i)].width = w
    
    # ── 找今日已有列，避免重複插入 ─────────────────────────────
    today_rows = {}  # {code: row_number}
    max_row = ws.max_row
    for r in range(4, max_row + 1):
        date_val = ws.cell(row=r, column=1).value
        code_val = ws.cell(row=r, column=2).value
        if str(date_val) == today_str and code_val:
            today_rows[str(code_val)] = r
    
    # ── 寫入/更新當日資料 ───────────────────────────────────
    start_row = max_row + 1 if not today_rows else None
    
    for result in results:
        code = result["code"]
        name = result.get("name", "")
        style = result.get("style", "")
        
        if code in today_rows:
            row = today_rows[code]
        else:
            row = ws.max_row + 1
        
        ws.cell(row=row, column=1, value=today_str).font = Font(name="Arial", size=10)
        ws.cell(row=row, column=2, value=code).font = Font(name="Arial", size=10)
        ws.cell(row=row, column=3, value=name).font = Font(name="Arial", size=10)
        ws.cell(row=row, column=4, value=style).font = Font(name="Arial", size=10)
        
        # 4 報決策
        ws.cell(row=row, column=5, value=early_decisions.get(code, "—"))
        ws.cell(row=row, column=6, value=morning_decisions.get(code, "—"))
        ws.cell(row=row, column=7, value=noon_decisions.get(code, "—"))
        ws.cell(row=row, column=8, value=closing_decisions.get(code, "—"))
        
        for col in range(5, 9):
            ws.cell(row=row, column=col).alignment = Alignment(horizontal="center")
            ws.cell(row=row, column=col).font = Font(name="Arial", size=10)
        
        # 一致性公式
        cons = f'=IF(COUNTIF(E{row}:H{row},H{row})=4,"✅ 一致","⚠️ 反覆")'
        ws.cell(row=row, column=9, value=cons).font = Font(name="Arial", size=10)
        ws.cell(row=row, column=9).alignment = Alignment(horizontal="center")
        
        # [V22.25.11] J(實際走勢%)今日NAV尚未結算 → 先留空、判定標「待結算」。
        # 真正的 J 由下方「後驗回填 pass」只認 DB actual_ret(權威值)填入，絕不用預測值冒充。
        ws.cell(row=row, column=11, value=_compute_judgment(closing_decisions.get(code, "—"), None))
        ws.cell(row=row, column=11).font = Font(name="Arial", size=10)
        ws.cell(row=row, column=11).alignment = Alignment(horizontal="center")

    # ═══ [V22.25.11] 後驗回填 pass：J/K 僅以 DB actual_ret(實際NAV,權威)為準 ═══
    # 處理場外NAV的T+1結算延遲:某日信號的實際走勢,在actual_ret結算後(後續任一收盤報)才回填。
    # 鐵律:只認DB真實actual_ret;DB無 → J留空、判定「待結算」。絕不用預測值冒充(杜絕假✅對)。
    # 此 pass 同時自癒既有污染列(如被填成pred的J:因DB無對應actual,會被歸正為待結算)。
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            _ar = conn.execute(
                "SELECT date(ts) d, code, actual_ret FROM predictions WHERE actual_ret IS NOT NULL"
            ).fetchall()
        actual_map = {(str(d), str(c)): ar for d, c, ar in _ar if ar is not None}
        n_fill = 0
        for r in range(4, ws.max_row + 1):
            dv = str(ws.cell(r, 1).value)[:10]; cv = str(ws.cell(r, 2).value)
            hv = ws.cell(r, 8).value
            if not dv[:4].isdigit():
                continue
            # [V22.25.13] 同日驗證。6/22 地真數據證實:收盤 pred 對應「同日」NAV
            # (11檔 pred vs 同日實際 平均誤差 0.25%、方向 7/7),且校正鏈
            # backfill_yesterday_actuals 本就是 prediction[D] ↔ actual[D] 同日配對。
            # 撤回 V22.25.12 的 T+1(那是從「明日主攻」敘事誤推,被地真數據否決)。
            ar = actual_map.get((dv, cv))
            if ar is not None:
                j_pct = round(float(ar) * 100, 2)   # actual_ret 為分數 → ×100;同日實際 NAV%
                ws.cell(r, 10, j_pct)
                ws.cell(r, 11, _compute_judgment(hv, j_pct))
                n_fill += 1
            else:
                # 同日 NAV 尚未結算/無資料 → J留空、判定待結算(若該格曾被pred污染,於此清除)
                # 注意:ws.cell(r,10,None) 在 openpyxl 中 value=None 被視為「只取格、不設值」,
                # 不會清空 → 必須用 .value=None 才真正清除。
                ws.cell(r, 10).value = None
                ws.cell(r, 11).value = _compute_judgment(hv, None)
        print(f"   [追蹤表] 後驗回填(同日):{n_fill} 列以 DB actual_ret 填實際走勢+判定;其餘待結算")
    except Exception as _e:
        _log_err(_e, "backfill_judgments_v2513")

    # ═══ [V22.13] 新增 Sheet 2-6 ═══════════════════════
    # Sheet 2: Regime市場狀態判斷記錄
    if "Regime判斷" not in wb.sheetnames:
        ws2 = wb.create_sheet("Regime判斷")
        ws2["A1"] = "🌡️ V22.13 市場狀態檢測記錄"
        ws2["A1"].font = Font(name="Arial", size=14, bold=True)
        ws2.merge_cells("A1:H1")
        headers2 = ["日期", "ETF代碼", "ETF名稱", "Regime",
                    "強度", "證據", "連漲天數", "5日漲幅"]
        for col, h in enumerate(headers2, 1):
            c = ws2.cell(row=3, column=col, value=h)
            c.font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
            c.fill = PatternFill("solid", start_color="2E7D32")
            c.alignment = Alignment(horizontal="center")
        for i, w in enumerate([12, 11, 22, 14, 8, 30, 10, 10], 1):
            ws2.column_dimensions[get_column_letter(i)].width = w
    else:
        ws2 = wb["Regime判斷"]
    # 寫入今日 regime 數據
    today_rows_2 = {}
    for r in range(4, ws2.max_row + 1):
        d = ws2.cell(row=r, column=1).value
        c = ws2.cell(row=r, column=2).value
        if str(d) == today_str and c:
            today_rows_2[str(c)] = r
    for result in results:
        code = result["code"]
        tech = result.get("tech", {})
        row = today_rows_2.get(code, ws2.max_row + 1)
        ws2.cell(row=row, column=1, value=today_str)
        ws2.cell(row=row, column=2, value=code)
        ws2.cell(row=row, column=3, value=result.get("name", ""))
        ws2.cell(row=row, column=4, value=tech.get("Regime", "ranging"))
        ws2.cell(row=row, column=5, value=tech.get("Regime_Strength", 0))
        ws2.cell(row=row, column=6, value=tech.get("Regime_Evidence", "")[:50])
        ev_text = tech.get("Regime_Evidence", "")
        # 從證據提取連漲天數
        import re as _re
        m1 = _re.search(r"連[漲跌](\d+)/5", ev_text)
        m2 = _re.search(r"5日[漲跌幅]+([+-]?\d+\.?\d*%)", ev_text)
        ws2.cell(row=row, column=7, value=m1.group(1) if m1 else "")
        ws2.cell(row=row, column=8, value=m2.group(1) if m2 else "")
        for col in range(1, 9):
            ws2.cell(row=row, column=col).font = Font(name="Arial", size=10)

    # Sheet 3: 信號統計總覽（累計）
    if "信號統計總覽" not in wb.sheetnames:
        ws3 = wb.create_sheet("信號統計總覽")
        ws3["A1"] = "📈 V22.14 信號統計總覽（從DB累計）"
        ws3["A1"].font = Font(name="Arial", size=14, bold=True)
        ws3.merge_cells("A1:H1")
        h3 = ["ETF代碼", "名稱", "主攻次數", "靜默次數", "減倉次數",
              "命中次數", "錯失次數", "可信度%"]
        for col, h in enumerate(h3, 1):
            c = ws3.cell(row=3, column=col, value=h)
            c.font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
            c.fill = PatternFill("solid", start_color="6A1B9A")
            c.alignment = Alignment(horizontal="center")
        for i, w in enumerate([11, 22, 10, 10, 10, 12, 12, 14], 1):
            ws3.column_dimensions[get_column_letter(i)].width = w
    else:
        ws3 = wb["信號統計總覽"]

    # [V22.14b] Sheet 3 寫入邏輯：從SQLite predictions匯總
    # 清空舊資料區（保留標題列1-3）
    if ws3.max_row > 3:
        for r in range(ws3.max_row, 3, -1):
            ws3.delete_rows(r)
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            row3 = 4
            for code in TARGET_ETFS.keys():
                name = TARGET_ETFS[code]["name"][:20]
                rows = conn.execute(
                    "SELECT action, predicted_ret, actual_ret FROM predictions WHERE code=?",
                    (code,)).fetchall()
                if not rows: continue
                n_attack = sum(1 for a, p, ac in rows if "主攻" in (a or ""))
                n_silent = sum(1 for a, p, ac in rows if "靜默" in (a or ""))
                n_reduce = sum(1 for a, p, ac in rows if "減倉" in (a or ""))
                # 命中：減倉時實際下跌 / 主攻時實際上漲 / 靜默時實際小波動
                n_hit = 0
                n_miss = 0
                for a, p, ac in rows:
                    if ac is None: continue
                    if "減倉" in (a or "") and ac < -0.003: n_hit += 1
                    elif "主攻" in (a or "") and ac > 0.003: n_hit += 1
                    elif "靜默" in (a or "") and abs(ac) <= 0.01: n_hit += 1
                    elif "減倉" in (a or "") and ac > 0.01: n_miss += 1
                    # [V22.22.7 修#19] 補主攻miss判定: 主攻call卻下跌>1%算落空
                    #   根因: 舊版主攻只有hit沒有miss, 跌了也不扣分 → credibility虛高
                    #   (與減倉miss門檻0.01對稱)
                    elif "主攻" in (a or "") and ac < -0.01: n_miss += 1
                    elif "靜默" in (a or "") and ac > 0.02: n_miss += 1
                total = n_attack + n_silent + n_reduce
                credibility = (n_hit / max(1, n_hit+n_miss)) * 100 if (n_hit+n_miss)>0 else 0
                ws3.cell(row=row3, column=1, value=code)
                ws3.cell(row=row3, column=2, value=name)
                ws3.cell(row=row3, column=3, value=n_attack)
                ws3.cell(row=row3, column=4, value=n_silent)
                ws3.cell(row=row3, column=5, value=n_reduce)
                ws3.cell(row=row3, column=6, value=n_hit)
                ws3.cell(row=row3, column=7, value=n_miss)
                ws3.cell(row=row3, column=8, value=round(credibility, 1))
                for col in range(1, 9):
                    ws3.cell(row=row3, column=col).font = Font(name="Arial", size=10)
                    ws3.cell(row=row3, column=col).alignment = Alignment(horizontal="center")
                row3 += 1
    except Exception as e:
        print(f"   ⚠️ Sheet 3 信號統計寫入失敗：{e}")

    # ═══════════════════════════════════════════════════════════
    # Sheet 4: 精度演進時序 [V22.22.8 改: 從DB全量重建 + 分報前瞻方向精度]
    #   根因(舊版2大問題):
    #     (1) append寫死 column2="收盤報" → 永遠只有收盤報(事後評分,循環論證)
    #     (2) append到xlsx, 歷史只存在檔案裡 → xlsx被換/重置就消失
    #   新版: 每次從DB全量重建(像信號統計那樣), DB是真相來源, 歷史不再消失;
    #         按 report_type 分組 → 早/午/收各自的真實前瞻方向精度;
    #         加「純方向精度%」(排除靜默, |pred|>=0.5% 才算方向呼叫) → 看真預測力
    if "精度演進" in wb.sheetnames:
        del wb["精度演進"]
    ws4 = wb.create_sheet("精度演進")
    ws4["A1"] = "📊 系統精度演進（DB全量重建·分報前瞻方向精度）"
    ws4["A1"].font = Font(name="Arial", size=14, bold=True)
    ws4.merge_cells("A1:G1")
    h4 = ["日期", "報告類型", "樣本數", "方向呼叫數", "方向命中",
          "方向精度%", "純方向精度%"]
    for col, h in enumerate(h4, 1):
        c = ws4.cell(row=3, column=col, value=h)
        c.font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
        c.fill = PatternFill("solid", start_color="C62828")
        c.alignment = Alignment(horizontal="center")
    for i, w in enumerate([12, 10, 9, 11, 10, 11, 12], 1):
        ws4.column_dimensions[get_column_letter(i)].width = w
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            ev_rows = conn.execute("""
                SELECT date(ts) AS d,
                       COALESCE(NULLIF(report_type,''),'收盤報') AS rt,
                       predicted_ret, actual_ret
                FROM predictions
                WHERE actual_ret IS NOT NULL AND predicted_ret IS NOT NULL
            """).fetchall()
        groups = {}
        for d, rt, p, a in ev_rows:
            groups.setdefault((d, rt), []).append((p, a))
        rt_order = {"盤前報": 0, "早報": 1, "午報": 2, "收盤報": 3}
        ordered = sorted(groups.items(),
                         key=lambda kv: (kv[0][0], rt_order.get(kv[0][1], 9)))
        row4 = 4
        for (d, rt), rows in ordered:
            n = len(rows)
            # 整體方向精度: 同號 或 雙方都接近0(靜默且實際也小動)
            hit = sum(1 for p, a in rows
                      if (p or 0) * (a or 0) > 0
                      or (abs(p or 0) < 0.005 and abs(a or 0) < 0.005))
            # 純方向: 只看有明確方向呼叫(|pred|>=0.5%), 排除靜默 → 真預測力
            dir_calls = [(p, a) for p, a in rows if abs(p or 0) >= 0.005]
            dir_hit = sum(1 for p, a in dir_calls if (p or 0) * (a or 0) > 0)
            acc = f"{hit}/{n}={hit*100//n}%" if n else "—"
            dacc = (f"{dir_hit}/{len(dir_calls)}={dir_hit*100//len(dir_calls)}%"
                    if dir_calls else "無方向呼叫")
            for col, v in enumerate([d, rt, n, len(dir_calls), dir_hit, acc, dacc], 1):
                ws4.cell(row=row4, column=col, value=v)
                ws4.cell(row=row4, column=col).font = Font(name="Arial", size=10)
                ws4.cell(row=row4, column=col).alignment = Alignment(horizontal="center")
            row4 += 1
    except Exception as _e:
        _log_err(_e, "unknown")
        print(f"   ⚠️ 精度演進重建失敗：{_e}")

    # Sheet 5: P&L 歸因彙總（每月）
    if "PnL歸因" not in wb.sheetnames:
        ws5 = wb.create_sheet("PnL歸因")
        ws5["A1"] = "💰 V22.14 P&L 歸因彙總（每月）"
        ws5["A1"].font = Font(name="Arial", size=14, bold=True)
        ws5.merge_cells("A1:F1")
        h5 = ["月份", "避開損失(元)", "錯失收益(元)",
              "主攻收益(元)", "淨貢獻(元)", "信號筆數"]
        for col, h in enumerate(h5, 1):
            c = ws5.cell(row=3, column=col, value=h)
            c.font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
            c.fill = PatternFill("solid", start_color="0277BD")
            c.alignment = Alignment(horizontal="center")
        for i, w in enumerate([10, 16, 16, 16, 14, 12], 1):
            ws5.column_dimensions[get_column_letter(i)].width = w
    else:
        ws5 = wb["PnL歸因"]

    # [V22.14b] Sheet 5 寫入邏輯：按月分組信號統計（金額估算需要持倉金額）
    if ws5.max_row > 3:
        for r in range(ws5.max_row, 3, -1):
            ws5.delete_rows(r)
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            # 按月聚合
            rows = conn.execute("""
                SELECT strftime('%Y-%m', ts) as ym, code, action, predicted_ret, actual_ret
                FROM predictions
                WHERE actual_ret IS NOT NULL
                ORDER BY ym ASC
            """).fetchall()
            month_stats = {}
            for ym, code, a, p, ac in rows:
                if ym not in month_stats:
                    month_stats[ym] = {"avoid":0.0, "miss":0.0, "attack":0.0, "n":0}
                month_stats[ym]["n"] += 1
                # [V22.17.1] 用真實持倉金額（取代 pos_amt=1000 硬編碼）
                # 未持倉的 ETF 用預設 1000（粗略估算）
                pos_amt = USER_HOLDINGS.get(code, 1000.0)
                if "減倉" in (a or "") and ac < 0:
                    # 避開的損失（按真實持倉金額計）
                    month_stats[ym]["avoid"] += abs(ac) * pos_amt
                elif ("靜默" in (a or "")) and ac > 0.01:
                    # 錯失的收益
                    month_stats[ym]["miss"] += ac * pos_amt
                elif "主攻" in (a or "") and ac > 0:
                    # 命中主攻收益
                    month_stats[ym]["attack"] += ac * pos_amt

            row5 = 4
            for ym, stat in sorted(month_stats.items()):
                net = stat["avoid"] + stat["attack"] - stat["miss"]
                ws5.cell(row=row5, column=1, value=ym)
                ws5.cell(row=row5, column=2, value=round(stat["avoid"], 0))
                ws5.cell(row=row5, column=3, value=round(stat["miss"], 0))
                ws5.cell(row=row5, column=4, value=round(stat["attack"], 0))
                ws5.cell(row=row5, column=5, value=round(net, 0))
                ws5.cell(row=row5, column=6, value=stat["n"])
                for col in range(1, 7):
                    ws5.cell(row=row5, column=col).font = Font(name="Arial", size=10)
                    ws5.cell(row=row5, column=col).alignment = Alignment(horizontal="center")
                row5 += 1
            if row5 == 4:
                # 還沒任何 actual_ret 數據
                ws5.cell(row=4, column=1, value="待累積")
                ws5.cell(row=4, column=2, value="—")
                ws5.cell(row=4, column=6, value="0")
                ws5.cell(row=4, column=1).font = Font(name="Arial", size=10, italic=True, color="888888")
    except Exception as e:
        print(f"   ⚠️ Sheet 5 PnL歸因寫入失敗：{e}")

    # Sheet 6: 跨報180°反轉記錄
    if "跨報反轉" not in wb.sheetnames:
        ws6 = wb.create_sheet("跨報反轉")
        ws6["A1"] = "⚡ V22.14 跨報180°反轉警示記錄"
        ws6["A1"].font = Font(name="Arial", size=14, bold=True)
        ws6.merge_cells("A1:F1")
        h6 = ["日期", "ETF代碼", "名稱", "早報信號", "收盤信號", "結果"]
        for col, h in enumerate(h6, 1):
            c = ws6.cell(row=3, column=col, value=h)
            c.font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
            c.fill = PatternFill("solid", start_color="E65100")
            c.alignment = Alignment(horizontal="center")
        for i, w in enumerate([12, 11, 22, 18, 18, 12], 1):
            ws6.column_dimensions[get_column_letter(i)].width = w
    # 檢查當日是否有反轉並寫入
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            for r in results:
                code = r.get("code", "")
                if not code:
                    continue
                rows_d = conn.execute(
                    "SELECT action FROM predictions WHERE code=? AND date(ts)=? ORDER BY ts ASC",
                    (code, today_str)).fetchall()
                if len(rows_d) >= 2:
                    first_act = rows_d[0][0]
                    last_act = r.get("action", "")
                    is_180 = (("主攻" in first_act and "減倉" in last_act) or
                              ("減倉" in first_act and "主攻" in last_act))
                    if is_180:
                        ws6 = wb["跨報反轉"]
                        # 檢查是否已記錄
                        already = False
                        for rx in range(4, ws6.max_row + 1):
                            if (str(ws6.cell(row=rx, column=1).value) == today_str and
                                str(ws6.cell(row=rx, column=2).value) == code):
                                already = True
                                break
                        if not already:
                            rx = ws6.max_row + 1
                            ws6.cell(row=rx, column=1, value=today_str)
                            ws6.cell(row=rx, column=2, value=code)
                            ws6.cell(row=rx, column=3, value=r.get("name", "")[:20])
                            ws6.cell(row=rx, column=4, value=first_act[:15])
                            ws6.cell(row=rx, column=5, value=last_act[:15])
                            ws6.cell(row=rx, column=6, value="待覆盤")
    except Exception as e:
        print(f"   ⚠️ Sheet 6 反轉記錄寫入失敗：{e}")

    try:
        wb.save(tracker_path)
        print(f"   ✅ 追蹤表已更新（6 Sheet）：{tracker_path}")
    except PermissionError:
        print(f"   ⚠️ 追蹤表儲存失敗（檔案可能被 Excel 開啟，請關閉後重跑收盤報）")
    except Exception as e:
        print(f"   ⚠️ 追蹤表儲存錯誤：{e}")



# ══════════════════════════════════════════════════════════════════════
# 🎖️ main() — 九龍合璧主流程
# ══════════════════════════════════════════════════════════════════════
def _run_self_audit():
    """
    [V22.17.3 新增] 一鍵代碼自檢
    
    執行 5 大類審計，輸出每項通過/警告/失敗
    用法：python V22_17_ETF_System.py --audit
    """
    import re as _re
    src = open(__file__, encoding='utf-8').read()
    print("=" * 70)
    print(f"📋 {__VERSION__} 代碼自檢（5 大類審計）")
    print("=" * 70)
    
    # 1. 語法檢查
    print("\n[1/5] 語法檢查")
    try:
        import ast as _ast
        _ast.parse(src)
        print("  ✅ 語法正確")
    except SyntaxError as e:
        print(f"  ❌ L{e.lineno}: {e.msg}")
    
    # 2. 一致性審計（USER_HOLDINGS / Style cap）
    print("\n[2/5] 一致性審計")
    # [V22.18] PositionBook 自動持倉引擎狀態
    for _m in _PB_MSGS:
        print(f"  📒 {_m}")
    if _PB_DIFF_WARNINGS:
        print(f"  ⚠️ PositionBook 對拍 LEGACY 有 {len(_PB_DIFF_WARNINGS)} 處不符：")
        for _w in _PB_DIFF_WARNINGS:
            print(_w)
    else:
        print(f"  ✅ PositionBook 對拍 LEGACY 完全一致（並存驗證通過）")
    holdings_missing = [c for c in USER_HOLDINGS if c not in TARGET_ETFS]
    if holdings_missing:
        print(f"  ⚠️ USER_HOLDINGS 有 code 不在 TARGET_ETFS: {holdings_missing}")
    else:
        print(f"  ✅ USER_HOLDINGS 全部 {len(USER_HOLDINGS)} 檔在 TARGET_ETFS")
    
    all_styles = set()
    for d in (TARGET_ETFS, WATCHLIST_ETFS):
        for info in d.values():
            all_styles.add(info.get("style", "?"))
    missing = all_styles - set(STYLE_CAP_PCT.keys())
    if missing:
        print(f"  ⚠️ Style 沒設定上限: {missing}")
    else:
        print(f"  ✅ 所有 Style ({len(all_styles)} 個) 都有 STYLE_CAP_PCT")
    
    # 3. KeyError 風險掃描
    print("\n[3/5] KeyError 風險（r['xxx'] 硬取）")
    risky = _re.findall(r'r\["(action|code|name|style|ev|predicted_ret)"\]', src)
    if risky:
        print(f"  ⚠️ 找到 {len(risky)} 處硬取 key")
    else:
        print(f"  ✅ 0 處硬取 key")
    
    # 4. except+pass 黑洞
    print("\n[4/5] except+pass 吞錯誤")
    pass_count = 0
    lines = src.split('\n')
    for i, line in enumerate(lines):
        if line.strip().startswith('except') and i+1 < len(lines):
            if lines[i+1].strip() == 'pass':
                pass_count += 1
    print(f"  ℹ️ 找到 {pass_count} 處 except+pass")
    if pass_count <= 3:
        print(f"  ✅ 僅 sqlite3.OperationalError 預期失敗")
    else:
        print(f"  ⚠️ 應檢查是否需改 _log_err")
    
    # 5. 真實計算測試
    print("\n[5/5] 真實計算邏輯測試")
    try:
        # compute_macro_prior None 防禦
        r = TrendRegimeDetector.compute_macro_prior(None, None, None, None, "Tech")
        print(f"  ✅ compute_macro_prior None 防禦: {r}")
        # compute_order 真實場景
        styles_pct = {}
        for c, amt in USER_HOLDINGS.items():
            i = TARGET_ETFS.get(c, {})
            if not i: continue
            isq = _is_qdii_etf(i)
            sk = "QDII" if isq else i.get("style", "?")
            styles_pct[sk] = styles_pct.get(sk, 0) + _pct_of_capital(amt)
        o, d, w = compute_order("008888", TARGET_ETFS["008888"], "🔴 減倉", 22, 0, styles_pct)
        print(f"  ✅ compute_order 008888 減倉(EV=22): {o[:50]}")
    except Exception as e:
        print(f"  ❌ 測試失敗: {e}")
    
    # 總結
    print("\n" + "=" * 70)
    print("📊 持倉摘要")
    print("=" * 70)
    print(f"   TOTAL_CAPITAL: {TOTAL_CAPITAL:.0f} 元")
    for sty, pct in sorted(styles_pct.items()):
        cap = STYLE_CAP_PCT.get(sty, 25)
        flag = "⚠️ 超標" if pct > cap else "✅"
        print(f"   {sty:<12} {pct:>5.1f}% (上限 {cap}%) {flag}")
    print("=" * 70)






def _rs_style_groups(rs_ev_map, targets):
    """同 Style 內依 RS 分數排名。預掃描登記為不可用(中性 50 分)的標的不參與排名:
    否則全部取不到價格時,報告仍會印出依字典順序排出的「#1/11」,看似有依據其實沒有。"""
    groups = {}
    for code, ev_score in rs_ev_map.items():
        if data_availability.is_unavailable(f"RS_RANK:{code}"):
            continue
        groups.setdefault(targets[code]["style"], []).append((code, ev_score))
    for s in groups:
        groups[s].sort(key=lambda x: x[1], reverse=True)
    return groups


def main():
    import argparse
    parser = argparse.ArgumentParser(description=f"{__VERSION__} 九龍合璧")

    def _iso_date(s):
        try:
            datetime.datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            raise argparse.ArgumentTypeError(f"日期格式須為 YYYY-MM-DD,收到 {s!r}")
        return s

    parser.add_argument("--date", type=_iso_date, help="以指定日期執行,格式 YYYY-MM-DD")
    parser.add_argument("--force-type",
                        choices=["盤前報","早報","午報","收盤報","夜間快報"])
    parser.add_argument("--set-position",
                        help="設定持倉成本,格式 代碼,成本[,入倉日],例如 --set-position 004070,1.234")
    parser.add_argument("--remove-position",
                        help="移除持倉成本記錄，例如 --remove-position 004070")
    parser.add_argument("--list-positions", action="store_true",
                        help="列出所有持倉")
    # [V22.17.3 新增] 詳細錯誤輸出 + 一鍵審計模式
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="顯示詳細錯誤訊息（debug 模式）")
    parser.add_argument("--audit", action="store_true",
                        help="一鍵跑完整代碼審計（不執行任何報告）")
    args, _unknown = parser.parse_known_args()
    if _unknown:   # 拼錯的參數原本被靜默忽略,改成照預設時段跑報告卻不告知
        print(f"⚠️ 無法識別的參數已忽略:{' '.join(_unknown)}(用 --help 查看可用參數)")
    reset_news_audit()   # [V22.74] 每輪歸零新聞審計計數(在任何 compute_news_score 之前)
    data_availability.reset()   # [雲端任務A] 每次執行從乾淨的可用性登記表開始
    _PRICE_SKIPPED.clear(); _WATCH_SKIPPED.clear()
    _register_history_status(_history_present(LOG_DB), BASE_DIR)

    # 啟用 verbose 模式
    if args.verbose:
        global VERBOSE_ERRORS
        VERBOSE_ERRORS = True
        print("🔍 Verbose 模式已啟用（詳細錯誤輸出）")

    # [V22.17.3] --audit 模式：一鍵代碼自檢
    if args.audit:
        _run_self_audit()
        return

    if args.set_position:
        parts = args.set_position.split(",")
        try:
            float(parts[1])
        except (IndexError, ValueError):
            parser.error(f"--set-position 格式須為 代碼,成本[,入倉日],收到 {args.set_position!r}")
        if len(parts) >= 2:
            PositionBook.load()
            PositionBook.set_position(parts[0].strip(), float(parts[1]),
                                      parts[2].strip() if len(parts)>2 else None)
            print(f"✅ 持倉設置：{parts[0]} @ {parts[1]}")
        return

    # [V22.12新增] 移除持倉命令
    if args.remove_position:
        PositionBook.load()
        code = args.remove_position.strip()
        if PositionBook.remove_position(code):
            print(f"✅ 持倉已移除：{code}")
        else:
            print(f"⚠️ {code} 不在持倉表中（可能未曾設置）")
        return

    # [V22.12新增] 列出所有持倉
    if args.list_positions:
        PositionBook.load()
        if PositionBook._data:
            print("📋 當前持倉：")
            for c, info in PositionBook._data.items():
                print(f"   {c}: 成本={info.get('cost','?')} | "
                      f"入倉日={info.get('date_in','?')} | {info.get('note','')}")
        else:
            print("⚠️ 持倉表為空")
        return

    now_h = datetime.datetime.now().hour
    # [V22.17.0 修 5/28] 確保 DB schema 最新（_init_db 之前從沒被呼叫！）
    # 根因：predictions 表是舊版建立，缺 report_type/macro_prior 欄位
    # ALTER TABLE IF NOT EXISTS 冪等，每次跑都安全
    try:
        _init_db()
        # [V22.27.7 FixB] 首次運行自動將CSV持倉遷入DB(冪等:有資料則跳過)
        # [V24.27] ★原傳 BASE_DIR(=...\ETF_Monitor 上一層),但期初快照在 ...\代碼,
        #   而 build_positionbook 用的是 _PB_BASE_DIR(=代碼) → 兩者找不同資料夾。
        #   使用者 2026-08-10 實測訊息「搜尋目錄: ...\ETF_Monitor」即為證據。
        _mig_msg = _migrate_positions_to_db(_PB_BASE_DIR)
        if "無 positions_opening" in (_mig_msg or ""):
            _mig_msg = _migrate_positions_to_db(BASE_DIR)   # 回退再試上一層
        if _mig_msg:
            print(f"   {_mig_msg}")
    except Exception as e:
        print(f"   ⚠️ _init_db 失敗: {e}")

    # [V22.27] 條件beta引擎:為有β_US的標的實算滾動條件beta(Engle Cov/Var,date隔夜對齊)寫入快取;
    #          任何抓取/計算失敗 → get_beta_us 自動 fallback 固定 BETA_US(零回歸)。
    try:
        prime_conditional_betas(list(BETA_US.keys()))
        try:
            _IC_CACHE["A股"] = compute_information_coefficient("A股")
            _IC_CACHE["台股"] = compute_information_coefficient("台股")
            _TRADABLE_IC_CACHE["A股"] = compute_tradable_ic("A股")
            _TRADABLE_IC_CACHE["台股"] = compute_tradable_ic("台股")
        except Exception as _ice:
            print(f"   ⚠️ IC prime 失敗(不影響報告): {_ice}")
    except Exception as e:
        print(f"   ⚠️ 條件beta prime 失敗→全用固定BETA_US: {e}")

    if args.date:
        run_date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
        r_type = "收盤報"; is_backfill = True
    else:
        run_date = datetime.date.today(); is_backfill = False

    IS_LATE_RERUN = False
    r_type_db = None
    if args.force_type:
        r_type = args.force_type
        # [V22.62] ★時間窗外補跑偵測 —— 治「補跑會製造前視偏誤」
        #   早報的定位是【09:xx 對當日的事前預測】。若收盤後補跑,系統抓的是收盤後資料
        #   → 產出一份標著「早報」的「事後諸葛」→ 落庫後四報準確率虛高。
        #   這正是 #211/#212 修掉的 95%→55% 前視偏誤同一個病,不能再犯。
        #   修法:比對【實際執行時間】與【該報正常時窗】,超出即:
        #     ① 印顯著橫幅 ② DB 的 report_type 加「(補跑)」後綴
        #        → 不被 `report_type IN ('早報','午報')` 的統計/回測抓到,不污染樣本。
        _WIN = {"盤前報": (6, 9), "早報": (9, 12), "午報": (12, 15),
                "收盤報": (16, 19), "夜間快報": (22, 24)}
        _lo, _hi = _WIN.get(r_type, (0, 24))
        _nh = _now_utc8().hour
        IS_LATE_RERUN = not (_lo <= _nh < _hi)
        if IS_LATE_RERUN:
            r_type_db = f"{r_type}(補跑)"
            print(f"   ⚠️ [補跑偵測] 現在 {_nh:02d} 時,超出「{r_type}」正常時窗 {_lo:02d}-{_hi:02d} 時")
            print(f"      → 本次用【當下最新資料】,非該時段即時資料")
            print(f"      → DB 標記「{r_type_db}」,**不計入四報準確率、不進回測樣本**")
    elif not is_backfill:
        if   6    <= now_h < 9:                      r_type = "盤前報"
        elif now_h == 9 and datetime.datetime.now().minute < 30:
                                                     r_type = "盤前報"  # [V22.16 修] 9:00-9:30仍盤前
        elif 9    <= now_h < 12:                     r_type = "早報"    # 9:30-12:00
        elif 12   <= now_h < 15:                     r_type = "午報"    # 12:00-15:00
        elif 15   <= now_h < 16:
            # [V22.14b] 15:00-16:00 是NAV公布等待期，A股場外NAV尚未更新
            # 此時跑收盤報→P&L歸因和Kalman學習用到零值
            print("⚠️ 15:00-16:00 為A股NAV等待期，不自動執行收盤報")
            print("   如需強制執行請用 --force-type 收盤報")
            return
        elif 16   <= now_h <= 18:                    r_type = "收盤報"  # 16:00-18:00
        elif now_h >= 22:                            r_type = "夜間快報"
        else:                                        r_type = "收盤報"

    print(f"\n[{__VERSION__}] {r_type} 啟動 | {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")

    # [V22.33.5·治使用者原則①「產業鏈新聞四報都要有,是公式一部分有占比」]
    #   ★根因(源碼坐實):原 _news_items / _news_score_map 【從未被 populate】→ compute_news_score([])
    #     恆回 0 → 產業新聞因子四報全都是 0(公式在、資料源沒接)。這裡在【r_type 分支前】統一載入+算分,
    #     盤前/早/午/收四報共用同一因子。資料源 news.csv(同目錄, WB/用戶策展)。缺檔→[]→分數0(零回歸)。
    try:
        _news_items = load_news_csv()
        _news_map = build_news_score_map(_news_items)                  # {code:(score,reason)} 四報共用
        _news_score_map = {_c: _v[0] for _c, _v in _news_map.items()}  # 供收盤歸因 attribute_today(L9117)
    except Exception as _e:
        _log_err(_e, "news_map_init")
        _news_items, _news_map, _news_score_map = [], {}, {}

    # [V22.17.5 新增] 非交易日判斷（除非用戶 --force-type 強制執行）
    if not args.force_type and not is_backfill:
        today_date = datetime.date.today()
        # 美股相關報告（盤前/夜間快報）跑就好（美股周五晚 23:00 仍可能有夜盤）
        # 但早報、午報、收盤報 = A 股交易時段，周末/節假日無數據意義
        if r_type in ("早報", "午報", "收盤報"):
            if not is_trading_day(today_date, market="A股"):
                reason = get_non_trading_reason(today_date)
                print(f"⚠️ {today_date.isoformat()} 為 A 股非交易日（{reason}）")
                print(f"   {r_type} 通常用於即時交易時段，今日 A 股無交易，自動跳過")
                print(f"   如需測試請用 --force-type {r_type}")
                _send_telegram(f"🌴 {today_date.isoformat()} 非交易日（{reason}），跳過 {r_type}", silent=True)
                return
        # 盤前報在週末早晨可選擇執行（顯示美股周五收盤值供前瞻分析）
        # 但會在報告中明確標註「今日為非交易日」
        if r_type == "盤前報" and not is_trading_day(today_date, market="A股"):
            print(f"ℹ️ {today_date.isoformat()} 非交易日，盤前報以「下個交易日前瞻」模式運行")

    # ── 夜間快報捷徑 ──────────────────────────────────
    if r_type == "夜間快報":
        print("[策略師] 夜間快報...")
        vix  = DirectDataFetcher.get_vix()
        ndx  = DirectDataFetcher.get_ndx()
        sox  = DirectDataFetcher.get_sox()
        gold, wti, _ = IntelTeam.get_gold_wti_yahoo()  # [V22.11] Yahoo直連
        usd_cny_nb, _ = IntelTeam.get_usd_cny()        # [V22.11] 活匯率
        # [V22.25.5fix P0] None 防禦
        _ndx_safe = ndx if ndx is not None else 0.0
        _vix_safe = vix if vix is not None else 18.0
        brief = OutlookTeam.build_night_brief(_ndx_safe, sox or 0.0, _vix_safe, gold, wti, usd_cny_nb)
        _send_telegram(f"🌙 夜間快報 | NDX={_ndx_safe*100:+.2f}% VIX={_vix_safe:.1f}", silent=True)
        print(f"✅ {r_type} 完成！")
        return

    # ══ [盤前報] V22.17.0 升級流程 ══════════════════════
    # V22.17.0 升級內容:
    #   1. 加「🌏 宏觀先驗預判」區塊（5 個 Style EV 預期影響）
    #   2. 加「👁️ 觀察池前瞻」區塊（5 檔 RSI 變化趨勢）
    #   3. 加「📋 昨日減倉信號回顧」區塊
    #   4. HTML 雙輸出（之前忽略，現補上）
    if r_type == "盤前報":
        print("[情報官] 盤前報專屬流程 V22.17.5（戰略定調 + 5 大指標 + 3 大關注 + 交易日防呆）...")
        vix  = DirectDataFetcher.get_vix()
        ndx  = DirectDataFetcher.get_ndx()
        sox  = DirectDataFetcher.get_sox()
        gold, wti, gold_src = IntelTeam.get_gold_wti_yahoo()
        usd_cny, usd_cny_src = IntelTeam.get_usd_cny()
        gold_ret_pm = IntelTeam.get_gold_ret()

        # [V22.25.7 P0] Path A 統整 None 正規化 + 盲區顯性化
        _intel_na = []
        if vix is None:         _intel_na.append("VIX");  vix = 18.0
        if ndx is None:         _intel_na.append("NDX");  ndx = 0.0
        if sox is None:         _intel_na.append("SOX");  sox = 0.0
        if gold_ret_pm is None: _intel_na.append("黃金"); gold_ret_pm = 0.0
        # [V22.25.7-fix P0] _intel_warning 必須在使用點(macro_pred_block 注入)之前定義
        _intel_warning = (" ⚠️" + "/".join(_intel_na) + "數據不可用，以中性預設替代") if _intel_na else ""
        for _na in _intel_na:   # [雲端任務A] 同步進可用性登記表,報告頂部統一列出
            _data_unavailable({"黃金": "GOLD_RET"}.get(_na, _na), f"{_na} 抓取失敗,以中性預設替代")
        _tw_warn = check_tracker_freshness()  # [V22.25.10] 追蹤表時效警報
        _pa_warn = check_position_anchor_freshness()  # [V22.33.9] 持倉錨點時效(治D10)
        if _pa_warn: print(f"   [持倉錨點] {_pa_warn}"); _intel_warning += " " + _pa_warn  # [V22.34] 治WB缺陷A:必須進報告本體
        # [V22.46 治 WB F-01] check_proxy_collisions 在 V22.45 只有 def【沒有呼叫點】=死碼,
        #   等於「加了偵測器卻沒掛」(缺陷A 家族第四次)。此處掛進報告本體,與 _pa_warn 同範式。
        # ⚠️ 變數名必須是實際存在的 TARGET_ETFS —— 我第一版誤寫 POSITIONS_INFO(不存在),
        #    那會永遠傳 {} → 告警永不觸發 = 換一種形式的死碼。交付前自查抓到。
        for _pc in check_proxy_collisions(TARGET_ETFS):
            print(f"   [代理共用] {_pc}"); _intel_warning += " " + _pc
        # [V22.48] Yahoo 快取回退警示 → 必須進報告本體(否則又是「算了不顯示」缺陷A)
        _f5_out = flow5_outage_days()
        if _f5_out >= 3:
            _f5w = (f"🔴 **資金流資料已中斷 {_f5_out} 個交易日**(東財端點失敗)→ "
                    f"①進場條件④無法檢核 ②動態上限第②條恆不通過(Tech 鎖在硬上限) "
                    f"③脆弱度輸入殘缺 ④資金背離失效(減倉少一條放行路徑)。"
                    f"**這不是『沒有資金流』,是『不知道』**")
            print(f"   [資金流中斷] {_f5w}"); _intel_warning += " " + _f5w
        # [V22.65] ★板塊輪動接進報告本體 —— 不是算了不顯示(缺陷A家族教訓)
        try:
            _secs = fetch_sector_flow()
            _sec_block = format_sector_rotation(_secs, codes=list(TARGET_ETFS.keys()))
            # [V22.67] 落庫供擁擠度分位累積 + 產出擁擠度區塊
            _sn = save_sector_snapshot(_secs)
            if _sn:
                print(f"   [擁擠度] 板塊快照已落庫 {_sn} 筆(分位需 {CROWD_MIN_DAYS} 日樣本)")
            _sec_block += format_crowdedness(_secs, codes=list(TARGET_ETFS.keys()))
            # [V22.68] 公告掃描接進報告(WB 已驗證來源)
            try:
                _anns = fetch_announcements(100)
                _ahit = scan_announcements_for_holdings(_anns, set(TARGET_ETFS.keys()))
                _sec_block += format_announcements(_ahit, len(_anns))
                if _ahit:
                    _amsg = ("📢 **公告情報**:" + "、".join(
                        f"{k}({v[0][0]}{'利好' if v[0][3]>0 else '風險'})" for k, v in list(_ahit.items())[:3]))
                    print(f"   [公告] {_amsg}"); _intel_warning += " " + _amsg
            except Exception as _ae:
                _log_err(_ae, "announce_scan")
            if _secs:
                _in0, _out0 = _secs[0], _secs[-1]
                _rot = (f"🔄 **板塊輪動**:資金流入 **{_in0['name']}**({_in0['net_yi']:+.1f}億)、"
                        f"流出 **{_out0['name']}**({_out0['net_yi']:+.1f}億)")
                print(f"   [板塊輪動] {_rot}"); _intel_warning += " " + _rot
        except Exception as _se:
            _log_err(_se, "sector_rotation")
            _sec_block = ""
        _yh_st = yahoo_staleness()
        if _yh_st:
            _yw = ("🔴 **Yahoo 資料為快取回退**(代理不可用/抓取失敗):"
                   + "、".join(f"{k}({v})" for k, v in list(_yh_st.items())[:6])
                   + " → VIX/NDX/SOX 與條件β **非即時值**,macro_prior 可能失真,請勿據此判斷隔夜方向")
            print(f"   [Yahoo回退] {_yw}"); _intel_warning += " " + _yw
        if _tw_warn: _intel_warning += " " + _tw_warn

        try:
            wti_hist = IntelTeam.get_5day_history("CL=F")
            wti_ret = (wti_hist[-1] - wti_hist[-2]) / wti_hist[-2] if len(wti_hist) >= 2 else 0
            if len(wti_hist) < 2:   # [雲端任務A] 條件式靜默給 0,與「今日持平」無法區分
                _data_unavailable("WTI_RET_PM", f"WTI 5日歷史僅 {len(wti_hist)} 筆(抓取失敗/無資料),以 0 替代")
        except Exception as _e:
            _log_err(_e, "wti_ret")
            # [雲端任務A] 0 在「WTI日漲跌率」語境下與「今日持平」無法區分,
            # 會直接餵進 derive_c_drain(地緣溫度),登記不可用供查詢。
            _data_unavailable("WTI_RET_PM", f"wti_ret(盤前) 計算失敗,改用0 ({type(_e).__name__}: {_e})")
            wti_ret = 0

        # [V22.25.9] 鮮數據覆寫(盤前報):US休市時段Yahoo回stale值,用WebSearch鮮數據取代
        _ov, _ov_meta = load_overnight_override()
        if _ov:
            _ov_vals = {"vix": vix, "ndx": ndx, "sox": sox, "gold": gold,
                        "wti": wti, "wti_ret": wti_ret, "gold_ret": gold_ret_pm}
            _ov_prov = apply_overnight_override(_ov_vals, _ov, _ov_meta)  # [V22.33.8] 傳meta(原死碼)
            vix, ndx, sox = _ov_vals["vix"], _ov_vals["ndx"], _ov_vals["sox"]
            gold, wti, wti_ret, gold_ret_pm = (_ov_vals["gold"], _ov_vals["wti"],
                                               _ov_vals["wti_ret"], _ov_vals["gold_ret"])
            if _ov_prov:
                _intel_warning += f" 🔄鮮數據覆寫({_ov_meta.get('source','')}): " + " / ".join(_ov_prov)
                print(f"   [鮮數據覆寫·盤前] {' / '.join(_ov_prov)}")

        # [V22.17.4 新增] 抓 5 日歷史用於 3 日趨勢分析
        sox_hist = IntelTeam.get_5day_history("%5ESOX")
        ndx_hist = IntelTeam.get_5day_history("%5ENDX")
        vix_hist = IntelTeam.get_5day_history("%5EVIX")
        gold_hist = IntelTeam.get_5day_history("GC=F")
        sox_trend = analyze_3day_trend(sox_hist)
        ndx_trend = analyze_3day_trend(ndx_hist)
        # [V22.35 治D2顯示層] SOX/NDX 軌跡改用【與當前錨同一份釘選價格序列】→ 軌跡末位==當前錨。
        #   原本 get_5day_history 未釘選,與當前錨各抓各的,錨凍結期(7/15-7/17)兩者可不一致。
        #   ★形狀不變:仍餵給 analyze_3day_trend,下游 ['pattern'] / _fmt_3d(returns_3d) 契約不動 = 零回歸。
        #   取不到(無網/序列不足)則保留上面舊路徑結果。
        try:
            for _sym, _var in (("%5ESOX", "sox"), ("%5ENDX", "ndx")):
                _pin = fetch_dated_us_series(_sym, days=12)
                if _pin:
                    _exp = _expected_latest_us_date(_now_utc8().strftime("%Y-%m-%d"))
                    _px = [c for (d, c) in _pin if d <= _exp]       # 同一把釘選尺
                    if len(_px) >= 4:
                        if _var == "sox":
                            sox_trend = analyze_3day_trend(_px)
                        else:
                            ndx_trend = analyze_3day_trend(_px)
        except Exception as _e:
            _log_err(_e, "pinned_trajectory_wire")
        vix_trend = analyze_3day_trend(vix_hist)
        gold_trend = analyze_3day_trend(gold_hist)

        # [V22.17.4 新增] CNY 雙軌制分析
        cny_20ma = IntelTeam.get_cny_20ma()
        cny_yesterday = None
        try:
            cny_hist = IntelTeam.get_5day_history("CNY=X")
            if len(cny_hist) >= 2:
                cny_yesterday = cny_hist[-2]
        except Exception as _e:
            _log_err(_e, "cny_yesterday")
        cny_info = analyze_cny_dual_track(usd_cny, cny_yesterday, cny_20ma)

        # [V22.17.4 新增] 黃金/原油蹺蹺板劇本判定
        gold_oil_info = analyze_gold_oil_seesaw(gold_ret_pm, wti_ret, vix)

        # [V22.17.4 新增] Tech 集中度兩段式檢查
        _current_styles_pct = {}
        for _c, _amt in USER_HOLDINGS.items():
            _i = TARGET_ETFS.get(_c, {})
            if not _i: continue
            _is_q = _is_qdii_etf(_i)
            _sk = "QDII" if _is_q else _i.get("style", "?")
            _current_styles_pct[_sk] = _current_styles_pct.get(_sk, 0) + _pct_of_capital(_amt)
        tech_info = analyze_tech_concentration(_current_styles_pct)

        # [V22.17.4 新增] 讀取 events.csv
        events = load_events_csv()

        # 地緣狀態讀取
        _brain_pm = {}
        if os.path.exists(DB_FILE):
            try:
                with open(DB_FILE, encoding='utf-8') as f:
                    _brain_pm = json.load(f)
            except Exception as _e:
                _log_err(_e, "unknown")
                # [雲端任務A] 同主流程 brain.json:損毀時靜默以空 dict 繼續,
                # Kalman權重/海峽狀態回到預設值且無任何可見訊息。
                _data_unavailable("BRAIN_JSON_PM", f"brain.json(盤前) 讀取/解析失敗,macro/kalman"
                                  f"已靜默回退預設值 ({type(_e).__name__}: {_e})")
        _macro_pm   = _brain_pm.get("macro", {})
        _strait_pm  = _macro_pm.get("strait_status", "conditional_open")
        # [V22.25.7] 盤前報 c_drain 自動推導，取代 brain.json 死值
        _manual_cdrain_pm = _macro_pm.get("c_drain")
        try:
            _cdrain_pm, _cdrain_pm_reason = derive_c_drain(vix, wti, wti_ret, gold_ret_pm, manual_override=_manual_cdrain_pm, override_as_of=_macro_pm.get("c_drain_as_of"))
            print(f"   [盤前地緣溫度] c_drain={_cdrain_pm:.2f} <- {_cdrain_pm_reason}")
            _cd_na = [k for k in ("VIX", "WTI_RET_PM", "GOLD_RET") if data_availability.is_unavailable(k)]
            if _cd_na and "人工加碼至" not in _cdrain_pm_reason:   # [雲端任務A] 有效人工值不受輸入影響
                _data_unavailable("C_DRAIN_INPUTS", f"c_drain={_cdrain_pm:.2f} 由預設輸入推導(不可用:{'/'.join(_cd_na)})")
        except Exception as _e:
            _log_err(_e, "derive_c_drain_pm")
            # [雲端任務A] 同主流程 derive_c_drain:盤前報版本一樣會靜默代入預設值。
            _data_unavailable("C_DRAIN_PM", f"derive_c_drain(盤前) 計算失敗,改用預設0.20/人工值 "
                              f"({type(_e).__name__}: {_e})")
            _cdrain_pm = _manual_cdrain_pm if _manual_cdrain_pm is not None else 0.20
        _w_tw_pm    = _brain_pm.get("kalman", {}).get("台股", {}).get("W", 0.481)
        _w_a_pm     = _brain_pm.get("kalman", {}).get("A股",  {}).get("W", 0.531)
        _geo_icon   = "🔴" if _strait_pm == "closed_hostile" else _geo_icon_from_cdrain(_cdrain_pm)

        now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

        # [V22.17.0 既有] 計算 5 個 Style 的 macro_prior 預判
        macro_pred_block = "\n## 🌏 宏觀先驗預判（今日對 EV 的預期影響）\n"
        macro_pred_block += "| Style | macro_prior | EV 預期影響 | 解讀 |\n|-------|-------------|------------|------|\n"
        sty_targets = [
            ("Tech (008888/011613/018177)", "Tech", False),
            ("QDII (006479/018967)",         "Tech", True),
            ("Defensive (000216 黃金)",      "Defensive", False),
            ("Cyclical (004432 有色)",       "Cyclical", False),
            ("Dividend (007466 紅利)",       "Dividend", False),
        ]
        for label, sty, qdii in sty_targets:
            mp = TrendRegimeDetector.compute_macro_prior(
                sox_ret=sox, ndx_ret=ndx, vix_val=vix,
                cny_val=usd_cny, style=sty,
                gold_ret=gold_ret_pm, is_qdii=qdii,
                tech_coupling=compute_tech_coupling() if (sty == "Tech" and not qdii) else None,  # [V22.27.9]
            )
            ev_delta = int(round(50 * (1 + mp * 0.20))) - 50
            interp = ("📈 利多支撐" if mp > 0.15 else
                      "📉 利空壓制" if mp < -0.15 else "🟡 中性")
            macro_pred_block += f"| {label} | {mp:+.2f} | {ev_delta:+d} 分 | {interp} |\n"
        macro_pred_block += f"\n> 算式：`ev = ev × (1 + macro_prior × 0.20)`\n"
        # [V22.25.5fix P0] None 安全格式化
        _sox_str = f"{sox:+.2%}" if sox is not None else "⚠️N/A"
        _ndx_str = f"{ndx:+.2%}" if ndx is not None else "⚠️N/A"
        _vix_str = f"{vix:.2f}"  if vix is not None else "⚠️N/A"
        # [V22.33.7] ★錨 as-of 可見化:7/15-7/17 錨凍結三天沒被發現,正因報告只印值、不印【值代表哪天】
        try:
            _a_rec = get_pinned_anchor("%5ESOX")
            _a_exp = _expected_latest_us_date(_now_utc8().strftime("%Y-%m-%d"))
            _a_asof = _a_rec.get("as_of")
            _a_flag = "✅" if _a_asof == _a_exp else f"🔴錨過期/凍結(應為{_a_exp})"
            _anchor_line = f"> 🕐 隔夜錨 as-of：{_a_asof or '未取得'} {_a_flag} | 來源:{_a_rec.get('source','?')}\n\n"
        except Exception as _e:
            _log_err(_e, "anchor_asof_line"); _anchor_line = ""
        macro_pred_block += f"> 輸入：SOX={_sox_str} / NDX={_ndx_str} / VIX={_vix_str} / 黃金日漲={(gold_ret_pm*100 if gold_ret_pm is not None else 0):+.2f}% / CNY={usd_cny:.4f}\n\n" + _anchor_line
        if _intel_warning:
            macro_pred_block += f"\n> {_intel_warning}\n"

        # [V22.17.0 既有 → V22.24.1 修復+升級] 觀察池前瞻
        #   🐛修(WorkBuddy揪出): ev鍵名錯(ev→ev_est)害EV永遠顯示0;
        #     scan_watchlist回傳鍵是ev_est(L3410), 盤前報卻取w.get('ev')→取不到→0。
        #     上一輪我誤判'EV=0是盤前不算EV的設計', 實為bug。
        #   升(吸收Gemini呈現): priority_watch🎯排最前 + 閉環對照列。
        watchlist_pm_block = "\n## 👁️ 觀察池前瞻（今日進場可能 + 🎯重點觀察閉環對照）\n"
        try:
            wl_results = WatchlistScanner.scan_watchlist()
            _wn, _wm = save_watchlist_snapshot(wl_results)   # [V22.55] 落庫,建立趨勢序列
            print(f"   [觀察池] {_wm}")
            if wl_results:
                # 🎯重點觀察排最前
                wl_results = sorted(
                    wl_results, key=lambda r: (0 if r.get("priority_watch") else 1))
                watchlist_pm_block += "| 代碼 | 主題 | RSI | Regime | EV | 進場狀態 |\n"
                watchlist_pm_block += "|------|------|-----|--------|-----|----------|\n"
                for w in wl_results:
                    _star = "🎯" if w.get("priority_watch") else ""
                    watchlist_pm_block += (
                        f"| {_star}{w.get('code','?')} | {w.get('theme','?')[:12]} | "
                        f"{w.get('rsi',0):.1f} | {w.get('regime','?')} | "
                        f"{w.get('ev_est',0)} | {w.get('entry_signal','?')[:20]} |\n"
                    )
                watchlist_pm_block += "\n"
                # 🎯重點觀察閉環對照(Claude web初判 vs 系統實算)
                for w in wl_results:
                    if w.get("priority_watch"):
                        watchlist_pm_block += (
                            f"- 🎯 **{w.get('code')} {w.get('name','')}**："
                            f"{_pw_qualitative_display(w.get('priority_watch'), w.get('added_date'))}{_pw_freshness_tag(w.get('added_date'))}\n"
                            f"  - 🔄 閉環：系統實算 RSI={w.get('rsi',0):.1f}/"
                            f"EV={w.get('ev_est',0)}/{w.get('entry_signal','?')} "
                            f"← 對比Claude web初判\n")
                        _ddm = w.get("dd") or {}
                        if _ddm:   # [V24.3] 盤前報路徑也印盡調(原本只有早報那條有)
                            watchlist_pm_block += "  - 🔍 **盡調(基金經理人視角)**：\n"
                            for _k, _v in _ddm.items():
                                watchlist_pm_block += f"      · {_k}：{_v}\n"
                watchlist_pm_block += "\n"
            else:
                watchlist_pm_block += "_觀察池暫無資料_\n\n"
        except Exception as e:
            watchlist_pm_block += f"_觀察池前瞻採集失敗: {e}_\n\n"

        # [V22.17.0 新增] 昨日減倉信號回顧（DB 查最近一日所有減倉）
        reduce_recall_block = ""
        try:
            yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
            with sqlite3.connect(LOG_DB, timeout=10) as conn:
                # [修 5/28] 兼容 V22.16 舊資料 report_type 為空字串
                # 優先抓收盤報，沒有則抓任何當日記錄
                rows = conn.execute("""
                    SELECT code, name, action, ROUND(predicted_ret*100,2)
                    FROM predictions
                    WHERE date(ts)=? AND action LIKE '%減倉%'
                      AND (report_type='收盤報' OR report_type='' OR report_type IS NULL)
                    ORDER BY ev ASC
                """, (yesterday,)).fetchall()
                # 去重：同一個 code 只保留 1 筆（按 ts DESC 取最新）
                seen = set()
                unique_rows = []
                for r in rows:
                    if r[0] not in seen:
                        seen.add(r[0])
                        unique_rows.append(r)
                if unique_rows:
                    # [V22.23 L2因果鏈] 顯性後驗: β_US × 隔夜美股 → 判昨日結論被推翻多少
                    _beta_tag = "實測" if BETA_US_CALIBRATED else "估計·未校準"
                    reduce_recall_block = (
                        f"\n## 📋 昨日({yesterday})減倉信號 → 今晨後驗（β_US {_beta_tag}）\n"
                        "| 代碼 | 名稱 | 昨日信號 | 昨日預測 | β_US | 隔夜推力 | 後驗判定 |\n"
                        "|------|------|---------|---------|------|---------|---------|\n"
                    )
                    _sox_pp = (sox or 0) * 100   # 隔夜SOX(百分點)
                    _ndx_pp = (ndx or 0) * 100   # 隔夜NDX(百分點)
                    for code_r, name_r, action_r, pred_r in unique_rows:
                        beta = get_beta_us(code_r)  # [V22.27] 條件beta優先,fallback固定
                        is_qdii = _is_qdii_etf(TARGET_ETFS.get(code_r, {}))
                        drv_pp = _ndx_pp if is_qdii else _sox_pp   # QDII看NDX其餘看SOX
                        drv_lbl = "NDX" if is_qdii else "SOX"
                        push_pp = beta * drv_pp                     # 後驗推力(百分點)
                        prior_abs = abs(pred_r) if pred_r else 0.0
                        ratio = (push_pp / prior_abs) if prior_abs > 0 else 0.0
                        # 昨日減倉=偏空; 正推力=對抗該偏空
                        if push_pp > 0 and ratio >= POSTERIOR_FLIP_RATIO:
                            verdict = "🔄 前提翻轉(昨空→今或偏多)"
                        elif push_pp > 0 and ratio >= POSTERIOR_LOOSEN_RATIO:
                            verdict = "🟡 鬆動(待早報確認)"
                        elif push_pp < 0:
                            verdict = "➡️ 維持(隔夜同向加強)"
                        else:
                            verdict = "➡️ 維持(隔夜影響小)"
                        reduce_recall_block += (
                            f"| {code_r} | {name_r[:8]} | {action_r[:8]} | "
                            f"{pred_r:+.2f}% | {beta:+.2f} | "
                            f"{drv_lbl}{drv_pp:+.1f}%×β={push_pp:+.2f}pp | {verdict} |\n"
                        )
                    reduce_recall_block += (
                        f"\n> 後驗推力 = β_US × 隔夜美股。≥1.0倍且反向→翻轉；"
                        f"0.3~1.0倍→鬆動(待早報確認)；<0.3→維持。β為{_beta_tag}，"
                        f"跑一週後用真實 actual_ret 回歸校準。\n\n"
                    )
        except Exception as _e:
            _log_err(_e, "unknown")

        # [V22.17.4] 計算 3 大關注（自適應引擎）
        macro_data = {"vix": vix, "cny": cny_info, "gold_oil": gold_oil_info,
                      "sox_trend": sox_trend, "ndx_trend": ndx_trend,
                      "sox": sox, "ndx": ndx}  # [V22.22.5] 隔夜美股當日漲跌(供暴跌警報)
        holdings_data = {"tech_concentration": tech_info, "styles_pct": _current_styles_pct}
        similarity_data = {"conflicts": []}  # 盤前無 ETF 個股信號，留空
        focuses = generate_today_focus(macro_data, holdings_data, events, similarity_data)

        # [V22.17.4] 組裝「今日 3 大關注」區塊
        focus_block = "## 🎯 今日核心關注（自適應引擎）\n\n"
        for i, f in enumerate(focuses, 1):
            focus_block += f"**{i}. {f['icon']} {f['title']}**\n   {f['text']}\n\n"

        # [V24.5 Q4] 異常狀態檢測(雙向:好/壞都警示;自適應隱藏)
        anomaly_lines = []
        # 黃金: 單日|漲跌|>1.5% 即為波動異常;上漲=避險需求升溫,下跌=獲利了結/美元壓制
        if gold_ret_pm is not None and abs(gold_ret_pm) > 0.015:
            _gd = "上漲" if gold_ret_pm > 0 else "下跌"
            if gold_ret_pm > 0:
                anomaly_lines.append(
                    f"* **000216 黃金**：單日{_gd} {gold_ret_pm*100:+.2f}%，突破 1.5% 波動閾值 "
                    f"→ 🔴 避險需求升溫(風險事件驅動)｜{gold_oil_info['scenario']}")
            else:
                anomaly_lines.append(
                    f"* **000216 黃金**：單日{_gd} {gold_ret_pm*100:+.2f}%，突破 1.5% 波動閾值 "
                    f"→ 🟡 貴金屬獲利了結/美元走強壓制(中性偏空)")
        # VIX: >25 恐慌(壞) / 20-25 警戒 / <14 平靜(好)
        if vix is not None:
            if vix > 25:
                anomaly_lines.append(f"* **VIX**：{vix:.2f} > 25 → 🔴 恐慌區(系統性殺估值,避險/降杠杆優先)")
            elif vix > 20:
                anomaly_lines.append(f"* **VIX**：{vix:.2f} 處 20-25 → 🟡 警戒(波動偏高,謹慎)")
            elif vix < 14:
                anomaly_lines.append(f"* **VIX**：{vix:.2f} < 14 → 🟢 平靜(風險偏好佳,有利風險資產)")
        # SOX: >4% 強勢(好:科技多頭溢價) / <-4% 重挫(壞:費半崩跌)
        if sox is not None and abs(sox) > 0.04:
            if sox > 0.04:
                anomaly_lines.append(
                    f"* **SOX**：+{sox*100:.2f}% 強勢(>4%) → 🟢 美股科技多頭溢價,A股科技可望跟漲"
                    f"（防誘多:V22.17.1 對SOX加成自動打3折）")
            else:
                anomaly_lines.append(
                    f"* **SOX**：{sox*100:+.2f}% 重挫(<-4%) → 🔴 費半崩跌,A股科技承壓(風險優先)")
        if cny_info["level"] != "🟢 健康":
            anomaly_lines.append(f"* **CNY**：{cny_info['level']} - " + " / ".join(cny_info["reason"]))

        anomaly_block = ""
        # [V22.25.7-fix] 原此處的 _intel_warning 晚定義已移至 5943（使用點之前），刪除冗餘
        if anomaly_lines:
            anomaly_block = (
                "## 🚨 異常狀態檢測（雙向：好/壞皆警示）\n\n"
                "> 說明：下列指標突破常態波動閾值即列入。🔴=偏空風險 / 🟡=中性警示 / "
                "🟢=偏多利好。單日漲跌以「突破 1.5%」為黃金波動線,其餘以歷史分位/絕對幅度判定。\n\n"
                + "\n".join(anomaly_lines) + "\n\n")
        else:
            anomaly_block = (
                "## 🚨 異常狀態檢測（雙向：好/壞皆警示）\n\n"
                "> 本報暫無指標突破異常閾值（探測器正常運作,非未開機）。"
                "VIX/SOX/CNY/黃金均在常態區間,無須特別警示。\n\n")

        # [V22.17.4] 隔夜定價中樞表格化 + 3 日軌跡
        def _fmt_3d(trend):
            if not trend.get("returns_3d"):
                return "資料不足"
            r3 = trend["returns_3d"]
            traj = " → ".join(f"{x*100:+.2f}%" for x in r3)
            return f"{traj} (cum {trend['cum_3d']*100:+.2f}%, stdev {trend['stdev']*100:.2f}%)"
        
        pricing_block = "## 🌏 隔夜定價中樞 (3 日軌跡 + 系統判定)\n\n"
        pricing_block += "| 指標 | 當前 | 3 日軌跡 | 系統判定 |\n|------|------|---------|----------|\n"
        vix_judge = "🔴 恐慌" if vix > 25 else "🟡 偏高" if vix > 20 else "🟢 安全"
        pricing_block += f"| **VIX** | {vix:.2f} | {_fmt_3d(vix_trend)} | {vix_judge} ({vix_trend['pattern']}) |\n"
        pricing_block += f"| **SOX** | {sox*100:+.2f}% | {_fmt_3d(sox_trend)} | {sox_trend['pattern']} |\n"
        pricing_block += f"| **NDX** | {ndx*100:+.2f}% | {_fmt_3d(ndx_trend)} | {ndx_trend['pattern']} |\n"
        cny_traj = f"{usd_cny:.4f}"
        if cny_info.get("vs_yesterday") is not None:
            cny_traj += f" (日內 {cny_info['vs_yesterday']:+.2f}%)"
        if cny_info.get("vs_20ma") is not None:
            cny_traj += f" / 20MA {cny_info['vs_20ma']:+.2f}%"
        pricing_block += f"| **CNY** | {usd_cny:.4f} | {cny_traj} | {cny_info['level']} |\n"
        gold_judge = f"{gold_oil_info['scenario']}"
        pricing_block += f"| **黃金** | ${gold:.0f} ({(gold_ret_pm*100 if gold_ret_pm is not None else 0):+.2f}%) | {_fmt_3d(gold_trend)} | {gold_judge} |\n"
        pricing_block += f"| **WTI** | ${wti:.2f} ({wti_ret*100:+.2f}%) | - | {'🔴 高油壓' if wti > 100 else '🟡 中性'} |\n\n"

        # [V22.17.4] 黃金/原油蹺蹺板劇本影響
        seesaw_block = ""
        if gold_oil_info["impact"]:
            seesaw_block = "## ⚖️ 黃金/原油蹺蹺板劇本\n\n"
            seesaw_block += f"**{gold_oil_info['scenario']}**\n\n"
            seesaw_block += "* 黃金日漲 {:+.2f}% / 原油日漲 {:+.2f}% / VIX {:.2f}\n\n".format(
                (gold_ret_pm*100 if gold_ret_pm is not None else 0), wti_ret*100, vix)
            seesaw_block += "**對 EV 預期影響:**\n"
            for sty, impact in gold_oil_info["impact"].items():
                if isinstance(impact, str):
                    seesaw_block += f"* **{sty}**：{impact}\n"
                else:
                    seesaw_block += f"* **{sty}**：{impact:+d}% EV 調整\n"
            seesaw_block += "\n"

        # [V22.17.4] 持倉戰局摘要（用 USER_COSTS + classify_position_status）
        # [V22.18] 加5日資金流向(全持倉)
        _flow_cache2 = {}
        for code in USER_COSTS:
            info = TARGET_ETFS.get(code, {})
            _proxy = info.get("proxy")
            if _proxy and code not in _flow_cache2:
                try:
                    _flow_cache2[code] = _flow5_with_manual(code, _proxy, DirectDataFetcher.get_etf_5day_flow(_proxy))
                except Exception:
                    _flow_cache2[code] = None
        portfolio_block = f"## 💰 持倉戰局摘要（{len(USER_COSTS)} 檔浮盈狀態+5日資金）\n\n"
        groups = {}  # {(priority, label): [lines]}
        for code, cost in USER_COSTS.items():
            info = TARGET_ETFS.get(code, {})
            name = info.get("name", code)[:10]
            pct = cost["pnl_pct"]
            _f = _flow_cache2.get(code)
            flow_tag = f" 💰5日{_f.get('flow5_yi', 0):+.1f}億{(_f.get('signal','') or '')[:2]}" if _f else ""
            line = f"* `{code}` {name} {pct:+.1f}% ({cost['total_pnl']:+.0f} 元 / 現值 {cost['current']:.0f}){flow_tag}"
            grp_key = classify_position_status(pct)
            groups.setdefault(grp_key, []).append(line)
        for grp_key in sorted(groups.keys()):
            portfolio_block += f"\n### {grp_key[1]}\n" + "\n".join(groups[grp_key]) + "\n"
        # Tech 集中度
        portfolio_block += (f"\n### 📊 板塊集中度（實際持倉）\n"
                            f"* 純Tech風格: {tech_info['tech_pct']:.1f}% {tech_info['level']} - {tech_info['action']}\n"
                            f"* 納指QDII: {tech_info['qdii_pct']:.1f}%（美股曝險,與A股Tech分開計;6/24已驗A股Tech↗與美股Tech↘可脫鉤）\n\n")

        # [V22.27.1] 條件β脫鉤監測顯示塊(讓引擎firing/fallback每跑可見)
        portfolio_block += render_conditional_beta_block()

        # [V22.17.4] 同質群組獨立區塊
        sim_block = "## 👥 同質群組聯動（同質性 ≥ 85%）\n\n"
        for grp_name, codes in SIMILARITY_GROUPS.items():
            in_holding = [c for c in codes if c in USER_HOLDINGS]
            if not in_holding:
                continue
            details = []
            for c in in_holding:
                amt = USER_HOLDINGS.get(c, 0)
                pnl = USER_COSTS.get(c, {}).get("pnl_pct", 0)
                details.append(f"`{c}` ({amt:.0f}元 / {pnl:+.1f}%)")
            sim_block += f"* **{grp_name}群**: {' / '.join(details)}\n"
        sim_block += "\n> 💡 同質群組應同步操作，避免重複曝險\n\n"

        # [V22.17.4] 重大事件日曆（events.csv 驅動，自適應隱藏）
        events_block = ""
        if events:
            events_block = "## 📅 重大事件日曆 (events.csv 驅動)\n\n"
            for evt in events[:5]:  # 最多 5 個
                sev = evt.get("severity", "")
                sev_icon = {"critical": "🔴", "high": "🟡", "medium": "🟢", "low": "⚪"}.get(sev, "")
                events_block += (
                    f"* **{evt.get('date','')} {evt.get('time','')}** | "
                    f"{evt.get('event','?')} | 影響: `{evt.get('impact','?')}` | "
                    f"{sev_icon} {sev}\n"
                )
            events_block += "\n"
        else:
            events_block = "## 📅 重大事件日曆\n\n> 📂 events.csv 未維護或無未來事件 - 系統建議手動新增重要日程\n\n"

        # [V22.17.4 既有] 昨日減倉信號回顧（重用）
        # reduce_recall_block 已在前面組裝

        # [V22.17.5 新增] 非交易日警示橫幅
        non_trading_banner = ""
        _today_d = datetime.date.today()
        if not is_trading_day(_today_d, market="A股"):
            _reason = get_non_trading_reason(_today_d)
            _next_d = _today_d + datetime.timedelta(days=1)
            while not is_trading_day(_next_d, market="A股"):
                _next_d += datetime.timedelta(days=1)
            non_trading_banner = (
                f"## ⚠️ 非交易日提示\n\n"
                f"* **今日**: {_today_d.isoformat()} ({_reason}) → A 股不開盤\n"
                f"* **下個交易日**: {_next_d.isoformat()}\n"
                f"* **本盤前報數據**: 美股最後交易日（凍結值），用於前瞻下個交易日\n\n"
            )
        
        # [V22.17.4] 完整盤前報組裝
        # [V22.33.6 F1活接線] 同夜間:008888 脫鉤則SOX不引用,不拿美股硬推A股芯片
        _s888pm = effective_sox_for(get_beta_us("008888"), sox)
        _s888pm_txt = (f"SOX{_s888pm*100:+.1f}%，A股芯片偏{'多' if _s888pm>0 else '空'}" if _s888pm is not None
                       else "條件β脫鉤·SOX不引用（β≈-0.10）")
        # [V22.69 四報因果鏈接通] ★盤前報讀取昨日收盤報寫入的 T+1 前提。
        #   load_morning_premise 自 V22.29 定義後【零呼叫】(本窗 grep 證實)→ 收盤報寫的
        #   前提隔天沒人讀 → 因果鏈斷。此處補上呼叫點,並把 _prem_block 插入 pm_report。
        #   取「最新一筆」而非硬算前一交易日:更穩(自動吃假日/系統漏跑),來源日顯式供辨識過期。
        _prem_block = ""
        try:
            with sqlite3.connect(LOG_DB, timeout=10) as _pmconn:
                _pmconn.execute("CREATE TABLE IF NOT EXISTS morning_premise("
                                "date TEXT PRIMARY KEY, payload TEXT, ts TEXT)")
                _prow = _pmconn.execute(
                    "SELECT date FROM morning_premise ORDER BY date DESC LIMIT 1").fetchone()
                _pdate = _prow[0] if _prow else None
                _prem = load_morning_premise(_pmconn, _pdate) if _pdate else None
            _prem_block = format_morning_premise(_prem, _pdate)
            # 今日盤勢預測:昨收前提 × 隔夜美股 × 產業新聞(原為永不執行的死碼,本版接線)
            if T1_PREDICT_ENABLED:
                _prem_block += build_premarket_forecast(
                    _prem, _pdate, ndx, sox, vix,
                    news_items=locals().get("_news_items"), unavailable=tuple(_intel_na))
        except Exception as _e:
            _log_err(_e, "prem_block")
            _prem_block = ""
        # [V22.80 補交棒缺口] ★昨日收盤『今日情報綜合』交棒(分板塊方向/最強預測/宏觀)。
        #   收盤不只傳逐檔 t1_pred,還傳市場級綜合 → 盤前起手情境,非從零重推。
        _handoff_block = ""
        try:
            with sqlite3.connect(LOG_DB, timeout=10) as _hoconn:
                _ho, _hodate = load_closing_handoff(_hoconn)
            # [V22.85] 盤前現場用今晨 _news_map 算市場級新聞(快照凍結0時救回,WB 定位 + Claude 補強)
            _lnv = [v[0] for v in (_news_map or {}).values()
                    if isinstance(v, (list, tuple)) and v and v[0]]
            _live = {"avg": round(sum(_lnv) / len(_lnv), 3), "n": len(_lnv)} if _lnv else None
            _handoff_block = format_closing_handoff(_ho, _hodate, live_news=_live)
        except Exception as _e:
            _log_err(_e, "handoff_block")
            _handoff_block = ""
        pm_report = (
            f"# ⚡ 盤前戰略定調 | {__VERSION__} | {now_str}\n\n"
            + non_trading_banner
            + focus_block
            + anomaly_block
            + pricing_block
            + seesaw_block
            + macro_pred_block
            # [V22.69 四報因果鏈] ★昨日收盤前提插入報告本體 —— 這正是上一窗崩潰當下
            #   「_prem_block 算了但沒插進報告」的那一步(與 _sec_block 同源的㉗反模式)。
            + (locals().get("_prem_block") or "")
            + (locals().get("_handoff_block") or "")   # [V22.80] 昨日收盤情報交棒→今日起手情境
            # [V22.66 治WB抓到的㉗反模式] ★_sec_block(板塊輪動完整表)在 V22.65 被算出來卻【從未插入報告】,
            #   grep 全檔只有「賦值」與「異常清空」兩處,產物靜默丟棄 → 用戶只看得到一行摘要。
            #   這與 #228(欄位不存在)、#232(delta歸零)同源:【算出來了但沒交到用戶手上】。
            + (locals().get("_sec_block") or "")
            + portfolio_block
            + sim_block
            + events_block
            + watchlist_pm_block
            + reduce_recall_block
            + f"## 🌍 地緣政治狀態\n"
            f"* 地緣/能源複合風險：{_geo_icon} | c_drain={_cdrain_pm:.2f}（VIX+油價+金價滾動合成,非單一事件）\n\n"
            f"## 🎯 今日QDII開盤方向預判\n"
            f"* 006479/018967 (纳指100)：預計 {(ndx*0.92*100):+.2f}% → "
            f"{'📈偏多' if ndx>0.003 else '📉偏空' if ndx<-0.003 else '🔄中性'}\n"
            f"* 008888 (芯片)：{_s888pm_txt}\n"
            # [V22.24.1 吸收Gemini行動化解讀] 把方向翻譯成「該怎麼做」的紀律, 不只報方向
            + (f"\n> 🛡️ **今日紀律**：SOX隔夜暴漲{sox*100:+.1f}%(>4%)已觸發誘多過濾(Tech先驗打3折)。"
               f"「偏多」是方向傾向**非買入指令**——A股科技若高開極易誘多後殺跌，"
               f"**禁止開盤重倉追高**，等早報完整EV+資金流確認。\n\n"
               if sox > 0.04 else
               "\n> 💡 **今日紀律**：方向預判是傾向參考，非買入指令；具體進場等早報09:30完整EV+Kelly定規。\n\n")
            + f"## ⏰ 經理人今日行動節奏\n"
            f"```\n"
            f"09:00  台股開盤 → 群益/復華方向確認\n"
            f"09:30  A股開盤  → 滬深成交額+主力淨流入 首15分鐘信號\n"
            f"09:30  早報執行 → 完整EV+信號+Kelly建議 (含風控)\n"
            f"13:00  午報     → 開盤2H表現+北向更新+修正指令\n"
            f"15:30  收盤報   → Kalman學習+P&L歸因+明日前瞻\n"
            f"23:00  夜間快報 → 美股收盤快照+QDII次日預判\n"
            f"```\n\n"
            f"> ⚠️ 盤前報為戰略定調，**不產生EV、不產生交易指令**。\n"
            f"> ✅ 早報(09:30+)將有完整 {len(USER_COSTS)} 檔 ETF 信號 + Kelly 定規 + 風控降級。\n"
        )
        ts_str = datetime.datetime.now().strftime('%m%d_%H%M')
        fn_pm = os.path.join(REPORT_DIR, f"{__VERSION__}_盤前報_{ts_str}.txt")
        pm_report = pm_report + news_audit_report_line()   # [V22.74] 新聞掃描覆蓋率提示
        pm_report = data_availability.insert_unavailable_block(pm_report)   # [雲端任務A]
        with open(fn_pm, 'w', encoding='utf-8') as f:
            f.write(pm_report)
        print(f"✅ 盤前報完成：{fn_pm}")

        # [V22.17.0 新增] 盤前報 HTML 雙輸出
        try:
            fn_pm_html = os.path.join(REPORT_DIR, f"{__VERSION__}_盤前報_{ts_str}.html")
            html_pm = markdown_to_html(
                pm_report,
                title=f"{__VERSION__} 盤前報",
                subtitle=f"{now_str} | {__VERSION__} | 含宏觀先驗預判"
            )
            with open(fn_pm_html, 'w', encoding='utf-8') as f:
                f.write(html_pm)
            print(f"   HTML：{fn_pm_html}")
        except Exception as e:
            print(f"   ⚠️ 盤前報 HTML 失敗（不影響 txt）: {e}")

        _send_telegram(f"📰 盤前報 | VIX={vix:.1f} NDX={ndx*100:+.1f}% 金=${gold:.0f}", silent=True)
        return
    PositionBook.load()

    # ── [Perf] T+1補寫（早報前）─────────────────────
    if r_type == "早報":
        PerformanceAttributor.backfill_yesterday_actuals()

    # ── [Intel] 情報採集 ─────────────────────────────
    print("\n[情報官] 採集宏觀數據（多源驗證）...")
    vix, ndx_d, sox_d = IntelTeam.get_vix_ndx_sox()

    # [V22.25.5fix P0] None 防禦 — 區分「無漲跌」和「數據不可用」
    _intel_na = []  # 記錄哪些指標不可用
    if vix is None:   _intel_na.append("VIX");   vix   = 18.0   # 中性預設(下游需數字)
    if ndx_d is None: _intel_na.append("NDX");   ndx_d = 0.0
    if sox_d is None: _intel_na.append("SOX");   sox_d = 0.0
    _intel_warning = (" ⚠️" + "/".join(_intel_na) + "數據不可用,用中性預設") if _intel_na else ""
    for _na in _intel_na:   # [雲端任務A] 同步進可用性登記表,報告頂部統一列出
        _data_unavailable(_na, f"{_na} 抓取失敗,以中性預設替代")
    _tw_warn = check_tracker_freshness()  # [V22.25.10] 追蹤表時效警報
    _pa_warn = check_position_anchor_freshness()  # [V22.33.9] 持倉錨點時效(治D10)
    if _pa_warn: print(f"   [持倉錨點] {_pa_warn}"); _intel_warning += " " + _pa_warn  # [V22.34] 治WB缺陷A:必須進報告本體
    if _tw_warn: _intel_warning += " " + _tw_warn
    _vix_display  = f"{vix:.2f}"
    _ndx_display  = f"{ndx_d:+.2%}"
    _sox_display  = f"{sox_d:+.2%}"

    # [V22.9] USD/CNY 活匯率（不再硬編碼）
    usd_cny, usd_cny_src = IntelTeam.get_usd_cny()

    # [V22.9] 黃金/WTI Yahoo優先
    gold, wti, gold_src = IntelTeam.get_gold_wti_yahoo()
    # [V22.16 5/27 完整版] 取黃金日漲跌率（給 macro_prior 用）
    gold_ret = IntelTeam.get_gold_ret()

    # [V22.25.7 P0] Path B gold_ret None 正規化（與 Path A 同源 bug）
    if gold_ret is None:
        _intel_na.append("黃金")
        # [雲端任務A] 此處已在 _intel_warning 組裝之後,原警示文字不含黃金;改由登記表列出
        _data_unavailable("GOLD_RET", "黃金日漲跌抓取失敗,以 0% 替代")
        gold_ret = 0.0
    # [V22.25.5] WTI漲跌率(主流程補算, 給c_drain自動推導用; 原只盤前分支算)
    try:
        _wti_hist = IntelTeam.get_5day_history("CL=F")
        wti_ret = ((_wti_hist[-1] - _wti_hist[-2]) / _wti_hist[-2]
                   if _wti_hist and len(_wti_hist) >= 2 and _wti_hist[-2] else 0.0)
        if not (_wti_hist and len(_wti_hist) >= 2 and _wti_hist[-2]):   # [雲端任務A] 同盤前報
            _data_unavailable("WTI_RET", f"WTI 5日歷史僅 {len(_wti_hist or [])} 筆(抓取失敗/無資料),以 0.0 替代")
    except Exception as _e:
        _log_err(_e, "wti_ret_main")
        # [雲端任務A] 同盤前報版本:0.0 會被 derive_c_drain 當成「今日WTI持平」往下算。
        _data_unavailable("WTI_RET", f"wti_ret 計算失敗,改用0.0 ({type(_e).__name__}: {_e})")
        wti_ret = 0.0

    # [V22.25.9] 鮮數據覆寫(早/午/收報):同盤前,US休市時段用WebSearch鮮數據取代stale Yahoo
    _ov, _ov_meta = load_overnight_override()
    if _ov:
        _ov_vals = {"vix": vix, "ndx": ndx_d, "sox": sox_d, "gold": gold,
                    "wti": wti, "wti_ret": wti_ret, "gold_ret": gold_ret}
        _ov_prov = apply_overnight_override(_ov_vals, _ov, _ov_meta)  # [V22.33.8] 傳meta(原死碼)
        vix, ndx_d, sox_d = _ov_vals["vix"], _ov_vals["ndx"], _ov_vals["sox"]
        gold, wti, wti_ret, gold_ret = (_ov_vals["gold"], _ov_vals["wti"],
                                        _ov_vals["wti_ret"], _ov_vals["gold_ret"])
        _vix_display, _ndx_display, _sox_display = f"{vix:.2f}", f"{ndx_d:+.2%}", f"{sox_d:+.2%}"
        if _ov_prov:
            _intel_warning += f" 🔄鮮數據覆寫({_ov_meta.get('source','')}): " + " / ".join(_ov_prov)
            print(f"   [鮮數據覆寫·主流程] {' / '.join(_ov_prov)}")

    # [V22.14] 市場資金面三合一（替代北向資金）
    market_sentiment = IntelTeam.get_market_sentiment_v14()
    north_flow_str = "實時已停止披露(2024-08-19監管)"

    print(f"   VIX={_vix_display} | NDX={_ndx_display} | SOX={_sox_display}")
    print(f"   金=${gold:.0f}{gold_src} | 油=${wti:.2f} | CNY={usd_cny:.4f}{usd_cny_src}")
    print(f"   市場資金面：{market_sentiment.get('summary','—')}")

    # ── brain.json讀取 ───────────────────────────────
    strait_status = "conditional_open"
    w_tw = 0.481; w_a = 0.531; trade_ease = 0
    _manual_cdrain = None
    # brain.json 缺失/損毀時 macro 原本從未定義 → 下方 macro.get("c_drain_as_of") 拋
    # UnboundLocalError → c_drain 一律落 0.20,自動推導從未執行。預設空 dict 讓推導照常跑。
    macro = {}
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, encoding='utf-8') as f:
                brain = json.load(f)
            macro = brain.get("macro", {})
            strait_status = macro.get("strait_status", "conditional_open")
            _manual_cdrain = macro.get("c_drain")  # 降級為人工加碼上限(突發事件用), 非主來源
            w_tw = brain.get("kalman",{}).get("台股",{}).get("W", 0.481)
            w_a  = brain.get("kalman",{}).get("A股", {}).get("W", 0.531)
            trade_ease = int(macro.get("trade_ease_signal", 0))
        except Exception as _bre:
            # [雲端任務A] brain.json 存在但讀取/解析失敗 → 靜默回退到與「從未校準過」
            # 完全相同的預設值(海峽狀態/Kalman權重/貿易順暢度全部重置),訊號層看不出
            # 這是「資料損毀」還是「本來就沒資料」的差別。登記不可用供查詢。
            _data_unavailable("BRAIN_JSON",
                              f"brain.json 讀取/解析失敗,strait_status/kalman權重/trade_ease"
                              f"已靜默重置為預設值 ({type(_bre).__name__}: {_bre})")
            brain = {"macro":{}, "kalman":{}}
    else:
        brain = {"macro":{}, "kalman":{}}

    # [V22.25.5] c_drain自動推導: 從已抓的VIX/WTI/金價算地緣溫度, 取代手填
    _cdrain_reason = ""
    try:
        c_drain, _cdrain_reason = derive_c_drain(vix, wti, wti_ret, gold_ret,
                                                 manual_override=_manual_cdrain, override_as_of=macro.get("c_drain_as_of"))
        print(f"   [地緣溫度] c_drain={c_drain:.2f} ← {_cdrain_reason}")
        _cd_na = [k for k in ("VIX", "WTI_RET", "GOLD_RET") if data_availability.is_unavailable(k)]
        if _cd_na and "人工加碼至" not in _cdrain_reason:   # [雲端任務A] 有效人工值不受輸入影響
            _data_unavailable("C_DRAIN_INPUTS", f"c_drain={c_drain:.2f} 由預設輸入推導(不可用:{'/'.join(_cd_na)})")
    except Exception as _e:
        _log_err(_e, "derive_c_drain")
        # [雲端任務A] c_drain 驅動 GEO_ADJ_TABLE,影響【每一檔】ETF的EV調整;
        # 失敗時靜默代入0.20(或人工值),訊號層無法得知這是即時算出還是預設值。
        _data_unavailable("C_DRAIN", f"derive_c_drain 計算失敗,改用預設0.20/人工值 "
                          f"({type(_e).__name__}: {_e})")
        c_drain = _manual_cdrain if _manual_cdrain is not None else 0.20

    geo_status_label = {
        "closed_hostile":   "🔴 武裝封鎖",
        "conditional_open": "🟡 有條件通航",
        "full_open":        "🟢 全面開放",
    }.get(strait_status, "🟡 有條件通航")

    acc = _get_accuracy()
    payoff = _get_signal_payoff()   # Kelly 用:可交易口徑(截單前訊號 → 次一交易日報酬)
    for _pm, _pv in payoff.items():
        if _pv["n"]:
            print(f"   [Kelly輸入·可交易口徑] {_pm}: {_pv['n_days']}日/{_pv['n']}筆 勝率{_pv['win_rate']:.1%} "
                  f"均盈{(_pv['avg_win'] or 0):.2%} 均虧{(_pv['avg_loss'] or 0):.2%}")
        else:
            print(f"   [Kelly輸入·可交易口徑] {_pm}: 無可配對樣本 → 最小倉位")

    # ── 午報：讀早報記錄 ─────────────────────────────
    noon_preds = {}
    if r_type == "午報":
        today_str = (run_date if is_backfill else datetime.date.today()).isoformat()
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            for code in TARGET_ETFS:
                row = conn.execute("""
                    SELECT ROUND(predicted_ret*100,2), action FROM predictions
                    WHERE code=? AND date(ts)=?
                      AND replace(ts,'T',' ') < ?
                    ORDER BY ts ASC LIMIT 1
                """, (code, today_str, f"{today_str} 10:30:00")).fetchone()
                if row:
                    noon_preds[code] = {"pred": row[0], "action": row[1]}

    # ══ [V22.9] RS-Rank 預計算（雙Pass） ════════════════
    # Pass 1：先掃描所有ETF的EV分數，用於同Style內排名
    # 同proxy問題也在此處處理：強制同proxy的ETF使用差異化liveRet
    print("\n[量化師 Pass1] RS-Rank預掃描...")
    rs_ev_map = {}   # code → ev_score
    proxy_last_seen = {}  # proxy → last_price（防止同proxy兩檔價格完全一樣）

    for code, info in TARGET_ETFS.items():
        try:
            if info["market"] == "台股":
                prices_rs = DirectDataFetcher.get_yahoo_prices(code)
            else:
                prices_rs = DirectDataFetcher.get_eastmoney_nav(code)

            if not prices_rs or len(prices_rs) < 20:
                # 與下方例外分支同理:沒有價格就沒有排名,登記後由 _rs_style_groups 排除
                _data_unavailable(f"RS_RANK:{code}",
                                  f"價格序列 {len(prices_rs or [])} 筆(<20),改用中性50分")
                rs_ev_map[code] = 50
                continue

            arr_rs = np.array(prices_rs[-60:], dtype=float)
            cur_rs = arr_rs[-1]
            ma20_rs = np.mean(arr_rs[-20:])

            # [V22.9] 同proxy差異化：若多檔ETF共用同一proxy，用小幅擾動區分
            proxy = info.get("proxy")
            if proxy and proxy in proxy_last_seen:
                # 同proxy已被使用：用NAV歷史最後兩個點算自身ret（避免完全一樣）
                if len(prices_rs) >= 2:
                    live_ret = (prices_rs[-1] - prices_rs[-2]) / prices_rs[-2]
                else:
                    live_ret = 0.0
            else:
                if proxy:
                    proxy_last_seen[proxy] = cur_rs
                live_ret = 0.0

            delta_rs = np.diff(arr_rs)
            ag = np.mean(np.where(delta_rs>0,delta_rs,0)[-14:]) or 0.001
            al = np.mean(np.where(delta_rs<0,-delta_rs,0)[-14:]) or 0.001
            rsi_rs = 100 - 100/(1 + ag/max(al,1e-9))
            bias_rs = (cur_rs - ma20_rs)/ma20_rs if ma20_rs>0 else 0
            trend_rs = min(1.0,max(-1.0,((arr_rs[-1]-arr_rs[-5])/(arr_rs[-5]+1e-9))*10)) if len(arr_rs)>=5 else 0
            rsi_s_rs = (50-abs(rsi_rs-50))/50
            bias_s_rs = max(-1,min(1,-bias_rs/0.15))
            geo_adj_rs = GEO_ADJ_TABLE.get(info["style"],{}).get(strait_status,1.0)
            W1_rs = w_tw if info["market"]=="台股" else w_a
            ev_rs = int(round(((W1_rs*(rsi_s_rs+trend_rs+bias_s_rs)/3*geo_adj_rs)+(1-W1_rs)*0.5)*100*max(0.6,1-c_drain)))
            rs_ev_map[code] = max(0, min(100, ev_rs))
            time.sleep(0.3)
        except Exception as _rse:
            # [雲端任務A] 原本完全靜默(無log、無print)。50分「中性」與真實算出的50分
            # 無法區分,而此分數會決定 get_rs_label() 顯示的同Style排名。登記不可用供查詢。
            _data_unavailable(f"RS_RANK:{code}",
                              f"main() RS-Rank預掃描失敗,改用中性50分 ({type(_rse).__name__}: {_rse})")
            rs_ev_map[code] = 50

    # 計算同Style排名
    style_groups = _rs_style_groups(rs_ev_map, TARGET_ETFS)

    def get_rs_label(code):
        style = TARGET_ETFS[code]["style"]
        group = style_groups.get(style, [])
        for rank, (c, _) in enumerate(group, 1):
            if c == code:
                return f"#{rank}/{len(group)}"
        return "#—/—"

    print(f"   RS-Rank預掃描完成：{len(rs_ev_map)}檔")

    # ══ [Quant] ETF掃描主循環 ════════════════════════
    print(f"\n[量化師] ETF掃描 {len(TARGET_ETFS)}檔...")
    results, final_report = [], ""

    # [V22.17.2 新增] 計算當前 Style 佔比（給 compute_order 用）
    _current_styles_pct = {}
    for _c, _amt in USER_HOLDINGS.items():
        _i = TARGET_ETFS.get(_c, {})
        if not _i:
            continue
        _is_q = _is_qdii_etf(_i)
        _sk = "QDII" if _is_q else _i.get("style", "?")
        _current_styles_pct[_sk] = _current_styles_pct.get(_sk, 0) + _pct_of_capital(_amt)

    # [V22.17.4 新增] 計算當前 Risk-off level（給 compute_order 用）
    # 早午晚報無法重新算盤前的 gold/wti/vix（已過時段），改用簡化判定：
    #   level 2 (重度): VIX > 25
    #   level 1 (輕度): VIX 20-25
    #   level 0 (正常): VIX < 20
    _risk_off_level = 0
    try:
        _vix_now = DirectDataFetcher.get_vix()
        if _vix_now > 25:
            _risk_off_level = 2
        elif _vix_now > 20:
            _risk_off_level = 1
    except Exception as _e:
        _log_err(_e, "main_risk_off_calc")
        # [雲端任務A] VIX 抓不到(或為None)時靜默維持 risk_off_level=0(=「正常」),
        # 這餵進 compute_order 的倉位風控閘門 —— 等於在資料不可用時假裝「風險正常」,
        # 而不是誠實標示「不知道」。登記不可用供訊號層查詢。
        _data_unavailable("VIX_RISK_OFF", f"main() risk_off_level 計算失敗(VIX不可用),"
                          f"維持預設0=正常 ({type(_e).__name__}: {_e})")

    # [V22.19] 預載未來2天事件影響的style(供操作建議分級用)
    _upcoming_event_styles = {}
    try:
        _upcoming_event_styles = get_upcoming_event_styles(load_events_csv(), lookahead_days=2)
    except Exception as _e:
        _log_err(_e, "upcoming_event_styles")

    # [V22.20 P3] 收集所有實際操作(供報告末尾'今日操作清單'單獨列出,防看錯)
    _action_list = []

    # [V22.25.2] 早報果因診斷收集: 開盤半小時「美股傳導的果 vs 有人提前布局的因」。
    #   設計定調:早報=前驗(美股傳導預期)×後驗(開盤實際)×資金流(布局探測)交匯點。
    _morning_diag = []
    _verif_actual = {}   # [V22.70] 早報迴圈收集每檔「後驗」(實際走勢)供前驗×後驗驗證

    for code, info in TARGET_ETFS.items():
        try:
            # ─ 數據採集（脫殼直連） ───────────────────
            proxy_meta = {"warning": "", "premium_info": {}, "method": ""}  # [V22.11] 預初始化防NameError
            if info["market"] == "台股":
                prices = DirectDataFetcher.get_yahoo_prices(code)
                ret_src = "TW_Direct"
                intraday_proxy_ret = 0.0
            else:
                prices = DirectDataFetcher.get_eastmoney_nav(code)
                proxy = info.get("proxy")

                # [V22.10] PrecisionProxyEngine — 精準代理取最佳漲跌幅
                intraday_proxy_ret, ret_src, proxy_meta = \
                    PrecisionProxyEngine.get_best_proxy_ret(code, info, prices)

                # 代理警示輸出
                if proxy_meta.get("warning"):
                    print(f"   [代理] {code} {proxy_meta['warning'][:60]}")

            # [V24.12] 價格有效性守衛(原本只驗長度,含0序列可通過→假信號)
            prices, _pdrop, _pok = sanitize_price_series(prices, code, 20)
            if _pdrop:
                print(f"   ⚠️ {code} 價格序列剔除 {_pdrop} 筆無效值(0/負/NaN,取數失敗)"
                      f" [剩 {len(prices)} 筆]")
            if not _pok:
                print(f"   ⚠️ {code} 有效價格不足20天(剔除{_pdrop}筆後剩{len(prices)})，跳過")
                _PRICE_SKIPPED.append(code)   # [審查] 報告頂部要明說哪些標的沒進本報
                _data_unavailable("ETF_PRICES", f"{len(_PRICE_SKIPPED)}/{len(TARGET_ETFS)} 檔價格取不到,"
                                  f"未納入本報:{', '.join(_PRICE_SKIPPED)}")
                continue

        except Exception:
            print(f"   🔴 {code} 數據崩潰:\n{traceback.format_exc()}")
            continue

        # ─ 量化計算 ──────────────────────────────────
        arr = np.array(prices[-60:], dtype=float)
        if len(arr) < 20: continue

        delta = np.diff(arr)
        avg_g = np.mean(np.where(delta>0, delta, 0)[-14:]) or 0.001
        avg_l = np.mean(np.where(delta<0, -delta, 0)[-14:]) or 0.001
        rsi = 100 - 100 / (1 + avg_g / max(avg_l, 1e-9))
        cur, ma20 = arr[-1], np.mean(arr[-20:])
        bias = (cur - ma20) / ma20 if ma20 > 0 else 0.0
        z_score = round((cur - ma20) / (np.std(arr[-20:]) + 1e-9), 2)
        trend_5 = ((arr[-1] - arr[-5]) / (arr[-5] + 1e-9)) if len(arr)>=5 else 0.0
        trend = round(min(1.0, max(-1.0, trend_5 * 10)), 2)

        rsi_s = round((50 - abs(rsi-50)) / 50, 4)
        bias_s = round(max(-1, min(1, -bias/0.15)), 4)

        tech = {
            "RSI": round(rsi, 2), "BIAS": round(bias, 4),
            "Z_Score": z_score, "Trend_Strength": trend,
            "price": round(cur, 4),
        }

        # ═══ [V22.13 架構升級] Trend Regime Detection ═══
        # 8D根因分析後新增：先判斷市場狀態，EV公式分流
        regime_info = TrendRegimeDetector.detect(prices)
        regime = regime_info["regime"]
        tech["Regime"] = regime
        tech["Regime_Strength"] = regime_info["strength"]
        tech["Regime_Evidence"] = regime_info["evidence"]

        # ─ EV計算（V22.13: regime-aware） ─────────────
        style = info["style"]
        _is_qdii = _is_qdii_etf(info)
        geo_adj = GEO_ADJ_TABLE.get(style, {}).get(strait_status, 1.0)

        # [V22.8] 超賣保護：RSI<32 時 geo_adj 補強
        if rsi < RSI_OVERSOLD:
            geo_adj = min(geo_adj + 0.05, 1.30)

        W1 = w_tw if info["market"]=="台股" else w_a

        # [V22.13] 使用 regime-aware EV 計算
        ev = TrendRegimeDetector.compute_ev_regime_aware(
            rsi, bias, trend, geo_adj, W1, c_drain, regime_info)

        # trade_ease 加成（保留）
        if trade_ease>0 and info["market"]=="A股" and style=="Tech" and not _is_qdii:
            ev = min(100, ev + trade_ease * 8)

        # ─ [V22.16 5/27 完整版] 跨報宏觀因子繼承機制 ─────────────
        # GEMINI 提案 / 用戶 P0 需求 / V22.17 P0 提前實裝
        # 完整版：係數 0.20、Defensive 黃金敏感、QDII NDX 主導
        # [Bug 修 5/27] sox/ndx 改為 main() 真實變數名 sox_d/ndx_d
        macro_prior = TrendRegimeDetector.compute_macro_prior(
            sox_ret=sox_d, ndx_ret=ndx_d, vix_val=vix,
            cny_val=usd_cny, style=style,
            gold_ret=gold_ret, is_qdii=_is_qdii,
            tech_coupling=compute_tech_coupling() if (style == "Tech" and not _is_qdii) else None,  # [V22.27.9]
        )
        ev_before_macro = ev
        # [V22.37 治錯誤歸因] ★EV 歸因鏈:報告原本只印「宏觀先驗(x) → EV a→b」,
        #   但 b 是【顯示當下】的 ev,期間 EV 還被資金流/夾擠/脆弱度/VIX 改過多次
        #   → 把整段崩塌全誣賴給宏觀(008888 實證:宏觀 -0.04 依公式 18→18,報告卻寫 18→0)。
        #   與缺陷C「誣賴第⑤條」同族。改為逐步記錄,顯示真兇。
        _ev_trace = [("基礎EV", ev, ev)]
        ev = int(round(ev * (1 + macro_prior * 0.20)))  # 完整版 0.20
        ev = max(0, min(100, ev))
        _ev_trace.append((f"宏觀先驗{macro_prior:+.2f}", ev_before_macro, ev))
        macro_log = ""
        if abs(ev - ev_before_macro) >= 1:
            macro_style_tag = "QDII" if _is_qdii else style
            macro_log = f"宏觀先驗={macro_prior:+.2f}, EV {ev_before_macro}→{ev}"
            print(f"   [宏觀先驗] {code} {macro_style_tag} {macro_log}")

        # [V22.13] 趨勢市的regime資訊用於後續信號決策
        is_trending_up   = (regime == "trending_up")
        is_trending_down = (regime == "trending_down")
        regime_strength  = regime_info["strength"]

        # ─ [V22.24.9] 資金流類信號合計EV調整封頂基準(防5日背離+單日背離疊加) ──
        _ev_pre_flow = ev
        # ─ [V22.18 新增] 持倉資金流向背離示警(在EV最終化後疊加) ──────
        holding_divergence = ""
        try:
            _proxy_h = info.get("proxy")
            if _proxy_h:
                _flow_h = _flow5_with_manual(code, _proxy_h, DirectDataFetcher.get_etf_5day_flow(_proxy_h))
                ev, holding_divergence = apply_flow_divergence(ev, rsi, _flow_h)
                if holding_divergence:
                    print(f"   [資金背離] {code} {holding_divergence[:40]}")
        except Exception as _e:
            _log_err(_e, "holding_flow_divergence")

        # ─ [V22.24.0 因果鏈L2] 資金流大小單拆解(Gemini盲點①: 防誘多假強流入) ──
        #   抓取依賴WorkBuddy真實環境(VPN), 抓不到靜默不影響主流程。EV輕量±5。
        #   [V22.24.3 觀測性] 無論通/中性/失敗都留痕到console, 便於區分VPN狀態。
        order_flow_tag = ""
        try:
            _proxy_of = info.get("proxy")
            if _proxy_of:
                _bd = get_order_flow_breakdown(_proxy_of)
                if _bd is None:
                    print(f"   [大小單拆解] {code} 抓取失敗(已帶ut; 疑東財限流/無數據, 非必VPN)")
                else:
                    # [V22.25.0] 主循環抓到就存DB——每份報告(盤前/早/午/收)都攒歷史,
                    #   限流時抓住每一次成功。nav_ret: 0.0視為無數據(傳None保留收盤回填值)。
                    # [V24.14 已知殘留·刻意不改] 真收平 0.0 為 falsy → 仍傳 None。
                    #   這是同一病灶的殘留(真收平被當無數據),但原設計的 fallback
                    #   (保留收盤回填值)比寫入 0.0 更準確,且不會崩潰 → 維持原樣。
                    _navr = (intraday_proxy_ret * 100
                             if intraday_proxy_ret else None)
                    persist_capital_flow(code, info.get("name", code)[:10], _bd,
                                         nav_ret=_navr)
                    # [V22.24.9] 傳同日場內漲跌→價量背離判定(只在背離時動EV,不重複計純方向)
                    _ofj = judge_order_flow(_bd, price_ret=intraday_proxy_ret)
                    if _ofj and _ofj["ev_adj"] != 0:
                        _evb = ev
                        ev = max(0, min(100, ev + _ofj["ev_adj"]))
                        try: _ev_trace.append((f"資金流{_ofj['ev_adj']:+d}", _evb, ev))
                        except Exception as _e:
                            _log_err(_e, "get_rs_label:L12020")  # [V24.16] 原為靜默 pass
                            pass
                        order_flow_tag = f"{_ofj['icon']}{_ofj['verdict']}"
                        print(f"   [大小單拆解] {code} {order_flow_tag} "
                              f"EV{_ofj['ev_adj']:+d} | {_ofj['detail']}")
                    else:
                        # 同向/無背離也留痕(證明接口通了, 只是今日價量無背離)
                        print(f"   [大小單拆解] {code} ➡️{_ofj['verdict'] if _ofj else '中性'}"
                              f"(主力{(_bd.get('主力') or 0):+.2f}億, 價{intraday_proxy_ret:+.1%})")
        except Exception as _e:
            _log_err(_e, "order_flow_breakdown")

        # [V22.25.2] 早報果因診斷收集(開盤半小時: 美股傳導的果 vs 有人提前布局的因)
        if r_type == "早報":
            try:
                _style = info.get("style", "")
                # 前驗: 美股傳導預期 (Tech看SOX, QDII看NDX, 其他無傳導鏈→None)
                if _style == "Tech":
                    _exp = sox_d
                elif _style == "QDII":
                    _exp = ndx_d
                else:
                    _exp = None
                # 後驗: 開盤實際。台股走Yahoo(intraday_proxy_ret寫死0)→從prices序列算當日漲跌
                if info.get("market") == "台股":
                    _act = ((prices[-1] - prices[-2]) / prices[-2]
                            if prices and len(prices) >= 2 and prices[-2] else 0.0)
                    _main = None  # 台股無東財大小單
                    _has_flow = False
                else:
                    _act = intraday_proxy_ret
                    # 主力淨流入額(供渲染算「當日市場共性」相對異常用)
                    # [V22.25.5fix P2] 標準寫法：用 locals() 取代 dir()，更可靠
                    _bd_local = locals().get("_bd")
                    _main = (_bd_local.get("主力") if _bd_local else None)
                    _has_flow = _main is not None
                _morning_diag.append({
                    "code": code, "name": info.get("name", code)[:10],
                    "style": _style, "market": info.get("market", ""),
                    "exp": _exp, "act": _act,
                    "main": _main, "has_flow": _has_flow})
                # [V22.70] 收集「後驗」(今日實際走勢)供前驗×後驗驗證。
                #   intraday_proxy_ret=同日場內漲跌(後驗本體);ret_src 標來源,
                #   台股 src=TW_Direct 且 ret=0.0 → 格式化時標「—」而非假裝 0。
                _verif_actual[code] = {"ret": intraday_proxy_ret,
                                       "src": ret_src,
                                       "market": info.get("market", "")}
            except Exception as _e:
                _log_err(_e, "morning_diag_collect")
        _evb_clamp = ev
        ev = int(max(_ev_pre_flow - 8, min(_ev_pre_flow + 8, ev)))
        ev = max(0, min(100, ev))
        try:
            if ev != _evb_clamp: _ev_trace.append(("資金流±8夾擠", _evb_clamp, ev))
        except Exception as _e:
            _log_err(_e, "get_rs_label:L12073")  # [V24.16] 原為靜默 pass
            pass

        # ─ [V22.28 L2] 內生脆弱度 → EV 扣分 + 禁主攻旗標 ──────────────
        fragility_score = 0
        fragility_tag = ""
        fragility_block_attack = False
        if FRAGILITY_ENABLED and info.get("market") == "A股" and not _is_qdii:
            try:
                _fi = _fragility_inputs_from(
                    prices, locals().get("_flow_h"), rsi, tech.get("Z_Score", 0)
                    if isinstance(locals().get("tech"), dict) else 0)
                fragility_score = compute_fragility(**_fi)
                _adj, fragility_tag, fragility_block_attack = fragility_action(fragility_score)
                if _adj:
                    _evb_f = ev
                    ev = max(0, min(100, ev + _adj))
                    try: _ev_trace.append((f"脆弱度{_adj:+d}", _evb_f, ev))
                    except Exception as _e:
                        _log_err(_e, "get_rs_label:L12090")  # [V24.16] 原為靜默 pass
                        pass
                    print(f"   [V22.28脆弱度] {code}: {fragility_score} → EV {_adj:+d} {fragility_tag}")
            except Exception as _e:
                _log_err(_e, f"fragility_{code}")

        # ─ [V22.23.6 影子] 低點買入三共振偵測(只記錄不觸發, 觀察期驗證) ──
        shadow_resonance_tag = ""
        try:
            _rc, _rhits, _rpush = detect_bottom_resonance(
                code, rsi, bias, holding_divergence, sox_d, ndx_d, _is_qdii)
            if _rc >= 2:
                record_shadow_signal(code, info.get("name", code), r_type,
                                     rsi, bias, _rc, _rhits, _rpush)
                if _rc == 3:
                    shadow_resonance_tag = (
                        f"🌑影子買入信號(3/3共振·未啟用觀察中): {'+'.join(_rhits)}")
                    print(f"   [影子] {code} 🌑三共振齊發(未啟用): {'+'.join(_rhits)}")
                else:
                    shadow_resonance_tag = (
                        f"🌑近觸發(2/3共振·觀察): {'+'.join(_rhits)}")
                    print(f"   [影子] {code} 2/3共振(觀察): {'+'.join(_rhits)}")
        except Exception as _e:
            _log_err(_e, "bottom_resonance_shadow")

        # ─ predicted_ret ──────────────────────────────
        if info["market"] == "台股":
            if len(prices) >= 2:
                predicted_ret = (prices[-1] - prices[-2]) / prices[-2]
            else:
                predicted_ret = None
        elif info["market"] == "A股":
            if _is_qdii and ndx_d != 0:
                # QDII：基於NDX走勢估算
                predicted_ret = ndx_d + (usd_cny - 7.0) * (-0.02)
            elif abs(intraday_proxy_ret) > 1e-6:
                # 有場內即時數據：使用場內漲跌
                # [V22.12 P0新增] 早報穩健化：異常波動時加權平均NAV變化
                # 5/19 008888 早報Sina抓到 -0.81%（早盤瞬時回調）
                #       NAV近5日均變化 +0.23%（基準）
                #       異常程度 z>2σ → 早報加權平均降低瞬時雜訊
                if r_type == "早報" and len(prices) >= 6:
                    # 近5日NAV日漲跌
                    nav_changes = []
                    for i in range(1, 6):
                        if prices[-i-1] > 0.01:
                            nav_changes.append((prices[-i]-prices[-i-1])/prices[-i-1])
                    if nav_changes:
                        avg_nc = sum(nav_changes)/len(nav_changes)
                        var_nc = sum((x-avg_nc)**2 for x in nav_changes)/len(nav_changes)
                        std_nc = var_nc ** 0.5
                        # 異常檢測：Sina當前漲跌 vs 歷史均值的Z-Score
                        if std_nc > 0.001:
                            z_proxy = abs(intraday_proxy_ret - avg_nc) / std_nc
                            if z_proxy > 2.0:
                                # 異常波動 → 加權平均（Sina:50% + NAV基準:50%）
                                smoothed = 0.5 * intraday_proxy_ret + 0.5 * avg_nc
                                print(f"   [V22.12早報平滑] {code}: "
                                      f"Sina={intraday_proxy_ret:+.2%} z={z_proxy:.1f}σ → "
                                      f"平滑={smoothed:+.2%}")
                                predicted_ret = smoothed
                            else:
                                predicted_ret = intraday_proxy_ret
                        else:
                            predicted_ret = intraday_proxy_ret
                    else:
                        predicted_ret = intraday_proxy_ret
                else:
                    predicted_ret = intraday_proxy_ret
            elif len(prices) >= 2:
                # 無場內數據：用NAV日間變化（T+1）
                predicted_ret = (prices[-1] - prices[-2]) / prices[-2]
            else:
                predicted_ret = None
        else:
            predicted_ret = None

        if predicted_ret is not None:
            # 超買截斷
            if rsi > 70 and predicted_ret > 0:
                predicted_ret *= max(0.15, 1.0-(rsi-70)*0.03)
            # [V22.17.3 改 5/29] 幅度上限 0.05 → 0.08
            # 根因：5/29 真實 008888 -3.68% / 011613 -3% / 018177 -2.8%
            #       系統壓到 -5% 熔斷上限 = 看起來「災難級別」
            #       實際只是普通減倉信號 → 傳遞錯誤量級訊號
            # 改為 0.08 後：真實 -3% 顯示 -3%（量級準確），
            #               真實 -7% 顯示 -7% (才真的接近熔斷)
            cap = 0.08
            predicted_ret = max(-cap, min(cap, predicted_ret))

        final_pred = predicted_ret if predicted_ret is not None else 0.0

        # [V22.23.3 自校正] 用歷史真實偏差修正本次預測(數據先於預期的反饋迴路)
        #   電力等被系統性高估的檔, 會被自動下修; 樣本<8不動作(安全靜默)。
        _bias_corr, _bias_info = compute_pred_bias(code)
        if _bias_info is not None:
            final_pred = max(-0.08, min(0.08, final_pred + _bias_corr))
            _bdir = "高估" if _bias_info["bias"] > 0 else "低估"
            print(f"   [自校正] {code}: 歷史{_bias_info['n']}筆 系統性{_bdir}"
                  f"{_bias_info['bias']:+.2%} → 本次修正{_bias_corr:+.2%}")

        # ─ [CRO] 信號決策（含所有護欄） ─────────────
        # 優先級0：VIX恐慌熔斷（V22.7新增，保留）
        if vix >= 25 and style != "Defensive":
            action = "💥 VIX恐慌熔斷 (強制靜默)"
            _evb_v = ev
            ev = min(ev, 35)
            try:
                if ev != _evb_v: _ev_trace.append(("VIX恐慌上限35", _evb_v, ev))
            except Exception as _e:
                _log_err(_e, "get_rs_label:L12198")  # [V24.16] 原為靜默 pass
                pass
        # 優先級1：物理熔斷
        elif z_score < -3.0:
            action = "💥 物理熔斷"
        # 優先級2：避險免疫
        elif style == "Defensive" and strait_status == "closed_hostile":
            action = ("🔴 減倉" if ev<40 else "🟢 主攻" if ev>=65 else "🟡 靜默")
        # 優先級3：Z觸頂
        elif z_score > 3.0:
            action = "🔥 極端超買"
        # 優先級4：RSI 90+止盈（任何regime都觸發）
        elif rsi >= 90:
            action = "🔴 減倉 (RSI90+止盈)"
        # ═══ [V22.13 架構升級] 優先級4.5：趨勢市分流 ═══
        # 8D根因：5/18-5/20連續錯失+348元都是因為「均值回歸假設」
        # 解決：在 P2/常規邏輯之前先過 regime，趨勢市用獨立邏輯
        elif is_trending_up:
            # 趨勢上行：超買是正常的，正向預測=順勢=主攻
            if final_pred >= 0.015:
                action = f"🟢 主攻 (上升趨勢追隨, strength={regime_strength:.1f})"
            elif final_pred >= 0.005:
                action = f"🟢 主攻 (上升趨勢延續, strength={regime_strength:.1f})"
            elif final_pred > -0.005:
                # 預測中性 → 持倉觀望（趨勢未變）
                action = "🟡 靜默 (趨勢內持倉)"
            else:
                # 趨勢內回調預警（但不減倉，因為趨勢未確認反轉）
                action = "🟡 靜默 (趨勢內回調預警)"
        elif is_trending_down:
            # 趨勢下行：超賣保護不可信，預測負向強化減倉
            if final_pred < -0.005:
                action = f"🔴 減倉 (下行趨勢加強, strength={regime_strength:.1f})"
            elif final_pred < 0.005:
                action = "🟡 靜默 (下行趨勢觀望)"
            else:
                # 趨勢內反彈：不主攻，謹慎觀察
                action = "🟡 靜默 (下行趨勢內反彈)"
        # 優先級5：P2超買區（震盪市專用）
        elif rsi > 75 and bias > 0.05:
            if final_pred >= 0.015 and trend >= 0.15 and ev >= 22:
                action = "🟢 主攻 (強動量+正向預測通道)"
            elif trend >= 0.4 and ev >= 55:
                action = "🟢 主攻 (趨勢延續)"
            elif trend >= 0.2:
                action = "🟡 靜默 (趨勢觀望)"
            elif ev < 40 and final_pred < -0.001:
                action = "🔴 減倉 (P2超買)"
            else:
                action = "🟡 靜默 (P2超買)"
        # 優先級6：常規（震盪市標準）
        else:
            if ev >= 65:
                action = "🟢 主攻"
            elif final_pred >= 0.015 and trend >= 0.15 and ev >= 22:
                action = "🟢 主攻 (強動量+正向預測通道)"
            elif ev >= 40:
                action = "🟡 靜默"
            else:
                # [V22.15 Bug2修復 v2] 弱負預測門檻調整
                # 根因：5/25 早報 019141 pred=-0.53% RSI=63.6 EV=23 → 🔴減倉
                #       但午報 pred=+0.66% 完全反轉，證明減倉信號是噪訊
                # 修復v1：-0.005 門檻仍無法擋下 -0.0053 (差 0.03pp 失敗)
                # 修復v2：門檻提到 -0.01 (-1%)，徹底擋下弱負信號
                # 歷史驗證：所有命中減倉案例 pred 都 < -3%，不受影響
                #          5/19 016185 -4.20% / 5/21 019141 -4.30% / 5/22 004432 -3.17%
                action = "🔴 減倉" if final_pred < -0.01 else "🟡 靜默 (低信心觀望)"

        # ═══ [V22.13 升級] 早報置信度懲罰（趨勢市+強信號排除）════
        # PLUS版：RSI 70-80懲罰，覆蓋率67%（4/6檔）
        # V22.12：擴大到 RSI 65-80，覆蓋率100%（6/6檔）
        # V22.13 進一步排除：
        #   1. 趨勢市的減倉/主攻信號（regime已確認方向，置信度高）
        #   2. |pred|>=2% 的強信號（016185 -4.44% 不該被延遲3小時）
        # [V22.16 P2-A 修 5/27] 邊界從 65-80 放寬到 60-85
        #   根因：5/25 019141 RSI=63.6 卡邊界外，弱負信號直接觸發減倉
        #   午報 pred 反轉到 +0.66%，證實減倉是噪訊
        # Style 過濾：Defensive/Dividend 不適用此懲罰
        if (r_type == "早報" and 60 <= rsi <= 85 and "減倉" in action
                and style not in ("Defensive", "Dividend")):
            strong_signals = ["熔斷", "RSI90", "Defensive", "💥",
                              "下行趨勢"]  # V22.13新增
            is_strong_pred = abs(final_pred) >= 0.02  # V22.13: 強預測排除
            if (not any(s in action for s in strong_signals)
                    and not is_strong_pred):
                original_action = action
                action = "🟡 靜默 (早報置信度不足，待午報校正)"
                print(f"   [V22.16 P2-A早報懲罰] {code}: RSI={rsi:.0f}∈[60,85] "
                      f"原[{original_action}] → [{action}]")

        # ═══ 護欄系統（全套保留）═══════════════════
        # [V22.8 P0 → V22.36.2 加下跌趨勢豁免] 超賣護欄:RSI<32 時禁止減倉。
        # ★使用者 7/20 質疑「反彈概率72%是真的嗎」→ 回源碼查證:【不是】。
        #   `QuantEnhanced.rsi_reversal_prob` 是【寫死查表】(RSI<25→0.81 / <30→0.72 / <35→0.55),
        #   全檔無任何回測/校準/樣本數註記,且【無時間窗、無幅度定義、只看RSI】。
        #   → 用一個未經校準的魔術數字去否決真實減倉信號,是本系統最貴的假精確。
        # ★致命處:本護欄【完全無視趨勢】。均值回歸只在【震盪市】有效;真實下跌趨勢中
        #   超賣會更超賣(接下墜的刀)。008888 實證:RSI=25.85 觸發護欄→靜默,但同一張卡
        #   trend=-1.00(最差)、5日-14.9%、主力連流出5/5日-8.2億 → 上週利潤蒸發約35%。
        # ★修法:趨勢明確向下時【豁免】本護欄,保留減倉信號(交給後續分級/時序閘決定)。
        #   非下跌趨勢(震盪/上行)時維持原行為 = 零回歸。
        _os_trend = locals().get("trend")
        _falling_knife = (_os_trend is not None and _os_trend < OVERSOLD_GUARD_TREND_MIN)
        if rsi < RSI_OVERSOLD and "減倉" in action and "RSI90" not in action:
            if _falling_knife:
                print(f"   [超賣護欄·豁免] {code}: RSI={rsi:.0f}<32 但趨勢={_os_trend:.2f}"
                      f"<{OVERSOLD_GUARD_TREND_MIN} → 下跌趨勢中『超賣≠反彈』(接刀風險),"
                      f"護欄不套用,保留減倉信號")
            else:
                print(f"   [超賣護欄] {code}: RSI={rsi:.0f}<32 且趨勢未明確向下 → 禁止減倉,改靜默")
                action = "🟡 靜默 (超賣保護·僅震盪市適用;⚠️反彈概率為未校準查表值)"

        # 護欄A：減倉+正向pred → 靜默
        elif "減倉" in action and final_pred > 0.001:
            action = (f"🟡 靜默 (⚠️pred={final_pred:+.1%}但EV={ev}%矛盾)"
                      if abs(final_pred) > 0.02 and ev < 35
                      else "🟡 靜默 (低信心觀望)")
            print(f"   [護欄A] {code}: 減倉+正向pred → {action}")

        # 護欄B：主攻+負向pred → 靜默
        elif "主攻" in action and final_pred < -0.001:
            action = "🟡 靜默 (⚠️主攻但預測負向)"
            print(f"   [護欄B] {code}: 主攻+負向pred → {action}")

        # ─ 風險評分 ───────────────────────────────────
        risk_level, _ = QuantEnhanced.composite_risk_score(
            rsi, z_score, bias, trend, ev)

        # ─ Kelly ─────────────────────────────────────
        _po = payoff.get(info["market"], {})
        # [V22.22] 該style臨近重大事件 → 觸發Kelly事件波動率懲罰(溫和降倉)
        _kelly_event_near = bool(_upcoming_event_styles.get(style)) if _upcoming_event_styles else False
        # [V22.26] vol-scaled Kelly(模式B相對基準)+ 條件硬煞 — 過熱煞車
        #   全程防禦式:任何例外/缺值→_vf=1.0,_hard_brake=False(=無變化,絕不crash活報告)
        _vf = 1.0; _hard_brake = False
        try:
            if prices and len(prices) > 21:
                _rets = [(prices[i] - prices[i-1]) / prices[i-1]
                         for i in range(1, len(prices)) if prices[i-1]]
                _rv = compute_realized_vol(_rets, lookback=20)   # 近期實現波動
                _bv = compute_realized_vol(_rets, lookback=60)   # 自身60d常態基準(模式B)
                _vf = vol_scale_factor(_rv, _bv)
            _overheated = sum([rsi >= 70, z_score >= 2.0, bias >= 0.10]) >= 2  # 三選二防誤殺
            try:
                _mhw = (vix >= 20) or (c_drain >= 0.28)   # 宏觀逆風(缺值→下方except→False)
            except Exception:
                _mhw = False
            _hard_brake = bool(_overheated and _mhw)
        except Exception as _ve:
            _vf = 1.0; _hard_brake = False
            print(f"   [V22.26 vol-scale] {code} 計算失敗→回退無縮放: {_ve}")
        # [V22.27.7] IC技巧乘子:量到的幅度技巧低→收縮倉位(防禦:不確定/樣本<30→×1.0)
        try:
            _vf *= ic_conviction_multiplier(info["market"])
        except Exception as _e:
            _log_err(_e, "get_rs_label:L12353")  # [V24.16] 原為靜默 pass
            pass
        kelly = KellyEngine.compute(ev, _po.get("win_rate"), _po.get("avg_win"), _po.get("avg_loss"),
                                    n_eff=_po.get("n_days", 0), event_near=_kelly_event_near,
                                    vol_factor=_vf, hard_brake=_hard_brake)

        # ─ 持倉P&L ───────────────────────────────────
        pos_pnl  = PositionBook.pnl(code, cur) if PositionBook.get(code) else {}
        stop_tip = PositionBook.stop_profit_check(code, cur) if pos_pnl else ""

        # ─ 午報修正標記 ──────────────────────────────
        noon_note = ""
        if r_type == "午報" and code in noon_preds:
            mp = noon_preds[code]
            m_pred = mp.get("pred", 0) or 0
            m_act  = mp.get("action", "") or ""
            drift  = (final_pred * 100) - m_pred
            act_chg = (("🔴" in action) != ("🔴" in m_act) or
                       ("🟢" in action) != ("🟢" in m_act))

            # [V22.11] 午報強制覆核：當信號180°反轉時發出強警示
            if act_chg:
                if "主攻" in m_act and "減倉" in action:
                    noon_note = f" | ⚡[午報強制覆核:早報主攻→午報減倉，方向180°反轉！早={m_pred:+.2f}%]"
                elif "減倉" in m_act and "主攻" in action:
                    noon_note = f" | ⚡[午報強制覆核:早報減倉→午報主攻，方向反轉！早={m_pred:+.2f}%]"
                else:
                    noon_note = f" | ⚡[午報修正:方向改變，早={m_pred:+.2f}%]"
            elif abs(drift) > 0.5:
                noon_note = f" | 📊[午報漂移{drift:+.2f}%，早={m_pred:+.2f}%]"
            elif abs(drift) > 0.3:
                noon_note = f" | [午報修正{drift:+.2f}%，早={m_pred:+.2f}%]"
            else:
                noon_note = f" | [維持早報判斷]"

        # ─ 敘事生成（差異化）──────────────────────────
        narrative = QuantEnhanced.build_narrative(info, tech, ndx_d, vix, c_drain, c_drain_reason=_cdrain_reason)

        # ─ DB寫入 ────────────────────────────────────
        # [V22.24.0 因果鏈] 組裝本次預測「基於什麼因」, 積累因→果歷史(正/負樣本)
        _causes = []
        _lv = locals()
        try:
            if abs(macro_prior) > 0.001:
                _causes.append(f"宏觀先驗{macro_prior:+.2f}")
            _hd = _lv.get("holding_divergence", "")
            if _hd:
                _causes.append("資金底背離" if "底背離" in _hd else "資金背離")
            _sr = _lv.get("shadow_resonance_tag", "")
            if _sr:
                _causes.append("三共振3/3" if "3/3" in _sr else "三共振2/3")
            _of = _lv.get("order_flow_tag", "")
            if _of:
                _causes.append(_of)  # 大小單真偽(機構吃貨🔴/誘多🚩)
            _bi = _lv.get("_bias_info")
            if _bi:
                _causes.append("自校正")
            _rt = _lv.get("regime_type", "")
            if _rt in ("trending_up", "trending_down"):
                _causes.append(f"Regime={_rt}")
        except Exception as _e:
            _log_err(_e, "get_rs_label:L12412")  # [V24.16] 原為靜默 pass
            pass
        _cause_tag = " | ".join(_causes) if _causes else "技術面為主"
        try:
            with sqlite3.connect(LOG_DB, timeout=10) as conn:
                # [V22.16 5/27 P1] 傳入 r_type 區分四報，macro_prior 留存
                _db_write(conn, code, info, ev, action,
                          strait_status, c_drain, geo_adj, tech, final_pred,
                          report_type=(r_type_db or r_type),  # [V22.62] 補跑標記,避免污染統計
                          macro_prior=macro_prior,
                          cause_tag=_cause_tag)
        except Exception as e:
            print(f"   🔴 {code} DB寫入失敗: {e}")

        # ─ RS排名（使用預計算結果）───────────────────
        rs_label = get_rs_label(code)

        # ─ [V22.10] 代理品質 + 溢折價信息 ────────────
        _prem = proxy_meta.get("premium_info", {}) if isinstance(proxy_meta, dict) else {}
        _prem_sig = _prem.get("signal", "")
        _proxy_q = f"代理:{info.get('proxy_name','?')[:35]}|TE≈{info.get('te','?')}"
        if info.get("fund_type") == "enhanced":
            _proxy_q += " ⚠️增強型"
        if proxy_meta.get("warning"):
            _proxy_q += f"|⚠️{proxy_meta['warning'][:30]}"

        # ─ 報告格式 ───────────────────────────────────
        section = ReportFormatter.format_etf_section(
            code, info, tech, ev, action, predicted_ret,
            geo_adj, ret_src, strait_status, geo_status_label,
            narrative, kelly, pos_pnl,
            noon_note=noon_note, risk_level=risk_level, stop_tip=stop_tip,
            rs_label=rs_label, proxy_quality=_proxy_q, premium_signal=_prem_sig)

        # [V22.16 → V22.37 治錯誤歸因] 原本只印「宏觀先驗(x) → EV a→b」,把整段 EV 崩塌
        #   全掛在宏觀頭上(008888 實證:宏觀依公式 18→18,報告卻寫 18→0)。
        #   改印【完整歸因鏈】並標出【主因】(絕對變動量最大者)。
        try:
            _tr = [t for t in (locals().get("_ev_trace") or []) if t[1] != t[2]]
            if _tr:
                _chain = " → ".join(f"{_lb} {_a}→{_b}" for _lb, _a, _b in _tr)
                _main_f = max(_tr, key=lambda t: abs(t[2] - t[1]))
                _mtxt = f"{_main_f[0]}({_main_f[2]-_main_f[1]:+d})"
                section += (f"> 📊 **EV 歸因鏈**：{_ev_trace[0][1]} → {_chain} → **最終 {ev}**\n"
                            f"> ➡️ **主因：{_mtxt}**"
                            f"（★非宏觀專屬;宏觀先驗{macro_prior:+.2f} 依公式 ev×(1+prior×0.2) 影響有限）\n")
            elif abs(ev - ev_before_macro) >= 1:
                _e2 = "📈" if macro_prior > 0 else "📉"
                section += (f"> {_e2} 宏觀先驗：{'宏觀利多支撐' if macro_prior>0 else '宏觀利空壓制'} "
                            f"({macro_prior:+.2f}) → EV {ev_before_macro}→{ev}\n")
        except Exception as _e:
            _log_err(_e, "ev_attrib_chain")

        # [V22.18.4] 個股詳情卡補5日資金流向+背離(WorkBuddy指出的詳情卡缺口)
        try:
            _proxy_d = info.get("proxy")
            if _proxy_d:
                _flow_d = DirectDataFetcher.get_etf_5day_flow(_proxy_d)
                if _flow_d:
                    # [V24.26] 同上:flow5_list 可能為 None,不可直接迭代
                    _fld = _flow_d.get("flow5_list") or []
                    _daily_d = ", ".join(f"{x:+.1f}" for x in _fld) if _fld else "無逐日序列(代理/手工值)"
                    section += (f"> 💰 5日主力淨流入：{_flow_d.get('flow5_yi', 0):+.1f}億 "
                                f"({_flow_d.get('signal','—')}, {_flow_d.get('trend','—')}) | 逐日[{_daily_d}]億\n")
            if holding_divergence:
                section += f"> {holding_divergence}\n"
            if shadow_resonance_tag:
                section += f"> {shadow_resonance_tag}\n"
        except Exception as _fe:
            _log_err(_fe, "detail_card_flow")

        # [V22.17.2 新增] 操作指令化（基金經理人視角的具體金額建議）
        # [V22.17.4] 加入 risk_off_level 護欄
        # [V22.17.4 5/30 修 Bug]: kelly 是 dict，必須取 half_kelly 並轉 % (5/30 早報 016185 失敗根因)
        try:
            kelly_pct_for_order = 0.0
            if isinstance(kelly, dict):
                kelly_pct_for_order = kelly.get("half_kelly", 0.0) * 100  # 0.114 → 11.4
            elif isinstance(kelly, (int, float)):
                kelly_pct_for_order = float(kelly)
            # ═ [V22.33.1] 接上動態上限五輸入(WB抓到:原本全None→突破區永遠False=死碼)═
            _dc_tech = tech if isinstance(tech, dict) else {}
            _dc_trend = _dc_tech.get("Trend_Strength")
            _dc_rsi = _dc_tech.get("RSI")
            _dc_frag = 0
            try:
                for _rr in results:
                    if _rr.get("code") == code:
                        _dc_frag = _rr.get("fragility", 0); break
            except Exception as _e:
                _log_err(_e, "get_rs_label:L12499")  # [V24.16] 原為靜默 pass
                pass
            _dc_flow5 = None
            _dc_flow5_prev = read_prev_flow5(code)   # [V22.61] ★真正賦值(原本從未賦值)
            try:
                _pxy = info.get("proxy")
                _ff = DirectDataFetcher.get_etf_5day_flow(_pxy) if _pxy else None
                if _ff and isinstance(_ff, dict):
                    _dc_flow5 = _ff.get("flow5_yi")
            except Exception as _fe:
                _log_err(_fe, f"dcap_flow_{code}")
            # ★估值分位:系統目前【無PE分位欄位】→ 傳None → 第⑤條必擋(誠實,不假放行)
            _dc_val = None
            # [V22.34] ★接上第⑤條估值分位(原恆 None → 突破區結構性關閉=等效40%硬上限)
            #   有真資料且未過期 → 傳入;無/過期 → 維持 None → check_dynamic_cap 保守擋下(誠實)
            try:
                _pv, _pmeta = load_pe_percentile(code)
                _psrc = (_pmeta.get("source") or "")
                if _pv is not None and _psrc.startswith("local_rolling"):
                    # [V22.72 治語義安全缺陷] ★滾動窗口分位【不驅動⑤條解鎖】,僅顯示。
                    #   滾動分位低 ≠ 絕對估值便宜:近期小跌即可讓【歷史高估值】PE 得到低滾動分位。
                    #   已用真 check_dynamic_cap 驗證:PE=40(3年真分位≈0.85 貴)但近25日區間[40,44]
                    #   → 滾動分位 0.04 → ⑤ 錯誤放行 40→50% 集中(軟銀式接刀,正是⑤要防的),
                    #   且比修前(None→安全擋下)更危險。故滾動分位維持 _dc_val=None → ⑤ 保守擋下。
                    #   真解鎖需【底層指數多年估值分位】(中證/國證指數估值,待 WB live 核端點,㉒)。
                    print(f"   [估值分位] {code} 滾動分位={_pv:.2f}({_pmeta.get('date')},觀察用)"
                          f" → 語義非絕對估值,不驅動⑤解鎖 → 第⑤條維持保守擋下")
                elif _pv is not None:
                    _dc_val = _pv   # 真·多年估值分位(來源非 local_rolling)→ 驅動⑤
                    _proxy_tag = "·⚠️代理近似(同板塊,非精確底層)" if "proxy" in _psrc else ""
                    print(f"   [估值分位] {code} {_pmeta.get('window')}={_pv:.2f} "
                          f"(資料日 {_pmeta.get('date')},來源 {_psrc}{_proxy_tag})")
                elif _pmeta.get("status") != "no_data":
                    print(f"   [估值分位] {code} 不採用:{_pmeta.get('status')} → 第⑤條保守擋下")
            except Exception as _e:
                _log_err(_e, f"dcap_pe_{code}")

            # [V22.73] 每日刷新 PE 分位:優先 funddb 真·多年分位(驅動⑤),失敗 fallback 滾動(觀察用)
            try:
                _rp, _rmeta = refresh_pe_percentile_funddb(code, info)
                if _rp is None:
                    _rp, _rmeta = refresh_pe_percentile(code, info)   # 滾動 fallback(不驅動⑤)
                if _rp is not None:
                    _tag = "funddb真分位" if _rmeta.get("source") == "funddb_index_3y" else "滾動分位"
                    print(f"   [PE分位] {code} {_tag}={_rp:.2f} ({_rmeta.get('status')})")
                elif _rmeta.get("status") not in ("skip", "no_proxy_secid", "no_funddb_index_mapping"):
                    print(f"   [PE分位] {code} {_rmeta.get('status')}")
            except Exception as _e:
                _log_err(_e, f"refresh_pe_wire_{code}")

            order_str, delta_amt, warn = compute_order(
                code, info, action, ev, kelly_pct_for_order, _current_styles_pct,
                risk_off_level=_risk_off_level,
                _dcap_trend=_dc_trend, _dcap_flow5=_dc_flow5,
                _dcap_rsi=_dc_rsi, _dcap_frag=_dc_frag, _dcap_val=_dc_val
            )
            # ═══════════════════════════════════════════════════════════
            # [V22.20 新增] P0 三層防護 — 治 compute_order 'Pred單因子獨裁'
            # ═══════════════════════════════════════════════════════════
            _veto_note = ""
            _is_reduce_order = delta_amt < 0  # 實際是減倉指令
            # [V22.20.1 邊界修] 預判grade:背離(actionable)是中線信號,豁免第1層
            _pre_grade = "none"
            try:
                # [V22.36] ★落庫【分級前】原始 action(persistence 的資料來源),再讀 streak 當第三把鑰匙
                save_raw_signal(code, info.get("name", code), r_type, action)
                _rstreak = read_reduce_streak(code, DCAP_REDUCE_STREAK_N)
                _pre_grade, _, _ = grade_action_signal(
                    action, style, holding_divergence, _upcoming_event_styles,
                    reduce_streak=_rstreak,
                    flow5_yi=_dc_flow5, flow5_prev=_dc_flow5_prev,   # [V22.61] 直用變數;locals().get 會把「不存在」偽裝成 None
                    trend=_dc_trend, rsi=_dc_rsi, fragility=_dc_frag, ev=ev)
            except Exception as _e:
                _log_err(_e, "pre_grade")
            try:
                # ── 第1層: 反向證據制衡(治黃金) ──
                # 減倉指令遇到強反向證據 → 否決, 降為觀望
                # [V22.22.4 修bug] 只有'真資金背離'豁免第1層, '非對稱事件放行'不豁免
                #   根因: 舊版用 _pre_grade=="actionable"豁免, 但actionable有兩種:
                #     ①真背離(holding_divergence有值)→該豁免(中線信號強)
                #     ②非對稱事件放行(減倉+CPI)→不該豁免(仍要過反向證據)
                #   黃金CPI日被判②, 卻豁免了第1層, 導致RSI34超賣+溢價166%沒被攔
                _true_divergence = bool(holding_divergence)  # 真背離才豁免
                if _is_reduce_order and not _true_divergence:
                    _reasons = []
                    # 證據1: 資金強流入(自適應數量級)
                    if _flow_h and isinstance(_flow_h, dict):
                        _f5 = _flow_h.get("flow5_yi", 0)
                        _scale = _flow_h.get("scale", "etf")
                        _thr = 3.0 if _scale == "index" else 1.0
                        if _f5 >= _thr:
                            _reasons.append(f"5日資金+{_f5:.1f}億強流入")
                    # 證據2: RSI超賣+反彈傾向
                    if rsi <= 35:
                        _reasons.append(f"RSI={rsi:.0f}超賣(反彈傾向)")
                    # 證據3: 高溢價(NAV可能補漲), premium為小數(0.05=5%)
                    _prem2 = proxy_meta.get("premium_info", {}) if isinstance(proxy_meta, dict) else {}
                    _prem_v = _prem2.get("premium", 0) if isinstance(_prem2, dict) else 0
                    if isinstance(_prem_v, (int, float)) and _prem_v > 0.02:
                        _reasons.append(f"溢價{_prem_v:.1%}(NAV或補漲)")
                    # 有反向證據 → 否決減倉
                    if _reasons:
                        # [V22.59] 同一原則:系統可以【不建議】,但不該【剝奪選擇】。
                        #   第1層與第2層都不再抹掉金額;差別在措辭:
                        #   第1層【有反向證據】→ 明說是什麼證據;第2層【證據不足】→ 明說差幾分。
                        _veto_note = "、".join(_reasons)
                        order_str = (f"⚖️ 系統不建議減倉(**有反向證據**:{_veto_note})"
                                     f"｜**仍可執行:{order_str}**")
                        _user_optional = True
                        warn = False
            except Exception as _ve:
                _log_err(_ve, "veto_layer1")

            if "_user_optional" not in dir():
                _user_optional = False   # [V22.59] 由兩層否決共用的旗標
            # ═══════════════════════════════════════════════════════════
            # [V22.22.6 修#14殘留] 明細卡片「建議操作」必須顯示否決後最終決定
            # 根因: 本行(原L5639)印 order_str 在第2層否決(下方)之前執行,
            #   導致「建議賣出832元」與下方「分級否決:不執行」並存(左手喊賣右手說別動)。
            #   V22.20第3層只修了報告頂部操作清單,漏修每檔明細卡片。
            # 修法: 用已算好的 _pre_grade 預判第2層/待收盤確認結果,
            #   若最終不執行 → 改寫 order_str 顯示最終決定, 原始Kelly數字降級進括號。
            #   不在此改 delta_amt(讓下方各層正常走並補印「為何不執行」的說明)。
            if not _veto_note:  # 第1層已否決者 order_str 已是「維持現倉」, 跳過
                _will_veto_reduce = (_pre_grade == "watch" and delta_amt < 0)
                _will_veto_buy    = (_pre_grade == "hold"  and delta_amt > 0)
                # [V22.36 治時序自我矛盾] 原本早報+午報減倉都→「待收盤確認」,但收盤報 17:37 跑,
                #   15:00 截單早過 → 等於當日永遠不可執行。午報=使用者定義的【最後執行窗】,不得再延。
                #   故 pending 只保留給【早報】(延到午報仍可執行);午報/收盤走明確指令分支。
                _will_pending     = (r_type == "早報" and delta_amt < 0
                                     and not _will_veto_reduce)
                if _will_veto_reduce or _will_veto_buy:
                    # [V22.59] 原文案「系統最終決定,原始信號已否決」與新設計矛盾:
                    #   系統不背書 ≠ 系統替你否決。金額仍可執行,措辭必須反映這件事。
                    order_str = (f"⚖️ 系統評分未達門檻·**不背書但可執行** → "
                                 f"見下方分數說明｜**可執行:{order_str}**")
                    warn = False
                elif _will_pending:
                    order_str = (f"⏳ 待午報確認（午報為最後執行窗，將給明確指令 → 見下方說明｜"
                                 f"原始：{order_str}）")
                    warn = False
            warn_prefix = "⚠️ " if warn else ""
            section += f"> 💡 **建議操作**：{warn_prefix}{order_str}\n"
            # [V22.19/V22.20] 操作建議分級 + 第2,3層防護
            try:
                _grade, _gicon, _gnote = _pre_grade, "", ""
                _g2, _gicon, _gnote = grade_action_signal(
                    action, style, holding_divergence, _upcoming_event_styles,
                    reduce_streak=(locals().get("_rstreak") or 0),   # [V22.36] 兩呼叫點都要傳
                    flow5_yi=locals().get("_dc_flow5"),
                    flow5_prev=_dc_flow5_prev,   # [V22.61] 直用變數;locals().get 會把「不存在」偽裝成 None
                    trend=locals().get("_dc_trend"), rsi=locals().get("_dc_rsi"),
                    fragility=locals().get("_dc_frag"), ev=ev)      # [V22.58] 證據入公式
                _grade = _g2  # 與第1層預判一致(同函數同輸入)
                _user_optional = False   # [V22.59] watch減倉時改 True,金額保留由用戶裁決
                _already_listed = False  # [V22.61] 每檔重置,防跨檔殘留
                # ── 第2層: 分級否決權(治'左手喊賣右手說別動') ──
                # [V22.22] 配合非對稱過濾:
                #   watch減倉(純噪音) → 否決不執行
                #   hold買入(事件前追高) → 否決不執行
                if _grade == "watch" and delta_amt < 0:
                    # [V22.59 治「通知減倉仍不執行」] ★使用者兩次指出同一件事,根因在這兩行:
                    #   ① 文案寫死「純單日信號」—— 與 V22.58 公式輸出【矛盾】
                    #      (公式已算出 20/50、覆蓋78%、缺哪些因子,這行卻還在講單日噪音)。
                    #   ② `delta_amt = 0` —— **把可執行金額抹掉**,選項直接消失。
                    #   使用者原話:「就算你說不動我也會看情況」→ 系統可以【不建議】,
                    #   但不該【剝奪選擇】。分數未達門檻 = 系統不背書,不等於不准做。
                    # 修法:保留金額,改標「待你裁決」,並在摘要與清單分開計數。
                    section += (f"> {_gicon} **操作分級**：{_gnote}\n"
                                f"> ⚖️ **系統不建議,但選擇權在你**：評分未達門檻,故系統不背書;"
                                f"金額已保留於下方【原始】欄,**你判斷該賣即可執行**"
                                f"(場外 15:00 前有效)\n")
                    _user_optional = True          # ← 不歸零,改標記
                elif _grade == "hold" and delta_amt > 0:
                    section += (f"> {_gicon} **操作分級**：{_gnote}\n"
                                f"> 🛑 **分級否決**：事件臨近，已攔截買入追高，不執行\n")
                    delta_amt = 0
                # ── 第3層: 修分級錯位(治#14維持現倉卻顯示可操作) ──
                elif _grade == "actionable" and delta_amt == 0:
                    pass  # 維持現倉不給可操作標籤(避免#14錯位)
                elif _gnote:
                    section += f"> {_gicon} **操作分級**：{_gnote}\n"
            except Exception as _ge:
                _log_err(_ge, "signal_grade")
            # ── [V22.20 P2] 收盤確認錨 ──
            # 早報/午報(盤中,數據不完整)的減倉 → 改'待收盤確認',不催操作
            # 場外ETF按收盤淨值成交,盤中數據是半成品,操作應集中盤前報/收盤報
            try:
                if r_type in ("早報", "午報", "收盤報") and delta_amt < 0:
                    # [V22.36] ★三分支取代原「一律待收盤確認」(該寫法邏輯自我矛盾:收盤報在截單後才跑)
                    if r_type == "早報":
                        section += (f"> ⏳ **待午報確認**：開盤數據未定型；"
                                    f"**午報(15:00截單前)為最後執行窗,屆時將給明確指令**\n")
                    elif r_type == "午報":
                        # [V22.36.1 治我自己的二次關門 bug] ★能走到這裡的減倉,delta_amt<0 代表
                        #   【已通過閘一】(grade=watch 者上方已被歸零,到不了這裡)。
                        #   原本我在此又用 streak 重關一次 → 把已核准信號再擋一遍 = 換湯不換藥。
                        #   實證(7/20 午報 V22.35):006479/018967 分級已是「🔴可操作·防禦優先放行」,
                        #   卻仍被閘二判「待收盤確認」→ 0檔可執行。修法:此處【一律給可執行指令】。
                        _rs_now = locals().get("_rstreak") or 0
                        _rs_txt = (f"(持續性佐證:近{DCAP_REDUCE_STREAK_N}日{_rs_now}日偏空)"
                                   if _rs_now >= DCAP_REDUCE_STREAK_N else
                                   f"(持續性:近{DCAP_REDUCE_STREAK_N}日僅{_rs_now}日偏空;本信號主要來自事件/資金背離)")
                        section += (f"> 🔴 **午報=最後執行窗·建議減倉,15:00 截單前可執行**{_rs_txt}\n"
                                    f"> ➡️ 本檔已通過分級閘(非單日噪音),**不再延後**;"
                                    f"若選擇不動,請視為主動決定,而非系統待確認\n")
                    else:  # 收盤報
                        section += (f"> ⏭️ **標記 T+1 執行**：今日 15:00 截單已過,"
                                    f"此減倉建議最快**明日 15:00 前下單、成交於明日收盤淨值**,"
                                    f"損益取決於**後天(T+2)**的漲跌,不是明天(誠實標示,不假裝今日可執行)\n")
                    # [V22.22.1] 待確認的減倉也記入清單(標pending),避免清單顯示'無操作'誤導
                    _cur_amt_p = USER_HOLDINGS.get(code, 0)
                    # [V22.42 治「顯示已修·清單沒修」] ★7/20 午報實證:逐檔已印
                    #   「🔴午報=最後執行窗·建議減倉,15:00截單前可執行」,摘要卻仍寫「0 檔可執行」
                    #   → 因本處【一律】標 pending=True 且 delta_amt 歸零,V22.36.1 只改了敘述文字。
                    #   同族於缺陷A(算了不顯示):這次是【顯示了但清單/計數沒跟上】。
                    #   修法:午報=最後執行窗 → pending=False 且【保留 delta_amt】(真正可執行);
                    #         早報(待午報)/收盤報(T+1)維持 pending。
                    _is_noon_exec = (r_type == "午報")
                    _action_list.append({
                        "code": code, "name": info.get("name", code)[:12],
                        "act": ("賣出/減倉(待午報確認)" if r_type=="早報" else
                                ("賣出/減倉(T+1執行)" if r_type=="收盤報" else
                                 "賣出/減倉(15:00前可執行)")),  # [V22.42]
                        "delta": delta_amt,
                        "cur": _cur_amt_p, "new": _cur_amt_p + delta_amt,
                        "pending": (not _is_noon_exec),
                        "user_optional": locals().get("_user_optional", False),  # [V22.59]
                    })
                    _already_listed = True   # [V22.61] 防第二個 append 重複計數(見下)
                    # [V22.61 治WB次要點:四報不一致] 原本早報/收盤報在此歸零 →
                    #   下方「現倉X→操作後Y」明細行(`if delta_amt != 0`)被抑制,
                    #   午報有印、收盤報沒印,同一份系統四報格式不一致。
                    #   金額本來就已進 _action_list(上方 append),此處歸零只影響【顯示】。
                    #   改為保留 delta 供明細行使用,以 _suppress_exec 標記「非本報可執行」。
                    _suppress_exec = (not _is_noon_exec)
            except Exception as _ce:
                _log_err(_ce, "close_confirm_anchor")
            # [V22.61] ★修兩件事:
            #  ① 四報一致(WB 次要點):原本早報/收盤報在上方把 delta 歸零 → 此處 `delta_amt != 0`
            #     為 False → 少印「現倉X→操作後Y」明細行,午報卻會印。改為不歸零、四報都印。
            #  ② ★防重複計數(我追查①時發現的既有 bug,V22.42 起潛伏):
            #     P2 區(L9901)已 append 過減倉;午報因 V22.42 保留 delta,
            #     走到此處會【再 append 一次】→ 摘要重複計數。加 _already_listed 擋掉。
            if delta_amt != 0:
                cur_amt = USER_HOLDINGS.get(code, 0)
                new_amt = cur_amt + delta_amt
                _tag = "(本報不執行,僅供參考)" if locals().get("_suppress_exec") else ""
                section += (f"> 　 現倉 {cur_amt:.0f} 元 → "
                            f"操作後 {new_amt:.0f} 元 "
                            f"（{'+' if delta_amt>0 else ''}{delta_amt:.0f}）{_tag}\n")
                if not locals().get("_already_listed"):
                    _act_word = "買入/加碼" if delta_amt > 0 else "賣出/減倉"
                    _action_list.append({
                        "code": code, "name": info.get("name", code)[:12],
                        "act": _act_word, "delta": delta_amt,
                        "cur": cur_amt, "new": new_amt, "pending": False,
                        "user_optional": locals().get("_user_optional", False),
                    })
        except Exception as _oe:
            section += f"> 💡 操作指令計算失敗: {_oe}\n"

        final_report += section
        # ─ [V22.29] T+1 預測(四報因果鏈:今日收盤 → 明日盤勢) ─────────
        _t1_pred, _t1_conf, _t1_reason, _t1_contrib = (0.0, "低", "未啟用", {})
        if T1_PREDICT_ENABLED:
            try:
                _fl = locals().get("_flow_h") or {}
                _t1_pred, _t1_conf, _t1_reason, _t1_contrib = predict_tomorrow(
                    rsi=rsi,
                    z_score=(tech or {}).get("Z_Score", 0),
                    trend=(tech or {}).get("Trend_Strength", 0),
                    main_flow_yi=((locals().get("_bd") or {}).get("主力") or 0),
                    flow5_yi=(_fl.get("flow5_yi") or 0) if isinstance(_fl, dict) else 0,
                    fragility=locals().get("fragility_score", 0) or 0,
                    today_ret=predicted_ret / 100.0 if predicted_ret else 0,
                    has_event=bool(locals().get("_upcoming_event_styles")),
                    news_score=(locals().get("_news_map") or {}).get(code, (0.0, ""))[0],  # [V22.33.5] 新聞進T+1(占比15%,四報)
                )
            except Exception as _e:
                _log_err(_e, f"t1_predict_{code}")

        # [V24.19] ★補抓持倉 flow5。根因:flow5 原本【只有觀察池】在抓(L8519),
        #   持倉 results 從不存 flow5 → `_flow_map` 全檔 0 處賦值、4 處 locals().get 讀取
        #   → 永遠 None → 反轉三條件①「機構回補」永遠 ❌、資金主體識別 inst_5d_yi 永遠 None。
        #   **這不是「東財封鎖拿不到」,是這個變數根本沒被建立過。**
        _h_flow5 = None
        try:
            _hp = info.get("proxy_flow") or info.get("proxy")
            if _hp:
                _h_flow5 = _flow5_with_manual(
                    code, _hp, DirectDataFetcher.get_etf_5day_flow(_hp))
        except Exception as _e:
            _log_err(_e, f"holding_flow5:{code}")
        results.append({
            "t1_contrib": _t1_contrib,   # [V24.30] 各因子 pp 貢獻(供統一因子表)
            "flow5": _h_flow5,   # [V24.19] 供 _flow_map 聚合
            "code": code, "name": info["name"], "style": style,
            "ev": ev, "action": action, "predicted_ret": predicted_ret,
            "tech": tech, "kelly": kelly,
            # [V22.29] T+1 預測(四報因果鏈核心)
            "t1_pred": _t1_pred, "t1_conf": _t1_conf, "t1_reason": _t1_reason,
            "fragility": locals().get("fragility_score", 0) or 0,
            # [V22.16 5/27 新增] 宏觀先驗資料（GEMINI/P0）
            "macro_prior": round(macro_prior, 2),
            "ev_before_macro": ev_before_macro,
            # [V22.18] 資金流向背離示警
            "flow_divergence": holding_divergence,
        })
        time.sleep(0.5)

    # [V22.11] 跨報信號一致性檢查（收盤報）
    consistency_block = ""
    if r_type == "收盤報":
        today_str2 = (run_date if is_backfill else datetime.date.today()).isoformat()
        reversals = []
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            for code, info in TARGET_ETFS.items():
                rows = conn.execute("""
                    SELECT action, ts FROM predictions
                    WHERE code=? AND date(ts)=?
                    ORDER BY ts ASC
                """, (code, today_str2)).fetchall()
                if len(rows) >= 2:
                    first_act, last_act = rows[0][0], rows[-1][0]
                    is_180 = (("主攻" in first_act and "減倉" in last_act) or
                              ("減倉" in first_act and "主攻" in last_act))
                    if is_180:
                        name = info["name"][:8]
                        reversals.append(f"  - **{name}**: 早報={first_act[:6]} → 收盤={last_act[:6]}")
        if reversals:
            consistency_block = (
                "\n## ⚡ 跨報信號180°反轉警示\n"
                "以下ETF今日發生早/收盤信號完全反轉，需重點覆核明日策略：\n"
                + "\n".join(reversals) + "\n"
            )

    # ══ [Kalman] 收盤後自我學習 ═══════════════════════
    if r_type == "收盤報" and results:
        print("\n[Kalman] 收盤自我學習...")
        brain = KalmanLearner.update(brain, results)

    # ══ 組合報告 ═══════════════════════════════════════
    concentration = ReportFormatter.format_concentration_alert(
        KellyEngine.style_concentration_check(results))

    # ══ [V22.17.3 新增] 同質群組信號衝突警示 ══
    # 根因：5/29 011613 vs 018177 同質性 95% 但被我差別處理
    # ══ [V22.17.4 新增] 持倉戰局摘要區塊（早午晚報共用） ══
    portfolio_battle_block = ""
    try:
        # [V22.18] 持倉每檔抓5日資金流向(情報輸入層,全持倉不只觀察池)
        _flow_cache = {}
        for code in USER_COSTS:
            info_ = TARGET_ETFS.get(code, {})
            _proxy = info_.get("proxy")
            if _proxy and code not in _flow_cache:
                try:
                    _flow_cache[code] = DirectDataFetcher.get_etf_5day_flow(_proxy)
                except Exception:
                    _flow_cache[code] = None

        groups = {}  # {(priority, label): [lines]}
        for code, cost in USER_COSTS.items():
            info_ = TARGET_ETFS.get(code, {})
            name = info_.get("name", code)[:10]
            pct = cost["pnl_pct"]
            # [V22.18] 5日資金流向標註
            _f = _flow_cache.get(code)
            flow_tag = ""
            if _f:
                flow_tag = f" 💰5日{_f.get('flow5_yi', 0):+.1f}億{(_f.get('signal','') or '')[:2]}"
            line = f"* `{code}` {name} {pct:+.1f}% ({cost['total_pnl']:+.0f} 元){flow_tag}"
            grp_key = classify_position_status(pct)
            groups.setdefault(grp_key, []).append(line)
        
        portfolio_battle_block = f"\n## 💰 持倉戰局摘要（{len(USER_COSTS)} 檔含已實現獲利+5日資金）\n"
        for grp_key in sorted(groups.keys()):
            portfolio_battle_block += f"\n### {grp_key[1]}\n" + "\n".join(groups[grp_key]) + "\n"
        # 總戰績
        total_pnl = sum(c["total_pnl"] for c in USER_COSTS.values())   # 絕對元收益(真實)
        total_current = sum(c["current"] for c in USER_COSTS.values()) # 當前總市值(真實)
        # [V22.23.7] 組合%改近1月市值加權(取代churn失真的 total_pnl/total_buy)。
        #   舊算法分母=各檔反推成本和, 被黃金殘餘成本367扭曲→+66%幻象。
        #   元收益(絕對)永遠真實; %改「近1月加權」與單檔一致, 反映組合現況。
        if total_current > 0:
            port_1m = sum(c["current"] * c.get("pnl_pct", 0.0)
                          for c in USER_COSTS.values()) / total_current
            pct_str = f"近1月加權 {port_1m:+.1f}%"
        else:
            pct_str = "N/A"
        portfolio_battle_block += (f"\n> **組合總獲利**: {total_pnl:+,.0f} 元 "
                                    f"({pct_str}) | 當前總市值 {total_current:,.0f} 元\n\n")
        # [V22.27.2] 條件β脫鉤監測(早午收共用區塊;走USER_COSTS確保拿得到持倉)
        portfolio_battle_block += render_conditional_beta_block(list(USER_COSTS.keys()))
        portfolio_battle_block += "<br>" + render_ic_scorecard(list(USER_COSTS.keys()), market="A股")
    except Exception as _e:
        _log_err(_e, "portfolio_battle_block")

    similarity_block = ""
    try:
        result_map = {r.get("code"): r for r in results}
        conflicts = []
        for grp_name, codes in SIMILARITY_GROUPS.items():
            in_group = [c for c in codes if c in result_map]
            if len(in_group) < 2:
                continue
            # 抓每檔信號（用 action 簡化判斷）
            sig_map = {}
            for c in in_group:
                act = result_map[c].get("action", "")
                if "主攻" in act: sig_map[c] = "🟢"
                elif "減倉" in act: sig_map[c] = "🔴"
                elif "清倉" in act: sig_map[c] = "💥"
                else: sig_map[c] = "🟡"
            unique_sigs = set(sig_map.values())
            if len(unique_sigs) > 1:
                # 同群組信號衝突！
                conflicts.append((grp_name, sig_map))
        if conflicts:
            similarity_block = "\n## ⚠️ 同質群組信號衝突警示\n\n"
            similarity_block += "> 以下 ETF 群組同質性 ≥85%，但出現不同信號，建議統一處理：\n\n"
            for grp, smap in conflicts:
                lines = " / ".join(f"{c} {s}" for c, s in smap.items())
                similarity_block += f"- **{grp}群**: {lines}\n"
            similarity_block += "\n> 💡 建議：同質性 ≥85% ETF 應同步減倉或同步保留\n"
            _conf_codes = "、".join(smap.keys())
            similarity_block += (f"> 💡 5/29 教訓:同質群組差別處理事後常證明邏輯失誤;"
                                 f"本次衝突標的 {_conf_codes} → 應統一減倉或統一保留。\n\n")
    except Exception as _e:
        _log_err(_e, "similarity_block")

    pnl_block = ""
    market_validation_rows = []  # [V22.17.3] A股當日 actual 命中表
    if r_type == "收盤報":
        # [V22.17.1 新增 5/29] 台股當日 actual 即時補寫（不需 T+1）
        try:
            PerformanceAttributor.backfill_taiwan_today_actuals()
        except Exception as e:
            print(f"   ⚠️ 台股當日補寫失敗（不影響報告）: {e}")
        
        # [V22.17.3 新增 5/29] A股當日 actual 即時補寫（sina 場內代理）
        # 根因：用戶 Q4 提案 — 17:30 場內 ETF 已收盤，當天就能驗證
        market_validation_rows = []
        try:
            _n, market_validation_rows = PerformanceAttributor.backfill_a_stock_today_actuals()
        except Exception as e:
            print(f"   ⚠️ A股當日補寫失敗（不影響報告）: {e}")
        
        pnl_block = PerformanceAttributor.daily_pnl_report(
            results, (run_date if is_backfill else datetime.date.today()).isoformat())

    # [V22.9] 四報校正鏈（收盤報）
    # [V22.17.3 新增] A股當日 系統 pred vs 真實 命中表
    market_validation_block = ""
    if r_type == "收盤報" and market_validation_rows:
        try:
            lines = [
                "\n## 📊 系統 pred vs 真實 當日命中表（A股 web 即時驗證）\n",
                "> 數據源：sina hq.sinajs.cn 場內 ETF 代理（17:00 後可用）\n",
                "",
                "| ETF | 代理 | 系統 pred | 真實 | 偏差 | 命中 |",
                "|-----|------|----------|------|------|------|",
            ]
            hit = miss = 0
            for r in market_validation_rows:
                diff = (r["actual"] - r["pred"]) * 100
                lines.append(
                    f"| {r['name']} ({r['code']}) | {r['proxy']} | "
                    f"{r['pred']*100:+.2f}% | {r['actual']*100:+.2f}% | "
                    f"{diff:+.2f}pp | {r['hit']} |"
                )
                if r["hit"] == "✅": hit += 1
                elif r["hit"] == "❌": miss += 1
            total = hit + miss
            _nopred = len(market_validation_rows) - total
            if _nopred:
                lines.append("")
                lines.append(f"> ⬜ {_nopred} 檔當日沒有盤前(早報)預測,命中欄標「—」且不計入命中率")
            if total > 0:
                lines.append("")
                lines.append(f"> 📊 方向命中: {hit}/{total} = {hit/total*100:.0f}%")
                lines.append(f"> ⏱️ T+0 即時驗證（取代 T+1 backfill）")
                lines.append(f"> 💡 V22.17.3 新功能：當日就能評估系統可信度")
            market_validation_block = "\n".join(lines) + "\n"
        except Exception as _e:
            _log_err(_e, "market_validation_block")

    correction_chain_block = ""
    if r_type == "收盤報":
        today_str = (run_date if is_backfill else datetime.date.today()).isoformat()
        _chain_rows = []   # [V24.30]
        chain_lines = [
            "\n## 📊 四報校正鏈 | 早報 → 午報 → 收盤驗證\n",
            "| ETF | 早報預測 | 午報預測 | 收盤實際 | 誤差 | 方向 |",
            "|-----|---------|---------|---------|------|------|",
        ]
        with sqlite3.connect(LOG_DB, timeout=10) as conn:
            for code, info in TARGET_ETFS.items():
                name = info["name"][:8]
                # [V22.16 5/27 P1 修] 改用 report_type 區分四報
                # 舊邏輯：按時間區段判斷（不準，因為 DB UPDATE 模式只有 1 筆）
                # 新邏輯：直接查 report_type='早報'/'午報' 對應記錄
                # 兼容舊資料：如果新 schema 查不到，fallback 用舊的時間區段
                morning = conn.execute(
                    "SELECT ROUND(predicted_ret*100,2) FROM predictions "
                    "WHERE code=? AND date(ts)=? AND report_type='早報' "
                    "ORDER BY ts DESC LIMIT 1",
                    (code, today_str)).fetchone()
                if not morning or morning[0] is None:
                    # Fallback: 舊資料用時間區段
                    morning = conn.execute(
                        "SELECT ROUND(predicted_ret*100,2) FROM predictions "
                        "WHERE code=? AND date(ts)=? AND (report_type='' OR report_type IS NULL) "
                        "AND replace(ts,'T',' ')<? "
                        "ORDER BY ts ASC LIMIT 1",
                        (code, today_str, f"{today_str} 10:30:00")).fetchone()

                noon_row = conn.execute(
                    "SELECT ROUND(predicted_ret*100,2) FROM predictions "
                    "WHERE code=? AND date(ts)=? AND report_type='午報' "
                    "ORDER BY ts DESC LIMIT 1",
                    (code, today_str)).fetchone()
                if not noon_row or noon_row[0] is None:
                    # Fallback: 舊資料用時間區段
                    noon_row = conn.execute(
                        "SELECT ROUND(predicted_ret*100,2) FROM predictions "
                        "WHERE code=? AND date(ts)=? AND (report_type='' OR report_type IS NULL) "
                        "AND replace(ts,'T',' ')>=? AND replace(ts,'T',' ')<=? "
                        "ORDER BY ts ASC LIMIT 1",
                        (code, today_str, f"{today_str} 12:00:00",
                         f"{today_str} 15:30:00")).fetchone()

                actual = conn.execute(
                    "SELECT ROUND(actual_ret*100,2) FROM predictions "
                    "WHERE code=? AND date(ts)=? AND actual_ret IS NOT NULL "
                    "AND ABS(actual_ret)>0.0001 ORDER BY corrected DESC LIMIT 1",
                    (code, today_str)).fetchone()

                m = f"{morning[0]:+.2f}%" if morning and morning[0] is not None else "——"
                n = f"{noon_row[0]:+.2f}%" if noon_row and noon_row[0] is not None else "——"
                a = f"{actual[0]:+.2f}%" if actual and actual[0] is not None else "T+1待確認"

                if actual and actual[0] is not None and morning and morning[0] is not None:
                    err = abs(actual[0] - morning[0])
                    m_dir = morning[0] > 0
                    a_dir = actual[0] > 0
                    dir_icon = "✅" if m_dir == a_dir else "❌"
                    err_str = f"{err:.2f}%"
                else:
                    dir_icon = "—"; err_str = "—"

                chain_lines.append(f"| {name} | {m} | {n} | {a} | {err_str} | {dir_icon} |")
                # [V24.30] 收集三元組供偏誤稽核(不只記錄,要診斷)
                if (actual and actual[0] is not None and morning and morning[0] is not None
                        and noon_row and noon_row[0] is not None):
                    _chain_rows.append((name, morning[0], noon_row[0], actual[0]))
        correction_chain_block = "\n".join(chain_lines) + "\n"
        # [V24.30] ★把「記錄」升級為「診斷」:算偏誤方向並歸因
        correction_chain_block += render_forecast_bias_audit(_chain_rows)

    outlook_block = (OutlookTeam.build_tomorrow_outlook(
        results, vix, strait_status, c_drain, gold, wti)
        if r_type == "收盤報" else "")

    # ═══ [V22.29] 收盤報:T+1 預測表 + 落庫 + morning_premise(因果鏈的錨) ═══
    t1_block = ""
    # [V22.30] 盤前報「今日盤勢預測」已移至 build_premarket_forecast,由盤前報流程呼叫
    #   (原本寫在這裡,但盤前報在前段就 return,永遠不會執行)。
    premise_block = ""

    # ═══ [V22.32] 早報:資金流主體識別 — 誰在買?誰在賣? ═══
    flow_actor_block = ""
    if r_type == "早報" and FLOW_ACTOR_ENABLED:
        try:
            _fa = ["\n## 🎭 資金流主體識別 — 誰在買?誰在賣?\n",
                   "> 📌 **早報核心**(開盤1.5-2h,第一波廝殺後漸穩):",
                   "> 看**機構 vs 散戶**的走向 → 判斷上半場走勢。",
                   "> ",
                   "> 🚨 **背離才是信號**:機構買+散戶賣=**吸籌**(最強看多);",
                   "> 機構賣+散戶買=**經典出貨**(最強看空)。",
                   "> ",
                   "> ⚠️ **局限(必讀)**:東財分類按**成交金額**非帳戶類型;",
                   "> 機構可用算法拆單偽裝成中小單,量化高頻會污染信號 → **須結合價格行為交叉驗證**。",
                   "> ⚠️ 校準期(observe-only),未經真數據驗證(E-LOG #258)。\n",
                   "| ETF | 主體判讀 | 強度 | 依據 |",
                   "|---|---|---|---|"]
            _fa_hdr_i = len(_fa) - 3   # [V24.3] 待回填「今日總結」的位置(教學文字之後、表頭之前)
            _strong = []
            for r in results:
                _c = r.get("code")
                _bdm = (locals().get("_bd_map") or {}).get(_c) or {}
                _nm = r.get("name", "")[:9]
                if not _bdm:
                    # [V24.1 修] 東財 fflow 結構性封鎖 → 16 檔全印「資料未取得」,整塊實質萬年不變。
                    #   改先走 V23.8 的量價代理(新浪/騰訊可達),真的抓不到才印未取得。
                    _pxy = (TARGET_ETFS.get(_c) or {}).get("proxy")
                    _vp_txt, _vp_ok = (None, False)
                    if _pxy and (TARGET_ETFS.get(_c) or {}).get("market") != "台股":
                        try:
                            _vp_txt, _vp_ok, _vp_val = _volume_flow_proxy(_pxy)   # [V24.19]
                        except Exception:
                            _vp_ok = False
                    if _vp_ok and _vp_txt:
                        _fa.append(f"| {_nm} | {_vp_txt} | — | 東財大單封鎖→量價代理(非主力淨額,僅示方向) |")
                    else:
                        _fa.append(f"| {_nm} | ⬜ 資料未取得 | 0 | 東財主體分解與量價代理雙源皆未取得 |")
                    continue
                _sig, _st, _rs = identify_flow_actors(
                    inst_yi=_bdm.get("超大單", 0),
                    retail_yi=_bdm.get("小單", 0),
                    price_ret_pct=r.get("predicted_ret", 0) or 0,
                    inst_5d_yi=(locals().get("_flow_map") or {}).get(_c),
                )
                _fa.append(f"| {_nm} | {_sig} | {_st:+d} | {_rs[:70]} |")
                if abs(_st) >= 2:
                    _strong.append(f"- **{_nm}** {_sig} — {_rs}")
            # [V24.3] 治「⑤頂部教學文字萬年不變」:表格算完後回填今日動態總結,讓這塊每天不同
            try:
                _in_n = sum(1 for _r in _fa if "淨流入傾向" in _r)
                _out_n = sum(1 for _r in _fa if "淨流出傾向" in _r)
                _na_n = sum(1 for _r in _fa if "資料未取得" in _r)
                if (_in_n + _out_n) > 0:
                    _tilt = ("資金偏多(流入多於流出)" if _in_n > _out_n else
                             "資金偏空(流出多於流入)" if _out_n > _in_n else "多空均衡")
                    _sum_line = (f"> 📊 **今日主體判讀總結**：淨流入傾向 {_in_n} 檔 / 淨流出傾向 {_out_n} 檔"
                                 f"{f' / 未取得 {_na_n} 檔' if _na_n else ''} → **{_tilt}**。"
                                 f"(東財大單封鎖期間以量價代理判讀,非主力淨額)\n")
                    _fa.insert(locals().get("_fa_hdr_i", 0) or 0, _sum_line)
            except Exception as _e:
                _log_err(_e, "get_rs_label:L13147")  # [V24.16] 原為靜默 pass
                pass
            if _strong:
                _fa.append("\n### 🚨 強背離信號(聰明錢 vs 笨錢)\n")
                _fa.extend(_strong[:6])
            # [V22.35] ★國家隊/主權資金判讀 —— 補使用者早報定義缺的「主權基金、國家操盤單位走向」。
            #   ⚠️ 行為特徵推斷,非直接識別(A股無公開即時國家隊持倉);故用「跡象/疑似」措辭。
            try:
                _bs = None
                for _bc in ("510300", "510050", "510500"):     # 滬深300/上證50/中證500
                    _bf = (locals().get("_flow_map") or {}).get(_bc)
                    if _bf is not None:
                        _bs = (_bs or 0) + float(_bf)
                _ssig, _sst, _srs = identify_state_actor(
                    broad_etf_flow_yi=_bs,
                    index_ret_pct=(locals().get("_idx_ret_pct") or 0.0),
                    inst_yi=sum((v or {}).get("超大單", 0) for v in (locals().get("_bd_map") or {}).values()),
                    retail_yi=sum((v or {}).get("小單", 0) for v in (locals().get("_bd_map") or {}).values()),
                )
                _fa.append(f"\n### 🏛️ 國家隊 / 主權資金判讀\n")
                _fa.append(f"- {_ssig}")
                _fa.append(f"  - 依據:{_srs}")
                _fa.append("  - ⚠️ **這是行為特徵【推斷】非直接識別**"
                           "(A股國家隊持倉僅季報披露);寬基ETF逆勢大額流入=經典護盤簽名。")
            except Exception as _e:
                _log_err(_e, "state_actor_block")
            _fa.append("\n> ➡️ **因果鏈**:盤前預測(先驗)→ **本表驗證誰在動**(證據)"
                       "→ 午報14:00 決策 → 收盤歸因(學習)。\n")
            flow_actor_block = "\n".join(_fa) if len(_fa) > 13 else ""
        except Exception as _e:
            _log_err(_e, "flow_actor_block")
            flow_actor_block = ""

    # ═══ [V22.31] 收盤報歸因引擎:「今天為什麼漲/跌?」 ═══
    attribution_block = ""
    if r_type == "收盤報" and ATTRIBUTION_ENABLED:
        try:
            _ab = ["\n## 🔍 今日歸因 — 為什麼漲?為什麼跌?\n",
                   "> 📌 **收盤報的核心價值**:不是決策(已無法下單),而是**解釋今天發生了什麼**。",
                   "> 是利多/利空新聞?是買家收網?還是賣家絞殺?理解它,才能預測下次。",
                   "> ",
                   "> 🚨 **殘差(residual)才是重點**:已知因子解釋不了的部分,",
                   "> **就是我們不知道的事**——殘差大 = 有未知因素主導(消息面/大單/情緒),需追。",
                   "> ⚠️ 歸因因子權重仍 observe-only 未校準(見 E-LOG #258;V22.90 已新增 forward IC 量預測技巧,但本歸因權重仍待真數據校準),observe 期僅供解釋。\n",
                   "| ETF | 實際 | 🌎美股 | 💰主力 | 📉技術 | 📰新聞 | 已解釋 | **殘差** |",
                   "|---|---|---|---|---|---|---|---|"]
            _alerts, _behaviors, _att_cache = [], [], {}
            for r in results:
                _c = r.get("code"); _t = r.get("tech") or {}
                _nm = r.get("name", "")[:9]
                _act = r.get("predicted_ret", 0) or 0   # 收盤報時 = 當日實際
                _att = attribute_today(
                    actual_ret_pct=_act,
                    macro_prior=r.get("macro_prior", 0),
                    main_flow_yi=((locals().get("_bd_map") or {}).get(_c, 0)),
                    flow5_yi=((locals().get("_flow_map") or {}).get(_c, 0)),
                    rsi_prev=_t.get("RSI", 50),
                    news_score=(locals().get("_news_score_map") or {}).get(_c, 0),
                    fragility=r.get("fragility", 0),
                    is_qdii=_is_qdii_etf(TARGET_ETFS.get(_c, {})),
                )
                _att_cache[_c] = (_nm, _act, _att)
                _cc = _att["contributions"]
                _ab.append(
                    f"| {_nm} | {_act:+.2f}% | {_cc['🌎 美股傳導']:+.2f} | {_cc['💰 主力資金']:+.2f} "
                    f"| {_cc['📉 技術回歸']:+.2f} | {_cc['📰 產業新聞']:+.2f} "
                    f"| {_att['explained']:+.2f} | **{_att['residual']:+.2f}** |")
                if _att["alert"]:
                    _alerts.append(f"- **{_nm}**:{_att['alert']}")
                if "無明顯" not in _att["behavior"]:
                    _behaviors.append(f"- **{_nm}** {_att['behavior']} — {_att['behavior_why']}")
            if _alerts:
                _ab.append("\n### 🚨 高殘差警報 — 有我們不知道的因素在主導\n")
                _ab.extend(_alerts[:6])
            if _behaviors:
                _ab.append("\n### 🎭 買賣方行為解讀 — 誰在收網?誰在絞殺?\n")
                _ab.extend(_behaviors[:8])
            # [V22.92] 盤面總結：填補「為什麼漲跌」的空白，給全局視角
            if _att_cache:
                _ab.append("\n### 📊 今日盤面總結\n")
                _ab.extend(_build_market_summary(results, _att_cache))
                # [V22.93] 由黑翻紅研判：反抽 or 誘敵（僅收盤報輸出，回答用戶核心疑問）
                if r_type == "收盤報":
                    _ab.extend(_build_reversal_judgment(results, _att_cache, locals().get("_flow_map")))
                    _ab.extend(build_volume_monitor())   # [V23.4 item6] 板塊成交額監測(東財限流替代)
            # [V22.91→V22.92] 敘事歸因:每個標的都有(不只高殘差)
            if _att_cache:
                _ab.extend(_build_attribution_narrative(_att_cache))
                if r_type == "收盤報":
                    _ab.extend(build_daily_conclusion(results, _att_cache))   # [V23.9] 今日結論總結
            _ab.append("\n> ➡️ **因果鏈**:本歸因 → 寫入 `morning_premise` → "
                       "**明日盤前報**(+隔夜美股+新聞)→ 明日預測 → 早報驗證 → 午報14:00決策。\n")
            attribution_block = "\n".join(_ab)
        except Exception as _e:
            _log_err(_e, "attribution_block")
            attribution_block = ""

    _is_decision = is_decision_window(r_type)          # [V22.30] 午報≥13:30 = 決策窗口
    if T1_PREDICT_ENABLED and (r_type == "收盤報" or _is_decision):
        try:
            _today = _now_utc8().date().isoformat()
            _rows, _premise = [], {}
            with sqlite3.connect(LOG_DB, timeout=10) as _conn:
                for r in results:
                    _c = r.get("code"); _tp = r.get("t1_pred", 0)
                    _tc = r.get("t1_conf", "低"); _tr = r.get("t1_reason", "")
                    _tech = r.get("tech") or {}
                    _rows.append((r.get("name","")[:10], _tp, _tc, _tr))
                    _premise[_c] = {"t1_pred": _tp, "conf": _tc, "reason": _tr,
                                    "rsi": _tech.get("RSI"), "frag": r.get("fragility", 0)}
                    save_daily_snapshot(
                        _conn, _c, _today,
                        _tech.get("RSI"), _tech.get("Z_Score"), _tech.get("Trend_Strength"),
                        None, None, r.get("fragility", 0),
                        r.get("predicted_ret"), _tp, _tc)
                save_morning_premise(_conn, _today, _premise)
                if r_type == "收盤報":                       # [V22.80] 收盤才把『今日情報綜合』交棒給明日盤前
                    save_closing_handoff(_conn, _today, results,
                                         news_map=locals().get("_news_map"),
                                         flow_map=locals().get("_flow_map"))
            _rows.sort(key=lambda x: -abs(x[1]))
            if _is_decision:
                t1_block = ("\n## 🎯 【決策窗口】T+1 方向預測 — 15:00 截單前最後行動機會\n\n"
                            "> ⏰ **場外基金 15:00 前下單 = 今日收盤 NAV。此刻是今天最後的買賣時點。**\n"
                            "> 📌 **你今天買進 = 用今日收盤 NAV → 賺不賺錢取決於【明天以後】。**\n"
                            "> **故此表預測 T+1 方向,直接驅動「今天要不要買」。**\n\n"
                            "> ⚠️ **校準期(observe-only)**:歸因權重未經真實數據驗證(見 E-LOG #258;V22.90 已加 forward IC 量預測技巧,但本表權重仍待校準),\n"
                            "> 僅供參考,不自動下單。兩週後用真實 T+1 結果校準。\n\n"
                            "| ETF | T+1預測 | 信心 | 依據 |\n|---|---|---|---|\n")
            else:
                t1_block = ("\n## 🔮 明日(T+1)前提 — 寫給明日盤前報\n\n"
                            "> 📌 本表**不是決策用**(收盤時已無法下單),而是寫入 `morning_premise`,\n"
                            "> 供**明日盤前報**結合隔夜美股 + 產業新聞後,產出明日的今日預測。\n"
                            "> ⚠️ 校準期(observe-only),歸因權重未經真實數據驗證(見 E-LOG #258;V22.90 已加 forward IC 量預測技巧,但本表權重仍待校準)。\n\n"
                            "| ETF | T+1前提 | 信心 | 依據 |\n|---|---|---|---|\n")
            for _n, _tp, _tc, _tr in _rows[:12]:
                _ic = "🔴" if _tp < -0.8 else ("🟢" if _tp > 0.8 else "⚪")
                t1_block += f"| {_n} | {_ic} {_tp:+.2f}% | {_tc} | {_tr} |\n"
            if _is_decision:
                t1_block += ("\n> 🚨 **執行提醒**:場外 ETF 無盤中時效——15:00 前下單皆為今日收盤 NAV。\n"
                             "> 若決定買進,**現在就要下單**;過了 15:00 就是明日 NAV(方向風險多擔一天)。\n")
            else:
                t1_block += ("\n> 📌 **四報因果鏈**:本表 → `morning_premise` → "
                             "**明日盤前報**(+隔夜美股+產業新聞)→ 明日預測 → "
                             "**早報驗證**(果因診斷)→ **午報14:00 決策** → 收盤報檢討。\n")
        except Exception as _e:
            _log_err(_e, "t1_block")
            t1_block = ""

    noon_header = (
        f"\n## 🔄 午報修正基準 | 早報→開盤2H表現\n"
        f"* 📡 市場資金面（午盤）：{market_sentiment.get('summary','—')}\n"
        f"* ⏰ 掃描時間：{datetime.datetime.now().strftime('%H:%M')}\n"
        + (("\n**📋 早報回顧：**\n| ETF | 早報預測 | 早報信號 |\n|-----|---------|----------|\n"
            + "".join(f"| {TARGET_ETFS.get(c,{}).get('name','')[:8]} "
                      f"| {(mp.get('pred') or 0):+.2f}% "
                      f"| {(mp.get('action') or '')[:8]} |\n"
                      for c, mp in noon_preds.items()))
           if noon_preds else "")
    ) if r_type == "午報" else ""

    # [V22.25.7] Path B 警告注入：數據缺失時暴露盲區在報頭
    if _intel_warning:
        market_sentiment["summary"] = _intel_warning + " | " + market_sentiment.get("summary", "")

    header = IntelTeam.format_header(
        vix, ndx_d, sox_d, gold, wti, usd_cny,
        c_drain, strait_status, w_tw, w_a,
        acc.get("台股",{}), acc.get("A股",{}), r_type,
        style_groups=style_groups,
        usd_cny_src=usd_cny_src,
        gold_src=gold_src,
        north_flow_str=north_flow_str,
        market_sentiment=market_sentiment)

    # ── brain.json儲存 ────────────────────────────────
    brain["macro"].update({
        "c_drain": c_drain, "strait_status": strait_status,
        "updated_at": datetime.datetime.now().isoformat(),
    })
    brain["macro"].setdefault("trade_ease_signal", 0)
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(brain, f, indent=4, ensure_ascii=False)

    # ══ [V22.16 新增] 信號統計表（按Style分組） ══
    signal_stats_block = ""
    if r_type in ("午報", "收盤報"):
        try:
            signal_stats_block = ReportFormatter.format_signal_statistics(results)
        except Exception as e:
            print(f"   [V22.16 信號統計] 失敗: {e}")
            signal_stats_block = ""

    # ══ [V22.16 5/27 完整版] 宏觀先驗摘要區塊 ══
    # 把 ETF 主掃描中計算的 macro_prior 按 Style 分組統計
    macro_summary_block = ""
    if r_type in ("早報", "午報", "收盤報") and results:
        try:
            from collections import defaultdict
            style_priors = defaultdict(list)
            for r in results:
                mp = r.get("macro_prior", 0.0)
                evb = r.get("ev_before_macro", r.get("ev", 0))
                ev_now = r.get("ev", 0)
                code = r.get("code", "")
                info = TARGET_ETFS.get(code, {})
                _is_qdii = _is_qdii_etf(info)
                grp = "QDII" if _is_qdii else r.get("style", "?")
                style_priors[grp].append({
                    "code": code, "macro_prior": mp,
                    "ev_before": evb, "ev_after": ev_now,
                    "delta": ev_now - evb,
                })
            if style_priors:
                lines = [
                    "\n## 🌏 宏觀先驗摘要（盤前數據對 EV 的真實影響）\n",
                    "| Style | 代表 ETF | macro_prior | EV 調整 | 解讀 |",
                    "|-------|---------|-------------|---------|------|",
                ]
                for grp, items in sorted(style_priors.items()):
                    if not items:
                        continue
                    # 取絕對值最大的代表
                    rep = max(items, key=lambda x: abs(x["macro_prior"]))
                    name_short = TARGET_ETFS.get(rep["code"], {}).get("name", rep["code"])[:8]
                    interp = (
                        "📈 利多支撐" if rep["macro_prior"] > 0.15 else
                        "📉 利空壓制" if rep["macro_prior"] < -0.15 else
                        "🟡 中性"
                    )
                    lines.append(
                        f"| **{grp}** | {name_short} ({rep['code']}) | "
                        f"{rep['macro_prior']:+.2f} | "
                        f"{rep['ev_before']}→{rep['ev_after']} ({rep['delta']:+d}) | "
                        f"{interp} |"
                    )
                lines.append("")
                lines.append(f"> 算式：`ev = ev × (1 + macro_prior × 0.20)`")
                lines.append(f"> 輸入：SOX={sox_d:+.2%} / NDX={ndx_d:+.2%} / VIX={vix:.2f} / 黃金日漲={gold_ret:+.2%} / CNY={usd_cny:.4f}")
                lines.append(f"> 設計：Tech→SOX+VIX | QDII→NDX+CNY+VIX | Defensive→黃金+VIX | 其他→VIX")
                lines.append("")
                macro_summary_block = "\n".join(lines)
        except Exception as e:
            print(f"   [V22.16 宏觀先驗摘要] 失敗: {e}")
            macro_summary_block = ""

    # ══ [V22.16 新增] 觀察池信號摘要 ══
    watchlist_block = ""
    if r_type in ("早報", "午報", "收盤報") and WATCHLIST_ETFS:
        try:
            print(f"\n   [V22.16 觀察池] 掃描 {len(WATCHLIST_ETFS)} 檔...")
            watchlist_results = WatchlistScanner.scan_watchlist()
            _wn2, _wm2 = save_watchlist_snapshot(watchlist_results)   # [V22.55] 落庫
            print(f"   [觀察池] {_wm2}")
            watchlist_block = WatchlistScanner.format_watchlist_block(watchlist_results)
            ready_count = sum(1 for r in watchlist_results
                              if r.get("entry_signal") == "✨ 達進場條件")
            print(f"   [V22.16 觀察池] {len(watchlist_results)}/{len(WATCHLIST_ETFS)} 檔成功掃描，"
                  f"{ready_count} 檔達進場條件")
        except Exception as e:
            print(f"   [V22.16 觀察池] 整體失敗: {e}")
            watchlist_block = ""

    # [V22.23.9 場內對照] 抓場內實時報價對照(轉場內鋪路, 午/收報跑, 僅記錄)
    if r_type in ("午報", "收盤報"):
        try:
            print(f"\n   [V22.23.9 場內對照] 抓 {len(INTRADAY_PAIRS)} 對場內↔場外...")
            _ic_n = record_intraday_compare(r_type)
            print(f"   [V22.23.9 場內對照] {_ic_n}/{len(INTRADAY_PAIRS)} 對成功記錄")
        except Exception as _e:
            print(f"   [V22.23.9 場內對照] 整體失敗(可能VPN/休市): {_e}")

    # ── 報告寫入（V22.16 5/27 新增：雙輸出 txt + html）─────────
    ts_str = datetime.datetime.now().strftime('%m%d_%H%M')
    # [V22.18.4] 事件日曆加入早/午/收盤報(原本只有盤前報有,WorkBuddy正確指出的缺口)
    _events_block_shared = ""
    try:
        _events_block_shared = format_events_block(load_events_csv())
    except Exception as _e:
        _log_err(_e, "events_block_shared")

    # 訊號健康度閘門:本報類型在該市場近期「照做真正拿得到的報酬」不顯著 → 不背書(金額保留、改列待你裁決)。
    #   機構做法是持續監控訊號實盤表現,表現消失就停止背書;無法驗證時一律不背書(fail-closed)。
    _health_block = ""
    try:
        with sqlite3.connect(LOG_DB, timeout=10) as _hconn:
            _href = returns_store.load_map(_hconn)
        _health = signal_health.compute(_be.load_records(LOG_DB), _href)
        signal_health.gate_actions(_action_list, _health, r_type,
                                   lambda c: TARGET_ETFS.get(c, {}).get("market"))
        _health_block = signal_health.format_block(_health, r_type)
    except Exception as _e:
        _log_err(_e, "signal_health")
        _data_unavailable("SIGNAL_HEALTH", f"{type(_e).__name__}: {_e}")
        for _a in _action_list:
            _a["user_optional"] = True
            _a["act"] = f"{_a.get('act', '')}(健康度無法計算·系統不背書)"

    # [V22.20 P3] 今日操作清單(單獨列出,防止在長詳情卡裡看錯標的)
    _oplist_block = ""
    try:
        if _action_list:
            # [V24.29] ★使用者 2026-08-10:「這種建議在收盤報有何意義?應該在午報就要有吧」——對。
            #   場外聯接基金 **15:00 截單**,收盤報 17:11 產出時今日已無法成交。
            #   標成「今日操作清單」等於叫人做一件今天已經做不到的事。
            #   四報時序:午報(12:00-14:00)才是 15:00 截單前的【最後決策窗】;
            #   收盤報的職責是【歸因 + 寫明日前提】,故其操作建議一律標為【明日】。
            _is_close = (r_type == "收盤報")
            _ttl = ("🌅 明日操作計畫（今日 15:00 已截單）" if _is_close
                    else "🎯 今日操作清單（系統建議，三層防護後）")
            _oplist_block = f"\n## {_ttl}\n\n"
            if _is_close:
                _oplist_block += ("> ⚠️ **場外聯接基金今日 15:00 已截單,以下為【明日 15:00 前】的執行計畫**。\n"
                                  "> 明早盤前報/早報會依隔夜美股與開盤實況覆核,**執行前請以午報為準**。\n\n")
            _oplist_block += "| 代碼 | 名稱 | 操作 | 金額 | 現倉→操作後 |\n"
            _oplist_block += "|------|------|------|------|------------|\n"
            for a in _action_list:
                _oplist_block += (f"| {a['code']} | {a['name']} | {a['act']} | "
                                  f"{a['delta']:+.0f}元 | {a['cur']:.0f}→{a['new']:.0f} |\n")
            # [V22.59] 三態計數:系統建議 / 待你裁決(系統不背書但金額保留) / 待確認
            _optional_n = sum(1 for a in _action_list if a.get("user_optional"))
            _endorsed_n = sum(1 for a in _action_list if not a.get("user_optional"))
            _pending_n = sum(1 for a in _action_list if a.get("pending"))
            _exec_n = len(_action_list) - _pending_n
            _oplist_block += (f"\n> 共 {len(_action_list)} 檔有信號："
                              # [V22.60 治 V22.59 我自己的計數 bug] ★收盤報實測出現「-4 檔系統建議」。
                              #   根因:我用 (_exec_n - _optional_n),但 _exec_n = 總數 - _pending_n,
                              #   而 pending(時序限制) 與 user_optional(系統不背書) 是【正交】的兩個維度,
                              #   同一檔可以【兩者皆是】→ 相減得負數。
                              #   修法:主軸改用「系統背書 vs 待你裁決」互斥二分,
                              #        時序待確認另列為附註(不參與相減)。
                              f"{_endorsed_n} 檔系統建議 / **{_optional_n} 檔待你裁決**"
                              f"(系統不背書但金額保留)"
                              + (f"｜其中 {_pending_n} 檔因時序須待確認(早報→待午報;收盤→T+1)"
                                 if _pending_n else "")
                              + "。"  # [V22.60]
                              f"⚠️ 場外ETF按收盤淨值成交，待確認者請於收盤報/盤前報覆核後執行。\n\n")
        else:
            _oplist_block = (f"\n## {'🌅 明日操作計畫' if r_type == '收盤報' else '🎯 今日操作清單'}\n\n"
                             "> ✅ 經三層防護過濾後，"
                             + ("明日暫無建議操作標的" if r_type == "收盤報" else "今日無建議操作標的")
                             + "（全部維持現倉/觀望）。\n\n")
    except Exception as _e:
        _log_err(_e, "oplist_block")
    _oplist_block = _health_block + _oplist_block

    # ══ [V22.25.7] 四報因果鏈：異常檢測+紀律+方向判斷+否決透明化 ══
    # [V24.19] ★建立 _flow_map(原本全檔 0 處賦值,4 處讀取 → 永遠 None)
    #   來源:results[i]["flow5"](V24.19 新增,走東財→手工→量價代理三層回退鏈)
    #   代理值會標 is_proxy,下游若需真主力淨額應檢查該旗標。
    _flow_map = {}
    try:
        for _r in (results or []):
            _fi = _r.get("flow5")
            if _fi and _fi.get("flow5_yi") is not None:
                _flow_map[_r.get("code")] = _fi["flow5_yi"]
    except Exception as _e:
        _log_err(_e, "build_flow_map")

    _morning_anomaly_block = ""
    _morning_discipline_block = ""
    _morning_direction_block = ""
    _morning_rejection_block = ""
    if r_type in ("早報", "午報", "收盤報"):
        try:
            # --- 異常檢測(好壞雙向:V24.5 擴充) ---
            _anom = []
            # 黃金: 單日|漲跌|>1.5% 即為波動異常;上漲=避險需求升溫,下跌=獲利了結/美元壓制
            if gold_ret is not None and abs(gold_ret) > 0.015:
                _gd = "上漲" if gold_ret > 0 else "下跌"
                if gold_ret > 0:
                    _anom.append(f"* **000216 黃金**：單日{_gd} {gold_ret*100:+.2f}%，突破 1.5% 波動閾值 → 🔴 避險需求升溫(風險事件驅動)")
                else:
                    _anom.append(f"* **000216 黃金**：單日{_gd} {gold_ret*100:+.2f}%，突破 1.5% 波動閾值 → 🟡 貴金屬獲利了結/美元走強壓制(中性偏空)")
            # VIX: >25 恐慌(壞) / 20-25 警戒 / <14 平靜(好)
            if vix > 25:
                _anom.append(f"* **VIX**：{vix:.2f} > 25 → 🔴 恐慌區(系統性殺估值,避險/降杠杆優先)")
            elif vix > 20:
                _anom.append(f"* **VIX**：{vix:.2f} 處 20-25 → 🟡 警戒(波動偏高,謹慎)")
            elif vix < 14:
                _anom.append(f"* **VIX**：{vix:.2f} < 14 → 🟢 平靜(風險偏好佳,有利風險資產)")
            # SOX: >4% 強勢(好) / <-4% 重挫(壞)
            if abs(sox_d) > 0.04:
                if sox_d > 0.04:
                    _anom.append(f"* **SOX**：+{sox_d*100:.2f}% 強勢(>4%) → 🟢 美股科技多頭溢價,A股科技可望跟漲(⚠️誘多過濾已打3折 V22.17.1)")
                else:
                    _anom.append(f"* **SOX**：{sox_d*100:+.2f}% 重挫(<-4%) → 🔴 費半崩跌,A股科技承壓(風險優先)")
            if _anom:
                _morning_anomaly_block = ("## 異常狀態檢測（盤前→本報因果鏈·好壞雙向）\n\n"
                                          "> 🔴=偏空風險 / 🟡=中性警示 / 🟢=偏多利好。單日漲跌以「突破 1.5%」為黃金波動線,其餘以絕對幅度判定。\n\n"
                                          + "\n".join(_anom) + "\n\n")
            else:
                _morning_anomaly_block = ("## 異常狀態檢測（盤前→本報因果鏈·好壞雙向）\n\n"
                                          "> 本報暫無指標突破異常閾值（探測器正常運作,非未開機）。VIX/SOX/CNY/黃金均在常態區間。\n\n")

            # --- 今日紀律 ---
            _disc = []
            if sox_d > 0.04:
                _disc.append(
                    f"SOX隔夜暴漲{sox_d*100:.1f}%(>4%)已觸發誘多過濾，"
                    f"Tech先驗打3折。**禁止開盤重倉追高**，等完整資金流確認。"
                )
            if vix > 20:
                _disc.append(f"VIX={vix:.1f} 偏高，控制總倉位，不加槓桿。")
            if gold_ret is not None and abs(gold_ret) > 0.015:
                _disc.append(
                    f"黃金日漲{abs(gold_ret)*100:.1f}%({gold_ret*100:+.1f}%)，"
                    + ("避險情緒升溫，謹慎對待風險資產。" if gold_ret > 0 else "資金風險偏好改善，注意風格切換。")
                )
            # [V24.18 使用者指正] 原本只有 SOX>4% / VIX>20 / |黃金|>1.5% 三條【全是隔夜美股】,
            #   只要黃金持續波動就天天輸出同一句,看起來像萬年不變。
            #   補上【當日 A 股實況】維度,讓紀律隨當天真正發生的事變動。
            try:
                _core = locals().get("_core_ret_detail") or {}
                _valid = {k: v for k, v in _core.items() if v is not None}
                if _valid:
                    _wk = min(_valid.items(), key=lambda x: x[1])
                    _st = max(_valid.items(), key=lambda x: x[1])
                    if _wk[1] < -0.02:
                        _disc.append(f"{_wk[0]}板塊今日{_wk[1]*100:+.1f}%為最弱,該板塊持倉今日勿加碼,等止跌訊號。")
                    if _st[1] > 0.02:
                        _disc.append(f"{_st[0]}板塊今日{_st[1]*100:+.1f}%領漲,留意是否為單日情緒,勿以一根K線改變中期判斷。")
                _rr = locals().get("regime_type")
                if _rr == "ranging":
                    _disc.append("Regime=震盪市:趨勢型訊號可信度下降,重倉突破易被雙巴。")
                elif _rr == "trending_down":
                    _disc.append("Regime=下行趨勢:反彈預設視為反抽,需反轉三條件齊備才改判。")
            except Exception as _e:
                _log_err(_e, "discipline_extra")
            if _disc:
                _morning_discipline_block = "## 今日紀律（盤前→本報因果鏈）\n\n" +                     "\n".join(f"> {r}" for r in _disc) + "\n\n"

            # --- 開盤方向判斷 ---
            # [V22.25.7-fix] 方向標籤抽成變數，避免 f-string 內三元式被雙大括號當字面量
            _wti_v  = wti if wti is not None else 0.0
            _wtir_v = wti_ret if wti_ret is not None else 0.0
            _bias = "偏多" if sox_d > 0.005 else "偏空" if sox_d < -0.005 else "中性/震盪"
            _bias_icon = "🔼" if sox_d > 0.005 else "🔽" if sox_d < -0.005 else "➖"
            _sox_dir  = "🔼 偏多" if sox_d > 0 else "🔽 偏空"
            _ndx_dir  = "🔼 偏多" if ndx_d > 0 else "🔽 偏空"
            _vix_dir  = "🔴 恐慌" if vix > 25 else "🟡 偏高" if vix > 20 else "🟢 安全"
            _gold_dir = "🛡️ 避險升溫" if gold_ret > 0 else "📈 風險偏好"
            _wti_dir  = "⛽ 高通脹壓力" if _wti_v > 95 else "中性"
            _intra    = " + 開盤30分鐘實況" if r_type == "早報" else ""
            # [V24.18 使用者指正] 收盤報不產生「開盤方向判斷」。
            #   根因:_bias 完全由【隔夜 SOX】決定,指向的是【今天開盤】。
            #   收盤報 20:17 產出時今天早就收盤,美股今晚也還沒開 → 這塊在收盤報是
            #   「用昨晚資料預判已經發生過的今天開盤」,零資訊。
            #   正確因果鏈:收盤報寫【明日前提】→ 今晚美股 → 盤前報才出方向判斷。
            _morning_direction_block = "" if r_type == "收盤報" else (
                f"## {_bias_icon} 開盤方向判斷\n\n"
                f"| 指標 | 數值 | 方向 |\n"
                f"|------|------|------|\n"
                f"| **SOX** | {sox_d*100:+.2f}% | {_sox_dir} |\n"
                f"| **NDX** | {ndx_d*100:+.2f}% | {_ndx_dir} |\n"
                f"| **VIX** | {vix:.2f} | {_vix_dir} |\n"
                f"| **黃金** | {gold_ret*100:+.2f}% | {_gold_dir} |\n"
                f"| **原油WTI** | ${_wti_v:.1f} ({_wtir_v*100:+.2f}%) | {_wti_dir} |\n"
                f"| **CNY** | {usd_cny:.4f} | — |\n\n"
                f"> **綜合判斷**：{_bias_icon} 今日{_bias}"
                f"（基於隔夜美股數據{_intra}）\n"
                f"> 此判斷為方向傾向參考，非買入指令。具體操作以ETF明細信號為準。\n\n"
            )

            # --- 操作否決原因（早報透明化）---
            if r_type == "早報" and not _action_list:
                _rej = []
                _has_attack = sum(1 for r_ in results
                                  if r_.get("action") and "主攻" in str(r_.get("action","")))
                if _has_attack > 0:
                    _rej.append(
                        f"{_has_attack} 檔ETF有「主攻」信號，但被今日重大事件（如LPR利率發布等）"
                        f"觸發事件日防護，操作延後至事件落地後。"
                    )
                if sox_d > 0.04:
                    _rej.append(
                        f"SOX隔夜{sox_d*100:.1f}%觸發誘多過濾，Tech相關ETF的SOX加成已打3折，"
                        f"部分本可進場的信號被降級為觀望。"
                    )
                if vix > 20:
                    _rej.append(
                        f"VIX={vix:.1f} 偏高，觸發風控降級，部分邊際信號被否決。"
                    )
                if _rej:
                    _morning_rejection_block = "## 操作否決原因（系統透明化）\n\n" +                         "\n".join(f"> {r_}" for r_ in _rej) + "\n\n"
        except Exception as _e:
            _log_err(_e, "morning_causal_chain")

    # [V22.33.5·治使用者原則①] 產業鏈新聞區塊 — 四報都渲染(新聞是公式一部分,進 EV/T+1 占比 15%)
    news_block = ""
    try:
        _nm = locals().get("_news_map") or {}
        # [V22.96 ⑥全覆蓋] 顯示全部追蹤 ETF(無新聞標「無今日新聞·0」),不再只印 |分|≥0.3 的命中者
        _sig = []
        for _c in TARGET_ETFS:
            _v = _nm.get(_c)
            _cn = TARGET_ETFS.get(_c, {}).get("name", _c)[:8]
            if _v and abs(_v[0]) >= 0.05:
                _sig.append((_cn, _v[0], _v[1] or "—"))
            else:
                _sig.append((_cn, 0.0, "無今日新聞"))
        if _sig:
            _nl = ["\n## 📰 產業鏈新聞因子(四報共用,進 EV/T+1 占比 15%)\n",
                   "| 標的 | 新聞分 | 依據 |", "|---|---|---|"]
            for _n, _s, _r in sorted(_sig, key=lambda x: -abs(x[1])):
                _nl.append(f"| {_n} | {_s:+.2f} | {_r} |")
            news_block = "\n".join(_nl) + "\n"
        elif locals().get("_news_items"):
            news_block = "\n## 📰 產業鏈新聞因子\n> 今日新聞已載入,無高影響(|分|≥0.3)信號 → 因子≈0。\n"
        else:
            news_block = ("\n## 📰 產業鏈新聞因子\n" + _news_diag())
    except Exception as _e:
        _log_err(_e, "news_block")
        news_block = ""

    # [V22.70 四報因果鏈·早報驗證閉環] ★早報讀 premise 做前驗×後驗。
    #   前驗=收盤報寫入的 T+1 預測(load_morning_premise,取最新一筆,自動吃假日/漏跑);
    #   後驗=早報迴圈收集的實際走勢(_verif_actual)。比對方向 → 命中/偏離 + 命中率。
    #   取「最新一筆」而非硬算前一交易日(與盤前報 _prem_block 同源做法,更穩)。
    verif_block = ""
    if r_type == "早報":
        try:
            with sqlite3.connect(LOG_DB, timeout=10) as _vconn:
                _vconn.execute("CREATE TABLE IF NOT EXISTS morning_premise("
                               "date TEXT PRIMARY KEY, payload TEXT, ts TEXT)")
                _vrow = _vconn.execute(
                    "SELECT date FROM morning_premise ORDER BY date DESC LIMIT 1").fetchone()
                _vdate = _vrow[0] if _vrow else None
                _vprem = load_morning_premise(_vconn, _vdate) if _vdate else None
            verif_block = format_premise_verification(_vprem, _verif_actual, _vdate)
        except Exception as _e:
            _log_err(_e, "verif_block")
            verif_block = ""

    # [V22.79 四報因果鏈·收盤級預測計分] ★收盤報讀 premise×daily_snapshot 做收盤級命中計分 + 滾動命中率。
    scorecard_block = ""
    if r_type == "收盤報":
        try:
            with sqlite3.connect(LOG_DB, timeout=10) as _scconn:
                scorecard_block = format_close_scorecard(
                    _scconn, _now_utc8().strftime("%Y-%m-%d"))
        except Exception as _e:
            _log_err(_e, "scorecard_block")
            scorecard_block = ""

    full_content = (header + noon_header + concentration +
                    _oplist_block +  # [V22.20 P3 新增] 今日操作清單(置頂醒目)
                    _morning_rejection_block +  # [V22.25.7] 操作否決原因(早報透明化)
                    _morning_anomaly_block + _morning_discipline_block + _morning_direction_block +  # [V22.25.7] 四報因果鏈(異常+紀律+方向)
                    portfolio_battle_block +  # [V22.17.4 新增] 持倉戰局摘要
                    render_factor_matrix(results) +   # [V24.30] 因子貢獻矩陣
                    render_signal_conflicts(results,
                                            locals().get("_layout_calls"),
                                            locals().get("_flow_calls")) +  # [V24.21]
                    render_drawdown_position(USER_HOLDINGS, list(WATCHLIST_ETFS.keys())
                                             if "WATCHLIST_ETFS" in globals() else None) +  # [V24.17] 機會視角
                    render_exposure_status(USER_HOLDINGS, _current_styles_pct) +  # [V24.10] 既有持倉曝險狀態
                    render_drawdown_budget(USER_HOLDINGS) +  # [V24.12] 回撤預算現況
                    similarity_block +  # [V22.17.3 新增] 同質群組信號衝突警示
                    macro_summary_block +  # [V22.16 5/27 完整版] 宏觀先驗摘要
                    _events_block_shared +  # [V22.18.4 新增] 重大事件日曆(早/午/收盤)
                    market_validation_block +  # [V22.17.3 新增] A股 web 即時驗證
                    signal_stats_block +
                    final_report + consistency_block +
                    correction_chain_block + pnl_block +
                    watchlist_block +
                    # [V22.24.4] 大小單拆解鏈路狀態(午/收報, 顯性化防誘多武器)
                    # [V22.25.2] 早報核心: 開盤半小時果因診斷(美股傳導果 vs 提前布局因)
                    (format_morning_diag_block(_morning_diag) if r_type == "早報" else "") +
                    (verif_block if r_type == "早報" else "") +   # [V22.70] 前驗×後驗驗證(因果鏈閉環)
                    (scorecard_block if r_type == "收盤報" else "") +   # [V22.79] 收盤級預測命中計分+滾動命中率
                    (format_order_flow_block() if r_type in ("早報", "午報", "收盤報") else "") +
                    flow_actor_block +   # [V22.32] 早報:資金流主體識別(誰在買?)
                    attribution_block +  # [V22.31] 收盤報:今日歸因(學習閉環)
                    premise_block +    # [V22.30] 盤前報:今日盤勢預測(因果鏈起點)
                    t1_block +          # [V22.29/30] T+1預測(午報決策窗口/收盤報premise)
                    news_block +        # [V22.33.5] 產業鏈新聞因子(治原則①:四報都有、進公式)
                    outlook_block)

    full_content = full_content + news_audit_report_line()   # [V22.74] 新聞掃描覆蓋率提示
    full_content = data_availability.insert_unavailable_block(full_content)   # [雲端任務A]
    fn = os.path.join(REPORT_DIR, f"{__VERSION__}_{r_type}_{ts_str}.txt")
    with open(fn, 'w', encoding='utf-8') as f:
        f.write(full_content)

    # [V22.16 5/27 新增] 同時輸出 HTML 互動式報告到同一 reports/ 資料夾
    try:
        fn_html = os.path.join(REPORT_DIR, f"{__VERSION__}_{r_type}_{ts_str}.html")
        ts_full = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

        # ── 組裝圖表資料 ─────────────────────────────────
        # 1. 信號統計圓餅圖
        signal_pie = {"🟢 主攻": 0, "🟡 靜默": 0, "🔴 減倉": 0}
        for r in results:
            act = r.get("action", "")
            if "主攻" in act:
                signal_pie["🟢 主攻"] += 1
            elif "減倉" in act:
                signal_pie["🔴 減倉"] += 1
            else:
                signal_pie["🟡 靜默"] += 1

        # 2. 觀察池雷達圖（用 watchlist_results 變數，若無則空）
        radar_data = []
        try:
            if 'watchlist_results' in dir() and watchlist_results:
                for w in watchlist_results:
                    radar_data.append({
                        "code": w.get("code", "?"),
                        "name": w.get("name", "?")[:8],
                        "rsi": round(w.get("rsi", 50), 1),
                    })
        except Exception as _e:
            _log_err(_e, "unknown")

        # 3. ETF 預測柱狀圖（依 pred 排序）
        pred_bar = []
        for r in sorted(results, key=lambda x: -(x.get("predicted_ret") or 0)):
            pred_val = r.get("predicted_ret")
            if pred_val is None:
                continue
            pred_bar.append({
                "code": r.get("code", "?"),
                "name": r.get("name", "?")[:8],
                "pred": round(pred_val * 100, 2),
                "action": r.get("action", "")[:10],
            })

        html_content = markdown_to_html(
            full_content,
            title=f"{__VERSION__} {r_type}",
            subtitle=f"{ts_full} | 九龍合璧版 | 觀察池架構",
            chart_data={
                "signal_pie": signal_pie,
                "watchlist_radar": radar_data,
                "pred_bar": pred_bar,
            }
        )
        with open(fn_html, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"   HTML：{fn_html}")
    except Exception as e:
        print(f"   ⚠️ HTML 生成失敗（不影響 txt）: {e}")

    n_attack = sum(1 for r in results if "主攻" in r.get("action", ""))
    n_sell   = sum(1 for r in results if "減倉" in r.get("action", ""))
    n_silent = len(results) - n_attack - n_sell

    print(f"\n✅ {__VERSION__} {r_type} 完成！")
    print(f"   報告：{fn}")
    print(f"   掃描：{len(results)}檔 ｜ 🟢主攻{n_attack} ｜ 🟡靜默{n_silent} ｜ 🔴減倉{n_sell}")
    print(f"   精度：台股={acc.get('台股',{}).get('n',0)}筆 | A股={acc.get('A股',{}).get('n',0)}筆")

    # [V22.11+ 追蹤表自動更新] 只在收盤報觸發
    if r_type == "收盤報":
        update_four_report_tracker(r_type, results)

    # Telegram
    _send_telegram(
        f"📊 {__VERSION__} {r_type}\n"
        f"主攻{n_attack}|靜默{n_silent}|減倉{n_sell}\n"
        f"VIX={vix:.1f} NDX={ndx_d:+.2%}", silent=(r_type!="收盤報"))


if __name__ == "__main__":
    main()
