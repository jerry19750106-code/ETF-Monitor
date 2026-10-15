# -*- coding: utf-8 -*-
"""news_fetcher — 產業新聞抓取器（V24.19 補件）

【為何有這支檔】
主系統 `load_news_csv()` 在 news.csv 缺失時會呼叫：
    importlib.import_module("news_fetcher").ensure_news_csv()
但這支模組**從來不存在於代碼目錄**，import 直接失敗被 except 吞掉，
於是報告永遠顯示「無今日新聞」，新聞因子永遠 = 0。
使用者以為是「今天沒新聞」，實際是「抓取器不存在，新聞永遠不會來」。

【放置位置】
與 V24_18_ETF_System.py 同一個資料夾（代碼\）。

【對外契約】主系統只呼叫 ensure_news_csv()，其餘為內部函式。
    ensure_news_csv() -> (寫入筆數, 訊息)
    每日僅抓一次（以 news.csv 內是否已有今日日期判定）。

【誠實邊界】
- 東財新聞介面可能改版或限流；**抓不到就留空，絕不編造新聞**。
- 本檔在沙盒**無法實測**（網路不通），需在本機驗證。
"""
from __future__ import annotations
import os, csv, datetime, json, re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
NEWS_CSV = os.path.join(BASE_DIR, "news.csv")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
           "Referer": "https://finance.eastmoney.com/"}

# scope 對照：把新聞歸到系統認得的板塊名（與 TARGET_ETFS 的 sector 標籤對齊）
SCOPE_KEYWORDS = {
    "光通信": ["光模块", "光模組", "CPO", "光通信", "中际旭创", "新易盛", "天孚"],
    "半導體": ["半导体", "晶圆", "芯片", "光刻", "封装", "存储", "中芯", "长电"],
    "半導體材料": ["ABF", "载板", "覆铜板", "T-Glass", "电子材料", "靶材", "光刻胶"],
    "科創50": ["科创板", "科创50", "硬科技"],
    "消費電子": ["消费电子", "手机", "折叠屏", "AI眼镜", "立讯", "歌尔"],
    "電力": ["电力", "特高压", "电网", "核电", "绿电", "储能"],
    "有色": ["有色", "铜", "铝", "稀土", "锂", "钴"],
    "黃金": ["黄金", "金价", "贵金属", "避险"],
    "電信": ["运营商", "中国移动", "中国电信", "中国联通", "5G"],
    "紅利": ["红利", "高股息", "分红"],
    "納指": ["纳斯达克", "美股科技", "英伟达", "台积电", "AI算力"],
    "機器人": ["机器人", "人形机器人", "灵巧手", "减速器"],
    "新能源車": ["新能源车", "动力电池", "宁德时代", "固态电池"],
}


def _today() -> str:
    return datetime.date.today().strftime("%Y-%m-%d")


def _http_get(url, timeout=8):
    """優先 requests，失敗回退 urllib。不可達回 None（不編造）。"""
    try:
        import requests
        s = requests.Session()
        s.trust_env = False          # 與主系統東財路徑一致：繞過 VPN proxy
        r = s.get(url, headers=HEADERS, timeout=timeout)
        return r.text if r.status_code == 200 else None
    except Exception:
        pass
    try:
        import urllib.request as _u
        req = _u.Request(url, headers=HEADERS)
        return _u.urlopen(req, timeout=timeout).read().decode("utf-8", errors="replace")
    except Exception:
        return None


def _classify(title: str):
    """標題 → scope。無法歸類回 None（不硬塞）。簡繁都收。"""
    for scope, kws in SCOPE_KEYWORDS.items():
        for kw in kws:
            if kw in title:
                return scope
    return None


def fetch_eastmoney_news(limit=80):
    """東財財經要聞。回傳 [(date, scope, title)]，抓不到回 []。"""
    url = ("https://np-listapi.eastmoney.com/comm/web/getFastNewsList"
           f"?client=web&biz=web_news_col&fastColumn=102&sortEnd=&pageSize={limit}&req_trace=1")
    raw = _http_get(url)
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        m = re.search(r"\{.*\}", raw, re.S)
        if not m:
            return []
        try:
            data = json.loads(m.group(0))
        except Exception:
            return []
    items = []
    node = data.get("data") or {}
    lst = node.get("fastNewsList") or node.get("list") or []
    today = _today()
    for it in lst:
        title = (it.get("title") or it.get("digest") or "").strip()
        if not title:
            continue
        ts = str(it.get("showTime") or it.get("publishTime") or "")[:10]
        if ts and ts != today:
            continue
        scope = _classify(title)
        if scope:
            items.append((today, scope, title[:60]))
    return items


def _existing_dates():
    if not os.path.exists(NEWS_CSV):
        return set(), []
    rows, dates = [], set()
    try:
        with open(NEWS_CSV, "r", encoding="utf-8-sig") as f:
            for r in csv.reader(f):
                if not r or r[0].strip().startswith("#"):
                    continue
                if r[0].strip().lower() == "date":
                    continue
                rows.append(r)
                dates.add(r[0].strip()[:10])
    except Exception:
        pass
    return dates, rows


def ensure_news_csv():
    """主系統呼叫的入口。今日已有資料則不重抓。回傳 (新增筆數, 訊息)。"""
    today = _today()
    dates, rows = _existing_dates()
    if today in dates:
        return 0, f"news.csv 已有 {today} 資料，不重抓"
    items = fetch_eastmoney_news()
    if not items:
        # 仍寫入表頭，讓主系統的診斷能分辨「檔案在但今天沒資料」
        if not os.path.exists(NEWS_CSV):
            with open(NEWS_CSV, "w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow(["date", "scope", "title"])
        return 0, "❌ 東財新聞介面不可達或今日無可歸類新聞（未編造任何內容）"
    write_header = not os.path.exists(NEWS_CSV)
    with open(NEWS_CSV, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["date", "scope", "title"])
        seen = set()
        n = 0
        for d, sc, t in items:
            k = (d, sc, t)
            if k in seen:
                continue
            seen.add(k)
            w.writerow([d, sc, t])
            n += 1
    return n, f"✅ 寫入 {n} 筆今日新聞至 news.csv"


if __name__ == "__main__":
    n, msg = ensure_news_csv()
    print(msg)
    if n:
        print(f"檔案：{NEWS_CSV}")
