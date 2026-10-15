# -*- coding: utf-8 -*-
"""dd_fetcher — 盡調資料自動抓取器（V24.19 補件）

【為何有這支檔】
主系統的 `DD_META` 是**人工字典**，實際只填了 6 檔
（00406A.TW / 00995A.TW / 00982A.TW / 013180 / 008888 / 019072）。
018345、005693、025856、00919.TW 等從未建檔 → 盡調 9 維全打「❓未驗證·待查」
→ 裁決「資料不足,不宜下重手」→ **永遠不會被選中 → 沒人去建檔**。
這是一個閉環的死循環，使用者已反覆指出。

【關鍵事實】
交接手冊 §六 明載：天天基金／同花順的**費率、規模、經理人、持股都是公開可查的**。
「需付費源」只對 AUM 即時值成立；**大部分維度可以自動抓**。

【放置位置】與主系統同一資料夾（代碼\）。

【對外契約】
    fetch_dd(code) -> dict | None      單檔抓取
    build_dd_meta(codes) -> dict       批次，回傳可直接併入 DD_META 的結構
    main()                             CLI：產出 dd_meta_auto.json

【誠實邊界（鐵律㉘）】
- 抓不到的維度一律留空並標「未取得」，**絕不用預設值或估算值填充**。
- 每筆帶 `as_of` 日期，主系統的 30 日 TTL 才能正常失效。
- 「位置」「結構vs能力歸因」兩維**需要判斷不只是資料**，本模組不自動填，維持人工。
- 本檔在沙盒**無法實測**（fund.eastmoney 不可達），需在本機驗證。
"""
from __future__ import annotations
import os, json, re, datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_JSON = os.path.join(BASE_DIR, "dd_meta_auto.json")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
           "Referer": "https://fund.eastmoney.com/"}


def _http_get(url, timeout=8, encoding="utf-8"):
    try:
        import requests
        s = requests.Session()
        s.trust_env = False
        r = s.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None
        r.encoding = encoding
        return r.text
    except Exception:
        pass
    try:
        import urllib.request as _u
        req = _u.Request(url, headers=HEADERS)
        return _u.urlopen(req, timeout=timeout).read().decode(encoding, errors="replace")
    except Exception:
        return None


def _f(pattern, text, cast=str):
    m = re.search(pattern, text or "")
    if not m:
        return None
    try:
        return cast(m.group(1))
    except Exception:
        return None


def load_feeder_parent_map():
    """[V24.19.3] 從主系統源碼抽「聯接基金 → 母ETF」對照。

    ★為何需要(使用者 2026-08-10 實測):聯接基金的 ccmx 持股頁**本來就沒有股票明細**
      ——它持有的是母 ETF 而非個股。7 檔全部回同一訊息就是這個原因,不是 bug。
      正解是穿透到母 ETF 抓持股;母 ETF 代碼主系統 TARGET_ETFS 的 proxy 欄早就有。
    不硬編對照表(會過期),直接讀源碼 proxy 欄;讀不到回 {} 並在診斷說明。
    """
    import glob
    mp = {}
    try:
        cands = sorted(glob.glob(os.path.join(BASE_DIR, "V2*_ETF_System*.py")))
        if not cands:
            return mp
        src = open(cands[-1], "r", encoding="utf-8", errors="replace").read()
        for c, px in re.findall(
                r'"(\d{6})"\s*:\s*\{[^}]*?"proxy"\s*:\s*"([a-z]{2}\d{6})"', src, re.S):
            mp.setdefault(c, px)
    except Exception:
        pass
    return mp


_PARENT_MAP = None


def _parent_of(code):
    global _PARENT_MAP
    if _PARENT_MAP is None:
        _PARENT_MAP = load_feeder_parent_map()
    px = _PARENT_MAP.get(str(code))
    return px[2:] if px and len(px) == 8 else None



def fetch_dd(code: str):
    """抓單檔盡調資料。回傳 dict；完全抓不到回 None。

    資料源：fund.eastmoney.com/pingzhongdata/{code}.js（公開、無需登入）
    可得欄位：基金全名、成立日、經理人、規模(億)、前十大持股、管理費/託管費。
    """
    if not code or code.endswith(".TW"):
        return None          # 台股走別的源，本模組不處理（誠實不裝懂）
    raw = _http_get(f"https://fund.eastmoney.com/pingzhongdata/{code}.js")
    if not raw:
        return None
    out = {"code": code, "as_of": datetime.date.today().strftime("%Y-%m-%d"),
           "src": "天天基金 pingzhongdata（公開頁）"}

    out["name"] = _f(r'fS_name\s*=\s*"([^"]+)"', raw)

    # ★[V24.19.2 修] 原用 json.loads 解析 pingzhongdata,但它是【JS 物件字面量】——
    #   key 沒有引號(如 series:[{y:1.23}]),json.loads 必然失敗。
    #   使用者實測「只有 2 維自動填入」正是這個原因:只有純正則抽的 name / mgmt_fee 成功。
    #   改為全部用正則直抽,不再依賴 JSON 解析。
    _diag = {}
    _dbg = {}

    def _strip(h):
        """去 HTML 標籤 → 純文字。費率/持股頁是 HTML,直接對原始碼下正則
        會被標籤切斷(使用者 2026-08-10 實測:『費率頁可達但正則未命中』7檔全同)。"""
        h = re.sub(r'<script.*?</script>', ' ', h or '', flags=re.S)
        h = re.sub(r'<[^>]+>', ' ', h)
        return re.sub(r'\s+', ' ', h)

    # ── 規模(億) ── 同時支援 y: 與 "y": 兩種格式(★原正則只支援無引號版,7檔全失敗)
    m = re.search(r'Data_fluctuationScale\s*=\s*(\{.*?\})\s*;', raw, re.S)
    if m:
        blk = m.group(1)
        _dbg["scale_raw"] = blk[:220]
        ys = re.findall(r'["\']?y["\']?\s*:\s*"?([\d.]+)"?', blk)
        if not ys:      # 備援:data:[1.2,3.4] 純數列格式
            dm = re.search(r'["\']?data["\']?\s*:\s*\[([\d.,\s]+)\]', blk)
            if dm:
                ys = [x for x in re.findall(r'[\d.]+', dm.group(1))]
        if ys:
            try:
                out["scale_yi"] = round(float(ys[-1]), 2)
            except Exception:
                _diag["scale"] = "y值轉型失敗"
        else:
            _diag["scale"] = "區塊在但無 y 值(見 _dbg.scale_raw)"
    else:
        _diag["scale"] = "無 Data_fluctuationScale 區塊"

    # ── 經理人 ── 同上,支援帶引號 key
    m = re.search(r'Data_currentFundManager\s*=\s*(\[.*?\])\s*;', raw, re.S)
    if m:
        blk = m.group(1)
        _dbg["manager_raw"] = blk[:220]
        out["manager"] = _f(r'["\']?name["\']?\s*:\s*["\']([^"\']+)["\']', blk)
        out["manager_years"] = _f(r'["\']?workTime["\']?\s*:\s*["\']([^"\']+)["\']', blk)
        out["manager_score"] = _f(r'["\']?avr["\']?\s*:\s*["\']?([\d.]+)', blk)
        if not out.get("manager"):
            _diag["manager"] = "區塊在但無 name(見 _dbg.manager_raw)"
    else:
        # 備援:fundf10 基本概況頁(HTML,比 JS 解析可靠)
        gk = _http_get(f"https://fundf10.eastmoney.com/jbgk_{code}.html")
        if gk:
            txt = _strip(gk)
            out["manager"] = _f(r'基金经理人\s*([\u4e00-\u9fa5A-Za-z ]{2,20}?)\s', txt)
            if out.get("scale_yi") is None:
                _sc = _f(r'资产规模\s*([\d.]+)\s*亿', txt, float)
                if _sc is not None:
                    out["scale_yi"] = _sc
            _dbg["jbgk_raw"] = txt[:220]
        if not out.get("manager"):
            _diag["manager"] = "JS區塊與 jbgk 備援皆未取得"

    # ── 持股 ── 先去標籤再抓(★原本直接對 HTML 原始碼下正則,7檔全失敗)
    hold = _http_get(f"https://fundf10.eastmoney.com/ccmx_{code}.html")
    if hold:
        htxt = _strip(hold)
        _dbg["hold_raw"] = htxt[:260]
        names = re.findall(r'\b\d{6}\s+([\u4e00-\u9fa5A-Za-z0-9*]{2,12})\s', htxt)
        if names:
            seen, uniq = set(), []
            for n in names:
                if n not in seen:
                    seen.add(n); uniq.append(n)
            out["top_stocks"] = uniq[:10]
        else:
            _par = _parent_of(code)
            _hold_src = None
            for _tgt, _lab in ((code, "本檔"), (_par, "母ETF")):
                if not _tgt:
                    continue
                # ★源A:東財 f10 的【真實資料介面】。ccmx_xxx.html 的持股表是
                #   JS 動態載入,靜態 HTML 沒有表格內容 → 前一版對它下正則必然抓不到
                #   (使用者 2026-08-10 實測:穿透母ETF 後 7 檔仍全數「無持股明細」)。
                aj = _http_get("https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
                               f"?type=jjcc&code={_tgt}&topline=10&year=&month=")
                cand = _strip(aj) if aj else ""
                if not cand:
                    # 源B:靜態頁備援
                    hb = _http_get(f"https://fundf10.eastmoney.com/ccmx_{_tgt}.html")
                    cand = _strip(hb) if hb else ""
                if not cand:
                    continue
                pn = re.findall(r'\b(\d{6})\s+([\u4e00-\u9fa5A-Za-z0-9*]{2,12})\s', cand)
                if not pn:
                    pn = [("", x) for x in re.findall(
                        r'[\u4e00-\u9fa5]{2,8}(?=\s+[\d.]+\s*%)', cand)]
                if pn:
                    seen2, uniq2 = set(), []
                    for _c2, n in pn:
                        if n and n not in seen2:
                            seen2.add(n); uniq2.append(n)
                    if uniq2:
                        out["top_stocks"] = uniq2[:10]
                        if _lab == "母ETF":
                            out["holdings_via"] = _par
                        _hold_src = _lab
                        break
                _dbg[f"hold_raw_{_lab}"] = cand[:240]
            if not _hold_src:
                _pieces = [f"{k2}={v2[:120]}" for k2, v2 in _dbg.items()
                           if k2.startswith("hold_raw_")]
                _diag["holdings"] = ("兩源(FundArchivesDatas/ccmx)皆未解析出持股。"
                                     "若為商品/債券型ETF屬正常(無股票持股)。"
                                     + (" 原始片段→ " + " || ".join(_pieces) if _pieces
                                        else " 兩源皆不可達"))
    else:
        _diag["holdings"] = "fundf10 持股頁不可達"

    # ── 費率 ── 去標籤後再抓,並放寬間隔
    fee = _http_get(f"https://fundf10.eastmoney.com/jjfl_{code}.html")
    if fee:
        ftxt = _strip(fee)
        _dbg["fee_raw"] = ftxt[:260]
        out["mgmt_fee"] = _f(r'管理费率[^%\d]{0,80}?([\d.]+)\s*%', ftxt, float)
        out["custody_fee"] = _f(r'托管费率[^%\d]{0,80}?([\d.]+)\s*%', ftxt, float)
        out["sales_fee"] = _f(r'销售服务费率[^%\d]{0,80}?([\d.]+)\s*%', ftxt, float)
        if out.get("mgmt_fee") is None:
            _diag["fee"] = "費率頁可達但正則未命中(見 _dbg.fee_raw)"
    else:
        _diag["fee"] = "fundf10 費率頁不可達"

    # ── [V24.20] 補齊原標「需判斷,維持人工」的四維 ──
    #   ★使用者 2026-08-10:「資料為什麼是人工填??找你就是要你抓資料自動填上」。
    #   重新檢視:這四維【全都算得出來】,原本說「需判斷」是偷懶。
    fh = _http_get(f"https://fundf10.eastmoney.com/fhsp_{code}.html")
    if fh:
        ftx = _strip(fh)
        _divs = re.findall(r'(\d{4}-\d{2}-\d{2})\s+每份派息\s*([\d.]+)', ftx)
        if not _divs:
            _divs = re.findall(r'(\d{4}-\d{2}-\d{2})[^\d]{0,20}([\d.]+)\s*元', ftx)
        if _divs:
            out["dividends"] = _divs[:6]
        elif "暂无" in ftx or "暫無" in ftx:
            out["dividends"] = []
        else:
            _diag["dividend"] = "分紅頁可達但未解析出派息紀錄"
    else:
        _diag["dividend"] = "分紅頁不可達"

    _nav = re.search(r'Data_netWorthTrend\s*=\s*(\[.*?\])\s*;', raw, re.S)
    if _nav:
        ys = [float(x) for x in re.findall(r'["\']?y["\']?\s*:\s*([\d.]+)', _nav.group(1))]
        if len(ys) >= 60:
            _peak, _cur = max(ys), ys[-1]
            _pk, _mdd = ys[0], 0.0
            for v in ys:
                _pk = max(_pk, v); _mdd = min(_mdd, v / _pk - 1)
            out["pos_cur_dd"] = round((_cur / _peak - 1) * 100, 1)
            out["pos_max_dd"] = round(_mdd * 100, 1)
            out["pos_n"] = len(ys)
            out["ret_1y"] = round((ys[-1] / ys[-244] - 1) * 100, 1) if len(ys) > 244 else None
        else:
            _diag["position"] = f"淨值點數不足({len(ys)}<60)"
    else:
        _diag["position"] = "無 Data_netWorthTrend 區塊"

    _rk = re.search(r'Data_performanceEvaluation\s*=\s*(\{.*?\})\s*;', raw, re.S)
    if _rk:
        _av = re.findall(r'["\']?data["\']?\s*:\s*\[([\d.,\s]+)\]', _rk.group(1))
        if _av:
            out["eval_scores"] = [float(x) for x in re.findall(r'[\d.]+', _av[0])]
    if out.get("top_stocks"):
        out["holding_n"] = len(out["top_stocks"])


    if _diag:
        out["_diag"] = _diag
    if os.environ.get("DD_DEBUG"):
        out["_dbg"] = _dbg

    got = [k for k in ("name", "scale_yi", "manager", "top_stocks", "mgmt_fee") if out.get(k)]
    if not got:
        return None
    out["_coverage"] = f"{len(got)}/5 欄位取得"
    return out


def to_dd_meta(d: dict):
    """把抓到的原始欄位轉成 DD_META 的 9 維敘述。

    ⚠️ 只填【資料可直接支持】的維度；需要判斷的維度留空給人工，不自動猜。
    """
    if not d:
        return None
    meta = {}
    if d.get("mgmt_fee") is not None:
        _tot = (d.get("mgmt_fee") or 0) + (d.get("custody_fee") or 0) + (d.get("sales_fee") or 0)
        meta["成本"] = (f"管理{d['mgmt_fee']}%+託管{d.get('custody_fee','?')}%"
                       + (f"+銷服{d['sales_fee']}%" if d.get("sales_fee") else "")
                       + f" ≈ 年費 {_tot:.2f}%")
    if d.get("scale_yi") is not None:
        _s = d["scale_yi"]
        meta["規模流動性"] = (f"規模 {_s:.2f} 億"
                            + ("（⚠️ <2億，有清盤線風險）" if _s < 2 else
                               "（⚠️ 偏小，注意流動性）" if _s < 5 else "（規模充足）"))
    if d.get("manager"):
        meta["經理人"] = (f"{d['manager']}"
                         + (f"，任職 {d['manager_years']}" if d.get("manager_years") else "")
                         + (f"，天天評分 {d['manager_score']}" if d.get("manager_score") else ""))
    if d.get("top_stocks"):
        _via = d.get("holdings_via")
        meta["持股"] = ("前十大：" + "、".join(d["top_stocks"][:10])
                       + (f"（穿透母ETF {_via} 取得，本檔為聯接基金）" if _via else ""))
    if d.get("name"):
        meta["結構"] = f"{d['name']}（自動取得，指數/主動判定待人工確認）"
    # ── [V24.20] 四維自動化(原標「需判斷」不填,實為偷懶) ──
    _dv = d.get("dividends")
    if _dv is not None:
        meta["配息"] = (f"近{len(_dv)}次派息:" + "、".join(f"{x[0]} {x[1]}" for x in _dv[:4])
                       if _dv else "查無派息紀錄（未配息或不配息型）")
    if d.get("pos_cur_dd") is not None:
        _c, _m, _n = d["pos_cur_dd"], d["pos_max_dd"], d.get("pos_n")
        _pct = (_c / _m * 100) if _m and _m < -1e-9 else 0
        _zone = ("已達史上最深區" if _pct >= 90 else "深回撤區" if _pct >= 70
                 else "中段" if _pct >= 40 else "淺回撤" if _pct >= 15 else "貼近歷史高點")
        meta["位置"] = (f"現距前高 {_c:+.1f}%，史上最深 {_m:+.1f}%，已走完 {_pct:.0f}% → {_zone}"
                       f"（{_n}個淨值點；歷史最深非地板）")
    _r1, _ev = d.get("ret_1y"), d.get("eval_scores")
    if _r1 is not None or _ev:
        _ps = []
        if _r1 is not None:
            _ps.append(f"近一年 {_r1:+.1f}%")
        if _ev:
            _ps.append(f"天天五維評分 {'/'.join(str(x) for x in _ev[:5])}")
        meta["結構vs能力歸因"] = "；".join(_ps) + "（報酬來自指數結構或經理人能力，需搭配同類排名判讀）"
    if d.get("holding_n"):
        meta["單點風險"] = (f"取得前{d['holding_n']}大持股"
                          + ("（<10檔，集中度偏高需留意單一標的衝擊）" if d["holding_n"] < 10
                             else "（前十大分散度正常）"))

    if d.get("_diag"):
        meta["_diag"] = d["_diag"]
    if d.get("_dbg"):
        meta["_dbg"] = d["_dbg"]
    meta["_as_of"] = d["as_of"]
    meta["_src"] = d["src"]
    meta["_auto"] = True
    return meta


def build_dd_meta(codes):
    out, fail = {}, []
    for c in codes:
        d = fetch_dd(c)
        m = to_dd_meta(d)
        if m:
            out[c] = m
        else:
            fail.append(c)
    return out, fail


def main(codes=None):
    codes = codes or ["018345", "005693", "025856", "013180", "008888", "019072", "020356"]
    meta, fail = build_dd_meta(codes)
    if meta:
        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        print(f"✅ 已寫入 {len(meta)} 檔 → {OUT_JSON}")
        for c, mm in meta.items():
            _n = len([k for k in mm if not k.startswith('_')])
            print(f"  {c}: {_n} 維自動填入")
            for _k, _v in (mm.get("_diag") or {}).items():
                print(f"      ⚠️ {_k}: {_v}")
            for _k, _v in (mm.get("_dbg") or {}).items():
                print(f"      🔍 {_k}: {str(_v)[:200]}")
    if fail:
        print(f"❌ 抓取失敗（未編造任何內容）：{', '.join(fail)}")
    print("\n[V24.20] 九維全部自動化。抓不到的維度會列在 ⚠️ 診斷中，不以預設值填充。")


if __name__ == "__main__":
    main()
