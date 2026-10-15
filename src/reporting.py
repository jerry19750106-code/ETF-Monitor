"""報告格式化純函式(任務C 重構 phase 1,自 V24_19_ETF_System.py 原樣搬入)。

本檔函式本體與搬移前【字元級一致】(tests/test_characterization.py 以 SHA-256 驗證),
且只依賴標準庫/numpy 與同檔其他函式,不讀取主程式任何全域狀態。
主程式以原名匯入,外部介面不變。
"""
import datetime
import re


def format_closing_handoff(payload, date_str, live_news=None):
    """[V22.80] 盤前報區塊:昨日收盤情報交棒 → 今日起手情境。"""
    if not payload:
        return ("\n## 🔮 昨日收盤情報交棒\n\n"
                "> ⬜ 無昨日收盤交棒(收盤報未寫入 `closing_handoff`)→ 今日盤前純用隔夜美股+新聞起手。\n\n")
    hdr = (f"\n## 🔮 昨日收盤情報交棒 → 今日起手情境\n\n"
           f"> 📌 昨日({date_str})收盤對「今日一切情報」的市場級綜合。"
           f"盤前據此 + 隔夜美股 + 新聞 → 今日預測(因果鏈:收盤總結→盤前起手)。\n\n")
    _RMAP = {"trending_up": "📈 趨勢上行", "trending_down": "📉 趨勢下行", "ranging": "🔄 震盪"}
    _reg = payload.get("regime")
    _rtxt = _RMAP.get(_reg, _reg or "—")
    _fnet = payload.get("flow_net_yi", 0)
    _fdir = "淨流入" if _fnet > 0 else ("淨流出" if _fnet < 0 else "中性")
    _nv = payload.get("news_avg", 0); _nn = payload.get("news_n", 0)
    _nvalid = payload.get("news_valid", True); _nsrc = "昨收"
    # [V22.85] 快照無效(抓失敗凍的0)或有今晨即時→現場重算優先,不被舊快照卡死(WB 定位 + Claude 補強)
    if live_news is not None and ((not _nvalid) or _nv == 0):
        _nv = live_news.get("avg", _nv); _nn = live_news.get("n", _nn); _nsrc = "今晨即時"
    _ntxt = "偏多" if _nv > 0.1 else ("偏空" if _nv < -0.1 else "中性")
    hdr += (f"**市場綜合**:regime {_rtxt} ｜ 機構5日資金 {_fnet:+.1f}億"
            f"({_fdir},{payload.get('flow_in_n',0)}進/{payload.get('flow_out_n',0)}出) ｜ "
            f"新聞面 {_nv:+.2f}({_ntxt},{_nn}則·{_nsrc})\n\n")
    _sb = payload.get("style_bias", {})
    if _sb:
        hdr += "**分板塊方向偏向**:" + " ｜ ".join(
            f"{st}:{d.get('bias','?')}" for st, d in _sb.items()) + "\n\n"
    _tp = payload.get("top_pred", [])
    if _tp:
        hdr += "**昨收最強方向預測(對今日)**:\n"
        for t in _tp:
            _ic = "🔴" if t["t1"] < -0.8 else ("🟢" if t["t1"] > 0.8 else "⚪")
            hdr += f"- {t['name']} {_ic} {t['t1']:+.2f}%(信心{t['conf']})\n"
        hdr += "\n"
    hdr += f"> 宏觀先驗傾向(均值):{payload.get('macro_avg', 0):+.2f}\n\n"
    return hdr


def format_morning_diag_block(diag_list):
    """[V22.25.3] 早報果因診斷 — 美股傳導的果 vs 有人提前布局的因 + 操作建議。
    核心修正(V22.25.3): 「布局型(因)」改為『相對異常』——只標偏離當日市場共性者。
    全市場逆勢高開時主力普遍減倉=市場beta(共性), 非8個內線; 真布局=逆市場共性而動的少數。
    台股納入(美股預期×開盤實際兩維, 無東財大小單故資金維標N/A)。
    場外T+1: 早盤買=盤中買=收盤NAV結算, 故直接給操作建議(可觀望至午報最終判斷)。
    """
    if not diag_list:
        return ""

    # ── 步驟1: 算「當日市場共性」——多數持倉的價×主力關係(只用有資金流的A股/QDII) ──
    flow_rows = [d for d in diag_list if d["has_flow"] and d["main"] is not None
                 and abs(d["act"]) > 1e-6]
    n_pos_div = sum(1 for d in flow_rows if d["act"] > 0 and d["main"] < 0)  # 價漲主力撤
    n_neg_div = sum(1 for d in flow_rows if d["act"] < 0 and d["main"] > 0)  # 價跌主力吸
    total_flow = len(flow_rows)
    # 共性閾值: 某種背離佔有資金流持倉≥半數 → 視為市場beta, 非個股布局
    market_mode = None
    if total_flow >= 4:
        if n_pos_div >= total_flow * 0.5:
            market_mode = "價漲主力撤"   # 市場級高開獲利了結
        elif n_neg_div >= total_flow * 0.5:
            market_mode = "價跌主力吸"   # 市場級低開承接

    # ── 步驟2: 逐檔分類(相對異常) + 操作建議 ──
    def _classify(d):
        exp, act, main = d["exp"], d["act"], d["main"]
        has_flow = d["has_flow"]
        # 個股當日背離方向
        my_div = None
        if has_flow and main is not None and abs(act) > 1e-6:
            if act > 0 and main < 0:
                my_div = "價漲主力撤"
            elif act < 0 and main > 0:
                my_div = "價跌主力吸"
        # 布局型(因) = 有背離 且 (無市場共性 或 我的背離≠市場共性=逆勢而動)
        if my_div and (market_mode is None or my_div != market_mode):
            if my_div == "價跌主力吸":
                return ("⚠️布局型·吸籌(因)",
                        "🔴 逆市場異常吸籌→該檔可考慮加/持有(場外T+1,今日任意時點等效;可觀望至午報)")
            else:
                return ("⚠️布局型·派發(因)",
                        "⚠️ 逆市場異常派發→該檔可考慮減(場外T+1,今日任意時點等效;可觀望至午報)")
        # 符合市場共性的背離 → 市場beta, 不算布局
        if my_div and market_mode and my_div == market_mode:
            return ("🌊市場共性(高開減/低開承接)",
                    "→ 隨市場,非個股信號;按既有倉位持有")
        # 無背離: 看美股傳導對照
        if exp is not None and abs(act) > 1e-6:
            if (exp > 0) == (act > 0):
                return ("✅跟隨型(果)", "→ 跟隨美股傳導,無新資訊;持有")
            else:
                return ("🔴預判落空(有他力)",
                        "→ 與美股預期反向,方向有變;建議觀望至午報確認")
        return ("➡️待觀察", "→ 數據不足;觀望至午報")

    rows = []
    for d in diag_list:
        diag, advice = _classify(d)
        rows.append({**d, "diag": diag, "advice": advice})

    # ── 步驟3: 渲染(布局型排最前——早報最該警惕的領先信號) ──
    def _sortkey(r):
        if "布局" in r["diag"]:
            return 0
        if "落空" in r["diag"]:
            return 1
        return 2
    rows.sort(key=_sortkey)

    lines = ["\n## 🔍 開盤半小時·果因診斷（早報核心：美股傳導的果 vs 有人提前布局的因）\n"]
    # [V22.25.4] 布局探測盲區顯性化: 多數持倉資金流限流時, 「0布局」是瞎不是準, 須明示
    _flow_holdings = [d for d in diag_list if d["market"] != "台股"]
    _flow_blind = (len(flow_rows) < max(2, len(_flow_holdings) * 0.4)) if _flow_holdings else True
    if _flow_blind:
        lines.append("> 🚨 **布局探測今日不可用**：多數持倉資金流被東財限流未取得"
                     f"（僅{len(flow_rows)}檔有資金數據）。下方「無布局型」是**探測器沒開機**，"
                     "**非『無異常』**——真有人布局也看不到。需換IP/換源恢復資金流。\n")
    if market_mode:
        lines.append(f"> 🌊 當日市場共性：多數持倉「{market_mode}」"
                     f"（{'高開獲利了結' if market_mode=='價漲主力撤' else '低開資金承接'}，"
                     f"屬市場beta，已從個股布局信號中剔除）\n")
    lines.append("| 代碼 | 名稱 | 美股預期 | 開盤實際 | 資金流 | 診斷 | 操作建議 |")
    lines.append("|------|------|---------|---------|--------|------|---------|")
    for r in rows:
        _exp = (f"{r['exp']*100:+.1f}%" if r['exp'] is not None else "—")
        _act = f"{r['act']*100:+.2f}%" if abs(r['act']) > 1e-6 else "~0"
        if not r["has_flow"]:
            _flow = "N/A(台股無大小單)" if r["market"] == "台股" else "—(限流未取得)"
        else:
            _flow = f"主力{r['main']:+.2f}億" if r["main"] is not None else "—"
        lines.append(f"| {r['code']} | {r['name']} | {_exp} | {_act} | {_flow} | "
                     f"{r['diag']} | {r['advice']} |")

    # 早報級總結
    n_layout = sum(1 for r in rows if "布局" in r["diag"])
    n_miss = sum(1 for r in rows if "落空" in r["diag"])
    n_follow = sum(1 for r in rows if "跟隨" in r["diag"])
    parts = []
    if n_layout:
        codes = "、".join(r['code'] for r in rows if "布局" in r["diag"])
        parts.append(f"**{n_layout}檔真布局背離(因): {codes}**⚠️")
    if n_miss:
        parts.append(f"{n_miss}檔預判落空")
    if n_follow:
        parts.append(f"{n_follow}檔跟隨美股(果)")
    if not parts:
        _summary = ("🚨 布局探測不可用(資金流限流)，非無異常" if _flow_blind
                    else "市場共性主導，無個股異常")
    else:
        _summary = ' / '.join(parts)
    lines.append(f"\n> 📋 今早判讀：{_summary}\n"
                 f"> 💡 布局型已用『相對異常』過濾市場共性：只標逆當日多數而動的少數，"
                 f"才是疑似提前知情的領先信號。操作建議為單日資金面參考，非重倉指令，可觀望至午報定。\n")
    return "".join(f"{ln}\n" for ln in lines)


def _auto_position(code, info, meta):
    _parts = []
    if meta.get("位置"):
        _parts.append(meta["位置"])
    _tech = info.get("tech") if isinstance(info.get("tech"), dict) else None
    _rsi = (_tech or {}).get("RSI") if _tech else info.get("RSI")
    if _rsi is not None:
        _parts.append(f"RSI={_rsi:.0f}")
    return " | ".join(_parts) if _parts else None


def _geo_lbl_for_style(style, is_qdii, base_label):
    """[V23.9 修C] 海峽通航標籤只掛在真正受能源航運影響的資產;
    黃金/紅利/電力(內需公用)不應照印「有條件通航」——原本 16/16 全同,無資訊量。"""
    if style in ("Defensive", "Dividend", "Utility", "Power"):
        return "—(非航運敏感)"
    return base_label


def _inline_md_format(text):
    """處理 inline markdown：粗體 / 信號 emoji 上色 / 代碼 / 百分比高亮"""
    # 信號 emoji 上色（最先處理避免被其他規則破壞）
    text = re.sub(r'🟢', '<span class="text-emerald-600 font-bold">🟢</span>', text)
    text = re.sub(r'🔴', '<span class="text-red-600 font-bold">🔴</span>', text)
    text = re.sub(r'🟡', '<span class="text-amber-600 font-bold">🟡</span>', text)
    text = re.sub(r'🟠', '<span class="text-orange-600 font-bold">🟠</span>', text)
    # 粗體 **xxx**
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong class="font-bold text-slate-900">\1</strong>', text)
    # 行內代碼 `xxx`
    text = re.sub(r'`([^`]+?)`', r'<code class="bg-slate-100 text-purple-700 px-1 rounded text-sm font-mono">\1</code>', text)
    # 百分比變化高亮 +X.XX% / -X.XX%
    text = re.sub(r'([+-]\d+\.\d+%)',
                  lambda m: f'<span class="font-mono font-semibold {"text-emerald-600" if m.group(1)[0] == "+" else "text-red-600"}">{m.group(1)}</span>',
                  text)
    return text


def format_events_block(events):
    """[V22.18.4] 重大事件日曆區塊格式化（早/午/收盤/盤前報共用）"""
    if events:
        block = "## 📅 重大事件日曆 (events.csv + 自動規律事件)\n\n"
        for evt in events[:6]:
            sev = evt.get("severity", "")
            sev_icon = {"critical":"🔴","high":"🟡","medium":"🟢","low":"⚪"}.get(sev, "")
            src = evt.get("source", "")
            src_tag = "🤖" if src.startswith("auto") else "✍️"
            block += (f"* **{evt.get('date','')} {evt.get('time','')}** | "
                      f"{evt.get('event','?')} | 影響: `{evt.get('impact','?')}` | "
                      f"{sev_icon}{sev} {src_tag}\n")
        block += "\n"
    else:
        block = "## 📅 重大事件日曆\n\n> 📂 近期無重大事件\n\n"
    return block


def generate_today_focus(macro_data, holdings_data, events, similarity_data):
    """
    [V22.17.4 新增] 「今日 3 大關注」自適應引擎
    [V22.17.5 hotfix] None 防禦
    
    優先級決策樹（用戶藍圖鎖定）:
      Level 1 內部生存: Tech ≥ 45% → 強制減倉
      Level 2 外部風險: VIX > 25 / 黃金 > 2.5% / Stagflation
      Level 3 戰術機會: 同質群組 / 觀察池 / Reflation
    
    自適應: 觸發 0 條 → 顯示「無重大異動」
            觸發 > 3 條 → 只顯示優先級最高 3 條
    """
    # [V22.17.5] None 防禦
    macro_data = macro_data if isinstance(macro_data, dict) else {}
    holdings_data = holdings_data if isinstance(holdings_data, dict) else {}
    events = events if isinstance(events, list) else []
    similarity_data = similarity_data if isinstance(similarity_data, dict) else {}
    
    # 嵌套 dict 也防 None
    for k in ["cny", "gold_oil"]:
        if not isinstance(macro_data.get(k), dict):
            macro_data[k] = {}
    if not isinstance(holdings_data.get("tech_concentration"), dict):
        holdings_data["tech_concentration"] = {}
    
    # [V22.17.5 hotfix B] 嵌套 numeric value 也防 None
    if not isinstance(macro_data.get("vix"), (int, float)):
        macro_data["vix"] = 18.0  # 中性 VIX
    
    focuses = []
    
    # Level 1: 內部生存（Tech 倉位）
    tech_info = holdings_data.get("tech_concentration", {})
    if tech_info.get("level") == "🔴 危急":
        focuses.append({
            "priority": 1, "icon": "🔴",
            "title": "防禦優先 - 倉位危急",
            "text": f"Tech 板塊 {tech_info['tech_pct']:.1f}% ≥ 45%，今日唯一任務：強制減倉至 40% 以下",
        })
    elif tech_info.get("level") == "🟡 警告":
        focuses.append({
            "priority": 1, "icon": "🟡",
            "title": "倉位警告",
            "text": f"Tech 板塊 {tech_info['tech_pct']:.1f}% (警戒區 40-45%)，今日 Tech 只出不進，禁止觸發主攻",
        })
    
    # Level 2: 外部系統風險
    gold_oil = macro_data.get("gold_oil", {})
    if "Stagflation" in gold_oil.get("scenario", ""):
        focuses.append({
            "priority": 2, "icon": "🚨",
            "title": "宏觀災難 - Stagflation",
            "text": gold_oil["scenario"] + "（黃金+原油+VIX 同步上行），降低所有槓桿",
        })
    elif "Risk-off" in gold_oil.get("scenario", ""):
        focuses.append({
            "priority": 2, "icon": "🔴",
            "title": "市場恐慌 - Risk-off",
            "text": gold_oil["scenario"] + "，所有 🟢 主攻強制降級為靜默",
        })
    elif macro_data.get("vix", 18) > 25:
        focuses.append({
            "priority": 2, "icon": "🔴",
            "title": "VIX 飆升",
            "text": f"VIX {macro_data['vix']:.1f} > 25 進入恐慌區，全市場降槓桿",
        })

    # [V22.22.5 修bug] 隔夜美股暴跌警報(治'SOX-3.57%卻顯示無異動')
    # 根因: 舊版只看VIX>25/gold>2.5%, 漏了隔夜SOX/NDX暴跌對A股科技開盤的壓力
    _sox = macro_data.get("sox", 0) or 0
    _ndx = macro_data.get("ndx", 0) or 0
    _vix_now = macro_data.get("vix", 18)
    if _sox <= -0.03 or _ndx <= -0.025:  # SOX跌>3% 或 NDX跌>2.5%
        focuses.append({
            "priority": 2, "icon": "🚨",
            "title": "隔夜美股暴跌 - A股科技開盤壓力",
            "text": f"SOX {_sox*100:+.2f}% / NDX {_ndx*100:+.2f}%，"
                    f"科技/QDII板塊今日開盤承壓，禁止追高、Tech只出不進",
        })
    elif (_sox <= -0.02 or _ndx <= -0.015) and _vix_now > 20:  # 中度跌+VIX偏高
        focuses.append({
            "priority": 2, "icon": "⚠️",
            "title": "隔夜美股走弱 + VIX偏高",
            "text": f"SOX {_sox*100:+.2f}% / NDX {_ndx*100:+.2f}% / VIX {_vix_now:.1f}，"
                    f"風險未解除，維持防守不追高",
        })
    
    # CNY 危險
    cny_info = macro_data.get("cny", {})
    if cny_info.get("level") == "🔴 危險":
        focuses.append({
            "priority": 2, "icon": "🔴",
            "title": "匯率破位",
            "text": f"CNY {cny_info['current']:.4f} - " + "/".join(cny_info["reason"]),
        })
    elif cny_info.get("level") == "🟡 警示":
        focuses.append({
            "priority": 2, "icon": "🟡",
            "title": "匯率走弱",
            "text": "/".join(cny_info["reason"]) + "，警惕外資撤離 A 股科技",
        })
    
    # 重大事件
    today = datetime.date.today().isoformat()
    today_events = [e for e in events if e.get("date") == today and e.get("severity") in ("critical", "high")]
    if today_events:
        evt = today_events[0]
        focuses.append({
            "priority": 2, "icon": "🚨" if evt.get("severity") == "critical" else "⚠️",
            "title": f"重大事件 - {evt.get('event','?')}",
            "text": f"{evt.get('time','')} 影響 {evt.get('impact','?')}，午報前不調倉",
        })
    
    # 輕度避險
    if "輕度避險" in gold_oil.get("scenario", ""):
        focuses.append({
            "priority": 2, "icon": "🟡",
            "title": "避險升溫",
            "text": f"黃金 {gold_oil.get('gold_ret',0)*100:+.2f}%，所有主攻 Kelly 倉位減半",
        })
    elif "Reflation" in gold_oil.get("scenario", ""):
        focuses.append({
            "priority": 3, "icon": "🟢",
            "title": "通膨交易機會",
            "text": f"黃金+原油同步走強，Cyclical 板塊機會升溫（004432 有色關注）",
        })
    
    # Level 3: 戰術機會（同質群組衝突）
    if similarity_data and similarity_data.get("conflicts"):
        focuses.append({
            "priority": 3, "icon": "👥",
            "title": "同質群組衝突",
            "text": "持倉有同質高 ETF 但信號不一致，今日需評估同步處理",
        })
    
    # 排序 + 取前 3
    focuses.sort(key=lambda x: x["priority"])
    if not focuses:
        return [{"priority": 0, "icon": "🟢",
                 "title": "今日盤面無重大異動",
                 "text": "維持既定策略，等待 09:30 早報訊號"}]
    return focuses[:3]


def _pw_freshness_tag(added_date_str):
    """[V22.84] 依 added_date 動態算保鮮期,取代硬寫死日期(F6)。
    原 L4049/4068 硬寫「數據截止2026-07-13,超過3-5交易日需重刷」,
    但今天(7/28)已15天仍每天照印→假保鮮期。改為渲染時實算天數。"""
    if not added_date_str:
        return ""
    try:
        _d0 = datetime.date.fromisoformat(str(added_date_str))
        _nd = (datetime.date.today() - _d0).days
        if _nd > 5:
            return f" ⏱️保鮮{_nd}日🔴已過期·此定性判斷勿依賴,需聯網重刷"   # [V23.4 缺陷2] 過期定性明確標不可靠
        return f" ⏱️保鮮{_nd}日✅"
    except Exception:
        return ""


def _pw_qualitative_display(pw_text, added_date_str):
    """[V23.6 B治標] 觀察池 web 初判過期時,不顯示凍結定性文字,改印過期提示,
    避免「估值分位約34%」這類千篇一律死句每天照印(治標:隱藏;治本聯網重刷另議)。"""
    _tag = _pw_freshness_tag(added_date_str)
    if "已過期" in _tag:
        import re as _re
        _m = _re.search(r"保鮮(\d+)日", _tag)
        _nd = _m.group(1) if _m else "?"
        return f"定性已過期{_nd}日·以系統實算RSI/EV為準·待重刷"
    return pw_text or ""


def _geo_icon_from_cdrain(cd: float) -> str:
    """[V22.88] 地緣圖標改跟 c_drain 聯動（取代永遠🟡的死標）。
    c_drain 已是即時由市場定價(VIX/WTI/金)推導, 圖標應如其反映真實壓力。
    strait_status=='closed_hostile' 仍強制🔴(戰爭級手動旗標優先於量化)。
    """
    if cd >= 0.45: return "🔴"
    if cd >= 0.30: return "🟠"
    if cd >= 0.15: return "🟡"
    return "🟢"
