"""訊號計算純函式(任務C 重構 phase 1,自 V24_19_ETF_System.py 原樣搬入)。

本檔函式本體與搬移前【字元級一致】(tests/test_characterization.py 以 SHA-256 驗證),
且只依賴標準庫/numpy 與同檔其他函式,不讀取主程式任何全域狀態。
主程式以原名匯入,外部介面不變。
"""
import datetime


def compute_fragility(prev3_ret=0.0, prev_day_ret=0.0, crash_in_5d=False,
                      main_flow_neg_days=0, rsi=50.0, z_score=0.0):
    """A股內生脆弱度 0-100(越高越危險)。全部輸入皆系統既有數據。
    prev3_ret/prev_day_ret 為小數(0.05=5%)。回傳 int。"""
    s = 0
    p3 = (prev3_ret or 0) * 100
    pd = (prev_day_ret or 0) * 100
    # ① 拋物線(前3日暴漲=不可持續) 0-30
    if   p3 >= 8: s += 30
    elif p3 >= 5: s += 20
    elif p3 >= 3: s += 10
    # ② 波動聚集:前一日大跌(崩後再崩機率高) 0-25
    if   pd <= -5: s += 25
    elif pd <= -3: s += 18
    elif pd <= -2: s += 10
    # ③ 近5日內有崩盤日 0-15
    if crash_in_5d: s += 15
    # ④ 主力連續淨流出天數 0-25
    # [V22.68] main_flow_neg_days 為 None 時原本當 0 →「沒有連續流出日」是一個【結論】。
    #   脆弱度是 EV 最大壓制因子(#207 實證),輸入缺失卻照算 = 給出看似正常的假分數。
    #   函式契約回 int 不便改,故【顯式標記並告警】,讓缺口可見(不靜默)。
    if main_flow_neg_days is None:
        print("   ⚠️ [脆弱度] main_flow_neg_days 缺失 → 該維度(0-25分)未計入,分數偏低,請留意")
    else:
        s += min(25, main_flow_neg_days * 5)
    # ⑤ RSI 高位 0-15
    _r = rsi if rsi is not None else 50
    if   _r >= 80: s += 15
    elif _r >= 70: s += 10
    elif _r >= 60: s += 5
    # ⑥ Z 異常 0-10
    _z = abs(z_score or 0)
    if   _z >= 2.0: s += 10
    elif _z >= 1.5: s += 5
    return max(0, min(100, int(s)))


def fragility_action(score):
    """脆弱度 → (EV調整, 標籤, 是否禁主攻)"""
    if score >= 70:
        return (-20, f"🔴 內生崩盤風險高(脆弱度{score})·禁主攻", True)
    if score >= 50:
        return (-12, f"🟠 日內反轉風險(脆弱度{score})·主攻降試水", False)
    if score >= 30:
        return (-5,  f"🟡 脆弱度偏高({score})", False)
    return (0, "", False)


def identify_state_actor(broad_etf_flow_yi=None, index_ret_pct=0.0,
                         inst_yi=0.0, retail_yi=0.0):
    """[V22.35] 國家隊/主權資金 護盤跡象識別 —— 補使用者早報定義缺的「主權基金、國家操盤單位走向」。
    ★誠實前提:A股【無公開即時的國家隊持倉數據】(匯金/證金持倉只在季報披露)。
      本函式做的是【行為特徵推斷】,不是直接識別。特徵取自公開研究對 2015/2024 兩輪護盤的歸納:
        ① 寬基ETF(滬深300/上證50/中證500)出現【異常大額淨流入】
        ② 同時大盤【下跌】(逆勢買 = 護盤簽名;順勢買則多為一般配置盤)
        ③ 機構淨流入 vs 散戶淨流出(國家隊走機構通道)
      三者同時成立 → 高度疑似;僅①② → 疑似;否則無跡象。
    ⚠️ 這是【機率性推斷】,不得當成事實斷言(㉒);報告須標「跡象/疑似」而非「確認」。
    回 (signal, strength, reason)。broad_etf_flow_yi=None(無資料)→ 誠實回「無法判斷」。
    """
    if broad_etf_flow_yi is None:
        return ("⚪ 國家隊:無寬基ETF資金流數據,無法判斷", 0,
                "需接寬基ETF(510300/510050/510500)當日淨流入")
    big_inflow = broad_etf_flow_yi >= 50          # 億;閾值待實盤校準(鐵律⑳)
    mid_inflow = broad_etf_flow_yi >= 20
    falling    = index_ret_pct < -0.5
    inst_led   = (inst_yi > 0) and (retail_yi < 0)
    if big_inflow and falling and inst_led:
        return ("🏛️ **國家隊護盤跡象:高度疑似**", 3,
                f"寬基ETF淨流入{broad_etf_flow_yi:+.0f}億 + 大盤{index_ret_pct:+.2f}%下跌(逆勢買) "
                f"+ 機構{inst_yi:+.1f}億/散戶{retail_yi:+.1f}億(機構通道) → 三特徵齊備")
    if (big_inflow or mid_inflow) and falling:
        return ("🏛️ 國家隊護盤跡象:疑似", 2,
                f"寬基ETF淨流入{broad_etf_flow_yi:+.0f}億 + 大盤{index_ret_pct:+.2f}%下跌(逆勢買) "
                f"→ 兩特徵;機構/散戶通道特徵未齊")
    if big_inflow and not falling:
        return ("⚪ 寬基ETF大額流入但大盤未跌 → 較可能是一般配置盤,非護盤", 1,
                f"寬基ETF{broad_etf_flow_yi:+.0f}億,大盤{index_ret_pct:+.2f}%")
    return ("⚪ 國家隊:無明顯護盤跡象", 0,
            f"寬基ETF{broad_etf_flow_yi:+.0f}億(未達20億門檻)或大盤未跌")


def interpret_flow_behavior(flow5_yi, today_ret_pct, main_flow_yi=0.0):
    """買賣方行為解讀:5日資金流 × 今日漲跌 → 誰在收網?誰在絞殺?"""
    # [V22.49 治靜默造假] ★原本 `f5 = flow5_yi or 0` 把 None 當 0 →
    #   資料斷線時報告會寫「⚪ 無明顯主力布局/5日資金流不顯著」= **把「不知道」講成「沒有」**。
    #   2026-07-16 起東財 flow5 全 NULL 已 7 天,期間報告一直如此陳述。
    #   NULL 與 0 語義完全不同,必須分開。
    if flow5_yi is None:
        return ("⬜ **資金流資料不可用**",
                "5日主力資金流【未取得】(東財fflow端點已結構性封鎖·非限流,本機亦然,見L5125)"
                "→ 此維度**無法判讀**,請勿當作『無主力布局』;"
                "進場條件④亦無法檢核(趨勢改以歷史DB為準)")
    f5 = flow5_yi
    r  = today_ret_pct or 0
    accum = abs(f5) >= 3.0          # 5日累積顯著
    if not accum:
        return ("⚪ 無明顯主力布局", f"5日資金流{f5:+.1f}億(不顯著)")
    if f5 > 0 and r > 1.0:
        return ("🟢 **買家收網**", f"5日淨流入{f5:.1f}億(布局)→今日拉升{r:+.1f}%,籌碼鎖定後拉抬")
    if f5 > 0 and r < -1.0:
        return ("🟡 **買家被套/假布局**", f"5日淨流入{f5:.1f}億卻跌{r:+.1f}%,主力可能誘多或被外力打斷")
    if f5 < 0 and r < -1.0:
        return ("🔴 **賣家絞殺成功**", f"5日淨流出{abs(f5):.1f}億(派發)→今日殺跌{r:+.1f}%,派發完成")
    if f5 < 0 and r > 1.0:
        return ("🟠 **逆勢拉升/空頭回補**", f"5日淨流出{abs(f5):.1f}億卻漲{r:+.1f}%,或為空頭回補、散戶接盤")
    return ("⚪ 盤整", f"5日{f5:+.1f}億,今日{r:+.1f}%")


def predict_tomorrow(rsi=50.0, z_score=0.0, trend=0.0, main_flow_yi=0.0,
                     flow5_yi=0.0, fragility=0, today_ret=0.0, has_event=False,
                     news_score=0.0):
    """T+1 方向預測。回傳 (pred_ret_pct, confidence, reason)。
    ★ 權重未經真數據校準(見 E-LOG #258),observe 期僅供顯示。

    輸入全部來自【今日收盤】即可得的數據:
      rsi/z_score/trend : 今日收盤技術面
      main_flow_yi      : 今日主力淨流入(億)  ← ★領先指標
      flow5_yi          : 近5日主力淨流入(億) ← ★領先指標
      fragility         : V22.28 內生脆弱度
      today_ret         : 今日漲跌(小數)
      has_event         : 明日有無重大事件(LPR/FOMC)
    """
    reasons = []
    # ① 資金流(領先指標,權重最高) — 主力今日買 → 明日易拉
    flow_s = 0.0
    if main_flow_yi is not None:
        flow_s = max(-1.0, min(1.0, main_flow_yi / 3.0))   # ±3億 → ±1.0
        if abs(main_flow_yi) >= 1.0:
            reasons.append(f"主力{'淨流入' if main_flow_yi>0 else '淨流出'}{abs(main_flow_yi):.1f}億")
    flow5_s = 0.0
    if flow5_yi is not None:
        flow5_s = max(-1.0, min(1.0, flow5_yi / 10.0))     # ±10億(5日) → ±1.0

    # ② 均值回歸(RSI 極端 → 反向) — 同步指標,做修正
    rev_s = 0.0
    _r = rsi if rsi is not None else 50
    if _r >= 80:   rev_s = -0.8; reasons.append(f"RSI{_r:.0f}極度超買→回調壓力")
    elif _r >= 70: rev_s = -0.4; reasons.append(f"RSI{_r:.0f}超買")
    elif _r <= 20: rev_s = +0.8; reasons.append(f"RSI{_r:.0f}極度超賣→反彈動能")
    elif _r <= 30: rev_s = +0.4; reasons.append(f"RSI{_r:.0f}超賣")

    # ③ 趨勢延續(動能)
    mom_s = max(-1.0, min(1.0, (trend or 0)))

    # ④ 脆弱度阻尼(高脆弱 → 下行風險,壓抑偏多)
    frag_pen = 0.0
    if fragility >= 70:   frag_pen = -0.5; reasons.append(f"脆弱度{fragility}極高")
    elif fragility >= 50: frag_pen = -0.3; reasons.append(f"脆弱度{fragility}偏高")

    # ⑤ 產業新聞(使用者:新聞是公式的一部分,有占比)
    news_s = max(-1.0, min(1.0, news_score or 0))
    if abs(news_s) >= 0.3:
        reasons.append(f"新聞{'偏多' if news_s>0 else '偏空'}({news_s:+.2f})")

    # 合成(權重★未校準,observe期校準)
    score = (flow_s * 0.28 + flow5_s * 0.12 + rev_s * 0.25
             + mom_s * 0.12 + frag_pen * 0.08 + news_s * 0.15)
    score = max(-1.0, min(1.0, score))
    pred = round(score * 2.5, 2)          # 映射到 ±2.5% 合理日波動

    # 信心度
    conf = "低"
    # [V22.68] main_flow_yi 為 None 時原本當 0 → 條件不成立、信心維持低,行為碰巧正確,
    #   但語義錯(把「不知道」當「流量為零」)。顯式化,避免日後有人改條件方向時踩雷。
    if abs(score) >= 0.5 and main_flow_yi is not None and abs(main_flow_yi) >= 1.0: conf = "中"
    if abs(score) >= 0.7 and abs(main_flow_yi or 0) >= 2.0: conf = "較高"
    if has_event:
        conf = "低"; reasons.append("明日有重大事件→不確定性高")

    # ── [V24.30] ★因子貢獻攤開(使用者 2026-08-11 三問的共同根因) ──
    #   問①「只有一個新聞分,其他的呢」→ 報告只印新聞欄,其餘因子從未攤開給人看。
    #   問②「無明顯信號是為什麼」→ 根因:reasons 只在【跨過門檻】時 append
    #     (主力≥1億/RSI≥70/脆弱≥50/新聞≥0.3),但 pred 是【連續計算】的
    #     → 出現「預測 +0.21% 卻說不出理由」= 數字有、依據沒有。
    #   修法:一律回傳每個因子的【實際 pp 貢獻】,並讓 reason 至少列出前兩大貢獻者,
    #        不再有「無明顯信號」這種空回答。
    _contrib = {
        "主力當日": round(flow_s * 0.28 * 2.5, 3),
        "主力5日":  round(flow5_s * 0.12 * 2.5, 3),
        "RSI反轉":  round(rev_s * 0.25 * 2.5, 3),
        "趨勢動能":  round(mom_s * 0.12 * 2.5, 3),
        "脆弱阻尼":  round(frag_pen * 0.08 * 2.5, 3),
        "產業新聞":  round(news_s * 0.15 * 2.5, 3),
    }
    if not reasons:
        _top = sorted(_contrib.items(), key=lambda kv: -abs(kv[1]))[:2]
        if any(abs(v) >= 0.01 for _, v in _top):
            reasons = [f"{k}{v:+.2f}pp" for k, v in _top if abs(v) >= 0.01]
            reasons.append("(各因子皆未達顯著門檻,以下為最大貢獻者)")
        else:
            reasons.append("全因子近零:無資金流數據且技術面中性 → 預測值不具方向意義")
    return (pred, conf, " | ".join(reasons[:3]), _contrib)


def effective_sox_for(cond_beta, sox_ret):
    """治 F1/Gemini G4:|β|<0.3 = 脫鉤 → 回 None(【不引用】SOX),而非乘負β當反向推力。"""
    if sox_ret is None or cond_beta is None:
        return sox_ret
    return None if abs(cond_beta) < 0.30 else sox_ret


def judge_order_flow(bd, price_ret=None):
    """[V22.24.9] 大小單『價量背離』判定 — 單日資料能合法提供的唯一新資訊。

    認知收尾(E-LOG#121 + V22.24.9審查): 單日大小單淨額 ≡ 主力方向——機構(超大+大)
      ≡主力為東財定義恆等, 散戶(中+小)為其補數, 非獨立信號。故純『方向』已被
      5日主力流入/EV其他管道反映, 再據此調EV=重複計算(且與apply_flow_divergence疊加)。
    本判定只在【價量背離】動EV, 因唯有此情境帶來新資訊:
      派發背離: 同日價漲 + 主力淨流出 → 🚩 EV-5 (誘多的日線近似)
      吸籌背離: 同日價跌 + 主力淨流入 → 🔴 EV+5
      價量同向/無顯著 → ➡️ EV不動(不重複計已被價格反映的方向)
    真·日內誘多(早盤砸大單→午後派發)仍需分鐘級時序, 留V22.19+。
    輸入: bd(get_order_flow_breakdown結果); price_ret(同日場內漲跌, 如intraday_proxy_ret)。
    回傳 {verdict, icon, ev_adj, detail} 或 None。
    """
    if not bd:
        return None
    main = bd.get("主力") or 0
    price_ret = price_ret if price_ret is not None else 0.0
    BIG_TH = 0.05      # 主力方向閾值(億), 過濾噪音
    PRICE_TH = 0.003   # 同日價格背離門檻(±0.3%), 與美股方向判定同尺度
    # 派發背離: 價漲而主力撤(誘多的日線近似)
    if price_ret > PRICE_TH and main < -BIG_TH:
        return {"verdict": "價漲主力撤(疑派發)", "icon": "🚩", "ev_adj": -5,
                "detail": f"今日價{price_ret:+.1%}漲卻主力淨流出{main:+.2f}億→單日派發背離(防誘多日線近似)"}
    # 吸籌背離: 價跌而主力吸
    if price_ret < -PRICE_TH and main > BIG_TH:
        return {"verdict": "價跌主力吸(疑吸籌)", "icon": "🔴", "ev_adj": +5,
                "detail": f"今日價{price_ret:+.1%}跌卻主力淨流入{main:+.2f}億→單日吸籌背離"}
    # 價量同向(方向已被價格/EV反映)或無顯著 → 不動EV
    _dir = "主力淨流入" if main > BIG_TH else "主力淨流出" if main < -BIG_TH else "主力中性"
    return {"verdict": f"{_dir}(與價同向/無背離,不重複計EV)", "icon": "➡️", "ev_adj": 0,
            "detail": f"主力{main:+.2f}億 vs 價{price_ret:+.1%}: 同向或無顯著背離"}


def compute_realized_vol(daily_returns, lookback=20, annualize=True):
    """[V22.26] 近lookback日已實現波動率。daily_returns:小數序列(時間正序)。
    資料不足(<5)回None→由呼叫端fallback(不假裝)。"""
    rs = [r for r in daily_returns[-lookback:] if r is not None]
    if len(rs) < 5:
        return None
    mean = sum(rs) / len(rs)
    var = sum((r - mean) ** 2 for r in rs) / (len(rs) - 1)
    sd = var ** 0.5
    return round(sd * (252 ** 0.5), 4) if annualize else round(sd, 4)


def vol_scale_factor(realized_vol, ref_vol, cap_lo=0.30, cap_hi=1.50):
    """[V22.26] vol-scaling核心乘數=clamp(ref_vol/realized_vol, cap_lo, cap_hi)。
    模式B(本系統採用): ref_vol=該ETF自身60日常態波動 → 只修剪「比自己平常更狂」
    的標的(froth),不懲罰天生高波動持倉,避免把整本帳壓到低絕對目標(=不過度保守)。
    無資料回1.0(不縮放,誠實)。"""
    if realized_vol is None or realized_vol <= 0 or ref_vol is None or ref_vol <= 0:
        return 1.0
    return round(max(cap_lo, min(cap_hi, ref_vol / realized_vol)), 4)


def factor_attribution(asset_rets, mkt_rets, label="大盤"):
    """[V24.5 ⑩] 單因子歸因: r_i = α + β·r_m + ε。
    回傳 α(年化%)、β、R²、t(α)、N,並誠實標註限制。無 numpy 依賴。
    用法:asset_rets/mkt_rets 為日報酬(小數);n>=5 才可估。"""
    import math
    n = min(len(asset_rets), len(mkt_rets))
    if n < 5:
        return {"ok": False, "reason": f"樣本不足(n={n}<5)", "n": n}
    y = asset_rets[:n]; x = mkt_rets[:n]
    mx = sum(x) / n; my = sum(y) / n
    sxx = sum((xi - mx) ** 2 for xi in x)
    sxy = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    if sxx <= 1e-12:
        return {"ok": False, "reason": "市場報酬無變異,無法估計β", "n": n}
    beta = sxy / sxx
    alpha = my - beta * mx
    ss_tot = sum((yi - my) ** 2 for yi in y)
    ss_res = sum((yi - (alpha + beta * xi)) ** 2 for xi, yi in zip(x, y))
    r2 = (1 - ss_res / ss_tot) if ss_tot > 1e-12 else 0.0
    dof = n - 2
    se_res = (ss_res / dof) ** 0.5 if dof > 0 else 0.0
    _denom = (1.0 / n) + (mx ** 2) / sxx
    se_a = se_res * _denom ** 0.5 if _denom > 0 else 0.0
    t_a = (alpha / se_a) if se_a > 1e-12 else 0.0
    return {"ok": True, "alpha_pct_ann": round(alpha * 252 * 100, 2),
            "beta": round(beta, 3), "r2": round(r2, 3), "t_alpha": round(t_a, 2),
            "n": n, "label": label,
            "caveat": f"⚠️單因子OLS,樣本n={n};α未經多因子/行業中性化,僅觀察用"}


def analyze_3day_trend(prices):
    """
    [V22.17.4 新增] 3 日趨勢分析（識別死貓反彈/動能耗竭/反覆震盪）
    
    輸入: prices = [day_-4, day_-3, day_-2, day_-1, today] (5 個收盤價或漲跌率)
    輸出: dict {
        "returns_3d": [t-2, t-1, t] 3 日漲跌%
        "cum_3d": 3 日累計%
        "stdev": 3 日標準差
        "pattern": "死貓反彈"/"動能耗竭"/"反覆震盪"/"穩定上行"/"穩定下行"/"中性"
    }
    """
    if not prices or len(prices) < 4:
        return {"returns_3d": [], "cum_3d": 0, "stdev": 0, "pattern": "資料不足"}
    
    # 計算 3 日漲跌（從 t-3 到 t-2, t-2 到 t-1, t-1 到 t）
    returns = []
    for i in range(max(0, len(prices)-3), len(prices)):
        if i == 0:
            continue
        if prices[i-1] != 0:
            ret = (prices[i] - prices[i-1]) / prices[i-1]
            returns.append(ret)
    
    if not returns:
        return {"returns_3d": [], "cum_3d": 0, "stdev": 0, "pattern": "資料不足"}
    
    cum = sum(returns)
    avg = cum / len(returns)
    var = sum((r - avg) ** 2 for r in returns) / len(returns)
    stdev = var ** 0.5
    
    # 模式判定（基金經理人視角）
    today_ret = returns[-1]
    early_cum = sum(returns[:-1]) if len(returns) > 1 else 0
    
    pattern = "中性"
    if stdev > 0.025 and abs(cum) < 0.01:
        pattern = "反覆震盪（資金分歧）"
    elif early_cum < -0.03 and today_ret > 0.005:
        pattern = "死貓反彈（注意持續性）"
    elif early_cum > 0.04 and abs(today_ret) < 0.005:
        pattern = "動能耗竭（注意轉折）"
    elif cum > 0.02 and stdev < 0.015:
        pattern = "穩定上行"
    elif cum < -0.02 and stdev < 0.015:
        pattern = "穩定下行"
    
    return {
        "returns_3d": returns,
        "cum_3d": cum,
        "stdev": stdev,
        "pattern": pattern,
    }


def analyze_cny_dual_track(cny_today, cny_yesterday=None, cny_20ma=None):
    """
    [V22.17.4 新增] USD/CNY 雙軌制警戒分析
    
    雙軌:
      動態觸發: 日內 +0.3% / 20MA 偏離 +1% / +2%
      絕對觸發: ≥ 7.10
    
    返回: dict {
        "current": 當前值, "vs_yesterday": 日內變化, "vs_20ma": 偏離 20MA %
        "level": "🟢健康" / "🟡警示" / "🔴危險"
        "reason": 觸發原因
    }
    """
    result = {
        "current": cny_today, "vs_yesterday": None, "vs_20ma": None,
        "level": "🟢 健康", "reason": [],
    }
    
    if cny_yesterday and cny_yesterday > 0:
        day_change = (cny_today - cny_yesterday) / cny_yesterday
        result["vs_yesterday"] = day_change * 100
        if day_change > 0.003:
            result["level"] = "🟡 警示"
            result["reason"].append(f"日內貶值 +{day_change*100:.2f}% > 0.3%")
    
    if cny_20ma and cny_20ma > 0:
        ma_dev = (cny_today - cny_20ma) / cny_20ma
        result["vs_20ma"] = ma_dev * 100
        if ma_dev > 0.02:
            result["level"] = "🔴 危險"
            result["reason"].append(f"突破 20MA +{ma_dev*100:.2f}% > 2%")
        elif ma_dev > 0.01:
            if result["level"] == "🟢 健康":
                result["level"] = "🟡 警示"
            result["reason"].append(f"突破 20MA +{ma_dev*100:.2f}% > 1%")
    
    # 絕對位（最高優先級）
    if cny_today >= 7.10:
        result["level"] = "🔴 危險"
        result["reason"].insert(0, f"絕對位 {cny_today:.4f} ≥ 7.10 心理關卡")
    
    if not result["reason"]:
        result["reason"].append("未觸發任何警戒")
    return result


def analyze_gold_oil_seesaw(gold_ret, wti_ret, vix_val):
    """
    [V22.17.4 新增] 黃金 vs 原油「蹺蹺板」劇本判定
    [V22.17.5 hotfix] None 防禦
    
    4 種劇本:
      Risk-off  恐慌避險:  黃金↑ + 原油↓ + VIX↑
      Reflation 通膨交易:  黃金↑ + 原油↑
      Risk-on   樂觀:      黃金↓ + 原油↑ + VIX↓
      Stagflation 滯脹:    黃金↑ + 原油↑ + VIX↑ (最壞)
    
    對 EV 影響（建議調整）:
      Risk-off →  Defensive +10% / Tech -5% / Cyclical -10%
      Reflation → Cyclical +10% / Defensive +5%
      Risk-on →   Tech +5% / Defensive -5%
      Stagflation → 全部觀望
    """
    # [V22.17.5] None 防禦
    gold_ret = gold_ret if isinstance(gold_ret, (int, float)) else 0.0
    wti_ret  = wti_ret  if isinstance(wti_ret, (int, float))  else 0.0
    vix_val  = vix_val  if isinstance(vix_val, (int, float))  else 18.0  # 中性 VIX 預設
    
    gold_up = gold_ret > 0.005
    gold_strong = gold_ret > 0.015
    oil_up = wti_ret > 0.005
    oil_down = wti_ret < -0.005
    vix_high = vix_val > 22
    
    scenario = "🔄 中性"
    impact = {}
    
    if gold_strong and oil_down and vix_high:
        scenario = "🔴 Risk-off（恐慌避險）"
        impact = {"Defensive": +10, "Tech": -5, "Cyclical": -10, "QDII": -5}
    elif gold_up and oil_up and vix_high:
        scenario = "🚨 Stagflation（滯脹，最壞）"
        impact = {"All": "全部觀望，降低槓桿"}
    elif gold_up and oil_up:
        scenario = "🟡 Reflation（通膨交易升溫）"
        impact = {"Cyclical": +10, "Defensive": +5, "Tech": 0}
    elif not gold_up and oil_up and not vix_high:
        scenario = "🟢 Risk-on（樂觀）"
        impact = {"Tech": +5, "Defensive": -5, "Cyclical": +5}
    elif gold_strong and not oil_up:
        scenario = "🟡 輕度避險升溫"
        impact = {"Defensive": +5, "Tech": -3}
    
    return {"scenario": scenario, "impact": impact,
            "gold_ret": gold_ret, "oil_ret": wti_ret, "vix": vix_val}


def analyze_tech_concentration(current_styles_pct):
    """
    [V22.17.4 新增] Tech 集中度兩段式警戒
    
    閾值（用戶 5/30 拍板）:
      🟢 < 40%:    健康
      🟡 40-44.9%: 警告（只出不進）
      🔴 ≥ 45%:    危急（強制減倉）
    """
    tech = current_styles_pct.get("Tech", 0)
    qdii = current_styles_pct.get("QDII", 0)
    
    if tech >= 45:
        level, action = "🔴 危急", "強制減倉至 40% 以下"
    elif tech >= 40:
        level, action = "🟡 警告", "只出不進，禁止觸發 🟢 主攻"
    else:
        level, action = "🟢 健康", "正常操作"
    
    return {"tech_pct": tech, "qdii_pct": qdii, "level": level, "action": action}


def _pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n; my = sum(ys) / n
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs); syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return None
    return sxy / ((sxx ** 0.5) * (syy ** 0.5))


def _spearman(xs, ys):
    def _rank(vals):
        order = sorted(range(len(vals)), key=lambda i: vals[i])
        r = [0] * len(vals)
        for rk, i in enumerate(order):
            r[i] = rk
        return r
    return _pearson(_rank(xs), _rank(ys))


def _action_to_symbol(action: str) -> str:
    """把完整 action 字串轉成單一符號"""
    if "主攻" in action:
        return "🟢"
    if "減倉" in action:
        return "🔴"
    if "熔斷" in action or "極端" in action:
        return "🔴"
    return "🟡"


def _compute_judgment(h_symbol, actual_pct):
    """[V22.25.11] 由收盤信號 + 實際NAV% 算判定(寫純文字值,避開 emoji 公式在 Excel 的脆弱性)。
    鐵律:actual_pct=None 表示實際NAV尚未結算 → 回「待結算」，**絕不用預測值冒充實際**
    (WorkBuddy v4 把未結算日的 J 填成 pred → 判定拿預測比自己 → 假✅對,本函數杜絕之)。
    語意修正:🟢主攻預判漲,跌了是『假突破/方向錯』不是『踏空』(踏空專指出場卻漲)。
    🟡靜默是『無方向押注』→ 中性,不判對錯(僅附註當日波動供參),避免把『沒押』當『押錯』。"""
    if h_symbol in (None, "—", ""):
        return "—"
    if actual_pct is None:
        return "待結算"
    a = float(actual_pct)
    if "🟢" in str(h_symbol):       # 主攻 = 預判上漲
        return "✅對" if a > 0 else "❌假突破"
    if "🔴" in str(h_symbol):       # 減倉 = 預判下跌
        return "✅對" if a < 0 else "❌踏空"
    # 🟡 靜默 = 無方向押注 → 中性(附當日波動,但不計對錯)
    return f"🟡中性(動{a:+.1f}%)" if abs(a) >= 1 else "🟡中性"


def derive_c_drain(vix, wti, wti_ret, gold_ret, manual_override=None, override_as_of=None, override_ttl_days=2):
    """[V22.25.5] 自動推導地緣溫度c_drain — 取代手填(用戶定調: 情報採集是系統的事)。
    從已抓的市場數據自動算: VIX(恐慌)+WTI急漲(霍爾木茲供應憂慮)+金價急動(避險情緒)。
    這些是地緣緊張最直接的市場定價, 系統本就在抓, 無需人工輸入。
    回傳 (c_drain, reason)。manual_override: brain.json若有更高人工值則取之(突發加碼)。
    基準0.20(平靜), 封頂0.60(避免過度壓制EV)。
    """
    base = 0.20
    score = 0.0
    reasons = []
    if vix and vix > 18:
        v_add = min(0.20, (vix - 18) / 10 * 0.20)
        score += v_add
        reasons.append(f"VIX{vix:.0f}偏高(+{v_add:.2f})")
    if wti_ret and wti_ret > 0.03:
        w_add = min(0.20, wti_ret / 0.10 * 0.20)
        score += w_add
        reasons.append(f"WTI急漲{wti_ret*100:+.1f}%(+{w_add:.2f})")
    if gold_ret and abs(gold_ret) > 0.015:
        g_add = min(0.10, abs(gold_ret) / 0.05 * 0.10)
        score += g_add
        _dir = "避險買盤" if gold_ret > 0 else "避險回吐"
        reasons.append(f"金價{gold_ret*100:+.1f}%{_dir}(+{g_add:.2f})")
    c_drain = round(min(0.60, base + score), 2)
    # [V23.2 治c_drain釘死] 人工加碼只在時間戳TTL內有效;無時間戳/過期→棄用改實時,避免突發值(如0.45)永久釘死
    if manual_override is not None and manual_override > c_drain:
        _ov_fresh = False
        if override_as_of:
            try:
                _asof = datetime.date.fromisoformat(str(override_as_of)[:10])
                _age = (datetime.date.today() - _asof).days
                _ov_fresh = (_age <= override_ttl_days)
                if not _ov_fresh:
                    reasons.append(f"(人工值{manual_override:.2f}已過期{_age}天→棄用改實時)")
            except Exception:
                reasons.append(f"(人工值{manual_override:.2f}時間戳無效→棄用改實時)")
        else:
            reasons.append(f"(人工值{manual_override:.2f}無時間戳c_drain_as_of→棄用改實時,防釘死)")
        if _ov_fresh:
            c_drain = round(manual_override, 2)
            reasons.append(f"人工加碼至{c_drain:.2f}(突發·效期內)")
    reason = " + ".join(reasons) if reasons else "市場平靜(基準0.20)"
    return c_drain, reason
