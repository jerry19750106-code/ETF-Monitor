# -*- coding: utf-8 -*-
"""
patch_reversal_criteria.py — 「輪動反抽 vs 反轉確認」量化判定式(給 WB 接進 V22.92 收盤報)
====================================================================================
回應 7/29 實況:大盤翻紅(滬指+0.40%/創業板+1.55%)但核心 AI 硬件鏈續跌(光通信-1.14%/半導體-1.73%)
→ 是「輪動反抽」不是「反轉」。系統該自動偵測並標紅字,別把輪動誤讀成反轉害用戶接刀子。

★★★ 數據來源鐵律(我差點踩的坑,WB 務必照做):
    判定式輸入【必須】是【板塊指數/大盤指數】報酬,【不能】用 results 的 predicted_ret
    (那是場外聯接 NAV,7/29 最大才 -0.07%,根本不反映板塊 -1.14%)。
    - broad_ret     : 大盤指數(滬指或創業板)今日報酬  ← WB live 指數報價
    - core_ret      : 核心板塊(光通信/半導體 sector index)今日報酬  ← WB live 板塊
    - core_flow5    : 核心板塊機構5日淨流(億)  ← 東財 sector flow
    - idx_price/ma10: 指數收盤價 vs 10日均線  ← WB live 指數 K 線
    - today_low/prev_trough_low: 今日最低 vs 前一波低點  ← 指數 K 線
    ★ observe-only:此為【偵測+警示】,不進 EV/決策 override(避免又一個未校準的東西改信號)。
"""

BROAD_UP_TH = 0.0        # 大盤翻紅門檻(報酬>0)
CORE_DOWN_TH = -0.005    # 核心板塊「續跌」門檻 -0.5%(過濾噪音)

def assess_reversal(broad_ret=None, core_ret=None, core_flow5=None,
                    idx_price=None, idx_ma10=None, today_low=None, prev_trough_low=None):
    """回 {verdict, warning, conditions, missing}。輸入見檔頭鐵律(板塊/指數,非聯接NAV)。"""
    broad_up = (broad_ret is not None) and (broad_ret > BROAD_UP_TH)
    core_down = (core_ret is not None) and (core_ret < CORE_DOWN_TH)

    # ── 反轉確認三條件(全 True 才算反轉)──
    c1 = (core_ret is not None and core_ret >= 0) and (core_flow5 is not None and core_flow5 > 0)  # 核心止跌+機構回補
    c2 = (idx_price is not None and idx_ma10 is not None and idx_price > idx_ma10)                  # 站回10日線
    c3 = (today_low is not None and prev_trough_low is not None and today_low > prev_trough_low)    # 二次探底不創新低
    conditions = {"①核心止跌+機構回補": c1, "②站回10日線": c2, "③二次探底不創新低": c3}
    missing = [k for k, v in conditions.items() if not v]
    reversal_confirmed = c1 and c2 and c3

    # ── 判定 ──
    if reversal_confirmed:
        return {"verdict": "反轉確認", "warning": "",
                "conditions": conditions, "missing": []}
    if broad_up and core_down:
        _b = f"{broad_ret*100:+.2f}%" if broad_ret is not None else "?"
        _c = f"{core_ret*100:+.2f}%" if core_ret is not None else "?"
        return {"verdict": "輪動反抽·非反轉",
                "warning": (f"⚠️ **輪動反抽·非反轉**:大盤{_b} 但核心板塊(光通信/半導體)仍 {_c} 續跌 "
                            f"→ 紅包是消費/金融/紅利輪動撐的,你的 AI 硬件持倉沒回血。"
                            f"反轉三條件未齊(缺:{'、'.join(missing)}),視同【接刀子區】,勿加碼。"),
                "conditions": conditions, "missing": missing}
    if broad_up and not core_down:
        return {"verdict": "廣義反彈(核心未確認)", "warning":
                f"大盤翻紅、核心未明顯續跌,但反轉三條件未齊(缺:{'、'.join(missing)})→ 脆弱反抽,待確認。",
                "conditions": conditions, "missing": missing}
    return {"verdict": "無反抽訊號(大盤未翻紅)", "warning": "",
            "conditions": conditions, "missing": missing}
