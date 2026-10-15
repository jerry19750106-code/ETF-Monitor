"""持倉成本本純函式(任務C 重構 phase 1,自 V24_19_ETF_System.py 原樣搬入)。

本檔函式本體與搬移前【字元級一致】(tests/test_characterization.py 以 SHA-256 驗證),
且只依賴標準庫/numpy 與同檔其他函式,不讀取主程式任何全域狀態。
主程式以原名匯入,外部介面不變。
"""


def _pb_status_from_pct(pct):
    if pct >= 20:  return "🛡️🛡️"
    if pct >= 5:   return "🛡️"
    if pct >= -3:  return "⚖️"
    if pct >= -10: return "⚠️"
    return "🩹"


def _pb_parse_float(s, default=0.0):
    try:
        s = (s or "").strip()
        return float(s) if s else default
    except (ValueError, TypeError):
        return default


def positionbook_diff_audit(auto_costs, legacy_costs, tol=1.0):
    """並存對拍：自動 vs LEGACY，回傳告警（空=一致）"""
    warns = []
    for code in sorted(set(auto_costs) | set(legacy_costs)):
        if code not in auto_costs:
            warns.append(f"  ❌ {code} LEGACY有自動無"); continue
        if code not in legacy_costs:
            warns.append(f"  ❌ {code} 自動有LEGACY無"); continue
        a, l = auto_costs[code], legacy_costs[code]
        for fld in ("buy", "sell", "current", "total_pnl"):
            if abs(a[fld] - l[fld]) > tol:
                warns.append(f"  ❌ {code}.{fld}: 自動={a[fld]:.2f} vs LEGACY={l[fld]:.2f}")
    return warns


def classify_position_status(pnl_pct):
    """
    [V22.17.4] 分類持倉狀態（用於戰局摘要分組）
    返回 (sort_priority, group_label)
    """
    if pnl_pct >= 20:    return (1, "🛡️🛡️ 強勢護城河 (≥+20%)")
    if pnl_pct >= 5:     return (2, "🛡️ 浮盈 (+5%~+20%)")
    if pnl_pct >= -3:    return (3, "⚖️ 打平 (-3%~+5%)")
    if pnl_pct >= -10:   return (4, "⚠️ 小虧 (-10%~-3%)")
    return                      (5, "🚨 重虧 (<-10%)")
