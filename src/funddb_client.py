# -*- coding: utf-8 -*-
"""funddb_client —— 多年 PE 序列資料源(公開版:僅介面,不含實作)。

原作者的私人版本直連韭圈說(api.jiucaishuo.com)取得底層指數多年 PE / PB / 股息率序列。
該站 API 需要 client 端簽章,私人版的簽章做法來自對站方前端程式的分析;
為避免散布繞過第三方存取機制的內容,公開版【不提供】簽章與連線實作。

本檔保留與主程式相容的介面與名稱別名表:
- fetch_index_pe_history_funddb(index_name, indicator) -> list[(date_str, pe)] | None
- fetch_index_pe_history_funddb_meta(index_name, indicator) -> (series|None, meta)

公開版一律回 None / status="not_implemented",主程式會自動 fallback 到滾動分位
(不會假成功)。若你有合法授權的 PE 資料源,實作 `fetch_pe_series()` 即可接上。
"""

# ── 名稱別名:主程式指數名 → 資料源 gu_name;is_proxy=True 為「同板塊代理指數」 ──
FUND_NAME_ALIAS = {
    "国证芯片":         ("国证芯片", False),
    "科创50":           ("科创50", False),
    "中证消费电子50":   ("消费电子", True),
    "中证大数据产业":   ("云计算", True),
    "中证电信主题":     ("通信设备", True),
    "国证通信":         ("全指通信", True),
    "中证全指电力":     ("中证全指电力指数", False),
    "中证有色金属":     ("中证有色", False),
    "中证红利":         ("中证红利", False),
}


def resolve_alias(index_name):
    """回 (gu_name, is_proxy) 或 (None, False)。"""
    return FUND_NAME_ALIAS.get(index_name, (None, False))


def fetch_pe_series(gu_name, indicator="pe"):
    """請自行實作:回 [(YYYY-MM-DD, value), ...] 升序;取不到回 None。"""
    return None


def fetch_index_pe_history_funddb(index_name, indicator="市盈率"):
    s, _ = fetch_index_pe_history_funddb_meta(index_name, indicator)
    return s


def fetch_index_pe_history_funddb_meta(index_name, indicator="市盈率"):
    meta = {"index_name": index_name, "is_proxy": False, "gu_code": None,
            "gu_name": None, "status": "ok"}
    if not index_name:
        meta["status"] = "empty_name"
        return (None, meta)
    gname, is_proxy = resolve_alias(index_name)
    if gname is None:
        meta["status"] = "no_alias"
        return (None, meta)
    meta["gu_name"], meta["is_proxy"] = gname, is_proxy
    s = fetch_pe_series(gname, {"市盈率": "pe", "市净率": "pb", "股息率": "xilv"}.get(indicator, indicator))
    if not s:
        meta["status"] = "not_implemented"
        return (None, meta)
    return (s, meta)
