"""實現報酬的單一來源(single source of truth):realized_returns 表。

為什麼需要:
  原本同一檔基金同一天的「實際報酬」散落在三處,且來源不同:
    - predictions.actual_ret        三條回填路徑誰先跑就用誰(場外 NAV / 場內代理 ETF / Yahoo 收盤)
    - daily_snapshot.today_ret      實際存的是【系統當日估值】(場內代理經 RSI 截斷與 ±8% 夾擠),不是實現值
    - capital_flow_daily.nav_ret    名為 NAV,實為盤中最後一次觀測到的場內代理漲跌(百分比)
  同日兩兩相關只有 0.28 / 0.42,下游(命中率、IC、Kelly、歸因)各讀各的,結論互相矛盾。

規則:
  1. 每個 (code, 日) 只有一個實現報酬,附來源 src。報酬一律以【小數】存(0.012 = +1.2%)。
  2. 來源有等級,高等級可覆蓋低等級,反之不行;同等級先寫者為準(不被後來的同級值改寫)。
       nav         場外基金官方單位淨值 —— 投資人實際成交的價格,權威
       exch_close  上市標的(台股 ETF)交易所收盤價 —— 該標的本身的成交價,權威
       legacy      遷移前的歷史值,來源無法追溯(可能是 NAV,也可能是代理)
       proxy       場內代理 ETF 的當日漲跌 —— 只是場外基金的估計,有追蹤誤差
  3. 日期是該報酬【實際發生的交易日】,由資料本身的日期決定,不由執行日推算。
"""
import datetime
import sqlite3

SRC_RANK = {"nav": 3, "exch_close": 3, "legacy": 2, "proxy": 1}
AUTHORITATIVE = 3

DDL = ("CREATE TABLE IF NOT EXISTS realized_returns("
       "code TEXT NOT NULL, date TEXT NOT NULL, ret REAL NOT NULL, src TEXT NOT NULL, "
       "ts TEXT NOT NULL, PRIMARY KEY(code, date))")


def rank(src):
    return SRC_RANK.get(src, 0)


def rank_sql(col):
    """SQL 中把來源欄位換成等級;NULL(遷移前寫入、來源不明)視同 legacy。"""
    cases = " ".join(f"WHEN '{k}' THEN {v}" for k, v in SRC_RANK.items())
    return f"(CASE {col} {cases} ELSE {SRC_RANK['legacy']} END)"


def ensure(conn):
    conn.execute(DDL)


def get(conn, code, date):
    """回 (ret, src);無則 None。"""
    ensure(conn)
    row = conn.execute("SELECT ret, src FROM realized_returns WHERE code=? AND date=?",
                       (code, date)).fetchone()
    return (row[0], row[1]) if row else None


def upsert(conn, code, date, ret, src, now=None):
    """寫入一筆實現報酬。回 True 表示寫入(新增或以更高等級覆蓋),False 表示被既有的同級/更高級值擋下。"""
    if src not in SRC_RANK:
        raise ValueError(f"未知來源 {src!r};必須是 {sorted(SRC_RANK)}")
    if ret is None:
        raise ValueError("ret 不可為 None:缺值應不寫,而不是寫入空值")
    datetime.date.fromisoformat(date)          # 日期格式錯誤直接拋出,不寫入髒資料
    cur = get(conn, code, date)
    if cur is not None and rank(cur[1]) >= rank(src):
        return False
    now = now or datetime.datetime.now().isoformat(timespec="seconds")
    conn.execute("INSERT OR REPLACE INTO realized_returns(code, date, ret, src, ts) VALUES(?,?,?,?,?)",
                 (code, date, float(ret), src, now))
    return True


def day_map(conn, date):
    """{code: ret}:某交易日所有標的的實現報酬。"""
    ensure(conn)
    return {c: r for c, r in conn.execute("SELECT code, ret FROM realized_returns WHERE date=?", (date,))}


def next_date(conn, after):
    """嚴格晚於 after 的最早一個有實現報酬的日期;無則 None。"""
    ensure(conn)
    row = conn.execute("SELECT MIN(date) FROM realized_returns WHERE date > ?", (after,)).fetchone()
    return row[0] if row else None


def all_rows(conn):
    """[(code, date, ret, src)],依 (code, date) 排序。"""
    ensure(conn)
    return conn.execute("SELECT code, date, ret, src FROM realized_returns ORDER BY code, date").fetchall()


T0_REPORT_TYPES = ("盤前報", "早報", "午報")
MIN_ABS_RET = 1e-4


def legacy_t0_consensus(conn):
    """遷移前的實現報酬:同 (code, 日) 盤前/早/午報的 actual_ret(T+0 語意)。
    多筆彼此一致才採用,不一致者剔除不猜。回 {(code, date): ret}。predictions 表不存在回 {}。"""
    vals = {}
    try:
        rows = conn.execute(
            "SELECT code, date(ts), actual_ret FROM predictions "
            f"WHERE report_type IN ({','.join('?' * len(T0_REPORT_TYPES))}) AND actual_ret IS NOT NULL",
            T0_REPORT_TYPES).fetchall()
    except sqlite3.Error:
        return {}
    raw = {}
    for code, d, a in rows:
        if abs(a) > MIN_ABS_RET:
            vals.setdefault((code, d), set()).add(round(a, 10))   # 比對一致性用四捨五入值
            raw.setdefault((code, d), a)                          # 存回原始精度
    return {k: raw[k] for k, v in vals.items() if len(v) == 1}


def seed_from_legacy(conn):
    """把遷移前的 T+0 實現報酬補進 realized_returns(src=legacy),只新增不覆蓋。回新增筆數。"""
    ensure(conn)
    n = 0
    for (code, d), r in sorted(legacy_t0_consensus(conn).items()):
        if upsert(conn, code, d, r, "legacy"):
            n += 1
    return n


def seed_if_empty(conn):
    """只在 realized_returns 尚無任何紀錄時做一次 seed_from_legacy。
    不可每次都 seed:之後新寫入的 proxy 值也會出現在 predictions 的 T+0 欄,
    再 seed 一次會以 legacy(等級較高)覆蓋並抹掉其真實來源。回新增筆數。"""
    ensure(conn)
    if conn.execute("SELECT 1 FROM realized_returns LIMIT 1").fetchone():
        return 0
    return seed_from_legacy(conn)


def load_map(conn):
    """{(code, date): ret}。realized_returns 尚未建立或為空時,退回 legacy_t0_consensus ——
    這正是 seed_from_legacy 會寫入的內容,所以遷移前後讀到的是同一套數字,不會混用來源。"""
    try:
        rows = conn.execute("SELECT code, date, ret FROM realized_returns").fetchall()
    except sqlite3.Error:
        rows = []
    if rows:
        return {(c, d): r for c, d, r in rows}
    return legacy_t0_consensus(conn)


def load(db):
    """從資料庫路徑讀取 load_map。供離線分析(回測、資料品質)使用。"""
    with sqlite3.connect(db) as conn:
        return load_map(conn)


def dates_by_code(rmap):
    """{code: [date 升序]}。"""
    out = {}
    for code, d in rmap:
        out.setdefault(code, []).append(d)
    for v in out.values():
        v.sort()
    return out
