"""回歸測試:觀察池的 sector_today_ret 必須在每檔基金的判斷開始前先給預設值。

實際連網跑收盤報時發現:只有「弱勢進場」分支會設定 sector_today_ret,
「2/3 條件達標」分支卻直接讀它 →
  * 若前面沒有基金走過弱勢分支 → UnboundLocalError(該檔被跳過);
  * 若前面有 → 讀到【上一檔基金】的板塊漲跌,判斷用錯資料。
scan_watchlist 是 main() 內的巢狀函式且需要網路,無法直接呼叫,
所以用 AST 檢查結構:每輪迴圈裡,先賦值,後讀取。
"""
import ast
import os

MODULE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "src", "V24_19_ETF_System.py")


def _scan_watchlist_loop_body():
    with open(MODULE_PATH, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "scan_watchlist")
    loop = next(n for n in ast.walk(fn)
                if isinstance(n, ast.For) and "WATCHLIST_ETFS" in ast.unparse(n.iter))
    return loop


def test_sector_today_ret_assigned_before_any_read_each_iteration():
    loop = _scan_watchlist_loop_body()
    # 迴圈本體可能包在 try 裡;取最外層的語句序列
    body = loop.body
    if len(body) == 1 and isinstance(body[0], ast.Try):
        body = body[0].body

    first_unconditional_assign = None
    for stmt in body:
        if isinstance(stmt, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "sector_today_ret" for t in stmt.targets):
            first_unconditional_assign = stmt.lineno
            break
    assert first_unconditional_assign is not None, \
        "sector_today_ret 沒有在每輪迴圈開頭(非條件分支內)先給預設值"

    reads = [n.lineno for n in ast.walk(loop)
             if isinstance(n, ast.Name) and n.id == "sector_today_ret"
             and isinstance(n.ctx, ast.Load)]
    assert reads, "找不到 sector_today_ret 的讀取,測試前提已變,請更新本測試"
    early = [ln for ln in reads if ln < first_unconditional_assign]
    assert not early, f"這些行在預設值之前就讀取 sector_today_ret: {early}"
