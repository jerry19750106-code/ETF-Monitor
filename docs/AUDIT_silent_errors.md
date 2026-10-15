# 任務A：靜默吞錯稽核與修復報告

> 版本提醒：檔名 `V24_19_ETF_System.py`，但 `__VERSION__ = "V24.30"`（L995），本報告一律以 `__VERSION__` 為準（檔名落差是本專案已知現象，見主程式 `_version_consistency_check()`）。
>
> 本報告取代同名的舊版報告（`245c5ec` commit）。舊版用 AST 粗分出「A類=5處純`pass`／B類=214處log後繼續／D類=69處其他」，並宣稱「先前21處靜默吞錯」是誤算。**這個舊分類法本身有一個未被發現的盲點**：它把「呼叫了 `_log_err()`」等同於「有留下紀錄、非真正靜默」，但 `_log_err()` 的定義是：
>
> ```python
> VERBOSE_ERRORS = os.getenv("ETF_VERBOSE", "0") == "1"
> def _log_err(exc, ctx=""):
>     if VERBOSE_ERRORS:
>         print(f"   ⚠️ [{ctx}] {type(exc).__name__}: {exc}")
> ```
>
> **`VERBOSE_ERRORS` 預設關閉**（L987）。也就是說，在日常正式運行（沒有人手動加 `--verbose` 或設 `ETF_VERBOSE=1`）下，`_log_err()` 呼叫**什麼都不會輸出** —— 舊報告的 214 處「B類：僅記錄後繼續執行」在預設模式下其實跟完全沉默的 `pass` 沒有本質差異。本報告重新以 CLOUD_TASKS.md 規定的 A/B/C 定義，逐一檢查這 214 處（以及其餘所有 except 區塊）**失敗後究竟流向什麼值**，而不是只看「有沒有呼叫 log 函式」。

## 零、數字先核對（實測，非引用）

```
$ python3 -c "import ast; print(len([n for n in ast.walk(ast.parse(open('src/V24_19_ETF_System.py',encoding='utf-8').read())) if isinstance(n, ast.ExceptHandler)]))"
288
```

`grep -c "except" src/V24_19_ETF_System.py` 會因為註解/字串裡含「except」字樣而略有誤差；用 Python `ast` 模組數 `ExceptHandler` 節點才是精確值。CLOUD_TASKS.md 前置事實寫「289 個」，實測為 **288 個**（與舊報告一致）。依「以實測為準」原則採用 288。

## 一、分類方法（CLOUD_TASKS.md 原定義）

| 分類 | 定義 |
|---|---|
| **A（必須修復）** | 捕捉網路/資料源錯誤後 `pass` 或只記 log，之後程式**繼續用預設值或陳舊快取**往下算，訊號層無法分辨這是真數據還是填充值 |
| **B（可接受）** | 捕捉後明確 `raise`，或捕捉後**明確回傳/組裝一個「不可用」訊號**（`None`／空集合／對使用者可見的錯誤文字）讓流程正確中止或可被下游檢查 |
| **C（無需處理）** | 捕捉的是預期內的邊界情況（單筆資料解析失敗、檔案不存在、純時區/編碼層級防呆等），不是整體資料源失效 |

判定的關鍵：**不是看「有沒有 log」，是看「失敗後下一步讀到的值，看起來會不會像一筆正常資料」**。`return None`／`return []`／一段寫著「⚠️ 計算失敗」的字串，使用者或下游程式碼都能分辨「這裡沒有數據」；但 `wti_ret = 0`（WTI 當日漲跌率失敗後設 0）、`c_drain = 0.20`（地緣風險溫度失敗後設 0.20）、`rs_ev_map[code] = 50`（RS排名失敗後設中性分）、硬編碼匯率 `6.79`——這些數字本身**跟真實算出來的結果長得一樣**，這才是 A 類要抓的。

## 二、全量分類結果

| 類別 | 數量 | 說明 |
|---|---|---|
| **A（已修復）** | **15** | 見第三節逐項說明，每項都有對應回歸測試 |
| **B（可接受）** | **256** | 明確 `raise`／明確回傳 None/空集合/False／回傳文字本身即可見錯誤訊息／main()內增強因子退化為保守基線 |
| **C（無需處理）** | **17** | 迴圈內跳過單筆異常資料、純時區/編碼防呆、內部記帳(EV歸因trace) |
| **合計** | **288** | 與 `ast.ExceptHandler` 實測數一致 |

分類依據的驗證深度分三層（皆為本輪逐行查證，非引用舊報告結論）：

1. **逐行人工覆核**：全部 47 個「直接呼叫網路/DB 做資料抓取」的函式（`fetch_*`/`get_*`/`_yh_*`/`_em_*`/`_dated_*`/`load_*`/`persist_*`/`scan_*` 等，含 `DirectDataFetcher`/`PrecisionProxyEngine`/`IntelTeam`/`WatchlistScanner` 內部方法），逐一讀原始碼確認失敗路徑的回傳值語意。結論：這批函式經過 V22.x~V24.x 多輪既有修復（如 `[V24.14] 失敗語意單一化`、`[V22.25.5fix P0] VIX直連 — 失敗返回 None 而非硬編碼18.0`），**絕大多數已經是誠實回傳 `None`/`[]`/`False`**，只有 `IntelTeam.get_usd_cny`、`IntelTeam.get_gold_wti_yahoo`、`IntelTeam.get_cny_20ma` 三個函式仍殘留硬編碼假值（即 A1-A3，見下）。
2. **逐行人工覆核 main()**：`main()` 單一函式內有 78 個 except 區塊（`main()` 本身約 3000 行）。逐一讀過後發現其中 8 處會把「資料不可用」靜默代換成一個看起來合理的數字且**完全不輸出任何可見訊息**（即 A5-A12，見下），其餘要么本身會印出/寫入報告可見的警示文字，要么只是讓某個「加分用增強因子」（資金背離/影子共振/估值分位/操作分級/否決層/EV歸因trace）退化為「本因子未觸發」的保守基線——這與「捏造一個看起來有效的數字」本質不同：少一個加分理由不會產生假的買賣訊號，頂多是少抓到一個本來該有的加分/減分。
3. **規則式分類**：對迴圈內 `continue`/`break`（跳過單筆解析失敗的資料列）與其餘明確 `raise`/明確回傳 None 類，用程式規則批次標記，並對每一條規則抽樣覆核程式碼邏輯是否與規則假設一致。

完整 288 筆逐行結果見文末「附錄：全量分類表」。

## 三、A 類（必須修復）：15 處，已全部修復

> 設計：新增 `src/data_availability.py`，提供 `_data_unavailable(code, reason)` 旁路（純新增檔案，不改動任何既有函式簽名/回傳值）。在下列 15 個 except 區塊（或其共用的 fallback 出口）加入這個函式的呼叫登記「這個 code 本次執行不可用」，讓訊號層/報告層之後能查詢，而**不改變原本的回傳值或控制流**（零回歸；`except Exception:` 改為 `except Exception as _bre/_rse:` 只為取得例外訊息，不影響流程）。

| # | Key | 行號 | 函式 | 問題 | 回歸測試 |
|---|---|---|---|---|---|
| A1 | `USD_CNY` | L9172, L9182 | `IntelTeam.get_usd_cny` | Yahoo + AkShare 皆失敗 → 回傳硬編碼 `6.7900`（雖帶"🔴 HARDCODED"字樣，但呼叫端若只取數值，QDII預測會把它當成即時匯率往下算） | `test_get_usd_cny_fetch_failure_marks_unavailable` |
| A2 | `GOLD` | L9207 | `IntelTeam.get_gold_wti_yahoo` | 黃金抓取失敗 → 回傳硬編碼 `4675.0` | `test_get_gold_wti_yahoo_gold_failure_marks_unavailable` |
| A3 | `WTI` | L9217, L9224 | `IntelTeam.get_gold_wti_yahoo` | **真正危險的案例**：黃金抓到(`src="🟢 LIVE"`)但WTI兩源都失敗時，`src` 標籤是**黃金與WTI共用**，只反映黃金的狀態 → 回傳 `(真黃金, 假97.0, "🟢 LIVE")`，呼叫端從 `src` 完全看不出WTI是假的。這比單純硬編碼更危險，因為它連「有警告字樣」這層保護都沒有。 | `test_get_gold_wti_yahoo_wti_failure_marks_unavailable_even_when_gold_live` |
| A4 | `CNY_20MA` | L9288 | `IntelTeam.get_cny_20ma` | 抓取失敗 → 回傳 `0.0`，在「USD/CNY 20日均線」語境下會被 `analyze_cny_dual_track` 當成真實均線值算偏離百分比 | `test_get_cny_20ma_fetch_failure_marks_unavailable` |
| A5 | `WTI_RET_PM` | L11945 | `main()`（盤前報分支） | `IntelTeam.get_5day_history("CL=F")` 失敗 → `wti_ret = 0`，在「WTI日漲跌率」語境下與「今日持平」無法區分，直接餵進 `derive_c_drain` | `test_main_wti_ret_pm_failure_marks_unavailable` |
| A6 | `WTI_RET` | L12528 | `main()`（主流程） | 同 A5，主流程版本 | `test_main_wti_ret_failure_marks_unavailable` |
| A7 | `C_DRAIN_PM` | L12041 | `main()`（盤前報分支） | `derive_c_drain(...)` 拋例外 → `_cdrain_pm = 0.20`（或人工值），驅動 `GEO_ADJ_TABLE` 影響當日每一檔ETF的EV調整 | `test_main_derive_c_drain_pm_failure_marks_unavailable` |
| A8 | `C_DRAIN` | L12590 | `main()`（主流程） | 同 A7，主流程版本 | `test_main_derive_c_drain_failure_marks_unavailable` |
| A9 | `BRAIN_JSON` | L12570 | `main()` | `brain.json` 存在但讀取/解析失敗（檔案損毀）→ **完全沒有任何 log/print**，靜默把 `strait_status`/Kalman權重(`w_tw`/`w_a`)/`trade_ease_signal` 全部重置成跟「從未校準過」一樣的預設值，訊號層無法分辨這是「資料損毀」還是「本來就沒資料」 | `test_main_brain_json_corrupt_marks_unavailable` |
| A10 | `RS_RANK:{code}` | L12669 | `main()`（RS-Rank預掃描迴圈） | `DirectDataFetcher.get_yahoo_prices`/`get_eastmoney_nav` 等拋出非預期例外 → **完全沒有任何 log/print**，`rs_ev_map[code] = 50`，中性50分與真實算出的50分無法區分，決定 `get_rs_label()` 顯示的同Style排名 | `test_main_rs_rank_scan_failure_marks_unavailable` |
| A11 | `VIX_RISK_OFF` | L12720 | `main()` | `DirectDataFetcher.get_vix()` 失敗/回None → `_risk_off_level` 靜默維持預設值 `0`（=「正常」），這個值會傳進 `compute_order` 的倉位風控閘門 —— 等於在VIX資料不可用時，假裝「風險正常」去放行原本該被風控攔住的操作 | `test_main_risk_off_vix_failure_marks_unavailable` |
| A12 | `BRAIN_JSON_PM` | L12025 | `main()`（盤前報分支） | 同 A9 的盤前版本：`brain.json` 損毀時 `_brain_pm` 停在 `{}`，僅 `_log_err`（預設不輸出），盤前報的 Kalman 權重/海峽狀態/人工 c_drain 全部靜默回到預設 | `test_main_brain_json_pm_corrupt_marks_unavailable` |
| A13 | `ACCURACY` | L10977 | `_get_accuracy` | **初版稽核誤歸 B 類，後於深度審查更正**。讀取預測紀錄失敗時回傳 `{"n":0}`（不含 `accuracy` 鍵）→ `main()` 的 `mkt_acc.get("accuracy", 0.6)` 拿到憑空的 60% 勝率與 0.8% 誤差 → Kelly 據此算出非零建議倉位。原判語「僅影響準確度顯示文字」是錯的 | `test_get_accuracy_failure_marks_unavailable` |

共 13 個登記 key（15 個 except 區塊；`USD_CNY` 的 2 個 try 共用 1 個最終 fallback，`WTI` 的 2 個 except 共用同 1 個登記點）。

### 修復機制

`src/data_availability.py` 新增 `_data_unavailable(code, reason)` / `is_unavailable(code)` / `get_reason(code)` / `get_count(code)` / `unavailable_codes()` / `snapshot()` / `reset()`。主程式只在上述 15 個既有 except 區塊（或其 fallback 出口）加入呼叫 `_data_unavailable(key, reason)`，**完全不改變原本的回傳值/賦值邏輯**——`get_usd_cny()` 失敗時依然回 `6.79`、`wti_ret` 失敗時依然是 `0`，但現在這些失敗「有名有姓」地被記錄下來，`data_availability.is_unavailable("WTI")` 之類的查詢即可讓報告層/下游判斷邏輯知道該值不可信。

### 紅→綠機械驗證

1. **整檔對照**：暫時拿掉主程式修改後跑測試，全數失敗；套回後全數通過。
2. **逐項突變檢查**（較嚴格）：上面的整檔對照只證明「測試依賴修復」。因此另對主程式中**每一個** `_data_unavailable(...)` 呼叫逐一替換成 `pass`，跑完整 `tests/test_silent_errors.py`，確認每一處都至少讓一個測試轉紅——沒有任何登記點是測試覆蓋不到的、也沒有空斷言。**20 個登記點全部轉紅**（含第三節之二的 except 以外登記）：

```
USD_CNY        L9187   RED  | WTI_RET_PM(條件式)  L11944 RED | C_DRAIN_INPUTS(主) L12589 RED
WTI            L9229   RED  | WTI_RET_PM          L11949 RED | C_DRAIN            L12594 RED
GOLD           L9233   RED  | BRAIN_JSON_PM       L12029 RED | RS_RANK:{code}     L12672 RED
CNY_20MA       L9292   RED  | C_DRAIN_INPUTS(盤前) L12040 RED | VIX_RISK_OFF       L12725 RED
ACCURACY       L10980  RED  | C_DRAIN_PM          L12044 RED |
_intel_na(盤前) L11886  RED  | _intel_na(主流程)    L12498 RED |
GOLD_RET(主)   L12519  RED  | WTI_RET(條件式)      L12527 RED | WTI_RET L12531 RED | BRAIN_JSON L12574 RED
```
（行號為 `_data_unavailable` 呼叫所在行，非 except 行。）

3. **端到端實跑**：沙盒斷網跑 `--force-type 收盤報`。加入報告區塊之前，修改前後兩份報告 `.txt`/`.html` 逐位元組一致；加入後 `MAIN_EXIT: OK`，報告頂部正確列出全部不可用項目（見第三節之二）。

## 三之二、except 以外的靜默預設（端到端實跑後追加）

任務 A 原始範圍限於 except 區塊。斷網實跑後發現報告上仍有看似正常、實為預設的數值，來源不在 except 內，一併登記：

| Key | 位置 | 問題 |
|---|---|---|
| `VIX`/`NDX`/`SOX`/`GOLD_RET` | 既有 `_intel_na` 正規化（盤前 L11878 起、主流程 L12492 起） | 抓不到時以 VIX 18.0、漲跌 0% 替代，報告第二行顯示「VIX=18.00🟢」。原本只埋在「市場資金面」一行的警示字串裡 |
| `GOLD_RET` | 主流程 `if gold_ret is None`（L12516 起） | **原警示永遠漏掉黃金**：`_intel_na.append("黃金")` 發生在 `_intel_warning` 組好之後 |
| `WTI_RET_PM`/`WTI_RET` | 盤前/主流程 wti_ret 條件式 | WTI 5日歷史不足時條件式直接給 0（沒有例外，所以 except 稽核抓不到），與「今日持平」無法區分 |
| `C_DRAIN_INPUTS` | 盤前/主流程 c_drain 推導成功後 | 推導成功，但 VIX/WTI日漲跌/黃金日漲跌任一為預設值 → 報告寫「市場平靜」實為「不知道」。有效期內人工加碼不依賴輸入，不登記 |

**報告頂部「⚠️ 本次資料不可用」區塊**：盤前報、早午收盤報、夜間快報寫檔前，把登記表插在標題下方，列出實際使用的值、影響與失敗原因；無任何登記時報告逐字不變（有測試）。

## 四、B/C 類代表性查證摘要（非窮舉，完整見附錄）

- **誠實回傳 `None`/`[]`/`False`**：`get_vix`/`get_ndx`/`get_sox`（兩個類各一組）、`get_etf_5day_flow`、`get_order_flow_breakdown`、`fetch_dated_us_series`、`_dated_yahoo`、`_dated_nav`、`get_pinned_trajectory`、`load_pe_percentile`、`fetch_pe_percentile_eastmoney`、`fetch_index_pe_history_funddb(_meta)`、`refresh_pe_percentile(_funddb)` 等——docstring 明確寫「失敗回 None/[]」，且程式碼內確實如此，呼叫端必須自行檢查。
- **`get_best_proxy_ret` 的 `src` 標籤設計**（B，非A）：雖然多層fallback，但每一層都會更新對應的 `src`/`method` 字串（`"TW_Direct"`/`"Proxy⚠️"`/`"OwnNAV[Enhanced]"`/`"NAV_Only"`/`"NoData"`），呼叫端若檢查該字串即可分辨資料來源品質——這跟 A2/A3 的 `get_gold_wti_yahoo`「標籤被另一個獨立欄位的成功狀態覆蓋」是不同情況。
- **main() 內「加分用增強因子」退化（B，非A）**：資金背離(`holding_divergence`)、大小單拆解(`order_flow_tag`)、影子共振(`shadow_resonance_tag`)、估值分位第⑤條(`_dc_val`)、分級否決層(`_pre_grade`/veto)、EV歸因鏈(`_ev_trace.append`) 等失敗時，對應變數停在「本因子未觸發」的初始狀態（空字串/`None`/不調整EV）。這與 A 類的差別：A 類是「捏造一個看起來真的數字去驅動核心決策」，這批是「少套用一個本來可能有的加分/減分」——保守方向，不產生假訊號。
- **迴圈內 `continue`（C）**：`sanitize_price_series`、`fetch_announcements` 逐筆解析、`get_etf_5day_flow` 的 `flow_list.append`、`get_upcoming_event_styles`、`scan_watchlist` 逐檔迴圈、`fetch_sector_flow` 逐筆解析——皆是「批量資料中混了一筆格式異常，跳過該筆、其餘照算」，屬預期邊界情況。
- **純時區/編碼防呆（C）**：`_now_utc8`（UTC+8轉換失敗退回本機 `now()`，跟任何外部資料源無關）、模組載入時的 `sys.stdout.reconfigure(encoding='utf-8')`。

## 五、過程中發現但不在本任務範圍內修的問題（如實記錄，未動）

依開發紀律第6條「不修改既有行為，除非任務明確要求」，以下在查證 A 類時順手發現的小毛病**不修**，列此存查：

1. `IntelTeam.get_cny_20ma()`（L9265）：`if r is None: return []` —— 函式簽名宣告回傳 `float`，這條路徑卻回傳空 list，型別不一致（雖然目前沒有呼叫點會踩到，因為 `_yahoo_get` 在代理不可達時才回 `None`，此時通常呼叫鏈已先被 `_yh_proxy_ok()` 短路）。
2. **`main()` 主流程 c_drain 在 brain.json 缺失/損毀時永遠是 0.20（潛伏 bug，本 PR 未修，已由 PR #3 獨立修復）**：`macro` 只在 brain.json 讀取 try 區塊內賦值（L12564），檔案不存在的 `else` 分支與 `except` 分支都沒有宣告 `macro`。於是下方 `derive_c_drain(..., override_as_of=macro.get("c_drain_as_of"))`（L12585）必定拋 `UnboundLocalError`(NameError 子類;`macro` 是 main() 的區域變數)，被 derive_c_drain 的 except 吞掉 → `c_drain = 0.20`。也就是說，**沒有 brain.json 時 c_drain 自動推導從未真正執行過**。修它（補 `macro = {}`）會改變 c_drain 的實際數值與所有 ETF 的 EV，屬行為變更，依紀律第6條不在本任務修；但本次 A8 修復後，這條路徑會登記 `C_DRAIN` 不可用、reason 帶 `UnboundLocalError: cannot access local variable 'macro'`(2026-10-15 沙盒斷網實跑收盤報確認)，問題至少已經可被查到。已由 PR #3 修復（預設 `macro = {}`；實測壓力日 VIX 32/油 +6%/金 +2.5% 應推導 0.57，修復前一律 0.20）。

## 六、產出檔案對照

- `docs/AUDIT_silent_errors.md`（本檔）
- `src/data_availability.py` —— `_data_unavailable(code, reason)` 及查詢 API
- `tests/test_silent_errors.py` —— 35 個測試：A 類回歸、except 以外登記、報告區塊、防漏斷言、網路攔截機制

驗收指令：
```
python3 -m pytest tests/test_silent_errors.py -v
```

---

## 附錄：全量分類表（288 筆，按行號排序）

> `判定依據` 欄為摘要（完整文字見 `/tmp` 產生腳本或重新執行 `ast` 查詢；篇幅所限，摘要已盡量保留關鍵判準）。`函式` 為 `<module>` 代表該 except 直接寫在模組最外層（非任何函式內）。

| # | 行號 | 函式 | 類別 | 判定依據(摘要) |
|---|---|---|---|---|
| 5 | L733 | `_yh_proxy_ok` | B | 明確回傳空集合/False/0 的既定『無資料』契約(docstring已聲明) |
| 6 | L742 | `_yh_cache_load` | B | 已逐行人工覆核該函式,確認誠實回傳/副作用,非偽裝成功 |
| 7 | L752 | `_yh_cache_put` | B | 已逐行人工覆核該函式,確認誠實回傳/副作用,非偽裝成功 |
| 8 | L787 | `_yahoo_get` | B | 明確回傳 None(或含None的tuple),訊號層需自行檢查,誠實示警 |
| 139 | L825 | `_probe_em` | B | 明確回傳空集合/False/0 的既定『無資料』契約(docstring已聲明) |
| 1 | L844 | `<module>` | B | except區塊內組裝的文字含對使用者可見的警示字樣(⚠️/失敗/錯誤),非靜默 |
| 9 | L906 | `_em_warmup` | B | 已逐行人工覆核該函式,確認誠實回傳/副作用,非偽裝成功 |
| 10 | L933 | `_em_throttled_get` | B | 已逐行人工覆核該函式,確認誠實回傳/副作用,非偽裝成功 |
| 140 | L964 | `_em_throttled_get` | B | 已逐行人工覆核該函式,確認誠實回傳/副作用,非偽裝成功 |
| 141 | L1019 | `_version_consistency_check` | B | 失敗後回傳docstring已聲明的保守值('') |
| 11 | L1030 | `_version_consistency_check` | B | 失敗後回傳docstring已聲明的保守值('') |
| 2 | L1039 | `<module>` | B | except區塊內組裝的文字含可見警示字樣,非靜默 |
| 3 | L1046 | `<module>` | C | 純時區/編碼層級防呆,與資料源完全無關 |
| 142 | L1083 | `load_overnight_override` | B | 已逐行人工覆核,確認誠實回傳,非偽裝成功 |
| 12 | L1103 | `load_overnight_override` | B | 明確回傳空集合/False/0 的既定『無資料』契約 |
| 13 | L1123 | `apply_overnight_override` | B | 失敗後 `_exp_us=None`,保守關閉錨點時效檢查,非造假 |
| 234 | L1184 | `check_position_anchor_freshness` | B | 失敗後回傳docstring已聲明的保守值('') |
| 14 | L1191 | `check_position_anchor_freshness` | B | 回傳 "" (空字串),誠實示警 |
| 15 | L1223 | `check_tracker_freshness` | B | 回傳值本身就是對使用者可見的錯誤訊息文字 |
| 16 | L1410 | `_pb_parse_float` | B | 失敗回傳呼叫端傳入的 `default` 參數,既定契約 |
| 17 | L1451 | `_pb_mark_to_market` | B | 明確回傳空集合/False/0 的既定『無資料』契約 |
| 18 | L1469 | `_pb_window_return` | B | 明確回傳 None,訊號層需自行檢查 |
| 19 | L1563 | `_migrate_positions_to_db` | B | 回傳值本身就是對使用者可見的錯誤訊息文字 |
| 20 | L1586 | `_pb_read_rows` | B | 明確回傳 (None, 錯誤訊息) 的既定契約 |
| 143 | L1601 | `_pb_read_rows` | B | 同上 |
| 144 | L1605 | `_pb_read_rows` | B | 同上 |
| 145 | L1623 | `_pb_read_rows` | B | 同上 |
| 21 | L1629 | `_pb_read_rows` | B | 同上 |
| 22 | L1706 | `build_positionbook` | B | except區塊內組裝的文字含可見警示字樣 |
| 146 | L1745 | `build_positionbook` | B | 明確回傳空集合/False/0 的既定『無資料』契約 |
| 235 | L1792 | `build_positionbook` | B | except區塊內組裝的文字含可見警示字樣 |
| 23 | L1961 | `_detect_pe_source_connected` | B | 回傳 False(純SQLite本機讀取,非網路資料源) |
| 24 | L2075 | `compute_max_drawdown_from_db` | B | 明確回傳 (None, 0),訊號層需自行檢查 |
| 147 | L2111 | `sanitize_price_series` | C | 迴圈內跳過單筆異常價格,非整體資料源失效 |
| 25 | L2132 | `compute_max_drawdown_from_nav` | B | 明確回傳空集合/False/0 的既定『無資料』契約 |
| 26 | L2188 | `compute_drawdown_position` | B | 明確回傳 None,訊號層需自行檢查 |
| 27 | L2260 | `render_factor_matrix` | B | 失敗回傳 "" ,純報告渲染層,非資料源 |
| 28 | L2322 | `render_forecast_bias_audit` | B | 失敗回傳 "" |
| 29 | L2371 | `render_signal_conflicts` | B | 失敗回傳 "" |
| 30 | L2450 | `render_drawdown_position` | B | 失敗回傳 "" |
| 31 | L2508 | `render_drawdown_budget` | B | 回傳值本身是可見錯誤訊息("⚠️ 計算失敗...") |
| 32 | L2579 | `render_exposure_status` | B | 回傳值本身是可見錯誤訊息 |
| 33 | L2704 | `check_proxy_collisions` | B | 失敗僅跳過該筆警示收集,不影響已收集項 |
| 34 | L2794 | `_fragility_inputs_from` | B | 子輸入維持保守預設(crash5=False等),非造假正訊號 |
| 35 | L2802 | `_fragility_inputs_from` | B | 同上(neg_days=0) |
| 36 | L2853 | `_now_utc8` | C | 純時區防呆,與資料源完全無關 |
| 37 | L3015 | `flow5_outage_days` | B | 明確回傳空集合/False/0 的既定契約 |
| 148 | L3148 | `build_volume_monitor` | C | 迴圈內跳過單筆異常資料 |
| 38 | L3174 | `build_daily_conclusion` | B | 失敗後若無真實板塊資料,明確印出「不編造」訊息 |
| 39 | L3238 | `build_daily_conclusion` | B | 失敗僅跳過該筆持倉關聯分析 |
| 40 | L3317 | `_build_market_summary` | B | `_real_lead=None`,回退到已有揭露文字的NAV摘要 |
| 41 | L3348 | `_load_assess_reversal` | B | 明確回傳 None,docstring已聲明「誠實標註」 |
| 42 | L3383 | `_build_reversal_judgment` | B | 子指標維持 None,下游 `_assess` 設計上可處理 |
| 43 | L3394 | `_build_reversal_judgment` | B | 同上 |
| 44 | L3408 | `_build_reversal_judgment` | B | 同上 |
| 45 | L3482 | `_build_attribution_narrative` | B | 失敗僅跳過該筆殘差列,不影響已收集項 |
| 46 | L3637 | `record_news_scan` | B | DB寫入失敗,不影響已算出訊號,下次讀取誠實回報無資料 |
| 47 | L3653 | `check_news_silent_streak` | B | 明確回傳 (0, []) 的既定契約 |
| 48 | L3674 | `news_audit_report_line` | B | 失敗回傳 "" |
| 4 | L3683 | `<module>` | B | 新聞計分自測,失敗僅 `_log_err`+pass,不影響主流程(非資料抓取) |
| 49 | L3726 | `fetch_announcements` | B | 明確回傳 [] 的既定契約 |
| 149 | L3739 | `fetch_announcements` | C | 迴圈內跳過單筆公告解析失敗 |
| 236 | L3762 | `scan_announcements_for_holdings` | B | 已逐行人工覆核,確認誠實回傳 |
| 50 | L3814 | `_news_diag` | B | `_has_mod=False`,僅影響診斷文字判斷分支 |
| 237 | L3856 | `load_news_csv` | B | 已逐行人工覆核(自動補抓失敗,優雅降級用既有檔) |
| 51 | L3858 | `load_news_csv` | B | 同上 |
| 281 | L3873 | `load_news_csv` | B | 同上(單列date解析失敗跳過) |
| 52 | L3880 | `load_news_csv` | B | 同上(整體讀取失敗回已初始化的 `items=[]`) |
| 150 | L3931 | `build_news_score_map` | B | 失敗僅跳過該筆新聞計分 |
| 53 | L4045 | `save_pe_percentile` | B | 明確回傳 False 的既定契約 |
| 151 | L4071 | `load_pe_percentile` | B | 已逐行人工覆核,確認誠實回傳 |
| 54 | L4083 | `load_pe_percentile` | B | 明確回傳 (None, meta) ,meta.status="error" |
| 152 | L4110 | `fetch_pe_percentile_eastmoney` | B | 已逐行人工覆核,確認誠實回傳 None |
| 55 | L4113 | `fetch_pe_percentile_eastmoney` | B | 明確回傳 None |
| 56 | L4132 | `update_pe_history` | B | 明確回傳 False |
| 57 | L4153 | `compute_pe_percentile_local` | B | 明確回傳 (None, n) |
| 58 | L4190 | `refresh_pe_percentile` | B | 明確回傳 (None, meta) ,meta.status="error" |
| 59 | L4231 | `_get_ak_funddb_fn` | B | akshare未安裝/無此函式 → 回 None,誠實 |
| 60 | L4250 | `fetch_index_pe_history_funddb` | B | 明確回傳 None,docstring聲明「客戶端回None」 |
| 61 | L4264 | `fetch_index_pe_history_funddb_meta` | B | 明確回傳 (None, {status:error}) |
| 153 | L4273 | `_parse_pe_date` | C | 迴圈內跳過單筆日期格式錯誤 |
| 62 | L4340 | `refresh_pe_percentile_funddb` | B | 明確回傳 (None, meta) |
| 63 | L4355 | `_clamp_snapshot_trend` | B | 明確回傳 None |
| 64 | L4429 | `save_watchlist_snapshot` | B | 明確回傳 (0, 可見錯誤訊息) |
| 65 | L4453 | `save_daily_snapshot` | B | DB寫入失敗,不影響已算出訊號 |
| 66 | L4461 | `backfill_t1_actual` | B | 同上(回填失敗不污染既有值) |
| 67 | L4507 | `_apply_actual_by_semantic` | B | 同上 |
| 68 | L4521 | `save_morning_premise` | B | 同上 |
| 69 | L4530 | `load_morning_premise` | B | 明確回傳 None |
| 70 | L4548 | `format_morning_premise` | B | `_age=None`,僅略過時效警示,不假裝新鮮 |
| 238 | L4618 | `save_closing_handoff` | C | 迴圈內跳過單筆 |
| 271 | L4629 | `save_closing_handoff` | B | DB寫入失敗,不影響已算出訊號 |
| 71 | L4652 | `save_closing_handoff` | B | 同上 |
| 72 | L4666 | `load_closing_handoff` | B | 明確回傳 (None, None) |
| 73 | L4722 | `format_close_scorecard` | B | 回傳值本身是可見錯誤訊息 |
| 154 | L4736 | `format_close_scorecard` | B | 同上模式,失敗回已含揭露文字的字串 |
| 74 | L4805 | `format_premise_verification` | B | `_age=None`,僅略過時效警示 |
| 155 | L4871 | `_us_ret_outlier_guard` | B | 離群防呆,失敗回傳原值不做判斷(保守) |
| 239 | L4889 | `_series_outlier_filter` | B | 同上邏輯,過濾失敗回傳原序列 |
| 75 | L4926 | `fetch_dated_us_series` | B | 明確回傳 [] |
| 76 | L4949 | `_load_anchor_cache` | B | 已逐行人工覆核,確認誠實回傳 {} |
| 77 | L4957 | `_save_anchor_cache` | B | 已逐行人工覆核,快取寫入失敗無害 |
| 78 | L5004 | `get_pinned_trajectory` | B | 明確回傳 ([], []) |
| 79 | L5030 | `_dated_yahoo` | B | 明確回傳 [] ,且會 print 可見警示 |
| 80 | L5059 | `_dated_nav` | B | 明確回傳 [] ,且會 print 可見警示 |
| 240 | L5091 | `prime_conditional_betas` | B | except區塊print可見警示 |
| 81 | L5100 | `prime_conditional_betas` | B | 明確回傳空集合/False/0 |
| 82 | L5139 | `render_conditional_beta_block` | B | 回傳值本身是可見錯誤訊息 |
| 83 | L5172 | `compute_pred_bias` | B | 明確回傳 (0.0, None) |
| 84 | L5481 | `fetch_intraday_quote` | B | 已逐行人工覆核(新浪源失敗,續試騰訊備援) |
| 85 | L5499 | `fetch_intraday_quote` | B | 已逐行人工覆核,騰訊也失敗則回 None |
| 156 | L5520 | `_volume_flow_proxy` | B | 子路徑失敗維持保守預設 |
| 241 | L5529 | `_volume_flow_proxy` | B | 同上 |
| 86 | L5572 | `_volume_flow_proxy` | B | 明確回傳 (None, False, None) |
| 87 | L5608 | `record_intraday_compare` | B | DB寫入失敗,不影響已算出訊號 |
| 88 | L5815 | `format_order_flow_block` | B | 失敗回傳 "" |
| 157 | L5889 | `get_order_flow_breakdown` | B | 已逐行人工覆核,確認誠實回傳 None |
| 89 | L5921 | `get_order_flow_breakdown` | B | 同上 |
| 90 | L5943 | `_read_today_flow_from_db` | B | 讀取失敗回保守值,不影響已算出訊號 |
| 158 | L5962 | `persist_capital_flow` | B | 已逐行人工覆核,DB寫入失敗無害 |
| 91 | L5984 | `persist_capital_flow` | B | 同上 |
| 92 | L6033 | `read_capital_flow_trend` | B | 明確回傳 None |
| 159 | L6117 | `get_yahoo_prices` | B | 明確回傳 [] ,且print可見警示 |
| 160 | L6137 | `get_eastmoney_nav` | B | 已逐行人工覆核,確認誠實回傳 |
| 161 | L6187 | `get_sina_proxy_ret` | B | 已逐行人工覆核([V24.14]修過:失敗統一回None,非0.0) |
| 162 | L6221 | `get_index_kline_sina` | B | 明確回傳 None |
| 282 | L6278 | `get_etf_5day_flow` | B | 已逐行人工覆核,確認誠實回傳 |
| 283 | L6297 | `get_etf_5day_flow` | B | 同上 |
| 163 | L6326 | `get_etf_5day_flow` | B | 明確回傳 None,且同步寫入快取避免誤用 |
| 164 | L6340 | `get_vix` | B | 明確回傳 None |
| 165 | L6351 | `get_ndx` | B | 已逐行人工覆核(pinned anchor路徑) |
| 166 | L6362 | `get_ndx` | B | 明確回傳 None |
| 167 | L6373 | `get_sox` | B | 已逐行人工覆核(pinned anchor路徑) |
| 168 | L6384 | `get_sox` | B | 明確回傳 None |
| 169 | L6477 | `get_premium_discount` | B | 已逐行人工覆核,確認初始化的 {premium:None} 誠實預設 |
| 242 | L6586 | `get_best_proxy_ret` | B | 已逐行人工覆核(增強型自身NAV失敗,回退代理,非造假) |
| 170 | L6643 | `get_sina_proxy_ret` | B | 已逐行人工覆核([V24.14]同款修復) |
| 171 | L6657 | `get_ndx` | B | 已逐行人工覆核(PrecisionProxyEngine版本) |
| 172 | L6668 | `get_ndx` | B | 明確回傳 None |
| 173 | L6679 | `get_sox` | B | 已逐行人工覆核 |
| 174 | L6690 | `get_sox` | B | 明確回傳 None |
| 175 | L6703 | `get_vix` | B | 明確回傳 None,docstring寫明「非硬編碼18.0」 |
| 243 | L6720 | `load` | B | PositionBook讀取失敗,回保守值 |
| 176 | L6757 | `pnl` | B | 計算失敗回保守值,不影響已算出訊號 |
| 93 | L7187 | `record_shadow_signal` | B | except區塊print/組字含可見警示 |
| 244 | L7215 | `_flow5_with_manual` | B | 失敗回退人工值/原始值,非造假 |
| 94 | L7227 | `_flow5_with_manual` | B | 同上 |
| 95 | L7262 | `_flow5_proxy_fallback` | B | 回傳既有 `live_flow` 輸入值,非新造假值 |
| 96 | L7298 | `_dd_freshness` | B | 回傳既有 `text` 輸入值 |
| 97 | L7472 | `build_factor_attr_block` | B | 回傳值本身是可見錯誤訊息 |
| 98 | L7532 | `_load_dd_auto` | B | 明確回傳 ({}, 可見錯誤訊息) |
| 177 | L7648 | `_ensure_dd_auto` | B | ImportError分支回可見訊息字串 |
| 99 | L7674 | `_ensure_dd_auto` | B | 同上 |
| 100 | L7677 | `_ensure_dd_auto` | B | 回傳值本身是可見錯誤訊息,docstring寫明「優雅降級」 |
| 272 | L7920 | `backfill_yesterday_actuals` | B | DB回填失敗,不污染既有值 |
| 273 | L7968 | `backfill_taiwan_today_actuals` | B | except區塊print可見警示 |
| 274 | L8040 | `backfill_a_stock_today_actuals` | B | 同上 |
| 245 | L8182 | `build_tomorrow_outlook` | B | 失敗僅跳過該段展望文字 |
| 275 | L8822 | `scan_watchlist` | B | 已逐行人工覆核 |
| 276 | L8884 | `scan_watchlist` | B | Regime偵測失敗回保守 ranging/0.0(非造假強訊號) |
| 286 | L8925 | `scan_watchlist` | B | 已逐行人工覆核 |
| 246 | L9040 | `scan_watchlist` | C | 迴圈內跳過單檔異常,且print可見訊息 |
| 247 | L9099 | `format_watchlist_block` | B | 失敗僅跳過該段文字 |
| 248 | L9107 | `format_watchlist_block` | B | 同上 |
| 178 | L9172 | `get_usd_cny` | **A** | 見第三節 A1,回歸測試 `test_get_usd_cny_fetch_failure_marks_unavailable` |
| 179 | L9182 | `get_usd_cny` | **A** | 同上 |
| 180 | L9207 | `get_gold_wti_yahoo` | **A** | 見第三節 A2,回歸測試 `test_get_gold_wti_yahoo_gold_failure_marks_unavailable` |
| 181 | L9217 | `get_gold_wti_yahoo` | **A** | 見第三節 A3,回歸測試 `test_get_gold_wti_yahoo_wti_failure_marks_unavailable_even_when_gold_live` |
| 277 | L9224 | `get_gold_wti_yahoo` | **A** | 同上 |
| 182 | L9250 | `get_gold_ret` | B | 已逐行人工覆核,明確回傳 None(docstring已修正) |
| 183 | L9269 | `get_5day_history` | B | 明確回傳 [] |
| 184 | L9288 | `get_cny_20ma` | **A** | 見第三節 A4,回歸測試 `test_get_cny_20ma_fetch_failure_marks_unavailable` |
| 185 | L9341 | `get_market_sentiment_v14` | B | 已逐行人工覆核,回傳可見的「採集失敗」文字欄位 |
| 284 | L9359 | `get_market_sentiment_v14` | B | 同上 |
| 186 | L9369 | `get_market_sentiment_v14` | B | 同上(大盤主力fflow失敗,回「—(採集失敗)」誠實文字) |
| 249 | L9395 | `get_market_sentiment_v14` | B | 同上 |
| 187 | L9409 | `get_market_sentiment_v14` | B | 同上 |
| 278 | L9506 | `_init_db` | B | DB初始化失敗,print可見警示 |
| 101 | L9561 | `_ae_nth_weekday` | B | 明確回傳 None |
| 102 | L9633 | `generate_auto_events` | B | 明確回傳 [] |
| 188 | L9667 | `load_events_csv` | B | 已逐行人工覆核,讀取失敗僅略過手動事件 |
| 189 | L9752 | `get_upcoming_event_styles` | C | 迴圈內跳過單筆日期格式錯誤 |
| 103 | L9784 | `save_raw_signal` | B | 明確回傳 False |
| 104 | L9804 | `read_reduce_streak` | B | 明確回傳 0(持續天數),docstring既定契約 |
| 105 | L9854 | `read_prev_flow5` | B | 明確回傳 None |
| 190 | L9907 | `fetch_sector_flow` | B | 已逐行人工覆核,全端點失敗回[]並print「誠實,不冒充」 |
| 250 | L9923 | `fetch_sector_flow` | C | 迴圈內跳過單筆板塊解析失敗 |
| 106 | L9974 | `save_sector_snapshot` | B | 明確回傳 0 |
| 107 | L9986 | `compute_crowdedness` | B | 明確回傳含 None 的既定契約 tuple |
| 191 | L10668 | `compute_order` | B | 失敗回保守的不動作指令,非造假操作建議 |
| 251 | L10719 | `_db_write` | B | DB寫入失敗,不影響已算出訊號(呼叫端L13234另有print可見警示) |
| 108 | L10792 | `compute_information_coefficient` | B | 明確回傳 None |
| 109 | L10842 | `compute_forward_ic` | B | 明確回傳含 note 說明的既定契約 dict |
| 110 | L10864 | `_pw_freshness_tag` | B | 失敗回傳 "" |
| 111 | L10894 | `ic_conviction_multiplier` | B | 失敗回傳中性乘數 1.0(docstring:「不確定→×1.0」既定設計) |
| 112 | L10977 | `_get_accuracy` | **A** | 見第三節 A13(初版誤歸 B,已更正),回歸測試 `test_get_accuracy_failure_marks_unavailable` |
| 113 | L10995 | `_send_telegram` | B | 推播失敗不影響報告生成,純通知副作用 |
| 114 | L11050 | `_mirror_four_report_db` | B | DB鏡像寫入失敗,不影響已算出訊號 |
| 115 | L11095 | `update_four_report_tracker` | B | except區塊print可見警示 |
| 116 | L11214 | `update_four_report_tracker` | B | DB操作失敗,不污染已算出訊號 |
| 117 | L11325 | `update_four_report_tracker` | B | except區塊print可見警示 |
| 118 | L11384 | `update_four_report_tracker` | B | 同上 |
| 119 | L11456 | `update_four_report_tracker` | B | 同上 |
| 120 | L11505 | `update_four_report_tracker` | B | 同上 |
| 121 | L11511 | `update_four_report_tracker` | B | 同上 |
| 122 | L11513 | `update_four_report_tracker` | B | 同上 |
| 123 | L11540 | `_run_self_audit` | B | 自我稽核失敗回保守值,非對外訊號 |
| 124 | L11608 | `_run_self_audit` | B | except區塊print可見警示 |
| 252 | L11668 | `derive_c_drain` | B | 內部子計算失敗回退保守分支,函式本體有既定防禦設計 |
| 125 | L11756 | `main` | B | except區塊print可見警示 |
| 192 | L11766 | `main` | B | 同上 |
| 126 | L11768 | `main` | B | 同上 |
| 127 | L11824 | `main` | B | 新聞因子初始化失敗→`[],{},{}`,屬加分用增強因子退化基線 |
| 253 | L11922 | `main` | B | 公告掃描失敗,屬增強因子退化基線 |
| 193 | L11929 | `main` | B | main()內print/寫入報告可見文字區塊 |
| 194 | L11945 | `main` | **A** | 見第三節 A5,回歸測試 `test_main_wti_ret_pm_failure_marks_unavailable` |
| 195 | L11987 | `main` | B | 美股釘選軌跡接線失敗,屬增強因子退化基線(保留舊路徑結果) |
| 196 | L11999 | `main` | B | CNY昨日值失敗維持 None,屬增強因子退化基線 |
| 254 | L12025 | `main` | **A** | 見第三節 A12,回歸測試 `test_main_brain_json_pm_corrupt_marks_unavailable` |
| 197 | L12041 | `main` | **A** | 見第三節 A7,回歸測試 `test_main_derive_c_drain_pm_failure_marks_unavailable` |
| 198 | L12086 | `main` | B | 隔夜錨as-of顯示失敗,`_anchor_line=""`,屬顯示層退化 |
| 199 | L12133 | `main` | B | except區塊print可見警示 |
| 200 | L12194 | `main` | B | 昨日減倉信號回顧查詢失敗(`unknown`context,DB讀取,不影響已算出訊號) |
| 279 | L12305 | `main` | B | 資金流快取失敗回 None,屬增強因子退化基線 |
| 201 | L12395 | `main` | B | main()內print/寫入報告可見文字區塊 |
| 202 | L12409 | `main` | B | 同上 |
| 203 | L12476 | `main` | B | except區塊print可見警示 |
| 128 | L12528 | `main` | **A** | 見第三節 A6,回歸測試 `test_main_wti_ret_failure_marks_unavailable` |
| 204 | L12570 | `main` | **A** | 見第三節 A9,回歸測試 `test_main_brain_json_corrupt_marks_unavailable` |
| 129 | L12590 | `main` | **A** | 見第三節 A8,回歸測試 `test_main_derive_c_drain_failure_marks_unavailable` |
| 205 | L12669 | `main` | **A** | 見第三節 A10,回歸測試 `test_main_rs_rank_scan_failure_marks_unavailable` |
| 130 | L12720 | `main` | **A** | 見第三節 A11,回歸測試 `test_main_risk_off_vix_failure_marks_unavailable` |
| 131 | L12732 | `main` | B | 未來事件風格預載失敗,屬增強因子退化基線(`_upcoming_event_styles={}`) |
| 206 | L12772 | `main` | C | 主ETF迴圈單檔數據崩潰:print完整traceback(可見)後continue跳過該檔 |
| 207 | L12867 | `main` | B | 持倉資金背離疊加失敗,屬增強因子退化基線 |
| 287 | L12896 | `main` | C | 保護EV歸因trace.append,純記帳 |
| 208 | L12906 | `main` | C | 同上 |
| 255 | L12944 | `main` | B | 早報果因診斷收集失敗,純診斷資料,不影響EV |
| 209 | L12951 | `main` | C | 保護EV歸因trace.append,純記帳 |
| 285 | L12970 | `main` | C | 同上 |
| 256 | L12974 | `main` | C | 同上 |
| 210 | L12993 | `main` | B | 影子共振偵測失敗,屬增強因子退化基線(觀察用,非觸發) |
| 257 | L13080 | `main` | C | 保護EV歸因trace.append,純記帳 |
| 258 | L13228 | `main` | B | 宏觀逆風判定失敗回False,有行內註解說明此為既定設計 |
| 211 | L13231 | `main` | B | except區塊print可見警示("計算失敗→回退無縮放") |
| 212 | L13237 | `main` | B | IC技巧乘子失敗,屬增強因子退化基線(中性乘數,見idx111) |
| 213 | L13297 | `main` | B | EV歸因文字組裝失敗,屬顯示層退化 |
| 214 | L13309 | `main` | B | except區塊print可見警示 |
| 215 | L13348 | `main` | B | EV歸因鏈顯示失敗,屬顯示層退化 |
| 216 | L13366 | `main` | B | 詳情卡資金流補充失敗,屬增強因子退化基線 |
| 259 | L13387 | `main` | B | 動態上限脆弱度輸入失敗,屬增強因子退化基線(`_dc_frag`維持0) |
| 260 | L13397 | `main` | B | 動態上限資金流輸入失敗,屬增強因子退化基線(`_dc_flow5`維持None,**保守擋下**非放行) |
| 261 | L13422 | `main` | B | PE分位讀取失敗,屬增強因子退化基線(`_dc_val`維持None,**保守擋下**) |
| 262 | L13435 | `main` | B | PE分位刷新失敗,同上(有print可見訊息) |
| 263 | L13460 | `main` | B | 預判分級失敗,屬增強因子退化基線(`_pre_grade`維持"none") |
| 264 | L13498 | `main` | B | 第1層否決邏輯失敗,屬增強因子退化基線(不否決≠造假訊號) |
| 265 | L13570 | `main` | B | 第2層分級顯示失敗,屬顯示層退化 |
| 266 | L13623 | `main` | B | 收盤確認錨顯示失敗,屬顯示層退化 |
| 217 | L13646 | `main` | B | except區塊print可見警示 |
| 267 | L13666 | `main` | B | T+1預測回填失敗,DB寫入,不影響本次已算出訊號 |
| 218 | L13679 | `main` | B | 持倉flow5補抓失敗,屬增強因子退化基線 |
| 280 | L13746 | `main` | B | 資金流快取失敗回 None,屬增強因子退化基線 |
| 132 | L13783 | `main` | B | main()內print/寫入報告可見文字區塊 |
| 133 | L13816 | `main` | B | 同上 |
| 219 | L13825 | `main` | B | except區塊print可見警示 |
| 220 | L13833 | `main` | B | 同上 |
| 221 | L13868 | `main` | B | main()內print/寫入報告可見文字區塊 |
| 222 | L13995 | `main` | B | 同上 |
| 288 | L14029 | `main` | B | 量價代理失敗,`_vp_ok=False`→明確印出「⬜ 資料未取得」(**可見**) |
| 268 | L14057 | `main` | B | 強背離信號彙整失敗,屬顯示層退化 |
| 269 | L14082 | `main` | B | main()內print/寫入報告可見文字區塊 |
| 223 | L14087 | `main` | B | 同上 |
| 224 | L14151 | `main` | B | 同上 |
| 225 | L14203 | `main` | B | 同上 |
| 226 | L14247 | `main` | B | except區塊print可見警示 |
| 227 | L14300 | `main` | B | 同上 |
| 228 | L14317 | `main` | B | 同上 |
| 229 | L14327 | `main` | B | 同上 |
| 134 | L14336 | `main` | B | main()內print/寫入報告可見文字區塊 |
| 135 | L14383 | `main` | B | 同上 |
| 136 | L14396 | `main` | B | 資金流map彙整失敗,屬增強因子退化基線 |
| 270 | L14467 | `main` | B | 開盤紀律文字彙整失敗,屬顯示層退化 |
| 230 | L14525 | `main` | B | 因果鏈文字組裝失敗,屬顯示層退化 |
| 137 | L14551 | `main` | B | main()內print/寫入報告可見文字區塊 |
| 231 | L14570 | `main` | B | 同上 |
| 232 | L14581 | `main` | B | 同上 |
| 233 | L14652 | `main` | B | 預測柱狀圖資料彙整失敗,屬顯示層退化 |
| 138 | L14681 | `main` | B | except區塊print可見警示 |
