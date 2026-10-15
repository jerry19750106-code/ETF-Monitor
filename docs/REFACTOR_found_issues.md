# 任務 C：重構中發現、刻意未修的問題

> 依任務 C 硬性約束「**不可修改任何計算邏輯**；發現疑似 bug 不要修，記錄行號交由後續處理」。
> 行號指**重構前**的 `src/V24_19_ETF_System.py`（commit `9154808`）。
> 標註「已由 #N 處理」者，是在其他 PR 中修正的，本 PR 一律未動。

| # | 位置 | 問題 | 影響 | 狀態 |
|---|---|---|---|---|
| 1 | `_sector_kw_map`（L3876） | 以 `globals().get('HOLDING_SECTOR_KEYWORDS')` 延遲讀取後面才定義的常數 | 隱性耦合：函式無法搬到其他模組，否則會讀到空字典 | 未修；Phase 3 搬常數時一併處理 |
| 2 | `DirectDataFetcher` 與 `PrecisionProxyEngine`（L6098、L6385） | `get_ndx`、`get_sox`、`get_vix`、`get_sina_proxy_ret` 在兩個類別各寫一份，內容幾乎相同 | 修一處容易漏改另一處（`[V22.33.3]` 註解記載過一次分岔事故） | 未修 |
| 3 | RS 預掃描（L12589–12595）、主迴圈（L12703–12714）、`WatchlistScanner.scan_watchlist`（L8785 起） | RSI、乖離、趨勢在三處各自實作，寫法略有不同 | 三處結果可能不一致 | 未修 |
| 4 | `main()`（L11652–14624） | 單一函式 2,973 行；以 `locals().get` 讀取變數 49 處 | 變數未定義時會靜默變成 `None` 繼續執行（L13382 註解已自承） | 未修；Phase 6 |
| 5 | 模組層級（L799、L1050、L1831、L7674） | import 時就會探測東財網路、建立目錄、建立持倉本、可能觸發盡調批量抓取 | 無法在不設環境變數的情況下安全 import 與測試 | 未修 |
| 6 | `_dated_nav`（L5026） | 抓取失敗回 `None`，解析失敗回 `[]`，與 docstring「失敗回 []」不符 | 呼叫端必須同時處理兩種哨兵值 | 未修 |
| 7 | `IntelTeam.get_cny_20ma`（L9263） | 宣告回傳 `float`，`r is None` 時卻回 `[]` | 型別不一致 | 未修（#1 稽核亦有記錄） |
| 8 | `VERBOSE_ERRORS`（L979、L11673） | 由 `main()` 以 `global` 改寫 | 將 `_log_err` 搬出主程式前，必須先把狀態集中，否則 `--verbose` 失效 | 未修；Phase 2 前置條件 |
| 9 | 主迴圈 `rsi_s`（L12714）、RS 預掃描 `rsi_s_rs`（L12595） | `(50 − abs(RSI − 50)) / 50` 對 RSI 30（超賣）與 70（超買）給出相同分數 | 丟失方向資訊 | 未修（屬模型設計，需回測驗證後再決定） |
| 10 | `BASE_DIR` 預設值（L1049） | 寫死作者本機的 Windows 使用者路徑 | 其他環境未設 `ETF_OUTPUT_DIR` 時，會建立名稱怪異的目錄 | 已修:預設改為程式所在資料夾的上一層 |
| 11 | `KellyEngine.compute`（L6789） | `PRIOR_WIN_RATE`、`PRIOR_WEIGHT` 定義後未使用；賠率實質寫死 1.5 | 倉位計算失真 | 已由 #4 處理 |
| 12 | `KalmanLearner.update`（L8091） | 平衡點在日誤差 5%，W 實務上只會單向上升 | 信心權重與表現無關 | 已由 #4 處理 |
| 13 | `main()` brain.json 區塊（L12505） | 檔案缺失或損毀時 `macro` 未定義，c_drain 推導必定失敗並落到 0.20 | 地緣溫度不更新 | 已由 #3 處理 |
| 14 | `_apply_actual_by_semantic`、`backfill_yesterday_actuals`（L4457、L7887） | 回填日期錯配 | 回測樣本污染 | 已由 #6 處理 |

## 後續:ruff F821(未定義名稱)掃描結果

> 行號指 PR #8 之後的 `src/V24_19_ETF_System.py`。

| # | 位置 | 問題 | 狀態 |
|---|---|---|---|
| 15 | `compute_holding_overlap` | `res` 未初始化,只要有任何重疊對就拋 `NameError`;盡調「單點風險/持股」兩欄整段失效 | 已修(PR #8,含測試) |
| 16 | `main()` 盤前報 premise 區塊(L13212 起) | 引用未定義的 `ndx_ret`/`sox_ret`;且盤前報在約 L11730 已 `return`,**此區塊永遠不會執行** | 已修:抽成 `build_premarket_forecast`,由盤前報流程呼叫(依使用者決定啟用) |
| 17 | L715 `_YH_CACHE_FILE` | `BASE_DIR` 尚未定義時以 `"BASE_DIR" in dir()` 防護,執行時不會出錯,屬 lint 誤報;但代表快取檔永遠落在目前工作目錄,而非輸出目錄 | 未修 |
