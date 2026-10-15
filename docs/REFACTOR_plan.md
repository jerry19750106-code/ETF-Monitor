# 任務 C：模組化重構計畫

> 目標：把 14,628 行單檔拆成可測試的模組，**行為完全不變**。
> 版本以 `__VERSION__ = "V24.30"` 為準。本文行號一律指**重構前**的 `src/V24_19_ETF_System.py`（commit `9154808`）。

## 一、核心風險：為什麼不能一次全搬

主程式大量依賴**模組層級的可變全域狀態**，函式在呼叫當下才讀取它們：

- 設定值：`LOG_DB`、`BASE_DIR`（import 時由環境變數決定）、`VERBOSE_ERRORS`（`main()` 內以 `global` 改寫）
- 快取：`_FLOW_CACHE`、`_DAYKLINE_CACHE`、`_ORDERFLOW_CACHE`、`_EM_DEAD_EP`、`_IC_CACHE`
- 持倉：`USER_HOLDINGS`、`USER_COSTS`、`TARGET_ETFS`（import 時由 `build_positionbook` 建立）

函式一旦搬到別的模組，讀到的就是**新模組自己的全域**。主程式在執行時期改寫這些變數，例如 `--verbose` 打開 `VERBOSE_ERRORS`，搬走的函式就看不到了。這種**沉默的行為改變**正是本任務禁止的，因此必須分階段進行。

## 二、Phase 1（本 PR 已完成）：44 個純函式

**判定方式**：以 AST 機械分析每個頂層函式**實際讀取的全域名稱**，只保留滿足以下條件者：
1. 只依賴 `datetime`、`re`、`numpy`、builtins，以及同批的其他純函式；
2. 不使用 `globals()`、`locals()`、`vars()`、`eval`、`exec`、`getattr` 等反射手段（`_sector_kw_map` 因使用 `globals()` 被排除）；
3. 沒有 `global` / `nonlocal` 宣告，沒有裝飾器；
4. 未被其他進行中的 PR（#1–#6）修改，避免合併衝突。

**搬移方式**：由腳本逐字切出函式文字（`def` 行到最後一行），寫入新模組；主程式刪除原文，並在第一個被搬函式的位置以**原名**匯入。**沒有任何一個函式經過人工修改。**

| 模組 | 搬入的函式（重構前行號） |
|---|---|

| `data_source.py` | `sanitize_price_series`(L2082–2107), `_series_to_returns_by_date`(L2612–2620), `_align_overnight`(L2622–2635), `_now_utc8`(L2840–2846), `_clamp_snapshot_trend`(L4338–4352), `_expected_latest_us_date`(L4898–4903), `compute_pinned_ret`(L4922–4934), `_is_qdii_etf`(L5252–5261), `_proxy_to_secid`(L5824–5833), `_ae_nth_weekday`(L9531–9539), `_ae_adjust_weekend`(L9542–9546) |

| `signals.py` | `compute_fragility`(L2727–2761), `fragility_action`(L2763–2771), `identify_state_actor`(L2900–2931), `interpret_flow_behavior`(L3012–3036), `predict_tomorrow`(L3929–4012), `effective_sox_for`(L5001–5005), `judge_order_flow`(L6029–6060), `compute_realized_vol`(L6764–6773), `vol_scale_factor`(L6776–6783), `factor_attribution`(L7381–7408), `analyze_3day_trend`(L10196–10249), `analyze_cny_dual_track`(L10252–10296), `analyze_gold_oil_seesaw`(L10299–10347), `analyze_tech_concentration`(L10350–10369), `_pearson`(L10737–10746), `_spearman`(L10748–10755), `_action_to_symbol`(L10975–10983), `_compute_judgment`(L10986–11002), `derive_c_drain`(L11607–11649) |

| `position_book.py` | `_pb_status_from_pct`(L1391–1396), `_pb_parse_float`(L1398–1403), `positionbook_diff_audit`(L1816–1828), `classify_position_status`(L1837–1846) |

| `reporting.py` | `format_closing_handoff`(L4662–4696), `format_morning_diag_block`(L5605–5719), `_auto_position`(L7685–7693), `_geo_lbl_for_style`(L7723–7728), `_inline_md_format`(L8451–8466), `format_events_block`(L9676–9691), `generate_today_focus`(L10372–10514), `_pw_freshness_tag`(L10829–10842), `_pw_qualitative_display`(L10844–10853), `_geo_icon_from_cdrain`(L11596–11604) |


**驗證結果**（全部機械完成）：

| 檢查 | 結果 |
|---|---|
| 特徵測試（重構前建立，93 個固定輸入，含例外型別） | 重構後全部相同 |
| 函式原始碼 SHA-256（44 個） | 全部相同 |
| 逐字 diff：重構前切片 vs 新模組切片 | 0 個不一致 |
| `grep "^def <name>"`：主程式殘留定義 | 0 個 |
| AST 未定義名稱：重構後新增 | 0 個（主程式與 4 個新模組皆然） |
| 斷網實跑收盤報：重構前 vs 重構後 | `.txt` 逐位元組一致；`.html` 除「生成時間」外一致 |

主程式：14,628 → 13,604 行。

## 三、Phase 2 以後：有狀態函式的搬移方式（尚未執行）

**原則**：先把共享狀態集中到單一模組（例如 `state.py`），所有函式改為以 `state.LOG_DB` 這種**屬性存取**讀取；主程式改寫時也寫入 `state.X`。這樣不論函式位於哪個模組，看到的都是同一份狀態。

這一步會改動函式本體（全域名稱改為 `state.` 前綴），因此**不再是字元級一致**。必須以下列兩種方式保證行為不變：
1. 擴充特徵測試，納入有狀態函式，以暫存 DB 與模擬網路回應作為輸入；
2. 以「重構前 vs 重構後」斷網實跑的報告比對作為驗收。

建議順序：先搬依賴最少的，`main()` 最後拆。

| 階段 | 內容 | 前置條件 |
|---|---|---|
| 2 | `db`：SQLite 讀寫與回填，集中 schema 建立（目前有 23 處分散的 `CREATE TABLE IF NOT EXISTS`） | 建立 `state.py`；先合併 #1、#6，避免與回填修正衝突 |
| 3 | `data_source`：抓取函式與四個資料類別，並集中快取 | 合併 #1（資料可用性登記會進入這些函式） |
| 4 | `position_book`、`signals`：含 `KellyEngine`、`KalmanLearner` | 合併 #4 |
| 5 | `reporting`：報告組裝 | — |
| 6 | 拆解 `main()`（2,973 行）為各報告類型的流程函式，並清除 49 處 `locals().get` | 前述階段完成，且有完整的報告比對基線 |

### 剩餘函式的預定歸屬（重構前行號）

以名稱與職責初步歸類。`signals` 組另含少數通用工具函式，搬移前需逐一人工確認歸屬。

#### `data_source`(48 項,2557 行)

`_yh_proxy_ok`(L709), `_yh_cache_load`(L730), `_yh_cache_put`(L739), `_yh_cache_get`(L746), `_YHResp`(L757·class), `_yahoo_get`(L762), `_em_warmup`(L890), `_em_throttled_get`(L902), `load_overnight_override`(L1058), `apply_overnight_override`(L1102), `get_non_trading_reason`(L1324), `get_beta_us`(L2652), `check_proxy_collisions`(L2678), `fetch_announcements`(L3710), `scan_announcements_for_holdings`(L3737), `_news_diag`(L3791), `load_news_csv`(L3825), `fetch_pe_percentile_eastmoney`(L4080), `refresh_pe_percentile`(L4149), `_get_ak_funddb_fn`(L4215), `fetch_index_pe_history_funddb`(L4227), `fetch_index_pe_history_funddb_meta`(L4246), `refresh_pe_percentile_funddb`(L4294), `_us_ret_outlier_guard`(L4857), `_series_outlier_filter`(L4869), `fetch_dated_us_series`(L4905), `_load_anchor_cache`(L4936), `_save_anchor_cache`(L4945), `get_pinned_anchor`(L4952), `get_pinned_trajectory`(L4974), `_dated_yahoo`(L5011), `_dated_nav`(L5026), `fetch_intraday_quote`(L5450), `_volume_flow_proxy`(L5495), `get_order_flow_breakdown`(L5835), `_read_today_flow_from_db`(L5918), `DirectDataFetcher`(L6098·class), `PrecisionProxyEngine`(L6385·class), `_flow5_with_manual`(L7185), `_flow5_proxy_fallback`(L7224), `_load_dd_auto`(L7502), `_ensure_dd_auto`(L7616), `WatchlistScanner`(L8735·class), `IntelTeam`(L9128·class), `generate_auto_events`(L9555), `load_events_csv`(L9615), `get_upcoming_event_styles`(L9720), `fetch_sector_flow`(L9873)

#### `signals`(45 項,1642 行)

`yahoo_staleness`(L753), `_log_err`(L981), `is_trading_day`(L1295), `check_dynamic_cap`(L1957), `compute_max_drawdown_from_db`(L2052), `compute_max_drawdown_from_nav`(L2110), `compute_max_drawdown_best`(L2140), `compute_drawdown_position`(L2159), `compute_conditional_beta`(L2637), `compute_tech_coupling`(L2701), `_fragility_inputs_from`(L2773), `is_decision_window`(L2851), `identify_flow_actors`(L2934), `attribute_today`(L3038), `_load_assess_reversal`(L3335), `reset_news_audit`(L3549), `compute_news_score`(L3552), `news_score_selftest`(L3604), `_sector_kw_map`(L3876), `_parse_pe_date`(L4260), `compute_pe_pct_from_series`(L4270), `prime_conditional_betas`(L5055), `compute_pred_bias`(L5151), `KellyEngine`(L6789·class), `TrendRegimeDetector`(L6866·class), `apply_flow_divergence`(L7101), `detect_bottom_resonance`(L7144), `_dd_has_warn`(L7265), `scan_duplicate_manager`(L7366), `compute_holding_overlap`(L7484), `_auto_single_point_risk`(L7696), `_auto_holding_overlap`(L7711), `QuantEnhanced`(L7731·class), `KalmanLearner`(L8091·class), `_styles_affected_by_event`(L9712), `compute_crowdedness`(L9956), `locate_holdings_in_sectors`(L10007), `compute_reduce_conviction`(L10074), `grade_action_signal`(L10142), `compute_order`(L10517), `compute_information_coefficient`(L10757), `compute_forward_ic`(L10789), `_effective_breadth`(L10855), `ic_conviction_multiplier`(L10863), `_run_self_audit`(L11494)

#### `position_book`(7 項,508 行)

`check_position_anchor_freshness`(L1135), `_pb_mark_to_market`(L1405), `_pb_window_return`(L1448), `_migrate_positions_to_db`(L1476), `_pb_read_rows`(L1559), `build_positionbook`(L1626), `PositionBook`(L6703·class)

#### `db`(30 項,1472 行)

`_detect_pe_source_connected`(L1945), `flow5_outage_days`(L2993), `record_news_scan`(L3614), `check_news_silent_streak`(L3632), `save_pe_percentile`(L4025), `load_pe_percentile`(L4041), `update_pe_history`(L4113), `compute_pe_percentile_local`(L4128), `save_watchlist_snapshot`(L4355), `save_daily_snapshot`(L4426), `backfill_t1_actual`(L4448), `_apply_actual_by_semantic`(L4457), `save_morning_premise`(L4504), `load_morning_premise`(L4516), `save_closing_handoff`(L4563), `load_closing_handoff`(L4647), `record_intraday_compare`(L5569), `persist_capital_flow`(L5943), `read_capital_flow_trend`(L5980), `record_shadow_signal`(L7160), `PerformanceAttributor`(L7883·class), `_init_db`(L9451), `save_raw_signal`(L9748), `read_reduce_streak`(L9765), `read_prev_flow5`(L9815), `save_sector_snapshot`(L9928), `_db_write`(L10665), `_get_accuracy`(L10929), `_mirror_four_report_db`(L11005), `update_four_report_tracker`(L11026)

#### `reporting`(32 項,2063 行)

`_version_consistency_check`(L990), `check_tracker_freshness`(L1189), `render_factor_matrix`(L2212), `render_forecast_bias_audit`(L2257), `render_signal_conflicts`(L2319), `render_drawdown_position`(L2368), `render_drawdown_budget`(L2447), `render_exposure_status`(L2514), `build_volume_monitor`(L3095), `build_daily_conclusion`(L3152), `_build_market_summary`(L3236), `_build_reversal_judgment`(L3345), `_build_attribution_narrative`(L3452), `news_audit_report_line`(L3649), `format_announcements`(L3769), `build_news_score_map`(L3881), `format_morning_premise`(L4526), `format_close_scorecard`(L4698), `format_premise_verification`(L4776), `render_conditional_beta_block`(L5096), `format_order_flow_block`(L5722), `_dd_freshness`(L7281), `build_dd_profile`(L7297), `build_factor_attr_block`(L7411), `OutlookTeam`(L8140·class), `ReportFormatter`(L8255·class), `markdown_to_html`(L8469), `format_crowdedness`(L9986), `format_sector_rotation`(L10027), `format_reduce_trace`(L10129), `render_ic_scorecard`(L10874), `_send_telegram`(L10959)

#### `main`(1 項,2973 行)

`main`(L11652)


