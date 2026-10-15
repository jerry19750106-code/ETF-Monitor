# 實現報酬的資料血緣(Data Lineage)

> 對應程式:`src/returns_store.py`、`src/signal_health.py`、`src/migrate_realized_returns.py`,
> 主程式 `_apply_actual_by_semantic`、`PerformanceAttributor` 三條回填路徑。
> 測試:`tests/test_realized_returns.py`(24 個;在修改前的程式上 24 個全部失敗,其中 11 個是行為錯誤,13 個是新模組/新介面尚不存在)。

## 一、問題:同一天的「實際報酬」有三個版本

| 位置 | 原本以為是 | 實際上是 | 誰在讀 |
|---|---|---|---|
| `predictions.actual_ret` | 實現報酬 | 三條回填路徑**誰先跑就用誰**(「已回填就不覆寫」):收盤時場內代理先寫,隔天的官方 NAV 永遠補不進去 → A股幾乎全是**代理估計** | 命中率、Kelly、Kalman、回測 |
| `daily_snapshot.today_ret` | 實際收盤(`format_close_scorecard` docstring 寫 `returns[-1]`) | **系統當日估值**:`predicted_ret`,即場內代理漲跌再經 RSI>70 截斷(最多砍 85%)與 ±8% 夾擠 | 收盤計分板、真前瞻 IC、因子歸因 |
| `capital_flow_daily.nav_ret` | 當日 NAV 漲跌 | 盤中最後一次觀測到的**場內代理漲跌,單位百分比**;歷史有 8 筆以小數寫入 | 量價背離 |

後果:收盤計分板與真前瞻 IC 是拿**自己的估計當答案**計分;倉位乘子用的是早報**同期** IC(含開盤後走勢)。

## 二、修正:單一來源 `realized_returns`

```
realized_returns(code, date, ret, src, ts)   PRIMARY KEY(code, date)   ret 一律小數
```

| src | 等級 | 意義 | 寫入者 |
|---|:-:|---|---|
| `nav` | 3 | 場外基金官方單位淨值 —— 投資人實際成交價 | `backfill_yesterday_actuals`(近 10 筆 NAV 每一對相鄰日) |
| `exch_close` | 3 | 上市標的交易所收盤價(台股 ETF 本身) | `backfill_taiwan_today_actuals`(帶日期的 Yahoo 收盤) |
| `legacy` | 2 | 遷移前的歷史值,來源不可追溯 | 首次建表時自動填入 / 遷移腳本 |
| `proxy` | 1 | 場內代理 ETF 漲跌 —— 場外基金的估計 | `backfill_a_stock_today_actuals`(僅交易日 15:00 後) |

規則:高等級覆蓋低等級,反之不行;同級先寫者為準。`predictions.actual_ret` 依 report_type 語意從此表推導,
並記錄 `actual_src`。下游一律讀此表:`format_close_scorecard`、`compute_forward_ic`、`build_factor_attr_block`、
`_get_signal_payoff`(Kelly)、`KalmanLearner._pairs`、新增的 `compute_tradable_ic`、回測引擎、訊號健康度。

日期一律來自資料本身:NAV 日期、Yahoo 時間戳(含交易所時區)。原本台股取「收盤序列最後兩筆」標成今天,
休市日執行會配錯日;A股代理在休市日/盤中也會寫入「今天」。兩者都已修正。

## 三、倉位乘子改用可交易 IC

`ic_conviction_multiplier` 原本 = 早報同期收縮 IC ÷ 0.05,夾在 0.70–1.15。同期 IC 含開盤後走勢,
所以乘子幾乎恆為上限。現改用 `compute_tradable_ic`:截單前訊號 vs **次一交易日**實現報酬,
收縮與標準誤以**交易日數**為有效樣本。隱含 IR 也改用可交易 IC。

沙盒斷網實跑收盤報(樣本庫,`MAIN_EXIT: OK`)的前後對照:

| 指標 | 修改前 | 修改後 |
|---|---:|---:|
| 倉位乘子(A股) | ×1.15 | **×0.70** |
| 驅動乘子的 IC | 同期收縮 IC +0.538 | 可交易 IC +0.018(49 交易日,未達顯著) |
| 隱含 IR | +1.072 | +0.026 |
| 真前瞻 IC | +0.150(對照的是系統估值) | +0.067(對照實現報酬) |

## 四、訊號健康度閘門(`signal_health.py`)

每次產出早/午/收盤報時,以 tradable 口徑計算近 120 日每個(報告 × 市場)的日聚合 t 值:

- 樣本 < 30 個交易日 → 樣本不足;t ≥ Bonferroni 校正臨界值(6 組時 2.39)→ 顯著;t ≥ 1.645 → 邊緣;t ≤ −1.645 → 顯著為負;其餘 → 未證實。
- **只有「顯著」與「邊緣」會被系統背書**。其餘的操作建議保留金額,改列「待你裁決」(沿用 V22.59「不剝奪選擇」原則)。
- 健康度算不出來時一律不背書(fail-closed),並登記到報告頂部「本次資料不可用」。

樣本庫(截至 2026-09-30,近 120 日,遷移後)的結果:

| 報告 | 市場 | 交易日 | 平均每筆 | t | 狀態 |
|---|---|---:|---:|---:|---|
| 早報 | A股 | 74 | +0.256% | +1.94 | ✅ 邊緣 |
| 早報 | 台股 | 74 | +0.445% | +1.72 | ✅ 邊緣 |
| 午報 | A股 | 80 | +0.166% | +1.36 | ⚠️ 未證實 |
| 午報 | 台股 | 80 | +0.535% | +2.20 | ✅ 邊緣 |
| 收盤報 | A股 | 73 | −0.204% | −1.38 | ⚠️ 未證實 |
| 收盤報 | 台股 | 73 | +0.107% | +0.39 | ⚠️ 未證實 |

**注意**:A股午報(15:00 截單前最後執行窗)目前不被背書。這是資料給的結論,不是閘門設定錯誤。

## 五、歷史清洗(`migrate_realized_returns.py`)

```bash
python src/migrate_realized_returns.py <db>                                  # 預覽,不寫入
python src/migrate_realized_returns.py <db> --apply                          # 備份 <db>.bak-時間戳 後寫入
python src/migrate_realized_returns.py <db> --apply --rebuild-from-nav       # 再以官方 NAV 重建A股歷史(需網路)
```

每筆變更寫入 `data_corrections`(表、主鍵、欄位、舊值、新值、原因)。冪等:第二次執行 0 筆。
樣本庫(`data_sample/` 保持原始未清洗,作為證據與測試輸入)預覽結果:

| 步驟 | 筆數 |
|---|---:|
| 1. 以歷史 T+0 實現值建立 realized_returns(src=legacy) | 1,198 |
| 2. 收盤報 actual_ret 由單一來源重新推導 | 660 |
| 　其中 T+0 錯配(完全等於當日報酬) | 280 |
| 　其中「來源不同無法判定」 | 377 |
| 　其中原本未回填、現由單一來源補上 | 3 |
| 　→ 改配次一交易日 / 清為未回填(中間缺漏工作日或無資料) | 490 / 170 |
| 3. `capital_flow_daily.nav_ret` 小數 → 百分比 | 8 |

為什麼「來源不同」也要重推:這群值與**當日**報酬的方向一致率 63%、與次日只有 51%,而且 8–9 月(V22.39 修復後)仍是如此。
來源之一是舊版週一早上把週五的 NAV 報酬標成「週日」,再配給週五的收盤報 —— 來源不同,精確比對抓不到。

遷移後,收盤報 stored 口徑準確度 65.9% → **51.7%**,與 semantic 口徑(52.0%)一致;每一筆仍有值的收盤報,
`actual_ret` 都等於單一來源的次一交易日實現報酬(測試 `test_migration_on_sample_copy_removes_contamination`)。

## 六、仍未解決(誠實列)

1. 遷移前的 1,198 筆 legacy 實現值仍是代理與 NAV 混用。要徹底解決須在本機執行 `--rebuild-from-nav`(沙盒無法連到天天基金)。
2. 中間缺漏工作日的判斷無法區分長假與漏跑,一律留空(寧缺勿錯),所以 170 筆收盤報變成未回填。
3. `daily_snapshot.t1_actual` 只有寫入、沒有任何讀取者;未刪除,但也不再被當成實現報酬使用。
4. `daily_snapshot.today_ret` 的寫入端沒有改(仍是系統估值),只是不再被任何程式當成實際值讀取。
