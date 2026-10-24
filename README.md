# ETF Monitor — A股場外基金 / 台股 ETF 輪動監控系統

[![tests](https://github.com/jerry19750106-code/ETF-Monitor/actions/workflows/tests.yml/badge.svg)](https://github.com/jerry19750106-code/ETF-Monitor/actions/workflows/tests.yml)
![python](https://img.shields.io/badge/python-3.10%20%7C%203.12-blue)
![license](https://img.shields.io/badge/license-MIT-green)

[English](README.en.md) | **中文**

每個交易日自動產出四份報告(盤前 / 早報 / 午報 / 收盤報),追蹤 A股場外聯接基金與台股 ETF 的方向訊號、
資金流、回撤位置與倉位風險,並**持續稽核自己的訊號到底有沒有用**。

> *A daily monitoring system for China A-share feeder funds and Taiwan ETFs. It produces four reports a day
> (pre-market, morning, midday, close), and audits its own signals: tradable edge, data lineage and backtests are
> published alongside the code — including where the signals do **not** work.*

> ⚠️ **不是投資建議。** 本系統的訊號經回測只有微弱、邊緣顯著的可交易優勢(見 `docs/BACKTEST_baseline.md`、
> `docs/DATA_LINEAGE.md`),收盤報的訊號照著做在樣本期是虧損的。請勿據此做任何實際交易決策。

## 特色

- **四報因果鏈**:收盤報寫下明日前提 → 盤前報預測 → 早報以開盤實況驗證 → 午報在 15:00 截單前給最後決策
- **誠實的資料**:抓不到的資料顯示為「不可用」並在報告頂部列出,不用 0 或預設值冒充
- **自我稽核**:每份報告附訊號「可交易優勢」(依實際成交時點)與資訊係數(IC);不顯著的建議不背書
- **場外基金實務**:收盤淨值成交、15:00 截單、QDII T+7 回款、A/C 份額差異都納入計算
- **270+ 個測試**,CI 於 Python 3.10 / 3.12 執行,測試全程不連網

## 快速開始

```bash
pip install "numpy>=1.24" "requests>=2.31" "openpyxl>=3.1" "pytest>=7.4"
# 選用:pip install xlrd akshare(讀舊版 .xls 持倉檔、備援資料源;沒有也能跑)

export ETF_OUTPUT_DIR=./etf_output   # 報告、資料庫、brain.json 的位置(未設時自動尋找,見下表)
                                     # Windows 命令提示字元:set ETF_OUTPUT_DIR=.\etf_output
python src/V24_19_ETF_System.py --force-type 收盤報     # 盤前報 / 早報 / 午報 / 收盤報
python src/V24_19_ETF_System.py --help
```

版本以主程式內的 `__VERSION__` 為準(目前 V24.30);檔名 `V24_19` 為歷史沿用。

| 環境變數 | 作用 | 預設 |
|---|---|---|
| `ETF_OUTPUT_DIR` | 輸出與資料庫目錄 | 未設時自動尋找有 `v20_prediction_log.db` 的資料夾:程式所在資料夾的上一層 → 桌面 `ETF_Monitor`;都沒有則用前者,並在報告頂部標明「從空白歷史開始」 |
| `EM_FORCE_IPV4` | `0` = import 時不探測東方財富網路 | 探測 |
| `DD_AUTO_REFRESH` | `0` = 不自動執行盡調抓取(需 `dd_fetcher.py`) | 自動 |
| `ETF_VERBOSE` | `1` = 印出所有被捕捉的例外 | `0`(靜音) |
| `TG_BOT_TOKEN`、`TG_CHAT_ID` | Telegram 推播;未設則不推播 | 未設 |

需要能連到:天天基金 / 東方財富(A股 NAV、資金流)、新浪財經(場內代理即時價)、Yahoo Finance(台股、美股指數)。
連不到時程式照樣跑完,但報告頂部「本次資料不可用」會逐項列出缺了哪些資料;第一次執行(資料夾裡還沒有歷史資料庫)也會在該處註明「從空白歷史開始」。

## 個人持倉

持倉金額不寫在原始碼裡。複製 `src/personal_holdings.example.json` 為 `src/personal_holdings.json`(已列入 `.gitignore`)
並填入自己的資料;沒有這個檔案時,持倉以資料庫 / CSV 的持倉本為準。

## 外部模組

主程式會在執行時 import 下列 4 個模組,皆放在 `src/`(與主程式同一資料夾)。缺少任何一個時程式仍可執行,
對應功能停用並在報告中明確警告,不會靜默。

| 模組 | 提供什麼 | 公開版狀態 |
|---|---|---|
| `dd_fetcher.py` | 基金盡職調查 9 維(規模、費率、經理、持股穿透等),產出 `dd_meta_auto.json`;主程式 7 天 TTL 自動觸發 | ✅ 完整 |
| `news_fetcher.py`(`ensure_news_csv()`) | 每日產業新聞抓取與 13 板塊分類,產出 `news.csv` | ✅ 完整 |
| `patch_reversal_criteria.py`(`assess_reversal`) | 「輪動反抽 vs 反轉確認」三條件判定式 | ✅ 完整 |
| `funddb_client.py` | 底層指數多年 PE 序列 | ⚠️ **僅介面**:原作者私人版直連韭圈說 API 需要 client 端簽章,公開版不提供簽章與連線實作。一律回 `None`,主程式自動改用滾動分位。有合法資料源者實作 `fetch_pe_series()` 即可接上 |

## 目錄結構

| 路徑 | 內容 |
|---|---|
| `src/V24_19_ETF_System.py` | 主程式(約 14,000 行) |
| `src/returns_store.py` | 實現報酬單一來源(`realized_returns`),來源分級 NAV / 交易所收盤 > 歷史 > 場內代理 |
| `src/signal_health.py` | 各報告 × 市場的滾動可交易優勢;不顯著的操作建議不背書 |
| `src/migrate_realized_returns.py` | 一次性歷史清洗(預覽 / 備份 / 稽核表 / 冪等) |
| `src/backtest_engine.py`、`src/data_quality.py` | 回測(stored / semantic / tradable 三口徑)與資料品質報告 |
| `src/data_availability.py` | 資料抓取失敗的登記;報告頂部顯示「本次資料不可用」 |
| `src/data_source.py`、`signals.py`、`position_book.py`、`reporting.py` | 自主程式拆出的純函式(重構第一階段) |
| `data_sample/v20_prediction_log.db` | 樣本資料庫(**原始、未清洗**,作為證據與測試輸入) |
| `tests/` | 270+ 個測試;CI 於 Python 3.10 / 3.12 執行 |
| `docs/` | 變更紀錄、稽核、回測基線、資料血緣、重構計畫 |

## 既有資料庫升級

第一次以新版執行主程式時,會自動建立 `realized_returns` 並以歷史值填入(只新增,不修改既有資料)。
歷史錯配的修正需手動執行,預設只預覽:

```bash
python src/migrate_realized_returns.py <你的 v20_prediction_log.db>                          # 預覽
python src/migrate_realized_returns.py <你的 v20_prediction_log.db> --apply                  # 自動備份後寫入
python src/migrate_realized_returns.py <你的 v20_prediction_log.db> --apply --rebuild-from-nav  # 再以官方 NAV 重建(需網路)
```

## 測試

```bash
EM_FORCE_IPV4=0 DD_AUTO_REFRESH=0 python -m pytest -q
```

測試在 import 主程式期間攔截所有 `requests` 呼叫,不連網。

## 文件

| 文件 | 內容 |
|---|---|
| `docs/CHANGELOG.md` | 版本變更紀錄 |
| `docs/AUDIT_silent_errors.md` | 288 個 `except` 區塊的完整分類與修復 |
| `docs/BACKTEST_baseline.md` | 三口徑回測基線與資料品質 |
| `docs/DATA_LINEAGE.md` | 實現報酬的來源、清洗與訊號健康度 |
| `docs/REFACTOR_plan.md`、`docs/REFACTOR_found_issues.md` | 模組化計畫與重構中發現、刻意未修的問題 |

## 開發紀律

- **雙軌驗證**:靜態(grep / AST)+ 行為測試(import 真碼,只 mock 網路)
- **每個修復先證明測試會失敗,再修到通過**
- **金額與統計一律用程式驗算**,每個結論附證據
- 修正 bug 時先列出全部同類問題,再逐一處理

## 授權與聯絡

本專案以 [MIT License](LICENSE) 授權:可自由使用、修改、再散布(含商業用途),須保留版權與授權聲明;
**作者不提供任何擔保,也不對使用本程式造成的任何損失負責。**

- 作者:蕭永滐(Jerry Hsiao)
- Email:jerry19750106@gmail.com
- GitHub:[@jerry19750106-code](https://github.com/jerry19750106-code)

程式直接連線天天基金、東方財富、新浪財經、Yahoo Finance、韭圈說等網站取得行情。
這些網站的資料受各自使用條款約束,MIT 授權**不涵蓋**這些資料;使用者須自行確認並遵守各站條款。
