# ETF Monitor — rotation monitor for China A-share funds and Taiwan ETFs

[![tests](https://github.com/jerry19750106-code/ETF-Monitor/actions/workflows/tests.yml/badge.svg)](https://github.com/jerry19750106-code/ETF-Monitor/actions/workflows/tests.yml)
![python](https://img.shields.io/badge/python-3.10%20%7C%203.12-blue)
![license](https://img.shields.io/badge/license-MIT-green)

**English** | [中文](README.md)

Every trading day it produces four reports (pre-market, morning, midday, close) that track direction signals,
fund flows, drawdown position and position risk for China A-share off-exchange feeder funds and Taiwan ETFs —
and it **keeps auditing whether its own signals actually work**.

> ⚠️ **Not investment advice.** Backtests show the signals have only a weak, borderline-significant tradable edge
> (see `docs/BACKTEST_baseline.md` and `docs/DATA_LINEAGE.md`); following the close-report signals lost money over
> the sample period. Do not use this for real trading decisions.

The reports, code comments and docs are written in Chinese, because the funds, data sources and trading rules
covered here are Chinese-market specific.

## Features

- **Four-report causal chain**: the close report records tomorrow's premises → the pre-market report predicts →
  the morning report checks against the actual open → the midday report gives the final call before the 15:00 cut-off
- **Honest data**: anything that could not be fetched is shown as "unavailable" and listed at the top of the report,
  never replaced by 0 or a default value
- **Self-audit**: every report shows each signal's tradable edge (at the real execution time) and information
  coefficient (IC); recommendations that are not significant are not endorsed
- **Off-exchange fund mechanics**: trades at the closing NAV, 15:00 order cut-off, QDII T+7 settlement and
  A/C share-class differences are all modelled
- **270+ tests**; CI runs on Python 3.10 and 3.12, and the tests never touch the network

## Quick start

```bash
pip install "numpy>=1.24" "requests>=2.31" "openpyxl>=3.1" "pytest>=7.4"
# optional: pip install xlrd akshare   (old .xls holdings files, backup data source; not required)

export ETF_OUTPUT_DIR=./etf_output        # Windows (cmd): set ETF_OUTPUT_DIR=.\etf_output
python src/V24_19_ETF_System.py --force-type 收盤報     # 盤前報 / 早報 / 午報 / 收盤報
python src/V24_19_ETF_System.py --help
```

The report type is passed in Chinese: `盤前報` (pre-market), `早報` (morning), `午報` (midday), `收盤報` (close).
The version is the `__VERSION__` constant in the main program (currently V24.30); the `V24_19` file name is historical.

| Environment variable | Purpose | Default |
|---|---|---|
| `ETF_OUTPUT_DIR` | Folder for reports and the database | If unset, uses the first folder that already has `v20_prediction_log.db`: the parent of the program's folder → `Desktop/ETF_Monitor`; if none has one, the former, and the report states it is "starting from an empty history" |
| `EM_FORCE_IPV4` | `0` = do not probe Eastmoney's network on import | probe |
| `DD_AUTO_REFRESH` | `0` = do not run the due-diligence fetcher automatically | auto |
| `ETF_VERBOSE` | `1` = print every caught exception | `0` (quiet) |
| `TG_BOT_TOKEN`, `TG_CHAT_ID` | Telegram push; no push if unset | unset |

Network access needed: Tiantian Fund / Eastmoney (A-share NAV, fund flows), Sina Finance (live prices of the
exchange-traded proxies) and Yahoo Finance (Taiwan stocks, US indices). If they cannot be reached the program still
finishes, and the "data unavailable" section at the top of the report lists exactly what is missing.

## Your own holdings

No holding amounts are stored in the source code. Copy `src/personal_holdings.example.json` to
`src/personal_holdings.json` (already in `.gitignore`) and fill in your own data; without it, holdings come from the
position book in the database / CSV.

## External modules

The main program imports these four modules from `src/` at run time. If any is missing the program still runs,
disables that feature and warns about it in the report.

| Module | What it provides | Status in this repo |
|---|---|---|
| `dd_fetcher.py` | 9-dimension fund due diligence (size, fees, manager, look-through holdings…), writes `dd_meta_auto.json`; refreshed automatically every 7 days | ✅ complete |
| `news_fetcher.py` (`ensure_news_csv()`) | Daily industry news, classified into 13 sectors, writes `news.csv` | ✅ complete |
| `patch_reversal_criteria.py` (`assess_reversal`) | Three-condition test: rotation bounce vs. confirmed reversal | ✅ complete |
| `funddb_client.py` | Multi-year PE series of the underlying index | ⚠️ **interface only**: always returns `None`, and the main program falls back to rolling percentiles. Implement `fetch_pe_series()` if you have a legitimate data source |

## Layout

| Path | Contents |
|---|---|
| `src/V24_19_ETF_System.py` | Main program (~14,000 lines) |
| `src/returns_store.py` | Single source of realized returns, ranked by source: NAV / exchange close > history > proxy |
| `src/signal_health.py` | Rolling tradable edge per report × market; non-significant actions are not endorsed |
| `src/migrate_realized_returns.py` | One-off history clean-up (preview / backup / audit table / idempotent) |
| `src/backtest_engine.py`, `src/data_quality.py` | Backtests (stored / semantic / tradable) and data-quality report |
| `src/data_availability.py` | Registry of failed fetches, shown at the top of each report |
| `src/data_source.py`, `signals.py`, `position_book.py`, `reporting.py` | Pure functions split out of the main program |
| `data_sample/v20_prediction_log.db` | Sample database (**raw, uncleaned**, used as evidence and test input) |
| `tests/` | Test suite |
| `docs/` | Changelog, audits, backtest baseline, data lineage, refactor plan (Chinese) |

## Upgrading an existing database

The first run of a new version creates `realized_returns` and fills it from history (adds only, never modifies).
Fixing historical mismatches is manual and previews by default:

```bash
python src/migrate_realized_returns.py <your v20_prediction_log.db>                            # preview
python src/migrate_realized_returns.py <your v20_prediction_log.db> --apply                    # back up, then write
python src/migrate_realized_returns.py <your v20_prediction_log.db> --apply --rebuild-from-nav # rebuild from official NAV (network)
```

## Tests

```bash
EM_FORCE_IPV4=0 DD_AUTO_REFRESH=0 python -m pytest -q
```

All `requests` calls are intercepted while the main program is imported, so the tests never go online.

## License and contact

[MIT License](LICENSE): free to use, modify and redistribute (including commercially) as long as the copyright and
license notice are kept. **No warranty; the author is not liable for any loss from using this program.**

- Author: Jerry Hsiao
- Email: jerry19750106@gmail.com
- GitHub: [@jerry19750106-code](https://github.com/jerry19750106-code)

The program fetches market data from Tiantian Fund, Eastmoney, Sina Finance, Yahoo Finance and Jiuquanshuo.
That data is governed by each site's own terms; the MIT license does **not** cover it, and users must check and
follow those terms themselves.
