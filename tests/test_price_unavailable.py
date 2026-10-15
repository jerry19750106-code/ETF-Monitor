"""價格取不到的標的必須出現在報告頂部「本次資料不可用」(2026-10-15 審查:原本只在第 99 行附近提一句)。"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
import data_availability as da  # noqa: E402


def test_etf_prices_shown_in_top_block():
    da.reset()
    da._data_unavailable("ETF_PRICES", "2/17 檔價格取不到,未納入本報:AAA001, BBB002")
    block = da.format_unavailable_block()
    assert "ETF 價格" in block and "AAA001" in block
    da.reset()


def test_main_registers_skipped_prices():
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "src", "V24_19_ETF_System.py"), encoding="utf-8").read()
    assert '_data_unavailable("ETF_PRICES"' in src and '_data_unavailable("WATCH_PRICES"' in src
    assert "_PRICE_SKIPPED.clear()" in src
