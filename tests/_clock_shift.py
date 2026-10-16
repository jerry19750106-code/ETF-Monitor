"""CI 專用 pytest 外掛:把系統時鐘往後撥 SHIFT_DAYS 天再跑整套測試。

用途:抓「今天寫的測試,明天就失敗」的日期定時炸彈。
平常跑測試不會載入本檔;CI 以 `-p _clock_shift` 並設 SHIFT_DAYS 啟用(需要 time-machine 套件)。
"""
import datetime
import os

_traveller = None


def pytest_configure(config):
    global _traveller
    days = int(os.environ.get("SHIFT_DAYS", "0") or 0)
    if not days:
        return
    import time_machine
    _traveller = time_machine.travel(
        datetime.datetime.now() + datetime.timedelta(days=days), tick=True)
    _traveller.start()


def pytest_unconfigure(config):
    if _traveller is not None:
        _traveller.stop()
