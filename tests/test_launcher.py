"""Windows 批次檔必須是 CRLF 換行。
LF 換行的 .cmd 會被 cmd.exe 切錯行,把註解碎片當指令執行
(使用者實跑:'er' / 'olves' / 'wuba' is not recognized as an internal or external command)。"""
import glob
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_batch_files_use_crlf():
    files = glob.glob(os.path.join(ROOT, "*.cmd")) + glob.glob(os.path.join(ROOT, "*.bat"))
    for f in files:
        data = open(f, "rb").read()
        bare_lf = data.replace(b"\r\n", b"").count(b"\n")
        assert bare_lf == 0, f"{os.path.basename(f)} 有 {bare_lf} 個 LF 換行,cmd.exe 會解析錯誤"


def test_gitattributes_forces_crlf_for_batch_files():
    attrs = open(os.path.join(ROOT, ".gitattributes"), encoding="utf-8").read()
    assert "*.cmd -text" in attrs and "*.bat -text" in attrs          # 以 CRLF 原樣存入,不靠 checkout 轉換
