"""在临时目录、回环网络限制下运行公开版全部检查。"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))


def main():
    for key in ("NO_PROXY", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                "http_proxy", "https_proxy", "all_proxy"):
        os.environ.pop(key, None)
    from public_scan import scan, candidate_files
    findings = scan()
    if findings:
        for hit in findings:
            print(f"扫描未通过：{hit['file']}:{hit['line']} {hit['rule']}")
        return 1
    print(f"公开候选扫描通过：{len(candidate_files())} 个文件，常见敏感特征命中 0。", flush=True)
    from signal_ledger.network_guard import install_loopback_only
    install_loopback_only()
    with tempfile.TemporaryDirectory(prefix="signal-ledger-tests-") as scratch:
        os.environ["SIGNAL_LEDGER_DATA_DIR"] = scratch
        suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), top_level_dir=str(ROOT))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
