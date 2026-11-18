"""公开扫描的回归：验证误放文件和硬编码凭据会被拦截，且不回显内容。"""

import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("public_scan_tested", Path(__file__).parents[1] / "tools" / "public_scan.py")
scanner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scanner)


class PublicScanTests(unittest.TestCase):
    def test_database_rejected_even_when_content_is_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "account.db").write_bytes(b"")
            self.assertTrue(any(hit["rule"] == "不应公开的文件" for hit in scanner.scan(root)))

    def test_private_config_and_environment_file_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".env").write_text("", encoding="utf-8")
            (root / "上下文交接.md").write_text("", encoding="utf-8")
            self.assertEqual(len(scanner.scan(root)), 2)

    def test_generic_hardcoded_credential_detected_without_echoing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            synthetic = "q" * 32
            (root / "config.py").write_text('exchange_secret = "' + synthetic + '"', encoding="utf-8")
            hits = scanner.scan(root)
            self.assertTrue(any(hit["rule"] == "疑似硬编码凭据" for hit in hits))
            self.assertNotIn(synthetic, str(hits))

    def test_explicit_example_value_and_empty_default_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config.py").write_text('exchange_secret = "example-not-a-real-secret"\npassword = ""', encoding="utf-8")
            self.assertEqual(scanner.scan(root), [])
