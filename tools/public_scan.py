"""扫描公开候选中的常见敏感文件、私网维护信息与密钥特征。"""

import ast
import ipaddress
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP = {".git", ".venv", "__pycache__", ".pytest_cache", "build", "dist", "htmlcov"}
BLOCK_SUFFIX = {".db", ".db-wal", ".db-shm", ".sqlite", ".sqlite3", ".jsonl", ".log",
                ".pem", ".key", ".pfx", ".p12", ".zip", ".7z"}
BLOCK_NAMES = {"上下文交接.md", "部署说明.md", "云端凭据.py", "晨间报告.py", "known_hosts"}
TEXT_SUFFIX = {".py", ".md", ".txt", ".json", ".html", ".yml", ".yaml", ".toml", ".lock"}
TOKEN_RULES = {
    "私钥内容": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "GitHub令牌": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})\b"),
    "AWS访问标识": re.compile(r"\bAKIA[A-Z0-9]{16}\b"),
    "Telegram令牌": re.compile(r"\b[0-9]{8,12}:[A-Za-z0-9_-]{30,}\b"),
    "Windows个人路径": re.compile(r"[A-Z]:[\\/]Users[\\/][^\\/\s]+", re.I),
}
IP_PATTERN = re.compile(r"(?<![\d\\])(?:\d{1,3}\.){3}\d{1,3}(?![\d\\])")
SECRET_NAME = re.compile(r"(?:^|_)(?:password|secret|token|api_key|passphrase)(?:_|$)", re.I)


def literal_secret_findings(tree, relative):
    """补充检查非空凭据字面量；明确标注的假值和短测试占位不算实际凭据。"""
    results = []

    def inspect(name, value, line):
        if (SECRET_NAME.search(name) and isinstance(value, ast.Constant)
                and isinstance(value.value, str) and len(value.value) >= 8
                and not value.value.lower().startswith(("example", "dummy", "fake", "test", "sample"))):
            results.append({"file": relative, "line": line, "rule": "疑似硬编码凭据"})

    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                name = target.id if isinstance(target, ast.Name) else target.attr if isinstance(target, ast.Attribute) else ""
                inspect(name, node.value, node.lineno)
        elif isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    inspect(key.value, value, key.lineno)
    return results


def candidate_files(root=ROOT):
    return sorted(p for p in root.rglob("*") if p.is_file()
                  and not any(part in SKIP or part.endswith(".egg-info")
                              for part in p.relative_to(root).parts))


def scan(root=ROOT):
    """只报告文件、行号和规则；不回显可能命中的敏感值。"""
    findings = []
    for path in candidate_files(root):
        relative = path.relative_to(root).as_posix()
        if (path.suffix in BLOCK_SUFFIX or path.name in BLOCK_NAMES
                or path.name.startswith("signal-update")
                or (path.name.startswith(".env") and path.name != ".env.example")
                or any(part in {"data", "backups"} for part in path.relative_to(root).parts)):
            findings.append({"file": relative, "line": 0, "rule": "不应公开的文件"})
            continue
        if path.suffix not in TEXT_SUFFIX and not path.name.startswith("."):
            continue
        try:
            text = path.read_text(encoding="utf-8-sig")
        except UnicodeError:
            continue
        for line_no, line in enumerate(text.splitlines(), 1):
            for name, pattern in TOKEN_RULES.items():
                if pattern.search(line):
                    findings.append({"file": relative, "line": line_no, "rule": name})
            for match in IP_PATTERN.finditer(line):
                try:
                    address = ipaddress.ip_address(match.group())
                except ValueError:
                    continue
                documentation = any(address in ipaddress.ip_network(net)
                                    for net in ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24"))
                if not address.is_loopback and not documentation:
                    findings.append({"file": relative, "line": line_no, "rule": "非示例IP地址"})
        if path.suffix == ".py":
            try:
                tree = ast.parse(text, filename=relative)
                findings.extend(literal_secret_findings(tree, relative))
            except SyntaxError as error:
                findings.append({"file": relative, "line": error.lineno, "rule": "Python语法错误"})
    return findings


if __name__ == "__main__":
    import json
    results = scan()
    print(json.dumps({"candidate_files": len(candidate_files()), "findings": results}, ensure_ascii=False, indent=2))
    raise SystemExit(bool(results))
