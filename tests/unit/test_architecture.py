"""Guards for the architecture rules in CLAUDE.md that can be checked statically."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src" / "trader"

# The domain may use only the standard library (minus IO/network) and the ULID helper.
DOMAIN_ALLOWED_THIRD_PARTY = {"ulid"}
DOMAIN_FORBIDDEN_STDLIB = {"asyncio", "socket", "ssl", "http", "urllib", "os", "subprocess", "sqlite3", "io"}


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module)
    return names


def _py_files(sub: str) -> list[Path]:
    return sorted((SRC / sub).rglob("*.py"))


@pytest.mark.parametrize("path", _py_files("domain"), ids=lambda p: p.name)
def test_domain_has_no_io_or_framework_imports(path: Path) -> None:
    import sys

    for name in _imports(path):
        top = name.split(".")[0]
        if top in ("__future__", "trader"):
            assert name.startswith(("__future__", "trader.domain")), f"{path.name} imports {name}"
            continue
        assert top not in DOMAIN_FORBIDDEN_STDLIB, f"{path.name} imports {name}"
        assert top in sys.stdlib_module_names or top in DOMAIN_ALLOWED_THIRD_PARTY, (
            f"{path.name} imports {name}"
        )


def test_no_broker_names_outside_adapters() -> None:
    pattern = re.compile(r"""==\s*["'](exness|ctrader)["']|["'](exness|ctrader)["']\s*==""")
    offenders = [
        str(p.relative_to(SRC))
        for p in SRC.rglob("*.py")
        if "brokers" not in p.relative_to(SRC).parts and pattern.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_no_float_annotations_in_domain_or_oms() -> None:
    for sub in ("domain", "oms"):
        for path in _py_files(sub):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.AnnAssign | ast.arg) and node.annotation is not None:
                    assert "float" not in ast.unparse(node.annotation), f"float in {path.name}"
