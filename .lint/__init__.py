"""DBOS lint rules loader and runner.

This module auto-discovers and runs all DBOS lint rules defined in .lint/DBOSxxx.py files.
Each rule must have a paired .adoc documentation file.

Usage:
    from .lint import run_all_rules, get_rule_docs, list_rules

    # Run all rules on a directory
    violations = run_all_rules(Path("job_scrape_application/"))

    # Get documentation for a rule
    docs = get_rule_docs("DBOS001")

    # List all available rules
    rules = list_rules()
"""

from __future__ import annotations

import importlib
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

# Directory containing this module
LINT_DIR = Path(__file__).parent

# Paths to skip when running rules
SKIP_PATHS = frozenset({
    "services/convex_client.py",
    "_archive/",
    "legacy/",
    "__pycache__/",
    "core/dependencies.py",
    "core/mock_clients.py",
    "core/test_helpers.py",
    "testing/",
    "schedule_audit.py",
})


class ViolationProtocol(Protocol):
    """Protocol for violation objects."""

    @property
    def rule(self) -> str: ...
    @property
    def file(self) -> Path: ...
    @property
    def line(self) -> int: ...
    def format(self) -> str: ...


class RuleModule(Protocol):
    """Protocol for rule modules."""

    RULE_ID: str
    SUMMARY: str

    def check_file(self, file_path: Path, source: str | None = None) -> Sequence[Any]: ...


def should_skip_path(path: Path) -> bool:
    """Check if a path should be skipped."""
    path_str = str(path)
    return any(skip in path_str for skip in SKIP_PATHS)


def discover_rules() -> list[str]:
    """Discover all available rule IDs (DBOS001, DBOS002, etc.)."""
    pattern = re.compile(r"^DBOS\d{3}$")
    rules = []
    for py_file in LINT_DIR.glob("DBOS*.py"):
        rule_id = py_file.stem
        if pattern.match(rule_id):
            adoc_file = LINT_DIR / f"{rule_id}.adoc"
            if adoc_file.exists():
                rules.append(rule_id)
    return sorted(rules)


def load_rule(rule_id: str) -> RuleModule:
    """Load a rule module by ID."""
    # Ensure .lint is importable
    lint_parent = LINT_DIR.parent
    if str(lint_parent) not in sys.path:
        sys.path.insert(0, str(lint_parent))

    module_name = f".lint.{rule_id}"
    return importlib.import_module(module_name)


def get_rule_docs(rule_id: str) -> str:
    """Get the AsciiDoc documentation for a rule."""
    adoc_path = LINT_DIR / f"{rule_id}.adoc"
    if not adoc_path.exists():
        raise FileNotFoundError(f"Documentation not found for {rule_id}")
    return adoc_path.read_text(encoding="utf-8")


def get_rule_summary(rule_id: str) -> str:
    """Get the short summary for a rule."""
    module = load_rule(rule_id)
    return module.SUMMARY


def list_rules() -> list[dict[str, str]]:
    """List all available rules with their summaries."""
    rules = []
    for rule_id in discover_rules():
        try:
            summary = get_rule_summary(rule_id)
            rules.append({"id": rule_id, "summary": summary})
        except Exception:
            rules.append({"id": rule_id, "summary": "(error loading rule)"})
    return rules


def check_file(file_path: Path, source: str | None = None) -> list[ViolationProtocol]:
    """Run all rules on a single file."""
    if should_skip_path(file_path):
        return []

    if source is None:
        try:
            source = file_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return []

    all_violations: list[ViolationProtocol] = []

    for rule_id in discover_rules():
        try:
            module = load_rule(rule_id)
            violations = module.check_file(file_path, source)
            all_violations.extend(violations)
        except Exception as e:
            print(f"Warning: Error running {rule_id} on {file_path}: {e}", file=sys.stderr)

    return all_violations


def check_directory(directory: Path) -> list[ViolationProtocol]:
    """Run all rules on all Python files in a directory."""
    all_violations: list[ViolationProtocol] = []
    for file_path in directory.rglob("*.py"):
        all_violations.extend(check_file(file_path))
    return all_violations


def run_all_rules(paths: list[Path]) -> list[ViolationProtocol]:
    """Run all rules on the given paths (files or directories)."""
    all_violations: list[ViolationProtocol] = []
    for path in paths:
        if path.is_file():
            all_violations.extend(check_file(path))
        elif path.is_dir():
            all_violations.extend(check_directory(path))
    return all_violations


def format_all_docs() -> str:
    """Format all rule documentation into a single document."""
    lines = ["= DBOS Lint Rules", "", ""]

    for rule_id in discover_rules():
        try:
            docs = get_rule_docs(rule_id)
            # Convert level-1 heading to level-2 for combined doc
            docs = re.sub(r"^= ", "== ", docs, count=1)
            docs = re.sub(r"^== ", "=== ", docs, flags=re.MULTILINE)
            lines.append(docs)
            lines.append("")
            lines.append("'''")  # Horizontal rule
            lines.append("")
        except Exception as e:
            lines.append(f"== {rule_id}")
            lines.append("")
            lines.append(f"Error loading documentation: {e}")
            lines.append("")

    return "\n".join(lines)


def print_rule_list() -> None:
    """Print a list of all rules with summaries."""
    print("Available DBOS lint rules:\n")
    for rule in list_rules():
        print(f"  {rule['id']}: {rule['summary']}")
    print()


def print_rule_docs(rule_id: str) -> None:
    """Print the documentation for a specific rule."""
    try:
        docs = get_rule_docs(rule_id)
        print(docs)
    except FileNotFoundError:
        print(f"Rule {rule_id} not found.", file=sys.stderr)
        sys.exit(1)


def print_all_docs() -> None:
    """Print all rule documentation."""
    print(format_all_docs())
