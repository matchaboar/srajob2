#!/usr/bin/env python3
"""Custom lint rules for @DBOS.step decorator usage.

This script uses AST analysis to enforce seven rules. Each rule is defined in
.lint/DBOSxxx.py with documentation in .lint/DBOSxxx.adoc.

Usage:
    uv run scripts/lint_dbos_step.py [paths...]
    uv run scripts/lint_dbos_step.py job_scrape_application/
    uv run scripts/lint_dbos_step.py  # defaults to job_scrape_application/

Documentation commands:
    uv run scripts/lint_dbos_step.py --list          # List all rules
    uv run scripts/lint_dbos_step.py --explain DBOS001  # Explain a specific rule
    uv run scripts/lint_dbos_step.py --all-docs      # Output all documentation

Ruff integration:
    uv run scripts/lint_dbos_step.py --sync-ruff     # Update ruff.toml external rules

Exit codes:
    0 - No violations found (or documentation was printed)
    1 - Violations found
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path
from typing import NamedTuple

# Directory containing rule files
LINT_DIR = Path(__file__).parent.parent / ".lint"


class FunctionInfo(NamedTuple):
    """Information about a function definition."""

    file: Path
    line: int
    name: str
    is_async: bool
    has_dbos_step: bool


class ExtendedFunctionInfo(NamedTuple):
    """Extended information about a function for call graph analysis."""

    file: Path
    line: int
    name: str
    is_async: bool
    calls_convex: bool  # Directly calls convex_query/mutation/action
    called_functions: frozenset[str]  # Names of functions called by this function

# httpx methods that require @DBOS.step when called
HTTPX_METHODS = frozenset({
    "get",
    "post",
    "put",
    "patch",
    "delete",
    "head",
    "options",
    "request",
    "send",
    "stream",
})

# Convex functions that must be called synchronously (DBOS004)
CONVEX_FUNCTIONS = frozenset({
    "convex_query",
    "convex_mutation",
    "convex_action",
})

# Sync DBOS methods that should be async in coroutine workflows (DBOS005/DBOS006)
# Maps sync method name -> async method name
DBOS_SYNC_TO_ASYNC_WORKFLOW_LAUNCH: dict[str, str] = {
    "start_workflow": "start_workflow_async",
    "enqueue": "enqueue_async",
}

# DBOS context methods that have async variants (DBOS006)
DBOS_SYNC_TO_ASYNC_CONTEXT: dict[str, str] = {
    "sleep": "sleep_async",
    "recv": "recv_async",
    "send": "send_async",
    "set_event": "set_event_async",
    "get_event": "get_event_async",
}

# Forbidden sleep/async primitives in DBOS workflows and steps (DBOS007)
# Maps (module, function) -> recommended DBOS alternative
FORBIDDEN_SLEEP_CALLS: dict[tuple[str, str], str] = {
    ("time", "sleep"): "DBOS.sleep (sync) or DBOS.sleep_async (async)",
    ("asyncio", "sleep"): "DBOS.sleep_async",
}

# Paths to skip (relative to repo root)
SKIP_PATHS = frozenset({
    "services/convex_client.py",  # The client itself doesn't need @DBOS.step
    "_archive/",
    "legacy/",
    "__pycache__/",
    # Test infrastructure
    "core/dependencies.py",  # Capturing wrappers for fixture generation
    "core/mock_clients.py",  # Mock implementations
    "core/test_helpers.py",  # Test helpers
    "testing/",  # Test utilities
    "schedule_audit.py",  # Audit script, not a workflow
})


class Violation(NamedTuple):
    """A lint violation."""

    file: Path
    line: int
    function_name: str
    call_type: str  # "convex", "httpx", or "await_sync"
    call_name: str
    rule: str = "DBOS001"  # DBOS001, DBOS002, or DBOS003


# Global registry of @DBOS.step decorated functions
# Key: function name, Value: FunctionInfo
# Note: This is a simplification that assumes unique function names across the codebase
# for @DBOS.step decorated functions (which is typically the case)
_function_registry: dict[str, FunctionInfo] = {}

# Global registry for call graph analysis (DBOS004)
# Key: function name, Value: ExtendedFunctionInfo
_extended_function_registry: dict[str, ExtendedFunctionInfo] = {}


# =============================================================================
# Documentation functions
# =============================================================================

def get_rule_ids() -> list[str]:
    """Get list of all rule IDs from .lint directory."""
    import re
    pattern = re.compile(r"^DBOS\d{3}$")
    rules = []
    if LINT_DIR.exists():
        for adoc_file in LINT_DIR.glob("DBOS*.adoc"):
            rule_id = adoc_file.stem
            if pattern.match(rule_id):
                rules.append(rule_id)
    return sorted(rules)


def get_rule_summary(rule_id: str) -> str:
    """Get the summary line from a rule's .py file."""
    py_file = LINT_DIR / f"{rule_id}.py"
    if not py_file.exists():
        return "(rule file not found)"
    try:
        source = py_file.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "SUMMARY":
                        if isinstance(node.value, ast.Constant):
                            return str(node.value.value)
        return "(no summary found)"
    except Exception:
        return "(error reading rule)"


def get_rule_docs(rule_id: str) -> str:
    """Get the AsciiDoc documentation for a rule."""
    adoc_file = LINT_DIR / f"{rule_id}.adoc"
    if not adoc_file.exists():
        return f"Documentation not found for {rule_id}"
    return adoc_file.read_text(encoding="utf-8")


def print_rule_list() -> None:
    """Print a list of all rules with summaries."""
    print("Available DBOS lint rules:\n")
    for rule_id in get_rule_ids():
        summary = get_rule_summary(rule_id)
        print(f"  {rule_id}: {summary}")
    print("\nRun with --explain <RULE> for detailed documentation.")


def print_rule_docs(rule_id: str) -> None:
    """Print the documentation for a specific rule."""
    rule_id = rule_id.upper()
    if rule_id not in get_rule_ids():
        print(f"Rule {rule_id} not found.", file=sys.stderr)
        print(f"Available rules: {', '.join(get_rule_ids())}", file=sys.stderr)
        sys.exit(1)
    print(get_rule_docs(rule_id))


def print_all_docs() -> None:
    """Print all rule documentation as a single document."""
    import re
    print("= DBOS Lint Rules")
    print()
    for rule_id in get_rule_ids():
        docs = get_rule_docs(rule_id)
        # Convert level-1 heading to level-2 for combined doc
        docs = re.sub(r"^= ", "== ", docs, count=1)
        docs = re.sub(r"^== ", "=== ", docs, flags=re.MULTILINE)
        print(docs)
        print()
        print("'''")  # Horizontal rule
        print()


# =============================================================================
# Ruff integration
# =============================================================================

RUFF_TOML_PATH = Path(__file__).parent.parent / "ruff.toml"


def sync_ruff_external_rules() -> bool:
    """Sync ruff.toml external rules from .lint/*.py files.

    Returns True if ruff.toml was updated, False if already in sync.
    """
    import re

    rule_ids = get_rule_ids()
    if not rule_ids:
        print("No DBOS rules found in .lint/", file=sys.stderr)
        return False

    if not RUFF_TOML_PATH.exists():
        print(f"ruff.toml not found at {RUFF_TOML_PATH}", file=sys.stderr)
        return False

    content = RUFF_TOML_PATH.read_text(encoding="utf-8")

    # Build the new external line (use JSON for proper double quotes in TOML)
    import orjson
    new_external = f'external = {orjson.dumps(rule_ids).decode("utf-8")}'

    # Pattern to match the external line
    pattern = r'^external\s*=\s*\[.*?\]'

    if re.search(pattern, content, re.MULTILINE):
        # Replace existing external line
        new_content = re.sub(pattern, new_external, content, flags=re.MULTILINE)
    else:
        # Add external line after [lint] section
        lint_pattern = r'(\[lint\]\s*\n(?:.*?\n)*?)(ignore\s*=.*?\n)'
        match = re.search(lint_pattern, content)
        if match:
            new_content = content[:match.end()] + new_external + "\n" + content[match.end():]
        else:
            print("Could not find [lint] section in ruff.toml", file=sys.stderr)
            return False

    if new_content == content:
        print(f"ruff.toml external rules already in sync: {rule_ids}")
        return False

    RUFF_TOML_PATH.write_text(new_content, encoding="utf-8")
    print(f"Updated ruff.toml external rules: {rule_ids}")
    return True


# =============================================================================
# AST Visitors and Checkers
# =============================================================================

class FunctionRegistryCollector(ast.NodeVisitor):
    """First-pass AST visitor that collects all @DBOS.step decorated function definitions."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.functions: list[FunctionInfo] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_function(node, is_async=False)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_function(node, is_async=True)
        self.generic_visit(node)

    def _check_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef, *, is_async: bool
    ) -> None:
        """Check if function has @DBOS.step decorator and record it."""
        has_dbos_step = self._has_dbos_step_decorator(node)
        if has_dbos_step:
            self.functions.append(
                FunctionInfo(
                    file=self.file_path,
                    line=node.lineno,
                    name=node.name,
                    is_async=is_async,
                    has_dbos_step=True,
                )
            )

    def _has_dbos_step_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """Check if function has @DBOS.step decorator."""
        for decorator in node.decorator_list:
            # Handle @DBOS.step or @DBOS.step(...)
            if isinstance(decorator, ast.Call):
                decorator = decorator.func

            # Check for DBOS.step attribute access
            if isinstance(decorator, ast.Attribute):
                if decorator.attr == "step":
                    # Check if it's DBOS.step
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False


class ExtendedFunctionCollector(ast.NodeVisitor):
    """AST visitor that collects function info including convex calls and call graph."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.functions: list[ExtendedFunctionInfo] = []
        # Import tracking: alias -> original_name
        self._imports: dict[str, str] = {}
        # Track current function context
        self._current_function: str | None = None
        self._current_function_line: int = 0
        self._current_is_async: bool = False
        self._current_calls_convex: bool = False
        self._current_called_functions: set[str] = set()

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Track imports from modules."""
        for alias in node.names:
            name = alias.name
            asname = alias.asname if alias.asname else name
            self._imports[asname] = name
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_function(node, is_async=False)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_function(node, is_async=True)

    def _check_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef, *, is_async: bool
    ) -> None:
        """Collect function info including convex calls and called functions."""
        # Save parent context
        prev_function = self._current_function
        prev_line = self._current_function_line
        prev_is_async = self._current_is_async
        prev_calls_convex = self._current_calls_convex
        prev_called_functions = self._current_called_functions

        # Set up new context
        self._current_function = node.name
        self._current_function_line = node.lineno
        self._current_is_async = is_async
        self._current_calls_convex = False
        self._current_called_functions = set()

        # Visit function body
        self.generic_visit(node)

        # Record function info
        self.functions.append(
            ExtendedFunctionInfo(
                file=self.file_path,
                line=self._current_function_line,
                name=self._current_function,
                is_async=self._current_is_async,
                calls_convex=self._current_calls_convex,
                called_functions=frozenset(self._current_called_functions),
            )
        )

        # Restore parent context
        self._current_function = prev_function
        self._current_function_line = prev_line
        self._current_is_async = prev_is_async
        self._current_calls_convex = prev_calls_convex
        self._current_called_functions = prev_called_functions

    def visit_Call(self, node: ast.Call) -> None:
        """Track function calls, especially convex functions."""
        if self._current_function is None:
            self.generic_visit(node)
            return

        # Check for direct function calls like convex_query(...)
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            # Resolve import alias to original name
            original_name = self._imports.get(func_name, func_name)

            # Check if this is a convex function
            if original_name in CONVEX_FUNCTIONS:
                self._current_calls_convex = True

            # Track all function calls
            self._current_called_functions.add(original_name)

        self.generic_visit(node)


def compute_transitive_convex_callers(
    registry: dict[str, ExtendedFunctionInfo],
) -> set[str]:
    """Compute the set of all functions that call convex (directly or transitively).

    Uses iterative fixed-point computation to find all functions that:
    1. Directly call convex_query/mutation/action
    2. Call any function that (directly or transitively) calls convex
    """
    # Start with direct convex callers
    convex_callers: set[str] = {
        name for name, info in registry.items() if info.calls_convex
    }

    # Iterate until no new callers are found
    changed = True
    while changed:
        changed = False
        for name, info in registry.items():
            if name in convex_callers:
                continue
            # Check if this function calls any known convex caller
            if info.called_functions & convex_callers:
                convex_callers.add(name)
                changed = True

    return convex_callers


class DBOSStepChecker(ast.NodeVisitor):
    """AST visitor that checks for @DBOS.step decorator on functions with external calls."""

    def __init__(
        self,
        file_path: Path,
        source_lines: list[str],
        function_registry: dict[str, FunctionInfo],
        transitive_convex_callers: set[str] | None = None,
    ) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.violations: list[Violation] = []
        self._current_function: str | None = None
        self._current_function_line: int = 0
        self._has_dbos_step: bool = False
        self._has_dbos_workflow: bool = False
        self._has_noqa: bool = False
        self._has_noqa_dbos002: bool = False
        self._has_noqa_dbos003: bool = False
        self._has_noqa_dbos004: bool = False
        self._has_noqa_dbos005: bool = False
        self._has_noqa_dbos006: bool = False
        self._has_noqa_dbos007: bool = False
        self._is_async_function: bool = False
        # (call_type, call_name, is_async_call)
        self._external_calls: list[tuple[str, str, bool]] = []
        # Import tracking: alias -> original_name
        self._imports: dict[str, str] = {}
        # Function registry for DBOS003 checks
        self._function_registry = function_registry
        # Set of functions that call convex (directly or transitively) for DBOS004 checks
        self._transitive_convex_callers = transitive_convex_callers or set()

    def _has_noqa_comment(self, lineno: int, rule: str = "DBOS001") -> bool:
        """Check if a line has a noqa comment for the specified rule.

        Supports various formats (case-insensitive):
        - # noqa: DBOS004
        - # noqa:DBOS004
        - # noqa DBOS004
        - #noqa: DBOS004
        """
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1].lower()
        rule_lower = rule.lower()
        # Match noqa followed by optional colon and optional space, then rule
        return (
            f"noqa: {rule_lower}" in line
            or f"noqa:{rule_lower}" in line
            or f"noqa {rule_lower}" in line
        )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Track imports from modules."""
        for alias in node.names:
            name = alias.name
            asname = alias.asname if alias.asname else name
            self._imports[asname] = name
        self.generic_visit(node)

    def visit_Await(self, node: ast.Await) -> None:
        """Check for await on synchronous functions (DBOS003) and convex functions (DBOS004)."""
        # Only check inside functions
        if self._current_function is None:
            self.generic_visit(node)
            return

        # Check if the awaited expression is a call
        if isinstance(node.value, ast.Call):
            func_name = self._get_call_name(node.value)
            if func_name:
                # Resolve alias to original name
                original_name = self._imports.get(func_name, func_name)

                # DBOS004: Check for awaiting convex functions
                if not self._has_noqa_comment(node.lineno, "DBOS004"):
                    if original_name in CONVEX_FUNCTIONS:
                        self.violations.append(
                            Violation(
                                file=self.file_path,
                                line=node.lineno,
                                function_name=self._current_function,
                                call_type="await_convex",
                                call_name=func_name,
                                rule="DBOS004",
                            )
                        )

                # DBOS003: Check if this function is in the registry and is sync
                if not self._has_noqa_comment(node.lineno, "DBOS003"):
                    if original_name in self._function_registry:
                        func_info = self._function_registry[original_name]
                        if not func_info.is_async:
                            self.violations.append(
                                Violation(
                                    file=self.file_path,
                                    line=node.lineno,
                                    function_name=self._current_function,
                                    call_type="await_sync",
                                    call_name=func_name,
                                    rule="DBOS003",
                                )
                            )

        self.generic_visit(node)

    def _get_call_name(self, node: ast.Call) -> str | None:
        """Extract the function name from a Call node."""
        if isinstance(node.func, ast.Name):
            return node.func.id
        return None

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_function(node, is_async=False)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_function(node, is_async=True)

    def _check_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef, *, is_async: bool
    ) -> None:
        """Check a function definition for @DBOS.step decorator requirement."""
        # Save parent context
        prev_function = self._current_function
        prev_line = self._current_function_line
        prev_has_step = self._has_dbos_step
        prev_has_workflow = self._has_dbos_workflow
        prev_has_noqa = self._has_noqa
        prev_has_noqa_dbos002 = self._has_noqa_dbos002
        prev_has_noqa_dbos004 = self._has_noqa_dbos004
        prev_has_noqa_dbos005 = self._has_noqa_dbos005
        prev_has_noqa_dbos006 = self._has_noqa_dbos006
        prev_has_noqa_dbos007 = self._has_noqa_dbos007
        prev_is_async = self._is_async_function
        prev_calls = self._external_calls

        # Set up new context for this function
        self._current_function = node.name
        self._current_function_line = node.lineno
        self._has_dbos_step = self._has_dbos_step_decorator(node)
        self._has_dbos_workflow = self._has_dbos_workflow_decorator(node)
        self._has_noqa = self._has_noqa_comment(node.lineno, "DBOS001")
        self._has_noqa_dbos002 = self._has_noqa_comment(node.lineno, "DBOS002")
        self._has_noqa_dbos004 = self._has_noqa_comment(node.lineno, "DBOS004")
        self._has_noqa_dbos005 = self._has_noqa_comment(node.lineno, "DBOS005")
        self._has_noqa_dbos006 = self._has_noqa_comment(node.lineno, "DBOS006")
        self._has_noqa_dbos007 = self._has_noqa_comment(node.lineno, "DBOS007")
        self._is_async_function = is_async
        self._external_calls = []

        # DBOS004: Check if async function calls convex (directly or transitively)
        # Exempt @DBOS.workflow() - workflows coordinate async steps that call convex
        if is_async and not self._has_noqa_dbos004 and not self._has_dbos_workflow:
            if node.name in self._transitive_convex_callers:
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=node.name,
                        call_type="async_convex_caller",
                        call_name="convex",
                        rule="DBOS004",
                    )
                )

        # Visit function body to find external calls
        self.generic_visit(node)

        # DBOS001: Report violations if external calls found without @DBOS.step (and no noqa)
        if self._external_calls and not self._has_dbos_step and not self._has_noqa:
            for call_type, call_name, _ in self._external_calls:
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=self._current_function_line,
                        function_name=self._current_function,
                        call_type=call_type,
                        call_name=call_name,
                        rule="DBOS001",
                    )
                )

        # DBOS002: Check sync/async consistency if function has @DBOS.step decorator
        if self._external_calls and self._has_dbos_step and not self._has_noqa_dbos002:
            has_async_calls = any(is_async_call for _, _, is_async_call in self._external_calls)
            has_sync_calls = any(not is_async_call for _, _, is_async_call in self._external_calls)

            # If function calls async functions but is not declared async
            if has_async_calls and not self._is_async_function:
                # Find the first async call for the error message
                for call_type, call_name, is_async_call in self._external_calls:
                    if is_async_call:
                        self.violations.append(
                            Violation(
                                file=self.file_path,
                                line=self._current_function_line,
                                function_name=self._current_function,
                                call_type=call_type,
                                call_name=call_name,
                                rule="DBOS002",
                            )
                        )
                        break

            # If function only calls sync functions but is declared async
            if has_sync_calls and not has_async_calls and self._is_async_function:
                # Find the first sync call for the error message
                for call_type, call_name, is_async_call in self._external_calls:
                    if not is_async_call:
                        self.violations.append(
                            Violation(
                                file=self.file_path,
                                line=self._current_function_line,
                                function_name=self._current_function,
                                call_type=call_type,
                                call_name=call_name,
                                rule="DBOS002",
                            )
                        )
                        break

        # Restore parent context
        self._has_noqa = prev_has_noqa
        self._has_noqa_dbos002 = prev_has_noqa_dbos002
        self._has_noqa_dbos004 = prev_has_noqa_dbos004
        self._has_noqa_dbos005 = prev_has_noqa_dbos005
        self._has_noqa_dbos006 = prev_has_noqa_dbos006
        self._has_noqa_dbos007 = prev_has_noqa_dbos007
        self._current_function = prev_function
        self._current_function_line = prev_line
        self._has_dbos_step = prev_has_step
        self._has_dbos_workflow = prev_has_workflow
        self._is_async_function = prev_is_async
        self._external_calls = prev_calls

    def _has_dbos_step_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """Check if function has @DBOS.step decorator."""
        for decorator in node.decorator_list:
            # Handle @DBOS.step or @DBOS.step(...)
            if isinstance(decorator, ast.Call):
                decorator = decorator.func

            # Check for DBOS.step attribute access
            if isinstance(decorator, ast.Attribute):
                if decorator.attr == "step":
                    # Check if it's DBOS.step
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False

    def _has_dbos_workflow_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """Check if function has @DBOS.workflow decorator."""
        for decorator in node.decorator_list:
            # Handle @DBOS.workflow or @DBOS.workflow(...)
            if isinstance(decorator, ast.Call):
                decorator = decorator.func

            # Check for DBOS.workflow attribute access
            if isinstance(decorator, ast.Attribute):
                if decorator.attr == "workflow":
                    # Check if it's DBOS.workflow
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False

    def visit_Call(self, node: ast.Call) -> None:
        """Check if a call is to convex or httpx functions."""
        if self._current_function is None:
            # Not inside a function
            self.generic_visit(node)
            return

        # Check for httpx.get, httpx.post, etc. or client.get, client.post on httpx.Client
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            # Check for httpx.method() pattern - these are SYNC module-level functions
            if isinstance(node.func.value, ast.Name):
                if node.func.value.id == "httpx" and method_name in HTTPX_METHODS:
                    # httpx.get(), httpx.post() are sync
                    self._external_calls.append(("httpx", f"httpx.{method_name}", False))
            # Check for await client.method() where client might be httpx.AsyncClient
            # This is a heuristic - we look for common httpx client variable names
            # These are ASYNC calls (used with httpx.AsyncClient)
            if isinstance(node.func.value, ast.Name):
                var_name = node.func.value.id
                if var_name in ("client", "http_client", "async_client", "httpx_client"):
                    if method_name in HTTPX_METHODS:
                        # Assume client variable is AsyncClient (async)
                        self._external_calls.append(("httpx", f"{var_name}.{method_name}", True))

            # DBOS005/DBOS006: Check for sync DBOS methods in async workflows
            if self._is_async_function and self._has_dbos_workflow:
                if isinstance(node.func.value, ast.Name) and node.func.value.id == "DBOS":
                    # Check for workflow launch methods (DBOS005)
                    if method_name in DBOS_SYNC_TO_ASYNC_WORKFLOW_LAUNCH:
                        if not self._has_noqa_comment(node.lineno, "DBOS005"):
                            self.violations.append(
                                Violation(
                                    file=self.file_path,
                                    line=node.lineno,
                                    function_name=self._current_function,
                                    call_type="sync_workflow_launch",
                                    call_name=f"DBOS.{method_name}",
                                    rule="DBOS005",
                                )
                            )

                    # Check for context methods (DBOS006)
                    if method_name in DBOS_SYNC_TO_ASYNC_CONTEXT:
                        if not self._has_noqa_comment(node.lineno, "DBOS006"):
                            self.violations.append(
                                Violation(
                                    file=self.file_path,
                                    line=node.lineno,
                                    function_name=self._current_function,
                                    call_type="sync_dbos_context",
                                    call_name=f"DBOS.{method_name}",
                                    rule="DBOS006",
                                )
                            )

            # DBOS007: Check for forbidden sleep calls in DBOS workflows and steps
            if self._has_dbos_workflow or self._has_dbos_step:
                if isinstance(node.func.value, ast.Name):
                    module_name = node.func.value.id
                    key = (module_name, method_name)
                    if key in FORBIDDEN_SLEEP_CALLS:
                        if not self._has_noqa_comment(node.lineno, "DBOS007"):
                            self.violations.append(
                                Violation(
                                    file=self.file_path,
                                    line=node.lineno,
                                    function_name=self._current_function,
                                    call_type="forbidden_sleep",
                                    call_name=f"{module_name}.{method_name}",
                                    rule="DBOS007",
                                )
                            )

        self.generic_visit(node)


def should_skip_path(path: Path) -> bool:
    """Check if a path should be skipped."""
    path_str = str(path)
    return any(skip in path_str for skip in SKIP_PATHS)


def collect_functions_from_file(file_path: Path) -> list[FunctionInfo]:
    """First pass: collect all @DBOS.step function definitions from a file."""
    if should_skip_path(file_path):
        return []

    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"Warning: Could not parse {file_path}: {e}", file=sys.stderr)
        return []

    collector = FunctionRegistryCollector(file_path)
    collector.visit(tree)
    return collector.functions


def collect_functions_from_directory(directory: Path) -> dict[str, FunctionInfo]:
    """First pass: collect all @DBOS.step function definitions from a directory."""
    registry: dict[str, FunctionInfo] = {}
    for file_path in directory.rglob("*.py"):
        for func_info in collect_functions_from_file(file_path):
            registry[func_info.name] = func_info
    return registry


def collect_extended_functions_from_file(file_path: Path) -> list[ExtendedFunctionInfo]:
    """Collect extended function info (including convex calls) from a file."""
    if should_skip_path(file_path):
        return []

    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"Warning: Could not parse {file_path}: {e}", file=sys.stderr)
        return []

    collector = ExtendedFunctionCollector(file_path)
    collector.visit(tree)
    return collector.functions


def collect_extended_functions_from_directory(directory: Path) -> dict[str, ExtendedFunctionInfo]:
    """Collect extended function info from all files in a directory."""
    registry: dict[str, ExtendedFunctionInfo] = {}
    for file_path in directory.rglob("*.py"):
        for func_info in collect_extended_functions_from_file(file_path):
            registry[func_info.name] = func_info
    return registry


def check_file(
    file_path: Path,
    function_registry: dict[str, FunctionInfo],
    transitive_convex_callers: set[str] | None = None,
) -> list[Violation]:
    """Second pass: check a single Python file for violations."""
    if should_skip_path(file_path):
        return []

    try:
        source = file_path.read_text(encoding="utf-8")
        source_lines = source.splitlines()
        tree = ast.parse(source, filename=str(file_path))
    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"Warning: Could not parse {file_path}: {e}", file=sys.stderr)
        return []

    checker = DBOSStepChecker(file_path, source_lines, function_registry, transitive_convex_callers)
    checker.visit(tree)
    return checker.violations


def check_directory(
    directory: Path,
    function_registry: dict[str, FunctionInfo],
    transitive_convex_callers: set[str] | None = None,
) -> list[Violation]:
    """Second pass: recursively check all Python files in a directory for violations."""
    violations: list[Violation] = []
    for file_path in directory.rglob("*.py"):
        violations.extend(check_file(file_path, function_registry, transitive_convex_callers))
    return violations


def format_violation(v: Violation) -> str:
    """Format a violation for display."""
    if v.rule == "DBOS001":
        return (
            f"{v.file}:{v.line}: [{v.rule}] "
            f"Function `{v.function_name}` calls {v.call_type} function `{v.call_name}` "
            f"but is missing @DBOS.step decorator"
        )
    elif v.rule == "DBOS002":
        return (
            f"{v.file}:{v.line}: [{v.rule}] "
            f"Function `{v.function_name}` has @DBOS.step but calls {v.call_type} function "
            f"`{v.call_name}` with mismatched sync/async signature"
        )
    elif v.rule == "DBOS003":
        return (
            f"{v.file}:{v.line}: [{v.rule}] "
            f"Function `{v.function_name}` awaits synchronous function `{v.call_name}`"
        )
    elif v.rule == "DBOS004":
        if v.call_type == "await_convex":
            return (
                f"{v.file}:{v.line}: [{v.rule}] "
                f"Function `{v.function_name}` awaits convex function `{v.call_name}` "
                f"(convex functions are synchronous)"
            )
        else:  # async_convex_caller
            return (
                f"{v.file}:{v.line}: [{v.rule}] "
                f"Function `{v.function_name}` is async but calls convex "
                f"(directly or transitively). Functions calling convex must not be async."
            )
    elif v.rule == "DBOS005":
        sync_method = v.call_name
        async_method = DBOS_SYNC_TO_ASYNC_WORKFLOW_LAUNCH.get(
            sync_method.replace("DBOS.", ""), f"{sync_method}_async"
        )
        return (
            f"{v.file}:{v.line}: [{v.rule}] "
            f"Async workflow `{v.function_name}` uses `{sync_method}`. "
            f"Use `DBOS.{async_method}` instead."
        )
    elif v.rule == "DBOS006":
        sync_method = v.call_name
        method_name = sync_method.replace("DBOS.", "")
        async_method = DBOS_SYNC_TO_ASYNC_CONTEXT.get(method_name, f"{method_name}_async")
        return (
            f"{v.file}:{v.line}: [{v.rule}] "
            f"Async workflow `{v.function_name}` uses `{sync_method}`. "
            f"Use `DBOS.{async_method}` instead."
        )
    elif v.rule == "DBOS007":
        call_name = v.call_name
        # Parse module.function to look up the recommendation
        parts = call_name.split(".")
        if len(parts) == 2:
            key = (parts[0], parts[1])
            recommendation = FORBIDDEN_SLEEP_CALLS.get(key, "DBOS.sleep or DBOS.sleep_async")
        else:
            recommendation = "DBOS.sleep or DBOS.sleep_async"
        return (
            f"{v.file}:{v.line}: [{v.rule}] "
            f"DBOS function `{v.function_name}` uses `{call_name}`. "
            f"Use {recommendation} instead for durable sleep."
        )
    else:
        return (
            f"{v.file}:{v.line}: [{v.rule}] "
            f"Function `{v.function_name}`: unknown rule violation"
        )


def run_lint(paths: list[str]) -> int:
    """Run lint checks on the given paths."""
    # First pass: collect all @DBOS.step function definitions
    function_registry: dict[str, FunctionInfo] = {}
    # Also collect extended function info for call graph analysis (DBOS004)
    extended_registry: dict[str, ExtendedFunctionInfo] = {}
    all_paths: list[Path] = []

    for path_str in paths:
        path = Path(path_str)
        if path.is_file():
            all_paths.append(path)
            for func_info in collect_functions_from_file(path):
                function_registry[func_info.name] = func_info
            for func_info in collect_extended_functions_from_file(path):
                extended_registry[func_info.name] = func_info
        elif path.is_dir():
            all_paths.append(path)
            function_registry.update(collect_functions_from_directory(path))
            extended_registry.update(collect_extended_functions_from_directory(path))
        else:
            print(f"Warning: Path not found: {path}", file=sys.stderr)

    # Compute transitive convex callers for DBOS004
    transitive_convex_callers = compute_transitive_convex_callers(extended_registry)

    # Second pass: check for violations
    all_violations: list[Violation] = []

    for path in all_paths:
        if path.is_file():
            all_violations.extend(check_file(path, function_registry, transitive_convex_callers))
        elif path.is_dir():
            all_violations.extend(check_directory(path, function_registry, transitive_convex_callers))

    if all_violations:
        print(f"Found {len(all_violations)} DBOS lint violation(s):\n")
        for v in sorted(all_violations, key=lambda x: (x.rule, str(x.file), x.line)):
            print(format_violation(v))

        # Print rule summaries with help hint
        violated_rules = sorted(set(v.rule for v in all_violations))
        print()
        for rule_id in violated_rules:
            summary = get_rule_summary(rule_id)
            print(f"[{rule_id}] {summary}")
            print(f"  Run: uv run scripts/lint_dbos_step.py --explain {rule_id}")
            print()

        return 1

    print("No DBOS lint violations found.")
    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="DBOS lint rules for @DBOS.step decorator usage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Lint job_scrape_application/
  %(prog)s path/to/file.py          # Lint specific file
  %(prog)s --list                   # List all rules
  %(prog)s --explain DBOS001        # Show docs for DBOS001
  %(prog)s --all-docs               # Output all documentation
        """,
    )

    parser.add_argument(
        "paths",
        nargs="*",
        default=["job_scrape_application/"],
        help="Paths to lint (default: job_scrape_application/)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        dest="list_rules",
        help="List all available rules with summaries",
    )
    parser.add_argument(
        "--explain",
        metavar="RULE",
        help="Show detailed documentation for a specific rule (e.g., DBOS001)",
    )
    parser.add_argument(
        "--all-docs",
        action="store_true",
        help="Output all rule documentation as a single AsciiDoc document",
    )
    parser.add_argument(
        "--sync-ruff",
        action="store_true",
        help="Sync ruff.toml external rules from .lint/*.py files",
    )

    args = parser.parse_args()

    # Handle documentation commands
    if args.list_rules:
        print_rule_list()
        return 0

    if args.explain:
        print_rule_docs(args.explain)
        return 0

    if args.all_docs:
        print_all_docs()
        return 0

    if args.sync_ruff:
        sync_ruff_external_rules()
        return 0

    # Run lint checks
    return run_lint(args.paths)


if __name__ == "__main__":
    sys.exit(main())
