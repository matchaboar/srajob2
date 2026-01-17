"""DBOS001: Functions calling convex or httpx must have @DBOS.step decorator."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS001"
SUMMARY = "Functions calling convex or httpx must have @DBOS.step decorator."

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

# Convex functions that require @DBOS.step
CONVEX_FUNCTIONS = frozenset({
    "convex_query",
    "convex_mutation",
    "convex_action",
})


@dataclass
class Violation:
    """A DBOS001 violation."""

    file: Path
    line: int
    function_name: str
    call_type: str  # "convex" or "httpx"
    call_name: str

    @property
    def rule(self) -> str:
        return RULE_ID

    def format(self) -> str:
        return (
            f"{self.file}:{self.line}: [{RULE_ID}] "
            f"Function `{self.function_name}` calls {self.call_type} function `{self.call_name}` "
            f"but is missing @DBOS.step decorator"
        )


class Checker(ast.NodeVisitor):
    """AST visitor that checks for @DBOS.step decorator on functions with external calls."""

    def __init__(self, file_path: Path, source_lines: list[str]) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.violations: list[Violation] = []
        self._current_function: str | None = None
        self._current_function_line: int = 0
        self._has_dbos_step: bool = False
        self._has_noqa: bool = False
        # (call_type, call_name)
        self._external_calls: list[tuple[str, str]] = []
        # Import tracking: alias -> original_name
        self._imports: dict[str, str] = {}

    def _has_noqa_comment(self, lineno: int) -> bool:
        """Check if a line has a noqa comment for DBOS001."""
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1]
        return "noqa: DBOS001" in line or "noqa:DBOS001" in line

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Track imports from modules."""
        for alias in node.names:
            name = alias.name
            asname = alias.asname if alias.asname else name
            self._imports[asname] = name
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_function(node)

    def _check_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Check a function definition for @DBOS.step decorator requirement."""
        # Save parent context
        prev_function = self._current_function
        prev_line = self._current_function_line
        prev_has_step = self._has_dbos_step
        prev_has_noqa = self._has_noqa
        prev_calls = self._external_calls

        # Set up new context
        self._current_function = node.name
        self._current_function_line = node.lineno
        self._has_dbos_step = self._has_dbos_step_decorator(node)
        self._has_noqa = self._has_noqa_comment(node.lineno)
        self._external_calls = []

        # Visit function body
        self.generic_visit(node)

        # Report violations if external calls found without @DBOS.step
        if self._external_calls and not self._has_dbos_step and not self._has_noqa:
            for call_type, call_name in self._external_calls:
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=self._current_function_line,
                        function_name=self._current_function,
                        call_type=call_type,
                        call_name=call_name,
                    )
                )

        # Restore parent context
        self._current_function = prev_function
        self._current_function_line = prev_line
        self._has_dbos_step = prev_has_step
        self._has_noqa = prev_has_noqa
        self._external_calls = prev_calls

    def _has_dbos_step_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """Check if function has @DBOS.step decorator."""
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                decorator = decorator.func
            if isinstance(decorator, ast.Attribute):
                if decorator.attr == "step":
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False

    def visit_Call(self, node: ast.Call) -> None:
        """Check if a call is to convex or httpx functions."""
        if self._current_function is None:
            self.generic_visit(node)
            return

        # Check for direct function calls like convex_query(...)
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            original_name = self._imports.get(func_name, func_name)
            if original_name in CONVEX_FUNCTIONS:
                self._external_calls.append(("convex", func_name))

        # Check for httpx.method() or client.method() patterns
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            if isinstance(node.func.value, ast.Name):
                var_name = node.func.value.id
                # httpx.get(), httpx.post() etc.
                if var_name == "httpx" and method_name in HTTPX_METHODS:
                    self._external_calls.append(("httpx", f"httpx.{method_name}"))
                # client.get(), http_client.post() etc.
                elif var_name in ("client", "http_client", "async_client", "httpx_client"):
                    if method_name in HTTPX_METHODS:
                        self._external_calls.append(("httpx", f"{var_name}.{method_name}"))

        self.generic_visit(node)


def check_file(file_path: Path, source: str | None = None) -> Sequence[Violation]:
    """Check a single file for DBOS001 violations."""
    if source is None:
        source = file_path.read_text(encoding="utf-8")

    try:
        tree = ast.parse(source, filename=str(file_path))
    except SyntaxError:
        return []

    source_lines = source.splitlines()
    checker = Checker(file_path, source_lines)
    checker.visit(tree)
    return checker.violations
