"""DBOS002: Functions with @DBOS.step must have correct sync/async signature."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS002"
SUMMARY = "Functions with @DBOS.step must have correct sync/async signature."

# httpx methods that indicate external calls
HTTPX_METHODS = frozenset({
    "get", "post", "put", "patch", "delete", "head", "options", "request", "send", "stream",
})


@dataclass
class Violation:
    """A DBOS002 violation."""

    file: Path
    line: int
    function_name: str
    call_type: str
    call_name: str
    is_async_call: bool
    is_async_function: bool

    @property
    def rule(self) -> str:
        return RULE_ID

    def format(self) -> str:
        if self.is_async_call and not self.is_async_function:
            hint = "Function should be `async def` to call async methods"
        else:
            hint = "Function should be `def` (not async) when only calling sync methods"
        return (
            f"{self.file}:{self.line}: [{RULE_ID}] "
            f"Function `{self.function_name}` has @DBOS.step but calls {self.call_type} function "
            f"`{self.call_name}` with mismatched sync/async signature. {hint}"
        )


class Checker(ast.NodeVisitor):
    """AST visitor that checks sync/async consistency for @DBOS.step functions."""

    def __init__(self, file_path: Path, source_lines: list[str]) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.violations: list[Violation] = []
        self._current_function: str | None = None
        self._current_function_line: int = 0
        self._has_dbos_step: bool = False
        self._has_noqa: bool = False
        self._is_async_function: bool = False
        # (call_type, call_name, is_async_call)
        self._external_calls: list[tuple[str, str, bool]] = []

    def _has_noqa_comment(self, lineno: int) -> bool:
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1]
        return "noqa: DBOS002" in line or "noqa:DBOS002" in line

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_function(node, is_async=False)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_function(node, is_async=True)

    def _check_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef, *, is_async: bool
    ) -> None:
        # Save parent context
        prev_function = self._current_function
        prev_line = self._current_function_line
        prev_has_step = self._has_dbos_step
        prev_has_noqa = self._has_noqa
        prev_is_async = self._is_async_function
        prev_calls = self._external_calls

        # Set up new context
        self._current_function = node.name
        self._current_function_line = node.lineno
        self._has_dbos_step = self._has_dbos_step_decorator(node)
        self._has_noqa = self._has_noqa_comment(node.lineno)
        self._is_async_function = is_async
        self._external_calls = []

        # Visit function body
        self.generic_visit(node)

        # Check sync/async consistency if function has @DBOS.step
        if self._external_calls and self._has_dbos_step and not self._has_noqa:
            has_async_calls = any(is_async_call for _, _, is_async_call in self._external_calls)
            has_sync_calls = any(not is_async_call for _, _, is_async_call in self._external_calls)

            # Async calls but sync function
            if has_async_calls and not self._is_async_function:
                for call_type, call_name, is_async_call in self._external_calls:
                    if is_async_call:
                        self.violations.append(
                            Violation(
                                file=self.file_path,
                                line=self._current_function_line,
                                function_name=self._current_function,
                                call_type=call_type,
                                call_name=call_name,
                                is_async_call=True,
                                is_async_function=False,
                            )
                        )
                        break

            # Only sync calls but async function
            if has_sync_calls and not has_async_calls and self._is_async_function:
                for call_type, call_name, is_async_call in self._external_calls:
                    if not is_async_call:
                        self.violations.append(
                            Violation(
                                file=self.file_path,
                                line=self._current_function_line,
                                function_name=self._current_function,
                                call_type=call_type,
                                call_name=call_name,
                                is_async_call=False,
                                is_async_function=True,
                            )
                        )
                        break

        # Restore parent context
        self._current_function = prev_function
        self._current_function_line = prev_line
        self._has_dbos_step = prev_has_step
        self._has_noqa = prev_has_noqa
        self._is_async_function = prev_is_async
        self._external_calls = prev_calls

    def _has_dbos_step_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                decorator = decorator.func
            if isinstance(decorator, ast.Attribute):
                if decorator.attr == "step":
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False

    def visit_Call(self, node: ast.Call) -> None:
        if self._current_function is None:
            self.generic_visit(node)
            return

        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            if isinstance(node.func.value, ast.Name):
                var_name = node.func.value.id
                # httpx.get() etc. are sync
                if var_name == "httpx" and method_name in HTTPX_METHODS:
                    self._external_calls.append(("httpx", f"httpx.{method_name}", False))
                # client.get() etc. assumed async (AsyncClient)
                elif var_name in ("client", "http_client", "async_client", "httpx_client"):
                    if method_name in HTTPX_METHODS:
                        self._external_calls.append(("httpx", f"{var_name}.{method_name}", True))

        self.generic_visit(node)


def check_file(file_path: Path, source: str | None = None) -> Sequence[Violation]:
    """Check a single file for DBOS002 violations."""
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
