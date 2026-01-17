"""DBOS006: Async workflows must use async DBOS context methods."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS006"
SUMMARY = "Async workflows must use async DBOS context methods."

# Sync -> async method mappings
SYNC_TO_ASYNC_METHODS: dict[str, str] = {
    "sleep": "sleep_async",
    "recv": "recv_async",
    "send": "send_async",
    "set_event": "set_event_async",
    "get_event": "get_event_async",
}


@dataclass
class Violation:
    """A DBOS006 violation."""

    file: Path
    line: int
    function_name: str
    sync_method: str

    @property
    def rule(self) -> str:
        return RULE_ID

    @property
    def async_method(self) -> str:
        return SYNC_TO_ASYNC_METHODS.get(self.sync_method, f"{self.sync_method}_async")

    def format(self) -> str:
        return (
            f"{self.file}:{self.line}: [{RULE_ID}] "
            f"Async workflow `{self.function_name}` uses `DBOS.{self.sync_method}`. "
            f"Use `DBOS.{self.async_method}` instead."
        )


class Checker(ast.NodeVisitor):
    """Checks for sync DBOS context methods in async workflows."""

    def __init__(self, file_path: Path, source_lines: list[str]) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.violations: list[Violation] = []
        self._current_function: str | None = None
        self._is_async_workflow: bool = False

    def _has_noqa_comment(self, lineno: int) -> bool:
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1]
        return "noqa: DBOS006" in line or "noqa:DBOS006" in line

    def _has_dbos_workflow_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                decorator = decorator.func
            if isinstance(decorator, ast.Attribute):
                if decorator.attr == "workflow":
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        prev_function = self._current_function
        prev_is_async = self._is_async_workflow
        self._current_function = node.name
        self._is_async_workflow = False
        self.generic_visit(node)
        self._current_function = prev_function
        self._is_async_workflow = prev_is_async

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        prev_function = self._current_function
        prev_is_async = self._is_async_workflow
        self._current_function = node.name
        self._is_async_workflow = self._has_dbos_workflow_decorator(node)
        self.generic_visit(node)
        self._current_function = prev_function
        self._is_async_workflow = prev_is_async

    def visit_Call(self, node: ast.Call) -> None:
        if self._current_function is None or not self._is_async_workflow:
            self.generic_visit(node)
            return

        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            if isinstance(node.func.value, ast.Name) and node.func.value.id == "DBOS":
                if method_name in SYNC_TO_ASYNC_METHODS:
                    if not self._has_noqa_comment(node.lineno):
                        self.violations.append(
                            Violation(
                                file=self.file_path,
                                line=node.lineno,
                                function_name=self._current_function,
                                sync_method=method_name,
                            )
                        )

        self.generic_visit(node)


def check_file(file_path: Path, source: str | None = None) -> Sequence[Violation]:
    """Check a single file for DBOS006 violations."""
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
