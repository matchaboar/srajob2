"""DBOS003: Synchronous functions must not be awaited."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS003"
SUMMARY = "Synchronous functions must not be awaited."


class FunctionInfo(NamedTuple):
    """Information about a @DBOS.step function."""

    file: Path
    line: int
    name: str
    is_async: bool


@dataclass
class Violation:
    """A DBOS003 violation."""

    file: Path
    line: int
    function_name: str
    awaited_function: str

    @property
    def rule(self) -> str:
        return RULE_ID

    def format(self) -> str:
        return (
            f"{self.file}:{self.line}: [{RULE_ID}] "
            f"Function `{self.function_name}` awaits synchronous function `{self.awaited_function}`"
        )


class FunctionCollector(ast.NodeVisitor):
    """Collects all @DBOS.step decorated function definitions."""

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
        if self._has_dbos_step_decorator(node):
            self.functions.append(
                FunctionInfo(
                    file=self.file_path,
                    line=node.lineno,
                    name=node.name,
                    is_async=is_async,
                )
            )

    def _has_dbos_step_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                decorator = decorator.func
            if isinstance(decorator, ast.Attribute):
                if decorator.attr == "step":
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False


class Checker(ast.NodeVisitor):
    """AST visitor that checks for awaiting synchronous @DBOS.step functions."""

    def __init__(
        self,
        file_path: Path,
        source_lines: list[str],
        function_registry: dict[str, FunctionInfo],
    ) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.violations: list[Violation] = []
        self._function_registry = function_registry
        self._current_function: str | None = None
        self._imports: dict[str, str] = {}

    def _has_noqa_comment(self, lineno: int) -> bool:
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1]
        return "noqa: DBOS003" in line or "noqa:DBOS003" in line

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            name = alias.name
            asname = alias.asname if alias.asname else name
            self._imports[asname] = name
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        prev = self._current_function
        self._current_function = node.name
        self.generic_visit(node)
        self._current_function = prev

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        prev = self._current_function
        self._current_function = node.name
        self.generic_visit(node)
        self._current_function = prev

    def visit_Await(self, node: ast.Await) -> None:
        if self._current_function is None:
            self.generic_visit(node)
            return

        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name):
            func_name = node.value.func.id
            original_name = self._imports.get(func_name, func_name)

            if not self._has_noqa_comment(node.lineno):
                if original_name in self._function_registry:
                    func_info = self._function_registry[original_name]
                    if not func_info.is_async:
                        self.violations.append(
                            Violation(
                                file=self.file_path,
                                line=node.lineno,
                                function_name=self._current_function,
                                awaited_function=func_name,
                            )
                        )

        self.generic_visit(node)


def collect_functions(file_path: Path, source: str) -> dict[str, FunctionInfo]:
    """Collect @DBOS.step function info from a file."""
    try:
        tree = ast.parse(source, filename=str(file_path))
    except SyntaxError:
        return {}

    collector = FunctionCollector(file_path)
    collector.visit(tree)
    return {f.name: f for f in collector.functions}


def check_file(
    file_path: Path,
    source: str | None = None,
    function_registry: dict[str, FunctionInfo] | None = None,
) -> Sequence[Violation]:
    """Check a single file for DBOS003 violations."""
    if source is None:
        source = file_path.read_text(encoding="utf-8")

    try:
        tree = ast.parse(source, filename=str(file_path))
    except SyntaxError:
        return []

    # Build registry from this file if not provided
    if function_registry is None:
        function_registry = collect_functions(file_path, source)

    source_lines = source.splitlines()
    checker = Checker(file_path, source_lines, function_registry)
    checker.visit(tree)
    return checker.violations
