"""DBOS007: DBOS workflows and steps must not use raw sleep or asyncio primitives."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS007"
SUMMARY = "DBOS workflows and steps must not use raw sleep or asyncio primitives."

# Forbidden calls: (module, function) -> recommended alternative
FORBIDDEN_CALLS: dict[tuple[str, str], str] = {
    ("time", "sleep"): "DBOS.sleep (sync) or DBOS.sleep_async (async)",
    ("asyncio", "sleep"): "DBOS.sleep_async",
}


@dataclass
class Violation:
    """A DBOS007 violation."""

    file: Path
    line: int
    function_name: str
    forbidden_call: str
    recommendation: str

    @property
    def rule(self) -> str:
        return RULE_ID

    def format(self) -> str:
        return (
            f"{self.file}:{self.line}: [{RULE_ID}] "
            f"DBOS function `{self.function_name}` uses `{self.forbidden_call}`. "
            f"Use {self.recommendation} instead for durable sleep."
        )


class Checker(ast.NodeVisitor):
    """Checks for forbidden sleep/asyncio primitives in DBOS functions."""

    def __init__(self, file_path: Path, source_lines: list[str]) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.violations: list[Violation] = []
        self._current_function: str | None = None
        self._is_dbos_function: bool = False

    def _has_noqa_comment(self, lineno: int) -> bool:
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1]
        return "noqa: DBOS007" in line or "noqa:DBOS007" in line

    def _has_dbos_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """Check for @DBOS.workflow or @DBOS.step decorator."""
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                decorator = decorator.func
            if isinstance(decorator, ast.Attribute):
                if decorator.attr in ("workflow", "step"):
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_function(node)

    def _check_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        prev_function = self._current_function
        prev_is_dbos = self._is_dbos_function
        self._current_function = node.name
        self._is_dbos_function = self._has_dbos_decorator(node)
        self.generic_visit(node)
        self._current_function = prev_function
        self._is_dbos_function = prev_is_dbos

    def visit_Call(self, node: ast.Call) -> None:
        if self._current_function is None or not self._is_dbos_function:
            self.generic_visit(node)
            return

        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            if isinstance(node.func.value, ast.Name):
                module_name = node.func.value.id
                key = (module_name, method_name)
                if key in FORBIDDEN_CALLS:
                    if not self._has_noqa_comment(node.lineno):
                        self.violations.append(
                            Violation(
                                file=self.file_path,
                                line=node.lineno,
                                function_name=self._current_function,
                                forbidden_call=f"{module_name}.{method_name}",
                                recommendation=FORBIDDEN_CALLS[key],
                            )
                        )

        self.generic_visit(node)


def check_file(file_path: Path, source: str | None = None) -> Sequence[Violation]:
    """Check a single file for DBOS007 violations."""
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
