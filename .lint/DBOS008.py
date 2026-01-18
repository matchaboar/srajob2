"""DBOS008: Workflows may only call DBOS-decorated functions."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS008"
SUMMARY = "Workflows may only call DBOS-decorated functions."

DECORATORS = frozenset({"step", "transaction", "pure_func", "workflow"})


@dataclass
class Violation:
    """A DBOS008 violation."""

    file: Path
    line: int
    function_name: str
    call_name: str

    @property
    def rule(self) -> str:
        return RULE_ID

    def format(self) -> str:
        return (
            f"{self.file}:{self.line}: [{RULE_ID}] "
            f"Workflow `{self.function_name}` calls `{self.call_name}` "
            "which is not annotated with @DBOS.step/@DBOS.transaction/@DBOS.pure_func/@DBOS.workflow."
        )


class DecoratorCollector(ast.NodeVisitor):
    """Collect local function names and DBOS-decorated names."""

    def __init__(self) -> None:
        self.function_names: set[str] = set()
        self.decorated_names: set[str] = set()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_function(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_function(node)
        self.generic_visit(node)

    def _check_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.function_names.add(node.name)
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                decorator = decorator.func
            if isinstance(decorator, ast.Attribute):
                if decorator.attr in DECORATORS:
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        self.decorated_names.add(node.name)
                        break


class Checker(ast.NodeVisitor):
    """Checks for DBOS008 violations."""

    def __init__(
        self,
        file_path: Path,
        source_lines: list[str],
        local_functions: set[str],
        decorated_functions: set[str],
    ) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.local_functions = local_functions
        self.decorated_functions = decorated_functions
        self.violations: list[Violation] = []
        self._current_function: str | None = None
        self._is_workflow: bool = False

    def _has_noqa_comment(self, lineno: int) -> bool:
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1]
        return "noqa: DBOS008" in line or "noqa:DBOS008" in line

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
        prev_is_workflow = self._is_workflow
        self._current_function = node.name
        self._is_workflow = self._has_dbos_workflow_decorator(node)
        self.generic_visit(node)
        self._current_function = prev_function
        self._is_workflow = prev_is_workflow

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        prev_function = self._current_function
        prev_is_workflow = self._is_workflow
        self._current_function = node.name
        self._is_workflow = self._has_dbos_workflow_decorator(node)
        self.generic_visit(node)
        self._current_function = prev_function
        self._is_workflow = prev_is_workflow

    def visit_Call(self, node: ast.Call) -> None:
        if self._current_function is None or not self._is_workflow:
            self.generic_visit(node)
            return

        if isinstance(node.func, ast.Name):
            call_name = node.func.id
            if (
                call_name in self.local_functions
                and call_name not in self.decorated_functions
                and not self._has_noqa_comment(node.lineno)
            ):
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=self._current_function,
                        call_name=call_name,
                    )
                )

        self.generic_visit(node)


def check_file(file_path: Path, source: str | None = None) -> Sequence[Violation]:
    """Check a single file for DBOS008 violations."""
    if source is None:
        source = file_path.read_text(encoding="utf-8")

    try:
        tree = ast.parse(source, filename=str(file_path))
    except SyntaxError:
        return []

    collector = DecoratorCollector()
    collector.visit(tree)

    source_lines = source.splitlines()
    checker = Checker(
        file_path,
        source_lines,
        collector.function_names,
        collector.decorated_names,
    )
    checker.visit(tree)
    return checker.violations
