"""DBOS010: queue.enqueue() can only be called from a workflow context."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS010"
SUMMARY = "queue.enqueue() can only be called from a workflow context."


@dataclass
class Violation:
    """A DBOS010 violation."""

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
            f"Function `{self.function_name}` calls `{self.call_name}` "
            "but is not a @DBOS.workflow. queue.enqueue() can only be called from workflows."
        )


class Checker(ast.NodeVisitor):
    """Checks for DBOS010 violations."""

    def __init__(
        self,
        file_path: Path,
        source_lines: list[str],
    ) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.violations: list[Violation] = []
        self._current_function: str | None = None
        self._current_function_line: int = 0
        self._is_workflow: bool = False
        self._is_step: bool = False

    def _has_noqa_comment(self, lineno: int) -> bool:
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1].lower()
        return "noqa: dbos010" in line or "noqa:dbos010" in line

    def _has_dbos_decorator(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef, decorator_name: str
    ) -> bool:
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                decorator = decorator.func
            if isinstance(decorator, ast.Attribute):
                if decorator.attr == decorator_name:
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_function(node)

    def _check_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        prev_function = self._current_function
        prev_function_line = self._current_function_line
        prev_is_workflow = self._is_workflow
        prev_is_step = self._is_step

        self._current_function = node.name
        self._current_function_line = node.lineno
        self._is_workflow = self._has_dbos_decorator(node, "workflow")
        self._is_step = self._has_dbos_decorator(node, "step")

        self.generic_visit(node)

        self._current_function = prev_function
        self._current_function_line = prev_function_line
        self._is_workflow = prev_is_workflow
        self._is_step = prev_is_step

    def visit_Call(self, node: ast.Call) -> None:
        if self._current_function is None:
            self.generic_visit(node)
            return

        # Check for *.enqueue() calls
        if isinstance(node.func, ast.Attribute) and node.func.attr == "enqueue":
            # This is a .enqueue() call - check context
            # It's only allowed in @DBOS.workflow, not in @DBOS.step or undecorated functions
            if not self._is_workflow and not self._has_noqa_comment(node.lineno):
                # Get the name of what we're calling enqueue on
                call_target = self._get_call_target(node.func.value)
                call_name = f"{call_target}.enqueue" if call_target else "enqueue"

                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=self._current_function,
                        call_name=call_name,
                    )
                )

        self.generic_visit(node)

    def _get_call_target(self, node: ast.AST) -> str | None:
        """Get string representation of the call target."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = self._get_call_target(node.value)
            if base:
                return f"{base}.{node.attr}"
            return node.attr
        return None


def check_file(file_path: Path, source: str | None = None) -> Sequence[Violation]:
    """Check a single file for DBOS010 violations."""
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
