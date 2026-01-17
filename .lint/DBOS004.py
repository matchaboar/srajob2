"""DBOS004: Functions calling convex must not be async."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS004"
SUMMARY = "Functions calling convex must not be async."

CONVEX_FUNCTIONS = frozenset({
    "convex_query",
    "convex_mutation",
    "convex_action",
})


class ExtendedFunctionInfo(NamedTuple):
    """Extended info for call graph analysis."""

    file: Path
    line: int
    name: str
    is_async: bool
    calls_convex: bool
    called_functions: frozenset[str]


@dataclass
class Violation:
    """A DBOS004 violation."""

    file: Path
    line: int
    function_name: str
    violation_type: str  # "await_convex" or "async_convex_caller"
    call_name: str

    @property
    def rule(self) -> str:
        return RULE_ID

    def format(self) -> str:
        if self.violation_type == "await_convex":
            return (
                f"{self.file}:{self.line}: [{RULE_ID}] "
                f"Function `{self.function_name}` awaits convex function `{self.call_name}` "
                f"(convex functions are synchronous)"
            )
        else:
            return (
                f"{self.file}:{self.line}: [{RULE_ID}] "
                f"Function `{self.function_name}` is async but calls convex "
                f"(directly or transitively). Functions calling convex must not be async."
            )


class FunctionCollector(ast.NodeVisitor):
    """Collects function info including convex calls and call graph."""

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.functions: list[ExtendedFunctionInfo] = []
        self._imports: dict[str, str] = {}
        self._current_function: str | None = None
        self._current_function_line: int = 0
        self._current_is_async: bool = False
        self._current_calls_convex: bool = False
        self._current_called_functions: set[str] = set()

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
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
        prev_function = self._current_function
        prev_line = self._current_function_line
        prev_is_async = self._current_is_async
        prev_calls_convex = self._current_calls_convex
        prev_called_functions = self._current_called_functions

        self._current_function = node.name
        self._current_function_line = node.lineno
        self._current_is_async = is_async
        self._current_calls_convex = False
        self._current_called_functions = set()

        self.generic_visit(node)

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

        self._current_function = prev_function
        self._current_function_line = prev_line
        self._current_is_async = prev_is_async
        self._current_calls_convex = prev_calls_convex
        self._current_called_functions = prev_called_functions

    def visit_Call(self, node: ast.Call) -> None:
        if self._current_function is None:
            self.generic_visit(node)
            return

        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            original_name = self._imports.get(func_name, func_name)
            if original_name in CONVEX_FUNCTIONS:
                self._current_calls_convex = True
            self._current_called_functions.add(original_name)

        self.generic_visit(node)


class Checker(ast.NodeVisitor):
    """Checks for DBOS004 violations."""

    def __init__(
        self,
        file_path: Path,
        source_lines: list[str],
        transitive_convex_callers: set[str],
    ) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.violations: list[Violation] = []
        self._transitive_convex_callers = transitive_convex_callers
        self._current_function: str | None = None
        self._imports: dict[str, str] = {}

    def _has_noqa_comment(self, lineno: int) -> bool:
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1]
        return "noqa: DBOS004" in line or "noqa:DBOS004" in line

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            self._imports[alias.asname or alias.name] = alias.name
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        prev = self._current_function
        self._current_function = node.name
        self.generic_visit(node)
        self._current_function = prev

    def _has_dbos_workflow_decorator(self, node: ast.AsyncFunctionDef) -> bool:
        """Check if function has @DBOS.workflow decorator."""
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                decorator = decorator.func
            if isinstance(decorator, ast.Attribute):
                if decorator.attr == "workflow":
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        # Check if async function calls convex transitively
        # Exempt @DBOS.workflow() - workflows coordinate async steps that call convex
        if not self._has_noqa_comment(node.lineno) and not self._has_dbos_workflow_decorator(node):
            if node.name in self._transitive_convex_callers:
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=node.name,
                        violation_type="async_convex_caller",
                        call_name="convex",
                    )
                )
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
                if original_name in CONVEX_FUNCTIONS:
                    self.violations.append(
                        Violation(
                            file=self.file_path,
                            line=node.lineno,
                            function_name=self._current_function,
                            violation_type="await_convex",
                            call_name=func_name,
                        )
                    )

        self.generic_visit(node)


def compute_transitive_convex_callers(
    registry: dict[str, ExtendedFunctionInfo],
) -> set[str]:
    """Compute all functions that call convex directly or transitively."""
    convex_callers: set[str] = {
        name for name, info in registry.items() if info.calls_convex
    }

    changed = True
    while changed:
        changed = False
        for name, info in registry.items():
            if name in convex_callers:
                continue
            if info.called_functions & convex_callers:
                convex_callers.add(name)
                changed = True

    return convex_callers


def collect_functions(file_path: Path, source: str) -> dict[str, ExtendedFunctionInfo]:
    """Collect extended function info from a file."""
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
    extended_registry: dict[str, ExtendedFunctionInfo] | None = None,
    transitive_convex_callers: set[str] | None = None,
) -> Sequence[Violation]:
    """Check a single file for DBOS004 violations."""
    if source is None:
        source = file_path.read_text(encoding="utf-8")

    try:
        tree = ast.parse(source, filename=str(file_path))
    except SyntaxError:
        return []

    # Build registries from this file if not provided
    if extended_registry is None:
        extended_registry = collect_functions(file_path, source)
    if transitive_convex_callers is None:
        transitive_convex_callers = compute_transitive_convex_callers(extended_registry)

    source_lines = source.splitlines()
    checker = Checker(file_path, source_lines, transitive_convex_callers)
    checker.visit(tree)
    return checker.violations
