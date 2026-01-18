"""DBOS009: @DBOS.pure_func must remain pure (no side effects)."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS009"
SUMMARY = "Pure functions must not perform side effects or call impure libraries."

BLACKLIST_PATH = Path(__file__).parent / "dbos_pure_func_blacklist.py"

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

CONVEX_FUNCTIONS = frozenset({
    "convex_query",
    "convex_mutation",
    "convex_action",
})

MUTATING_METHODS = frozenset({
    "append",
    "extend",
    "insert",
    "pop",
    "remove",
    "clear",
    "update",
    "setdefault",
    "add",
    "discard",
    "sort",
    "reverse",
    "put",
    "set",
    "write",
    "save",
    "commit",
    "rollback",
})

LOGGING_METHODS = frozenset({
    "debug",
    "info",
    "warning",
    "error",
    "critical",
    "exception",
    "log",
})


def load_blacklist() -> set[str]:
    if not BLACKLIST_PATH.exists():
        return set()
    import runpy

    data = runpy.run_path(str(BLACKLIST_PATH))
    raw = data.get("BLACKLIST", set())
    if isinstance(raw, (set, list, tuple)):
        return set(raw)
    return set()


BLACKLIST = load_blacklist()
BLACKLIST_ROOTS = {
    entry.split(".")[0]
    for entry in BLACKLIST
    if isinstance(entry, str) and entry
}
BLACKLIST_BUILTINS = {
    entry.split(".", 1)[1]
    for entry in BLACKLIST
    if isinstance(entry, str) and entry.startswith("builtins.")
}


@dataclass
class Violation:
    """A DBOS009 violation."""

    file: Path
    line: int
    function_name: str
    call_type: str
    call_name: str

    @property
    def rule(self) -> str:
        return RULE_ID

    def format(self) -> str:
        if self.call_type == "state_mutation":
            return (
                f"{self.file}:{self.line}: [{RULE_ID}] "
                f"Function `{self.function_name}` has @DBOS.pure_func but mutates state via "
                f"{self.call_name}."
            )
        return (
            f"{self.file}:{self.line}: [{RULE_ID}] "
            f"Function `{self.function_name}` has @DBOS.pure_func but calls impure function "
            f"`{self.call_name}`."
        )


class Checker(ast.NodeVisitor):
    """Checks for DBOS009 violations."""

    def __init__(self, file_path: Path, source_lines: list[str]) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.violations: list[Violation] = []
        self._current_function: str | None = None
        self._has_pure_func: bool = False
        self._imports: dict[str, str] = {}

    def _has_noqa_comment(self, lineno: int) -> bool:
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1]
        return "noqa: DBOS009" in line or "noqa:DBOS009" in line

    def _has_dbos_pure_func_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                decorator = decorator.func
            if isinstance(decorator, ast.Attribute):
                if decorator.attr == "pure_func":
                    if isinstance(decorator.value, ast.Name) and decorator.value.id == "DBOS":
                        return True
        return False

    def _get_dotted_name(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = self._get_dotted_name(node.value)
            if base:
                return f"{base}.{node.attr}"
        return None

    def _resolve_dotted_name(self, dotted_name: str) -> str:
        parts = dotted_name.split(".")
        if not parts:
            return dotted_name
        root = parts[0]
        resolved_root = self._imports.get(root, root)
        if len(parts) == 1:
            return resolved_root
        return ".".join([resolved_root, *parts[1:]])

    def _resolve_call_name(self, node: ast.Call) -> str | None:
        dotted = self._get_dotted_name(node.func)
        if not dotted:
            return None
        return self._resolve_dotted_name(dotted)

    def _is_logger_call(self, resolved_name: str | None, node: ast.Call) -> bool:
        if not resolved_name:
            return False
        if resolved_name.startswith("logging.") and resolved_name.split(".")[-1] in LOGGING_METHODS:
            return True
        dotted = self._get_dotted_name(node.func)
        if not dotted:
            return False
        parts = dotted.split(".")
        if len(parts) < 2:
            return False
        root = parts[0].lower()
        method = parts[-1]
        if method in LOGGING_METHODS and ("logger" in root):
            return True
        return False

    def _is_mutation_target(self, target: ast.AST) -> bool:
        if isinstance(target, (ast.Attribute, ast.Subscript)):
            return True
        if isinstance(target, ast.Starred):
            return self._is_mutation_target(target.value)
        return False

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            name = alias.name
            asname = alias.asname if alias.asname else name
            module = node.module or ""
            if module:
                self._imports[asname] = f"{module}.{name}"
            else:
                self._imports[asname] = name
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            name = alias.name
            asname = alias.asname if alias.asname else name
            self._imports[asname] = name
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        prev_function = self._current_function
        prev_pure = self._has_pure_func
        self._current_function = node.name
        self._has_pure_func = self._has_dbos_pure_func_decorator(node)
        self.generic_visit(node)
        self._current_function = prev_function
        self._has_pure_func = prev_pure

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        prev_function = self._current_function
        prev_pure = self._has_pure_func
        self._current_function = node.name
        self._has_pure_func = self._has_dbos_pure_func_decorator(node)
        self.generic_visit(node)
        self._current_function = prev_function
        self._has_pure_func = prev_pure

    def visit_Global(self, node: ast.Global) -> None:
        if self._has_pure_func and not self._has_noqa_comment(node.lineno):
            self.violations.append(
                Violation(
                    file=self.file_path,
                    line=node.lineno,
                    function_name=self._current_function or "<module>",
                    call_type="state_mutation",
                    call_name="global declaration",
                )
            )
        self.generic_visit(node)

    def visit_Nonlocal(self, node: ast.Nonlocal) -> None:
        if self._has_pure_func and not self._has_noqa_comment(node.lineno):
            self.violations.append(
                Violation(
                    file=self.file_path,
                    line=node.lineno,
                    function_name=self._current_function or "<module>",
                    call_type="state_mutation",
                    call_name="nonlocal declaration",
                )
            )
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if self._has_pure_func and not self._has_noqa_comment(node.lineno):
            if any(self._is_mutation_target(target) for target in node.targets):
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=self._current_function or "<module>",
                        call_type="state_mutation",
                        call_name="attribute/subscript assignment",
                    )
                )
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if self._has_pure_func and not self._has_noqa_comment(node.lineno):
            if node.target and self._is_mutation_target(node.target):
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=self._current_function or "<module>",
                        call_type="state_mutation",
                        call_name="attribute/subscript annotation assignment",
                    )
                )
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        if self._has_pure_func and not self._has_noqa_comment(node.lineno):
            if self._is_mutation_target(node.target):
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=self._current_function or "<module>",
                        call_type="state_mutation",
                        call_name="attribute/subscript augmented assignment",
                    )
                )
        self.generic_visit(node)

    def visit_Delete(self, node: ast.Delete) -> None:
        if self._has_pure_func and not self._has_noqa_comment(node.lineno):
            if any(self._is_mutation_target(target) for target in node.targets):
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=self._current_function or "<module>",
                        call_type="state_mutation",
                        call_name="attribute/subscript delete",
                    )
                )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if not self._has_pure_func:
            self.generic_visit(node)
            return

        if self._has_noqa_comment(node.lineno):
            self.generic_visit(node)
            return

        if isinstance(node.func, ast.Attribute) and node.func.attr in MUTATING_METHODS:
            self.violations.append(
                Violation(
                    file=self.file_path,
                    line=node.lineno,
                    function_name=self._current_function or "<module>",
                    call_type="state_mutation",
                    call_name=f"method `{node.func.attr}`",
                )
            )

        resolved_name = self._resolve_call_name(node)
        dotted_name = self._get_dotted_name(node.func)

        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            if func_name in BLACKLIST_BUILTINS:
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=self._current_function or "<module>",
                        call_type="impure_call",
                        call_name=f"builtins.{func_name}",
                    )
                )
            original_name = self._imports.get(func_name, func_name)
            if original_name in CONVEX_FUNCTIONS:
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=self._current_function or "<module>",
                        call_type="impure_call",
                        call_name=original_name,
                    )
                )

        if resolved_name:
            root = resolved_name.split(".")[0]
            if resolved_name in BLACKLIST or root in BLACKLIST_ROOTS:
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=self._current_function or "<module>",
                        call_type="impure_call",
                        call_name=resolved_name,
                    )
                )

        if self._is_logger_call(resolved_name, node):
            self.violations.append(
                Violation(
                    file=self.file_path,
                    line=node.lineno,
                    function_name=self._current_function or "<module>",
                    call_type="impure_call",
                    call_name=dotted_name or "logger call",
                )
            )

        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            if isinstance(node.func.value, ast.Name):
                var_name = node.func.value.id
                if var_name == "httpx" and method_name in HTTPX_METHODS:
                    self.violations.append(
                        Violation(
                            file=self.file_path,
                            line=node.lineno,
                            function_name=self._current_function or "<module>",
                            call_type="impure_call",
                            call_name=f"httpx.{method_name}",
                        )
                    )
                if var_name in ("client", "http_client", "async_client", "httpx_client"):
                    if method_name in HTTPX_METHODS:
                        self.violations.append(
                            Violation(
                                file=self.file_path,
                                line=node.lineno,
                                function_name=self._current_function or "<module>",
                                call_type="impure_call",
                                call_name=f"{var_name}.{method_name}",
                            )
                        )

        self.generic_visit(node)


def check_file(file_path: Path, source: str | None = None) -> Sequence[Violation]:
    """Check a single file for DBOS009 violations."""
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
