"""DBOS011: @DBOS.step function inputs and outputs must be serializable."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS011"
SUMMARY = "@DBOS.step function inputs and outputs must be serializable."

# Types that are always serializable (JSON-compatible)
SERIALIZABLE_BUILTINS = frozenset({
    "str",
    "int",
    "float",
    "bool",
    "None",
    "NoneType",
})

# Container types that are serializable if their contents are serializable
SERIALIZABLE_CONTAINERS = frozenset({
    "list",
    "List",
    "dict",
    "Dict",
    "tuple",
    "Tuple",
    "Sequence",
    "Mapping",
    "MutableMapping",
    "Iterable",
})

# Typing constructs that need recursive checking
TYPING_WRAPPERS = frozenset({
    "Optional",
    "Union",
    "Annotated",
    "Final",
})

# Types that indicate serializability through structure
SERIALIZABLE_SPECIAL = frozenset({
    "Any",  # Assumed serializable (duck typing)
    "TypedDict",
    "Literal",
    "LiteralString",
})

# Types that are NOT serializable
NON_SERIALIZABLE_TYPES = frozenset({
    "Callable",
    "Coroutine",
    "AsyncGenerator",
    "Generator",
    "Iterator",
    "AsyncIterator",
    "Type",
    "type",
    "set",
    "Set",
    "frozenset",
    "FrozenSet",
    "bytes",
    "bytearray",
    "memoryview",
    "object",
    "IO",
    "TextIO",
    "BinaryIO",
    "Pattern",
    "Match",
    "Path",
    "PurePath",
    "Connection",
    "Cursor",
    "Socket",
    "Lock",
    "RLock",
    "Semaphore",
    "Event",
    "Condition",
    "Thread",
    "Process",
})

# Common custom types that are known to be serializable (dataclass-like)
KNOWN_SERIALIZABLE_CUSTOM = frozenset({
    "datetime",
    "date",
    "time",
    "timedelta",
    "Decimal",
    "UUID",
    "Enum",
    "IntEnum",
    "StrEnum",
})


@dataclass
class Violation:
    """A DBOS011 violation."""

    file: Path
    line: int
    function_name: str
    param_name: str  # Parameter name or "return" for return type
    type_annotation: str
    reason: str

    @property
    def rule(self) -> str:
        return RULE_ID

    def format(self) -> str:
        if self.param_name == "return":
            return (
                f"{self.file}:{self.line}: [{RULE_ID}] "
                f"@DBOS.step function `{self.function_name}` has non-serializable return type "
                f"`{self.type_annotation}`: {self.reason}"
            )
        return (
            f"{self.file}:{self.line}: [{RULE_ID}] "
            f"@DBOS.step function `{self.function_name}` has non-serializable parameter "
            f"`{self.param_name}: {self.type_annotation}`: {self.reason}"
        )


class TypeChecker:
    """Checks if type annotations are serializable."""

    def __init__(self, class_defs: set[str], dataclass_names: set[str]) -> None:
        self.class_defs = class_defs
        self.dataclass_names = dataclass_names

    def check_type(self, node: ast.AST) -> tuple[bool, str]:
        """Check if a type annotation is serializable.

        Returns (is_serializable, reason) where reason explains why it's not serializable.
        """
        if node is None:
            return True, ""

        # Handle Constant (None)
        if isinstance(node, ast.Constant):
            if node.value is None:
                return True, ""
            return True, ""

        # Handle simple names like 'str', 'int', etc.
        if isinstance(node, ast.Name):
            name = node.id
            if name in SERIALIZABLE_BUILTINS:
                return True, ""
            if name in SERIALIZABLE_CONTAINERS:
                return True, ""
            if name in SERIALIZABLE_SPECIAL:
                return True, ""
            if name in TYPING_WRAPPERS:
                # Optional, Union without args - assume OK
                return True, ""
            if name in NON_SERIALIZABLE_TYPES:
                return False, f"`{name}` is not JSON-serializable"
            if name in KNOWN_SERIALIZABLE_CUSTOM:
                return True, ""
            # Check if it's a dataclass defined in this file
            if name in self.dataclass_names:
                return True, ""
            # Check if it's a class defined in this file (might be a dataclass)
            if name in self.class_defs:
                # Could be TypedDict or dataclass - allow it
                return True, ""
            # Unknown type - allow it with a warning
            return True, ""

        # Handle Attribute access like typing.List, datetime.datetime
        if isinstance(node, ast.Attribute):
            full_name = self._get_dotted_name(node)
            if full_name:
                # Check the final attribute name
                attr_name = node.attr
                if attr_name in SERIALIZABLE_BUILTINS:
                    return True, ""
                if attr_name in SERIALIZABLE_CONTAINERS:
                    return True, ""
                if attr_name in SERIALIZABLE_SPECIAL:
                    return True, ""
                if attr_name in NON_SERIALIZABLE_TYPES:
                    return False, f"`{full_name}` is not JSON-serializable"
                if attr_name in KNOWN_SERIALIZABLE_CUSTOM:
                    return True, ""
            return True, ""

        # Handle subscripted generics like list[str], dict[str, int], Optional[str]
        if isinstance(node, ast.Subscript):
            base_type = node.value
            base_name = self._get_type_name(base_type)

            # Check if base is non-serializable
            if base_name in NON_SERIALIZABLE_TYPES:
                return False, f"`{base_name}` is not JSON-serializable"

            # For containers and wrappers, check the contents
            if base_name in SERIALIZABLE_CONTAINERS or base_name in TYPING_WRAPPERS:
                # Check type arguments
                slice_node = node.slice
                if isinstance(slice_node, ast.Tuple):
                    # Multiple type args like dict[str, int]
                    for elt in slice_node.elts:
                        is_ok, reason = self.check_type(elt)
                        if not is_ok:
                            return False, reason
                else:
                    # Single type arg like list[str]
                    is_ok, reason = self.check_type(slice_node)
                    if not is_ok:
                        return False, reason
                return True, ""

            # Literal types
            if base_name == "Literal":
                return True, ""

            # Annotated types - check first arg
            if base_name == "Annotated":
                slice_node = node.slice
                if isinstance(slice_node, ast.Tuple) and slice_node.elts:
                    return self.check_type(slice_node.elts[0])
                return True, ""

            return True, ""

        # Handle BinOp for Union types (X | Y syntax)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            left_ok, left_reason = self.check_type(node.left)
            if not left_ok:
                return False, left_reason
            right_ok, right_reason = self.check_type(node.right)
            if not right_ok:
                return False, right_reason
            return True, ""

        # Handle string annotations (forward references)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            # Parse the string as a type annotation
            try:
                parsed = ast.parse(node.value, mode="eval")
                return self.check_type(parsed.body)
            except SyntaxError:
                # Can't parse - allow it
                return True, ""

        # Default: allow unknown constructs
        return True, ""

    def _get_type_name(self, node: ast.AST) -> str | None:
        """Extract the type name from a node."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return None

    def _get_dotted_name(self, node: ast.AST) -> str | None:
        """Extract dotted name like 'typing.Optional'."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = self._get_dotted_name(node.value)
            if base:
                return f"{base}.{node.attr}"
            return node.attr
        return None


class Checker(ast.NodeVisitor):
    """Checks for non-serializable types in @DBOS.step functions."""

    def __init__(self, file_path: Path, source_lines: list[str]) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.violations: list[Violation] = []
        self.class_defs: set[str] = set()
        self.dataclass_names: set[str] = set()

    def _has_noqa_comment(self, lineno: int) -> bool:
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1].lower()
        return "noqa: dbos011" in line or "noqa:dbos011" in line

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

    def _is_dataclass(self, node: ast.ClassDef) -> bool:
        """Check if a class has @dataclass decorator."""
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                decorator = decorator.func
            if isinstance(decorator, ast.Name) and decorator.id == "dataclass":
                return True
            if isinstance(decorator, ast.Attribute) and decorator.attr == "dataclass":
                return True
        return False

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Collect class definitions."""
        self.class_defs.add(node.name)
        if self._is_dataclass(node):
            self.dataclass_names.add(node.name)
        # Check for TypedDict inheritance
        for base in node.bases:
            if isinstance(base, ast.Name) and base.id == "TypedDict":
                self.dataclass_names.add(node.name)
            if isinstance(base, ast.Attribute) and base.attr == "TypedDict":
                self.dataclass_names.add(node.name)
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_function(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_function(node)
        self.generic_visit(node)

    def _check_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Check a function for non-serializable types."""
        if not self._has_dbos_step_decorator(node):
            return

        if self._has_noqa_comment(node.lineno):
            return

        type_checker = TypeChecker(self.class_defs, self.dataclass_names)

        # Check parameter types
        for arg in node.args.args:
            # Skip 'self' parameter
            if arg.arg == "self":
                continue

            if arg.annotation:
                is_ok, reason = type_checker.check_type(arg.annotation)
                if not is_ok:
                    type_str = ast.unparse(arg.annotation) if hasattr(ast, "unparse") else "<type>"
                    self.violations.append(
                        Violation(
                            file=self.file_path,
                            line=node.lineno,
                            function_name=node.name,
                            param_name=arg.arg,
                            type_annotation=type_str,
                            reason=reason,
                        )
                    )

        # Check *args
        if node.args.vararg and node.args.vararg.annotation:
            is_ok, reason = type_checker.check_type(node.args.vararg.annotation)
            if not is_ok:
                type_str = (
                    ast.unparse(node.args.vararg.annotation)
                    if hasattr(ast, "unparse")
                    else "<type>"
                )
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=node.name,
                        param_name=f"*{node.args.vararg.arg}",
                        type_annotation=type_str,
                        reason=reason,
                    )
                )

        # Check **kwargs
        if node.args.kwarg and node.args.kwarg.annotation:
            is_ok, reason = type_checker.check_type(node.args.kwarg.annotation)
            if not is_ok:
                type_str = (
                    ast.unparse(node.args.kwarg.annotation) if hasattr(ast, "unparse") else "<type>"
                )
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=node.name,
                        param_name=f"**{node.args.kwarg.arg}",
                        type_annotation=type_str,
                        reason=reason,
                    )
                )

        # Check keyword-only args
        for arg in node.args.kwonlyargs:
            if arg.annotation:
                is_ok, reason = type_checker.check_type(arg.annotation)
                if not is_ok:
                    type_str = ast.unparse(arg.annotation) if hasattr(ast, "unparse") else "<type>"
                    self.violations.append(
                        Violation(
                            file=self.file_path,
                            line=node.lineno,
                            function_name=node.name,
                            param_name=arg.arg,
                            type_annotation=type_str,
                            reason=reason,
                        )
                    )

        # Check return type
        if node.returns:
            is_ok, reason = type_checker.check_type(node.returns)
            if not is_ok:
                type_str = ast.unparse(node.returns) if hasattr(ast, "unparse") else "<type>"
                self.violations.append(
                    Violation(
                        file=self.file_path,
                        line=node.lineno,
                        function_name=node.name,
                        param_name="return",
                        type_annotation=type_str,
                        reason=reason,
                    )
                )


def check_file(file_path: Path, source: str | None = None) -> Sequence[Violation]:
    """Check a single file for DBOS011 violations."""
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
