"""DBOS012: Detect functions not used in any production code path (dead code).

This rule requires cross-file analysis to accurately detect dead code.
The check_file function returns empty results; use the cross-file analysis
functions in lint_dbos_step.py for accurate detection.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

RULE_ID = "DBOS012"
SUMMARY = "Function is not used in any production code path (dead code)."

# Decorators that mark a function as an entry point (production code path)
ENTRY_POINT_DECORATORS = frozenset({
    # DBOS entry points
    "workflow",
    "scheduled",
    "step",  # Steps can be entry points when called from external systems
    # Common test decorators
    "pytest.fixture",
    "fixture",
    # Common API framework decorators
    "route",
    "get",
    "post",
    "put",
    "patch",
    "delete",
    "api",
    "endpoint",
    # Property decorators
    "property",
    "staticmethod",
    "classmethod",
    # Abstract methods (implemented by subclasses)
    "abstractmethod",
})

# Function name patterns that indicate entry points
ENTRY_POINT_NAME_PATTERNS = frozenset({
    "main",
    # Dunder methods (called by Python runtime)
    "__init__",
    "__new__",
    "__call__",
    "__enter__",
    "__exit__",
    "__aenter__",
    "__aexit__",
    "__iter__",
    "__next__",
    "__getattr__",
    "__setattr__",
    "__delattr__",
    "__getitem__",
    "__setitem__",
    "__delitem__",
    "__contains__",
    "__len__",
    "__str__",
    "__repr__",
    "__eq__",
    "__hash__",
    "__lt__",
    "__le__",
    "__gt__",
    "__ge__",
    "__add__",
    "__sub__",
    "__mul__",
    "__truediv__",
    "__floordiv__",
    "__mod__",
    "__pow__",
    "__and__",
    "__or__",
    "__xor__",
    "__neg__",
    "__pos__",
    "__abs__",
    "__bool__",
    "__int__",
    "__float__",
    "__index__",
    "__dir__",
    "__format__",
    "__sizeof__",
    "__reduce__",
    "__reduce_ex__",
    "__copy__",
    "__deepcopy__",
    "__getstate__",
    "__setstate__",
    "__getnewargs__",
    "__getnewargs_ex__",
    # Logging handler methods
    "emit",
    "format",
    "formatException",
    "formatStack",
    "formatTime",
    "handle",
    "handleError",
    "flush",
    "close",
    # HTTP handler methods
    "do_GET",
    "do_POST",
    "do_PUT",
    "do_DELETE",
    "do_PATCH",
    "do_HEAD",
    "do_OPTIONS",
    # Dataclass/pydantic validators
    "validate",
    "validator",
    # Framework callbacks
    "setup",
    "teardown",
    "on_start",
    "on_stop",
    "on_event",
    # Protocol/abstract methods commonly overridden
    "extract",  # Extractor pattern
    "scrape",  # Scraper pattern
    "process",  # Processor pattern
    "handle",  # Handler pattern
    "run",  # Runner pattern
    "execute",  # Executor pattern
})

# Name prefixes that indicate entry points
ENTRY_POINT_NAME_PREFIXES = (
    "test_",
    "Test",
)

# Name suffixes that indicate entry points
ENTRY_POINT_NAME_SUFFIXES = (
    "_fixture",
    "_handler",
    "_callback",
    "_hook",
    "_validator",
)


@dataclass
class FunctionDefinition:
    """Information about a function definition."""

    name: str
    qualified_name: str  # module.ClassName.func_name or module.func_name
    file: Path
    line: int
    is_entry_point: bool
    is_method: bool
    parent_class: str | None
    module_name: str
    # Functions this function calls (by name, may be unqualified)
    calls: frozenset[str] = field(default_factory=frozenset)


@dataclass
class Violation:
    """A DBOS012 violation."""

    file: Path
    line: int
    function_name: str

    @property
    def rule(self) -> str:
        return RULE_ID

    def format(self) -> str:
        return (
            f"{self.file}:{self.line}: [{RULE_ID}] "
            f"Function `{self.function_name}` is not used in any production code path"
        )


class CallGraphCollector(ast.NodeVisitor):
    """Collect function definitions, calls, and imports from a single file."""

    def __init__(self, file_path: Path, source_lines: list[str], module_name: str) -> None:
        self.file_path = file_path
        self.source_lines = source_lines
        self.module_name = module_name

        # Collected data
        self.functions: dict[str, FunctionDefinition] = {}
        self.exports: set[str] = set()  # Names in __all__

        # Import tracking: local_name -> (module, original_name)
        self.imports: dict[str, tuple[str, str]] = {}
        # Star imports: set of module names
        self.star_imports: set[str] = set()

        # Context tracking
        self._current_class: str | None = None
        self._current_function: str | None = None
        self._current_function_calls: set[str] = set()

    def _has_noqa_comment(self, lineno: int) -> bool:
        """Check if a line has a noqa comment for DBOS012."""
        if lineno < 1 or lineno > len(self.source_lines):
            return False
        line = self.source_lines[lineno - 1]
        return "noqa: DBOS012" in line or "noqa:DBOS012" in line

    def _is_entry_point_decorator(self, decorator: ast.expr) -> bool:
        """Check if a decorator marks the function as an entry point."""
        # Handle @decorator or @decorator()
        if isinstance(decorator, ast.Call):
            decorator = decorator.func

        # Handle simple name: @workflow
        if isinstance(decorator, ast.Name):
            return decorator.id in ENTRY_POINT_DECORATORS

        # Handle attribute: @DBOS.workflow, @pytest.fixture
        if isinstance(decorator, ast.Attribute):
            return decorator.attr in ENTRY_POINT_DECORATORS

        return False

    def _is_entry_point(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """Determine if a function is an entry point."""
        # Check for entry point decorators
        for decorator in node.decorator_list:
            if self._is_entry_point_decorator(decorator):
                return True

        # Check for dunder methods and special names
        if node.name in ENTRY_POINT_NAME_PATTERNS:
            return True

        # Check prefixes
        for prefix in ENTRY_POINT_NAME_PREFIXES:
            if node.name.startswith(prefix):
                return True

        # Check suffixes
        for suffix in ENTRY_POINT_NAME_SUFFIXES:
            if node.name.endswith(suffix):
                return True

        return False

    def visit_Module(self, node: ast.Module) -> None:
        """Visit module to check for __all__ exports."""
        for stmt in node.body:
            # Check for __all__ = [...]
            if isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, ast.Name) and target.id == "__all__":
                        if isinstance(stmt.value, (ast.List, ast.Tuple)):
                            for elt in stmt.value.elts:
                                if isinstance(elt, ast.Constant) and isinstance(
                                    elt.value, str
                                ):
                                    self.exports.add(elt.value)

        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        """Track import statements."""
        for alias in node.names:
            name = alias.name
            asname = alias.asname if alias.asname else name
            # import foo or import foo as bar
            self.imports[asname] = (name, name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Track from ... import statements."""
        module = node.module or ""
        for alias in node.names:
            if alias.name == "*":
                self.star_imports.add(module)
            else:
                name = alias.name
                asname = alias.asname if alias.asname else name
                # from module import name or from module import name as alias
                self.imports[asname] = (module, name)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Visit class definitions."""
        prev_class = self._current_class
        self._current_class = node.name
        self.generic_visit(node)
        self._current_class = prev_class

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Process a function definition."""
        # Skip if noqa comment
        if self._has_noqa_comment(node.lineno):
            return

        # Skip private functions (single underscore prefix, not dunder)
        if node.name.startswith("_") and not node.name.startswith("__"):
            return

        # Build qualified name
        if self._current_class:
            qualified_name = f"{self.module_name}.{self._current_class}.{node.name}"
        else:
            qualified_name = f"{self.module_name}.{node.name}"

        # Set up context to collect calls
        prev_function = self._current_function
        prev_calls = self._current_function_calls
        self._current_function = qualified_name
        self._current_function_calls = set()

        # Visit function body to collect calls
        self.generic_visit(node)

        # Record function info
        self.functions[qualified_name] = FunctionDefinition(
            name=node.name,
            qualified_name=qualified_name,
            file=self.file_path,
            line=node.lineno,
            is_entry_point=self._is_entry_point(node),
            is_method=self._current_class is not None,
            parent_class=self._current_class,
            module_name=self.module_name,
            calls=frozenset(self._current_function_calls),
        )

        # Restore context
        self._current_function = prev_function
        self._current_function_calls = prev_calls

    def visit_Call(self, node: ast.Call) -> None:
        """Track function calls."""
        if self._current_function is None:
            self.generic_visit(node)
            return

        call_names = self._get_call_names(node)
        self._current_function_calls.update(call_names)
        self.generic_visit(node)

    def _get_call_names(self, node: ast.Call) -> list[str]:
        """Extract possible function names from a call."""
        func = node.func
        names: list[str] = []

        # Direct call: func()
        if isinstance(func, ast.Name):
            name = func.id
            names.append(name)
            # Also try to resolve via imports
            if name in self.imports:
                module, orig_name = self.imports[name]
                if module:
                    names.append(f"{module}.{orig_name}")
                else:
                    names.append(orig_name)

        # Method/attribute call: obj.method() or module.func()
        elif isinstance(func, ast.Attribute):
            # Get the full dotted name
            dotted = self._get_dotted_name(func)
            if dotted:
                names.append(dotted)
                # Also add just the method name (for self.method calls)
                names.append(func.attr)

                # Try to resolve the base through imports
                parts = dotted.split(".")
                if parts[0] in self.imports:
                    module, orig_name = self.imports[parts[0]]
                    if module:
                        resolved = f"{module}.{orig_name}"
                        if len(parts) > 1:
                            resolved += "." + ".".join(parts[1:])
                        names.append(resolved)

        return names

    def _get_dotted_name(self, node: ast.expr) -> str | None:
        """Get the full dotted name from an attribute chain."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = self._get_dotted_name(node.value)
            if base:
                return f"{base}.{node.attr}"
        return None

    def visit_Name(self, node: ast.Name) -> None:
        """Track function references (not just calls)."""
        if self._current_function is not None and isinstance(node.ctx, ast.Load):
            # This might be a function reference (callback, etc.)
            self._current_function_calls.add(node.id)
            # Also try to resolve via imports
            if node.id in self.imports:
                module, orig_name = self.imports[node.id]
                if module:
                    self._current_function_calls.add(f"{module}.{orig_name}")
        self.generic_visit(node)


def collect_from_file(
    file_path: Path,
    source: str | None = None,
) -> CallGraphCollector | None:
    """Collect function definitions and calls from a single file."""
    if source is None:
        try:
            source = file_path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return None

    try:
        tree = ast.parse(source, filename=str(file_path))
    except SyntaxError:
        return None

    # Derive module name from file path
    # e.g., job_scrape_application/workflows/site_handlers/greenhouse.py
    # -> job_scrape_application.workflows.site_handlers.greenhouse
    module_name = str(file_path.with_suffix("")).replace("/", ".").replace("\\", ".")
    # Remove leading dots if any
    module_name = module_name.lstrip(".")

    source_lines = source.splitlines()
    collector = CallGraphCollector(file_path, source_lines, module_name)
    collector.visit(tree)
    return collector


def collect_from_directory(directory: Path) -> dict[str, FunctionDefinition]:
    """Collect all function definitions from a directory."""
    all_functions: dict[str, FunctionDefinition] = {}

    for file_path in directory.rglob("*.py"):
        path_str = str(file_path)
        # Skip test files for dead code analysis (tests are entry points themselves)
        if "/tests/" in path_str or "\\tests\\" in path_str:
            continue
        # Skip __pycache__
        if "__pycache__" in path_str:
            continue
        # Skip archive directories
        if "_archive" in path_str:
            continue
        # Skip testing utilities
        if "/testing/" in path_str or "\\testing\\" in path_str:
            continue

        collector = collect_from_file(file_path)
        if collector:
            all_functions.update(collector.functions)

    return all_functions


def build_call_graph(
    functions: dict[str, FunctionDefinition],
) -> dict[str, set[str]]:
    """Build a mapping from each function to the functions it might call.

    Returns a dict where keys are qualified function names and values are
    sets of qualified function names that might be called.
    """
    # Build reverse lookup: unqualified name -> list of qualified names
    name_to_qualified: dict[str, list[str]] = {}
    for qname, func in functions.items():
        name_to_qualified.setdefault(func.name, []).append(qname)
        # Also index by class.method for method calls
        if func.parent_class:
            class_method = f"{func.parent_class}.{func.name}"
            name_to_qualified.setdefault(class_method, []).append(qname)

    # Build call graph with resolved names
    call_graph: dict[str, set[str]] = {}
    for qname, func in functions.items():
        resolved_calls: set[str] = set()
        for call in func.calls:
            # Try exact match first
            if call in functions:
                resolved_calls.add(call)
            # Try as unqualified name
            elif call in name_to_qualified:
                resolved_calls.update(name_to_qualified[call])
            # Try partial match (module.func might match full.module.func)
            else:
                for candidate in functions:
                    if candidate.endswith(f".{call}") or candidate.endswith(f".{call.split('.')[-1]}"):
                        resolved_calls.add(candidate)

        call_graph[qname] = resolved_calls

    return call_graph


def compute_reachable_functions(
    functions: dict[str, FunctionDefinition],
    call_graph: dict[str, set[str]],
) -> set[str]:
    """Compute all functions reachable from entry points."""
    # Start with entry points and exported functions
    reachable: set[str] = set()
    worklist: list[str] = []

    for qname, func in functions.items():
        if func.is_entry_point:
            reachable.add(qname)
            worklist.append(qname)

    # BFS to find all reachable functions
    while worklist:
        current = worklist.pop(0)
        if current not in call_graph:
            continue
        for called in call_graph[current]:
            if called not in reachable:
                reachable.add(called)
                worklist.append(called)

    return reachable


def find_dead_code(
    functions: dict[str, FunctionDefinition],
) -> list[Violation]:
    """Find all functions that are not reachable from any entry point."""
    call_graph = build_call_graph(functions)
    reachable = compute_reachable_functions(functions, call_graph)

    violations: list[Violation] = []
    for qname, func in functions.items():
        if qname not in reachable:
            violations.append(
                Violation(
                    file=func.file,
                    line=func.line,
                    function_name=func.name,
                )
            )

    return sorted(violations, key=lambda v: (str(v.file), v.line))


def check_file(file_path: Path, source: str | None = None) -> Sequence[Violation]:
    """Check a single file for DBOS012 violations.

    NOTE: This returns empty results because dead code detection requires
    cross-file analysis. Use find_dead_code() with collect_from_directory()
    for accurate detection.
    """
    # Single-file analysis is not meaningful for dead code detection
    # Return empty to avoid false positives
    return []


def check_directory(directory: Path) -> list[Violation]:
    """Check a directory for dead code using cross-file analysis."""
    functions = collect_from_directory(directory)
    return find_dead_code(functions)
