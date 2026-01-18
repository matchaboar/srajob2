#!/usr/bin/env python3
"""Find Python functions that appear to be unused (no callers found).

This script:
1. Parses all Python files to find function definitions
2. Searches the codebase for references to each function name
3. Reports functions that only appear at their definition site

Limitations:
- Cannot detect dynamic calls (getattr, importlib, etc.)
- May flag public API functions that are called externally
- May flag entry points (CLI, DBOS workflows, pytest fixtures)
"""

from __future__ import annotations

import argparse
import ast
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


@dataclass
class FunctionInfo:
    name: str
    file: Path
    line: int
    is_method: bool
    is_private: bool
    decorators: list[str]


def get_python_files(
    root: Path,
    exclude_patterns: list[str] | None = None,
) -> Iterator[Path]:
    """Yield all Python files, excluding specified patterns."""
    exclude_patterns = exclude_patterns or []
    for py_file in root.rglob("*.py"):
        rel_path = str(py_file.relative_to(root))
        if any(pattern in rel_path for pattern in exclude_patterns):
            continue
        yield py_file


def extract_functions(file_path: Path, root: Path) -> Iterator[FunctionInfo]:
    """Extract function definitions from a Python file."""
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"Warning: Could not parse {file_path}: {e}", file=sys.stderr)
        return

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # Check if it's a method (inside a class)
            is_method = False
            for parent in ast.walk(tree):
                if isinstance(parent, ast.ClassDef):
                    for child in ast.iter_child_nodes(parent):
                        if child is node:
                            is_method = True
                            break

            decorators = []
            for dec in node.decorator_list:
                if isinstance(dec, ast.Name):
                    decorators.append(dec.id)
                elif isinstance(dec, ast.Attribute):
                    decorators.append(dec.attr)
                elif isinstance(dec, ast.Call):
                    if isinstance(dec.func, ast.Name):
                        decorators.append(dec.func.id)
                    elif isinstance(dec.func, ast.Attribute):
                        decorators.append(dec.func.attr)

            yield FunctionInfo(
                name=node.name,
                file=file_path.relative_to(root),
                line=node.lineno,
                is_method=is_method,
                is_private=node.name.startswith("_"),
                decorators=decorators,
            )


def count_references(name: str, root: Path, exclude_patterns: list[str]) -> int:
    """Count references to a function name using ripgrep."""
    exclude_args = []
    for pattern in exclude_patterns:
        exclude_args.extend(["-g", f"!{pattern}"])

    # Search for the function name as a word boundary match
    cmd = [
        "rg",
        "--count-matches",
        "--word-regexp",
        "--type", "py",
        *exclude_args,
        name,
        str(root),
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        total = 0
        for line in result.stdout.strip().split("\n"):
            if ":" in line:
                count_str = line.rsplit(":", 1)[-1]
                try:
                    total += int(count_str)
                except ValueError:
                    pass
        return total
    except FileNotFoundError:
        print("Error: ripgrep (rg) not found. Please install it.", file=sys.stderr)
        sys.exit(1)


def is_likely_entrypoint(func: FunctionInfo) -> bool:
    """Check if a function is likely an entry point or framework hook."""
    entrypoint_decorators = {
        # DBOS
        "workflow",
        "step",
        "transaction",
        "scheduled",
        "handler",
        # pytest
        "fixture",
        "mark",
        # FastAPI/Flask
        "get",
        "post",
        "put",
        "delete",
        "route",
        "app",
        # Other
        "property",
        "staticmethod",
        "classmethod",
        "cached_property",
        "abstractmethod",
    }

    if any(dec in entrypoint_decorators for dec in func.decorators):
        return True

    # Common entry point names
    entrypoint_names = {
        "main",
        "setup",
        "teardown",
        "setUp",
        "tearDown",
        "setUpClass",
        "tearDownClass",
        "conftest",
    }

    if func.name in entrypoint_names:
        return True

    # pytest test functions
    if func.name.startswith("test_"):
        return True

    return False


def check_exported_in_init(func: FunctionInfo, root: Path) -> bool:
    """Check if function is exported in an __init__.py file."""
    init_file = root / func.file.parent / "__init__.py"
    if not init_file.exists():
        return False

    try:
        content = init_file.read_text(encoding="utf-8")
        # Check for import or __all__ inclusion
        if func.name in content:
            return True
    except (OSError, UnicodeDecodeError):
        pass

    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find potentially unused Python functions"
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Root directory to search (default: current directory)",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Patterns to exclude (can be specified multiple times)",
    )
    parser.add_argument(
        "--include-private",
        action="store_true",
        help="Include private functions (starting with _)",
    )
    parser.add_argument(
        "--include-methods",
        action="store_true",
        help="Include class methods",
    )
    parser.add_argument(
        "--include-entrypoints",
        action="store_true",
        help="Include likely entry points (decorated functions, test_*, etc.)",
    )
    parser.add_argument(
        "--min-refs",
        type=int,
        default=1,
        help="Report functions with fewer than this many references (default: 1)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show more details",
    )

    args = parser.parse_args()

    # Default exclusions
    default_excludes = [
        "_archive",
        ".venv",
        "venv",
        "__pycache__",
        ".git",
        "node_modules",
        ".egg-info",
        "build",
        "dist",
        ".uv-cache",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
    ]
    exclude_patterns = default_excludes + args.exclude

    # Glob patterns for ripgrep
    rg_excludes = [
        "*/_archive/*",
        "*/.venv/*",
        "*/venv/*",
        "*/__pycache__/*",
        "*/.git/*",
        "*/node_modules/*",
        "*/.egg-info/*",
        "*/build/*",
        "*/dist/*",
        "*/.uv-cache/*",
        "*/.mypy_cache/*",
        "*/.pytest_cache/*",
        "*/.ruff_cache/*",
    ] + [f"*/{p}/*" for p in args.exclude]

    root = args.root.resolve()

    print(f"Scanning {root} for unused functions...\n")

    all_functions: list[FunctionInfo] = []
    for py_file in get_python_files(root, exclude_patterns):
        for func in extract_functions(py_file, root):
            all_functions.append(func)

    if args.verbose:
        print(f"Found {len(all_functions)} function definitions\n")

    unused: list[tuple[FunctionInfo, int]] = []

    for func in all_functions:
        # Skip based on filters
        if not args.include_private and func.is_private:
            continue
        if not args.include_methods and func.is_method:
            continue
        if not args.include_entrypoints and is_likely_entrypoint(func):
            continue

        # Count references
        ref_count = count_references(func.name, root, rg_excludes)

        # A function defined once and referenced once means only the definition
        # (or one call). We look for functions with very few references.
        if ref_count <= args.min_refs:
            # Check if it's exported in __init__.py
            if check_exported_in_init(func, root):
                if args.verbose:
                    print(f"Skipping {func.name} (exported in __init__.py)")
                continue

            unused.append((func, ref_count))

    # Sort by file path and line number
    unused.sort(key=lambda x: (str(x[0].file), x[0].line))

    if unused:
        print(f"Found {len(unused)} potentially unused functions:\n")
        current_file = None
        for func, refs in unused:
            if func.file != current_file:
                current_file = func.file
                print(f"\n{current_file}:")
            decorator_str = f" @{','.join(func.decorators)}" if func.decorators else ""
            print(f"  L{func.line}: {func.name}{decorator_str} (refs: {refs})")
    else:
        print("No unused functions found.")


if __name__ == "__main__":
    main()
