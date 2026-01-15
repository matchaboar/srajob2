from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List


def _newline_for(text: str) -> str:
    if "\r\n" in text:
        return "\r\n"
    return "\n"


def _split_items(lines: List[str]) -> tuple[List[str], List[List[str]]]:
    header: List[str] = []
    items: List[List[str]] = []
    current: List[str] | None = None

    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("- url:"):
            if current is not None:
                items.append(current)
            current = [line]
            continue
        if current is None:
            header.append(line)
        else:
            current.append(line)

    if current is not None:
        items.append(current)

    return header, items


def _find_key_indent(lines: Iterable[str]) -> str:
    for line in lines:
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("- "):
            continue
        return line[: len(line) - len(stripped)]
    return "  "


def _item_has_pagination_limit(item: Iterable[str]) -> bool:
    for line in item:
        stripped = line.lstrip()
        if stripped.startswith("paginationLimit:"):
            return True
    return False


def _insert_pagination_limit(item: List[str], limit: int, newline: str) -> List[str]:
    indent = _find_key_indent(item[1:])
    insert_line = f"{indent}paginationLimit: {limit}{newline}"

    for idx, line in enumerate(item):
        stripped = line.lstrip()
        if stripped.startswith("schedule:"):
            return item[:idx] + [insert_line] + item[idx:]

    return item + [insert_line]


def apply_default_pagination_limit(text: str, limit: int) -> str:
    newline = _newline_for(text)
    lines = text.splitlines(keepends=True)
    header, items = _split_items(lines)
    updated_items: List[str] = []

    for item in items:
        if _item_has_pagination_limit(item):
            updated_items.extend(item)
        else:
            updated_items.extend(_insert_pagination_limit(item, limit, newline))

    return "".join(header + updated_items)


def main() -> None:
    parser = argparse.ArgumentParser(description="Add default paginationLimit to site schedule YAML entries.")
    parser.add_argument("--path", required=True, help="Path to a site_schedules.yml file.")
    parser.add_argument("--limit", type=int, default=3, help="Default pagination limit to apply.")
    args = parser.parse_args()

    path = Path(args.path)
    original = path.read_text(encoding="utf-8")
    updated = apply_default_pagination_limit(original, args.limit)

    if updated != original:
        path.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    main()
