from __future__ import annotations

import orjson
from pathlib import Path
from typing import Any


def _read_fixture_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_spidercloud_fixture(path: Path) -> Any:
    payload = orjson.loads(_read_fixture_text(path))
    if isinstance(payload, dict) and "response" in payload:
        return payload.get("response")
    return payload
