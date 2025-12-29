from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, List


def _gather_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, dict):
        for child in value.values():
            yield from _gather_strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _gather_strings(child)


def _extract_text(events: List[Any]) -> str:
    def _extract(value: Any) -> str:
        if isinstance(value, dict):
            for key in ("content", "raw_html", "html", "text", "body", "result", "raw"):
                candidate = value.get(key)
                if isinstance(candidate, (bytes, bytearray)):
                    candidate = candidate.decode("utf-8", errors="replace")
                if isinstance(candidate, str) and candidate.strip():
                    return candidate
            for child in value.values():
                found = _extract(child)
                if found:
                    return found
            return ""
        if isinstance(value, list):
            for child in value:
                found = _extract(child)
                if found:
                    return found
            return ""
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            return value
        return ""

    for event in events:
        candidate = _extract(event)
        if isinstance(candidate, str) and candidate.strip():
            return candidate
    return ""


def _merge_json_fragments(value: Any) -> str | None:
    fragments = [s.strip() for s in _gather_strings(value) if isinstance(s, str) and s.strip()]
    if len(fragments) < 2:
        return None
    jsonish = [fragment for fragment in fragments if any(ch in fragment for ch in ("{", "}", "[", "]"))]
    if len(jsonish) < 2:
        return None
    merged = "".join(jsonish)
    if "jobs" not in merged and "positions" not in merged:
        return None
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect SpiderCloud response payloads to understand content chunking.",
    )
    parser.add_argument("path", help="Path to JSON response written by dump_spidercloud_response.py")
    parser.add_argument("--max-samples", type=int, default=3, help="Max fragment samples to print")
    args = parser.parse_args()

    payload = json.loads(Path(args.path).read_text(encoding="utf-8"))
    raw_events = payload if isinstance(payload, list) else [payload]

    fragments = [s for s in _gather_strings(raw_events) if isinstance(s, str) and s.strip()]
    jsonish = [fragment for fragment in fragments if any(ch in fragment for ch in ("{", "}", "[", "]"))]

    raw_text = _extract_text(raw_events)
    merged = _merge_json_fragments(raw_events)

    print(
        json.dumps(
            {
                "raw_events_type": type(raw_events).__name__,
                "raw_events_len": len(raw_events),
                "string_fragments": len(fragments),
                "jsonish_fragments": len(jsonish),
                "raw_text_len": len(raw_text),
                "raw_text_preview": raw_text[:160].replace("\n", "\\n") if raw_text else "",
                "merged_len": len(merged) if merged else None,
                "merged_preview": merged[:160].replace("\n", "\\n") if merged else "",
            },
            indent=2,
            ensure_ascii=False,
        )
    )

    if jsonish and args.max_samples > 0:
        for idx, fragment in enumerate(jsonish[: args.max_samples], start=1):
            preview = fragment[:200].replace("\n", "\\n")
            print(f"[jsonish_fragment_{idx}] len={len(fragment)} preview={preview}")


if __name__ == "__main__":
    main()
