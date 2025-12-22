from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import re
import html as html_lib

from dotenv import load_dotenv
from spider import AsyncSpider

GITHUB_API_URL = (
    "https://www.github.careers/api/jobs?keywords=engineer&sortBy=relevance&limit=100"
)

SPIDER_PARAMS: Dict[str, Any] = {
    "return_format": ["raw_html"],
    "metadata": True,
    "request": "chrome",
    "follow_redirects": True,
    "redirect_policy": "Loose",
    "external_domains": ["*"],
    "preserve_host": True,
    "limit": 1,
}


async def _collect_response(response: Any) -> List[Any]:
    if hasattr(response, "__aiter__"):
        items = []
        async for item in response:
            items.append(item)
        return items
    if hasattr(response, "__await__"):
        result = await response
        return [result] if result is not None else []
    return [response] if response is not None else []


def _gather_strings(node: Any) -> Iterable[str]:
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for val in node.values():
            yield from _gather_strings(val)
    elif isinstance(node, list):
        for val in node:
            yield from _gather_strings(val)


def _find_jobs_payload(node: Any) -> Optional[Dict[str, Any]]:
    if isinstance(node, dict) and isinstance(node.get("jobs"), list):
        return node
    if isinstance(node, dict) and isinstance(node.get("positions"), list):
        return node
    if isinstance(node, dict):
        for val in node.values():
            found = _find_jobs_payload(val)
            if found:
                return found
    if isinstance(node, list):
        for val in node:
            found = _find_jobs_payload(val)
            if found:
                return found
    return None


def _extract_payload(events: List[Any]) -> Optional[Dict[str, Any]]:
    found = _find_jobs_payload(events)
    if found:
        return found
    for text in _gather_strings(events):
        try:
            parsed = json.loads(text)
        except Exception:
            continue
        if isinstance(parsed, str):
            try:
                parsed = json.loads(parsed)
            except Exception:
                pass
        found = _find_jobs_payload(parsed)
        if found:
            return found
    return None


def _extract_json_from_html(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    match = re.search(r"<pre>(?P<content>.*?)</pre>", text, flags=re.IGNORECASE | re.DOTALL)
    content = match.group("content") if match else text
    content = html_lib.unescape(content).strip()
    if not content:
        return None
    raw_candidate = None
    if content.startswith("{") and content.endswith("}"):
        raw_candidate = content
    else:
        brace_match = re.search(r"{.*}", content, flags=re.DOTALL)
        if brace_match:
            raw_candidate = brace_match.group(0)
    if not raw_candidate:
        return None
    try:
        return json.loads(raw_candidate)
    except Exception:
        try:
            unescaped = raw_candidate.encode("utf-8", errors="ignore").decode("unicode_escape")
            return json.loads(unescaped)
        except Exception:
            return None


def _summarize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    job_count = len(jobs) if isinstance(jobs, list) else 0
    positions = payload.get("positions") if isinstance(payload, dict) else None
    positions_count = len(positions) if isinstance(positions, list) else 0
    summary = {
        "jobs_count": job_count,
        "positions_count": positions_count,
        "payload_keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
    }
    for key in ("total", "count", "page", "per_page", "pageSize", "page_size"):
        if key in payload:
            summary[key] = payload.get(key)
    meta = payload.get("meta") if isinstance(payload, dict) else None
    if isinstance(meta, dict):
        summary["meta_keys"] = sorted(meta.keys())
        for key in ("total", "count", "page", "page_size", "per_page", "limit"):
            if key in meta:
                summary[f"meta.{key}"] = meta.get(key)
    return summary


def _extract_first_html(events: List[Any]) -> Optional[str]:
    def _candidate(value: Any) -> Optional[str]:
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            return value
        return None

    def _looks_like_html(text: str) -> bool:
        if "<html" in text.lower() or "<body" in text.lower():
            return True
        return "<div" in text.lower() or "<section" in text.lower()

    for event in events:
        if isinstance(event, dict):
            for key in ("raw_html", "html", "content", "body", "text", "result"):
                raw_val = event.get(key)
                text = _candidate(raw_val)
                if text and _looks_like_html(text):
                    return text
        text = _candidate(event)
        if text and _looks_like_html(text):
            return text

    for text in _gather_strings(events):
        if text and _looks_like_html(text):
            return text

    return None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose SpiderCloud scrape results.")
    parser.add_argument("--url", default=GITHUB_API_URL, help="URL to scrape")
    parser.add_argument("--out", help="Optional output path for raw HTML fixture")
    parser.add_argument("--out-json", help="Optional output path for extracted JSON fixture")
    args = parser.parse_args()

    load_dotenv()
    load_dotenv("job_board_application/.env.production", override=False)
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    async with AsyncSpider(api_key=api_key) as client:
        response = await _collect_response(
            client.scrape_url(
                args.url,
                params=SPIDER_PARAMS,
                stream=False,
                content_type="application/json",
            )
        )

    payload = _extract_payload(response)
    if not payload:
        html_payload = None
        for text in _gather_strings(response):
            html_payload = _extract_json_from_html(text)
            if html_payload:
                payload = html_payload
                break

    if payload:
        summary = _summarize_payload(payload)
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        sample = response[0] if response else None
        sample_type = type(sample).__name__
        sample_keys = sorted(sample.keys()) if isinstance(sample, dict) else None
        sample_str = None
        for text in _gather_strings(response):
            if text:
                preview = text[:200].replace("\n", "\\n")
                sample_str = f"string_sample_len={len(text)} preview={preview}"
                break
        print(
            json.dumps(
                {
                    "error": "No JSON payload with jobs[] found.",
                    "response_count": len(response),
                    "sample_type": sample_type,
                    "sample_keys": sample_keys,
                    "sample_string": sample_str,
                },
                indent=2,
            )
        )

    if args.out:
        html_text = _extract_first_html(response)
        if not html_text:
            raise SystemExit("Unable to find raw HTML in SpiderCloud response.")
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(html_text, encoding="utf-8")
        print(json.dumps({"saved_html": str(out_path), "chars": len(html_text)}, indent=2))

    if args.out_json:
        if not payload:
            raise SystemExit("Unable to extract JSON payload for out-json.")
        out_path = Path(args.out_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"saved_json": str(out_path), "chars": len(json.dumps(payload, ensure_ascii=False))}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
