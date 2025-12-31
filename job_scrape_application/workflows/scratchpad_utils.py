from __future__ import annotations

from itertools import islice
from typing import Any, Dict, Optional


def _shrink_for_log(
    value: Any,
    max_chars: int = 400,
    *,
    max_items: int = 6,
    max_depth: int = 2,
) -> Any:
    """Return a compact preview without expensive serialization in workflow code."""

    if value is None:
        return None

    if isinstance(value, (int, float, bool)):
        return value

    if isinstance(value, str):
        if len(value) <= max_chars:
            return value
        return f"{value[:max_chars]}... (+{len(value) - max_chars} chars)"

    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"

    if isinstance(value, dict):
        size = len(value)
        if size == 0:
            return {}
        if max_depth <= 0:
            return f"<dict size={size}>"
        if size <= max_items:
            preview: Dict[str, Any] = {}
            for key, child in value.items():
                preview[str(key)] = _shrink_for_log(
                    child,
                    max_chars=max_chars,
                    max_items=max_items,
                    max_depth=max_depth - 1,
                )
            return preview
        sample_keys = [str(key) for key in islice(value.keys(), max_items)]
        return {"_type": "dict", "size": size, "keys": sample_keys}

    if isinstance(value, (list, tuple)):
        size = len(value)
        if max_depth <= 0:
            return f"<{type(value).__name__} size={size}>"
        if size <= max_items:
            return [
                _shrink_for_log(
                    child,
                    max_chars=max_chars,
                    max_items=max_items,
                    max_depth=max_depth - 1,
                )
                for child in value
            ]
        return {
            "_type": type(value).__name__,
            "size": size,
            "sample": [
                _shrink_for_log(
                    child,
                    max_chars=max_chars,
                    max_items=max_items,
                    max_depth=max_depth - 1,
                )
                for child in value[:max_items]
            ],
        }

    if isinstance(value, set):
        return f"<set size={len(value)}>"

    try:
        text = str(value)
    except Exception:
        return f"<{type(value).__name__}>"

    if len(text) <= max_chars:
        return text

    return f"{text[:max_chars]}... (+{len(text) - max_chars} chars)"


def extract_http_exchange(scrape_result: Any) -> Optional[Dict[str, Any]]:
    """Build a scratchpad-friendly view of provider request/response data."""

    if not isinstance(scrape_result, dict):
        return None

    items = scrape_result.get("items")
    item_block = items if isinstance(items, dict) else {}

    request = (
        scrape_result.get("request")
        or item_block.get("request")
        or item_block.get("request_data")
        or item_block.get("requestData")
    )
    response = (
        scrape_result.get("response")
        or scrape_result.get("asyncResponse")
        or item_block.get("response")
        or item_block.get("raw")
    )
    provider = scrape_result.get("provider") or item_block.get("provider")
    job_id = scrape_result.get("jobId") or item_block.get("jobId")
    status_url = scrape_result.get("statusUrl") or item_block.get("statusUrl")
    webhook_id = scrape_result.get("webhookId") or item_block.get("webhookId")

    max_chars = 12000 if provider == "fetchfox" else 400

    request_preview = _shrink_for_log(request, max_chars=max_chars)
    response_preview = _shrink_for_log(response, max_chars=max_chars)

    if request_preview is None and response_preview is None:
        return None

    payload: Dict[str, Any] = {}
    if provider is not None:
        payload["provider"] = provider
    if job_id is not None:
        payload["jobId"] = str(job_id)
    if status_url is not None:
        payload["statusUrl"] = status_url
    if webhook_id is not None:
        payload["webhookId"] = str(webhook_id)
    if request_preview is not None:
        payload["request"] = request_preview
    if response_preview is not None:
        payload["response"] = response_preview

    return payload if payload else None
