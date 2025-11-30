from __future__ import annotations

import json
from typing import Any, Dict, Optional


def _shrink_for_log(value: Any, max_chars: int = 400) -> Any:
    """Return a compact preview while keeping structured data when small enough."""

    if value is None:
        return None

    try:
        serialized = json.dumps(value, ensure_ascii=False)
    except Exception:
        try:
            serialized = str(value)
        except Exception:
            return None

    if len(serialized) <= max_chars:
        return value

    return f"{serialized[:max_chars]}... (+{len(serialized) - max_chars} chars)"


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

    request_preview = _shrink_for_log(request)
    response_preview = _shrink_for_log(response)

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
