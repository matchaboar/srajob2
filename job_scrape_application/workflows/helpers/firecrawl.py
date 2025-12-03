from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from temporalio.exceptions import ApplicationError

from ...config import settings
from ..activities.constants import FIRECRAWL_WEBHOOK_EVENTS, FirecrawlJobKind


def convex_http_base() -> str:
    """Return Convex HTTP base with .convex.site domain for webhooks."""

    if settings.convex_http_url:
        base = settings.convex_http_url.rstrip("/")
    elif settings.convex_url:
        base = settings.convex_url.rstrip("/").replace(".convex.cloud", ".convex.site")
    else:
        raise ApplicationError(
            "CONVEX_HTTP_URL or CONVEX_URL env var is required for Convex HTTP routes",
            non_retryable=True,
        )

    if ".convex.site" not in base and ".convex.cloud" in base:
        base = base.replace(".convex.cloud", ".convex.site")

    return base


def build_firecrawl_webhook(site: Dict[str, Any], kind: FirecrawlJobKind) -> Dict[str, Any]:
    """Construct webhook config with metadata for Firecrawl jobs."""

    selected_events = FIRECRAWL_WEBHOOK_EVENTS.for_kind(kind)
    events = [event.value for event in selected_events]

    metadata: Dict[str, Any] = {
        "siteId": site.get("_id"),
        "siteUrl": site.get("url"),
        "siteType": site.get("type") or "general",
        "pattern": site.get("pattern"),
        "kind": kind,
        "providerVersion": "v2",
    }

    # Firecrawl rejects nulls in webhook metadata; drop any None values.
    metadata = {k: v for k, v in metadata.items() if v is not None}

    return {
        "url": f"{convex_http_base()}/api/firecrawl/webhook",
        "events": events,
        "metadata": metadata,
    }


def stringify_firecrawl_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Convert webhook metadata values to strings for Firecrawl's API contract."""

    def _to_string(value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    return {key: _to_string(val) for key, val in metadata.items() if val is not None}


def should_use_mock_firecrawl(site_url: Optional[str]) -> bool:
    """Return True when the site URL should route to the mock Firecrawl client."""

    flag = os.getenv("FIRECRAWL_FORCE_MOCK")
    if flag is not None:
        return flag.lower() not in {"", "0", "false"}

    # Tests expect the real (monkeypatched) Firecrawl client even for example.com
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False

    if not site_url:
        return False
    try:
        hostname = urlparse(site_url).hostname
    except Exception:
        return False
    return bool(hostname and hostname.lower().endswith("example.com"))


def should_mock_convex_webhooks() -> bool:
    """Return True when webhook bookkeeping should skip real Convex calls."""

    flag = os.getenv("MOCK_CONVEX_WEBHOOKS")
    if flag is not None:
        return flag.lower() not in {"", "0", "false"}

    # Default to mock when running tests to avoid hitting real Convex.
    if os.getenv("PYTEST_CURRENT_TEST") or "pytest" in sys.modules:
        return True

    # Also avoid real Convex when using demo/test endpoints.
    base = (settings.convex_http_url or settings.convex_url or "").lower()
    if "demo.convex.site" in base or "example" in base:
        return True

    return False


def metadata_urls_to_list(value: Any) -> List[str]:
    """Parse a Firecrawl metadata urls/seedUrls field into a list of strings."""

    if isinstance(value, list):
        return [url for url in value if isinstance(url, str) and url.strip()]

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [url for url in parsed if isinstance(url, str) and url.strip()]
        except Exception:
            pass
        return [value] if value.strip() else []

    return []


def extract_first_json_doc(payload: Any) -> Any:
    """Return the first json/data field from a Firecrawl status payload."""

    documents: List[Any] = []
    if hasattr(payload, "data"):
        documents = getattr(payload, "data") or []
    elif isinstance(payload, dict):
        documents = payload.get("data") or []

    for doc in documents:
        val = None
        if hasattr(doc, "json"):
            val = getattr(doc, "json")
        elif isinstance(doc, dict):
            val = doc.get("json") or doc.get("data")
        if val is not None:
            return val

    return None


def extract_first_text_doc(payload: Any) -> str | None:
    """Return the first raw/html/text field from a Firecrawl status payload."""

    documents: List[Any] = []
    if hasattr(payload, "data"):
        documents = getattr(payload, "data") or []
    elif isinstance(payload, dict):
        documents = payload.get("data") or []

    for doc in documents:
        if isinstance(doc, str):
            return doc
        if hasattr(doc, "raw_html") and isinstance(getattr(doc, "raw_html"), str):
            return getattr(doc, "raw_html")
        if hasattr(doc, "html") and isinstance(getattr(doc, "html"), str):
            return getattr(doc, "html")
        if isinstance(doc, dict):
            for key in ("raw_html", "html", "text", "content"):
                val = doc.get(key)
                if isinstance(val, str):
                    return val
    return None
