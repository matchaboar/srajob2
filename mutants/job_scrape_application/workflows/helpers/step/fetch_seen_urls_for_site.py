"""DBOS step function for fetching seen URLs for a site."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from dbos import DBOS


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def fetch_seen_urls_for_site(
    source_url: str,
    pattern: Optional[str],
    candidate_urls: Optional[List[str]] = None,
) -> List[str]:
    """Fetch URLs already seen for a site from Convex."""
    from ....services.convex_client import convex_query

    payload: Dict[str, Any] = {"sourceUrl": source_url}
    if pattern is not None:
        payload["pattern"] = pattern
    if candidate_urls is not None:
        cleaned_candidates = [
            url.strip() for url in candidate_urls if isinstance(url, str) and url.strip()
        ]
        if cleaned_candidates:
            payload["urls"] = cleaned_candidates

    try:
        res = convex_query("router:listSeenJobUrlsForSite", payload)
    except Exception:
        return []

    urls = res.get("urls", []) if isinstance(res, dict) else []
    return [u for u in urls if isinstance(u, str)]
