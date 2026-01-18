"""DBOS step function for looking up job IDs by URL."""

from __future__ import annotations

import logging
from typing import List, Optional

from dbos import DBOS

from ...helpers.link_extractors import normalize_url
from ...site_handlers import get_site_handler

logger = logging.getLogger("temporal.worker.activities")


def _to_greenhouse_marketing_url(url: str) -> Optional[str]:
    """Convert Greenhouse API detail URL to the public marketing page."""
    from urllib.parse import urlparse

    try:
        parsed = urlparse(url)
    except Exception:
        return None

    host = (parsed.hostname or "").lower()
    if "greenhouse.io" not in host:
        return None

    parts = [p for p in parsed.path.split("/") if p]
    # Expected API shape: /v1/boards/{slug}/jobs/{id}
    if len(parts) >= 5 and parts[0] == "v1" and parts[1] == "boards" and parts[3] == "jobs":
        slug = parts[2]
        job_id = parts[4]
        return f"https://boards.greenhouse.io/{slug}/jobs/{job_id}"

    return None


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def lookup_job_id_for_url(url: str) -> Optional[str]:
    """Look up a job ID in Convex by URL, trying various URL formats."""
    from ....services.convex_client import convex_query

    candidates: List[str] = []
    seen: set[str] = set()

    def _add_candidate(value: str | None) -> None:
        if not isinstance(value, str) or not value.strip():
            return
        normalized = normalize_url(value) or value.strip()
        if normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    _add_candidate(url)

    handler = get_site_handler(url)
    if handler and handler.name == "greenhouse":
        _add_candidate(handler.get_api_uri(url))
        _add_candidate(_to_greenhouse_marketing_url(url))

    last_exc: Exception | None = None
    for candidate in candidates:
        try:
            result = convex_query("jobs:getJobIdByUrl", {"url": candidate})
        except Exception as exc:
            logger.debug(
                "Job ID lookup failed for candidate %s: %s",
                candidate,
                exc,
                exc_info=exc,
            )
            last_exc = exc
            continue
        if result:
            return result

    if last_exc is not None:
        logger.warning(
            "Failed to lookup job id for %s (tried %d candidates): %s",
            url,
            len(candidates),
            last_exc,
            exc_info=last_exc,
        )
    return None
