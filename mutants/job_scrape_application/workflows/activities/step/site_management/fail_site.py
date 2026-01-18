"""DBOS step function for marking a site as failed."""

from __future__ import annotations

from typing import Any, Dict

from dbos import DBOS


def _looks_like_convex_id(value: str) -> bool:
    """Check if a string looks like a valid Convex document ID."""
    return isinstance(value, str) and len(value) >= 26 and value.isalnum()


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def fail_site_step(payload: Dict[str, Any]) -> None:
    """Mark a site as failed with an error message.

    Args:
        payload: Dict containing 'id' (site ID) and optional 'error' message.

    Note:
        Silently returns if site_id doesn't look like a Convex ID
        or if there's an ArgumentValidationError for the .id field.
    """
    from .....services.convex_client import ArgumentValidationError, convex_mutation

    site_id = payload.get("id")
    if not isinstance(site_id, str) or not _looks_like_convex_id(site_id):
        return

    try:
        convex_mutation("router:failSite", {"id": site_id, "error": payload.get("error")})
    except ArgumentValidationError as exc:
        # Swallow validator errors for .id field so workflows continue
        if ".id" in str(exc):
            return
        raise
