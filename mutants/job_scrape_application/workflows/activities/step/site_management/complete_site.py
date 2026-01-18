"""DBOS step function for completing a site scrape."""

from __future__ import annotations

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
def complete_site_step(site_id: str) -> None:
    """Mark a site as completed after successful scraping.

    Args:
        site_id: The Convex document ID of the site to complete.

    Note:
        Silently returns if site_id doesn't look like a Convex ID
        or if there's an ArgumentValidationError for the .id field.
    """
    from .....services.convex_client import ArgumentValidationError, convex_mutation

    if not _looks_like_convex_id(site_id):
        # Skip best-effort if id is not a Convex document id
        return

    try:
        convex_mutation("router:completeSite", {"id": site_id})
    except ArgumentValidationError as exc:
        # Swallow validator errors for .id field so workflows continue
        if ".id" in str(exc):
            return
        raise
