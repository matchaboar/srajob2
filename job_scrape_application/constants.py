from __future__ import annotations

import os
from typing import Iterable, Tuple


def _parse_keywords(raw: str | None) -> Tuple[str, ...]:
    if not raw:
        return ()
    return tuple(
        keyword.strip().lower()
        for keyword in raw.split(",")
        if keyword and keyword.strip()
    )


# Comma-separated list of required keywords; default ensures "engineer" must appear.
_raw_keywords = (
    os.getenv("JOB_TITLE_REQUIRED_KEYWORDS")
    or os.getenv("JOB_TITLE_KEYWORDS")
    or "engineer"
)
REQUIRED_JOB_TITLE_KEYWORDS: Tuple[str, ...] = _parse_keywords(_raw_keywords)


def title_matches_required_keywords(
    title: str | None, keywords: Iterable[str] | None = None
) -> bool:
    """
    Return True when the provided title matches the configured required keywords.

    - If no keywords are configured, allow all titles.
    - If the title is unknown/empty, allow it (we'll still scrape in that case).
    - Otherwise, require that at least one keyword appears case-insensitively as a substring.
    """

    required = tuple(k.lower() for k in keywords) if keywords is not None else tuple(REQUIRED_JOB_TITLE_KEYWORDS)
    if not required:
        return True

    if not title:
        return True

    normalized_title = title.lower()
    return any(keyword in normalized_title for keyword in required)
