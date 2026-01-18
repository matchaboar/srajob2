from __future__ import annotations

import os

from dbos import Queue

LISTING_QUEUE_NAME = "listing"
DETAIL_QUEUE_NAME = "detail"


def _resolve_worker_concurrency(env_key: str, default_value: int = 1) -> int:
    raw = os.getenv(env_key)
    if raw is None:
        return max(1, default_value)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return max(1, default_value)


LISTING_QUEUE = Queue(
    LISTING_QUEUE_NAME,
    worker_concurrency=_resolve_worker_concurrency("DBOS_LISTING_WORKER_CONCURRENCY", 1),
)
DETAIL_QUEUE = Queue(
    DETAIL_QUEUE_NAME,
    worker_concurrency=_resolve_worker_concurrency("DBOS_DETAIL_WORKER_CONCURRENCY", 1),
)

__all__ = [
    "DETAIL_QUEUE",
    "DETAIL_QUEUE_NAME",
    "LISTING_QUEUE",
    "LISTING_QUEUE_NAME",
]
