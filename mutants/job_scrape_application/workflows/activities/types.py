from __future__ import annotations

from typing import Any, Dict, List, NotRequired, TypedDict


# Use a loose dict to avoid Temporal payload converter rejecting Convex fields we don't enumerate.
Site = Dict[str, Any]


class FirecrawlWebhookEvent(TypedDict, total=False):
    """Shape of Firecrawl webhook events delivered in the raw request body."""

    success: bool
    type: str
    event: str
    id: str
    data: List[Any]
    metadata: Dict[str, Any]
    error: NotRequired[str]
    jobId: NotRequired[str]
    status: NotRequired[str]
    status_url: NotRequired[str]
    statusUrl: NotRequired[str]
