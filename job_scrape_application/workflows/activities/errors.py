from __future__ import annotations

import time
from typing import Any, Dict, TypedDict, cast


class ScrapeErrorInputRequired(TypedDict):
    error: str


class ScrapeErrorInputOptional(TypedDict, total=False):
    jobId: str
    sourceUrl: str
    siteId: str
    event: str
    status: str
    metadata: Any
    payload: Any
    createdAt: int


class ScrapeErrorInput(ScrapeErrorInputRequired, ScrapeErrorInputOptional):
    pass


class ScrapeErrorPayloadRequired(TypedDict):
    error: str
    createdAt: int


class ScrapeErrorPayloadOptional(TypedDict, total=False):
    jobId: str
    sourceUrl: str
    siteId: str
    event: str
    status: str
    metadata: Any
    payload: Any


class ScrapeErrorPayload(ScrapeErrorPayloadRequired, ScrapeErrorPayloadOptional):
    pass


def clean_scrape_error_payload(payload: ScrapeErrorInput) -> ScrapeErrorPayload:
    """Drop None values and ensure Convex payload strings never receive null."""

    created_at = payload.get("createdAt")
    cleaned: Dict[str, Any] = {
        "error": payload["error"],
        "createdAt": int(created_at if created_at is not None else int(time.time() * 1000)),
    }

    optional_fields = (
        "jobId",
        "sourceUrl",
        "siteId",
        "event",
        "status",
        "metadata",
        "payload",
    )
    for key in optional_fields:
        value = payload.get(key)
        if value is not None:
            cleaned[key] = value

    return cast(ScrapeErrorPayload, cleaned)


async def log_scrape_error(payload: ScrapeErrorInput) -> None:
    """Persist scrape/HTTP errors to Convex for audit visibility."""

    from ...services.convex_client import convex_mutation
    from ...services import telemetry

    data = clean_scrape_error_payload(payload)
    try:
        await convex_mutation("router:insertScrapeError", data)
    except Exception:
        # Best-effort; do not raise
        pass

    try:
        telemetry.emit_posthog_log(
            {
                "event": "scrape.error",
                "level": "error",
                "siteUrl": data.get("sourceUrl", ""),
                "data": data,
            }
        )
    except Exception:
        # Best-effort; do not raise
        return
