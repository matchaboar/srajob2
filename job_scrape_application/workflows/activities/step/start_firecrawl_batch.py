"""DBOS step function for starting a Firecrawl batch scrape."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict

from dbos import DBOS

logger = logging.getLogger("dbos.step.firecrawl")


class WebhookModel:
    """Minimal shim to satisfy Firecrawl client's model_dump expectation."""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def model_dump(self, *_: Any, exclude_none: bool = False, **__: Any) -> Dict[str, Any]:
        if not exclude_none:
            return self._data

        def _strip_none(val: Any) -> Any:
            if isinstance(val, dict):
                return {k: _strip_none(v) for k, v in val.items() if v is not None}
            if isinstance(val, list):
                return [_strip_none(v) for v in val if v is not None]
            return val

        cleaned = _strip_none(self._data)
        return cleaned if isinstance(cleaned, dict) else self._data


@DBOS.step(retries_allowed=True, max_attempts=3, interval_seconds=2.0, backoff_rate=2.0)
async def start_firecrawl_batch_step(
    start_fn: Callable[[Any], Any],
    webhook_model: Any,
    webhook_payload: Dict[str, Any],
) -> Any:
    """Start a Firecrawl batch scrape via the Firecrawl SDK.

    This step calls the Firecrawl SDK asynchronously using asyncio.to_thread()
    since the SDK is synchronous. It handles the model_dump compatibility shim
    for webhook payloads.

    Args:
        start_fn: A callable that invokes the Firecrawl SDK's start method.
        webhook_model: The webhook model to pass to the start function.
        webhook_payload: The webhook payload dict (used for retry with shim).

    Returns:
        The Firecrawl job object from the SDK response.

    Raises:
        Exception: On non-retryable errors after retry attempts exhausted.
    """
    logger.info(
        "firecrawl.start_batch entering webhook_keys=%s metadata_keys=%s",
        list(webhook_payload.keys()),
        list((webhook_payload.get("metadata") or {}).keys()),
    )
    try:
        return await asyncio.to_thread(start_fn, webhook_model)
    except AttributeError as exc:
        if "model_dump" not in str(exc):
            raise

        retry_webhook = WebhookModel(webhook_payload)
        logger.warning("Retrying Firecrawl start with wrapped webhook after model_dump error")
        return await asyncio.to_thread(start_fn, retry_webhook)
