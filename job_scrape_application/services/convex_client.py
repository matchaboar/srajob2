from __future__ import annotations

import asyncio
import random
from typing import Any, Mapping

from convex import ConvexClient

from ..config import settings
from . import telemetry

_client: ConvexClient | None = None
_REQUEST_TIMEOUT_SECONDS = 4.0
_TOTAL_BUDGET_SECONDS = 8.0
_MAX_RETRIES = 3
_BACKOFF_BASE_SECONDS = 0.5
_BACKOFF_MAX_SECONDS = 4.0


async def _call_with_retry(fn, name: str, args: Mapping[str, Any] | None) -> Any:
    last_error: Exception | None = None
    start = asyncio.get_event_loop().time()
    for attempt in range(1, _MAX_RETRIES + 1):
        elapsed = asyncio.get_event_loop().time() - start
        remaining_budget = _TOTAL_BUDGET_SECONDS - elapsed
        if remaining_budget <= 0:
            break
        per_attempt_timeout = min(_REQUEST_TIMEOUT_SECONDS, max(0.0, remaining_budget))
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(fn, name, args),
                timeout=per_attempt_timeout,
            )
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= _MAX_RETRIES:
                break
            elapsed = asyncio.get_event_loop().time() - start
            remaining_budget = _TOTAL_BUDGET_SECONDS - elapsed
            if remaining_budget <= 0:
                break
            backoff = min(_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), _BACKOFF_MAX_SECONDS)
            jitter = random.uniform(0, 0.25)
            sleep_for = min(backoff + jitter, max(0.0, remaining_budget))
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
    raise last_error if last_error else RuntimeError("Convex call failed")


def _normalize_deployment_url() -> str:
    """
    Prefer CONVEX_URL (Convex deployment URL).
    Fallback to CONVEX_HTTP_URL by converting .convex.site -> .convex.cloud.
    """

    if settings.convex_url:
        return settings.convex_url

    if settings.convex_http_url:
        url = settings.convex_http_url.rstrip("/")
        if ".convex.site" in url:
            url = url.replace(".convex.site", ".convex.cloud")
        return url

    raise RuntimeError("CONVEX_URL env var is required for Convex client")


def get_client() -> ConvexClient:
    global _client
    if _client is None:
        _client = ConvexClient(_normalize_deployment_url())
    return _client


async def convex_query(name: str, args: Mapping[str, Any] | None = None) -> Any:
    client = get_client()
    return await _call_with_retry(client.query, name, args)


async def convex_mutation(name: str, args: Mapping[str, Any] | None = None) -> Any:
    client = get_client()
    try:
        return await _call_with_retry(client.mutation, name, args)
    except Exception:
        try:
            payload = {
                "event": "convex.mutation_failed",
                "name": name,
            }
            if isinstance(args, Mapping):
                payload["argKeys"] = list(args.keys())
            telemetry.emit_posthog_log(payload)
        except Exception:
            pass
        raise


# Test helper to inject a mock client
def _set_client_for_tests(client: ConvexClient | None) -> None:
    global _client
    _client = client
