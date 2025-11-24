from __future__ import annotations

import asyncio
from typing import Any, Optional

from convex import ConvexClient

from .config import settings

_client: Optional[ConvexClient] = None


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


async def convex_query(name: str, args: Any | None = None) -> Any:
    client = get_client()
    return await asyncio.to_thread(client.query, name, args)


async def convex_mutation(name: str, args: Any | None = None) -> Any:
    client = get_client()
    return await asyncio.to_thread(client.mutation, name, args)


# Test helper to inject a mock client
def _set_client_for_tests(client: ConvexClient | None) -> None:
    global _client
    _client = client
