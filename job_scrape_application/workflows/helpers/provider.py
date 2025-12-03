from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from ...config import settings

logger = logging.getLogger("temporal.worker.activities")


def build_provider_status_url(
    provider: str, job_id: str | None, *, status_url: str | None = None, kind: str | None = None
) -> str | None:
    """Return a human-friendly status link for a provider if we can infer one."""

    def _parse_http_url(url: str) -> Optional[Any]:
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return None
        return parsed

    status_url_str = status_url.strip() if isinstance(status_url, str) else None
    job_id_str = str(job_id) if job_id not in (None, "") else None

    if provider == "firecrawl":
        firecrawl_status = None
        if job_id_str:
            firecrawl_status = f"https://api.firecrawl.dev/v2/batch/scrape/{job_id_str}"

        if status_url_str:
            parsed = _parse_http_url(status_url_str)
            host = parsed.netloc.lower() if parsed else ""
            if parsed and "firecrawl" in host and (job_id_str is None or job_id_str in parsed.path):
                return status_url_str

        return firecrawl_status

    if status_url_str:
        return status_url_str
    return None


def log_provider_dispatch(provider: str, url: str, **context: Any) -> None:
    """Emit a colored stdout log when dispatching a scrape request to a provider."""

    context_parts = [f"{k}={v}" for k, v in context.items() if v is not None]
    context_str = " ".join(context_parts)
    msg = f"[SCRAPE DISPATCH] provider={provider} url={url}"
    if context_str:
        msg = f"{msg} {context_str}"
    print(f"\x1b[36m{msg}\x1b[0m")
    logger.info(msg)


def mask_secret(secret: str | None) -> str | None:
    """Return a lightly redacted version of a secret for audit purposes."""

    if secret is None:
        return None
    secret_str = str(secret)
    if not secret_str:
        return None
    if len(secret_str) <= 4:
        return "*" * len(secret_str)
    return f"{secret_str[:4]}...{secret_str[-2:]}"


def sanitize_headers(headers: Dict[str, Any] | None) -> Dict[str, Any] | None:
    """Mask sensitive header values while keeping the shape visible."""

    if not isinstance(headers, dict):
        return None

    sanitized: Dict[str, Any] = {}
    for key, value in headers.items():
        if value is None:
            continue
        if isinstance(value, str):
            masked = mask_secret(value)
            sanitized[key] = masked if masked is not None else value
        else:
            sanitized[key] = value

    return sanitized if sanitized else None


def build_request_snapshot(
    body: Any,
    *,
    provider: str | None = None,
    url: str | None = None,
    method: str | None = None,
    headers: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Construct a serializable view of the outbound provider request."""

    header_block: Dict[str, Any] = dict(headers or {})
    if provider == "firecrawl" and settings.firecrawl_api_key:
        header_block.setdefault("authorization", f"Bearer {mask_secret(settings.firecrawl_api_key)}")
    if provider == "fetchfox" and settings.fetchfox_api_key:
        header_block.setdefault("x-api-key", mask_secret(settings.fetchfox_api_key))
    if provider == "spidercloud" and (
        settings.spider_api_key or os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    ):
        api_key = settings.spider_api_key or os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
        header_block.setdefault("authorization", f"Bearer {mask_secret(api_key)}")

    sanitized_headers = sanitize_headers(header_block) if header_block else None
    snapshot: Dict[str, Any] = {}

    if method:
        snapshot["method"] = method
    if url:
        snapshot["url"] = url
    if body is not None:
        snapshot["body"] = body
    if sanitized_headers:
        snapshot["headers"] = sanitized_headers

    return snapshot


def log_sync_response(
    provider: str,
    *,
    action: str,
    url: str | None = None,
    job_id: str | None = None,
    status_url: str | None = None,
    kind: str | None = None,
    summary: str | None = None,
    metadata: Dict[str, Any] | None = None,
    response: Any | None = None,
) -> None:
    """Print a synchronous provider response to stdout for quick debugging."""

    link = build_provider_status_url(provider, job_id, status_url=status_url, kind=kind)

    def _fmt(val: Any) -> str:
        return str(val)

    parts = [f"provider={provider}", f"action={action}"]
    if url:
        parts.append(f"url={url}")
    if kind:
        parts.append(f"kind={kind}")
    if job_id:
        parts.append(f"job_id={job_id}")
    if link:
        parts.append(f"status_url={link}")
    if summary:
        parts.append(summary)
    if metadata:
        meta_bits = [f"{k}={_fmt(v)}" for k, v in metadata.items() if v is not None]
        if meta_bits:
            parts.append(" ".join(meta_bits))

    if response is not None:
        try:
            serialized = json.dumps(response, default=str)
        except Exception:
            serialized = str(response)

        max_len = 4000
        if len(serialized) > max_len:
            serialized = f"{serialized[:max_len]}...(+{len(serialized) - max_len} chars)"

        parts.append(f"response={serialized}")

    msg = f"[SCRAPE RESPONSE] {' '.join(parts)}"
    print(f"\x1b[33m{msg}\x1b[0m")
    logger.info(msg)


__all__ = [
    "build_provider_status_url",
    "build_request_snapshot",
    "log_provider_dispatch",
    "log_sync_response",
    "mask_secret",
    "sanitize_headers",
]
