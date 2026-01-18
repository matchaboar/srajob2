from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import hashlib
import logging
import os
import sqlite3
import uuid

from dbos import SetEnqueueOptions, error as dbos_error
from .sqlite import _resolve_db_path, initialize_schema, now_ms, read_only, transaction
from .workflow_queues import (
    DETAIL_QUEUE,
    DETAIL_QUEUE_NAME,
    LISTING_QUEUE,
    LISTING_QUEUE_NAME,
)
from ..workflows.helpers.link_extractors import normalize_url

logger = logging.getLogger("dbos.queue")

QUEUE_LISTING = LISTING_QUEUE_NAME
QUEUE_DETAIL = DETAIL_QUEUE_NAME
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
MAX_ATTEMPTS = 3


@dataclass(frozen=True)
class QueueItem:
    id: str
    url: str
    source_url: str | None
    provider: str | None
    site_id: str | None
    pattern: str | None
    url_type: str | None
    posted_at: int | None
    status: str
    attempts: int
    dedupe_key: str | None
    created_at: int
    run_after: int


@dataclass(frozen=True)
class LeaseResult:
    urls: list[dict[str, Any]]
    skipped_urls: list[str]


def _dedupe_key(url: str, site_id: str | None, provider: str | None) -> str:
    normalized = normalize_url(url) or url.strip()
    provider_val = (provider or "unknown").strip().lower()
    site_val = (site_id or "unknown").strip().lower()
    return f"{provider_val}:{site_val}:{normalized.strip().lower()}"


def _normalize_url_type(value: str | None) -> str | None:
    if not value:
        return None
    lowered = value.lower()
    if lowered in {"listing", "detail"}:
        return lowered
    return None


def _hash_dedupe_id(prefix: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


def _listing_dedupe_id(url: str) -> str:
    normalized = normalize_url(url) or url.strip()
    return _hash_dedupe_id("listing", normalized)


def _detail_dedupe_id(url: str, site_id: str | None, provider: str | None) -> str:
    return _hash_dedupe_id("detail", _dedupe_key(url, site_id, provider))


def _build_queue_entry(
    *,
    url: str,
    source_url: str | None,
    provider: str | None,
    site_id: str | None,
    pattern: str | None,
    url_type: str | None,
    posted_at: int | None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "_id": str(uuid.uuid4()),
        "url": url,
        "sourceUrl": source_url,
        "provider": provider,
        "siteId": site_id,
        "pattern": pattern,
        "urlType": url_type,
        "attempts": 1,
    }
    if posted_at is not None:
        entry["postedAt"] = posted_at
    return entry


def _enqueue_listing_batches(entries: list[dict[str, Any]], *, force_refresh: bool) -> int:
    """Enqueue listing batches. Only called from workflow context via enqueue_scrape_urls_step."""
    from ..workflows.workflow.scrape_listing_batch import scrape_listing_batch

    queued = 0
    for entry in entries:
        url_val = entry.get("url")
        if not isinstance(url_val, str) or not url_val.strip():
            continue
        dedupe_id = None if force_refresh else _listing_dedupe_id(url_val)
        try:
            if dedupe_id:
                with SetEnqueueOptions(deduplication_id=dedupe_id):
                    LISTING_QUEUE.enqueue(scrape_listing_batch, batch={"urls": [entry]})  # noqa: DBOS010
            else:
                LISTING_QUEUE.enqueue(scrape_listing_batch, batch={"urls": [entry]})  # noqa: DBOS010
            queued += 1
        except dbos_error.DBOSQueueDeduplicatedError:
            continue
    return queued


def _enqueue_detail_batches(entries: list[dict[str, Any]], *, force_refresh: bool) -> int:
    """Enqueue detail batches. Only called from workflow context via enqueue_scrape_urls_step."""
    from ..workflows.workflow.scrape_job_detail_batch import scrape_job_detail_batch

    queued = 0
    for entry in entries:
        url_val = entry.get("url")
        if not isinstance(url_val, str) or not url_val.strip():
            continue
        site_id = entry.get("siteId") if isinstance(entry.get("siteId"), str) else None
        provider = entry.get("provider") if isinstance(entry.get("provider"), str) else None
        dedupe_id = None if force_refresh else _detail_dedupe_id(url_val, site_id, provider)
        try:
            if dedupe_id:
                with SetEnqueueOptions(deduplication_id=dedupe_id):
                    DETAIL_QUEUE.enqueue(  # noqa: DBOS010
                        scrape_job_detail_batch,
                        batch={"urls": [entry]},
                        persist_scrapes=True,
                    )
            else:
                DETAIL_QUEUE.enqueue(  # noqa: DBOS010
                    scrape_job_detail_batch,
                    batch={"urls": [entry]},
                    persist_scrapes=True,
                )
            queued += 1
        except dbos_error.DBOSQueueDeduplicatedError:
            continue
    return queued


def enqueue_scrape_urls(payload: dict[str, Any], *, force_refresh: bool = False) -> dict[str, Any]:
    initialize_schema()
    urls_raw = payload.get("urls")
    if not isinstance(urls_raw, list):
        return {"queued": 0}

    source_url = payload.get("sourceUrl") if isinstance(payload.get("sourceUrl"), str) else None
    provider = payload.get("provider") if isinstance(payload.get("provider"), str) else None
    site_id = payload.get("siteId") if isinstance(payload.get("siteId"), str) else None
    pattern = payload.get("pattern") if isinstance(payload.get("pattern"), str) else None
    delays_ms = payload.get("delaysMs") if isinstance(payload.get("delaysMs"), list) else None
    url_types = payload.get("urlTypes") if isinstance(payload.get("urlTypes"), list) else None
    posted_ats = payload.get("postedAts") if isinstance(payload.get("postedAts"), list) else None

    if delays_ms and any(isinstance(val, (int, float)) and val > 0 for val in delays_ms):
        logger.info("delaysMs provided but DBOS queues do not support per-URL delays; ignoring.")

    listing_entries: list[dict[str, Any]] = []
    detail_entries: list[dict[str, Any]] = []
    seen_listing: set[str] = set()
    seen_detail: set[str] = set()

    for idx, raw_url in enumerate(urls_raw):
        if not isinstance(raw_url, str):
            continue
        url = raw_url.strip()
        if not url:
            continue
        url_type_val = None
        if url_types and idx < len(url_types) and isinstance(url_types[idx], str):
            url_type_val = _normalize_url_type(url_types[idx])
        posted_at = None
        if posted_ats and idx < len(posted_ats):
            val = posted_ats[idx]
            if isinstance(val, (int, float)):
                posted_at = int(val)

        if url_type_val == "detail":
            dedupe = _dedupe_key(url, site_id, provider)
            if dedupe in seen_detail:
                continue
            seen_detail.add(dedupe)
            if not force_refresh and _detail_already_completed(dedupe):
                continue
            detail_entries.append(_build_queue_entry(
                url=url,
                source_url=source_url,
                provider=provider,
                site_id=site_id,
                pattern=pattern,
                url_type=url_type_val,
                posted_at=posted_at,
            ))
        else:
            normalized = normalize_url(url) or url
            if normalized in seen_listing:
                continue
            seen_listing.add(normalized)
            listing_entries.append(_build_queue_entry(
                url=url,
                source_url=source_url,
                provider=provider,
                site_id=site_id,
                pattern=pattern,
                url_type=url_type_val,
                posted_at=posted_at,
            ))

    queued = 0
    if listing_entries:
        queued += _enqueue_listing_batches(listing_entries, force_refresh=force_refresh)
    if detail_entries:
        queued += _enqueue_detail_batches(detail_entries, force_refresh=force_refresh)

    return {"queued": queued}


def _enqueue_url(
    *,
    url: str,
    queue_name: str,
    url_type: str | None,
    posted_at: int | None,
    source_url: str | None,
    provider: str | None,
    site_id: str | None,
    pattern: str | None,
    run_after_ms: int = 0,
    force_refresh: bool = False,
) -> bool:
    initialize_schema()
    dedupe = None
    if queue_name == QUEUE_DETAIL:
        dedupe = _dedupe_key(url, site_id, provider)
        if not force_refresh and _detail_already_completed(dedupe):
            return False

    run_after = now_ms() + max(run_after_ms, 0)
    created_at = now_ms()
    item_id = str(uuid.uuid4())

    with transaction() as conn:
        if dedupe:
            row = conn.execute(
                "SELECT 1 FROM queue_items WHERE queue_name=? AND dedupe_key=? AND status IN (?, ?)",
                (queue_name, dedupe, STATUS_PENDING, STATUS_PROCESSING),
            ).fetchone()
            if row is not None and not force_refresh:
                return False
        conn.execute(
            """
            INSERT INTO queue_items (
                id, queue_name, url, source_url, provider, site_id, pattern, url_type, posted_at,
                status, attempts, dedupe_key, created_at, run_after, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                queue_name,
                url,
                source_url,
                provider,
                site_id,
                pattern,
                url_type,
                posted_at,
                STATUS_PENDING,
                0,
                dedupe,
                created_at,
                run_after,
                created_at,
            ),
        )
    return True


def _detail_already_completed(dedupe_key: str) -> bool:
    initialize_schema()
    with read_only() as conn:
        row = conn.execute(
            "SELECT 1 FROM job_detail_dedupe WHERE dedupe_key=?",
            (dedupe_key,),
        ).fetchone()
        return row is not None


def lease_scrape_url_batch(
    *,
    provider: str | None = None,
    limit: int = 50,
    url_type: str | None = None,
) -> LeaseResult:
    initialize_schema()
    queue_name = QUEUE_DETAIL if url_type == "detail" else QUEUE_LISTING
    now = now_ms()
    leased: list[dict[str, Any]] = []
    skipped: list[str] = []

    with transaction() as conn:
        rows = conn.execute(
            """
            SELECT * FROM queue_items
            WHERE queue_name = ?
              AND status = ?
              AND run_after <= ?
              AND attempts < ?
              AND (? IS NULL OR provider = ?)
            ORDER BY created_at
            LIMIT ?
            """,
            (queue_name, STATUS_PENDING, now, MAX_ATTEMPTS, provider, provider, limit),
        ).fetchall()

        for row in rows:
            item = _row_to_item(row)
            if item.attempts >= MAX_ATTEMPTS:
                skipped.append(item.url)
                _update_status(conn, item.id, STATUS_FAILED, error="max_attempts_exceeded")
                continue
            leased.append(_item_to_payload(item))
            conn.execute(
                """
                UPDATE queue_items
                SET status = ?, attempts = attempts + 1, processing_started_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (STATUS_PROCESSING, now, now, item.id),
            )

    return LeaseResult(urls=leased, skipped_urls=skipped)


def complete_scrape_urls(payload: dict[str, Any]) -> dict[str, Any]:
    initialize_schema()
    items = payload.get("items")
    status = payload.get("status")
    if not isinstance(items, list) or not isinstance(status, str):
        return {"updated": 0}
    error = payload.get("error") if isinstance(payload.get("error"), str) else None
    run_after_ms = payload.get("runAfterMs")
    if isinstance(run_after_ms, (int, float)):
        run_after_ms = int(run_after_ms)
    else:
        run_after_ms = None

    updated = 0
    with transaction() as conn:
        for entry in items:
            if not isinstance(entry, dict):
                continue
            row_id = entry.get("id") if isinstance(entry.get("id"), str) else None
            url = entry.get("url") if isinstance(entry.get("url"), str) else None
            if not row_id and not url:
                continue
            row = _find_row(conn, row_id=row_id, url=url)
            if row is not None:
                if status == STATUS_PENDING:
                    _requeue_item(conn, row["id"], run_after_ms=run_after_ms, error=error)
                else:
                    _update_status(conn, row["id"], status, error=error)
                updated += 1

            if status == STATUS_COMPLETED:
                entry_url_type = entry.get("urlType")
                is_detail = isinstance(entry_url_type, str) and entry_url_type.lower() == "detail"
                if row is not None and row.get("url_type") == "detail":
                    is_detail = True
                if is_detail:
                    url_val = url or (row.get("url") if row else None)
                    provider = entry.get("provider") if isinstance(entry.get("provider"), str) else None
                    site_id = entry.get("siteId") if isinstance(entry.get("siteId"), str) else None
                    if row is not None:
                        provider = provider or row.get("provider")
                        site_id = site_id or row.get("site_id")
                    if isinstance(url_val, str) and url_val.strip():
                        dedupe = _dedupe_key(url_val, site_id, provider)
                        conn.execute(
                            "INSERT OR REPLACE INTO job_detail_dedupe (dedupe_key, url, completed_at) VALUES (?, ?, ?)",
                            (dedupe, url_val, now_ms()),
                        )

    return {"updated": updated}


def _dbos_system_db_path() -> str:
    return str(_resolve_db_path().parent / "dbos_system.sqlite")


def _dbos_queue_counts(queue_name: str) -> dict[str, int]:
    path = _dbos_system_db_path()
    if not os.path.exists(path):
        return {}
    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error:
        return {}
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT status, COUNT(1) AS total
            FROM workflow_status
            WHERE queue_name = ?
            GROUP BY status
            """,
            (queue_name,),
        ).fetchall()
        return {row["status"]: int(row["total"]) for row in rows}
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def queue_status() -> dict[str, Any]:
    def _normalize(counts: dict[str, int]) -> dict[str, int]:
        failed_total = (
            counts.get("ERROR", 0)
            + counts.get("CANCELLED", 0)
            + counts.get("MAX_RECOVERY_ATTEMPTS_EXCEEDED", 0)
        )
        return {
            "pending": counts.get("ENQUEUED", 0),
            "processing": counts.get("PENDING", 0),
            "completed": counts.get("SUCCESS", 0),
            "failed": failed_total,
        }

    listing_counts = _dbos_queue_counts(QUEUE_LISTING)
    detail_counts = _dbos_queue_counts(QUEUE_DETAIL)
    return {"listing": _normalize(listing_counts), "detail": _normalize(detail_counts)}


def detail_queue_has_pending(*, include_processing: bool = False) -> bool:
    counts = _dbos_queue_counts(QUEUE_DETAIL)
    if include_processing:
        return (counts.get("ENQUEUED", 0) + counts.get("PENDING", 0)) > 0
    return counts.get("ENQUEUED", 0) > 0


def list_scrape_urls(
    *,
    provider: str | None = None,
    site_id: str | None = None,
    status: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    initialize_schema()
    normalized_status = status.strip().lower() if isinstance(status, str) else None
    valid_statuses = {STATUS_PENDING, STATUS_PROCESSING, STATUS_COMPLETED, STATUS_FAILED}
    if normalized_status and normalized_status not in valid_statuses:
        return []
    statuses = [normalized_status] if normalized_status else [STATUS_PENDING, STATUS_PROCESSING]
    queue_name = QUEUE_DETAIL
    safe_limit = max(1, min(int(limit), 500))
    placeholders = ", ".join("?" for _ in statuses)

    with read_only() as conn:
        rows = conn.execute(
            f"""
            SELECT url, status, created_at, updated_at, source_url, provider, site_id, pattern,
                   attempts, error, url_type
            FROM queue_items
            WHERE queue_name = ?
              AND status IN ({placeholders})
              AND (? IS NULL OR provider = ?)
              AND (? IS NULL OR site_id = ?)
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (queue_name, *statuses, provider, provider, site_id, site_id, safe_limit),
        ).fetchall()

    return [
        {
            "url": row["url"],
            "status": row["status"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"] or row["created_at"],
            "sourceUrl": row["source_url"],
            "provider": row["provider"],
            "siteId": row["site_id"],
            "pattern": row["pattern"],
            "attempts": row["attempts"],
            "error": row["error"],
            "lastError": row["error"],
            "urlType": row["url_type"],
        }
        for row in rows
    ]


def _row_to_item(row: Any) -> QueueItem:
    return QueueItem(
        id=row["id"],
        url=row["url"],
        source_url=row["source_url"],
        provider=row["provider"],
        site_id=row["site_id"],
        pattern=row["pattern"],
        url_type=row["url_type"],
        posted_at=row["posted_at"],
        status=row["status"],
        attempts=int(row["attempts"] or 0),
        dedupe_key=row["dedupe_key"],
        created_at=int(row["created_at"]),
        run_after=int(row["run_after"]),
    )


def _item_to_payload(item: QueueItem) -> dict[str, Any]:
    return {
        "_id": item.id,
        "url": item.url,
        "sourceUrl": item.source_url,
        "provider": item.provider,
        "siteId": item.site_id,
        "pattern": item.pattern,
        "postedAt": item.posted_at,
        "attempts": item.attempts,
        "urlType": item.url_type,
    }


def _find_row(conn, *, row_id: str | None, url: str | None) -> dict[str, Any] | None:
    if row_id:
        row = conn.execute("SELECT * FROM queue_items WHERE id=?", (row_id,)).fetchone()
        if row is not None:
            return dict(row)
    if url:
        row = conn.execute(
            "SELECT * FROM queue_items WHERE url=? ORDER BY created_at DESC LIMIT 1",
            (url,),
        ).fetchone()
        if row is not None:
            return dict(row)
    return None


def _requeue_item(
    conn,
    row_id: str,
    *,
    run_after_ms: int | None = None,
    error: str | None = None,
) -> None:
    now = now_ms()
    delay_ms = max(int(run_after_ms), 0) if isinstance(run_after_ms, (int, float)) else 0
    run_after = now + delay_ms
    conn.execute(
        """
        UPDATE queue_items
        SET status = ?, run_after = ?, processing_started_at = NULL,
            completed_at = NULL, updated_at = ?, error = ?
        WHERE id = ?
        """,
        (STATUS_PENDING, run_after, now, error, row_id),
    )


def _update_status(conn, row_id: str, status: str, *, error: str | None = None) -> None:
    now = now_ms()
    conn.execute(
        """
        UPDATE queue_items
        SET status = ?, completed_at = ?, updated_at = ?, error = ?
        WHERE id = ?
        """,
        (status, now, now, error, row_id),
    )


def recover_stale_processing_items(*, stale_threshold_ms: int = 300_000) -> int:
    """Reset items stuck in 'processing' status back to 'pending'.

    Called at worker startup to recover from crashes. Items processing for longer
    than stale_threshold_ms (default 5 minutes) are considered stale.

    Returns the number of items recovered.
    """
    initialize_schema()
    now = now_ms()
    cutoff = now - stale_threshold_ms

    with transaction() as conn:
        result = conn.execute(
            """
            UPDATE queue_items
            SET status = ?, processing_started_at = NULL, updated_at = ?
            WHERE status = ?
              AND processing_started_at < ?
              AND attempts < ?
            """,
            (STATUS_PENDING, now, STATUS_PROCESSING, cutoff, MAX_ATTEMPTS),
        )
        recovered = result.rowcount

    return recovered
