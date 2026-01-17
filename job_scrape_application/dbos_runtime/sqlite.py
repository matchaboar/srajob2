from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import logging
import os
import sqlite3
import threading
import time

_DEFAULT_DB_NAME = "dbos.sqlite"
_CONNECTIONS = threading.local()
_SCHEMA_LOCK = threading.Lock()
_DB_LOCK = threading.Lock()  # Serialize all DB access to prevent lock contention

logger = logging.getLogger("dbos.sqlite")

# Timeout for acquiring SQLite locks (seconds)
SQLITE_TIMEOUT = 10.0
SQLITE_BUSY_TIMEOUT_MS = 5000


def _resolve_db_path() -> Path:
    env_path = os.getenv("DBOS_SQLITE_PATH")
    if env_path:
        return Path(env_path)
    return Path(__file__).resolve().parent / _DEFAULT_DB_NAME


def get_connection() -> sqlite3.Connection:
    conn = getattr(_CONNECTIONS, "connection", None)
    if conn is not None:
        return conn
    db_path = _resolve_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=SQLITE_TIMEOUT)
    conn.row_factory = sqlite3.Row
    # WAL mode is much better for concurrent access
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    # Allow retries when database is busy
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
    _CONNECTIONS.connection = conn
    return conn


def initialize_schema() -> None:
    with _SCHEMA_LOCK:
        conn = get_connection()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS queue_items (
                id TEXT PRIMARY KEY,
                queue_name TEXT NOT NULL,
                url TEXT NOT NULL,
                source_url TEXT,
                provider TEXT,
                site_id TEXT,
                pattern TEXT,
                url_type TEXT,
                posted_at INTEGER,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                dedupe_key TEXT,
                created_at INTEGER NOT NULL,
                run_after INTEGER NOT NULL,
                processing_started_at INTEGER,
                completed_at INTEGER,
                updated_at INTEGER,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS queue_items_by_status
                ON queue_items(queue_name, status, run_after, created_at);
            CREATE INDEX IF NOT EXISTS queue_items_by_url
                ON queue_items(url, queue_name);
            CREATE INDEX IF NOT EXISTS queue_items_by_dedupe
                ON queue_items(queue_name, dedupe_key);

            CREATE TABLE IF NOT EXISTS job_detail_dedupe (
                dedupe_key TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                completed_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS workflow_runs (
                id TEXT PRIMARY KEY,
                workflow_name TEXT NOT NULL,
                queue_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                completed_at INTEGER,
                error TEXT
            );
            """
        )
        conn.commit()


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    """Write transaction with exclusive lock. Use read_only() for queries."""
    start = time.monotonic()
    acquired = _DB_LOCK.acquire(timeout=SQLITE_TIMEOUT)
    if not acquired:
        logger.error("Failed to acquire DB lock after %.1fs", SQLITE_TIMEOUT)
        raise sqlite3.OperationalError("Database lock timeout")
    lock_wait = time.monotonic() - start
    if lock_wait > 0.1:
        logger.warning("Slow DB lock acquisition: %.3fs", lock_wait)
    try:
        conn = get_connection()
        conn.execute("BEGIN IMMEDIATE;")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        _DB_LOCK.release()
        elapsed = time.monotonic() - start
        if elapsed > 1.0:
            logger.warning("Slow transaction: %.3fs", elapsed)


@contextmanager
def read_only() -> Iterator[sqlite3.Connection]:
    """Read-only access without write lock. Use for SELECT queries.

    WAL mode allows concurrent reads during writes, so no Python lock needed.
    SQLite's busy_timeout handles any transient lock contention.
    """
    start = time.monotonic()
    conn = get_connection()
    yield conn
    elapsed = time.monotonic() - start
    if elapsed > 1.0:
        logger.warning("Slow read operation: %.3fs", elapsed)


def now_ms() -> int:
    return int(time.time() * 1000)
