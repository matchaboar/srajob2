from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import fcntl
import logging
import os
import sqlite3
import threading
import time

_DEFAULT_DB_NAME = "dbos.sqlite"
_CONNECTIONS = threading.local()
_SCHEMA_LOCK = threading.Lock()
_DB_LOCK = threading.Lock()  # Serialize all DB access to prevent lock contention
_SCHEMA_INITIALIZED = False

logger = logging.getLogger("dbos.sqlite")

# Timeout for acquiring SQLite locks (seconds)
SQLITE_TIMEOUT = 10.0
SQLITE_BUSY_TIMEOUT_MS = 5000
FILE_LOCK_TIMEOUT = 30.0


def _resolve_db_target() -> tuple[str, bool]:
    env_uri = os.getenv("DBOS_SQLITE_URI")
    if env_uri:
        return env_uri, True

    env_path = os.getenv("DBOS_SQLITE_PATH")
    if env_path:
        if env_path == ":memory:" or env_path.startswith("file:"):
            return env_path, True
        return env_path, False

    default_path = str(Path(__file__).resolve().parent / _DEFAULT_DB_NAME)
    return default_path, False


def _resolve_db_path() -> Path:
    db_target, is_uri = _resolve_db_target()
    if not is_uri:
        return Path(db_target)
    if db_target == ":memory:":
        return Path(__file__).resolve().parent / _DEFAULT_DB_NAME
    if db_target.startswith("file:"):
        from urllib.parse import urlparse

        parsed = urlparse(db_target)
        if parsed.path:
            return Path(parsed.path)
    return Path(__file__).resolve().parent / _DEFAULT_DB_NAME


def get_connection() -> sqlite3.Connection:
    conn = getattr(_CONNECTIONS, "connection", None)
    if conn is not None:
        return conn
    db_target, is_uri = _resolve_db_target()
    if not is_uri:
        Path(db_target).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        db_target,
        check_same_thread=False,
        timeout=SQLITE_TIMEOUT,
        uri=is_uri,
    )
    conn.row_factory = sqlite3.Row
    # WAL mode is much better for concurrent access
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    # Allow retries when database is busy
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
    _CONNECTIONS.connection = conn
    return conn


@contextmanager
def _file_lock(lock_path: Path, timeout: float = FILE_LOCK_TIMEOUT) -> Iterator[None]:
    """Acquire an exclusive file lock for inter-process coordination.

    Uses fcntl.flock() which works across processes on the same machine.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(lock_path, "w")  # noqa: SIM115
    start = time.monotonic()
    acquired = False
    try:
        while time.monotonic() - start < timeout:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except OSError:
                time.sleep(0.1)
        if not acquired:
            raise sqlite3.OperationalError(
                f"Failed to acquire file lock at {lock_path} after {timeout}s"
            )
        yield
    finally:
        if acquired:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


def initialize_schema() -> None:
    global _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return

    with _SCHEMA_LOCK:
        if _SCHEMA_INITIALIZED:
            return

        lock_path = _resolve_db_path().with_suffix(".lock")
        with _file_lock(lock_path):
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
            _SCHEMA_INITIALIZED = True


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


def _reset_schema_flag() -> None:
    """Reset the schema initialized flag. For testing only."""
    global _SCHEMA_INITIALIZED
    _SCHEMA_INITIALIZED = False


if __name__ == "__main__":
    # Allow running as a module to pre-initialize the schema before workers start
    # Usage: python -m job_scrape_application.dbos_runtime.sqlite
    initialize_schema()
    print(f"Schema initialized at {_resolve_db_path()}")
