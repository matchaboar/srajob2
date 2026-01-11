from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import os
import sqlite3
import threading
import time

_DEFAULT_DB_NAME = "dbos.sqlite"
_CONNECTIONS = threading.local()
_SCHEMA_LOCK = threading.Lock()


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
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
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
    conn = get_connection()
    conn.execute("BEGIN IMMEDIATE;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def now_ms() -> int:
    return int(time.time() * 1000)
