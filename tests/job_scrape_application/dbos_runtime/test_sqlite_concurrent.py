"""Tests for concurrent SQLite schema initialization.

These tests verify that the file-based locking mechanism in sqlite.py
prevents race conditions when multiple processes try to initialize
the database schema simultaneously.
"""
from __future__ import annotations

import multiprocessing
import os
import sqlite3
import time
from pathlib import Path

import pytest

from job_scrape_application.dbos_runtime import sqlite as sqlite_module
from job_scrape_application.dbos_runtime.sqlite import (
    _file_lock,
    _reset_schema_flag,
    initialize_schema,
)


@pytest.fixture
def temp_db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Create a temporary database path and configure sqlite module to use it."""
    db_path = tmp_path / "test.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    _reset_schema_flag()
    # Clear thread-local connection
    if hasattr(sqlite_module._CONNECTIONS, "connection"):
        try:
            sqlite_module._CONNECTIONS.connection.close()
        except Exception:
            pass
        delattr(sqlite_module._CONNECTIONS, "connection")
    yield db_path
    _reset_schema_flag()


def test_file_lock_acquires_and_releases(tmp_path: Path) -> None:
    """Test that file lock can be acquired and released properly."""
    lock_path = tmp_path / "test.lock"

    with _file_lock(lock_path):
        assert lock_path.exists()

    # Lock should be released - we should be able to acquire it again
    with _file_lock(lock_path):
        pass


def test_file_lock_blocks_concurrent_access(tmp_path: Path) -> None:
    """Test that file lock prevents concurrent access."""
    lock_path = tmp_path / "test.lock"

    with _file_lock(lock_path):
        # Try to acquire the same lock with a very short timeout
        # This should fail since we already hold the lock
        with pytest.raises(sqlite3.OperationalError, match="Failed to acquire file lock"):
            with _file_lock(lock_path, timeout=0.2):
                pass


def test_initialize_schema_creates_tables(temp_db_path: Path) -> None:
    """Test that initialize_schema creates the expected tables."""
    initialize_schema()

    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()

    # Check that tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = {row[0] for row in cursor.fetchall()}

    assert "queue_items" in tables
    assert "job_detail_dedupe" in tables
    assert "workflow_runs" in tables

    conn.close()


def test_initialize_schema_is_idempotent(temp_db_path: Path) -> None:
    """Test that calling initialize_schema multiple times is safe."""
    initialize_schema()
    initialize_schema()
    initialize_schema()

    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = {row[0] for row in cursor.fetchall()}

    assert "queue_items" in tables
    conn.close()


def _worker_initialize_schema(db_path: str, result_queue: multiprocessing.Queue) -> None:
    """Worker function for testing concurrent schema initialization."""
    os.environ["DBOS_SQLITE_PATH"] = db_path
    try:
        # Reset state for this new process
        sqlite_module._SCHEMA_INITIALIZED = False
        if hasattr(sqlite_module._CONNECTIONS, "connection"):
            delattr(sqlite_module._CONNECTIONS, "connection")

        initialize_schema()
        result_queue.put(("success", None))
    except Exception as e:
        result_queue.put(("error", str(e)))


def test_concurrent_schema_initialization_from_multiple_processes(tmp_path: Path) -> None:
    """Test that multiple processes can safely initialize the schema concurrently.

    This test simulates the actual failure scenario where multiple DBOS workers
    start simultaneously and all try to initialize the database schema.
    """
    db_path = tmp_path / "concurrent_test.sqlite"

    num_workers = 10
    result_queue: multiprocessing.Queue = multiprocessing.Queue()
    processes: list[multiprocessing.Process] = []

    # Start multiple processes simultaneously
    for _ in range(num_workers):
        p = multiprocessing.Process(
            target=_worker_initialize_schema,
            args=(str(db_path), result_queue),
        )
        processes.append(p)

    # Start all processes at approximately the same time
    for p in processes:
        p.start()

    # Wait for all processes to complete
    for p in processes:
        p.join(timeout=60)

    # Collect results
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    # All processes should succeed
    errors = [r for r in results if r[0] == "error"]
    if errors:
        pytest.fail(f"Some workers failed: {errors}")

    successes = [r for r in results if r[0] == "success"]
    assert len(successes) == num_workers

    # Verify the database is properly initialized
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = {row[0] for row in cursor.fetchall()}

    assert "queue_items" in tables
    assert "job_detail_dedupe" in tables
    assert "workflow_runs" in tables

    conn.close()


def test_schema_initialized_flag_prevents_redundant_work(temp_db_path: Path) -> None:
    """Test that _SCHEMA_INITIALIZED flag prevents redundant schema initialization."""
    # First call should initialize the schema
    initialize_schema()
    assert sqlite_module._SCHEMA_INITIALIZED is True

    # Record the file modification time
    mtime_after_first = temp_db_path.stat().st_mtime

    # Give a small delay to ensure any subsequent write would have different mtime
    time.sleep(0.01)

    # Second call should be a no-op due to the flag
    initialize_schema()
    initialize_schema()

    # The database file shouldn't have been modified again
    mtime_after_additional = temp_db_path.stat().st_mtime
    assert mtime_after_first == mtime_after_additional


def test_file_lock_timeout_is_configurable(tmp_path: Path) -> None:
    """Test that file lock timeout can be configured."""
    lock_path = tmp_path / "test.lock"

    start = time.monotonic()
    with _file_lock(lock_path):
        # Try to acquire with a short timeout while already holding the lock
        with pytest.raises(sqlite3.OperationalError):
            with _file_lock(lock_path, timeout=0.5):
                pass
    elapsed = time.monotonic() - start

    # Should have waited approximately 0.5 seconds (with some tolerance)
    assert elapsed >= 0.4
    assert elapsed < 2.0
