from __future__ import annotations

from pathlib import Path

import pytest

from job_scrape_application.dbos_runtime import queue as dbos_queue
from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite


def _reset_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    dbos_sqlite._CONNECTIONS.connection = None


def test_detail_queue_has_pending_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_db(tmp_path, monkeypatch)

    assert dbos_queue.detail_queue_has_pending() is False


def test_detail_queue_has_pending_true_when_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_db(tmp_path, monkeypatch)

    url = "https://example.com/jobs/1"
    result = dbos_queue.enqueue_scrape_urls({"urls": [url], "urlTypes": ["detail"]})

    assert result.get("queued") == 1
    assert dbos_queue.detail_queue_has_pending() is True


def test_detail_queue_has_pending_false_after_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_db(tmp_path, monkeypatch)

    url = "https://example.com/jobs/2"
    dbos_queue.enqueue_scrape_urls({"urls": [url], "urlTypes": ["detail"]})
    dbos_queue.complete_scrape_urls({"items": [{"url": url}], "status": "completed"})

    assert dbos_queue.detail_queue_has_pending() is False
