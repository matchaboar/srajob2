"""Pytest fixtures for workflow tests."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Generator

import pytest
from dbos import DBOS
from dbos import _sys_db_sqlite


@pytest.fixture
def reset_dbos(tmp_path: Path) -> Generator[None, None, None]:
    """Reset DBOS for testing.

    This fixture initializes DBOS with a SQLite database for testing.
    Each test gets a fresh DBOS instance.

    Usage:
        def test_my_workflow(reset_dbos):
            # DBOS is ready to use
            result = await my_workflow(...)
    """
    # Clean up any existing instance
    DBOS.destroy()

    original_sleep = _sys_db_sqlite.time.sleep

    def _fast_sleep(seconds: float) -> None:
        original_sleep(min(seconds, 0.05))

    _sys_db_sqlite.time.sleep = _fast_sleep

    # Use shared in-memory SQLite for faster tests.
    db_path = "file::memory:?cache=shared"
    sqlite_url = "sqlite:///file::memory:?cache=shared&check_same_thread=false&uri=true"

    # Set environment variables needed by app
    os.environ["DBOS_SQLITE_PATH"] = str(db_path)
    os.environ.setdefault("SPIDER_API_KEY", "test_key")
    os.environ.setdefault("CONVEX_HTTP_URL", "http://test.convex.site")

    # Initialize DBOS with test config
    config = {
        "name": "job-scrape-test",
        "system_database_url": sqlite_url,
    }
    DBOS(config=config)
    DBOS.reset_system_database()
    DBOS.launch()

    yield

    # Cleanup after test
    DBOS.destroy()
    _sys_db_sqlite.time.sleep = original_sleep


@pytest.fixture
def workflow_test(tmp_path: Path, monkeypatch: Any, reset_dbos: None) -> Any:
    """Provide a configured WorkflowTest instance with DBOS initialized.

    Usage:
        async def test_my_workflow(workflow_test):
            workflow_test.with_spidercloud_response(...)
            result = await workflow_test.run(my_workflow, ...)
            assert workflow_test.call_count("step_name") == 1
    """
    from job_scrape_application.workflows.workflow.test_utils import WorkflowTest

    return WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)
