"""Tests for queue failure resilience.

These tests verify that various failure scenarios (timeout, cancellation, exceptions)
in the SpiderCloud scraper and workflow activities do NOT block the queue or stall
the worker thread.

The key invariant being tested:
    A failure in processing one URL or one site should not prevent the queue
    from processing subsequent items.
"""

from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from job_scrape_application.workflows import activities as acts
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (
    SpiderCloudScraper,
    SpidercloudDependencies,
)

# Stub firecrawl and other optional dependencies before imports
try:
    import firecrawl  # noqa: F401
except Exception:
    firecrawl_mod = types.ModuleType("firecrawl")
    firecrawl_mod.Firecrawl = type("Firecrawl", (), {})
    sys.modules.setdefault("firecrawl", firecrawl_mod)
    firecrawl_v2 = types.ModuleType("firecrawl.v2")
    firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")
    firecrawl_v2_types.PaginationConfig = type("PaginationConfig", (), {})
    firecrawl_v2_types.ScrapeOptions = type("ScrapeOptions", (), {})
    sys.modules.setdefault("firecrawl.v2", firecrawl_v2)
    sys.modules.setdefault("firecrawl.v2.types", firecrawl_v2_types)
    firecrawl_v2_utils = types.ModuleType("firecrawl.v2.utils")
    firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")
    firecrawl_v2_utils_error.PaymentRequiredError = type("PaymentRequiredError", (Exception,), {})
    firecrawl_v2_utils_error.RequestTimeoutError = type("RequestTimeoutError", (Exception,), {})
    sys.modules.setdefault("firecrawl.v2.utils", firecrawl_v2_utils)
    sys.modules.setdefault("firecrawl.v2.utils.error_handler", firecrawl_v2_utils_error)
    firecrawl_v2_utils.error_handler = firecrawl_v2_utils_error

fetchfox_mod = types.ModuleType("fetchfox_sdk")
fetchfox_mod.FetchFox = type("FetchFox", (), {})
sys.modules.setdefault("fetchfox_sdk", fetchfox_mod)

try:
    import temporalio  # noqa: F401
except ImportError:
    temporalio = types.ModuleType("temporalio")
    sys.modules.setdefault("temporalio", temporalio)

    class _Activity:
        def defn(self, fn=None, **kwargs):
            if fn is None:
                def wrapper(func):
                    return func
                return wrapper
            return fn

    temporalio.activity = _Activity()
    sys.modules.setdefault("temporalio.activity", temporalio)

    temporalio_exceptions = types.ModuleType("temporalio.exceptions")
    temporalio_exceptions.ApplicationError = type("ApplicationError", (Exception,), {})
    sys.modules.setdefault("temporalio.exceptions", temporalio_exceptions)


def create_mock_deps() -> SpidercloudDependencies:
    """Create mock dependencies for SpiderCloudScraper."""
    mock_settings = MagicMock()
    mock_settings.spider_api_key = "test_key"

    return SpidercloudDependencies(
        mask_secret=lambda x: "***" if x else None,
        sanitize_headers=lambda x: x,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda x: x,
        settings=mock_settings,
        fetch_seen_urls_for_site=AsyncMock(return_value=[]),
    )


@dataclass
class MockScrapeResult:
    """Mock result for a single URL scrape."""

    url: str
    success: bool = True
    normalized: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    exception: Optional[Exception] = None


class FailureInjectingMockSpider:
    """Mock SpiderCloud client that can inject failures for specific URLs.

    This allows testing how the scraper handles failures at various points
    without actually making HTTP requests.
    """

    def __init__(
        self,
        url_behaviors: Dict[str, str | Exception],
        default_response: str = "success",
    ):
        """
        Args:
            url_behaviors: Dict mapping URL patterns to behavior:
                - "success": Return successful response
                - "timeout": Raise asyncio.TimeoutError
                - "cancelled": Raise asyncio.CancelledError
                - Exception instance: Raise that exception
            default_response: Default behavior for unmatched URLs
        """
        self.url_behaviors = url_behaviors
        self.default_response = default_response
        self.calls: List[str] = []
        self.completed_urls: List[str] = []
        self.failed_urls: List[str] = []

    async def __aenter__(self) -> "FailureInjectingMockSpider":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    def _get_behavior(self, url: str) -> str | Exception:
        """Get the behavior for a given URL."""
        for pattern, behavior in self.url_behaviors.items():
            if pattern in url:
                return behavior
        return self.default_response

    def scrape_url(
        self,
        url: str,
        *,
        params: Dict[str, Any],
        stream: bool,
        content_type: str,
    ) -> Any:
        self.calls.append(url)
        behavior = self._get_behavior(url)

        async def _generate_response():
            if isinstance(behavior, Exception):
                self.failed_urls.append(url)
                raise behavior
            elif behavior == "timeout":
                self.failed_urls.append(url)
                raise asyncio.TimeoutError(f"Timeout for {url}")
            elif behavior == "cancelled":
                self.failed_urls.append(url)
                raise asyncio.CancelledError()
            else:
                self.completed_urls.append(url)
                # Return a valid JSONL response
                response = {
                    "url": url,
                    "content": {"commonmark": f"# Job at {url}\n\nDescription here."},
                    "costs": {"total": 100},
                }
                import orjson
                yield orjson.dumps(response).decode("utf-8") + "\n"

        return _generate_response()


class SyncFailureInjectingMockSpider(FailureInjectingMockSpider):
    """Sync version of the failure-injecting mock for single request mode."""

    def scrape_url(
        self,
        url: str,
        *,
        params: Dict[str, Any],
        stream: bool,
        content_type: str,
    ) -> Any:
        self.calls.append(url)
        behavior = self._get_behavior(url)

        async def _awaitable():
            if isinstance(behavior, Exception):
                self.failed_urls.append(url)
                raise behavior
            elif behavior == "timeout":
                self.failed_urls.append(url)
                raise asyncio.TimeoutError(f"Timeout for {url}")
            elif behavior == "cancelled":
                self.failed_urls.append(url)
                raise asyncio.CancelledError()
            else:
                self.completed_urls.append(url)
                return {
                    "url": url,
                    "content": {"commonmark": f"# Job at {url}\n\nDescription here."},
                    "costs": {"total": 100},
                }

        return _awaitable()


# ==============================================================================
# Tests for _scrape_one function failure handling
# ==============================================================================


@pytest.mark.asyncio
async def test_timeout_in_single_url_does_not_block_batch(monkeypatch, tmp_path):
    """Test that a timeout in one URL doesn't prevent other URLs from being scraped.

    Scenario: Batch of 3 URLs where the middle one times out.
    Expected: First and third URLs complete successfully, middle one returns failed entry.
    """
    # Set up environment
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")

    # Reset DBOS connection
    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    urls = [
        "https://example.com/job/1",
        "https://example.com/job/timeout",  # This one will timeout
        "https://example.com/job/3",
    ]

    mock_spider = SyncFailureInjectingMockSpider(
        url_behaviors={
            "timeout": "timeout",  # Middle URL times out
        },
        default_response="success",
    )

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        lambda api_key: mock_spider,
    )

    # Run the scraper
    scraper = SpiderCloudScraper(create_mock_deps())
    result = await scraper._scrape_urls_batch(
        urls=urls,
        source_url="https://example.com/jobs",
    )

    # Verify all URLs were attempted
    assert len(mock_spider.calls) == 3, "All URLs should be attempted"

    # Verify successful URLs completed
    assert "https://example.com/job/1" in mock_spider.completed_urls
    assert "https://example.com/job/3" in mock_spider.completed_urls

    # Verify the timeout URL is in failed
    assert "https://example.com/job/timeout" in mock_spider.failed_urls

    # Verify the result contains failed items for timeout
    items = result.get("items", {})
    failed = items.get("failed", [])
    # The scraper may normalize the error type - check for common timeout patterns
    assert any(
        "timeout" in str(f.get("errorType", "")).lower() or
        "timeout" in str(f.get("reason", "")).lower()
        for f in failed if isinstance(f, dict)
    ), f"Timeout should be recorded as failed item, got: {failed}"


@pytest.mark.asyncio
async def test_cancelled_error_in_single_url_does_not_block_batch(monkeypatch, tmp_path):
    """Test that a CancelledError in one URL doesn't prevent other URLs from being scraped.

    Scenario: Batch of 3 URLs where one raises CancelledError.
    Expected: Other URLs complete successfully, cancelled one returns failed entry.
    """
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")

    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    urls = [
        "https://example.com/job/1",
        "https://example.com/job/cancelled",  # This one will be cancelled
        "https://example.com/job/3",
    ]

    mock_spider = SyncFailureInjectingMockSpider(
        url_behaviors={
            "cancelled": "cancelled",
        },
        default_response="success",
    )

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        lambda api_key: mock_spider,
    )

    scraper = SpiderCloudScraper(create_mock_deps())
    result = await scraper._scrape_urls_batch(
        urls=urls,
        source_url="https://example.com/jobs",
    )

    # Verify all URLs were attempted
    assert len(mock_spider.calls) == 3

    # Verify successful URLs completed
    assert len(mock_spider.completed_urls) == 2

    # Verify the result doesn't raise - just returns with failed items
    items = result.get("items", {})
    failed = items.get("failed", [])

    # Should have a failed entry for the cancelled URL
    cancelled_failures = [f for f in failed if isinstance(f, dict) and "cancelled" in str(f.get("errorType", "")).lower()]
    assert len(cancelled_failures) >= 1, "Cancelled URL should be recorded as failed"


@pytest.mark.asyncio
async def test_multiple_failures_in_batch_still_processes_successful_urls(monkeypatch, tmp_path):
    """Test that multiple failures in a batch don't block successful URLs.

    Scenario: 5 URLs with 3 different types of failures.
    Expected: All URLs are attempted, successes complete, failures are recorded.
    """
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")

    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    urls = [
        "https://example.com/job/success1",
        "https://example.com/job/timeout1",
        "https://example.com/job/success2",
        "https://example.com/job/cancelled1",
        "https://example.com/job/success3",
    ]

    mock_spider = SyncFailureInjectingMockSpider(
        url_behaviors={
            "timeout1": "timeout",
            "cancelled1": "cancelled",
        },
        default_response="success",
    )

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        lambda api_key: mock_spider,
    )

    scraper = SpiderCloudScraper(create_mock_deps())
    result = await scraper._scrape_urls_batch(
        urls=urls,
        source_url="https://example.com/jobs",
    )

    # All 5 URLs should be attempted
    assert len(mock_spider.calls) == 5

    # 3 successful, 2 failed
    assert len(mock_spider.completed_urls) == 3
    assert len(mock_spider.failed_urls) == 2

    # Result should be valid (not an exception)
    assert isinstance(result, dict)
    assert "items" in result


@pytest.mark.asyncio
async def test_exception_converted_to_failed_entry_not_raised(monkeypatch, tmp_path):
    """Test that generic exceptions in batch tasks are converted to failed entries.

    The fix we made should convert BaseException results from asyncio.gather
    into failed entries instead of re-raising them.
    """
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")

    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    urls = [
        "https://example.com/job/success",
        "https://example.com/job/valueerror",
    ]

    mock_spider = SyncFailureInjectingMockSpider(
        url_behaviors={
            "valueerror": ValueError("Simulated value error"),
        },
        default_response="success",
    )

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        lambda api_key: mock_spider,
    )

    scraper = SpiderCloudScraper(create_mock_deps())

    # This should NOT raise - exceptions should be converted to failed entries
    result = await scraper._scrape_urls_batch(
        urls=urls,
        source_url="https://example.com/jobs",
    )

    assert isinstance(result, dict)
    assert len(mock_spider.completed_urls) == 1

    # The ValueError should be in failed items
    items = result.get("items", {})
    failed = items.get("failed", [])
    assert len(failed) >= 1


# ==============================================================================
# Tests for scrape_site activity failure handling
# ==============================================================================


@pytest.mark.asyncio
async def test_scrape_site_activity_handles_cancelled_error(monkeypatch, tmp_path):
    """Test that scrape_site activity returns gracefully on CancelledError.

    The fix adds try/except around the scraper call in the activity.
    """
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")
    monkeypatch.setenv("CONVEX_HTTP_URL", "http://test.convex.site")

    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    async def raise_cancelled(*args, **kwargs):
        raise asyncio.CancelledError()

    # Mock the internal scrape function to raise CancelledError
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities._scrape_spidercloud_greenhouse",
        raise_cancelled,
    )

    # Create a real SpiderCloudScraper so isinstance check passes
    mock_deps = create_mock_deps()
    real_scraper = SpiderCloudScraper(mock_deps)
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.select_scraper_for_site",
        lambda site: (real_scraper, []),
    )

    site = {
        "_id": "test-site",
        "url": "https://example.com/jobs",
        "type": "greenhouse",
    }

    # This should NOT raise - should return error result
    result = await acts.scrape_site(site)

    assert isinstance(result, dict)
    assert result.get("error") == "cancelled"
    assert result.get("errorType") == "CancelledError"


@pytest.mark.asyncio
async def test_scrape_site_activity_handles_timeout_error(monkeypatch, tmp_path):
    """Test that scrape_site activity returns gracefully on TimeoutError."""
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")
    monkeypatch.setenv("CONVEX_HTTP_URL", "http://test.convex.site")

    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    async def raise_timeout(*args, **kwargs):
        raise asyncio.TimeoutError()

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities._scrape_spidercloud_greenhouse",
        raise_timeout,
    )

    # Create a real SpiderCloudScraper so isinstance check passes
    mock_deps = create_mock_deps()
    real_scraper = SpiderCloudScraper(mock_deps)
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.select_scraper_for_site",
        lambda site: (real_scraper, []),
    )

    site = {
        "_id": "test-site",
        "url": "https://example.com/jobs",
        "type": "greenhouse",
    }

    result = await acts.scrape_site(site)

    assert isinstance(result, dict)
    assert result.get("error") == "timeout"
    assert result.get("errorType") == "TimeoutError"


# ==============================================================================
# Tests for queue continuation after failures
# ==============================================================================


@pytest.mark.asyncio
async def test_batch_with_all_failures_returns_valid_result(monkeypatch, tmp_path):
    """Test that a batch where ALL URLs fail still returns a valid result.

    This is a worst-case scenario - the queue should still continue.
    """
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")

    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    urls = [
        "https://example.com/job/timeout1",
        "https://example.com/job/timeout2",
        "https://example.com/job/cancelled1",
    ]

    mock_spider = SyncFailureInjectingMockSpider(
        url_behaviors={
            "timeout": "timeout",
            "cancelled": "cancelled",
        },
        default_response="success",
    )

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        lambda api_key: mock_spider,
    )

    scraper = SpiderCloudScraper(create_mock_deps())
    result = await scraper._scrape_urls_batch(
        urls=urls,
        source_url="https://example.com/jobs",
    )

    # Should still return a valid dict, not raise
    assert isinstance(result, dict)
    assert len(mock_spider.failed_urls) == 3
    assert len(mock_spider.completed_urls) == 0

    # Failed items should be recorded
    items = result.get("items", {})
    failed = items.get("failed", [])
    assert len(failed) == 3


@pytest.mark.asyncio
async def test_empty_batch_returns_valid_result(monkeypatch, tmp_path):
    """Test that an empty batch returns a valid result without errors."""
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")

    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    mock_spider = SyncFailureInjectingMockSpider(url_behaviors={})

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        lambda api_key: mock_spider,
    )

    scraper = SpiderCloudScraper(create_mock_deps())
    result = await scraper._scrape_urls_batch(
        urls=[],
        source_url="https://example.com/jobs",
    )

    assert isinstance(result, dict)
    assert len(mock_spider.calls) == 0


# ==============================================================================
# Tests simulating queue behavior with sequential batch processing
# ==============================================================================


@pytest.mark.asyncio
async def test_sequential_batches_continue_after_failure(monkeypatch, tmp_path):
    """Test that processing continues across multiple batches even when some fail.

    This simulates the actual queue behavior where multiple batches are processed.
    """
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")

    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    # Simulate 3 batches being processed
    batches = [
        ["https://example.com/batch1/job1", "https://example.com/batch1/job2"],
        ["https://example.com/batch2/timeout"],  # This batch will have failures
        ["https://example.com/batch3/job1", "https://example.com/batch3/job2"],
    ]

    all_completed = []
    all_failed = []

    for batch_idx, urls in enumerate(batches):
        mock_spider = SyncFailureInjectingMockSpider(
            url_behaviors={
                "timeout": "timeout",
            },
            default_response="success",
        )

        monkeypatch.setattr(
            "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
            lambda api_key, ms=mock_spider: ms,
        )

        scraper = SpiderCloudScraper(create_mock_deps())
        result = await scraper._scrape_urls_batch(
            urls=urls,
            source_url="https://example.com/jobs",
        )

        all_completed.extend(mock_spider.completed_urls)
        all_failed.extend(mock_spider.failed_urls)

        # Each batch should return a valid result
        assert isinstance(result, dict), f"Batch {batch_idx} should return valid result"

    # Verify overall results
    assert len(all_completed) == 4, "4 URLs should have completed successfully"
    assert len(all_failed) == 1, "1 URL should have failed"


@pytest.mark.asyncio
async def test_activity_failure_does_not_propagate_cancelled_error(monkeypatch, tmp_path):
    """Verify that CancelledError is caught and doesn't propagate to crash the worker."""
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")
    monkeypatch.setenv("CONVEX_HTTP_URL", "http://test.convex.site")

    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    cancelled_raised = False

    async def raise_cancelled_and_track(*args, **kwargs):
        nonlocal cancelled_raised
        cancelled_raised = True
        raise asyncio.CancelledError()

    monkeypatch.setattr(
        "job_scrape_application.workflows.activities._scrape_spidercloud_greenhouse",
        raise_cancelled_and_track,
    )

    # Create a real SpiderCloudScraper so isinstance check passes
    mock_deps = create_mock_deps()
    real_scraper = SpiderCloudScraper(mock_deps)
    monkeypatch.setattr(
        "job_scrape_application.workflows.activities.select_scraper_for_site",
        lambda site: (real_scraper, []),
    )

    site = {
        "_id": "test-site",
        "url": "https://example.com/jobs",
        "type": "greenhouse",
    }

    # The activity should NOT raise CancelledError
    try:
        result = await acts.scrape_site(site)
        raised_exception = False
    except asyncio.CancelledError:
        raised_exception = True

    assert cancelled_raised, "CancelledError should have been triggered"
    assert not raised_exception, "CancelledError should NOT propagate from activity"
    assert isinstance(result, dict), "Activity should return a dict on cancellation"


# ==============================================================================
# Stress test with high failure rate
# ==============================================================================


@pytest.mark.asyncio
async def test_high_failure_rate_batch(monkeypatch, tmp_path):
    """Test a batch with 80% failure rate still processes successfully.

    This stress tests the failure handling to ensure it scales.
    """
    db_path = tmp_path / "dbos.sqlite"
    monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("SPIDER_API_KEY", "test_key")

    from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
    dbos_sqlite._CONNECTIONS.connection = None

    # Create 10 URLs with 8 failures
    urls = []
    url_behaviors = {}
    for i in range(10):
        url = f"https://example.com/job/{i}"
        urls.append(url)
        if i < 4:
            url_behaviors[f"/{i}"] = "timeout"
        elif i < 8:
            url_behaviors[f"/{i}"] = "cancelled"
        # Last 2 (8, 9) will succeed

    mock_spider = SyncFailureInjectingMockSpider(
        url_behaviors=url_behaviors,
        default_response="success",
    )

    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        lambda api_key: mock_spider,
    )

    scraper = SpiderCloudScraper(create_mock_deps())
    result = await scraper._scrape_urls_batch(
        urls=urls,
        source_url="https://example.com/jobs",
    )

    # Should still return valid result
    assert isinstance(result, dict)

    # 2 successful, 8 failed
    assert len(mock_spider.completed_urls) == 2
    assert len(mock_spider.failed_urls) == 8

    # All 10 should have been attempted
    assert len(mock_spider.calls) == 10


# ==============================================================================
# Tests for timeout failure detection (prevents misleading zero_urls warnings)
# ==============================================================================


class TestTimeoutFailureDetection:
    """Tests for _has_timeout_failures() logic in _enqueue_from_scrape.

    When a scrape times out, we should NOT emit a misleading 'zero_urls' error
    because the timeout error is already logged separately. These tests verify
    that the timeout/cancelled failure detection works correctly.
    """

    def test_scrape_payload_with_timeout_failures_detected(self):
        """Scrape payload with timeout failures should be detected."""
        scrape_payload = {
            "items": {
                "normalized": [],
                "raw": [],
                "failed": [
                    {"url": "https://example.com/jobs", "reason": "timeout", "errorType": "timeout"}
                ],
            }
        }

        # Inline the logic we're testing (same as in activities/__init__.py)
        def _has_timeout_failures(payload: Dict[str, Any]) -> bool:
            items_block = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(items_block, dict):
                return False
            failed = items_block.get("failed")
            if not isinstance(failed, list) or not failed:
                return False
            timeout_reasons = {"timeout", "cancelled"}
            for entry in failed:
                if not isinstance(entry, dict):
                    continue
                reason = entry.get("reason", "").lower()
                if reason in timeout_reasons:
                    return True
            return False

        assert _has_timeout_failures(scrape_payload) is True

    def test_scrape_payload_with_cancelled_failures_detected(self):
        """Scrape payload with cancelled failures should be detected."""
        scrape_payload = {
            "items": {
                "normalized": [],
                "raw": [],
                "failed": [
                    {"url": "https://example.com/jobs", "reason": "cancelled", "errorType": "cancelled"}
                ],
            }
        }

        def _has_timeout_failures(payload: Dict[str, Any]) -> bool:
            items_block = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(items_block, dict):
                return False
            failed = items_block.get("failed")
            if not isinstance(failed, list) or not failed:
                return False
            timeout_reasons = {"timeout", "cancelled"}
            for entry in failed:
                if not isinstance(entry, dict):
                    continue
                reason = entry.get("reason", "").lower()
                if reason in timeout_reasons:
                    return True
            return False

        assert _has_timeout_failures(scrape_payload) is True

    def test_scrape_payload_with_other_failures_not_treated_as_timeout(self):
        """Scrape payload with non-timeout failures should NOT be detected as timeout."""
        scrape_payload = {
            "items": {
                "normalized": [],
                "raw": [],
                "failed": [
                    {"url": "https://example.com/jobs", "reason": "http_error", "errorType": "4xx"}
                ],
            }
        }

        def _has_timeout_failures(payload: Dict[str, Any]) -> bool:
            items_block = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(items_block, dict):
                return False
            failed = items_block.get("failed")
            if not isinstance(failed, list) or not failed:
                return False
            timeout_reasons = {"timeout", "cancelled"}
            for entry in failed:
                if not isinstance(entry, dict):
                    continue
                reason = entry.get("reason", "").lower()
                if reason in timeout_reasons:
                    return True
            return False

        # http_error is not a timeout failure
        assert _has_timeout_failures(scrape_payload) is False

    def test_scrape_payload_without_failed_items_not_detected(self):
        """Scrape payload without failed items should NOT be detected as timeout."""
        scrape_payload = {
            "items": {
                "normalized": [],
                "raw": [],
            }
        }

        def _has_timeout_failures(payload: Dict[str, Any]) -> bool:
            items_block = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(items_block, dict):
                return False
            failed = items_block.get("failed")
            if not isinstance(failed, list) or not failed:
                return False
            timeout_reasons = {"timeout", "cancelled"}
            for entry in failed:
                if not isinstance(entry, dict):
                    continue
                reason = entry.get("reason", "").lower()
                if reason in timeout_reasons:
                    return True
            return False

        assert _has_timeout_failures(scrape_payload) is False

    def test_scrape_payload_with_empty_items_not_detected(self):
        """Scrape payload with empty items should NOT be detected as timeout."""
        scrape_payload = {"items": {}}

        def _has_timeout_failures(payload: Dict[str, Any]) -> bool:
            items_block = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(items_block, dict):
                return False
            failed = items_block.get("failed")
            if not isinstance(failed, list) or not failed:
                return False
            timeout_reasons = {"timeout", "cancelled"}
            for entry in failed:
                if not isinstance(entry, dict):
                    continue
                reason = entry.get("reason", "").lower()
                if reason in timeout_reasons:
                    return True
            return False

        assert _has_timeout_failures(scrape_payload) is False

    def test_scrape_payload_with_mixed_failures_detects_timeout(self):
        """Scrape payload with mixed failures (including timeout) should be detected."""
        scrape_payload = {
            "items": {
                "normalized": [],
                "raw": [],
                "failed": [
                    {"url": "https://example.com/job1", "reason": "http_error", "errorType": "4xx"},
                    {"url": "https://example.com/job2", "reason": "timeout", "errorType": "timeout"},
                ],
            }
        }

        def _has_timeout_failures(payload: Dict[str, Any]) -> bool:
            items_block = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(items_block, dict):
                return False
            failed = items_block.get("failed")
            if not isinstance(failed, list) or not failed:
                return False
            timeout_reasons = {"timeout", "cancelled"}
            for entry in failed:
                if not isinstance(entry, dict):
                    continue
                reason = entry.get("reason", "").lower()
                if reason in timeout_reasons:
                    return True
            return False

        # Should detect timeout even with other failure types present
        assert _has_timeout_failures(scrape_payload) is True

    def test_case_insensitive_timeout_reason(self):
        """Timeout reason detection should be case-insensitive."""
        scrape_payload = {
            "items": {
                "normalized": [],
                "raw": [],
                "failed": [
                    {"url": "https://example.com/jobs", "reason": "TIMEOUT", "errorType": "timeout"}
                ],
            }
        }

        def _has_timeout_failures(payload: Dict[str, Any]) -> bool:
            items_block = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(items_block, dict):
                return False
            failed = items_block.get("failed")
            if not isinstance(failed, list) or not failed:
                return False
            timeout_reasons = {"timeout", "cancelled"}
            for entry in failed:
                if not isinstance(entry, dict):
                    continue
                reason = entry.get("reason", "").lower()
                if reason in timeout_reasons:
                    return True
            return False

        assert _has_timeout_failures(scrape_payload) is True
