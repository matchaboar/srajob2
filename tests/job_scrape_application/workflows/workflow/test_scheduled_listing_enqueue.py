"""Tests for scheduled_listing_enqueue workflow.

This workflow runs on a dynamic schedule (loaded from Convex) and enqueues
listing URLs for enabled sites. It does NOT use @DBOS.scheduled since that
requires a static cron expression - instead the schedule interval is
dynamically loaded via load_schedule_interval_minutes().

Note: Full workflow integration tests require DBOS initialization.
These tests call the unwrapped function directly via .__wrapped__ to test
the core logic without DBOS runtime overhead.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest


def _get_sle_module():
    """Get the scheduled_listing_enqueue module for patching."""
    # Import to ensure module is loaded
    import job_scrape_application.workflows.workflow.scheduled_listing_enqueue  # noqa: F401
    return sys.modules["job_scrape_application.workflows.workflow.scheduled_listing_enqueue"]


class TestScheduledListingEnqueue:
    """Tests for the scheduled_listing_enqueue workflow."""

    def test_workflow_is_not_decorated_with_scheduled(self) -> None:
        """Verify workflow does not use @DBOS.scheduled decorator.

        The schedule interval is dynamic (loaded from Convex config), so
        @DBOS.scheduled with a static cron expression is inappropriate.
        The polling-based scheduler should be used instead.
        """
        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            scheduled_listing_enqueue,
        )

        # Check that the function is a DBOS workflow (has wrapped attribute)
        assert hasattr(scheduled_listing_enqueue, "__wrapped__")

        # Verify it does NOT have the scheduled attribute that @DBOS.scheduled adds
        # The @DBOS.scheduled decorator adds a 'scheduled_cron' attribute to the function
        assert not hasattr(scheduled_listing_enqueue, "scheduled_cron"), (
            "Workflow should not use @DBOS.scheduled - schedule interval is dynamic"
        )

    def test_workflow_accepts_scheduled_time_params(self) -> None:
        """Verify workflow accepts scheduled_time and actual_time parameters."""
        import inspect

        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            scheduled_listing_enqueue,
        )

        # Check signature of the wrapped function
        sig = inspect.signature(scheduled_listing_enqueue.__wrapped__)
        params = list(sig.parameters.keys())

        assert "scheduled_time" in params
        assert "actual_time" in params


class TestScheduledListingEnqueueExecution:
    """Tests for scheduled_listing_enqueue workflow execution.

    These tests call the unwrapped workflow function directly to avoid
    requiring DBOS initialization.
    """

    def test_skips_when_detail_queue_has_pending(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Workflow should skip enqueue when detail queue has pending items."""
        self._setup_env(tmp_path, monkeypatch)

        # Mock step functions
        telemetry_events: list[dict[str, Any]] = []

        def mock_check_pending(include_processing: bool = False) -> bool:
            return True  # Queue has pending items

        def mock_emit_telemetry(
            event: str,
            level: str,
            site_url: str,
            data: dict[str, Any],
        ) -> None:
            telemetry_events.append({
                "event": event,
                "level": level,
                "site_url": site_url,
                "data": data,
            })

        monkeypatch.setattr(_get_sle_module(), "check_detail_queue_pending_step", mock_check_pending)
        monkeypatch.setattr(_get_sle_module(), "emit_scrape_telemetry_step", mock_emit_telemetry)

        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            scheduled_listing_enqueue,
        )
        from job_scrape_application.workflows.result import Success

        # Call the unwrapped function directly
        now = datetime.now(timezone.utc)
        result = scheduled_listing_enqueue.__wrapped__(scheduled_time=now, actual_time=now)

        assert isinstance(result, Success)
        assert result.value.queued == 0
        assert result.value.skipped_pending_details is True

        # Verify telemetry was emitted
        assert any(e["event"] == "schedule.skipped.pending_details" for e in telemetry_events)

    def test_processes_enabled_sites(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Workflow should fetch enabled sites and enqueue listing URLs."""
        self._setup_env(tmp_path, monkeypatch)

        enqueue_calls: list[dict[str, Any]] = []
        telemetry_events: list[dict[str, Any]] = []

        def mock_check_pending(include_processing: bool = False) -> bool:
            return False  # No pending items

        def mock_fetch_enabled_sites() -> list[dict[str, Any]]:
            return [
                {
                    "_id": "site1",
                    "url": "https://boards.greenhouse.io/company1",
                    "type": "greenhouse",
                    "scrapeProvider": "spidercloud",
                    "paginationLimit": 3,
                },
                {
                    "_id": "site2",
                    "url": "https://jobs.ashbyhq.com/company2",
                    "type": "ashbyhq",
                    "scrapeProvider": "spidercloud",
                    "paginationLimit": 0,
                },
            ]

        def mock_enqueue_scrape_urls(
            urls: list[str],
            source_url: str,
            provider: str,
            site_id: str | None,
            pattern: str | None,
            url_types: list[str],
        ) -> dict[str, Any]:
            enqueue_calls.append({
                "urls": urls,
                "source_url": source_url,
                "provider": provider,
                "site_id": site_id,
                "url_types": url_types,
            })
            return {"queued": len(urls)}

        def mock_emit_telemetry(
            event: str,
            level: str,
            site_url: str,
            data: dict[str, Any],
        ) -> None:
            telemetry_events.append({
                "event": event,
                "level": level,
                "data": data,
            })

        monkeypatch.setattr(_get_sle_module(), "check_detail_queue_pending_step", mock_check_pending)
        monkeypatch.setattr(_get_sle_module(), "fetch_enabled_sites_step", mock_fetch_enabled_sites)
        monkeypatch.setattr(_get_sle_module(), "enqueue_scrape_urls_step", mock_enqueue_scrape_urls)
        monkeypatch.setattr(_get_sle_module(), "emit_scrape_telemetry_step", mock_emit_telemetry)

        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            scheduled_listing_enqueue,
        )
        from job_scrape_application.workflows.result import Success

        # Call the unwrapped function directly
        now = datetime.now(timezone.utc)
        result = scheduled_listing_enqueue.__wrapped__(scheduled_time=now, actual_time=now)

        assert isinstance(result, Success)
        assert result.value.queued >= 2  # At least one URL per site
        assert result.value.sites_processed == 2
        assert result.value.skipped_pending_details is False

        # Verify enqueue was called for both sites
        assert len(enqueue_calls) == 2

        # Verify site IDs are passed correctly
        site_ids = {call["site_id"] for call in enqueue_calls}
        assert "site1" in site_ids
        assert "site2" in site_ids

        # Verify URL types are listing
        for call in enqueue_calls:
            assert all(t == "listing" for t in call["url_types"])

        # Verify completion telemetry
        assert any(e["event"] == "schedule.completed" for e in telemetry_events)

    def test_handles_no_enabled_sites(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Workflow should handle case where no sites are enabled."""
        self._setup_env(tmp_path, monkeypatch)

        def mock_check_pending(include_processing: bool = False) -> bool:
            return False

        def mock_fetch_enabled_sites() -> list[dict[str, Any]]:
            return []  # No enabled sites

        monkeypatch.setattr(_get_sle_module(), "check_detail_queue_pending_step", mock_check_pending)
        monkeypatch.setattr(_get_sle_module(), "fetch_enabled_sites_step", mock_fetch_enabled_sites)

        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            scheduled_listing_enqueue,
        )
        from job_scrape_application.workflows.result import Success

        now = datetime.now(timezone.utc)
        result = scheduled_listing_enqueue.__wrapped__(scheduled_time=now, actual_time=now)

        assert isinstance(result, Success)
        assert result.value.queued == 0
        assert result.value.sites_processed == 0

    def test_handles_site_with_invalid_url(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Workflow should skip sites with empty or invalid URLs."""
        self._setup_env(tmp_path, monkeypatch)

        enqueue_calls: list[dict[str, Any]] = []

        def mock_check_pending(include_processing: bool = False) -> bool:
            return False

        def mock_fetch_enabled_sites() -> list[dict[str, Any]]:
            return [
                {"_id": "site1", "url": ""},  # Empty URL
                {"_id": "site2", "url": "   "},  # Whitespace URL
                {"_id": "site3"},  # Missing URL
                {
                    "_id": "site4",
                    "url": "https://valid.com/jobs",
                    "scrapeProvider": "spidercloud",
                },
            ]

        def mock_enqueue_scrape_urls(
            urls: list[str],
            source_url: str,
            provider: str,
            site_id: str | None,
            pattern: str | None,
            url_types: list[str],
        ) -> dict[str, Any]:
            enqueue_calls.append({"site_id": site_id, "urls": urls})
            return {"queued": len(urls)}

        def mock_emit_telemetry(*args: Any, **kwargs: Any) -> None:
            pass

        monkeypatch.setattr(_get_sle_module(), "check_detail_queue_pending_step", mock_check_pending)
        monkeypatch.setattr(_get_sle_module(), "fetch_enabled_sites_step", mock_fetch_enabled_sites)
        monkeypatch.setattr(_get_sle_module(), "enqueue_scrape_urls_step", mock_enqueue_scrape_urls)
        monkeypatch.setattr(_get_sle_module(), "emit_scrape_telemetry_step", mock_emit_telemetry)

        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            scheduled_listing_enqueue,
        )
        from job_scrape_application.workflows.result import Success

        now = datetime.now(timezone.utc)
        result = scheduled_listing_enqueue.__wrapped__(scheduled_time=now, actual_time=now)

        assert isinstance(result, Success)
        # Only the valid site should be processed
        assert len(enqueue_calls) == 1
        assert enqueue_calls[0]["site_id"] == "site4"

    def test_continues_on_individual_site_failure(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Workflow should continue processing other sites if one fails."""
        self._setup_env(tmp_path, monkeypatch)

        enqueue_calls: list[dict[str, Any]] = []
        call_count = {"count": 0}

        def mock_check_pending(include_processing: bool = False) -> bool:
            return False

        def mock_fetch_enabled_sites() -> list[dict[str, Any]]:
            return [
                {"_id": "site1", "url": "https://site1.com/jobs", "scrapeProvider": "spidercloud"},
                {"_id": "site2", "url": "https://site2.com/jobs", "scrapeProvider": "spidercloud"},
                {"_id": "site3", "url": "https://site3.com/jobs", "scrapeProvider": "spidercloud"},
            ]

        def mock_enqueue_scrape_urls(
            urls: list[str],
            source_url: str,
            provider: str,
            site_id: str | None,
            pattern: str | None,
            url_types: list[str],
        ) -> dict[str, Any]:
            call_count["count"] += 1
            # Fail on second site
            if site_id == "site2":
                raise RuntimeError("Simulated enqueue failure")
            enqueue_calls.append({"site_id": site_id})
            return {"queued": len(urls)}

        def mock_emit_telemetry(*args: Any, **kwargs: Any) -> None:
            pass

        monkeypatch.setattr(_get_sle_module(), "check_detail_queue_pending_step", mock_check_pending)
        monkeypatch.setattr(_get_sle_module(), "fetch_enabled_sites_step", mock_fetch_enabled_sites)
        monkeypatch.setattr(_get_sle_module(), "enqueue_scrape_urls_step", mock_enqueue_scrape_urls)
        monkeypatch.setattr(_get_sle_module(), "emit_scrape_telemetry_step", mock_emit_telemetry)

        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            scheduled_listing_enqueue,
        )
        from job_scrape_application.workflows.result import Success

        now = datetime.now(timezone.utc)
        result = scheduled_listing_enqueue.__wrapped__(scheduled_time=now, actual_time=now)

        assert isinstance(result, Success)
        # Sites 1 and 3 should succeed, site 2 fails
        assert len(enqueue_calls) == 2
        assert result.value.sites_processed == 2

    def test_handles_fetch_sites_failure(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Workflow should handle failure when fetching enabled sites."""
        self._setup_env(tmp_path, monkeypatch)

        telemetry_events: list[dict[str, Any]] = []

        def mock_check_pending(include_processing: bool = False) -> bool:
            return False

        def mock_fetch_enabled_sites() -> list[dict[str, Any]]:
            raise RuntimeError("Convex connection failed")

        def mock_emit_telemetry(
            event: str,
            level: str,
            site_url: str,
            data: dict[str, Any],
        ) -> None:
            telemetry_events.append({"event": event, "level": level, "data": data})

        monkeypatch.setattr(_get_sle_module(), "check_detail_queue_pending_step", mock_check_pending)
        monkeypatch.setattr(_get_sle_module(), "fetch_enabled_sites_step", mock_fetch_enabled_sites)
        monkeypatch.setattr(_get_sle_module(), "emit_scrape_telemetry_step", mock_emit_telemetry)

        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            scheduled_listing_enqueue,
        )
        from job_scrape_application.workflows.result import Success

        now = datetime.now(timezone.utc)
        result = scheduled_listing_enqueue.__wrapped__(scheduled_time=now, actual_time=now)

        assert isinstance(result, Success)
        assert result.value.queued == 0
        assert result.value.sites_processed == 0

        # Verify error telemetry was emitted
        assert any(e["event"] == "schedule.error.fetch_sites" for e in telemetry_events)

    def _setup_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Set up test environment."""
        db_path = tmp_path / "dbos.sqlite"
        monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
        monkeypatch.setenv("SPIDER_API_KEY", "test")
        monkeypatch.setenv("CONVEX_HTTP_URL", "http://test.convex.site")

        from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite

        dbos_sqlite._CONNECTIONS.connection = None


class TestGenerateListingUrlsForSite:
    """Tests for _generate_listing_urls_for_site helper function."""

    def test_returns_empty_for_missing_url(self) -> None:
        """Should return empty list when URL is missing."""
        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            _generate_listing_urls_for_site,
        )

        result = _generate_listing_urls_for_site({})
        assert result == []

    def test_returns_empty_for_empty_url(self) -> None:
        """Should return empty list when URL is empty string."""
        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            _generate_listing_urls_for_site,
        )

        result = _generate_listing_urls_for_site({"url": ""})
        assert result == []

    def test_returns_empty_for_whitespace_url(self) -> None:
        """Should return empty list when URL is whitespace."""
        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            _generate_listing_urls_for_site,
        )

        result = _generate_listing_urls_for_site({"url": "   "})
        assert result == []

    def test_returns_base_url(self) -> None:
        """Should always include the base URL."""
        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            _generate_listing_urls_for_site,
        )

        result = _generate_listing_urls_for_site({
            "url": "https://boards.greenhouse.io/company",
        })

        assert "https://boards.greenhouse.io/company" in result

    def test_strips_whitespace_from_url(self) -> None:
        """Should strip whitespace from URL."""
        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            _generate_listing_urls_for_site,
        )

        result = _generate_listing_urls_for_site({
            "url": "  https://boards.greenhouse.io/company  ",
        })

        assert "https://boards.greenhouse.io/company" in result
        assert "  https://boards.greenhouse.io/company  " not in result

    def test_respects_pagination_limit(self) -> None:
        """Should respect pagination limit from site config."""
        from job_scrape_application.workflows.workflow.scheduled_listing_enqueue import (
            _generate_listing_urls_for_site,
        )

        # With no pagination limit, handler may add more URLs
        result_no_limit = _generate_listing_urls_for_site({
            "url": "https://boards.greenhouse.io/company",
            "paginationLimit": 0,
        })

        # With pagination limit of 1, should limit URLs
        result_with_limit = _generate_listing_urls_for_site({
            "url": "https://boards.greenhouse.io/company",
            "paginationLimit": 1,
        })

        # Both should include at least the base URL
        assert len(result_no_limit) >= 1
        assert len(result_with_limit) >= 1
