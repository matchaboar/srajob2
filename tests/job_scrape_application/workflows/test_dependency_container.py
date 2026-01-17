"""Tests for the core DependencyContainer and mock implementations."""

from __future__ import annotations


import pytest


from job_scrape_application.workflows.core import (
    DependencyContainer,
    MockConvexFunctions,
    MockQueueService,
    MockSettings,
    MockSpiderClient,
)


class TestMockConvexFunctions:
    """Tests for MockConvexFunctions."""

    def test_query_returns_fixture_data(self) -> None:
        """Query should return configured fixture data."""
        mock = MockConvexFunctions(
            query_fixtures={
                "router:listSites": [{"id": "abc", "name": "Test Site"}],
            }
        )

        result = mock.query("router:listSites", {})

        assert result == [{"id": "abc", "name": "Test Site"}]

    def test_query_captures_calls(self) -> None:
        """Query should capture all calls."""
        captured: list = []
        mock = MockConvexFunctions(captured_queries=captured)

        mock.query("router:listSites", {"enabledOnly": True})
        mock.query("jobs:getJobById", {"id": "xyz"})

        assert len(captured) == 2
        assert captured[0]["name"] == "router:listSites"
        assert captured[0]["args"] == {"enabledOnly": True}
        assert captured[1]["name"] == "jobs:getJobById"

    def test_mutation_returns_fixture_data(self) -> None:
        """Mutation should return configured fixture data."""
        mock = MockConvexFunctions(
            mutation_fixtures={
                "router:leaseSite": {"id": "site123", "leased": True},
            }
        )

        result = mock.mutation("router:leaseSite", {"id": "site123"})

        assert result == {"id": "site123", "leased": True}

    def test_callable_fixture_for_dynamic_responses(self) -> None:
        """Fixtures can be callable for dynamic responses."""
        def dynamic_fixture(args):
            site_id = (args or {}).get("siteId", "default")
            return {"id": site_id, "urls": ["url1", "url2"]}

        mock = MockConvexFunctions(
            query_fixtures={
                "jobs:getSeenUrls": dynamic_fixture,
            }
        )

        result1 = mock.query("jobs:getSeenUrls", {"siteId": "abc"})
        result2 = mock.query("jobs:getSeenUrls", {"siteId": "xyz"})

        assert result1["id"] == "abc"
        assert result2["id"] == "xyz"


class TestMockQueueService:
    """Tests for MockQueueService."""

    def test_enqueue_adds_urls_to_queue(self) -> None:
        """Enqueue should add URLs to the queue."""
        queue = MockQueueService()

        result = queue.enqueue_scrape_urls({
            "urls": ["https://example.com/job1", "https://example.com/job2"],
            "siteId": "test-site",
            "provider": "spidercloud",
        })

        assert result["added"] == 2
        assert result["skipped"] == 0

    def test_enqueue_deduplicates_urls(self) -> None:
        """Enqueue should skip duplicate URLs."""
        queue = MockQueueService()

        queue.enqueue_scrape_urls({
            "urls": ["https://example.com/job1"],
            "siteId": "test-site",
        })

        result = queue.enqueue_scrape_urls({
            "urls": ["https://example.com/job1", "https://example.com/job2"],
            "siteId": "test-site",
        })

        assert result["added"] == 1
        assert result["skipped"] == 1

    def test_lease_batch_returns_urls(self) -> None:
        """Lease batch should return pending URLs."""
        queue = MockQueueService()
        queue.enqueue_scrape_urls({
            "urls": ["https://example.com/job1", "https://example.com/job2"],
            "siteId": "test-site",
            "provider": "spidercloud",
        })

        result = queue.lease_scrape_url_batch(limit=10)

        assert len(result.urls) == 2
        assert result.urls[0]["url"] == "https://example.com/job1"

    def test_complete_marks_urls_completed(self) -> None:
        """Complete should mark URLs as completed."""
        queue = MockQueueService()
        queue.enqueue_scrape_urls({
            "urls": ["https://example.com/job1"],
            "siteId": "test-site",
        })
        leased = queue.lease_scrape_url_batch(limit=10)

        queue.complete_scrape_urls({"ids": [leased.urls[0]["id"]]})

        status = queue.queue_status()
        assert status["completed"] == 1
        assert status["pending"] == 0

    def test_queue_status_returns_counts(self) -> None:
        """Queue status should return accurate counts."""
        queue = MockQueueService()
        queue.enqueue_scrape_urls({
            "urls": ["https://example.com/job1", "https://example.com/job2"],
            "siteId": "test-site",
        })

        status = queue.queue_status()

        assert status["pending"] == 2
        assert status["processing"] == 0
        assert status["completed"] == 0
        assert status["total"] == 2


class TestMockSettings:
    """Tests for MockSettings."""

    def test_default_values(self) -> None:
        """MockSettings should have sensible defaults."""
        settings = MockSettings()

        assert settings.convex_url == "https://test.convex.cloud"
        assert settings.spider_api_key == "test_spider_key"
        assert settings.enable_firecrawl is False

    def test_overrides(self) -> None:
        """MockSettings should respect overrides."""
        settings = MockSettings(overrides={
            "convex_url": "https://custom.convex.cloud",
            "enable_firecrawl": True,
        })

        assert settings.convex_url == "https://custom.convex.cloud"
        assert settings.enable_firecrawl is True


class TestMockSpiderClient:
    """Tests for MockSpiderClient."""

    @pytest.mark.asyncio
    async def test_returns_fixture_response(self) -> None:
        """Spider client should return fixture data for matching URLs."""
        captured: list = []
        client = MockSpiderClient(
            responses={
                "https://example.com/jobs": [{"content": "job data"}],
            },
            captured_scrapes=captured,
        )

        async with client as c:
            result = await c.scrape_url("https://example.com/jobs", {"timeout": 30000})

        assert result == [{"content": "job data"}]
        assert len(captured) == 1
        assert captured[0]["url"] == "https://example.com/jobs"


class TestDependencyContainer:
    """Tests for DependencyContainer."""

    def test_testing_mode_uses_mock_convex(self) -> None:
        """Testing mode should use mock Convex functions."""
        captured: list = []
        deps = DependencyContainer.testing(
            query_fixtures={"router:listSites": [{"id": "test"}]},
            captured_queries=captured,
        )

        result = deps.query("router:listSites", {})

        assert result == [{"id": "test"}]
        assert len(captured) == 1

    def test_testing_mode_captures_mutations(self) -> None:
        """Testing mode should capture mutations."""
        captured: list = []
        deps = DependencyContainer.testing(
            mutation_fixtures={"router:leaseSite": {"leased": True}},
            captured_mutations=captured,
        )

        deps.mutation("router:leaseSite", {"id": "site123"})

        assert len(captured) == 1
        assert captured[0]["name"] == "router:leaseSite"
        assert captured[0]["args"]["id"] == "site123"

    def test_testing_mode_provides_mock_queue(self) -> None:
        """Testing mode should provide mock queue service."""
        deps = DependencyContainer.testing()

        result = deps.enqueue_urls({
            "urls": ["https://example.com/job1"],
            "siteId": "test",
        })

        assert result["added"] == 1

    def test_testing_mode_provides_mock_settings(self) -> None:
        """Testing mode should provide mock settings."""
        deps = DependencyContainer.testing(
            settings_overrides={"spider_api_key": "custom_key"},
        )

        assert deps.settings is not None
        assert deps.settings.spider_api_key == "custom_key"

    def test_captured_lists_are_accessible(self) -> None:
        """Captured lists should be accessible via properties."""
        queries: list = []
        mutations: list = []
        scrapes: list = []

        deps = DependencyContainer.testing(
            captured_queries=queries,
            captured_mutations=mutations,
            captured_scrapes=scrapes,
        )

        assert deps.captured_queries is queries
        assert deps.captured_mutations is mutations
        assert deps.captured_scrapes is scrapes
