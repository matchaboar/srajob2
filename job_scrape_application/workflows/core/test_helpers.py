"""Test helpers for workflow testing.

This module provides helper classes that simplify setting up tests
for workflow activities. It uses the DependencyContainer pattern
internally while still working with the existing activity code.

Usage:
    from job_scrape_application.workflows.core.test_helpers import WorkflowTestHelper

    async def test_my_workflow(tmp_path, monkeypatch):
        helper = WorkflowTestHelper(
            fixtures={"https://example.com/jobs": fixture_data},
            monkeypatch=monkeypatch,
            tmp_path=tmp_path,
        )
        await helper.setup()

        # Run your workflow/activity
        result = await some_activity(...)

        # Check captured data
        assert len(helper.captured_mutations) > 0
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List



@dataclass
class CapturedConvexData:
    """Container for captured Convex operations during tests."""

    queries: List[Dict[str, Any]] = field(default_factory=list)
    mutations: List[Dict[str, Any]] = field(default_factory=list)
    ingested_jobs: List[Dict[str, Any]] = field(default_factory=list)
    stored_scrapes: List[Dict[str, Any]] = field(default_factory=list)
    description_uploads: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class SpiderFixture:
    """Fixture data for a SpiderCloud request.

    Attributes:
        url: The URL that was requested
        response: The response data (list of JSONL strings or dict for sync mode)
        params: Request parameters
        is_sync: Whether this is a sync (non-streaming) fixture
    """

    url: str
    response: Any
    params: Dict[str, Any] = field(default_factory=dict)
    is_sync: bool = False
    _raw_dict: Dict[str, Any] = field(default_factory=dict)

    @property
    def request_url(self) -> str:
        """Alias for url for compatibility."""
        return self.url

    @property
    def source_url(self) -> str | None:
        """Return the original source/input URL if stored in fixture."""
        return self._raw_dict.get("source_url")

    @property
    def raw(self) -> Dict[str, Any]:
        """Return the raw fixture dict for tests that need full access."""
        return self._raw_dict

    @classmethod
    def from_file(cls, path: Path) -> "SpiderFixture":
        """Load a fixture from a JSON file."""
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SpiderFixture":
        """Load a fixture from a dictionary."""
        if not isinstance(data, dict):
            raise ValueError("Fixture data must be a dict")
        request = data.get("request", {})
        return cls(
            url=request.get("url", ""),
            response=data.get("response", []),
            params=request.get("params", {}),
            is_sync=request.get("stream") is False or isinstance(data.get("response"), dict),
            _raw_dict=data,
        )


class _MockAsyncSpider:
    """Mock SpiderCloud client for JSONL streaming mode."""

    def __init__(
        self,
        fixtures: Dict[str, SpiderFixture],
        calls: List[Dict[str, Any]],
    ):
        self._fixtures = fixtures
        self._calls = calls

    async def __aenter__(self) -> "_MockAsyncSpider":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    def scrape_url(
        self,
        url: str,
        *,
        params: Dict[str, Any],
        stream: bool,
        content_type: str,
    ) -> Any:
        fixture = self._fixtures.get(url)
        if not fixture:
            # Try partial match
            for key, fix in self._fixtures.items():
                if key in url or url in key:
                    fixture = fix
                    break
        if not fixture:
            raise AssertionError(f"Unexpected SpiderCloud URL: {url}")

        self._calls.append({"url": url, "params": params, "stream": stream})
        response = fixture.response

        async def _iterator():
            if isinstance(response, list):
                full_response = "".join(response)
                if full_response and not full_response.endswith("\n"):
                    full_response += "\n"
                if full_response:
                    yield full_response
            elif isinstance(response, str):
                if response and not response.endswith("\n"):
                    yield response + "\n"
                else:
                    yield response

        return _iterator()


class _MockSyncSpider:
    """Mock SpiderCloud client for synchronous JSON mode."""

    def __init__(
        self,
        fixtures: Dict[str, SpiderFixture],
        calls: List[Dict[str, Any]],
    ):
        self._fixtures = fixtures
        self._calls = calls

    async def __aenter__(self) -> "_MockSyncSpider":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    def scrape_url(
        self,
        url: str,
        *,
        params: Dict[str, Any],
        stream: bool,
        content_type: str,
    ) -> Any:
        fixture = self._fixtures.get(url)
        if not fixture:
            for key, fix in self._fixtures.items():
                if key in url or url in key:
                    fixture = fix
                    break
        if not fixture:
            raise AssertionError(f"Unexpected SpiderCloud URL: {url}")

        self._calls.append({"url": url, "params": params, "stream": stream})

        async def _awaitable():
            return fixture.response

        return _awaitable()


class WorkflowTestHelper:
    """Helper class for setting up workflow tests.

    Simplifies the common test setup patterns by:
    - Managing environment variables for DBOS
    - Mocking SpiderCloud client with fixtures
    - Mocking Convex query/mutation
    - Capturing all data sent to Convex

    Example:
        async def test_extraction(tmp_path, monkeypatch):
            fixture = SpiderFixture.from_file(Path("fixtures/site_detail.json"))
            helper = WorkflowTestHelper(
                fixtures={fixture.url: fixture},
                monkeypatch=monkeypatch,
                tmp_path=tmp_path,
            )
            await helper.setup()

            result = await process_spidercloud_job_batch(batch)

            assert len(helper.captured.ingested_jobs) > 0
    """

    def __init__(
        self,
        fixtures: Dict[str, SpiderFixture],
        monkeypatch: Any,
        tmp_path: Path,
        *,
        site_id: str = "test-site",
        source_url: str = "",
        query_responses: Dict[str, Any] | None = None,
        mutation_responses: Dict[str, Any] | None = None,
    ):
        """Initialize the test helper.

        Args:
            fixtures: Dict mapping URLs to SpiderFixture objects
            monkeypatch: pytest monkeypatch fixture
            tmp_path: pytest tmp_path fixture for database
            site_id: Site identifier for Convex queries
            source_url: Source URL for the scrape
            query_responses: Custom responses for specific Convex queries
            mutation_responses: Custom responses for specific Convex mutations
        """
        self.fixtures = fixtures
        self.monkeypatch = monkeypatch
        self.tmp_path = tmp_path
        self.site_id = site_id
        self.source_url = source_url
        self.query_responses = query_responses or {}
        self.mutation_responses = mutation_responses or {}

        self.captured = CapturedConvexData()
        self.spider_calls: List[Dict[str, Any]] = []

        # Determine if using sync mode based on first fixture
        self._is_sync_mode = False
        if fixtures:
            first_fixture = next(iter(fixtures.values()))
            self._is_sync_mode = first_fixture.is_sync

    async def setup(self) -> None:
        """Configure all mocks and environment.

        Call this before running any workflow/activity code.
        """
        # Set up DBOS environment
        db_path = self.tmp_path / "dbos.sqlite"
        self.monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
        self.monkeypatch.setenv("SPIDER_API_KEY", "test_key")
        self.monkeypatch.setenv("CONVEX_HTTP_URL", "http://test.convex.site")

        # Reset DBOS connection
        from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
        dbos_sqlite._CONNECTIONS.connection = None

        # Set runtime config to match fixture format
        from job_scrape_application.config import runtime_config
        object.__setattr__(runtime_config, "spidercloud_single_request_mode", self._is_sync_mode)

        # Patch SpiderCloud client
        if self._is_sync_mode:
            def spider_class(api_key):
                return _MockSyncSpider(self.fixtures, self.spider_calls)
        else:
            def spider_class(api_key):
                return _MockAsyncSpider(self.fixtures, self.spider_calls)

        self.monkeypatch.setattr(
            "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
            spider_class,
        )

        # Patch Convex client
        self.monkeypatch.setattr(
            "job_scrape_application.services.convex_client.convex_query",
            self._fake_convex_query,
        )
        self.monkeypatch.setattr(
            "job_scrape_application.services.convex_client.convex_mutation",
            self._fake_convex_mutation,
        )

        # Patch activity functions
        from job_scrape_application.workflows import activities as acts

        self.monkeypatch.setattr(acts, "store_scrape", self._fake_store_scrape)
        self.monkeypatch.setattr(acts, "fetch_seen_urls_for_site", self._fake_fetch_seen_urls)
        self.monkeypatch.setattr(acts, "filter_existing_job_urls", self._fake_filter_existing)
        self.monkeypatch.setattr(acts, "filter_new_job_urls", self._fake_filter_new)
        self.monkeypatch.setattr(acts, "_store_job_descriptions_via_http", self._fake_store_descriptions)
        self.monkeypatch.setattr(acts, "_lookup_job_id_for_url", self._fake_lookup_job_id)

    async def _fake_convex_query(self, name: str, payload: Dict[str, Any]) -> Any:
        """Mock Convex query function."""
        self.captured.queries.append({"name": name, "args": payload})

        # Check custom responses first
        if name in self.query_responses:
            response = self.query_responses[name]
            return response(payload) if callable(response) else response

        # Default responses for common queries
        if name == "router:getSiteById" and payload.get("id") == self.site_id:
            return {"paginationLimit": 3}
        if name == "router:listJobDetailConfigs":
            return []
        return None

    async def _fake_convex_mutation(self, name: str, payload: Dict[str, Any]) -> Any:
        """Mock Convex mutation function."""
        self.captured.mutations.append({"name": name, "args": payload})

        # Capture ingested jobs
        if name == "router:ingestJobsFromScrape":
            jobs = payload.get("jobs", [])
            if isinstance(jobs, list):
                self.captured.ingested_jobs.extend(jobs)

        # Check custom responses
        if name in self.mutation_responses:
            response = self.mutation_responses[name]
            return response(payload) if callable(response) else response

        return None

    async def _fake_store_scrape(self, scrape: Dict[str, Any]) -> str:
        """Mock store_scrape activity."""
        self.captured.stored_scrapes.append(scrape)
        return f"scrape-{len(self.captured.stored_scrapes)}"

    async def _fake_fetch_seen_urls(self, *args: Any, **kwargs: Any) -> List[str]:
        """Mock fetch_seen_urls_for_site activity."""
        return []

    async def _fake_filter_existing(self, urls: List[str]) -> List[str]:
        """Mock filter_existing_job_urls activity."""
        return []

    async def _fake_filter_new(self, urls: List[str]) -> List[str]:
        """Mock filter_new_job_urls activity - return all as new."""
        return urls

    async def _fake_store_descriptions(
        self,
        jobs: List[Dict[str, Any]],
        source_url: str | None,
        provider: str | None,
        workflow_name: str | None,
        log_workflow_event: Any = None,
    ) -> None:
        """Mock _store_job_descriptions_via_http activity."""
        for job in jobs:
            description = job.get("description")
            url = job.get("url")
            if isinstance(description, str) and description.strip():
                self.captured.description_uploads.append({
                    "url": url,
                    "description": description,
                    "word_count": len(description.split()),
                })

    async def _fake_lookup_job_id(self, url: str) -> str | None:
        """Mock _lookup_job_id_for_url activity."""
        return f"job-{hash(url) % 10000}"

    def get_first_stored_scrape(self) -> Dict[str, Any] | None:
        """Get the first stored scrape, if any."""
        return self.captured.stored_scrapes[0] if self.captured.stored_scrapes else None

    def get_normalized_items(self) -> List[Dict[str, Any]]:
        """Extract normalized items from all stored scrapes."""
        items = []
        for scrape in self.captured.stored_scrapes:
            scrape_items = scrape.get("items") if isinstance(scrape, dict) else {}
            if not isinstance(scrape_items, dict):
                continue
            normalized = scrape_items.get("normalized")
            if not isinstance(normalized, list):
                normalized = scrape_items.get("normalizedSample")
            if isinstance(normalized, list):
                items.extend(normalized)
        return items
