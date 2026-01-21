"""Test utilities for DBOS workflows.

This module provides a `WorkflowTest` class that makes workflow testing simple
and declarative with minimal boilerplate.

Usage:
    @pytest.fixture
    def workflow_test(tmp_path, monkeypatch):
        from job_scrape_application.workflows.workflow.test_utils import WorkflowTest
        return WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)

    async def test_listing_workflow(workflow_test):
        workflow_test.with_spidercloud_response(
            url="https://boards.greenhouse.io/company",
            response={"jobs": [{"url": "https://company.com/job/123"}]}
        )

        from job_scrape_application.workflows.workflow import scrape_listing_batch
        result = await workflow_test.run(scrape_listing_batch, batch={...})

        assert isinstance(result, Success)
        assert workflow_test.call_count("enqueue_detail_urls") == 1

    # Using SpiderFixture for loading fixtures from files:
    async def test_job_extraction(workflow_test):
        fixture = SpiderFixture.from_file(Path("fixtures/single_request/adobe_20260116T214330_detail.json"))
        workflow_test.with_spider_fixture(fixture)

        result = await workflow_test.run(scrape_job_detail_batch, ...)
        assert len(workflow_test.captured.stored_scrapes) == 1
"""

from __future__ import annotations

import orjson
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, TypeVar

T = TypeVar("T")

# Fixture directories (relative to repo root)
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
GROUND_TRUTH_DIR = Path("tests/job_scrape_application/workflows/ground_truth")


@dataclass
class SpiderFixture:
    """Fixture data for a SpiderCloud request.

    This dataclass encapsulates the request/response data from a SpiderCloud
    fixture file, providing convenient access to URLs, response content, and
    metadata.

    Attributes:
        url: The URL that was requested
        response: The response data (list of dicts for single-request mode)
        params: Request parameters
        is_sync: Whether this is a sync (non-streaming) fixture
        _raw_dict: The raw fixture dict for full access

    Usage:
        # Load from file
        fixture = SpiderFixture.from_file(Path("fixtures/single_request/adobe_20260116T214330_detail.json"))

        # Load from dict
        fixture = SpiderFixture.from_dict({"request": {...}, "response": [...]})

        # Access properties
        print(fixture.url)  # Request URL
        print(fixture.source_url)  # Original source URL if stored
        print(fixture.raw)  # Full raw dict
    """

    url: str
    response: Any
    params: dict[str, Any] = field(default_factory=dict)
    is_sync: bool = True
    _raw_dict: dict[str, Any] = field(default_factory=dict)

    @property
    def request_url(self) -> str:
        """Alias for url for compatibility."""
        return self.url

    @property
    def source_url(self) -> str | None:
        """Return the original source/input URL if stored in fixture."""
        return self._raw_dict.get("source_url")

    @property
    def raw(self) -> dict[str, Any]:
        """Return the raw fixture dict for tests that need full access."""
        return self._raw_dict

    @classmethod
    def from_file(cls, path: Path) -> "SpiderFixture":
        """Load a fixture from a JSON file.

        Args:
            path: Path to the fixture JSON file

        Returns:
            SpiderFixture instance with loaded data
        """
        data = orjson.loads(path.read_text(encoding="utf-8"))
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SpiderFixture":
        """Load a fixture from a dictionary.

        Handles multiple fixture formats:
        - {"request": {...}, "response": [...]} - standard format
        - [[{...}]] - nested list format (legacy)
        - [{...}] - simple list format

        Args:
            data: Fixture data dictionary

        Returns:
            SpiderFixture instance
        """
        if not isinstance(data, dict):
            # Handle list format fixtures
            if isinstance(data, list):
                return cls(
                    url="",
                    response=data,
                    params={},
                    is_sync=True,
                    _raw_dict={"response": data},
                )
            raise ValueError("Fixture data must be a dict or list")

        request = data.get("request", {})
        return cls(
            url=request.get("url", ""),
            response=data.get("response", []),
            params=request.get("params", {}),
            is_sync=request.get("stream") is not True,
            _raw_dict=data,
        )

    @classmethod
    def load_dbos_schedule_fixture(cls, name: str) -> "SpiderFixture":
        """Load a fixture from the dbos_schedule directory.

        Args:
            name: Fixture name (e.g., "netflix_detail" or "netflix")

        Returns:
            SpiderFixture instance
        """
        # Normalize name - add _detail suffix if not present
        if not name.endswith("_detail") and not name.endswith("_listing"):
            name = f"{name}_detail"

        if name.endswith("_detail"):
            slug = name[:-len("_detail")]
            suffix = "detail"
        else:
            slug = name[:-len("_listing")]
            suffix = "listing"

        path = FIXTURE_DIR / "dbos_schedule" / f"{name}.json"
        if not path.exists():
            candidates = sorted(
                (FIXTURE_DIR / "dbos_schedule").glob(f"{slug}_*_{suffix}.json"),
                reverse=True,
            )
            if candidates:
                path = candidates[0]
            else:
                raise FileNotFoundError(f"Fixture not found: {path}")
        return cls.from_file(path)

    @classmethod
    def load_debug_fixture(cls, company: str, fixture_name: str) -> "SpiderFixture":
        """Load a fixture from the debug directory.

        Args:
            company: Company folder name (e.g., "adobe", "hubspot")
            fixture_name: Fixture file name without .json extension

        Returns:
            SpiderFixture instance
        """
        path = FIXTURE_DIR / "debug" / company / f"{fixture_name}.json"
        if not path.exists():
            # Try adding _detail suffix
            path = FIXTURE_DIR / "debug" / company / f"{fixture_name}_detail.json"
        if not path.exists():
            if fixture_name.endswith("_detail"):
                slug = fixture_name[:-len("_detail")]
                suffix = "detail"
            elif fixture_name.endswith("_listing"):
                slug = fixture_name[:-len("_listing")]
                suffix = "listing"
            else:
                slug = fixture_name
                suffix = "detail"
            candidates = sorted(
                (FIXTURE_DIR / "debug" / company).glob(f"{slug}_*_{suffix}.json"),
                reverse=True,
            )
            if candidates:
                path = candidates[0]
        if not path.exists():
            raise FileNotFoundError(f"Fixture not found: {path}")
        return cls.from_file(path)


@dataclass
class CapturedStepCalls:
    """Container for captured step invocations during tests."""

    calls: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))
    convex_queries: list[dict[str, Any]] = field(default_factory=list)
    convex_mutations: list[dict[str, Any]] = field(default_factory=list)
    stored_scrapes: list[dict[str, Any]] = field(default_factory=list)
    queue_operations: list[dict[str, Any]] = field(default_factory=list)
    telemetry_events: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class StepOverride:
    """Configuration for overriding a step's behavior."""

    return_value: Any = None
    side_effect: Exception | Callable[..., Any] | None = None


class _MockAsyncSpider:
    """Mock SpiderCloud client for async context manager pattern."""

    def __init__(
        self,
        fixtures: dict[str, dict[str, Any]],
        calls: list[dict[str, Any]],
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
        params: dict[str, Any],
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

        async def _awaitable() -> dict[str, Any]:
            return fixture

        return _awaitable()


class WorkflowTest:
    """Test harness for DBOS workflows.

    Provides a fluent API for configuring test fixtures and mocks, running
    workflows, and inspecting captured step calls.

    Features:
    - Auto-mocking of all known steps with sensible defaults
    - Selective overrides for specific steps
    - Step call tracking by step name
    - Result pattern support (Success/Failure)
    - SpiderCloud fixture matching by URL

    Example:
        async def test_workflow(workflow_test):
            workflow_test.with_spidercloud_response(
                url="https://example.com",
                response={"jobs": [...]}
            )
            workflow_test.mock_step("filter_new_job_urls", return_value=[])

            result = await workflow_test.run(my_workflow, input_data={...})

            assert isinstance(result, Success)
            assert workflow_test.call_count("store_job_scrape") == 2
    """

    def __init__(self, tmp_path: Path, monkeypatch: Any):
        """Initialize the test harness.

        Args:
            tmp_path: pytest tmp_path fixture for temporary database
            monkeypatch: pytest monkeypatch fixture for mocking
        """
        self.tmp_path = tmp_path
        self.monkeypatch = monkeypatch
        self.captured = CapturedStepCalls()
        self.spider_calls: list[dict[str, Any]] = []

        self._fixtures: dict[str, dict[str, Any]] = {}
        self._step_overrides: dict[str, StepOverride] = {}
        self._query_responses: dict[str, Any] = {}
        self._mutation_responses: dict[str, Any] = {}
        self._mocks_applied = False

    def with_spidercloud_response(
        self,
        url: str,
        response: dict[str, Any],
    ) -> "WorkflowTest":
        """Add a SpiderCloud fixture for a URL.

        Args:
            url: The URL that SpiderCloud will be called with
            response: The response data to return

        Returns:
            self for method chaining
        """
        self._fixtures[url] = response
        return self

    def with_spider_fixture(self, fixture: SpiderFixture) -> "WorkflowTest":
        """Add a SpiderFixture to the test harness.

        This is the preferred method for loading fixtures from files.

        Args:
            fixture: SpiderFixture instance (e.g., from SpiderFixture.from_file())

        Returns:
            self for method chaining

        Example:
            fixture = SpiderFixture.from_file(Path("fixtures/single_request/adobe_20260116T214330_detail.json"))
            workflow_test.with_spider_fixture(fixture)
        """
        if fixture.url:
            self._fixtures[fixture.url] = fixture.response
        return self

    def with_fixture_file(self, path: Path) -> "WorkflowTest":
        """Load a SpiderFixture from a file and add it.

        Convenience method that combines SpiderFixture.from_file() and
        with_spider_fixture().

        Args:
            path: Path to the fixture JSON file

        Returns:
            self for method chaining

        Example:
            workflow_test.with_fixture_file(Path("fixtures/single_request/adobe_20260116T214330_detail.json"))
        """
        fixture = SpiderFixture.from_file(path)
        return self.with_spider_fixture(fixture)

    def with_dbos_schedule_fixture(self, name: str) -> "WorkflowTest":
        """Load a fixture from the dbos_schedule directory.

        Args:
            name: Fixture name (e.g., "netflix_detail" or "netflix")

        Returns:
            self for method chaining

        Example:
            workflow_test.with_dbos_schedule_fixture("netflix")
        """
        fixture = SpiderFixture.load_dbos_schedule_fixture(name)
        return self.with_spider_fixture(fixture)

    def with_debug_fixture(self, company: str, fixture_name: str) -> "WorkflowTest":
        """Load a fixture from the debug directory.

        Args:
            company: Company folder name (e.g., "adobe", "hubspot")
            fixture_name: Fixture file name without .json extension

        Returns:
            self for method chaining

        Example:
            workflow_test.with_debug_fixture("adobe", "adobe_careers_20260114T134509")
        """
        fixture = SpiderFixture.load_debug_fixture(company, fixture_name)
        return self.with_spider_fixture(fixture)

    def mock_step(
        self,
        step_name: str,
        *,
        return_value: Any = None,
        side_effect: Exception | Callable[..., Any] | None = None,
    ) -> "WorkflowTest":
        """Override a step's behavior.

        Args:
            step_name: Name of the step function to mock
            return_value: Value to return when the step is called
            side_effect: Exception to raise or callable to invoke

        Returns:
            self for method chaining
        """
        self._step_overrides[step_name] = StepOverride(
            return_value=return_value,
            side_effect=side_effect,
        )
        return self

    def with_query_response(
        self,
        query_name: str,
        response: Any,
    ) -> "WorkflowTest":
        """Configure response for a specific Convex query.

        Args:
            query_name: Name of the Convex query (e.g., "router:getSiteById")
            response: Value to return, or callable that takes payload and returns value

        Returns:
            self for method chaining
        """
        self._query_responses[query_name] = response
        return self

    def with_mutation_response(
        self,
        mutation_name: str,
        response: Any,
    ) -> "WorkflowTest":
        """Configure response for a specific Convex mutation.

        Args:
            mutation_name: Name of the Convex mutation
            response: Value to return, or callable that takes payload and returns value

        Returns:
            self for method chaining
        """
        self._mutation_responses[mutation_name] = response
        return self

    @property
    def step_calls(self) -> dict[str, list[dict[str, Any]]]:
        """Get all captured step calls by step name."""
        return self.captured.calls

    def call_count(self, step_name: str) -> int:
        """Get the number of times a step was called.

        Args:
            step_name: Name of the step function

        Returns:
            Number of invocations
        """
        return len(self.captured.calls[step_name])

    def get_step_call(self, step_name: str, index: int = 0) -> dict[str, Any] | None:
        """Get a specific step call by name and index.

        Args:
            step_name: Name of the step function
            index: Which call to retrieve (default: first call)

        Returns:
            The call arguments dict, or None if not found
        """
        calls = self.captured.calls[step_name]
        if index < len(calls):
            return calls[index]
        return None

    async def run(self, workflow_fn: Callable[..., Any], **kwargs: Any) -> Any:
        """Run a workflow with all mocks applied.

        Args:
            workflow_fn: The workflow function to execute
            **kwargs: Arguments to pass to the workflow

        Returns:
            The workflow result
        """
        if not self._mocks_applied:
            self._apply_mocks()
        return await workflow_fn(**kwargs)

    def _apply_mocks(self) -> None:
        """Apply all configured mocks."""
        self._mocks_applied = True

        # Set up DBOS environment
        db_path = self.tmp_path / "dbos.sqlite"
        self.monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
        self.monkeypatch.setenv("SPIDER_API_KEY", "test_key")
        self.monkeypatch.setenv("CONVEX_HTTP_URL", "http://test.convex.site")

        # Reset DBOS connection
        from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite

        dbos_sqlite._CONNECTIONS.connection = None

        # Set runtime config
        from job_scrape_application.config import runtime_config

        object.__setattr__(runtime_config, "spidercloud_single_request_mode", True)

        # Patch SpiderCloud client
        def spider_class(api_key: str) -> _MockAsyncSpider:
            return _MockAsyncSpider(self._fixtures, self.spider_calls)

        self.monkeypatch.setattr(
            "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
            spider_class,
        )

        # Patch Convex client
        self.monkeypatch.setattr(
            "job_scrape_application.services.convex_client.convex_query",
            self._mock_convex_query,
        )
        self.monkeypatch.setattr(
            "job_scrape_application.services.convex_client.convex_mutation",
            self._mock_convex_mutation,
        )

        # Patch known step modules
        self._patch_step_modules()

    def _patch_step_modules(self) -> None:
        """Patch known step modules with mock implementations."""
        # Patch store_scrape in workflow module
        from job_scrape_application.workflows import workflow as workflow_module

        self.monkeypatch.setattr(workflow_module, "store_scrape", self._mock_store_scrape)

        # Patch fetch_seen_urls_for_site in helpers module
        from job_scrape_application.workflows.helpers import scrape_utils

        self.monkeypatch.setattr(scrape_utils, "fetch_seen_urls_for_site", self._mock_fetch_seen_urls)

        # Patch step module functions
        try:
            from job_scrape_application.workflows.activities import step as step_module

            if hasattr(step_module, "filter_new_job_urls"):
                self.monkeypatch.setattr(
                    step_module, "filter_new_job_urls", self._mock_filter_new_job_urls
                )
            if hasattr(step_module, "record_scrape_url_attempts"):
                self.monkeypatch.setattr(
                    step_module, "record_scrape_url_attempts", self._mock_record_scrape_url_attempts
                )
            if hasattr(step_module, "log_scrape_error"):
                self.monkeypatch.setattr(
                    step_module, "log_scrape_error", self._mock_log_scrape_error
                )
            if hasattr(step_module, "scrape_listing_urls"):
                original_scrape_listing_urls = step_module.scrape_listing_urls

                async def _capture_scrape_listing_urls(*args: Any, **kwargs: Any) -> Any:
                    result = await original_scrape_listing_urls(*args, **kwargs)
                    payload: dict[str, Any] = {"result": result}
                    if args:
                        payload["args"] = args
                    if kwargs:
                        payload["kwargs"] = kwargs
                    self.captured.calls["scrape_listing_urls"].append(payload)
                    return result

                self.monkeypatch.setattr(
                    step_module, "scrape_listing_urls", _capture_scrape_listing_urls
                )
        except (ImportError, AttributeError):
            pass

        # Patch queue operations
        try:
            from job_scrape_application.dbos_runtime import queue as queue_module

            self.monkeypatch.setattr(queue_module, "enqueue_scrape_urls", self._mock_enqueue_scrape_urls)
            self.monkeypatch.setattr(
                queue_module, "complete_scrape_urls", self._mock_complete_scrape_urls
            )
        except (ImportError, AttributeError):
            pass

        # Patch step-level queue wrappers used by workflows (module-level imports).
        try:
            from job_scrape_application.dbos_runtime import step as runtime_step

            self.monkeypatch.setattr(
                runtime_step, "enqueue_scrape_urls_step", self._mock_enqueue_scrape_urls_step
            )
            self.monkeypatch.setattr(
                runtime_step, "complete_scrape_urls_step", self._mock_complete_scrape_urls_step
            )
        except (ImportError, AttributeError):
            pass

        # Patch workflow module bindings for queue steps.
        # NOTE: We use importlib to import the MODULE, not the function exported by __init__.py
        import importlib
        try:
            listing_module = importlib.import_module("job_scrape_application.workflows.workflow.scrape_listing_batch")

            self.monkeypatch.setattr(
                listing_module, "enqueue_scrape_urls_step", self._mock_enqueue_scrape_urls_step
            )
            self.monkeypatch.setattr(
                listing_module, "complete_scrape_urls_step", self._mock_complete_scrape_urls_step
            )
            if hasattr(listing_module, "scrape_listing_urls"):
                step_module = __import__(
                    "job_scrape_application.workflows.activities.step",
                    fromlist=["scrape_listing_urls"],
                )
                self.monkeypatch.setattr(
                    listing_module, "scrape_listing_urls", step_module.scrape_listing_urls
                )
        except (ImportError, AttributeError):
            pass

        try:
            detail_module = importlib.import_module("job_scrape_application.workflows.workflow.scrape_job_detail_batch")

            self.monkeypatch.setattr(
                detail_module, "complete_scrape_urls_step", self._mock_complete_scrape_urls_step
            )
        except (ImportError, AttributeError):
            pass

        # Patch telemetry
        try:
            from job_scrape_application.services import telemetry as telemetry_module

            self.monkeypatch.setattr(
                telemetry_module, "emit_posthog_log", self._mock_emit_telemetry
            )
            self.monkeypatch.setattr(
                telemetry_module, "emit_posthog_exception", self._mock_emit_telemetry
            )
        except (ImportError, AttributeError):
            pass

    def _get_override_result(self, step_name: str, *args: Any, **kwargs: Any) -> tuple[bool, Any]:
        """Check if a step has an override and return its result.

        Returns:
            Tuple of (has_override, result_or_exception)
        """
        override = self._step_overrides.get(step_name)
        if override is None:
            return False, None

        if override.side_effect is not None:
            if isinstance(override.side_effect, Exception):
                raise override.side_effect
            if callable(override.side_effect):
                return True, override.side_effect(*args, **kwargs)

        return True, override.return_value

    def _mock_convex_query(self, name: str, payload: dict[str, Any]) -> Any:
        """Mock Convex query function (synchronous)."""
        self.captured.convex_queries.append({"name": name, "args": payload})
        self.captured.calls["convex_query"].append({"name": name, "args": payload})

        # Check custom responses
        if name in self._query_responses:
            response = self._query_responses[name]
            return response(payload) if callable(response) else response

        # Default responses
        if name == "router:getSiteById":
            return {"paginationLimit": 3}
        if name == "router:listSites":
            return []
        if name == "router:filterNewJobUrls":
            return {"new": payload.get("urls", [])}
        if name == "router:listJobDetailConfigs":
            return []

        return None

    def _mock_convex_mutation(self, name: str, payload: dict[str, Any]) -> Any:
        """Mock Convex mutation function (synchronous)."""
        self.captured.convex_mutations.append({"name": name, "args": payload})
        self.captured.calls["convex_mutation"].append({"name": name, "args": payload})

        # Check custom responses
        if name in self._mutation_responses:
            response = self._mutation_responses[name]
            return response(payload) if callable(response) else response

        # Default: return success for known mutations
        if name == "router:recordScrapeUrlAttempts":
            return None
        if name == "router:insertScrapeError":
            return None
        if name == "router:ingestJobsFromScrape":
            return f"scrape-{len(self.captured.stored_scrapes) + 1}"

        return None

    def _mock_store_scrape(self, scrape: dict[str, Any]) -> str:
        """Mock store_scrape activity (synchronous)."""
        has_override, result = self._get_override_result("store_scrape", scrape)
        if has_override:
            return result

        self.captured.stored_scrapes.append(scrape)
        self.captured.calls["store_scrape"].append(scrape)
        return f"scrape-{len(self.captured.stored_scrapes)}"

    def _mock_fetch_seen_urls(
        self,
        source_url: str,
        pattern: str | None,
        urls: list[str],
    ) -> list[str]:
        """Mock fetch_seen_urls_for_site activity (synchronous)."""
        has_override, result = self._get_override_result(
            "fetch_seen_urls_for_site", source_url, pattern, urls
        )
        if has_override:
            return result

        self.captured.calls["fetch_seen_urls_for_site"].append({
            "source_url": source_url,
            "pattern": pattern,
            "urls": urls,
        })
        return []

    def _mock_filter_new_job_urls(self, urls: list[str]) -> list[str]:
        """Mock filter_new_job_urls step - return all as new by default (synchronous)."""
        has_override, result = self._get_override_result("filter_new_job_urls", urls)
        if has_override:
            return result

        self.captured.calls["filter_new_job_urls"].append({"urls": urls})
        return urls

    def _mock_record_scrape_url_attempts(self, entries: list[dict[str, Any]]) -> None:
        """Mock record_scrape_url_attempts step (synchronous)."""
        has_override, result = self._get_override_result("record_scrape_url_attempts", entries)
        if has_override:
            return result

        self.captured.calls["record_scrape_url_attempts"].append({"entries": entries})
        return None

    def _mock_log_scrape_error(self, error: dict[str, Any]) -> None:
        """Mock log_scrape_error step (synchronous)."""
        has_override, result = self._get_override_result("log_scrape_error", error)
        if has_override:
            return result

        self.captured.calls["log_scrape_error"].append(error)
        return None

    def _mock_enqueue_scrape_urls(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Mock enqueue_scrape_urls queue operation."""
        has_override, result = self._get_override_result("enqueue_scrape_urls", payload)
        if has_override:
            return result

        self.captured.queue_operations.append({"type": "enqueue", "payload": payload})
        self.captured.calls["enqueue_scrape_urls"].append(payload)
        urls = payload.get("urls", [])
        return {"queued": len(urls) if isinstance(urls, list) else 0}

    def _mock_complete_scrape_urls(self, payload: dict[str, Any]) -> None:
        """Mock complete_scrape_urls queue operation."""
        has_override, result = self._get_override_result("complete_scrape_urls", payload)
        if has_override:
            return result

        self.captured.queue_operations.append({"type": "complete", "payload": payload})
        self.captured.calls["complete_scrape_urls"].append(payload)
        return None

    def _mock_enqueue_scrape_urls_step(
        self,
        urls: list[str],
        source_url: str,
        provider: str = "spidercloud",
        site_id: str | None = None,
        pattern: str | None = None,
        url_types: list[str] | None = None,
        posted_ats: list[int | None] | None = None,
        delays_ms: list[int] | None = None,
    ) -> dict[str, Any]:
        """Mock enqueue_scrape_urls_step wrapper."""
        payload: dict[str, Any] = {
            "urls": urls,
            "sourceUrl": source_url,
            "provider": provider,
        }
        if site_id:
            payload["siteId"] = site_id
        if pattern:
            payload["pattern"] = pattern
        if url_types:
            payload["urlTypes"] = url_types
        if posted_ats:
            payload["postedAts"] = posted_ats
        if delays_ms:
            payload["delaysMs"] = delays_ms
        return self._mock_enqueue_scrape_urls(payload)

    def _mock_complete_scrape_urls_step(
        self,
        items: list[dict[str, Any]],
        status: str,
        error: str | None = None,
        run_after_ms: int | None = None,
    ) -> dict[str, Any]:
        """Mock complete_scrape_urls_step wrapper."""
        payload: dict[str, Any] = {
            "items": items,
            "status": status,
        }
        if error:
            payload["error"] = error
        if run_after_ms is not None:
            payload["runAfterMs"] = run_after_ms
        return self._mock_complete_scrape_urls(payload)

    def _mock_emit_telemetry(self, *args: Any, **kwargs: Any) -> None:
        """Mock telemetry emission - no-op by default."""
        has_override, result = self._get_override_result("emit_telemetry", *args, **kwargs)
        if has_override:
            return result

        event_data = {"args": args, "kwargs": kwargs}
        self.captured.telemetry_events.append(event_data)
        self.captured.calls["emit_telemetry"].append(event_data)
        return None
