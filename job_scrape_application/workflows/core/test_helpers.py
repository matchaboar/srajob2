"""Test helpers for workflow testing.

.. deprecated:: 2026-01-16
    This module is deprecated. Use the following instead:

    - For new tests, use ``WorkflowTest`` from ``workflow.test_utils``
    - For SpiderFixture loading, use ``SpiderFixture.from_file()`` from ``workflow.test_utils``
    - For company-specific tests, see ``tests/job_scrape_application/workflows/companies/``

Migration Guide:
    Old pattern (this module):
        helper = WorkflowTestHelper(
            fixtures={url: SpiderFixture.from_file(path)},
            monkeypatch=monkeypatch,
            tmp_path=tmp_path,
        )
        await helper.setup()
        result = await some_activity(...)

    New pattern (workflow/test_utils.py):
        from job_scrape_application.workflows.workflow.test_utils import (
            WorkflowTest,
            SpiderFixture,
        )

        # WorkflowTest is provided as a pytest fixture
        async def test_my_workflow(workflow_test):
            fixture = SpiderFixture.from_file(path)
            workflow_test.with_spider_fixture(fixture)

            result = await workflow_test.run(my_workflow, **kwargs)

            assert len(workflow_test.captured.stored_scrapes) > 0

This module will be removed in a future version. Migrate tests to use
the consolidated test infrastructure in ``workflow/test_utils.py``.

Legacy Usage (for reference):
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

import orjson
import re
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

# Emit deprecation warning when this module is imported
warnings.warn(
    "job_scrape_application.workflows.core.test_helpers is deprecated. "
    "Use job_scrape_application.workflows.workflow.test_utils instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Description truncation limits - must match scrape_utils.py
DESCRIPTION_PREVIEW_MAX_WORDS = 100
DESCRIPTION_PREVIEW_MAX_BYTES = 4_000
DESCRIPTION_TRUNCATION_SUFFIX = "..."

_WORD_SPLIT_RE = re.compile(r"(\S+)")
_MARKDOWN_TITLE_RE = re.compile(r"^#\s+(.+?)$", re.MULTILINE)
_MARKDOWN_LOCATION_RE = re.compile(
    r"(?:^|\n)(?:Engineering|Product|Design|Sales|Marketing|HR|Finance|Operations|Legal)?\n?"
    r"(Remote(?:[ ,]+[\w\s]+)?|(?:[\w\s]+,\s+)?(?:United States|US|USA|Canada|UK|Germany|India|Singapore|Australia|Japan|France|Netherlands|Ireland))",
    flags=re.MULTILINE | re.IGNORECASE,
)
_MARKDOWN_CAREERS_URL_RE = re.compile(r"careers\.(\w+)\.(?:io|com|co)")
_MARKDOWN_DESCRIPTION_RE = re.compile(
    r"(?:#{1,5}\s*)?Description\s*\n(.+?)(?=\n#{1,5}\s|$)",
    flags=re.DOTALL | re.IGNORECASE,
)


def _truncate_description_for_ingest(description: str) -> str:
    """Truncate description for Convex jobs table (mirrors scrape_utils.build_description_preview).

    IMPORTANT: This truncation happens AFTER extraction. Extractors ALWAYS operate
    on the FULL description. This function only truncates for the ingested job record.

    The full description is separately posted to Convex file storage.
    """
    if not description:
        return ""

    trimmed = description.strip()
    if not trimmed:
        return ""

    # Step 1: Truncate to word limit
    tokens = _WORD_SPLIT_RE.split(trimmed)
    words_seen = 0
    truncate_at = len(trimmed)
    pos = 0

    for i, token in enumerate(tokens):
        if i % 2 == 1:  # This is a word
            words_seen += 1
            if words_seen > DESCRIPTION_PREVIEW_MAX_WORDS:
                truncate_at = pos
                break
        pos += len(token)

    if words_seen > DESCRIPTION_PREVIEW_MAX_WORDS:
        trimmed = trimmed[:truncate_at].rstrip() + DESCRIPTION_TRUNCATION_SUFFIX

    # Step 2: Clamp to byte limit
    encoded = trimmed.encode("utf-8")
    if len(encoded) > DESCRIPTION_PREVIEW_MAX_BYTES:
        suffix_bytes = len(DESCRIPTION_TRUNCATION_SUFFIX.encode("utf-8"))
        target_bytes = max(0, DESCRIPTION_PREVIEW_MAX_BYTES - suffix_bytes)
        low, high = 0, len(trimmed)
        while low < high:
            mid = (low + high + 1) // 2
            chunk = trimmed[:mid]
            size = len(chunk.encode("utf-8"))
            if size <= target_bytes:
                low = mid
            else:
                high = mid - 1
        trimmed = trimmed[:low] + DESCRIPTION_TRUNCATION_SUFFIX

    return trimmed



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
    is_sync: bool = True
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
        data = orjson.loads(path.read_text(encoding="utf-8"))
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
            is_sync=request.get("stream") is not True,  # Default to sync mode (streaming is deprecated)
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

        # Determine if using sync mode based on first fixture (default to sync, streaming is deprecated)
        self._is_sync_mode = True
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

        # Patch step functions (used by DBOS workflows)
        from job_scrape_application.workflows.activities import step as step_module
        from job_scrape_application.workflows.helpers import scrape_utils
        from job_scrape_application.workflows.helpers import step as helpers_step

        self.monkeypatch.setattr(scrape_utils, "fetch_seen_urls_for_site", self._fake_fetch_seen_urls)
        self.monkeypatch.setattr(helpers_step, "fetch_seen_urls_for_site", self._fake_fetch_seen_urls)
        self.monkeypatch.setattr(step_module, "filter_new_job_urls", self._fake_filter_new)
        self.monkeypatch.setattr(step_module, "lookup_job_id_for_url", self._fake_lookup_job_id)

        # Also patch step functions in the DBOS workflow module where they're imported
        # This is needed because Python imports create local bindings
        import importlib
        detail_workflow_module = importlib.import_module(
            "job_scrape_application.workflows.workflow.scrape_job_detail_batch"
        )

        self.monkeypatch.setattr(
            detail_workflow_module, "record_scrape_url_attempts", self._fake_record_scrape_url_attempts
        )
        self.monkeypatch.setattr(
            detail_workflow_module, "filter_new_job_urls", self._fake_filter_new
        )
        self.monkeypatch.setattr(
            detail_workflow_module, "complete_scrape_urls_step", self._fake_complete_scrape_urls
        )
        self.monkeypatch.setattr(
            detail_workflow_module, "ingest_jobs_from_scrape_step", self._fake_ingest_jobs
        )
        self.monkeypatch.setattr(
            detail_workflow_module, "emit_scrape_telemetry_step", self._fake_emit_telemetry
        )


    def _fake_convex_query(self, name: str, payload: Dict[str, Any]) -> Any:
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

    def _fake_convex_mutation(self, name: str, payload: Dict[str, Any]) -> Any:
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

    def _fake_store_scrape(self, scrape: Dict[str, Any]) -> str:
        """Mock store_scrape activity."""
        self.captured.stored_scrapes.append(scrape)
        return f"scrape-{len(self.captured.stored_scrapes)}"

    def _fake_fetch_seen_urls(self, *args: Any, **kwargs: Any) -> List[str]:
        """Mock fetch_seen_urls_for_site activity."""
        return []

    def _fake_filter_existing(self, urls: List[str]) -> List[str]:
        """Mock filter_existing_job_urls activity."""
        return []

    def _fake_filter_new(self, urls: List[str]) -> List[str]:
        """Mock filter_new_job_urls activity - return all as new."""
        return urls

    def _fake_store_descriptions(
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

    def _fake_lookup_job_id(self, url: str) -> str | None:
        """Mock _lookup_job_id_for_url activity."""
        return f"job-{hash(url) % 10000}"

    def _fake_record_scrape_url_attempts(self, entries: List[Dict[str, Any]]) -> None:
        """Mock record_scrape_url_attempts step."""
        pass

    def _fake_complete_scrape_urls(
        self,
        items: List[Dict[str, Any]],
        status: str,
        error: str | None = None,
    ) -> None:
        """Mock complete_scrape_urls_step."""
        pass

    async def _fake_scrape_job_details(
        self,
        urls: List[str],
        source_url: str,
        pattern: str | None = None,
        posted_at_by_url: Dict[str, int] | None = None,
        site_id: str | None = None,
    ) -> Dict[str, Any]:
        """Mock scrape_job_details step - return normalized job data.

        This mock processes fixture data the same way the real scraper would,
        extracting normalized job data from the SpiderCloud response.
        """
        import orjson

        def extract_json_from_content(raw_html: str) -> Dict[str, Any] | None:
            """Extract JSON object from HTML or markdown content."""
            if not raw_html or not isinstance(raw_html, str):
                return None

            # Try to find JSON by locating first { and last }
            # This handles both markdown code blocks and HTML <pre> tags
            start = raw_html.find("{")
            end = raw_html.rfind("}")
            if start != -1 and end > start:
                try:
                    return orjson.loads(raw_html[start : end + 1])
                except orjson.JSONDecodeError:
                    pass

            return None

        def extract_level_from_title(title: str) -> str:
            """Extract level from job title, default to 'mid' if not found."""
            if not title:
                return "mid"
            title_lower = title.lower()
            if any(
                kw in title_lower
                for kw in ["junior", "jr.", "jr ", "entry", "associate", "graduate", "intern"]
            ):
                return "junior"
            # Staff-level titles (highest seniority) - check these first
            if any(
                kw in title_lower
                for kw in ["staff", "principal", "director", "vp", "chief", "head", "lead", "distinguished"]
            ):
                return "staff"
            if any(
                kw in title_lower
                for kw in ["senior", "sr ", "sr.", "sr-", "sr/"]
            ):
                return "senior"
            # Manager is ambiguous - map to senior as middle ground
            if "manager" in title_lower:
                return "senior"
            # Default to mid for unspecified levels
            return "mid"

        def parse_posted_at(date_str: str | None) -> int | None:
            """Parse ISO date string to milliseconds timestamp."""
            if not date_str:
                return None
            try:
                from datetime import datetime
                # Handle ISO format with timezone
                if "T" in date_str:
                    # Replace timezone offset format for parsing
                    clean = date_str.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(clean)
                    return int(dt.timestamp() * 1000)
                else:
                    # Handle date-only format (e.g., "2024-01-09")
                    dt = datetime.fromisoformat(date_str)
                    return int(dt.timestamp() * 1000)
            except (ValueError, AttributeError):
                pass
            return None

        def extract_from_markdown(markdown: str) -> Dict[str, Any]:
            """Extract job info from markdown content (for non-Greenhouse sites)."""
            result: Dict[str, Any] = {}

            # Extract title from # heading (first h1)
            title_match = _MARKDOWN_TITLE_RE.search(markdown)
            if title_match:
                result["title"] = title_match.group(1).strip()

            # Extract location from common patterns
            # Pattern: "Engineering\nRemote, United States" or just "Remote, United States"
            location_match = _MARKDOWN_LOCATION_RE.search(markdown)
            if location_match:
                result["location"] = location_match.group(1).strip()

            # Don't aggressively set remote=True from text matching
            # Let the heuristics extractor handle remote detection properly

            # Extract company from URL domain
            url_match = _MARKDOWN_CAREERS_URL_RE.search(markdown)
            if url_match:
                result["company"] = url_match.group(1).title()

            # Extract description - content after "Description" header
            desc_match = _MARKDOWN_DESCRIPTION_RE.search(markdown)
            if desc_match:
                result["description"] = desc_match.group(1).strip()

            return result

        def process_response_item(item: Dict[str, Any], url: str) -> Dict[str, Any] | None:
            """Process a single response item using extract_job_from_scrape.

            This ensures the test mock uses the same extraction logic as production,
            including proper handler-based company, location, and other field extraction.
            """
            from ..extractors.integration import extract_job_from_scrape
            from ..site_handlers import get_site_handler

            content = item.get("content", {})
            raw_html = content.get("raw") or content.get("commonmark") or ""

            # Get handler for proper field extraction
            handler = get_site_handler(url)

            # Try to extract structured data from content (Greenhouse JSON, etc.)
            structured_data = extract_json_from_content(raw_html)

            # Use extract_job_from_scrape for consistent extraction
            job_result = extract_job_from_scrape(
                url=url,
                markdown=raw_html,
                handler=handler,
                structured_data=structured_data,
                raw_row=item,
                debug=False,
            )

            if not job_result:
                return None

            return job_result

        normalized_jobs: List[Dict[str, Any]] = []

        # Find matching fixture for the URLs
        for url in urls:
            if url not in self.fixtures:
                continue

            fixture = self.fixtures[url]
            response = fixture.response if hasattr(fixture, "response") else None

            if response is None:
                continue

            # Handle list format (streaming or single-request)
            if isinstance(response, list):
                for item in response:
                    # Handle JSONL string format (streaming)
                    if isinstance(item, str):
                        try:
                            parsed_item = orjson.loads(item.strip())
                            job = process_response_item(parsed_item, url)
                            if job:
                                normalized_jobs.append(job)
                        except (orjson.JSONDecodeError, TypeError):
                            continue
                    # Handle nested list format (single-request: [[{...}]])
                    elif isinstance(item, list):
                        for sub_item in item:
                            if isinstance(sub_item, dict):
                                job = process_response_item(sub_item, url)
                                if job:
                                    normalized_jobs.append(job)
                    # Handle dict format in list
                    elif isinstance(item, dict):
                        job = process_response_item(item, url)
                        if job:
                            normalized_jobs.append(job)

            # Handle sync format (dict)
            elif isinstance(response, dict):
                job = process_response_item(response, url)
                if job:
                    normalized_jobs.append(job)

        # Return in format expected by _normalize_job_fields
        return {
            "scrape": {
                "items": {
                    "normalized": normalized_jobs,
                    "raw": [],
                }
            }
        }

    def _fake_ingest_jobs(self, jobs: List[Dict[str, Any]], site_id: str | None = None) -> None:
        """Mock ingest_jobs_from_scrape_step.

        IMPORTANT: This mock properly simulates the real workflow behavior:
        1. stored_scrapes contains FULL descriptions (for extractor testing)
        2. ingested_jobs contains TRUNCATED descriptions (for Convex jobs table testing)

        The truncation mirrors what happens in the real workflow before calling
        ingestJobsFromScrape - extractors operate on full descriptions, then
        truncation happens for the DB row.
        """
        if isinstance(jobs, list):
            # Calculate total cost from jobs
            total_cost = sum(
                job.get("cost_milli_cents", 0) or 0
                for job in jobs
                if isinstance(job, dict)
            )

            # Store FULL descriptions in stored_scrapes (pre-truncation)
            # This is what extractors see and operate on
            self.captured.stored_scrapes.append({
                "items": {"normalized": jobs},  # Full descriptions here
                "siteId": site_id,
                "costMilliCents": total_cost,
            })

            # Store TRUNCATED descriptions in ingested_jobs (post-truncation)
            # This mirrors what would be sent to Convex jobs table
            for job in jobs:
                if isinstance(job, dict):
                    # Create copy with truncated description
                    truncated_job = dict(job)
                    if "description" in truncated_job:
                        full_desc = truncated_job["description"]
                        truncated_desc = _truncate_description_for_ingest(full_desc)
                        truncated_job["description"] = truncated_desc
                        # Store full description word count for assertions
                        truncated_job["_full_description_word_count"] = len(full_desc.split()) if full_desc else 0
                    self.captured.ingested_jobs.append(truncated_job)

                    # If description was long, also record full description upload
                    if "description" in job:
                        full_desc = job.get("description", "")
                        if full_desc and len(full_desc.split()) > DESCRIPTION_PREVIEW_MAX_WORDS:
                            self.captured.description_uploads.append({
                                "url": job.get("url"),
                                "description": full_desc,
                                "word_count": len(full_desc.split()),
                            })

    def _fake_emit_telemetry(
        self,
        event: str,
        level: str = "info",
        site_url: str | None = None,
        data: Dict[str, Any] | None = None,
    ) -> None:
        """Mock emit_scrape_telemetry_step."""
        pass

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
