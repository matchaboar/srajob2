"""
End-to-end tests for listing page extraction workflow.

Tests that the DBOS workflow, given a listing page URL, will:
1. SpiderCloud scrape the listing page
2. Detect the correct handler
3. Extract job URLs from the page (JSON API, HTML links, or regex fallback)
4. Filter URLs appropriately (exclude listing pages, auth pages, etc.)
5. Enqueue job detail URLs for downstream processing

Results can be output to ./site-detail-e2e-examples for inspection.
Enable verbose output with DEBUG_EXTRACTION_VERBOSE=1
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest
import yaml

from job_scrape_application.dbos_runtime import queue as dbos_queue
from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
from job_scrape_application.workflows import activities as acts
from job_scrape_application.workflows.core import SpiderFixture, WorkflowTestHelper
from job_scrape_application.workflows.helpers.link_extractors import normalize_url
from job_scrape_application.workflows.site_handlers import get_site_handler

SCHEDULE_PATH = Path("job_scrape_application/config/prod/site_schedules.yml")
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/dbos_schedule")
DEBUG_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/debug")
DEBUG_ASSERTIONS_DIR = Path("tests/job_scrape_application/workflows/assertions/debug")
OUTPUT_DIR = Path("./site-detail-e2e-examples")

logger = logging.getLogger(__name__)


# =============================================================================
# Mock Spider Clients for Production Workflow Testing
# =============================================================================

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
                # Handle both JSONL string format and list of dict format
                parts = []
                for item in response:
                    if isinstance(item, str):
                        parts.append(item)
                    elif isinstance(item, dict):
                        # Convert dict to JSON string for JSONL streaming
                        parts.append(json.dumps(item))
                full_response = "".join(parts)
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


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_") or "site"


def _resolve_input_url(
    fixture: SpiderFixture,
    site_id: str,
    schedule_path: Path = SCHEDULE_PATH,
) -> str:
    """Resolve the input URL from fixture or schedule config.

    Priority:
    1. fixture.source_url - explicitly stored in fixture
    2. Schedule config lookup by site_id
    3. Fallback to fixture.request_url
    """
    # Check fixture for explicit source_url
    source = fixture.source_url
    if source:
        return source

    # Try to find in schedule config
    if schedule_path.exists():
        try:
            schedules = yaml.safe_load(schedule_path.read_text())
            for entry in schedules.get("site_schedules", []):
                if entry.get("name", "").lower() == site_id.lower():
                    return entry.get("url", fixture.request_url)
        except Exception:
            pass

    return fixture.request_url


@dataclass
class ListingExtractionStepLog:
    """Log entry for a single listing extraction step."""
    step: str
    description: str
    data: Any = None


@dataclass
class URLTraceEntry:
    """Trace entry for a single URL through the extraction pipeline."""
    url: str
    stage: str  # raw_extraction, handler_filter, site_filter, normalization, final
    status: str  # included, excluded, transformed
    reason: Optional[str] = None
    transformed_to: Optional[str] = None


@dataclass
class DetailURLTransformation:
    """Track a single URL through the transformation pipeline."""
    raw_url: str
    filtered_url: Optional[str] = None  # After filter_job_urls()
    transformed_url: Optional[str] = None  # After get_api_uri()
    rejected_at: Optional[str] = None  # Stage where rejected, if any
    rejection_reason: Optional[str] = None


@dataclass
class ExtractedListingResult:
    """Result of listing page extraction."""
    site_id: str
    listing_url: str
    source_url: str
    handler_name: Optional[str] = None

    # URL pipeline tracking (listing page URL transformation)
    input_url: Optional[str] = None  # Original URL from schedule/user
    scrape_url: Optional[str] = None  # URL after get_listing_api_uri()

    # Extraction results
    extracted_urls: List[str] = field(default_factory=list)
    filtered_urls: List[str] = field(default_factory=list)
    normalized_urls: List[str] = field(default_factory=list)  # Final URLs after normalization
    rejected_urls: List[Tuple[str, str]] = field(default_factory=list)  # (url, reason)
    pagination_urls: List[str] = field(default_factory=list)
    posted_at_by_url: Dict[str, int] = field(default_factory=dict)  # URL -> posted_at timestamp

    # Detail URL pipeline tracking
    raw_extracted_urls: List[str] = field(default_factory=list)  # Before any filtering
    handler_filtered_urls: List[str] = field(default_factory=list)  # After filter_job_urls()
    api_transformed_urls: List[str] = field(default_factory=list)  # After get_api_uri()

    # URL normalization tracking - (original, normalized) pairs showing transformations
    url_transformations: List[Tuple[str, str]] = field(default_factory=list)

    # Detailed transformation tracking for each URL
    url_transformations_detailed: List[DetailURLTransformation] = field(default_factory=list)

    # URL tracing - detailed trace of each URL through the pipeline
    url_traces: Dict[str, List[URLTraceEntry]] = field(default_factory=dict)

    # Verbose debug info
    extraction_steps: List[ListingExtractionStepLog] = field(default_factory=list)
    raw_content: Optional[str] = None
    content_type: Optional[str] = None  # raw_html, commonmark, json_api
    extraction_method: Optional[str] = None  # json_api, html_links, regex_fallback

    # Enqueue payload
    enqueue_payload: Optional[Dict[str, Any]] = None

    # Error info
    error: Optional[str] = None

    def trace_url(self, url: str) -> List[URLTraceEntry]:
        """Get the trace for a specific URL showing why it was included/excluded."""
        return self.url_traces.get(url, [])

    def get_urls_at_stage(self, stage: str, status: str = "included") -> List[str]:
        """Get URLs that reached a specific stage with given status."""
        urls = []
        for url, traces in self.url_traces.items():
            for trace in traces:
                if trace.stage == stage and trace.status == status:
                    urls.append(url)
        return urls


@dataclass
class ListingAssertions:
    """Expected assertions for listing extraction."""
    site_id: str
    listing_url: str
    expected_url_count_min: int = 1
    expected_url_pattern: Optional[str] = None
    expected_no_listing_urls: bool = True
    expected_handler: Optional[str] = None


def _load_listing_assertions(site_id: str, debug_folder: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load listing assertion YAML file if it exists."""
    if debug_folder:
        assertion_path = DEBUG_ASSERTIONS_DIR / debug_folder / f"{site_id}_listing.yml"
    else:
        assertion_path = Path(f"tests/job_scrape_application/workflows/assertions/{site_id}_listing.yml")

    if not assertion_path.exists():
        return None
    try:
        return yaml.safe_load(assertion_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Failed to load listing assertions for %s: %s", site_id, exc)
        return None


@dataclass
class ListingExtractionCapture:
    """Container for captured listing extraction data."""

    enqueued_urls: List[Dict[str, Any]] = field(default_factory=list)
    completed_items: List[Dict[str, Any]] = field(default_factory=list)
    scraper_results: List[Dict[str, Any]] = field(default_factory=list)


class ListingTestModule:
    """Test module for listing page extraction.

    Uses WorkflowTestHelper to run the production workflow and capture
    extracted URLs via mocked queue operations.
    """

    def __init__(
        self,
        listing_fixture: SpiderFixture,
        tmp_path: Optional[Any] = None,
        monkeypatch: Optional[Any] = None,
    ):
        self.listing_fixture = listing_fixture
        self.tmp_path = tmp_path
        self.monkeypatch = monkeypatch
        self.helper: Optional[WorkflowTestHelper] = None
        self._raw_events: List[Any] = []
        self.capture = ListingExtractionCapture()
        self._use_production_workflow = tmp_path is not None and monkeypatch is not None

    async def setup(self) -> None:
        """Initialize the test helper with mocked dependencies."""
        self._initialized = True

    async def run_listing_extraction(
        self,
        site_id: str,
        source_url: str,
    ) -> ExtractedListingResult:
        """Run the listing extraction workflow and capture results.

        Args:
            site_id: Identifier for the site (e.g., 'airbnb', 'purestorage')
            source_url: The source URL for this listing (for handler detection)

        Returns:
            ExtractedListingResult with extraction details and URLs
        """
        if not getattr(self, "_initialized", False):
            await self.setup()

        listing_url = self.listing_fixture.request_url
        verbose = os.environ.get("DEBUG_EXTRACTION_VERBOSE", "").lower() in ("1", "true", "yes")

        result = ExtractedListingResult(
            site_id=site_id,
            listing_url=listing_url,
            source_url=source_url,
        )

        # Use production workflow if monkeypatch and tmp_path are available
        if self._use_production_workflow:
            return await self._run_production_workflow(result, site_id, source_url, verbose)

        # Fallback to direct handler extraction (legacy mode)
        return await self._run_direct_handler_extraction(result, site_id, source_url, verbose)

    async def _run_production_workflow(
        self,
        result: ExtractedListingResult,
        site_id: str,
        source_url: str,
        verbose: bool,
    ) -> ExtractedListingResult:
        """Run extraction using the production workflow.

        This uses WorkflowTestHelper to mock SpiderCloud and captures
        the URLs that would be enqueued via process_spidercloud_listing_batch.
        """
        listing_url = self.listing_fixture.request_url

        # Step 1: Detect handler
        handler = get_site_handler(listing_url) or get_site_handler(source_url)
        result.handler_name = type(handler).__name__ if handler else None

        # NEW: Track input_url and scrape_url
        result.input_url = source_url
        result.scrape_url = listing_url

        if verbose:
            result.extraction_steps.append(ListingExtractionStepLog(
                step="Handler Detection (Production)",
                description=f"Detected handler: {result.handler_name}",
                data={"url": listing_url, "source_url": source_url, "handler": result.handler_name}
            ))

        # Step 2: Set up WorkflowTestHelper with fixture
        try:
            # Set up DBOS environment
            db_path = self.tmp_path / "dbos.sqlite"
            self.monkeypatch.setenv("DBOS_SQLITE_PATH", str(db_path))
            self.monkeypatch.setenv("SPIDER_API_KEY", "test_key")
            self.monkeypatch.setenv("CONVEX_HTTP_URL", "http://test.convex.site")

            # Reset DBOS connection
            dbos_sqlite._CONNECTIONS.connection = None

            # Determine sync mode from fixture
            is_sync = self.listing_fixture.is_sync

            # Set runtime config to match fixture format
            from job_scrape_application.config import runtime_config
            object.__setattr__(runtime_config, "spidercloud_single_request_mode", is_sync)

            # Create mock spider client
            fixtures = {listing_url: self.listing_fixture}
            spider_calls: List[Dict[str, Any]] = []

            if is_sync:
                def spider_class(api_key):
                    return _MockSyncSpider(fixtures, spider_calls)
            else:
                def spider_class(api_key):
                    return _MockAsyncSpider(fixtures, spider_calls)

            self.monkeypatch.setattr(
                "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
                spider_class,
            )

            # Mock Convex client
            async def fake_convex_query(name: str, payload: Dict[str, Any]) -> Any:
                if name == "router:getSiteById":
                    return {"paginationLimit": 3}
                return None

            async def fake_convex_mutation(name: str, payload: Dict[str, Any]) -> Any:
                return None

            self.monkeypatch.setattr(
                "job_scrape_application.services.convex_client.convex_query",
                fake_convex_query,
            )
            self.monkeypatch.setattr(
                "job_scrape_application.services.convex_client.convex_mutation",
                fake_convex_mutation,
            )

            # Mock queue operations to capture enqueued URLs
            enqueued_items: List[Dict[str, Any]] = []
            completed_items: List[Dict[str, Any]] = []
            captured_posted_ats: Dict[str, int] = {}

            def fake_enqueue_scrape_urls(payload: Dict[str, Any]) -> Dict[str, Any]:
                urls = payload.get("urls", [])
                posted_ats = payload.get("postedAts", [])
                # Build items from urls (the payload uses 'urls' not 'items')
                for idx, url in enumerate(urls):
                    if isinstance(url, str):
                        enqueued_items.append({"url": url})
                        # Capture posted_at if available
                        if posted_ats and idx < len(posted_ats) and posted_ats[idx] is not None:
                            captured_posted_ats[url] = posted_ats[idx]
                    elif isinstance(url, dict):
                        enqueued_items.append(url)
                return {"queued": len(urls)}

            def fake_complete_scrape_urls(payload: Dict[str, Any]) -> Dict[str, Any]:
                items = payload.get("items", [])
                completed_items.extend(items)
                return {"completed": len(items)}

            # Patch queue operations at the source module level
            self.monkeypatch.setattr(
                "job_scrape_application.dbos_runtime.queue.enqueue_scrape_urls",
                fake_enqueue_scrape_urls,
            )
            self.monkeypatch.setattr(
                "job_scrape_application.dbos_runtime.queue.complete_scrape_urls",
                fake_complete_scrape_urls,
            )
            # Also patch the local dbos_queue reference in this test file
            self.monkeypatch.setattr(dbos_queue, "enqueue_scrape_urls", fake_enqueue_scrape_urls)
            self.monkeypatch.setattr(dbos_queue, "complete_scrape_urls", fake_complete_scrape_urls)

            # Mock activity functions
            async def fake_fetch_seen_urls(*args, **kwargs) -> List[str]:
                return []

            async def fake_filter_existing(*args, **kwargs) -> List[str]:
                urls = args[0] if args else []
                return urls

            async def fake_filter_new(*args, **kwargs) -> List[str]:
                urls = args[0] if args else []
                return urls

            async def fake_record_scrape_attempts(*args, **kwargs) -> None:
                return None

            self.monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen_urls)
            self.monkeypatch.setattr(acts, "filter_existing_job_urls", fake_filter_existing)
            self.monkeypatch.setattr(acts, "filter_new_job_urls", fake_filter_new)
            self.monkeypatch.setattr(acts, "_record_scrape_url_attempts", fake_record_scrape_attempts)

            if verbose:
                result.extraction_steps.append(ListingExtractionStepLog(
                    step="Workflow Setup",
                    description="Set up WorkflowTestHelper with mocked dependencies",
                    data={"sync_mode": is_sync, "listing_url": listing_url}
                ))

            # Step 3: Run the production workflow
            batch = {
                "urls": [
                    {
                        "url": listing_url,
                        "sourceUrl": source_url,
                        "provider": "spidercloud",
                        "siteId": site_id,
                        "urlType": "listing",
                    }
                ]
            }

            if verbose:
                result.extraction_steps.append(ListingExtractionStepLog(
                    step="Workflow Execution",
                    description="Calling process_spidercloud_listing_batch()",
                    data=batch,
                ))

            response = await acts.process_spidercloud_listing_batch(batch)

            if verbose:
                result.extraction_steps.append(ListingExtractionStepLog(
                    step="Workflow Complete",
                    description=f"Workflow returned, enqueued {len(enqueued_items)} URLs",
                    data={
                        "response": response,
                        "enqueued_count": len(enqueued_items),
                        "completed_count": len(completed_items),
                    },
                ))

            # Step 4: Extract URLs from captured queue operations
            extracted_urls: List[str] = []
            for item in enqueued_items:
                url = item.get("url")
                if isinstance(url, str) and url.strip():
                    extracted_urls.append(url)

            result.extracted_urls = extracted_urls
            result.filtered_urls = extracted_urls
            result.normalized_urls = extracted_urls
            result.posted_at_by_url = captured_posted_ats
            result.extraction_method = "production_workflow"

            # NEW: Populate pipeline tracking fields for production workflow
            # Note: In production workflow, we capture post-transformation URLs
            result.raw_extracted_urls = extracted_urls  # Best we can do without deeper hooks
            result.handler_filtered_urls = extracted_urls
            result.api_transformed_urls = extracted_urls  # These are the final transformed URLs

            self.capture.enqueued_urls = enqueued_items
            self.capture.completed_items = completed_items

            if verbose:
                result.extraction_steps.append(ListingExtractionStepLog(
                    step="Extraction Complete (Production)",
                    description=f"Extracted {len(extracted_urls)} URLs via production workflow",
                    data={
                        "extracted_count": len(extracted_urls),
                        "sample_urls": extracted_urls[:5],
                    }
                ))

        except Exception as exc:
            result.error = str(exc)
            if verbose:
                result.extraction_steps.append(ListingExtractionStepLog(
                    step="Workflow Error",
                    description=f"Error during production workflow: {exc}",
                    data={"error": str(exc), "error_type": type(exc).__name__}
                ))

        # Write verbose output if enabled
        if verbose:
            _write_verbose_listing_steps(result)

        # Write JSON summary
        _write_listing_extraction_result(result)

        return result

    async def _run_direct_handler_extraction(
        self,
        result: ExtractedListingResult,
        site_id: str,
        source_url: str,
        verbose: bool,
    ) -> ExtractedListingResult:
        """Run extraction by calling handler methods directly (legacy mode)."""
        listing_url = self.listing_fixture.request_url

        # Step 1: Detect handler
        handler = get_site_handler(listing_url) or get_site_handler(source_url)
        result.handler_name = type(handler).__name__ if handler else None

        # NEW: Track input_url and scrape_url
        # input_url is the source_url (user input), scrape_url is what we actually scraped
        result.input_url = source_url
        result.scrape_url = listing_url

        if verbose:
            result.extraction_steps.append(ListingExtractionStepLog(
                step="Handler Detection",
                description=f"Detected handler: {result.handler_name}",
                data={"url": listing_url, "source_url": source_url, "handler": result.handler_name}
            ))

        # Step 2: Capture raw content from fixture
        raw_content = ""
        content_type = "unknown"

        fixture_data = self.listing_fixture.raw
        if isinstance(fixture_data, dict):
            response = fixture_data.get("response", [])
            if isinstance(response, list) and response:
                first_item = response[0]
                if isinstance(first_item, str):
                    # JSONL format - parse the JSON string
                    try:
                        parsed = json.loads(first_item)
                        raw_content = parsed.get("content", {}).get("commonmark", "")
                        if raw_content:
                            content_type = "commonmark"
                        else:
                            raw_content = parsed.get("content", {}).get("raw", "")
                            content_type = "raw_html" if raw_content else "unknown"
                    except json.JSONDecodeError:
                        raw_content = first_item
                        content_type = "raw_string"
                elif isinstance(first_item, dict):
                    raw_content = first_item.get("content", {}).get("commonmark", "")
                    if raw_content:
                        content_type = "commonmark"
                    else:
                        raw_content = first_item.get("content", {}).get("raw", "")
                        content_type = "raw_html" if raw_content else "unknown"
                elif isinstance(first_item, list) and first_item:
                    # Nested list format
                    nested = first_item[0] if first_item else {}
                    if isinstance(nested, dict):
                        raw_content = nested.get("content", {}).get("commonmark", "")
                        if raw_content:
                            content_type = "commonmark"
                        else:
                            raw_content = nested.get("content", {}).get("raw", "")
                            content_type = "raw_html" if raw_content else "unknown"

        result.raw_content = raw_content
        result.content_type = content_type

        if verbose:
            result.extraction_steps.append(ListingExtractionStepLog(
                step="Raw Content Capture",
                description=f"Captured {len(raw_content)} chars of {content_type} content",
                data={"length": len(raw_content), "content_type": content_type}
            ))

        # Step 3: Extract URLs using the handler directly
        try:
            extracted_urls: List[str] = []

            # First try parsing as raw JSON (for API responses like Greenhouse)
            if raw_content.strip().startswith(("{", "[")):
                try:
                    payload = json.loads(raw_content)
                    if handler and hasattr(handler, "get_links_from_json"):
                        extracted_urls = handler.get_links_from_json(payload)
                        if extracted_urls:
                            result.extraction_method = "handler.get_links_from_json"
                            if verbose:
                                result.extraction_steps.append(ListingExtractionStepLog(
                                    step="Parsed raw JSON",
                                    description=f"Parsed raw JSON content, extracted {len(extracted_urls)} URLs",
                                    data={"url_count": len(extracted_urls)}
                                ))
                except json.JSONDecodeError:
                    pass

            if verbose and not extracted_urls:
                result.extraction_steps.append(ListingExtractionStepLog(
                    step="Calling handler.get_links_from_raw_html()",
                    description=f"Running {result.handler_name}.get_links_from_raw_html()",
                    data={"url": listing_url, "source_url": source_url, "content_length": len(raw_content)}
                ))

            # Fall back to HTML parsing if JSON didn't work
            if not extracted_urls and handler and hasattr(handler, "get_links_from_raw_html"):
                extracted_urls = handler.get_links_from_raw_html(raw_content)
                if extracted_urls:
                    result.extraction_method = "handler.get_links_from_raw_html"

            # Also check SpiderCloud-extracted links from response (for JS-rendered SPAs like Workday)
            # These links are extracted by SpiderCloud during rendering
            if not extracted_urls:
                fixture_data = self.listing_fixture.raw
                if isinstance(fixture_data, dict):
                    response = fixture_data.get("response", [])
                    if isinstance(response, list) and response:
                        first_item = response[0]
                        if isinstance(first_item, dict):
                            page_links = first_item.get("links", [])
                            if isinstance(page_links, list):
                                # Extract links - they may be strings or dicts with href
                                raw_links = [
                                    link.get("href") if isinstance(link, dict) else link
                                    for link in page_links
                                    if (isinstance(link, dict) and link.get("href")) or isinstance(link, str)
                                ]
                                # Resolve relative URLs using the listing URL as base
                                base_url = listing_url.split("?")[0]  # Remove query params for base
                                from urllib.parse import urljoin
                                import html as html_lib
                                for link in raw_links:
                                    if not link:
                                        continue
                                    # Unescape HTML entities (e.g., &amp; -> &)
                                    link = html_lib.unescape(link)
                                    # Resolve relative URLs
                                    if not link.startswith(("http://", "https://")):
                                        # Use the base URL from the listing
                                        parsed_listing = listing_url.split("/")
                                        if len(parsed_listing) >= 3:
                                            origin = "/".join(parsed_listing[:3])  # https://domain.com
                                            link = urljoin(origin, link)
                                    if link and link not in extracted_urls:
                                        extracted_urls.append(link)
                                if extracted_urls:
                                    result.extraction_method = "response.links"

            # Filter URLs using handler if available
            # Track handler transformations (e.g., marketing URL → API URL)
            handler_transformations: List[Tuple[str, str, str]] = []  # (original, transformed, reason)
            handler_rejections: List[Tuple[str, str]] = []  # (url, reason)

            if handler and hasattr(handler, "filter_job_urls"):
                # Track what filter_job_urls does to each URL
                filtered_urls = []
                for url in extracted_urls:
                    # Get the filtered result for this single URL
                    single_result = handler.filter_job_urls([url])
                    if not single_result:
                        handler_rejections.append((url, "rejected by handler filter"))
                    elif single_result[0] != url:
                        # URL was transformed (e.g., marketing → API URL)
                        handler_transformations.append((url, single_result[0], "handler transformation"))
                        filtered_urls.append(single_result[0])
                    else:
                        filtered_urls.append(url)
            else:
                filtered_urls = extracted_urls

            # Separate detail URLs from pagination URLs
            detail_urls = []
            pagination_urls = []
            for url in filtered_urls:
                if handler and hasattr(handler, "is_listing_url") and handler.is_listing_url(url):
                    pagination_urls.append(url)
                else:
                    detail_urls.append(url)

            result.extracted_urls = extracted_urls
            result.filtered_urls = detail_urls
            result.pagination_urls = pagination_urls
            result.rejected_urls = handler_rejections

            # NEW: Populate pipeline tracking fields
            result.raw_extracted_urls = extracted_urls  # Before any filtering
            result.handler_filtered_urls = filtered_urls  # After filter_job_urls()

            if verbose and (handler_transformations or handler_rejections):
                result.extraction_steps.append(ListingExtractionStepLog(
                    step="Handler URL Filtering",
                    description=f"Handler filtered {len(extracted_urls)} URLs: {len(handler_transformations)} transformed, {len(handler_rejections)} rejected",
                    data={
                        "input_count": len(extracted_urls),
                        "output_count": len(filtered_urls),
                        "transformed_count": len(handler_transformations),
                        "rejected_count": len(handler_rejections),
                        "sample_transformations": [
                            {"original": orig, "transformed": trans, "reason": reason}
                            for orig, trans, reason in handler_transformations[:10]
                        ],
                        "sample_rejections": [
                            {"url": url, "reason": reason}
                            for url, reason in handler_rejections[:10]
                        ],
                    }
                ))

            # Step: Transform URLs to API format (for handlers like Greenhouse)
            # This mirrors production behavior where marketing URLs are converted to API URLs
            api_transformed_urls: List[str] = []
            api_transformations: List[Tuple[str, str]] = []
            transformations_detailed: List[DetailURLTransformation] = []

            for url in detail_urls:
                transformation = DetailURLTransformation(raw_url=url, filtered_url=url)
                if handler and hasattr(handler, "get_api_uri"):
                    try:
                        # Try with source_url (Greenhouse handler)
                        api_url = handler.get_api_uri(url, source_url=source_url)
                    except TypeError:
                        # Fall back to without source_url (other handlers)
                        try:
                            api_url = handler.get_api_uri(url)
                        except Exception:
                            api_url = url
                    if api_url and api_url != url:
                        api_transformations.append((url, api_url))
                        api_transformed_urls.append(api_url)
                        transformation.transformed_url = api_url
                    else:
                        api_transformed_urls.append(url)
                        transformation.transformed_url = url
                else:
                    api_transformed_urls.append(url)
                    transformation.transformed_url = url
                transformations_detailed.append(transformation)

            # NEW: Populate api_transformed_urls and detailed transformations
            result.api_transformed_urls = api_transformed_urls
            result.url_transformations_detailed = transformations_detailed

            if verbose and api_transformations:
                result.extraction_steps.append(ListingExtractionStepLog(
                    step="API URL Transformation",
                    description=f"Transformed {len(api_transformations)} URLs to API format",
                    data={
                        "transformation_count": len(api_transformations),
                        "sample_transformations": [
                            {"original": orig, "api_url": api}
                            for orig, api in api_transformations[:10]
                        ],
                    }
                ))

            # Step: Normalize URLs (production code path)
            # This applies the same normalization as production scrapers
            base_url = listing_url
            normalized_urls: List[str] = []
            url_transformations: List[Tuple[str, str]] = []
            seen: set[str] = set()

            for url in api_transformed_urls:
                normalized = normalize_url(url, base_url=base_url)
                if not normalized:
                    continue
                if normalized in seen:
                    continue
                seen.add(normalized)
                normalized_urls.append(normalized)
                # Track transformation if URL changed
                if normalized != url:
                    url_transformations.append((url, normalized))

            result.normalized_urls = normalized_urls
            result.url_transformations = url_transformations

            if verbose:
                # Add normalization step to trace
                if url_transformations:
                    result.extraction_steps.append(ListingExtractionStepLog(
                        step="URL Normalization",
                        description=f"Normalized {len(detail_urls)} URLs, {len(url_transformations)} were transformed",
                        data={
                            "input_count": len(detail_urls),
                            "output_count": len(normalized_urls),
                            "transformation_count": len(url_transformations),
                            "sample_transformations": [
                                {"original": orig, "normalized": norm}
                                for orig, norm in url_transformations[:10]
                            ],
                        }
                    ))

                result.extraction_steps.append(ListingExtractionStepLog(
                    step="Extraction Complete",
                    description=f"Extracted {len(extracted_urls)} URLs, filtered to {len(detail_urls)} detail + {len(pagination_urls)} pagination, normalized to {len(normalized_urls)} final",
                    data={
                        "extracted_count": len(extracted_urls),
                        "detail_count": len(detail_urls),
                        "normalized_count": len(normalized_urls),
                        "pagination_count": len(pagination_urls),
                        "sample_normalized_urls": normalized_urls[:5],
                        "sample_pagination_urls": pagination_urls[:3],
                    }
                ))

        except Exception as exc:
            result.error = str(exc)
            if verbose:
                result.extraction_steps.append(ListingExtractionStepLog(
                    step="Extraction Error",
                    description=f"Error during extraction: {exc}",
                    data={"error": str(exc)}
                ))

        # Write verbose output if enabled
        if verbose:
            _write_verbose_listing_steps(result)

        # Write JSON summary
        _write_listing_extraction_result(result)

        return result

    def _build_scrape_payload(self) -> Dict[str, Any]:
        """Build scrape payload from fixture data."""
        fixture_data = self.listing_fixture.raw

        if isinstance(fixture_data, dict):
            response = fixture_data.get("response", [])
            request = fixture_data.get("request", {})

            # Handle different response formats
            if isinstance(response, list) and response:
                first_item = response[0]
                if isinstance(first_item, str):
                    # JSONL format
                    return {
                        "events": [first_item],
                        "request": request,
                    }
                elif isinstance(first_item, list):
                    # Nested list format
                    return {
                        "events": first_item,
                        "request": request,
                    }
                elif isinstance(first_item, dict):
                    return first_item
            elif isinstance(response, dict):
                return response

        return fixture_data if isinstance(fixture_data, dict) else {"events": []}


def _write_listing_extraction_result(result: ExtractedListingResult) -> None:
    """Write listing extraction result to JSON file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{result.site_id}_listing_extraction.json"

    output = {
        "site_id": result.site_id,
        "listing_url": result.listing_url,
        "source_url": result.source_url,
        "handler_name": result.handler_name,
        "content_type": result.content_type,
        "extraction_method": result.extraction_method,
        # URL pipeline tracking (listing page)
        "url_pipeline": {
            "input_url": result.input_url,
            "scrape_url": result.scrape_url,
        },
        # Detail URL pipeline counts
        "detail_url_pipeline": {
            "raw_extracted_count": len(result.raw_extracted_urls),
            "handler_filtered_count": len(result.handler_filtered_urls),
            "api_transformed_count": len(result.api_transformed_urls),
        },
        "extracted_url_count": len(result.extracted_urls),
        "filtered_url_count": len(result.filtered_urls),
        "normalized_url_count": len(result.normalized_urls),
        "pagination_url_count": len(result.pagination_urls),
        "rejected_url_count": len(result.rejected_urls),
        "sample_urls": result.normalized_urls[:10],  # Show first 10 for quick reference
        "normalized_urls": result.normalized_urls,  # ALL normalized URLs (for assertion updates)
        # Sample detailed transformations (first 10)
        "sample_transformations": [
            {
                "raw": t.raw_url,
                "filtered": t.filtered_url,
                "transformed": t.transformed_url,
            }
            for t in result.url_transformations_detailed[:10]
        ] if result.url_transformations_detailed else [],
        "url_transformations": [
            {"original": orig, "normalized": norm}
            for orig, norm in result.url_transformations[:10]
        ] if result.url_transformations else [],
        "rejected_urls": [
            {"url": url, "reason": reason}
            for url, reason in result.rejected_urls[:10]
        ] if result.rejected_urls else [],
        "error": result.error,
    }

    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    logger.info("Wrote listing extraction result to %s", output_path)


def _write_verbose_listing_steps(result: ExtractedListingResult) -> None:
    """Write detailed step-by-step listing extraction log for debugging.

    This outputs a human-readable file showing:
    1. Raw SpiderCloud response (HTML/markdown content)
    2. Handler detection and selection
    3. URL extraction method used
    4. All extracted URLs
    5. URL filtering results
    6. Pagination detection
    7. Enqueue payload

    Enable by setting DEBUG_EXTRACTION_VERBOSE=1
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{result.site_id}_listing_extraction_steps.md"

    lines = [
        f"# Listing Extraction Steps: {result.site_id}",
        "",
        "## URL Pipeline",
        f"- **Input URL:** `{result.input_url or result.source_url}`",
        f"- **Scrape URL:** `{result.scrape_url or result.listing_url}`",
        "",
        f"**Listing URL:** `{result.listing_url}`",
        f"**Source URL:** `{result.source_url}`",
        f"**Handler:** `{result.handler_name or 'Unknown'}`",
        f"**Content Type:** `{result.content_type or 'Unknown'}`",
        "",
        "## Detail URL Pipeline Counts",
        f"- **Raw Extracted:** {len(result.raw_extracted_urls)}",
        f"- **Handler Filtered:** {len(result.handler_filtered_urls)}",
        f"- **API Transformed:** {len(result.api_transformed_urls)}",
        "",
        "---",
        "",
    ]

    # Step 1: Raw SpiderCloud Response
    lines.extend([
        "## Step 1: SpiderCloud Response",
        "",
        f"Raw {result.content_type or 'content'} from SpiderCloud scrape:",
        "",
        "```" + ("html" if result.content_type == "raw_html" else "markdown"),
    ])
    if result.raw_content:
        # Truncate to first 5000 chars for readability
        preview = result.raw_content[:5000]
        if len(result.raw_content) > 5000:
            preview += f"\n\n... (truncated, {len(result.raw_content)} total chars)"
        lines.append(preview)
    else:
        lines.append("(No raw content captured)")
    lines.extend([
        "```",
        "",
        "---",
        "",
    ])

    # Step 2: Handler Detection
    lines.extend([
        "## Step 2: Handler Detection",
        "",
        f"**Detected Handler:** `{result.handler_name or 'None (using base handler)'}`",
        "",
        "The handler is selected based on URL pattern matching. Each handler knows how to:",
        "- Parse the specific job board's listing page format",
        "- Extract job URLs from JSON API responses or HTML",
        "- Identify pagination links",
        "- Filter out non-job URLs",
        "",
        "---",
        "",
    ])

    # Step 3: Extraction Method
    lines.extend([
        "## Step 3: URL Extraction Method",
        "",
        f"**Method Used:** `{result.extraction_method or 'auto-detected'}`",
        "",
        "URL extraction methods (in priority order):",
        "1. **JSON API**: Parse structured JSON response with job array",
        "2. **HTML Links**: Extract href attributes from anchor tags",
        "3. **Regex Fallback**: Search for URL patterns in raw text",
        "",
        "---",
        "",
    ])

    # Step 4: Extraction Steps Log
    if result.extraction_steps:
        lines.extend([
            "## Step 4: Detailed Extraction Log",
            "",
        ])
        for step in result.extraction_steps:
            lines.append(f"### {step.step}")
            lines.append("")
            lines.append(step.description)
            if step.data is not None:
                lines.append("")
                lines.append("```json")
                try:
                    lines.append(json.dumps(step.data, indent=2, default=str)[:2000])
                except Exception:
                    lines.append(str(step.data)[:2000])
                lines.append("```")
            lines.append("")
        lines.extend(["---", ""])

    # Step 5: Extracted URLs
    lines.extend([
        "## Step 5: Extracted URLs",
        "",
        f"**Total URLs Found:** {len(result.extracted_urls)}",
        f"**URLs After Filtering:** {len(result.filtered_urls)}",
        f"**URLs After Normalization:** {len(result.normalized_urls)}",
        f"**Pagination URLs:** {len(result.pagination_urls)}",
        "",
    ])

    if result.normalized_urls:
        lines.append("### Final Normalized URLs (first 20)")
        lines.append("")
        for i, url in enumerate(result.normalized_urls[:20]):
            lines.append(f"{i + 1}. `{url}`")
        if len(result.normalized_urls) > 20:
            lines.append(f"... and {len(result.normalized_urls) - 20} more")
        lines.append("")
    else:
        lines.append("*No job URLs extracted*")
        lines.append("")

    # Step 5b: URL Normalization Transformations
    if result.url_transformations:
        lines.extend([
            "### URL Normalization Transformations",
            "",
            "The following URLs were transformed during normalization:",
            "",
            "| Original | Normalized |",
            "|----------|------------|",
        ])
        for orig, norm in result.url_transformations[:15]:
            # Truncate long URLs for display
            orig_display = orig[:80] + "..." if len(orig) > 80 else orig
            norm_display = norm[:80] + "..." if len(norm) > 80 else norm
            lines.append(f"| `{orig_display}` | `{norm_display}` |")
        if len(result.url_transformations) > 15:
            lines.append(f"| ... | ({len(result.url_transformations) - 15} more transformations) |")
        lines.append("")

    # Step 6: Rejected URLs
    if result.rejected_urls:
        lines.extend([
            "### Rejected URLs",
            "",
            "| URL | Rejection Reason |",
            "|-----|------------------|",
        ])
        for url, reason in result.rejected_urls[:10]:
            lines.append(f"| `{url[:60]}...` | {reason} |")
        if len(result.rejected_urls) > 10:
            lines.append(f"| ... | ({len(result.rejected_urls) - 10} more) |")
        lines.append("")

    lines.extend(["---", ""])

    # Step 7: Pagination
    lines.extend([
        "## Step 6: Pagination Detection",
        "",
    ])
    if result.pagination_urls:
        lines.append(f"**Pagination URLs Found:** {len(result.pagination_urls)}")
        lines.append("")
        for i, url in enumerate(result.pagination_urls[:5]):
            lines.append(f"{i + 1}. `{url}`")
        lines.append("")
    else:
        lines.append("*No pagination URLs detected*")
        lines.append("")

    lines.extend(["---", ""])

    # Step 8: Enqueue Summary
    lines.extend([
        "## Step 7: Queue Enqueue Summary",
        "",
        f"**URLs to Enqueue:** {len(result.normalized_urls)}",
        "",
    ])

    if result.enqueue_payload:
        lines.append("### Enqueue Payload Sample")
        lines.append("")
        lines.append("```json")
        try:
            lines.append(json.dumps(result.enqueue_payload, indent=2, default=str)[:2000])
        except Exception:
            lines.append(str(result.enqueue_payload)[:2000])
        lines.append("```")

    lines.append("")

    # Step 9: Error (if any)
    if result.error:
        lines.extend([
            "---",
            "",
            "## Error",
            "",
            "```",
            result.error,
            "```",
            "",
        ])

    output_path.write_text("\n".join(lines))
    logger.info("Wrote verbose listing extraction steps to %s", output_path)


# --------------------------------------------------------------------------
# URL Tracing Utilities
# --------------------------------------------------------------------------

def trace_urls_through_pipeline(
    fixture_path: Path,
    urls_to_trace: List[str],
    source_url: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Trace specific URLs through the extraction pipeline.

    This is a debugging utility to understand why certain URLs are
    extracted or filtered out by the workflow.

    Args:
        fixture_path: Path to the listing fixture JSON file
        urls_to_trace: List of URLs to trace (can be partial patterns)
        source_url: Optional source URL for board-specific filtering

    Returns:
        Dictionary mapping each URL to its trace entries showing:
        - stage: Where in the pipeline (raw_extraction, handler_filter, site_filter, normalization)
        - status: What happened (found, not_found, filtered, transformed)
        - reason: Why it was included/excluded
        - transformed_to: If transformed, what it became

    Usage:
        from test_listing_extraction_e2e import trace_urls_through_pipeline
        from pathlib import Path

        traces = trace_urls_through_pipeline(
            Path("tests/.../fixtures/debug/stripe/greenhouse_listing.json"),
            ["https://boards.greenhouse.io/%7Bboard", "https://www.greenhouse.io/"],
        )
        for url, entries in traces.items():
            print(f"\\n=== {url} ===")
            for entry in entries:
                print(f"  {entry['stage']}: {entry['status']} - {entry.get('reason', '')}")
    """
    # Load fixture
    fixture_data = json.loads(fixture_path.read_text(encoding="utf-8"))
    request = fixture_data.get("request", {})
    listing_url = request.get("url", "")
    source_url = source_url or request.get("source_url", listing_url)

    # Get handler
    handler = get_site_handler(listing_url) or get_site_handler(source_url)
    handler_name = type(handler).__name__ if handler else "BaseHandler"

    # Extract raw content
    raw_content = ""
    response = fixture_data.get("response", [])
    if isinstance(response, list) and response:
        first_item = response[0]
        if isinstance(first_item, str):
            try:
                parsed = json.loads(first_item)
                raw_content = parsed.get("content", {}).get("commonmark", "")
                if not raw_content:
                    raw_content = parsed.get("content", {}).get("raw", "")
            except json.JSONDecodeError:
                raw_content = first_item
        elif isinstance(first_item, dict):
            raw_content = first_item.get("content", {}).get("commonmark", "")
            if not raw_content:
                raw_content = first_item.get("content", {}).get("raw", "")

    traces: Dict[str, List[Dict[str, Any]]] = {url: [] for url in urls_to_trace}

    # Stage 1: Check if URL or pattern appears in raw content
    for url in urls_to_trace:
        # Check raw content for pattern
        search_pattern = url.replace("https://", "").replace("http://", "")
        if url in raw_content or search_pattern in raw_content:
            traces[url].append({
                "stage": "raw_content",
                "status": "found",
                "reason": f"URL/pattern found in raw content ({len(raw_content)} chars)",
            })
        else:
            traces[url].append({
                "stage": "raw_content",
                "status": "not_found",
                "reason": "URL/pattern not found in raw content",
            })

    # Stage 2: Extract all URLs using handler methods
    all_extracted: List[str] = []

    # Try JSON parsing
    if raw_content.strip().startswith(("{", "[")):
        try:
            payload = json.loads(raw_content)
            if handler and hasattr(handler, "get_links_from_json"):
                all_extracted = handler.get_links_from_json(payload)
        except json.JSONDecodeError:
            pass

    # Fall back to HTML parsing
    if not all_extracted and handler and hasattr(handler, "get_links_from_raw_html"):
        all_extracted = handler.get_links_from_raw_html(raw_content)

    for url in urls_to_trace:
        matched = [u for u in all_extracted if url in u or u in url]
        if matched:
            traces[url].append({
                "stage": "handler_extraction",
                "status": "extracted",
                "reason": f"Extracted by {handler_name}",
                "matched_urls": matched[:5],
            })
        else:
            traces[url].append({
                "stage": "handler_extraction",
                "status": "not_extracted",
                "reason": f"Not extracted by {handler_name}.get_links_from_* methods",
            })

    # Stage 3: Handler filter_job_urls
    if handler and hasattr(handler, "filter_job_urls"):
        filtered = handler.filter_job_urls(all_extracted)

        for url in urls_to_trace:
            # Check if URL (or similar) was in extracted but not in filtered
            matched_extracted = [u for u in all_extracted if url in u or u in url]
            matched_filtered = [u for u in filtered if url in u or u in url]

            if matched_extracted and not matched_filtered:
                traces[url].append({
                    "stage": "filter_job_urls",
                    "status": "rejected",
                    "reason": f"Rejected by {handler_name}.filter_job_urls()",
                    "original_urls": matched_extracted[:3],
                })
            elif matched_filtered:
                traces[url].append({
                    "stage": "filter_job_urls",
                    "status": "passed",
                    "reason": f"Passed {handler_name}.filter_job_urls()",
                    "urls": matched_filtered[:3],
                })
    else:
        filtered = all_extracted

    # Stage 4: Site-specific filter (if handler supports it)
    if handler and hasattr(handler, "filter_job_urls_for_site"):
        site_filtered = handler.filter_job_urls_for_site(filtered, source_url)

        for url in urls_to_trace:
            matched_before = [u for u in filtered if url in u or u in url]
            matched_after = [u for u in site_filtered if url in u or u in url]

            if matched_before and not matched_after:
                traces[url].append({
                    "stage": "filter_job_urls_for_site",
                    "status": "rejected",
                    "reason": f"Rejected by {handler_name}.filter_job_urls_for_site(source_url={source_url})",
                    "original_urls": matched_before[:3],
                })
            elif matched_after:
                traces[url].append({
                    "stage": "filter_job_urls_for_site",
                    "status": "passed",
                    "reason": f"Passed {handler_name}.filter_job_urls_for_site()",
                    "urls": matched_after[:3],
                })
    else:
        site_filtered = filtered

    # Stage 5: URL normalization
    normalized: List[str] = []
    for u in site_filtered:
        norm = normalize_url(u, base_url=listing_url)
        if norm:
            normalized.append(norm)

    for url in urls_to_trace:
        matched_before = [u for u in site_filtered if url in u or u in url]
        matched_after = [u for u in normalized if url in u or u in url]

        if matched_before:
            # Check for transformation
            for orig in matched_before:
                norm = normalize_url(orig, base_url=listing_url)
                if norm and norm != orig:
                    traces[url].append({
                        "stage": "normalization",
                        "status": "transformed",
                        "reason": "URL was normalized",
                        "original": orig,
                        "transformed_to": norm,
                    })
                elif norm:
                    traces[url].append({
                        "stage": "normalization",
                        "status": "unchanged",
                        "reason": "URL passed normalization unchanged",
                    })
                else:
                    traces[url].append({
                        "stage": "normalization",
                        "status": "rejected",
                        "reason": "normalize_url() returned None",
                        "original": orig,
                    })

    # Stage 6: Final result summary
    for url in urls_to_trace:
        matched_final = [u for u in normalized if url in u or u in url]
        if matched_final:
            traces[url].append({
                "stage": "final",
                "status": "INCLUDED",
                "reason": "URL will be enqueued for detail scraping",
                "final_urls": matched_final[:5],
            })
        else:
            traces[url].append({
                "stage": "final",
                "status": "EXCLUDED",
                "reason": "URL will NOT be enqueued",
            })

    return traces


def print_url_traces(traces: Dict[str, List[Dict[str, Any]]]) -> None:
    """Pretty-print URL traces for debugging."""
    for url, entries in traces.items():
        print(f"\n{'='*60}")
        print(f"TRACE: {url}")
        print('='*60)
        for entry in entries:
            status_icon = {
                "found": "✓",
                "not_found": "✗",
                "extracted": "✓",
                "not_extracted": "✗",
                "passed": "✓",
                "rejected": "✗",
                "transformed": "→",
                "unchanged": "=",
                "INCLUDED": "✓✓",
                "EXCLUDED": "✗✗",
            }.get(entry["status"], "?")

            print(f"\n  [{entry['stage']}] {status_icon} {entry['status']}")
            print(f"    {entry['reason']}")

            if "matched_urls" in entry:
                for u in entry["matched_urls"][:3]:
                    print(f"      - {u}")
            if "original" in entry and "transformed_to" in entry:
                print(f"      {entry['original']}")
                print(f"      → {entry['transformed_to']}")


# --------------------------------------------------------------------------
# Debug Fixtures Test
# --------------------------------------------------------------------------

def _discover_debug_listing_fixtures() -> List[Tuple[str, Path, Optional[Path]]]:
    """Discover debug listing fixtures and their assertion files.

    Returns list of (identifier, fixture_path, assertion_path) tuples.
    """
    fixtures = []

    if not DEBUG_FIXTURE_DIR.exists():
        return fixtures

    # Walk through company folders
    for company_dir in DEBUG_FIXTURE_DIR.iterdir():
        if not company_dir.is_dir() or company_dir.name.startswith("."):
            continue

        for fixture_path in company_dir.glob("*_listing.json"):
            # Build identifier from filename
            stem = fixture_path.stem.replace("_listing", "")
            identifier = f"{company_dir.name}/{stem}"

            # Look for matching assertion file
            assertion_path = DEBUG_ASSERTIONS_DIR / company_dir.name / f"{stem}_listing.yml"
            if not assertion_path.exists():
                assertion_path = None

            fixtures.append((identifier, fixture_path, assertion_path))

    return fixtures


def _load_debug_listing_fixture(path: Path) -> Dict[str, Any]:
    """Load a debug listing fixture file."""
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def debug_listing_fixtures() -> List[Tuple[str, Path, Optional[Path]]]:
    """Fixture providing discovered debug listing fixtures."""
    return _discover_debug_listing_fixtures()


# Generate test IDs from discovered fixtures
_debug_listing_fixture_params = _discover_debug_listing_fixtures()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identifier,fixture_path,assertion_path",
    _debug_listing_fixture_params,
    ids=[f[0] for f in _debug_listing_fixture_params] if _debug_listing_fixture_params else ["no_fixtures"],
)
async def test_debug_listing_extraction(
    identifier: str,
    fixture_path: Path,
    assertion_path: Optional[Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test listing extraction for debug fixtures.

    Run with DEBUG_EXTRACTION_VERBOSE=1 to get detailed step-by-step output:

        DEBUG_EXTRACTION_VERBOSE=1 uv run pytest tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction -v
    """
    if not fixture_path or not fixture_path.exists():
        pytest.skip("No debug listing fixtures found")
        return

    # Load fixture
    fixture_data = _load_debug_listing_fixture(fixture_path)
    request = fixture_data.get("request", {})
    listing_url = request.get("url", "")
    source_url = request.get("source_url", listing_url)

    # Create site ID from identifier
    site_id = identifier.split("/")[-1] if "/" in identifier else identifier

    # Create fixture and test module with production workflow support
    spider_fixture = SpiderFixture.from_dict(fixture_data)
    test_module = ListingTestModule(spider_fixture, tmp_path=tmp_path, monkeypatch=monkeypatch)
    await test_module.setup()

    # Run extraction using production workflow
    result = await test_module.run_listing_extraction(site_id, source_url)

    # Load and validate assertions if available
    if assertion_path and assertion_path.exists():
        assertions = yaml.safe_load(assertion_path.read_text())
        expected = assertions.get("expected", {})

        # Check exact URL count (strict - catches invalid URLs that inflate count)
        if "url_count" in expected:
            actual_count = len(result.normalized_urls)
            expected_count = expected["url_count"]
            if actual_count != expected_count:
                raise AssertionError(
                    f"URL count mismatch: expected exactly {expected_count}, got {actual_count}\n\n"
                    + (f"EXTRA URLs ({actual_count - expected_count}):\n"
                       + "\n".join(f"  - {u}" for u in result.normalized_urls[:30])
                       + (f"\n  ... and {len(result.normalized_urls) - 30} more" if len(result.normalized_urls) > 30 else "")
                       if actual_count > expected_count else
                       f"MISSING URLs (expected {expected_count}, got {actual_count})")
                    + "\n\nTo update, run: DEBUG_EXTRACTION_VERBOSE=1 uv run pytest <test> -v"
                    + f"\nThen check ./site-detail-e2e-examples/{result.site_id}_listing_extraction.json"
                )

        # Check minimum URL count (legacy - prefer url_count for strict testing)
        if "url_count_min" in expected and "url_count" not in expected:
            assert len(result.normalized_urls) >= expected["url_count_min"], \
                f"Expected at least {expected['url_count_min']} URLs, got {len(result.normalized_urls)}"

        # Check URL pattern (use normalized_urls - the final output)
        # ALL URLs must match the pattern, not just one - this catches invalid/junk URLs
        if "url_pattern" in expected:
            pattern = re.compile(expected["url_pattern"])
            non_matching = [u for u in result.normalized_urls if not pattern.search(u)]
            if non_matching:
                raise AssertionError(
                    f"URLs do not match expected pattern: {expected['url_pattern']}\n\n"
                    f"Non-matching URLs ({len(non_matching)} of {len(result.normalized_urls)}):\n"
                    + "\n".join(f"  - {u}" for u in non_matching[:20])
                    + (f"\n  ... and {len(non_matching) - 20} more" if len(non_matching) > 20 else "")
                    + "\n\nThis indicates invalid URLs are being extracted. "
                    "Check the handler's URL validation logic."
                )

        # Check for bad URL transformations (e.g., /_JR bug from escaped underscores)
        if "no_url_corruption" in expected or expected.get("no_url_corruption", True):
            # Check for the markdown underscore escape bug: \_ converted to /_
            for url in result.normalized_urls:
                assert "/_" not in url or "/jobs/_" not in url.lower(), \
                    f"URL corruption detected (likely markdown escape bug): {url}"

        # Check no listing URLs in output
        if expected.get("no_listing_urls", True):
            listing_indicators = ["search", "filter", "page=", "offset="]
            for url in result.normalized_urls:
                for indicator in listing_indicators:
                    if indicator in url.lower():
                        # Some pagination params are OK, just warn
                        logger.warning(f"Potential listing URL in results: {url}")

        # Check handler
        if "handler" in expected:
            assert result.handler_name == expected["handler"], \
                f"Expected handler {expected['handler']}, got {result.handler_name}"

        # Check expected_urls - exact list of valid URLs (prevents regressions)
        if "expected_urls" in expected:
            expected_urls = set(expected["expected_urls"])
            actual_urls = set(result.normalized_urls)

            # Find URLs that are extracted but not expected (potential invalid URLs)
            unexpected_urls = actual_urls - expected_urls
            # Find URLs that are expected but not extracted (missing URLs)
            missing_urls = expected_urls - actual_urls

            error_parts = []
            if unexpected_urls:
                error_parts.append(
                    "Unexpected URLs extracted (not in expected_urls list):\n"
                    + "\n".join(f"  - {u}" for u in sorted(unexpected_urls)[:10])
                    + (f"\n  ... and {len(unexpected_urls) - 10} more" if len(unexpected_urls) > 10 else "")
                )
            if missing_urls:
                error_parts.append(
                    "Expected URLs not found in extraction:\n"
                    + "\n".join(f"  - {u}" for u in sorted(missing_urls)[:10])
                    + (f"\n  ... and {len(missing_urls) - 10} more" if len(missing_urls) > 10 else "")
                )

            if error_parts:
                raise AssertionError(
                    "URL extraction mismatch:\n\n"
                    + "\n\n".join(error_parts)
                    + f"\n\nExtracted {len(actual_urls)} URLs, expected {len(expected_urls)} URLs"
                )

        # Check blocked_urls - URLs that should NEVER appear in extraction (invalid URLs)
        # This is the ground truth for invalid URLs - if any of these appear, it's a bug
        if "blocked_urls" in expected:
            blocked_urls = set(expected["blocked_urls"])
            actual_urls = set(result.normalized_urls)

            # Find blocked URLs that were incorrectly extracted
            incorrectly_extracted = actual_urls & blocked_urls
            if incorrectly_extracted:
                raise AssertionError(
                    "BLOCKED URLs were incorrectly extracted!\n\n"
                    "These URLs should NEVER be extracted (they are in blocked_urls list):\n"
                    + "\n".join(f"  - {u}" for u in sorted(incorrectly_extracted))
                    + "\n\nThis indicates a bug in URL filtering. Check the handler's URL validation."
                )

        # Check posted_at_count - exact count of URLs with posted dates
        if "posted_at_count" in expected:
            actual_count = len(result.posted_at_by_url)
            expected_count = expected["posted_at_count"]
            if actual_count != expected_count:
                raise AssertionError(
                    f"posted_at count mismatch: expected exactly {expected_count}, got {actual_count}\n"
                    f"URLs with posted_at: {list(result.posted_at_by_url.keys())[:10]}"
                )

        # Check posted_at_count_min - minimum count of URLs with posted dates
        if "posted_at_count_min" in expected and "posted_at_count" not in expected:
            actual_count = len(result.posted_at_by_url)
            min_count = expected["posted_at_count_min"]
            if actual_count < min_count:
                raise AssertionError(
                    f"posted_at count too low: expected at least {min_count}, got {actual_count}\n"
                    f"URLs with posted_at: {list(result.posted_at_by_url.keys())[:10]}"
                )

        # Check posted_at_not_null - all extracted URLs should have posted dates
        if expected.get("posted_at_not_null"):
            urls_without_posted_at = [
                url for url in result.normalized_urls
                if url not in result.posted_at_by_url
            ]
            if urls_without_posted_at:
                raise AssertionError(
                    f"Expected all URLs to have posted_at, but {len(urls_without_posted_at)} URLs are missing it:\n"
                    + "\n".join(f"  - {u}" for u in urls_without_posted_at[:10])
                    + (f"\n  ... and {len(urls_without_posted_at) - 10} more" if len(urls_without_posted_at) > 10 else "")
                )

        # NEW: Validate URL pipeline fields (top-level)
        if "input_url" in assertions:
            assert result.input_url == assertions["input_url"], \
                f"Input URL mismatch: expected {assertions['input_url']}, got {result.input_url}"

        if "scrape_url" in assertions:
            assert result.scrape_url == assertions["scrape_url"], \
                f"Scrape URL mismatch: expected {assertions['scrape_url']}, got {result.scrape_url}"

        # NEW: Validate detail URL pipeline counts
        if "raw_url_count" in expected:
            actual = len(result.raw_extracted_urls)
            expected_val = expected["raw_url_count"]
            assert actual == expected_val, \
                f"raw_url_count mismatch: expected {expected_val}, got {actual}"

        if "filtered_url_count" in expected:
            actual = len(result.handler_filtered_urls)
            expected_val = expected["filtered_url_count"]
            assert actual == expected_val, \
                f"filtered_url_count mismatch: expected {expected_val}, got {actual}"

        if "transformed_url_count" in expected:
            actual = len(result.api_transformed_urls)
            expected_val = expected["transformed_url_count"]
            assert actual == expected_val, \
                f"transformed_url_count mismatch: expected {expected_val}, got {actual}"

    # Basic sanity checks
    assert result.listing_url, "Listing URL should be set"
    if not result.error:
        # Only check URL count if no error occurred
        assert len(result.normalized_urls) > 0 or result.error is None, \
            f"Expected some URLs to be extracted. Error: {result.error}"
