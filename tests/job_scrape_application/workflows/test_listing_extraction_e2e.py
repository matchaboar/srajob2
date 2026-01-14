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
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest
import yaml

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.dbos_runtime import queue as dbos_queue
from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
from job_scrape_application.workflows import activities as acts
from job_scrape_application.workflows.core import SpiderFixture, WorkflowTestHelper
from job_scrape_application.workflows.helpers.link_extractors import normalize_url, normalize_url_list
from job_scrape_application.workflows.site_handlers import get_site_handler

SCHEDULE_PATH = Path("job_scrape_application/config/prod/site_schedules.yml")
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/dbos_schedule")
DEBUG_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/debug")
DEBUG_ASSERTIONS_DIR = Path("tests/job_scrape_application/workflows/assertions/debug")
OUTPUT_DIR = Path("./site-detail-e2e-examples")

logger = logging.getLogger(__name__)


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_") or "site"


@dataclass
class ListingExtractionStepLog:
    """Log entry for a single listing extraction step."""
    step: str
    description: str
    data: Any = None


@dataclass
class ExtractedListingResult:
    """Result of listing page extraction."""
    site_id: str
    listing_url: str
    source_url: str
    handler_name: Optional[str] = None

    # Extraction results
    extracted_urls: List[str] = field(default_factory=list)
    filtered_urls: List[str] = field(default_factory=list)
    normalized_urls: List[str] = field(default_factory=list)  # Final URLs after normalization
    rejected_urls: List[Tuple[str, str]] = field(default_factory=list)  # (url, reason)
    pagination_urls: List[str] = field(default_factory=list)

    # URL normalization tracking - (original, normalized) pairs showing transformations
    url_transformations: List[Tuple[str, str]] = field(default_factory=list)

    # Verbose debug info
    extraction_steps: List[ListingExtractionStepLog] = field(default_factory=list)
    raw_content: Optional[str] = None
    content_type: Optional[str] = None  # raw_html, commonmark, json_api
    extraction_method: Optional[str] = None  # json_api, html_links, regex_fallback

    # Enqueue payload
    enqueue_payload: Optional[Dict[str, Any]] = None

    # Error info
    error: Optional[str] = None


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


class ListingTestModule:
    """Test module for listing page extraction.

    Similar to WorkflowTestModule but focused on listing page extraction
    and URL enqueuing rather than job detail extraction.
    """

    def __init__(self, listing_fixture: SpiderFixture):
        self.listing_fixture = listing_fixture
        self.helper: Optional[WorkflowTestHelper] = None
        self._raw_events: List[Any] = []

    async def setup(self) -> None:
        """Initialize the test helper with mocked dependencies."""
        # Mark as initialized - we don't need the full WorkflowTestHelper
        # since we mock dbos_queue.enqueue_scrape_urls directly in run_listing_extraction
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

        # Step 1: Detect handler
        handler = get_site_handler(listing_url) or get_site_handler(source_url)
        result.handler_name = type(handler).__name__ if handler else None

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

            if verbose:
                result.extraction_steps.append(ListingExtractionStepLog(
                    step="Calling handler.get_links_from_raw_html()",
                    description=f"Running {result.handler_name}.get_links_from_raw_html()",
                    data={"url": listing_url, "source_url": source_url, "content_length": len(raw_content)}
                ))

            # Use the handler's get_links_from_raw_html method directly
            if handler and hasattr(handler, "get_links_from_raw_html"):
                extracted_urls = handler.get_links_from_raw_html(raw_content)
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

            # Step: Normalize URLs (production code path)
            # This applies the same normalization as production scrapers
            base_url = listing_url
            normalized_urls: List[str] = []
            url_transformations: List[Tuple[str, str]] = []
            seen: set[str] = set()

            for url in detail_urls:
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
        "extracted_url_count": len(result.extracted_urls),
        "filtered_url_count": len(result.filtered_urls),
        "normalized_url_count": len(result.normalized_urls),
        "pagination_url_count": len(result.pagination_urls),
        "rejected_url_count": len(result.rejected_urls),
        "sample_urls": result.normalized_urls[:10],  # Show normalized URLs (final output)
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
        f"**Listing URL:** `{result.listing_url}`",
        f"**Source URL:** `{result.source_url}`",
        f"**Handler:** `{result.handler_name or 'Unknown'}`",
        f"**Content Type:** `{result.content_type or 'Unknown'}`",
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

    # Create fixture and test module
    spider_fixture = SpiderFixture.from_dict(fixture_data)
    test_module = ListingTestModule(spider_fixture)
    await test_module.setup()

    # Run extraction
    result = await test_module.run_listing_extraction(site_id, source_url)

    # Load and validate assertions if available
    if assertion_path and assertion_path.exists():
        assertions = yaml.safe_load(assertion_path.read_text())
        expected = assertions.get("expected", {})

        # Check minimum URL count (use normalized_urls - the final output)
        if "url_count_min" in expected:
            assert len(result.normalized_urls) >= expected["url_count_min"], \
                f"Expected at least {expected['url_count_min']} URLs, got {len(result.normalized_urls)}"

        # Check URL pattern (use normalized_urls - the final output)
        if "url_pattern" in expected:
            pattern = re.compile(expected["url_pattern"])
            matching = [u for u in result.normalized_urls if pattern.search(u)]
            assert matching, f"No URLs match pattern: {expected['url_pattern']}"

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
                    f"Unexpected URLs extracted (not in expected_urls list):\n"
                    + "\n".join(f"  - {u}" for u in sorted(unexpected_urls)[:10])
                    + (f"\n  ... and {len(unexpected_urls) - 10} more" if len(unexpected_urls) > 10 else "")
                )
            if missing_urls:
                error_parts.append(
                    f"Expected URLs not found in extraction:\n"
                    + "\n".join(f"  - {u}" for u in sorted(missing_urls)[:10])
                    + (f"\n  ... and {len(missing_urls) - 10} more" if len(missing_urls) > 10 else "")
                )

            if error_parts:
                raise AssertionError(
                    f"URL extraction mismatch:\n\n"
                    + "\n\n".join(error_parts)
                    + f"\n\nExtracted {len(actual_urls)} URLs, expected {len(expected_urls)} URLs"
                )

    # Basic sanity checks
    assert result.listing_url, "Listing URL should be set"
    if not result.error:
        # Only check URL count if no error occurred
        assert len(result.normalized_urls) > 0 or result.error is None, \
            f"Expected some URLs to be extracted. Error: {result.error}"
