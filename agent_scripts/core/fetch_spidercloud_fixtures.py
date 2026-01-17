"""
Fetch live SpiderCloud scrape payloads and write them into our test fixtures.

Usage:
    uv run agent_scripts/fetch_spidercloud_fixtures.py

Requires:
    - SPIDER_API_KEY in .env (loaded via python-dotenv)
Outputs:
    - tests/job_scrape_application/workflows/fixtures/spidercloud_greenhouse_api_raw.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_greenhouse_api_commonmark.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_datadog_greenhouse_listing_raw.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_pinterest_marketing_commonmark.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_bloomberg_avature_search_page_1.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_bloomberg_avature_search_page_2.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_bloomberg_avature_search_page_3.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_godaddy_search_page_1.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_godaddy_job_detail_commonmark.json
    - tests/job_scrape_application/workflows/fixtures/spidercloud_confluent_engineering_commonmark.json
    - tests/fixtures/spidercloud_lambda_ai_careers.json
"""

from __future__ import annotations

import argparse
import asyncio
import orjson
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin

import yaml
from dotenv import load_dotenv
from spider import AsyncSpider as SpiderAsyncSpider

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.config import get_env_dir
from job_scrape_application.workflows import activities as workflow_activities
from job_scrape_application.workflows.core import CapturingSpiderClient
from job_scrape_application.workflows.helpers.job_url_extractor import extract_job_urls_from_scrape
from job_scrape_application.workflows.scrapers import spidercloud_scraper
from job_scrape_application.workflows.site_handlers import get_site_handler
from job_scrape_application.workflows.site_handlers import AvatureHandler

GH_JOB_URL = "https://boards-api.greenhouse.io/v1/boards/pinterest/jobs/5572858"
DATADOG_GREENHOUSE_LISTING_URL = "https://api.greenhouse.io/v1/boards/datadog/jobs"
PINTEREST_MARKETING_URL = "https://www.pinterestcareers.com/jobs/?gh_jid=5572858"
CONFLUENT_ENGINEERING_URL = (
    "https://careers.confluent.io/jobs/united_states-engineering?engineering=engineering"
)
LAMBDA_CAREERS_URL = "https://lambda.ai/careers"
AVATURE_BLOOMBERG_URL = (
    "https://bloomberg.avature.net/careers/SearchJobs/engineer?"
    "1845=%5B162508%5D&1845_format=3996&1686=%5B57029%5D&1686_format=2312&"
    "listFilterMode=1&jobRecordsPerPage=12"
)
AVATURE_BLOOMBERG_PAGE_2_URL = f"{AVATURE_BLOOMBERG_URL}&jobOffset=12"
AVATURE_BLOOMBERG_PAGE_3_URL = f"{AVATURE_BLOOMBERG_URL}&jobOffset=24"
GODADDY_SEARCH_URL = "https://careers.godaddy/jobs/search?page=1&query=engineer&country_codes%5B%5D=US"
GODADDY_DETAIL_URL = "https://careers.godaddy/jobs/principal-security-engineer-florida-united-states"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

FIXTURES: Tuple[Tuple[str, str, Dict[str, Any]], ...] = (
    (
        "spidercloud_greenhouse_api_raw.json",
        "/scrape",
        {"return_format": "raw_html", "url": GH_JOB_URL, "limit": 1},
    ),
    (
        "spidercloud_greenhouse_api_commonmark.json",
        "/scrape",
        {"return_format": "commonmark", "url": GH_JOB_URL, "limit": 1},
    ),
    (
        "spidercloud_pinterest_marketing_commonmark.json",
        "/scrape",
        {"return_format": "commonmark", "url": PINTEREST_MARKETING_URL, "limit": 1},
    ),
    (
        "spidercloud_bloomberg_avature_search_page_1.json",
        "/scrape",
        {"return_format": "raw_html", "url": AVATURE_BLOOMBERG_URL, "limit": 1},
    ),
    (
        "spidercloud_bloomberg_avature_search_page_2.json",
        "/scrape",
        {"return_format": "raw_html", "url": AVATURE_BLOOMBERG_PAGE_2_URL, "limit": 1},
    ),
    (
        "spidercloud_bloomberg_avature_search_page_3.json",
        "/scrape",
        {"return_format": "raw_html", "url": AVATURE_BLOOMBERG_PAGE_3_URL, "limit": 1},
    ),
    (
        "spidercloud_godaddy_search_page_1.json",
        "/scrape",
        {"return_format": "raw_html", "url": GODADDY_SEARCH_URL, "limit": 1},
    ),
    (
        "spidercloud_godaddy_job_detail_commonmark.json",
        "/scrape",
        {"return_format": "commonmark", "url": GODADDY_DETAIL_URL, "limit": 1},
    ),
    (
        "spidercloud_datadog_greenhouse_listing_raw.json",
        "/scrape",
        {
            "url": DATADOG_GREENHOUSE_LISTING_URL,
            "return_format": ["raw"],
            "request": "basic",
            "metadata": True,
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
            "limit": 1,
        },
    ),
    (
        "spidercloud_confluent_engineering_commonmark.json",
        "/scrape",
        {
            "url": CONFLUENT_ENGINEERING_URL,
            "return_format": ["commonmark", "raw_html"],
            "request": "chrome",
            "metadata": True,
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
            "limit": 1,
        },
    ),
    (
        "tests/fixtures/spidercloud_lambda_ai_careers.json",
        "/scrape",
        {
            "url": LAMBDA_CAREERS_URL,
            "return_format": ["commonmark"],
            "request": "chrome",
            "metadata": True,
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
            "limit": 1,
        },
    ),
)

FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
SCHEDULE_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/dbos_schedule")
SINGLE_REQUEST_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/single_request")
GROUND_TRUTH_DIR = Path("tests/job_scrape_application/workflows/ground_truth")
# Legacy path - kept for backward compatibility
ASSERTIONS_DIR = GROUND_TRUTH_DIR

# Description truncation limits (must match scrape_utils.py)
DESCRIPTION_PREVIEW_MAX_WORDS = 100
DESCRIPTION_PREVIEW_MAX_BYTES = 4_000
DESCRIPTION_TRUNCATION_SUFFIX = "..."


def _generate_timestamp() -> str:
    """Generate a timestamp in the format YYYYMMDDTHHMMSS."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _relative_path(path: Path, base: Path | None = None) -> str:
    """Get a relative path string from the repo root."""
    if base is None:
        base = Path.cwd()
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def _create_timestamped_ground_truth_file(
    ground_truth_dir: Path,
    site_id: str,
    timestamp: str,
    detail_url: str,
    fixture_path: Path,
    full_description: str | None = None,
) -> Path:
    """Create a timestamped ground truth YAML file for the fixture.

    IMPORTANT: This function NEVER overwrites existing files. Each fixture
    generation creates a new timestamped ground_truth file, allowing multiple
    test runs with different fixture versions.

    Args:
        ground_truth_dir: Directory to store ground_truth files
        site_id: Site identifier (e.g., 'netflix')
        timestamp: Timestamp string (e.g., '20260116T120000')
        detail_url: Job detail URL being scraped
        fixture_path: Path to the corresponding fixture file
        full_description: Full job description (for computing preview metadata)

    Returns:
        Path to the created ground_truth file
    """
    fixture_rel_path = _relative_path(fixture_path)

    # Always create timestamped ground_truth files
    ground_truth_filename = f"{site_id}_{timestamp}.yml"
    ground_truth_path = ground_truth_dir / ground_truth_filename

    # SAFETY: Never overwrite - check if file exists and warn
    if ground_truth_path.exists():
        logger.warning(f"Ground truth file already exists, will not overwrite: {ground_truth_path}")
        # Return path but don't overwrite
        return ground_truth_path

    ground_truth_dir.mkdir(parents=True, exist_ok=True)

    # Compute description metadata
    full_desc_word_count = len(full_description.split()) if full_description else 0
    truncated_preview = _build_description_preview(full_description) if full_description else ""
    truncated_word_count = len(truncated_preview.rstrip(DESCRIPTION_TRUNCATION_SUFFIX).split()) if truncated_preview else 0

    stub_content = f"""# fixture_path: {fixture_rel_path}
# Generated: {datetime.now(timezone.utc).isoformat()}
# IMPORTANT: Update expected values based on actual job data in the fixture.
# This file is auto-generated - existing files are NEVER overwritten.
site_id: {site_id}
detail_url: {detail_url}
expected:
  title_contains: ""  # TODO: Add expected title substring
  company: ""  # TODO: Add expected company name
  location_contains: ""  # TODO: Add expected location substring
  is_remote: false  # TODO: Set expected remote status
  level: mid  # Options: junior, mid, senior, staff
  description_min_words: 50
  # Full description assertions - proves workflow posts to file storage
  full_description_word_count_min: {full_desc_word_count}  # Full description for file storage
  truncated_description_word_count_max: {min(truncated_word_count, DESCRIPTION_PREVIEW_MAX_WORDS)}  # Truncated for jobs table (max {DESCRIPTION_PREVIEW_MAX_WORDS})
  cost_milli_cents_min: 1
"""
    ground_truth_path.write_text(stub_content, encoding="utf-8")
    logger.info(f"Created ground truth file: {ground_truth_path}")
    return ground_truth_path


def _create_or_update_assertion_file(
    assertion_path: Path,
    site_id: str,
    detail_url: str,
    fixture_path: Path,
) -> None:
    """Create or update an assertion YAML file with fixture reference.

    DEPRECATED: Use _create_timestamped_ground_truth_file for new fixtures.
    This function is kept for backward compatibility with legacy fixtures.

    If the assertion file already exists, only update the fixture_path comment.
    If it doesn't exist, create a stub with basic structure.
    """
    fixture_rel_path = _relative_path(fixture_path)

    if assertion_path.exists():
        # Update existing assertion file with fixture reference comment
        content = assertion_path.read_text(encoding="utf-8")
        # Check if fixture reference already exists
        if "# fixture_path:" in content:
            # Update existing fixture_path comment
            lines = content.split("\n")
            new_lines = []
            for line in lines:
                if line.startswith("# fixture_path:"):
                    new_lines.append(f"# fixture_path: {fixture_rel_path}")
                else:
                    new_lines.append(line)
            content = "\n".join(new_lines)
        else:
            # Add fixture_path comment at the top
            content = f"# fixture_path: {fixture_rel_path}\n{content}"
        assertion_path.write_text(content, encoding="utf-8")
        logger.info(f"Updated assertion file with fixture reference: {assertion_path}")
    else:
        # Create new assertion file stub
        assertion_path.parent.mkdir(parents=True, exist_ok=True)
        stub_content = f"""# fixture_path: {fixture_rel_path}
# IMPORTANT: Update expected values based on actual job data in the fixture.
site_id: {site_id}
detail_url: {detail_url}
expected:
  title_contains: ""  # TODO: Add expected title substring
  company: ""  # TODO: Add expected company name
  location_contains: ""  # TODO: Add expected location substring
  is_remote: false  # TODO: Set expected remote status
  level: mid  # Options: junior, mid, senior, staff
  description_min_words: 50
  cost_milli_cents_min: 1
"""
        assertion_path.write_text(stub_content, encoding="utf-8")
        logger.info(f"Created assertion stub: {assertion_path}")


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_") or "site"


def _build_description_preview(description: str) -> str:
    """Build a truncated description preview for fixture metadata.

    This mirrors the truncation logic in scrape_utils.py to show what would
    be posted to the Convex jobs table (truncated) vs file storage (full).

    Returns:
        Truncated description with '...' suffix if truncated, or original if short enough.
    """
    if not description:
        return ""

    # Step 1: Truncate to word limit
    trimmed = description.strip()
    tokens = re.split(r"(\S+)", trimmed)
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


def _extract_full_description_from_payload(payload: Dict[str, Any]) -> str | None:
    """Extract the full description from a scrape response payload.

    This is used to include both the truncated and full description metadata
    in fixtures, proving that the workflow would post both to Convex.
    """
    if not isinstance(payload, dict):
        return None

    # Check items.normalized for job data
    items = payload.get("items")
    if isinstance(items, dict):
        normalized = items.get("normalized")
        if isinstance(normalized, list):
            for row in normalized:
                if isinstance(row, dict):
                    desc = row.get("description") or row.get("job_description")
                    if isinstance(desc, str) and desc.strip():
                        return desc.strip()

    # Check content.commonmark for raw content
    content = payload.get("content")
    if isinstance(content, dict):
        commonmark = content.get("commonmark")
        if isinstance(commonmark, str) and commonmark.strip():
            return commonmark.strip()

    return None
def _load_schedule_entries(env: str) -> List[Dict[str, Any]]:
    schedule_path = get_env_dir(env) / "site_schedules.yml"
    if not schedule_path.exists():
        raise SystemExit(f"Missing site schedule config: {schedule_path}")
    payload = yaml.safe_load(schedule_path.read_text(encoding="utf-8")) or {}
    entries = payload if isinstance(payload, list) else payload.get("site_schedules", [])
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict) and entry.get("enabled", True)]
def _detail_url_from_scrape(scrape_payload: Dict[str, Any], source_url: str) -> str | None:
    items = scrape_payload.get("items") if isinstance(scrape_payload, dict) else None
    if not isinstance(items, dict):
        return None
    normalized = items.get("normalized")
    if not isinstance(normalized, list):
        return None
    saw_description = False
    for row in normalized:
        if not isinstance(row, dict):
            continue
        description = row.get("description") or row.get("job_description")
        if not isinstance(description, str) or not description.strip():
            continue
        saw_description = True
        for key in ("url", "job_url", "absolute_url", "apply_url"):
            value = row.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if saw_description and source_url:
        return source_url
    return None
def _extract_listing_job_urls(
    payload: Dict[str, Any],
    source_url: str,
    pattern: str | None,
) -> List[str]:
    if not isinstance(payload, dict):
        return []
    extracted = extract_job_urls_from_scrape(payload)
    handler = get_site_handler(source_url) if source_url else None
    detail_override = _detail_url_from_scrape(payload, source_url) if not extracted else None
    if detail_override:
        return [detail_override]
    return workflow_activities._filter_job_urls(  # noqa: SLF001
        extracted,
        handler,
        workflow_activities._is_probable_listing_url,  # noqa: SLF001
        pattern=pattern,
        source_url=source_url,
    )


def _derive_apply_urls(urls: List[str], handler: Any | None) -> List[str]:
    apply_urls: List[str] = []
    seen: set[str] = set()
    for url in urls:
        if not isinstance(url, str) or not url.strip():
            continue
        candidate = None
        if handler and hasattr(handler, "get_company_uri"):
            try:
                candidate = handler.get_company_uri(url)
            except Exception:
                candidate = None
        if candidate and handler and hasattr(handler, "is_listing_url"):
            try:
                if handler.is_listing_url(candidate):
                    candidate = None
            except Exception:
                candidate = None
        final_url = candidate or url
        if final_url in seen:
            continue
        seen.add(final_url)
        apply_urls.append(final_url)
    return apply_urls
class _FixtureCaptureSpiderFactory:
    """Factory that creates CapturingSpiderClient instances for fixture generation.

    This wrapper is needed because the monkeypatch expects a callable that takes api_key
    and returns a spider client. It uses CapturingSpiderClient from the core module.
    """

    def __init__(self, captures: List[Dict[str, Any]]) -> None:
        self._captures = captures

    def __call__(self, api_key: str) -> CapturingSpiderClient:
        real_client = SpiderAsyncSpider(api_key=api_key)
        return CapturingSpiderClient(real_client, self._captures)
async def _collect_response(response: Any) -> Any:
    if hasattr(response, "__aiter__"):
        items = []
        async for item in response:
            items.append(item)
        return items
    if hasattr(response, "__await__"):
        return await response
    return response
def _normalize_capture(value: Any) -> Any:
    import types
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {key: _normalize_capture(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_normalize_capture(item) for item in value]
    # Handle async generators and other non-serializable types
    if isinstance(value, (types.AsyncGeneratorType, types.GeneratorType)):
        return None  # Can't serialize generators
    if hasattr(value, "__dict__"):
        # Try to convert objects to dicts
        try:
            return _normalize_capture(vars(value))
        except Exception:
            return str(value)
    return value
async def _capture_workflow_scrape(
    url: str,
    *,
    source_url: str,
    pattern: str | None,
    label: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Run workflow scrape and capture the SpiderCloud request/response.

    Uses CapturingSpiderClient from the core module to capture all SpiderCloud
    interactions for fixture generation.
    """
    captures: List[Dict[str, Any]] = []
    original_async_spider = spidercloud_scraper.AsyncSpider
    try:
        # Use the factory to create CapturingSpiderClient instances
        spidercloud_scraper.AsyncSpider = _FixtureCaptureSpiderFactory(captures)
        scraper = workflow_activities._make_spidercloud_scraper()
        payload = await scraper._scrape_urls_batch(
            [url],
            source_url=source_url,
            pattern=pattern,
        )
    finally:
        spidercloud_scraper.AsyncSpider = original_async_spider
    if not captures:
        raise SystemExit(f"No SpiderCloud response captured for {label} {url}")
    capture = _normalize_capture(captures[0])
    if not isinstance(capture, dict):
        raise SystemExit(f"Unexpected SpiderCloud capture for {label} {url}")
    return capture, payload
async def _fetch_schedule_fixtures(
    *,
    env: str,
    output_dir: Path,
    only: List[str] | None,
    limit: int | None,
    use_timestamps: bool = True,
) -> None:
    """Fetch SpiderCloud fixtures for scheduled sites.

    Args:
        env: Environment to load schedules from (dev/prod).
        output_dir: Directory to write fixtures to.
        only: Optional list of site slugs to include.
        limit: Optional maximum number of sites to process.
        use_timestamps: If True, include timestamp in filenames (e.g., netflix_20260116T120000_detail.json).
                       If False, use legacy format (e.g., netflix_detail.json).
    """
    entries = _load_schedule_entries(env)
    if only:
        allow = {_slugify(val) for val in only}
        entries = [
            entry
            for entry in entries
            if _slugify(str(entry.get("name") or entry.get("url") or "")) in allow
        ]
    if limit is not None:
        entries = entries[: max(limit, 0)]

    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate a single timestamp for all fixtures in this batch
    timestamp = _generate_timestamp() if use_timestamps else None

    for entry in entries:
        url = entry.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        slug = _slugify(str(entry.get("name") or url))
        pattern = entry.get("pattern") if isinstance(entry.get("pattern"), str) else None

        # Generate filenames with optional timestamp
        if timestamp:
            listing_filename = f"{slug}_{timestamp}_listing.json"
            detail_filename = f"{slug}_{timestamp}_detail.json"
        else:
            listing_filename = f"{slug}_listing.json"
            detail_filename = f"{slug}_detail.json"

        listing_path = output_dir / listing_filename
        detail_path = output_dir / detail_filename

        # SAFETY: Check if fixtures already exist - NEVER overwrite
        if listing_path.exists():
            logger.warning(f"Listing fixture already exists, skipping: {listing_path}")
            continue
        if detail_path.exists():
            logger.warning(f"Detail fixture already exists, skipping: {detail_path}")
            continue

        listing_capture_clean, listing_payload = await _capture_workflow_scrape(
            url,
            source_url=url,
            pattern=pattern,
            label="listing",
        )

        # Determine ground_truth path for listing fixture _meta
        if timestamp:
            listing_ground_truth_path = GROUND_TRUTH_DIR / f"{slug}_{timestamp}.yml"
        else:
            listing_ground_truth_path = GROUND_TRUTH_DIR / f"{slug}.yml"

        handler = get_site_handler(url)

        scraped_urls = extract_job_urls_from_scrape(listing_payload)
        listing_job_urls = _extract_listing_job_urls(listing_payload, url, pattern)
        if not listing_job_urls:
            raw_response = listing_capture_clean.get("response")
            if raw_response:
                listing_job_urls = _extract_listing_job_urls(
                    {"items": {"raw": raw_response}, "sourceUrl": url},
                    url,
                    pattern,
                )
                if not scraped_urls:
                    scraped_urls = extract_job_urls_from_scrape(
                        {"items": {"raw": raw_response}, "sourceUrl": url}
                    )

        listing_url_key = url.rstrip("/").lower()
        listing_job_urls = [
            job_url
            for job_url in listing_job_urls
            if isinstance(job_url, str) and job_url.rstrip("/").lower() != listing_url_key
        ]
        apply_urls = _derive_apply_urls(listing_job_urls, handler)

        # Add _meta to listing fixture
        listing_capture_with_meta = {
            "_meta": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "site_id": slug,
                "ground_truth_file": _relative_path(listing_ground_truth_path),
            },
            "scraped_urls": sorted(set(scraped_urls)),
            "normalized_urls": sorted(set(listing_job_urls)),
            "apply_urls": sorted(set(apply_urls)),
            **listing_capture_clean,
        }
        listing_path.write_text(
            orjson.dumps(listing_capture_with_meta, option=orjson.OPT_INDENT_2).decode("utf-8"),
            encoding="utf-8",
        )
        if not listing_job_urls:
            raise SystemExit(f"No job URLs extracted for listing {url}")

        detail_url: str | None = None
        for candidate in listing_job_urls:
            if not isinstance(candidate, str) or not candidate.strip():
                continue
            normalized = (
                candidate
                if candidate.startswith(("http://", "https://"))
                else urljoin(url, candidate)
            )
            candidate_handler = handler or get_site_handler(normalized)
            if candidate_handler:
                if candidate_handler.is_listing_url(normalized):
                    continue
            elif workflow_activities._is_probable_listing_url(normalized):  # noqa: SLF001
                continue
            detail_url = normalized
            break
        if detail_url is None:
            detail_url = listing_job_urls[0]
            if not detail_url.startswith(("http://", "https://")):
                detail_url = urljoin(url, detail_url)
        handler = get_site_handler(detail_url) or handler
        if handler and handler.name == "greenhouse":
            api_url = handler.get_api_uri(detail_url, source_url=url)
            if api_url:
                detail_url = api_url

        detail_capture_clean, detail_payload = await _capture_workflow_scrape(
            detail_url,
            source_url=url,
            pattern=pattern,
            label="detail",
        )

        # Extract full description from detail response for metadata
        full_description = _extract_full_description_from_payload(detail_payload)
        if not full_description:
            # Try extracting from capture response
            response = detail_capture_clean.get("response")
            if isinstance(response, dict):
                full_description = _extract_full_description_from_payload(response)

        # Compute description truncation metadata
        full_desc_word_count = len(full_description.split()) if full_description else 0
        truncated_preview = _build_description_preview(full_description) if full_description else ""
        truncated_word_count = len(truncated_preview.split()) if truncated_preview else 0
        is_truncated = truncated_preview.endswith(DESCRIPTION_TRUNCATION_SUFFIX) if truncated_preview else False

        # Determine ground_truth path
        if timestamp:
            ground_truth_path = GROUND_TRUTH_DIR / f"{slug}_{timestamp}.yml"
        else:
            ground_truth_path = GROUND_TRUTH_DIR / f"{slug}.yml"

        # Add _meta to detail fixture with description truncation info
        detail_capture_with_meta = {
            "_meta": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "site_id": slug,
                "ground_truth_file": _relative_path(ground_truth_path),
                "listing_fixture": _relative_path(listing_path),
                # Description metadata - proves workflow posts both truncated AND full
                "description_info": {
                    "full_word_count": full_desc_word_count,
                    "truncated_word_count": truncated_word_count,
                    "is_truncated": is_truncated,
                    "truncated_preview_sample": truncated_preview[:200] if truncated_preview else None,
                },
            },
            **detail_capture_clean,
        }
        detail_path.write_text(
            orjson.dumps(detail_capture_with_meta, option=orjson.OPT_INDENT_2).decode("utf-8"),
            encoding="utf-8",
        )

        # Create timestamped ground_truth file (NEVER overwrites)
        if timestamp:
            _create_timestamped_ground_truth_file(
                ground_truth_dir=GROUND_TRUTH_DIR,
                site_id=slug,
                timestamp=timestamp,
                detail_url=detail_url,
                fixture_path=detail_path,
                full_description=full_description,
            )
        else:
            # Legacy mode: use old assertion file creation
            _create_or_update_assertion_file(
                assertion_path=ground_truth_path,
                site_id=slug,
                detail_url=detail_url,
                fixture_path=detail_path,
            )

        logger.info(f"Wrote {listing_path} and {detail_path}")
async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch SpiderCloud fixtures for tests.")
    parser.add_argument(
        "--only",
        nargs="*",
        help="Optional fixture filenames to refresh (e.g. spidercloud_godaddy_search_page_1.json).",
    )
    parser.add_argument(
        "--schedule-env",
        choices=["dev", "prod"],
        help="Generate fixtures for the site schedule in this environment.",
    )
    parser.add_argument(
        "--schedule-only",
        nargs="*",
        help="Optional site names/slugs to include when fetching schedule fixtures.",
    )
    parser.add_argument(
        "--schedule-limit",
        type=int,
        help="Optional limit on number of schedule sites to fetch.",
    )
    parser.add_argument(
        "--schedule-out",
        default=str(SCHEDULE_FIXTURE_DIR),
        help="Output directory for schedule fixtures.",
    )
    parser.add_argument(
        "--single-request-mode",
        action="store_true",
        default=True,
        help="Generate fixtures for single request mode (stream=False). Default: True",
    )
    parser.add_argument(
        "--legacy-stream-mode",
        action="store_true",
        help="Generate fixtures for legacy streaming mode (stream=True, JSONL).",
    )
    parser.add_argument(
        "--no-timestamps",
        action="store_true",
        help="Disable timestamps in fixture filenames (use legacy naming like netflix_detail.json).",
    )
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    # Determine fixture mode (single request vs legacy streaming)
    use_single_request = args.single_request_mode and not args.legacy_stream_mode

    # Set runtime config to match requested mode
    # This affects how _capture_workflow_scrape calls the scraper
    from job_scrape_application.config import runtime_config as _rc
    object.__setattr__(_rc, "spidercloud_single_request_mode", use_single_request)

    if args.schedule_env:
        # Determine output directory based on mode
        if use_single_request and args.schedule_out == str(SCHEDULE_FIXTURE_DIR):
            output_dir = SINGLE_REQUEST_FIXTURE_DIR
            logger.info("Using single request mode, output to: %s", output_dir)
        else:
            output_dir = Path(args.schedule_out)
            logger.info("Using %s mode, output to: %s",
                       "single request" if use_single_request else "legacy stream",
                       output_dir)

        # Log timestamp mode
        if args.no_timestamps:
            logger.info("Timestamps disabled: using legacy filenames (e.g., netflix_detail.json)")
        else:
            logger.info("Timestamps enabled: using timestamped filenames (e.g., netflix_20260116T120000_detail.json)")

        await _fetch_schedule_fixtures(
            env=args.schedule_env,
            output_dir=output_dir,
            only=args.schedule_only,
            limit=args.schedule_limit,
            use_timestamps=not args.no_timestamps,
        )
        return

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    async with SpiderAsyncSpider(api_key=api_key) as client:
        avature_handler = AvatureHandler()
        fixtures = list(FIXTURES)
        if args.only:
            allow = set(args.only)
            fixtures = [entry for entry in fixtures if entry[0] in allow]
            missing = allow.difference({entry[0] for entry in fixtures})
            if missing:
                raise SystemExit(f"Unknown fixture(s): {', '.join(sorted(missing))}")
        for filename, endpoint, payload in fixtures:
            path = Path(filename)
            if not path.is_absolute() and not str(path).startswith("tests/"):
                path = FIXTURE_DIR / filename
            try:
                url = payload.get("url")
                if not isinstance(url, str) or not url:
                    raise SystemExit(f"Fixture {filename} missing URL")
                params = {k: v for k, v in payload.items() if k != "url"}
                if avature_handler.matches_url(url):
                    params.update(avature_handler.get_spidercloud_config(url))
                return_format = params.get("return_format")
                if isinstance(return_format, str):
                    params["return_format"] = [return_format]
                logger.info(f"Fetching {url} -> {filename} via {endpoint}")
                response = await _collect_response(
                    client.scrape_url(
                        url,
                        params=params,
                        stream=False,
                        content_type="application/json",
                    )
                )
                fixture = {
                    "request": {"endpoint": endpoint, "url": url, "params": params},
                    "response": response,
                }
                path.write_text(
                    orjson.dumps(fixture, option=orjson.OPT_INDENT_2).decode("utf-8"),
                    encoding="utf-8",
                )
                logger.info(f"  wrote {path} ({len(orjson.dumps(fixture))} bytes)")
            except Exception as exc:  # noqa: BLE001
                logger.error(f"  failed to fetch {filename}: {exc}")
if __name__ == "__main__":
    asyncio.run(main())
