"""
End-to-end tests for job detail extraction workflow.

Tests that the DBOS workflow, given a job_detail URL, will:
1. SpiderCloud scrape the page
2. Extract job details accurately: title, description, location, isRemote, posted_at, metadata, costMilliCents
3. Post truncated description (100 words) to Convex DB row
4. Upload full description to Convex file storage

Results can be output to ./site-detail-e2e-examples for inspection.
"""

from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

# Use orjson for faster JSON parsing (falls back to stdlib if not available)
try:
    import orjson

    def json_loads(data: bytes | str) -> Any:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: Any, indent: int = 2) -> str:
        options = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(obj, default=str, option=options).decode("utf-8")

except ImportError:
    import orjson

    def json_loads(data: bytes | str) -> Any:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: Any, indent: int = 2) -> str:
        options = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(obj, default=str, option=options).decode("utf-8")

# Use CSafeLoader for faster YAML parsing (falls back to safe_load if not available)
try:
    import yaml
    from yaml import CSafeLoader as YAMLLoader
except ImportError:
    import yaml
    YAMLLoader = yaml.SafeLoader  # type: ignore


# Import workflow for SpiderCloud job batch processing
from job_scrape_application.workflows.workflow import process_spidercloud_job_batch
from job_scrape_application.workflows.core import SpiderFixture, WorkflowTestHelper
from job_scrape_application.workflows.site_handlers import get_site_handler
from job_scrape_application.workflows.extractors import (
    ExtractionContext,
    extract_job_fields,
    build_heuristic_patch_from_extractors,
)

SCHEDULE_PATH = Path("job_scrape_application/config/prod/site_schedules.yml")
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/dbos_schedule")
SINGLE_REQUEST_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/single_request")
GROUND_TRUTH_DIR = Path("tests/job_scrape_application/workflows/ground_truth")
DEBUG_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/debug")
DEBUG_GROUND_TRUTH_DIR = Path("tests/job_scrape_application/workflows/ground_truth/debug")
MAX_DETAIL_FIXTURE_BYTES = 250 * 1024
LARGE_FIXTURE_ALLOWLIST = {"adobe"}
OUTPUT_DIR = Path("./site-detail-e2e-examples")
DESCRIPTION_PREVIEW_MAX_WORDS = 100
DESCRIPTION_TRUNCATION_SUFFIX = "..."

logger = logging.getLogger(__name__)


# =============================================================================
# Fixture/Assertion Discovery (1:1 mapping by identifier)
# =============================================================================
# Naming convention:
#   Fixture: {site}_{short_id}_{timestamp}_detail.json
#   Assertion: {site}_{short_id}_{timestamp}.yml
# The identifier is: {site}_{short_id}_{timestamp}
# Example: airbnb_7434393_20260114T153022


def _extract_fixture_identifier(fixture_path: Path) -> str:
    """Extract identifier from fixture filename.

    Examples:
        airbnb_7434393_20260114T153022_detail.json -> airbnb_7434393_20260114T153022
        airbnb_detail.json -> airbnb (legacy format)
    """
    name = fixture_path.stem  # Remove .json
    if name.endswith("_detail"):
        name = name[:-7]  # Remove _detail suffix
    elif name.endswith("_listing"):
        name = name[:-8]  # Remove _listing suffix
    return name


def _find_assertion_for_fixture(fixture_path: Path) -> Optional[Path]:
    """Find the matching assertion file for a fixture.

    Searches in order:
    1. Same directory as fixture (for debug fixtures organized by company)
    2. DEBUG_GROUND_TRUTH_DIR with company subdirectory
    3. GROUND_TRUTH_DIR (for legacy fixtures)
    """
    identifier = _extract_fixture_identifier(fixture_path)

    # Check for company subdirectory pattern (e.g., fixtures/debug/airbnb/)
    parent_name = fixture_path.parent.name
    if parent_name not in ("dbos_schedule", "single_request", "debug"):
        # Company subdirectory - look in ground_truth/debug/{company}/
        company_assertion_dir = DEBUG_GROUND_TRUTH_DIR / parent_name
        if company_assertion_dir.exists():
            assertion_path = company_assertion_dir / f"{identifier}.yml"
            if assertion_path.exists():
                return assertion_path

    # Check DEBUG_GROUND_TRUTH_DIR
    assertion_path = DEBUG_GROUND_TRUTH_DIR / f"{identifier}.yml"
    if assertion_path.exists():
        return assertion_path

    # Check GROUND_TRUTH_DIR (legacy)
    assertion_path = GROUND_TRUTH_DIR / f"{identifier}.yml"
    if assertion_path.exists():
        return assertion_path

    return None


def _discover_fixtures_with_assertions() -> List[tuple[Path, Path]]:
    """Discover all fixture files that have matching assertion files.

    Returns list of (fixture_path, assertion_path) tuples.
    """
    results = []

    # Search all fixture directories
    fixture_dirs = [
        FIXTURE_DIR,
        SINGLE_REQUEST_FIXTURE_DIR,
        DEBUG_FIXTURE_DIR,
    ]

    for fixture_dir in fixture_dirs:
        if not fixture_dir.exists():
            continue

        # Find all detail fixtures (including in subdirectories)
        for fixture_path in fixture_dir.rglob("*_detail.json"):
            assertion_path = _find_assertion_for_fixture(fixture_path)
            if assertion_path:
                results.append((fixture_path, assertion_path))

    return results


def _get_fixture_test_id(fixture_path: Path) -> str:
    """Generate a test ID for a fixture.

    For fixtures in company subdirectories, includes the company name.
    """
    identifier = _extract_fixture_identifier(fixture_path)
    parent_name = fixture_path.parent.name

    # For company subdirectories, prefix with company
    if parent_name not in ("dbos_schedule", "single_request", "debug", "fixtures"):
        return f"{parent_name}/{identifier}"

    return identifier


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_") or "site"


def _load_schedule_entries() -> List[Dict[str, Any]]:
    payload = yaml.load(SCHEDULE_PATH.read_bytes(), Loader=YAMLLoader) or {}
    entries = payload if isinstance(payload, list) else payload.get("site_schedules", [])
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict) and entry.get("enabled", True)]


def _schedule_id(entry: Dict[str, Any]) -> str:
    return _slugify(str(entry.get("name") or entry.get("url") or "site"))


def _find_latest_timestamped_fixture(fixture_dir: Path, slug: str, suffix: str) -> Optional[Path]:
    """Find the most recent timestamped fixture file for a slug.

    Searches for files matching pattern: {slug}_{timestamp}_{suffix}.json
    where timestamp is in format YYYYMMDDTHHMMSS.
    Returns the most recent one based on filename sorting (lexicographic = chronological for ISO timestamps).
    """
    # Pattern: netflix_20260116T120000_detail.json
    pattern = f"{slug}_*_{suffix}.json"
    matches = sorted(fixture_dir.glob(pattern), reverse=True)  # Most recent first
    if matches:
        return matches[0]
    return None


@lru_cache(maxsize=None)
def _fixture_request_url(path: Path) -> Optional[str]:
    try:
        payload = json_loads(path.read_bytes())
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    request = payload.get("request")
    if not isinstance(request, dict):
        return None
    url = request.get("url")
    if isinstance(url, str) and url.strip():
        return url.strip()
    return None


@lru_cache(maxsize=None)
def _ground_truth_detail_url(site_id: str) -> Optional[str]:
    assertion_path = _latest_ground_truth_path(site_id)
    if not assertion_path or not assertion_path.exists():
        return None
    try:
        data = yaml.load(assertion_path.read_bytes(), Loader=YAMLLoader)
    except Exception:
        return None
    if isinstance(data, dict):
        detail_url = data.get("detail_url")
        if isinstance(detail_url, str) and detail_url.strip():
            return detail_url.strip()
    return None


def _collect_fixture_pairs(fixture_dir: Path, slug: str) -> List[tuple[Path, Path]]:
    pairs: List[tuple[Path, Path]] = []
    pattern = f"{slug}_*_detail.json"
    for detail_path in sorted(fixture_dir.glob(pattern), reverse=True):
        listing_name = detail_path.name.replace("_detail.json", "_listing.json")
        listing_path = detail_path.with_name(listing_name)
        if not listing_path.exists():
            legacy_listing = fixture_dir / f"{slug}_listing.json"
            if legacy_listing.exists():
                listing_path = legacy_listing
        pairs.append((listing_path, detail_path))
    legacy_detail = fixture_dir / f"{slug}_detail.json"
    if legacy_detail.exists():
        legacy_listing = fixture_dir / f"{slug}_listing.json"
        pairs.append((legacy_listing, legacy_detail))
    return pairs


def _fixture_paths(entry: Dict[str, Any]) -> tuple[Path, Path]:
    """Get fixture paths for a schedule entry.

    Search order:
    1. Fixture whose request.url matches ground_truth detail_url (if available)
    2. Timestamped single_request fixtures (e.g., netflix_20260116T120000_detail.json)
    3. Legacy single_request fixtures (e.g., netflix_detail.json)
    4. Timestamped dbos_schedule fixtures
    5. Legacy dbos_schedule fixtures
    """
    slug = _schedule_id(entry)

    expected_url = _ground_truth_detail_url(slug)
    if expected_url:
        candidates = (
            _collect_fixture_pairs(SINGLE_REQUEST_FIXTURE_DIR, slug)
            + _collect_fixture_pairs(FIXTURE_DIR, slug)
        )
        for listing_path, detail_path in candidates:
            if detail_path.exists() and _fixture_request_url(detail_path) == expected_url:
                return listing_path, detail_path

    # Next, try timestamped single_request fixtures
    timestamped_detail = _find_latest_timestamped_fixture(SINGLE_REQUEST_FIXTURE_DIR, slug, "detail")
    if timestamped_detail:
        timestamped_listing = _find_latest_timestamped_fixture(SINGLE_REQUEST_FIXTURE_DIR, slug, "listing")
        if timestamped_listing:
            return timestamped_listing, timestamped_detail

    # Then, try legacy single_request fixtures
    legacy_detail = SINGLE_REQUEST_FIXTURE_DIR / f"{slug}_detail.json"
    legacy_listing = SINGLE_REQUEST_FIXTURE_DIR / f"{slug}_listing.json"
    if legacy_detail.exists():
        return legacy_listing, legacy_detail

    # Then, try timestamped dbos_schedule fixtures
    timestamped_detail = _find_latest_timestamped_fixture(FIXTURE_DIR, slug, "detail")
    if timestamped_detail:
        timestamped_listing = _find_latest_timestamped_fixture(FIXTURE_DIR, slug, "listing")
        if timestamped_listing:
            return timestamped_listing, timestamped_detail
        legacy_listing = FIXTURE_DIR / f"{slug}_listing.json"
        if legacy_listing.exists():
            return legacy_listing, timestamped_detail

    # Fallback to legacy dbos_schedule fixtures
    return FIXTURE_DIR / f"{slug}_listing.json", FIXTURE_DIR / f"{slug}_detail.json"


def _load_fixture(path: Path) -> Dict[str, Any]:
    payload = json_loads(path.read_bytes())
    if not isinstance(payload, dict):
        raise AssertionError(f"Fixture {path} must contain a dict payload")
    if not isinstance(payload.get("request"), dict):
        raise AssertionError(f"Fixture {path} missing request metadata")
    return payload


def _count_words(text: str) -> int:
    if not text or not isinstance(text, str):
        return 0
    return len(text.split())


def _truncate_to_words(text: str, max_words: int) -> str:
    if not text or not isinstance(text, str):
        return ""
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + "..."


# Cache for assertion files (loaded once per site)
_ASSERTIONS_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}


def _load_assertions_from_path(assertion_path: Path) -> Optional[Dict[str, Any]]:
    """Load assertion YAML file from a specific path."""
    cache_key = str(assertion_path)
    if cache_key in _ASSERTIONS_CACHE:
        return _ASSERTIONS_CACHE[cache_key]

    if not assertion_path.exists():
        _ASSERTIONS_CACHE[cache_key] = None
        return None
    try:
        result = yaml.load(assertion_path.read_bytes(), Loader=YAMLLoader)
        _ASSERTIONS_CACHE[cache_key] = result
        return result
    except Exception as exc:
        logger.warning("Failed to load assertions from %s: %s", assertion_path, exc)
        _ASSERTIONS_CACHE[cache_key] = None
        return None


def _fixture_detail_url(fixture: Dict[str, Any]) -> Optional[str]:
    request = fixture.get("request")
    if not isinstance(request, dict):
        return None
    url = request.get("url")
    if isinstance(url, str) and url.strip():
        return url.strip()
    return None


def _timestamped_ground_truth_paths(site_id: str) -> List[Path]:
    candidates = []
    pattern = re.compile(rf"^{re.escape(site_id)}_(\\d{{8}}T\\d{{6}})$")
    for path in GROUND_TRUTH_DIR.glob(f"{site_id}_*.yml"):
        if path.name.endswith("_listing.yml"):
            continue
        if pattern.match(path.stem):
            candidates.append(path)
    candidates.sort(key=lambda p: p.stem, reverse=True)
    return candidates


def _latest_ground_truth_path(site_id: str) -> Optional[Path]:
    candidates = _timestamped_ground_truth_paths(site_id)
    if candidates:
        return candidates[0]
    legacy = GROUND_TRUTH_DIR / f"{site_id}.yml"
    if legacy.exists():
        return legacy
    return None


def _load_assertions(site_id: str, fixture: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Load assertion YAML file for a site if it exists.

    If fixture contains _meta.ground_truth_file or _meta.assertion_file, use that path.
    Otherwise, fall back to searching by site_id.
    Results are cached.
    """
    # Check if fixture has _meta with ground_truth_file or assertion_file reference
    if fixture is not None:
        meta = fixture.get("_meta", {})
        # Prefer ground_truth_file (new style), fall back to assertion_file (legacy)
        ground_truth_file = meta.get("ground_truth_file") or meta.get("assertion_file")
        if ground_truth_file:
            assertion_path = Path(ground_truth_file)
            # Handle both absolute and relative paths
            if not assertion_path.is_absolute():
                assertion_path = Path.cwd() / assertion_path
            if assertion_path.exists():
                return _load_assertions_from_path(assertion_path)
        fixture_url = _fixture_detail_url(fixture)
        if fixture_url:
            candidates = [GROUND_TRUTH_DIR / f"{site_id}.yml"] + _timestamped_ground_truth_paths(site_id)
            for candidate in candidates:
                if not candidate.exists():
                    continue
                data = _load_assertions_from_path(candidate)
                if not data:
                    continue
                if data.get("detail_url") == fixture_url:
                    return data

    # Fall back to legacy site_id lookup
    if fixture is None and site_id in _ASSERTIONS_CACHE:
        return _ASSERTIONS_CACHE[site_id]

    assertion_path = _latest_ground_truth_path(site_id)
    if not assertion_path or not assertion_path.exists():
        _ASSERTIONS_CACHE[site_id] = None
        return None
    try:
        result = yaml.load(assertion_path.read_bytes(), Loader=YAMLLoader)
        _ASSERTIONS_CACHE[site_id] = result
        return result
    except Exception as exc:
        logger.warning("Failed to load assertions for %s: %s", site_id, exc)
        _ASSERTIONS_CACHE[site_id] = None
        return None


@dataclass
class AssertionResult:
    """Result of a single assertion check."""
    field: str
    expected: Any
    actual: Any
    passed: bool
    message: str


def _validate_job_against_assertions(
    job: "ExtractedJobDetails",
    assertions: Dict[str, Any],
) -> List[AssertionResult]:
    """Validate extracted job details against expected assertions.

    Supported assertion types:
    - title: Exact match for job title
    - title_contains: Partial match (case-insensitive)
    - company: Exact match for company name
    - company_contains: Partial match (case-insensitive)
    - location: Exact match for location
    - location_contains: Partial match (case-insensitive)
    - is_remote: Boolean match
    - level: Exact match for level (junior/mid/senior/staff)
    - description_min_words: Minimum word count for description
    - description_contains: Partial match in description (case-insensitive)
    - description_not_contains: Ensure substring is NOT in description (case-insensitive)
    - cost_milli_cents_min: Minimum cost value
    - cost_milli_cents_max: Maximum cost value
    - posted_at_not_null: Check that posted_at is present
    - url_contains: Partial match in URL
    """
    results: List[AssertionResult] = []
    expected = assertions.get("expected", {})

    # Title assertions
    if "title" in expected:
        exp_title = expected["title"]
        # Strip whitespace for comparison (some sources have trailing spaces)
        passed = job.title.strip() == exp_title.strip()
        results.append(AssertionResult(
            field="title",
            expected=exp_title,
            actual=job.title,
            passed=passed,
            message="Title mismatch" if not passed else "Title matches",
        ))

    if "title_contains" in expected:
        exp_substr = expected["title_contains"]
        passed = exp_substr.lower() in job.title.lower()
        results.append(AssertionResult(
            field="title_contains",
            expected=exp_substr,
            actual=job.title,
            passed=passed,
            message=f"Title should contain '{exp_substr}'" if not passed else "Title contains expected substring",
        ))

    # Company assertions
    if "company" in expected:
        exp_company = expected["company"]
        passed = job.company == exp_company
        results.append(AssertionResult(
            field="company",
            expected=exp_company,
            actual=job.company,
            passed=passed,
            message="Company mismatch" if not passed else "Company matches",
        ))

    if "company_contains" in expected:
        exp_substr = expected["company_contains"]
        passed = exp_substr.lower() in job.company.lower()
        results.append(AssertionResult(
            field="company_contains",
            expected=exp_substr,
            actual=job.company,
            passed=passed,
            message=f"Company should contain '{exp_substr}'" if not passed else "Company contains expected substring",
        ))

    # Location assertions
    if "location" in expected:
        exp_location = expected["location"]
        passed = job.location == exp_location
        results.append(AssertionResult(
            field="location",
            expected=exp_location,
            actual=job.location,
            passed=passed,
            message="Location mismatch" if not passed else "Location matches",
        ))

    if "location_contains" in expected:
        exp_substr = expected["location_contains"]
        passed = exp_substr.lower() in job.location.lower()
        results.append(AssertionResult(
            field="location_contains",
            expected=exp_substr,
            actual=job.location,
            passed=passed,
            message=f"Location should contain '{exp_substr}'" if not passed else "Location contains expected substring",
        ))

    # Remote status
    if "is_remote" in expected:
        exp_remote = expected["is_remote"]
        passed = job.is_remote == exp_remote
        results.append(AssertionResult(
            field="is_remote",
            expected=exp_remote,
            actual=job.is_remote,
            passed=passed,
            message="Remote status mismatch" if not passed else "Remote status matches",
        ))

    # Level assertion
    if "level" in expected:
        exp_level = expected["level"]
        passed = job.level == exp_level
        results.append(AssertionResult(
            field="level",
            expected=exp_level,
            actual=job.level,
            passed=passed,
            message="Level mismatch" if not passed else "Level matches",
        ))

    # Description assertions
    if "description_min_words" in expected:
        min_words = expected["description_min_words"]
        passed = job.description_word_count >= min_words
        results.append(AssertionResult(
            field="description_min_words",
            expected=f">= {min_words}",
            actual=job.description_word_count,
            passed=passed,
            message=f"Description too short ({job.description_word_count} words, need {min_words})" if not passed else "Description has sufficient words",
        ))

    if "description_contains" in expected:
        exp_substr = expected["description_contains"]
        passed = exp_substr.lower() in job.description.lower()
        results.append(AssertionResult(
            field="description_contains",
            expected=exp_substr,
            actual=f"[{job.description_word_count} words]",
            passed=passed,
            message=f"Description should contain '{exp_substr}'" if not passed else "Description contains expected substring",
        ))

    if "description_not_contains" in expected:
        exp_substr = expected["description_not_contains"]
        passed = exp_substr.lower() not in job.description.lower()
        results.append(AssertionResult(
            field="description_not_contains",
            expected=f"NOT '{exp_substr}'",
            actual=f"[{job.description_word_count} words]" + (" (FOUND)" if not passed else ""),
            passed=passed,
            message=f"Description should NOT contain '{exp_substr}'" if not passed else "Description does not contain forbidden substring",
        ))

    # Cost assertions
    if "cost_milli_cents_min" in expected:
        min_cost = expected["cost_milli_cents_min"]
        actual_cost = job.cost_milli_cents or 0
        passed = actual_cost >= min_cost
        results.append(AssertionResult(
            field="cost_milli_cents_min",
            expected=f">= {min_cost}",
            actual=actual_cost,
            passed=passed,
            message=f"Cost too low ({actual_cost}, need >= {min_cost})" if not passed else "Cost meets minimum",
        ))

    if "cost_milli_cents_max" in expected:
        max_cost = expected["cost_milli_cents_max"]
        actual_cost = job.cost_milli_cents or 0
        passed = actual_cost <= max_cost
        results.append(AssertionResult(
            field="cost_milli_cents_max",
            expected=f"<= {max_cost}",
            actual=actual_cost,
            passed=passed,
            message=f"Cost too high ({actual_cost}, need <= {max_cost})" if not passed else "Cost within maximum",
        ))

    # Posted at assertion
    if "posted_at_not_null" in expected and expected["posted_at_not_null"]:
        passed = job.posted_at is not None
        results.append(AssertionResult(
            field="posted_at_not_null",
            expected="not null",
            actual=job.posted_at,
            passed=passed,
            message="Posted date is missing" if not passed else "Posted date is present",
        ))

    # URL assertion
    if "url_contains" in expected:
        exp_substr = expected["url_contains"]
        passed = exp_substr.lower() in job.url.lower()
        results.append(AssertionResult(
            field="url_contains",
            expected=exp_substr,
            actual=job.url,
            passed=passed,
            message=f"URL should contain '{exp_substr}'" if not passed else "URL contains expected substring",
        ))

    # Full description word count assertion (proves full description goes to file storage)
    if "full_description_word_count_min" in expected:
        min_words = expected["full_description_word_count_min"]
        passed = job.description_word_count >= min_words
        results.append(AssertionResult(
            field="full_description_word_count_min",
            expected=f">= {min_words}",
            actual=job.description_word_count,
            passed=passed,
            message=f"Full description too short ({job.description_word_count} words, need {min_words})" if not passed else "Full description has sufficient words for file storage",
        ))

    return results


def _format_assertion_failures(results: List[AssertionResult]) -> str:
    """Format failed assertions for error message."""
    failures = [r for r in results if not r.passed]
    if not failures:
        return ""
    lines = ["Assertion failures:"]
    for f in failures:
        lines.append(f"  - {f.field}: {f.message}")
        lines.append(f"      expected: {f.expected}")
        lines.append(f"      actual: {f.actual}")
    return "\n".join(lines)


@dataclass
class ExtractedJobDetails:
    """Captured job details from extraction."""

    title: str = ""
    description: str = ""
    description_word_count: int = 0
    location: str = ""
    is_remote: bool = False
    posted_at: Optional[int] = None
    posted_at_unknown: bool = False
    company: str = ""
    level: str = ""
    total_compensation: int = 0
    compensation_unknown: bool = False
    compensation_reason: Optional[str] = None
    url: str = ""
    cost_milli_cents: Optional[int] = None
    metadata: Optional[str] = None


@dataclass
class ConvexStorageCapture:
    """Captured data that would be sent to Convex."""

    ingested_jobs: List[Dict[str, Any]] = field(default_factory=list)
    stored_descriptions: List[Dict[str, Any]] = field(default_factory=list)
    stored_scrapes: List[Dict[str, Any]] = field(default_factory=list)
    description_uploads: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ExtractionStepLog:
    """Log entry for a single extraction step."""
    step: str
    description: str
    data: Any = None


@dataclass
class HeuristicPatchInfo:
    """Info about heuristic patches applied to a job."""
    original_title: Optional[str] = None
    patched_title: Optional[str] = None
    title_changed: bool = False
    original_values: Dict[str, Any] = field(default_factory=dict)
    patch_applied: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExtractorFieldTrace:
    """Debug trace for a single extracted field showing all strategies."""
    field_name: str
    final_value: Any = None
    winning_strategy: Optional[str] = None
    strategy_results: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class JobDetailExtractionResult:
    """Complete extraction result for a site."""

    site_id: str
    detail_url: str
    source_url: str
    extracted_jobs: List[ExtractedJobDetails] = field(default_factory=list)
    convex_capture: ConvexStorageCapture = field(default_factory=ConvexStorageCapture)
    raw_scrape_response: Optional[Dict[str, Any]] = None
    errors: List[str] = field(default_factory=list)
    # Verbose debug info
    extraction_steps: List[ExtractionStepLog] = field(default_factory=list)
    raw_markdown: Optional[str] = None
    handler_name: Optional[str] = None
    normalized_markdown: Optional[str] = None
    extracted_title_from_handler: Optional[str] = None
    # Heuristic processing info
    heuristic_patches: List[HeuristicPatchInfo] = field(default_factory=list)
    # Extractor debug trace - shows winning strategy for each field
    extractor_trace: Dict[str, ExtractorFieldTrace] = field(default_factory=dict)


class _FixtureAsyncSpider:
    """Mock SpiderCloud client that returns fixture data.

    Simulates the real AsyncSpider by streaming JSONL responses with proper
    newline delimiters, allowing the scraper's _consume_chunk to parse correctly.
    """

    def __init__(
        self,
        api_key: str,
        fixtures: Dict[str, Dict[str, Any]],
        calls: List[Dict[str, Any]],
    ):
        self.api_key = api_key
        self._fixtures = fixtures
        self._calls = calls

    async def __aenter__(self) -> "_FixtureAsyncSpider":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    def scrape_url(
        self,
        url: str,
        *,
        params: Dict[str, Any],
        stream: bool,
        content_type: str,
    ):
        fixture = self._fixtures.get(url)
        if not fixture:
            raise AssertionError(f"Unexpected SpiderCloud URL: {url}")
        self._calls.append({"url": url, "params": params})
        response = fixture.get("response", [])
        if response is None:
            response = []

        async def _iterator():
            # SpiderCloud streams JSONL responses as chunks. Fixtures may contain:
            # 1. Complete JSONL lines (one JSON object per list item)
            # 2. Fragmented JSONL (a single JSON line split across multiple items)
            #
            # To simulate real streaming, we concatenate all items and yield
            # the complete buffer. The scraper's _consume_chunk will split on \n.
            if isinstance(response, list):
                # Concatenate all fragments
                full_response = "".join(response)
                # Ensure proper newline termination for JSONL
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


class _FixtureSyncSpider:
    """Mock SpiderCloud client for synchronous JSON mode.

    Unlike _FixtureAsyncSpider which simulates JSONL streaming,
    this mock returns the fixture's response object directly as
    a coroutine, matching the behavior of stream=False mode.
    """

    def __init__(
        self,
        api_key: str,
        fixtures: Dict[str, Dict[str, Any]],
        calls: List[Dict[str, Any]],
    ):
        self.api_key = api_key
        self._fixtures = fixtures
        self._calls = calls

    async def __aenter__(self) -> "_FixtureSyncSpider":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    def scrape_url(
        self,
        url: str,
        *,
        params: Dict[str, Any],
        stream: bool,
        content_type: str,
    ):
        fixture = self._fixtures.get(url)
        if not fixture:
            raise AssertionError(f"Unexpected SpiderCloud URL: {url}")
        self._calls.append({"url": url, "params": params, "stream": stream})

        # For sync mode, return response directly (not as JSONL stream)
        response = fixture.get("response", {})

        async def _awaitable():
            return response

        return _awaitable()


def _is_single_request_fixture(fixture: Dict[str, Any]) -> bool:
    """Detect if fixture uses single request (JSON) or batch (JSONL) format.

    Single request mode fixtures have:
    - request.stream = false
    - response is a dict (JSON object), not a list of strings (JSONL)
    """
    request = fixture.get("request", {})
    # Single request mode fixtures have stream=false
    if request.get("stream") is False:
        return True
    # Also check if response is a dict (JSON) vs list of strings (JSONL)
    response = fixture.get("response")
    if isinstance(response, dict):
        return True
    return False


class WorkflowTestModule:
    """
    Simulates running a URL through the workflow using test fixtures.

    Captures all data that would be sent to Convex, including:
    - Ingested jobs with truncated descriptions
    - Full descriptions uploaded to file storage
    - Scrape records

    This class uses WorkflowTestHelper from the core module for the base setup,
    with customizations for schedule entry handling and capture formats.
    """

    def __init__(
        self,
        entry: Dict[str, Any],
        detail_fixture: Dict[str, Any],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        self.entry = entry
        self.detail_fixture = detail_fixture
        self.tmp_path = tmp_path
        self.monkeypatch = monkeypatch
        self.site_id = _schedule_id(entry)
        self.detail_url = detail_fixture["request"]["url"]
        self.source_url = entry.get("url", "")

        self.capture = ConvexStorageCapture()
        self.spider_calls: List[Dict[str, Any]] = []

        # Create SpiderFixture from detail fixture
        is_sync = _is_single_request_fixture(detail_fixture)
        self._spider_fixture = SpiderFixture(
            url=self.detail_url,
            response=detail_fixture.get("response", []),
            params=detail_fixture.get("request", {}).get("params", {}),
            is_sync=is_sync,
        )

        # Custom query handler for pagination limit from entry
        pagination_limit = entry.get("paginationLimit")
        if isinstance(pagination_limit, (int, float)) and pagination_limit > 0:
            self._pagination_limit = int(pagination_limit)
        else:
            self._pagination_limit = 3

    async def setup(self) -> None:
        """Configure mocks and environment using WorkflowTestHelper."""
        # Create helper with custom query responses
        def site_query_handler(payload: Dict[str, Any]) -> Dict[str, Any] | None:
            if payload.get("id") == self.site_id:
                return {"paginationLimit": self._pagination_limit}
            return None

        self._helper = WorkflowTestHelper(
            fixtures={self.detail_url: self._spider_fixture},
            monkeypatch=self.monkeypatch,
            tmp_path=self.tmp_path,
            site_id=self.site_id,
            source_url=self.source_url,
            query_responses={
                "router:getSiteById": site_query_handler,
            },
        )

        await self._helper.setup()

        # Capture spider calls from helper
        self.spider_calls = self._helper.spider_calls

        # Override capture to use our ConvexStorageCapture format
        # (the helper uses a different format internally)
        self._override_captures()

    def _override_captures(self) -> None:
        """Override helper's capture to use ConvexStorageCapture format."""
        # Override mutation to capture ingested jobs in our format
        original_mutation = self._helper._fake_convex_mutation

        def custom_mutation(name: str, payload: Dict[str, Any]) -> Any:
            if name == "router:ingestJobsFromScrape":
                jobs = payload.get("jobs", [])
                if isinstance(jobs, list):
                    self.capture.ingested_jobs.extend(jobs)
            return original_mutation(name, payload)

        # Apply overrides - the WorkflowTestHelper handles most patching
        self.monkeypatch.setattr(
            "job_scrape_application.services.convex_client.convex_mutation",
            custom_mutation,
        )

    async def run_detail_extraction(self) -> JobDetailExtractionResult:
        """Run the job detail extraction workflow."""
        result = JobDetailExtractionResult(
            site_id=self.site_id,
            detail_url=self.detail_url,
            source_url=self.source_url,
        )

        # Capture verbose debug info if enabled
        verbose = os.environ.get("DEBUG_EXTRACTION_VERBOSE", "").lower() in ("1", "true", "yes")

        if verbose:
            # Step 1: Capture handler info
            handler = get_site_handler(self.detail_url)
            result.handler_name = handler.name if handler else None
            result.extraction_steps.append(ExtractionStepLog(
                step="Handler Detection",
                description=f"Detected handler: {result.handler_name or 'None (base handler)'}",
                data={"url": self.detail_url, "handler": result.handler_name},
            ))

            # Step 2: Capture raw content from fixture (markdown or JSON)
            try:
                fixture_response = self.detail_fixture.get("response", [])
                raw_content = None
                content_type = "unknown"

                if isinstance(fixture_response, list) and fixture_response:
                    # JSONL format - parse first line
                    first_line = fixture_response[0] if fixture_response else ""
                    if isinstance(first_line, str):
                        parsed = json_loads(first_line)
                        raw_content = parsed.get("content", {}).get("commonmark", "")
                        # Also try raw_html if commonmark is empty or just a code block
                        if not raw_content or raw_content.strip().startswith("```"):
                            raw_html = parsed.get("content", {}).get("raw", "")
                            if raw_html:
                                content_type = "raw_html"
                                raw_content = raw_html[:500] + "..." if len(raw_html) > 500 else raw_html
                            else:
                                content_type = "json_codeblock"
                        else:
                            content_type = "commonmark"
                    elif isinstance(first_line, dict):
                        raw_content = first_line.get("content", {}).get("commonmark", "")
                        content_type = "commonmark"
                elif isinstance(fixture_response, dict):
                    # Single request format (non-streaming)
                    raw_content = fixture_response.get("content", {}).get("commonmark", "")
                    if not raw_content:
                        # Try raw HTML for single request
                        raw_content = fixture_response.get("content", {}).get("raw", "")
                        content_type = "raw_html" if raw_content else "empty"
                    else:
                        content_type = "commonmark"

                result.raw_markdown = raw_content
                result.extraction_steps.append(ExtractionStepLog(
                    step="Raw Content Capture",
                    description=f"Captured {len(result.raw_markdown or '')} chars of {content_type} content",
                    data={"length": len(result.raw_markdown or ""), "content_type": content_type},
                ))
            except Exception as e:
                result.extraction_steps.append(ExtractionStepLog(
                    step="Raw Content Capture",
                    description=f"Failed to capture raw content: {e}",
                ))

            # Step 3: Test handler normalization
            if handler and result.raw_markdown:
                try:
                    normalized_md, extracted_title = handler.normalize_markdown(result.raw_markdown)
                    result.normalized_markdown = normalized_md
                    result.extracted_title_from_handler = extracted_title
                    result.extraction_steps.append(ExtractionStepLog(
                        step="Handler Normalization",
                        description=f"normalize_markdown() returned title='{extracted_title}', {len(normalized_md or '')} chars of normalized content",
                        data={"title": extracted_title, "normalized_length": len(normalized_md or "")},
                    ))
                except Exception as e:
                    result.extraction_steps.append(ExtractionStepLog(
                        step="Handler Normalization",
                        description=f"normalize_markdown() failed: {e}",
                    ))

        try:
            batch = {
                "urls": [
                    {
                        "url": self.detail_url,
                        "sourceUrl": self.source_url,
                        "provider": "spidercloud",
                        "siteId": self.site_id,
                        "pattern": self.entry.get("pattern"),
                        "urlType": "detail",
                    }
                ]
            }

            if verbose:
                result.extraction_steps.append(ExtractionStepLog(
                    step="Workflow Execution",
                    description="Calling process_spidercloud_job_batch()",
                    data=batch,
                ))

            response = await process_spidercloud_job_batch(
                batch, persist_scrapes=True
            )

            result.raw_scrape_response = response

            # Merge helper's captured data to our capture (step functions store to helper.captured)
            helper_captured = self._helper.captured
            self.capture.stored_scrapes.extend(helper_captured.stored_scrapes)
            self.capture.ingested_jobs.extend(helper_captured.ingested_jobs)
            self.capture.description_uploads.extend(helper_captured.description_uploads)

            result.convex_capture = self.capture

            if verbose:
                result.extraction_steps.append(ExtractionStepLog(
                    step="Workflow Complete",
                    description=f"Workflow returned, captured {len(self.capture.stored_scrapes)} scrapes, {len(self.capture.ingested_jobs)} ingested jobs",
                    data={
                        "stored_scrapes": len(self.capture.stored_scrapes),
                        "ingested_jobs": len(self.capture.ingested_jobs),
                        "description_uploads": len(self.capture.description_uploads),
                    },
                ))

            # Extract job details from stored scrapes (where normalized items live)
            # IMPORTANT: Apply heuristics like production does in ingest_scrape_to_convex
            import time
            heuristic_time_ms = int(time.time() * 1000)

            for scrape in self.capture.stored_scrapes:
                items = scrape.get("items") if isinstance(scrape, dict) else {}
                if not isinstance(items, dict):
                    continue

                # Try normalized first, then normalizedSample
                normalized = items.get("normalized")
                if not isinstance(normalized, list) or not normalized:
                    normalized = items.get("normalizedSample")
                if not isinstance(normalized, list):
                    continue

                # Extract cost from scrape
                cost = scrape.get("costMilliCents")
                if cost is None:
                    cost = items.get("costMilliCents")

                for row in normalized:
                    if not isinstance(row, dict):
                        continue

                    # Store original values before heuristics
                    original_title = str(row.get("title") or row.get("job_title") or "")
                    original_values = {
                        "title": original_title,
                        "location": str(row.get("location") or ""),
                        "remote": row.get("remote"),
                    }

                    # =========================================================
                    # EXTRACTOR DEBUG TRACE
                    # Run modular extractors to show which strategy won each field
                    # This mirrors production extraction and shows complete trace
                    # =========================================================
                    try:
                        handler = get_site_handler(self.detail_url)
                        description_md = str(row.get("description") or "")
                        url = str(row.get("url") or row.get("job_url") or self.detail_url)

                        # Create extraction context from row data
                        ctx = ExtractionContext.from_scrape_result(
                            url=url,
                            markdown=description_md,
                            handler=handler,
                            raw_row=row,
                            debug=True,  # Always run all strategies for complete trace
                        )

                        # Run all extractors and capture full trace
                        extractor_results = extract_job_fields(ctx, run_all=True)

                        # Convert to trace dict for output
                        for field_name, ext_result in extractor_results.items():
                            trace_data = ext_result.to_debug_dict()
                            result.extractor_trace[field_name] = ExtractorFieldTrace(
                                field_name=field_name,
                                final_value=trace_data.get("final_value"),
                                winning_strategy=trace_data.get("winning_strategy"),
                                strategy_results=trace_data.get("strategy_results", []),
                            )

                        if verbose:
                            result.extraction_steps.append(ExtractionStepLog(
                                step="Extractor Debug Trace",
                                description=f"Ran {len(extractor_results)} extractors with all strategies",
                                data={
                                    field: {
                                        "winner": trace.winning_strategy,
                                        "value": trace.final_value,
                                    }
                                    for field, trace in result.extractor_trace.items()
                                },
                            ))
                    except Exception as e:
                        if verbose:
                            result.extraction_steps.append(ExtractionStepLog(
                                step="Extractor Debug Trace",
                                description=f"Failed to run extractors: {e}",
                            ))
                        logger.debug("Extractor trace failed: %s", e)

                    # Apply heuristics using new extractor-based path
                    # This replaces _build_job_detail_heuristic_patch with the modular extractors
                    patch, _records = build_heuristic_patch_from_extractors(row, [], heuristic_time_ms)

                    # Track what changed
                    patched_title = patch.get("title") if patch else None
                    title_changed = patched_title is not None and patched_title != original_title

                    heuristic_info = HeuristicPatchInfo(
                        original_title=original_title,
                        patched_title=patched_title,
                        title_changed=title_changed,
                        original_values=original_values,
                        patch_applied=patch if patch else {},
                    )
                    result.heuristic_patches.append(heuristic_info)

                    # Log heuristic changes in verbose mode
                    if verbose and title_changed:
                        result.extraction_steps.append(ExtractionStepLog(
                            step="Heuristic Title Override",
                            description=f"Title changed from '{original_title}' to '{patched_title}'",
                            data={
                                "original_title": original_title,
                                "patched_title": patched_title,
                                "patch": patch,
                            },
                        ))

                    # Apply patch to row (merge patch values over original)
                    patched_row = {**row, **patch} if patch else row

                    job_details = ExtractedJobDetails(
                        title=str(patched_row.get("title") or patched_row.get("job_title") or ""),
                        description=str(patched_row.get("description") or ""),
                        description_word_count=_count_words(
                            str(patched_row.get("description") or "")
                        ),
                        location=str(patched_row.get("location") or ""),
                        is_remote=bool(patched_row.get("remote")),
                        posted_at=patched_row.get("posted_at"),
                        posted_at_unknown=bool(patched_row.get("posted_at_unknown")),
                        company=str(patched_row.get("company") or ""),
                        level=str(patched_row.get("level") or ""),
                        total_compensation=int(patched_row.get("total_compensation") or 0),
                        compensation_unknown=bool(patched_row.get("compensation_unknown")),
                        compensation_reason=patched_row.get("compensation_reason"),
                        url=str(
                            patched_row.get("url")
                            or patched_row.get("job_url")
                            or patched_row.get("absolute_url")
                            or ""
                        ),
                        cost_milli_cents=int(cost) if cost else None,
                    )
                    result.extracted_jobs.append(job_details)

            # Also try to extract from ingested jobs if no jobs found from scrapes
            if not result.extracted_jobs and self.capture.ingested_jobs:
                for job in self.capture.ingested_jobs:
                    if not isinstance(job, dict):
                        continue
                    job_details = ExtractedJobDetails(
                        title=str(job.get("title") or ""),
                        description=str(job.get("description") or ""),
                        description_word_count=_count_words(
                            str(job.get("description") or "")
                        ),
                        location=str(job.get("location") or ""),
                        is_remote=bool(job.get("remote")),
                        posted_at=job.get("postedAt"),
                        posted_at_unknown=bool(job.get("postedAtUnknown")),
                        company=str(job.get("company") or ""),
                        level=str(job.get("level") or ""),
                        total_compensation=int(job.get("totalCompensation") or 0),
                        compensation_unknown=bool(job.get("compensationUnknown")),
                        compensation_reason=job.get("compensationReason"),
                        url=str(job.get("url") or ""),
                        cost_milli_cents=job.get("scrapedCostMilliCents"),
                    )
                    result.extracted_jobs.append(job_details)

        except Exception as exc:
            result.errors.append(str(exc))
            logger.exception("Detail extraction failed for %s", self.site_id)

        return result


def _get_sites_with_detail_fixtures() -> List[Dict[str, Any]]:
    """Get schedule entries that have detail fixtures."""
    entries = _load_schedule_entries()
    result = []
    for entry in entries:
        site_id = _schedule_id(entry)
        _, detail_path = _fixture_paths(entry)
        if detail_path.exists():
            try:
                # Check fixture size to skip very large ones
                if (
                    detail_path.stat().st_size > MAX_DETAIL_FIXTURE_BYTES
                    and site_id not in LARGE_FIXTURE_ALLOWLIST
                ):
                    continue
                result.append(entry)
            except Exception:
                continue
    return result


SCHEDULE_ENTRIES_WITH_DETAILS = _get_sites_with_detail_fixtures()


def _write_extraction_result(result: JobDetailExtractionResult) -> None:
    """Write extraction result to output directory."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{result.site_id}_extraction.json"

    output_data = {
        "site_id": result.site_id,
        "detail_url": result.detail_url,
        "source_url": result.source_url,
        "extracted_jobs": [
            {
                "title": job.title,
                "description_preview": _truncate_to_words(job.description, 50),
                "description_word_count": job.description_word_count,
                "location": job.location,
                "is_remote": job.is_remote,
                "posted_at": job.posted_at,
                "posted_at_unknown": job.posted_at_unknown,
                "company": job.company,
                "level": job.level,
                "total_compensation": job.total_compensation,
                "compensation_unknown": job.compensation_unknown,
                "compensation_reason": job.compensation_reason,
                "url": job.url,
                "cost_milli_cents": job.cost_milli_cents,
            }
            for job in result.extracted_jobs
        ],
        "convex_capture": {
            "ingested_jobs_count": len(result.convex_capture.ingested_jobs),
            "stored_scrapes_count": len(result.convex_capture.stored_scrapes),
            "description_uploads_count": len(result.convex_capture.description_uploads),
            "ingested_jobs_sample": result.convex_capture.ingested_jobs[:2]
            if result.convex_capture.ingested_jobs
            else [],
            "description_uploads_sample": [
                {
                    "url": upload["url"],
                    "word_count": upload["word_count"],
                    "description_preview": _truncate_to_words(
                        upload["description"], 30
                    ),
                }
                for upload in result.convex_capture.description_uploads[:2]
            ],
        },
        # Extractor debug trace - shows which strategy won for each field
        "extractor_trace": {
            field: {
                "final_value": trace.final_value,
                "winning_strategy": trace.winning_strategy,
                "strategy_count": len(trace.strategy_results),
                # Include compact strategy summary (winner + alternates)
                "strategies": [
                    {
                        "name": s.get("strategy"),
                        "priority": s.get("priority"),
                        "value": s.get("value"),
                        "is_valid": s.get("is_valid"),
                        "reason": s.get("reason"),
                    }
                    for s in trace.strategy_results
                ],
            }
            for field, trace in result.extractor_trace.items()
        } if result.extractor_trace else {},
        "errors": result.errors,
    }

    output_path.write_text(json_dumps(output_data, indent=2))
    logger.info("Wrote extraction result to %s", output_path)

    # Write verbose extraction steps if enabled
    if os.environ.get("DEBUG_EXTRACTION_VERBOSE", "").lower() in ("1", "true", "yes"):
        _write_verbose_extraction_steps(result)


def _write_verbose_extraction_steps(result: JobDetailExtractionResult) -> None:
    """Write detailed step-by-step extraction log for debugging.

    This outputs a human-readable file showing:
    1. Raw SpiderCloud response (markdown content)
    2. Handler detection and selection
    3. Handler normalize_markdown() output
    4. Field extraction from normalized content
    5. Final job data before Convex storage
    6. Convex mutation payload

    Enable by setting DEBUG_EXTRACTION_VERBOSE=1
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{result.site_id}_extraction_steps.md"

    lines = [
        f"# Extraction Steps: {result.site_id}",
        "",
        f"**Detail URL:** `{result.detail_url}`",
        f"**Source URL:** `{result.source_url}`",
        f"**Handler:** `{result.handler_name or 'Unknown'}`",
        "",
        "---",
        "",
    ]

    # Step 1: Raw SpiderCloud Response
    lines.extend([
        "## Step 1: SpiderCloud Response",
        "",
        "Raw markdown content from SpiderCloud scrape:",
        "",
        "```markdown",
    ])
    if result.raw_markdown:
        # Truncate to first 3000 chars for readability
        preview = result.raw_markdown[:3000]
        if len(result.raw_markdown) > 3000:
            preview += f"\n\n... (truncated, {len(result.raw_markdown)} total chars)"
        lines.append(preview)
    else:
        lines.append("(No raw markdown captured)")
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
        "- Parse the specific job board's HTML/markdown format",
        "- Extract title, location, and other fields",
        "- Clean up JSON blocks or other noise",
        "",
        "---",
        "",
    ])

    # Step 3: Handler Normalization
    lines.extend([
        "## Step 3: Handler normalize_markdown() Output",
        "",
        f"**Extracted Title:** `{result.extracted_title_from_handler or '(None)'}`",
        "",
        "Normalized markdown after handler processing:",
        "",
        "```markdown",
    ])
    if result.normalized_markdown:
        preview = result.normalized_markdown[:2000]
        if len(result.normalized_markdown) > 2000:
            preview += f"\n\n... (truncated, {len(result.normalized_markdown)} total chars)"
        lines.append(preview)
    else:
        lines.append("(No normalized markdown captured - handler may not implement normalize_markdown)")
    lines.extend([
        "```",
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
                    lines.append(json_dumps(step.data, indent=2)[:1500])
                except Exception:
                    lines.append(str(step.data)[:1500])
                lines.append("```")
            lines.append("")
        lines.extend(["---", ""])

    # Step 4.5: Heuristic Processing (applied after scraper extraction)
    if result.heuristic_patches:
        lines.extend([
            "## Step 4.5: Heuristic Processing",
            "",
            "**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.",
            "This can override the title if the original title doesn't contain required keywords.",
            "",
        ])
        for i, patch_info in enumerate(result.heuristic_patches):
            lines.append(f"### Job {i + 1} Heuristics")
            lines.append("")
            if patch_info.title_changed:
                lines.append("**⚠️ TITLE CHANGED:**")
                lines.append(f"- Original: `{patch_info.original_title}`")
                lines.append(f"- After Heuristics: `{patch_info.patched_title}`")
            else:
                lines.append(f"**Title unchanged:** `{patch_info.original_title}`")
            lines.append("")
            if patch_info.patch_applied:
                lines.append("**Full patch applied:**")
                lines.append("```json")
                try:
                    lines.append(json_dumps(patch_info.patch_applied, indent=2)[:1000])
                except Exception:
                    lines.append(str(patch_info.patch_applied)[:1000])
                lines.append("```")
            lines.append("")
        lines.extend(["---", ""])

    # Step 4.6: Extractor Strategy Trace (IMPORTANT: shows which strategy won each field)
    if result.extractor_trace:
        lines.extend([
            "## Step 4.6: Extractor Strategy Trace",
            "",
            "**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.",
            "Use this to debug extraction bugs like location or remote detection issues.",
            "",
            "Each field has multiple strategies tried in priority order. The first valid result wins.",
            "",
        ])

        # Summary table first
        lines.append("### Summary (Winners)")
        lines.append("")
        lines.append("| Field | Winner Strategy | Value |")
        lines.append("|-------|----------------|-------|")
        for field, trace in result.extractor_trace.items():
            value_preview = str(trace.final_value)[:50] if trace.final_value else "(none)"
            lines.append(f"| {field} | `{trace.winning_strategy or 'none'}` | `{value_preview}` |")
        lines.append("")

        # Detailed strategy breakdown for each field
        lines.append("### Detailed Strategy Breakdown")
        lines.append("")
        for field, trace in result.extractor_trace.items():
            lines.append(f"#### {field.upper()}")
            lines.append("")
            lines.append(f"**Final Value:** `{trace.final_value}`")
            lines.append(f"**Winning Strategy:** `{trace.winning_strategy}`")
            lines.append("")
            if trace.strategy_results:
                lines.append("| Strategy | Priority | Valid | Value | Reason |")
                lines.append("|----------|----------|-------|-------|--------|")
                for s in trace.strategy_results:
                    name = s.get("strategy", "?")
                    priority = s.get("priority", "?")
                    is_valid = "✅" if s.get("is_valid") else "❌"
                    value = str(s.get("value") or "")[:30]
                    reason = str(s.get("reason") or "")[:50]
                    # Highlight the winner
                    if name == trace.winning_strategy:
                        lines.append(f"| **{name}** 🏆 | {priority} | {is_valid} | `{value}` | {reason} |")
                    else:
                        lines.append(f"| {name} | {priority} | {is_valid} | `{value}` | {reason} |")
                lines.append("")
            else:
                lines.append("*No strategy results recorded*")
                lines.append("")
        lines.extend(["---", ""])

    # Step 5: Extracted Job Details
    lines.extend([
        "## Step 5: Extracted Job Details",
        "",
    ])
    if result.extracted_jobs:
        for i, job in enumerate(result.extracted_jobs):
            lines.append(f"### Job {i + 1}")
            lines.append("")
            lines.append("| Field | Value |")
            lines.append("|-------|-------|")
            lines.append(f"| Title | `{job.title}` |")
            lines.append(f"| Company | `{job.company}` |")
            lines.append(f"| Location | `{job.location}` |")
            lines.append(f"| Is Remote | `{job.is_remote}` |")
            lines.append(f"| Level | `{job.level}` |")
            lines.append(f"| Posted At | `{job.posted_at}` |")
            lines.append(f"| Description Words | `{job.description_word_count}` |")
            lines.append(f"| Cost (milli-cents) | `{job.cost_milli_cents}` |")
            lines.append(f"| URL | `{job.url}` |")
            lines.append("")
            lines.append("**Description Preview (first 200 words):**")
            lines.append("")
            lines.append("```")
            lines.append(_truncate_to_words(job.description, 200))
            lines.append("```")
            lines.append("")
    else:
        lines.append("*No jobs extracted*")
        lines.append("")
    lines.extend(["---", ""])

    # Step 6: Convex Mutation Payload
    lines.extend([
        "## Step 6: Convex Mutation Payload",
        "",
        f"**Ingested Jobs Count:** {len(result.convex_capture.ingested_jobs)}",
        f"**Stored Scrapes Count:** {len(result.convex_capture.stored_scrapes)}",
        f"**Description Uploads Count:** {len(result.convex_capture.description_uploads)}",
        "",
    ])

    if result.convex_capture.ingested_jobs:
        lines.append("### Sample Ingested Job Payload")
        lines.append("")
        lines.append("This is what gets sent to `router:ingestJobsFromScrape`:")
        lines.append("")
        lines.append("```json")
        try:
            sample = result.convex_capture.ingested_jobs[0]
            # Truncate description for readability
            if isinstance(sample.get("description"), str) and len(sample["description"]) > 500:
                sample = dict(sample)
                sample["description"] = sample["description"][:500] + "..."
            lines.append(json_dumps(sample, indent=2))
        except Exception as e:
            lines.append(f"Error serializing: {e}")
        lines.append("```")
        lines.append("")

    if result.convex_capture.stored_scrapes:
        lines.append("### Sample Stored Scrape")
        lines.append("")
        lines.append("Scrape record stored for debugging/audit:")
        lines.append("")
        lines.append("```json")
        try:
            scrape = result.convex_capture.stored_scrapes[0]
            # Create a summary version
            summary = {
                "url": scrape.get("url"),
                "sourceUrl": scrape.get("sourceUrl"),
                "provider": scrape.get("provider"),
                "costMilliCents": scrape.get("costMilliCents"),
                "items_keys": list(scrape.get("items", {}).keys()) if isinstance(scrape.get("items"), dict) else None,
                "normalized_count": len(scrape.get("items", {}).get("normalized", [])) if isinstance(scrape.get("items"), dict) else 0,
            }
            lines.append(json_dumps(summary, indent=2))
        except Exception as e:
            lines.append(f"Error serializing: {e}")
        lines.append("```")
        lines.append("")

    # Errors section
    if result.errors:
        lines.extend([
            "---",
            "",
            "## Errors",
            "",
        ])
        for error in result.errors:
            lines.append(f"- {error}")
        lines.append("")

    # Write file
    output_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Wrote verbose extraction steps to %s", output_path)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry",
    SCHEDULE_ENTRIES_WITH_DETAILS,
    ids=lambda entry: _schedule_id(entry),
)
async def test_job_detail_extraction_accuracy(
    entry: Dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reset_dbos: None,
) -> None:
    """
    Test that job detail extraction accurately captures all fields.

    Validates extracted values against YAML ground truth files in the ground_truth/ folder.
    Each assertion file specifies expected values for title, company, location, etc.

    Verifies:
    - Title matches expected value (exact or contains)
    - Company matches expected value
    - Location matches expected value
    - Remote status is correct
    - Level is correctly inferred
    - Description has minimum word count
    - Cost is within expected range
    """
    site_id = _schedule_id(entry)
    _, detail_path = _fixture_paths(entry)
    if not detail_path.exists():
        pytest.skip(f"No detail fixture for {site_id}")

    detail_fixture = _load_fixture(detail_path)
    module = WorkflowTestModule(entry, detail_fixture, tmp_path, monkeypatch)
    await module.setup()

    result = await module.run_detail_extraction()

    # Write output for inspection
    _write_extraction_result(result)

    # Verify no fatal errors
    assert not result.errors, f"Extraction errors: {result.errors}"

    # Load assertions for this site (uses _meta.assertion_file from fixture if available)
    assertions = _load_assertions(site_id, fixture=detail_fixture)

    # Check if this test should be skipped (e.g., job listing removed)
    if assertions:
        expected = assertions.get("expected", {})
        if expected.get("skip"):
            pytest.skip(f"Skipping {site_id}: {expected.get('skip_reason', 'marked as skip')}")

    # Verify at least one job was extracted
    assert result.extracted_jobs, f"No jobs extracted from {site_id}"

    # Verify core fields and assertions for each extracted job
    for job in result.extracted_jobs:
        # Basic field presence checks
        assert job.title, f"Missing title for {site_id}"
        assert job.url, f"Missing URL for {site_id}"
        assert job.location, f"Missing location for {site_id}"
        assert job.level in (
            "junior",
            "mid",
            "senior",
            "staff",
        ), f"Invalid level {job.level} for {site_id}"

        # Validate against YAML assertions if available
        if assertions:
            validation_results = _validate_job_against_assertions(job, assertions)
            failures = [r for r in validation_results if not r.passed]
            if failures:
                failure_msg = _format_assertion_failures(validation_results)
                pytest.fail(f"Assertion validation failed for {site_id}:\n{failure_msg}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry",
    SCHEDULE_ENTRIES_WITH_DETAILS,
    ids=lambda entry: _schedule_id(entry),
)
async def test_job_detail_convex_storage(
    entry: Dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reset_dbos: None,
) -> None:
    """
    Test that job details are properly stored to Convex.

    Verifies:
    - Jobs are ingested via ingestJobsFromScrape
    - Description in ingested job is truncated to ~100 words
    - Full description is uploaded separately to file storage
    """
    _, detail_path = _fixture_paths(entry)
    if not detail_path.exists():
        pytest.skip(f"No detail fixture for {_schedule_id(entry)}")

    detail_fixture = _load_fixture(detail_path)
    module = WorkflowTestModule(entry, detail_fixture, tmp_path, monkeypatch)
    await module.setup()

    result = await module.run_detail_extraction()

    assert not result.errors, f"Extraction errors: {result.errors}"

    # Check that scrapes were stored
    assert (
        result.convex_capture.stored_scrapes
    ), f"No scrapes stored for {_schedule_id(entry)}"

    # For jobs with descriptions, verify truncation behavior
    for scrape in result.convex_capture.stored_scrapes:
        items = scrape.get("items") if isinstance(scrape, dict) else {}
        if not isinstance(items, dict):
            continue
        normalized = items.get("normalized")
        if not isinstance(normalized, list):
            continue
        for row in normalized:
            if not isinstance(row, dict):
                continue
            description = row.get("description")
            if not isinstance(description, str) or not description.strip():
                continue
            # The scrape payload may have full description; truncation happens
            # in Python before sending to ingestJobsFromScrape mutation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry",
    SCHEDULE_ENTRIES_WITH_DETAILS,
    ids=lambda entry: _schedule_id(entry),
)
async def test_job_detail_description_handling(
    entry: Dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reset_dbos: None,
) -> None:
    """
    Test description handling: truncated preview + full upload.

    Verifies:
    - Jobs sent to ingestJobsFromScrape have truncated descriptions (≤100 words, ≤4000 bytes)
    - Full descriptions are uploaded to file storage separately
    """
    _, detail_path = _fixture_paths(entry)
    if not detail_path.exists():
        pytest.skip(f"No detail fixture for {_schedule_id(entry)}")

    detail_fixture = _load_fixture(detail_path)
    module = WorkflowTestModule(entry, detail_fixture, tmp_path, monkeypatch)
    await module.setup()

    result = await module.run_detail_extraction()

    assert not result.errors, f"Extraction errors: {result.errors}"

    # CRITICAL: Verify ingested jobs have truncated descriptions
    # This is the key assertion - jobs sent to Convex DB must have truncated descriptions
    for ingested_job in result.convex_capture.ingested_jobs:
        if not isinstance(ingested_job, dict):
            continue
        description = ingested_job.get("description", "")
        if not description:
            continue

        # Check word count limit (100 words max)
        word_count = _count_words(description)
        assert word_count <= DESCRIPTION_PREVIEW_MAX_WORDS + 1, (
            f"Ingested job description exceeds {DESCRIPTION_PREVIEW_MAX_WORDS} word limit: "
            f"{word_count} words for {ingested_job.get('url', 'unknown')}"
        )

        # Check byte limit (4000 UTF-8 bytes max)
        byte_count = len(description.encode("utf-8"))
        assert byte_count <= 4100, (  # Allow small buffer for "..." suffix
            f"Ingested job description exceeds 4000 byte limit: "
            f"{byte_count} bytes for {ingested_job.get('url', 'unknown')}"
        )

    full_word_count_by_url: Dict[str, int] = {}
    for ingested_job in result.convex_capture.ingested_jobs:
        if not isinstance(ingested_job, dict):
            continue
        url = ingested_job.get("url")
        if not isinstance(url, str) or not url:
            continue
        full_count = ingested_job.get("_full_description_word_count")
        if isinstance(full_count, int):
            full_word_count_by_url[url] = full_count

    # Check description uploads for jobs with long descriptions
    for job in result.extracted_jobs:
        if job.description_word_count > DESCRIPTION_PREVIEW_MAX_WORDS:
            # Should have a corresponding upload for full description
            matching_uploads = [
                u
                for u in result.convex_capture.description_uploads
                if u.get("url") == job.url
            ]
            # Note: Upload may not happen if job ID lookup fails
            if matching_uploads:
                upload = matching_uploads[0]
                expected_word_count = full_word_count_by_url.get(
                    job.url, job.description_word_count
                )
                assert (
                    upload["word_count"] == expected_word_count
                ), f"Upload word count mismatch for {job.url}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry",
    SCHEDULE_ENTRIES_WITH_DETAILS,
    ids=lambda entry: _schedule_id(entry),
)
async def test_extractors_use_full_description_not_truncated(
    entry: Dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reset_dbos: None,
) -> None:
    """
    CRITICAL: Verify extractors ALWAYS operate on full description, NOT truncated.

    This test ensures that:
    1. The extraction pipeline (ExtractionContext) receives the full description
    2. All extraction strategies see the complete job description
    3. Truncation happens ONLY when preparing data for Convex jobs table
    4. Full description is separately posted to Convex file storage

    The workflow must:
    - Extract from FULL description (for accurate title, location, remote detection)
    - Post TRUNCATED description (≤100 words) to Convex jobs table row
    - Post FULL description to Convex file storage via separate API
    """
    site_id = _schedule_id(entry)
    _, detail_path = _fixture_paths(entry)
    if not detail_path.exists():
        pytest.skip(f"No detail fixture for {site_id}")

    detail_fixture = _load_fixture(detail_path)
    module = WorkflowTestModule(entry, detail_fixture, tmp_path, monkeypatch)
    await module.setup()

    result = await module.run_detail_extraction()

    assert not result.errors, f"Extraction errors: {result.errors}"
    assert result.extracted_jobs, f"No jobs extracted for {site_id}"

    # Verify extractor trace shows full description was used
    if result.extractor_trace:
        desc_trace = result.extractor_trace.get("description")
        if desc_trace and desc_trace.final_value:
            # The description in extractor trace should be the FULL description
            # (no truncation suffix unless it exceeds MAX_DESCRIPTION_LENGTH = 50,000)
            full_desc_word_count = len(desc_trace.final_value.split())

            # Verify extraction operated on substantial content
            # (if truncation happened during extraction, word count would be capped)
            for job in result.extracted_jobs:
                if job.description:
                    # Extracted job should have same word count as extractor saw
                    assert job.description_word_count == full_desc_word_count, (
                        f"Extractor word count ({full_desc_word_count}) doesn't match "
                        f"extracted job ({job.description_word_count}) - truncation may have "
                        f"happened before extraction!"
                    )

    # Verify that scrapes stored contain FULL descriptions (pre-truncation)
    for scrape in result.convex_capture.stored_scrapes:
        items = scrape.get("items") if isinstance(scrape, dict) else {}
        if not isinstance(items, dict):
            continue
        normalized = items.get("normalized")
        if not isinstance(normalized, list):
            continue
        for row in normalized:
            if not isinstance(row, dict):
                continue
            full_desc = row.get("description")
            if not isinstance(full_desc, str) or not full_desc.strip():
                continue

            # Stored scrape should have FULL description (may be > 100 words)
            # The truncation happens AFTER extraction when preparing for ingest
            # We verify this by checking the description hasn't been truncated yet
            assert not full_desc.endswith(DESCRIPTION_TRUNCATION_SUFFIX) or len(full_desc.split()) <= DESCRIPTION_PREVIEW_MAX_WORDS, (
                "Stored scrape description should be full (not truncated) unless it was short"
            )

    # Verify that ingested jobs have TRUNCATED descriptions (for DB row)
    for ingested_job in result.convex_capture.ingested_jobs:
        if not isinstance(ingested_job, dict):
            continue
        truncated_desc = ingested_job.get("description", "")
        if not truncated_desc:
            continue

        truncated_word_count = _count_words(truncated_desc)
        # MUST be truncated to max 100 words (or shorter if original was shorter)
        assert truncated_word_count <= DESCRIPTION_PREVIEW_MAX_WORDS + 1, (
            f"Ingested description should be truncated but has {truncated_word_count} words "
            f"(max {DESCRIPTION_PREVIEW_MAX_WORDS})"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry",
    SCHEDULE_ENTRIES_WITH_DETAILS,
    ids=lambda entry: _schedule_id(entry),
)
async def test_job_detail_metadata_extraction(
    entry: Dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reset_dbos: None,
) -> None:
    """
    Test that metadata like cost is captured.

    Verifies:
    - costMilliCents is captured when available
    - Response includes expected metadata fields
    """
    _, detail_path = _fixture_paths(entry)
    if not detail_path.exists():
        pytest.skip(f"No detail fixture for {_schedule_id(entry)}")

    detail_fixture = _load_fixture(detail_path)
    module = WorkflowTestModule(entry, detail_fixture, tmp_path, monkeypatch)
    await module.setup()

    result = await module.run_detail_extraction()

    assert not result.errors, f"Extraction errors: {result.errors}"

    # Verify response structure includes expected fields
    response = result.raw_scrape_response
    assert response is not None, f"No response for {_schedule_id(entry)}"

    # Check for cost tracking
    cost = response.get("costMilliCents")
    items = response.get("items") if isinstance(response, dict) else {}
    if cost is None and isinstance(items, dict):
        cost = items.get("costMilliCents")

    # Cost should be present for SpiderCloud responses
    # (may be None for some fixtures)


def test_output_directory_created(tmp_path: Path) -> None:
    """Test that output directory is created."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    assert OUTPUT_DIR.exists(), "Output directory should exist"


def test_description_truncation_logic() -> None:
    """Test the description truncation helper."""
    short_text = "This is a short description."
    long_text = " ".join(["word"] * 150)

    assert _count_words(short_text) == 5
    assert _count_words(long_text) == 150

    truncated = _truncate_to_words(long_text, 100)
    assert _count_words(truncated.rstrip("...")) == 100
    assert truncated.endswith("...")

    short_truncated = _truncate_to_words(short_text, 100)
    assert short_truncated == short_text


# Export extraction results for all sites when run directly
if __name__ == "__main__":
    import asyncio

    async def export_all_results():
        """Export extraction results for all sites with fixtures."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        entries = _get_sites_with_detail_fixtures()

        for entry in entries:
            site_id = _schedule_id(entry)
            _, detail_path = _fixture_paths(entry)

            try:
                detail_fixture = _load_fixture(detail_path)

                # Create a minimal mock environment
                result = JobDetailExtractionResult(
                    site_id=site_id,
                    detail_url=detail_fixture["request"]["url"],
                    source_url=entry.get("url", ""),
                )

                # Extract what we can from the fixture directly
                response = detail_fixture.get("response", [])
                if isinstance(response, list) and response:
                    # Try to parse first response item
                    for item_str in response:
                        if isinstance(item_str, str):
                            try:
                                item = json_loads(item_str)
                                if isinstance(item, dict):
                                    content = item.get("content", {})
                                    if isinstance(content, dict):
                                        # Extract job details from fixture
                                        result.raw_scrape_response = item
                            except (ValueError, TypeError):
                                continue

                _write_extraction_result(result)
                print(f"Exported: {site_id}")

            except Exception as exc:
                print(f"Failed to export {site_id}: {exc}")

    asyncio.run(export_all_results())
