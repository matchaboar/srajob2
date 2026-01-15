# Plan: Modular Workflow Architecture Refactor

## Executive Summary

This plan addresses four major refactoring goals:

1. **Modularize workflows** - Break monolithic files into focused modules
2. **Create SpiderCloud module** - Extract 5K+ lines into organized submodules
3. **Eliminate patch/heuristic concepts** - Replace with clear step-based normalization pipeline
4. **Remove Temporal legacy code** - Delete ~30 dead files and improve concurrency

### Core Philosophy

**Before:** Jobs are scraped, then "patched" with "heuristics" in opaque ways
**After:** Jobs flow through a clear **normalization pipeline** with explicit steps

```
Raw Scrape → Parse → Extract Fields → Normalize → Validate → Store
```

Each step is:
- **Isolated** - One responsibility per function
- **Testable** - Unit tests for each step
- **Traceable** - Debug output shows exactly what each step did
- **Reproducible** - Same input always produces same output

---

## Phase 1: Remove Dead Temporal Code

**Goal:** Clean house before restructuring

### Files to Delete (31 files)

#### Workflow Archive (7 files)
```
job_scrape_application/workflows/_archive/
├── temporal_worker.py              (559 lines)
├── temporal_scrape_workflow.py     (993 lines)
├── temporal_greenhouse_workflow.py
├── temporal_webhook_workflow.py
├── temporal_create_schedule.py
├── temporal_trigger_schedule.py
└── temporal_heuristic_workflow.py
```

#### Test Archive (20+ files)
```
tests/job_scrape_application/workflows/_archive/
├── test_scrape_workflow_signatures.py
├── test_process_webhook_workflow.py
├── test_site_lease_workflow.py
├── test_deadlock_guardrails.py
├── test_deadlock_timing_simulation.py
├── test_greenhouse_workflow_signatures.py
├── test_job_details_workflow_unit.py
├── test_process_webhook_database_rows.py
├── test_process_webhook_failure_events.py
├── test_recover_missing_firecrawl_webhook.py
├── test_startup_duplicate_leases.py
├── test_worker_multi_queue.py
├── test_spidercloud_job_details_queue_fixture.py
└── ... (others in _archive/)
```

#### Legacy Components (4 files)
```
components/legacy/_archive/
├── temporal_health_check.py
└── temporal_real_server_check.py

job_scrape_application/workflows/helpers/_archive/
├── workflow_logging.py
└── workflow_debug.py
```

### Verification
```bash
# Before deletion - ensure no imports reference these files
rg "from.*_archive" --type py
rg "temporal_worker|temporal_scrape_workflow" --type py

# After deletion - ensure tests still pass
uv run pytest tests/job_scrape_application/workflows/ -v --tb=short -x
```

---

## Phase 2: Create SpiderCloud Module

**Goal:** Extract 5,049-line monolith into organized submodules

### Current State
```
scrapers/
└── spidercloud_scraper.py  (5,049 lines - EVERYTHING)
```

### Target State
```
spidercloud/
├── __init__.py              # Public API exports
├── client.py                # SpiderCloud HTTP client (~400 lines)
│   ├── SpiderCloudClient
│   ├── make_request()
│   └── handle_rate_limits()
│
├── request_builder.py       # Request construction (~300 lines)
│   ├── build_listing_request()
│   ├── build_detail_request()
│   └── RequestParams dataclass
│
├── response_parser.py       # Response parsing (~600 lines)
│   ├── parse_listing_response()
│   ├── parse_detail_response()
│   ├── extract_job_urls()
│   └── ParsedResponse dataclass
│
├── batch_processor.py       # Batch operations (~800 lines)
│   ├── process_listing_batch()
│   ├── process_detail_batch()
│   └── BatchResult dataclass
│
├── error_handling.py        # Error strategies (~300 lines)
│   ├── classify_error()
│   ├── should_retry()
│   └── ErrorDecision dataclass
│
├── fixtures.py              # Test fixture generation (~200 lines)
│   ├── capture_response()
│   ├── load_fixture()
│   └── FixtureData dataclass
│
└── types.py                 # Shared types (~150 lines)
    ├── SpiderCloudConfig
    ├── ScrapeResult
    └── CostBreakdown
```

### Migration Strategy

1. **Extract types first** - Create `types.py` with all dataclasses
2. **Extract client** - HTTP logic with no business logic
3. **Extract parsers** - Response parsing separate from requesting
4. **Extract batch logic** - High-level orchestration
5. **Update imports** - Change all `from scrapers.spidercloud_scraper import X` to `from spidercloud import X`
6. **Delete old file** - Remove monolith after all imports updated

### Public API (`__init__.py`)
```python
# spidercloud/__init__.py
from .client import SpiderCloudClient
from .batch_processor import process_listing_batch, process_detail_batch
from .response_parser import parse_listing_response, parse_detail_response
from .types import ScrapeResult, SpiderCloudConfig, CostBreakdown
from .error_handling import classify_error, should_retry

__all__ = [
    "SpiderCloudClient",
    "process_listing_batch",
    "process_detail_batch",
    "parse_listing_response",
    "parse_detail_response",
    "ScrapeResult",
    "SpiderCloudConfig",
    "CostBreakdown",
    "classify_error",
    "should_retry",
]
```

---

## Phase 3: Eliminate Patch/Heuristic Concepts

**Goal:** Replace opaque "patching" with explicit normalization pipeline

### Current Problems

1. **"Patch" is confusing** - Implies mutation of existing data
2. **"Heuristic" is vague** - Unclear what rules are applied
3. **Two implementations** - `heuristics.py` (legacy) vs `extractors/integration.py` (new)
4. **Version tracking** - HEURISTIC_VERSION 4 vs 5 is opaque
5. **Hard to debug** - Can't see what each step contributed

### New Architecture: Normalization Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    NORMALIZATION PIPELINE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  RawScrape ──► Step 1: Parse ──► Step 2: Extract ──►           │
│               (markdown,        (fields from                    │
│                JSON-LD,          content)                       │
│                HTML)                                            │
│                                                                 │
│  ──► Step 3: Normalize ──► Step 4: Validate ──► NormalizedJob  │
│      (location format,    (required fields,                     │
│       URL cleanup,         bounds checking)                     │
│       company name)                                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### New Module Structure

```
normalizers/
├── __init__.py              # Public API
├── pipeline.py              # Main pipeline orchestration
│   ├── normalize_job()      # Single entry point
│   └── NormalizationResult  # Result with trace
│
├── steps/
│   ├── __init__.py
│   ├── parse_step.py        # Step 1: Parse raw content
│   │   ├── parse_markdown()
│   │   ├── parse_json_ld()
│   │   └── parse_html()
│   │
│   ├── extract_step.py      # Step 2: Extract fields
│   │   ├── extract_title()
│   │   ├── extract_company()
│   │   ├── extract_location()
│   │   ├── extract_compensation()
│   │   ├── extract_posted_at()
│   │   ├── extract_level()
│   │   ├── extract_remote()
│   │   └── extract_description()
│   │
│   ├── normalize_step.py    # Step 3: Normalize values
│   │   ├── normalize_location()
│   │   ├── normalize_company()
│   │   ├── normalize_url()
│   │   ├── normalize_compensation()
│   │   └── normalize_timestamp()
│   │
│   └── validate_step.py     # Step 4: Validate result
│       ├── validate_required_fields()
│       ├── validate_bounds()
│       └── validate_url_format()
│
├── trace.py                 # Debug tracing
│   ├── StepTrace
│   ├── PipelineTrace
│   └── format_trace_md()
│
└── types.py                 # Pipeline types
    ├── RawScrape
    ├── ParsedContent
    ├── ExtractedFields
    └── NormalizedJob
```

### Pipeline Entry Point

```python
# normalizers/pipeline.py

@dataclass
class NormalizationResult:
    """Result of normalizing a job scrape."""
    job: NormalizedJob
    trace: PipelineTrace
    success: bool
    errors: list[str]

def normalize_job(
    raw_scrape: RawScrape,
    handler: BaseSiteHandler | None = None,
    trace_enabled: bool = False,
) -> NormalizationResult:
    """
    Single entry point for job normalization.

    Pipeline steps:
    1. Parse - Convert raw content to structured format
    2. Extract - Pull field values from parsed content
    3. Normalize - Standardize formats (location, URL, etc.)
    4. Validate - Check required fields and bounds

    Each step is independent and testable.
    """
    trace = PipelineTrace() if trace_enabled else None

    # Step 1: Parse
    parsed = parse_step.parse_content(raw_scrape, trace)

    # Step 2: Extract
    extracted = extract_step.extract_fields(parsed, handler, trace)

    # Step 3: Normalize
    normalized = normalize_step.normalize_fields(extracted, trace)

    # Step 4: Validate
    errors = validate_step.validate_job(normalized, trace)

    return NormalizationResult(
        job=normalized,
        trace=trace,
        success=len(errors) == 0,
        errors=errors,
    )
```

### Migration from Heuristics

| Old Concept | New Concept | Location |
|-------------|-------------|----------|
| `_build_job_detail_heuristic_patch()` | `normalize_job()` | `normalizers/pipeline.py` |
| `build_heuristic_patch_from_extractors()` | `extract_step.extract_fields()` | `normalizers/steps/extract_step.py` |
| `HEURISTIC_VERSION` | Removed - pipeline is deterministic | N/A |
| "patch" dict | `NormalizedJob` dataclass | `normalizers/types.py` |
| `heuristics.py` | Deleted | N/A |

### Files to Delete After Migration
```
job_scrape_application/workflows/activities/heuristics.py  (817 lines)
```

### Files to Modify
```
job_scrape_application/workflows/activities/__init__.py
  - Remove: _build_job_detail_heuristic_patch imports
  - Replace: Call normalize_job() instead

job_scrape_application/workflows/extractors/integration.py
  - Move: build_heuristic_patch_from_extractors() logic to extract_step.py
  - Keep: Extractor classes (they become the extract step implementation)
```

---

## Phase 4: Modularize Workflow Test Module

**Goal:** Tests call production code, not reimplementations

### Current Problems

1. **Test reimplements production logic** - 250+ lines of extraction code in tests
2. **Logic drift** - Test logic diverges from production
3. **Hard to maintain** - Changes require updating both places

### New Test Architecture

```
tests/job_scrape_application/workflows/
├── conftest.py                    # Shared fixtures and helpers
│
├── test_modules/                  # Modular test infrastructure
│   ├── __init__.py
│   ├── workflow_test_runner.py   # Runs production workflow with mocks
│   │   ├── WorkflowTestRunner
│   │   ├── run_listing_workflow()
│   │   └── run_detail_workflow()
│   │
│   ├── mock_boundaries.py        # Data boundary mocks only
│   │   ├── MockSpiderCloud
│   │   ├── MockConvex
│   │   └── MockQueue
│   │
│   ├── fixture_loader.py         # Load and validate fixtures
│   │   ├── load_listing_fixture()
│   │   ├── load_detail_fixture()
│   │   └── validate_fixture_format()
│   │
│   ├── assertion_checker.py      # YAML assertion validation
│   │   ├── check_assertions()
│   │   ├── load_assertions()
│   │   └── AssertionResult
│   │
│   └── trace_output.py           # Test trace generation
│       ├── write_trace_md()
│       ├── write_trace_json()
│       └── TraceConfig
│
├── fixtures/                      # Test input data
│   ├── debug/                    # Problem job fixtures
│   ├── dbos_schedule/            # Schedule-based fixtures
│   └── single_request/           # Single request fixtures
│
├── assertions/                    # Expected output (ground truth)
│   ├── debug/                    # Debug assertions
│   └── *.yml                     # Standard assertions
│
├── test_listing_extraction_e2e.py    # Uses WorkflowTestRunner
├── test_job_detail_extraction_e2e.py # Uses WorkflowTestRunner
├── test_debug_fixtures.py            # Uses WorkflowTestRunner
└── ... (other tests)
```

### WorkflowTestRunner

```python
# test_modules/workflow_test_runner.py

class WorkflowTestRunner:
    """
    Runs production workflow code with mocked data boundaries.

    Mocks: SpiderCloud API, Convex DB, DBOS Queue
    Does NOT mock: Handlers, Extractors, Normalizers, URL processing
    """

    def __init__(
        self,
        spidercloud_fixture: Path | None = None,
        convex_data: dict | None = None,
    ):
        self.mock_spidercloud = MockSpiderCloud(spidercloud_fixture)
        self.mock_convex = MockConvex(convex_data)
        self.mock_queue = MockQueue()

    async def run_listing_workflow(
        self,
        site_config: SiteConfig,
        trace_enabled: bool = True,
    ) -> ListingWorkflowResult:
        """
        Run production listing workflow with mocked boundaries.

        Calls actual production code:
        - Handler.get_links_from_raw_html()
        - URL normalization
        - URL filtering
        """
        # Production code runs here with mocked I/O
        ...

    async def run_detail_workflow(
        self,
        urls: list[str],
        trace_enabled: bool = True,
    ) -> DetailWorkflowResult:
        """
        Run production detail workflow with mocked boundaries.

        Calls actual production code:
        - SpiderCloud response parsing
        - normalize_job() pipeline
        - Handler.normalize_markdown()
        """
        # Production code runs here with mocked I/O
        ...
```

### Test Example (After Refactor)

```python
# test_listing_extraction_e2e.py

@pytest.mark.parametrize("site_name", SITES_WITH_FIXTURES)
async def test_listing_extraction_accuracy(site_name: str):
    """Test listing extraction using production code path."""

    # Load fixture and assertions
    fixture = load_listing_fixture(site_name)
    assertions = load_assertions(f"assertions/{site_name}.yml")

    # Run production workflow with mocked data boundaries
    runner = WorkflowTestRunner(spidercloud_fixture=fixture.path)
    result = await runner.run_listing_workflow(fixture.site_config)

    # Write trace output (dual format: .md for context, .json for detail)
    write_trace_md(result.trace, f"site-detail-e2e-examples/{site_name}_listing.md")
    write_trace_json(result.trace, f"site-detail-e2e-examples/{site_name}_listing.json")

    # Check assertions
    assertion_result = check_assertions(result, assertions)
    assert assertion_result.passed, assertion_result.failure_message
```

---

## Phase 5: Improve Concurrency

**Goal:** Remove Temporal-era bottlenecks, improve parallelism

### Current Bottlenecks

| Bottleneck | Location | Current | Target |
|------------|----------|---------|--------|
| SpiderCloud concurrency | `process_spidercloud_job_batch()` | Semaphore limited | Dynamic based on rate limits |
| Convex store concurrency | `batch_store_scrapes_background()` | Capped at 10 | Increase to 50+ |
| Listing batches per run | `SpidercloudListingWorkflow` | 1 batch | Multiple with flow control |
| Payload shrinking | `_shrink_for_activity()` | Temporal-era limits | Profile DBOS limits |

### Improvements

#### 1. Dynamic Concurrency Control

```python
# spidercloud/batch_processor.py

class AdaptiveConcurrency:
    """Dynamically adjust concurrency based on API responses."""

    def __init__(self, initial: int = 5, min_val: int = 1, max_val: int = 20):
        self.current = initial
        self.min_val = min_val
        self.max_val = max_val
        self._success_streak = 0
        self._error_streak = 0

    def on_success(self):
        self._success_streak += 1
        self._error_streak = 0
        if self._success_streak >= 5:
            self.current = min(self.current + 1, self.max_val)
            self._success_streak = 0

    def on_rate_limit(self):
        self._error_streak += 1
        self._success_streak = 0
        self.current = max(self.current // 2, self.min_val)

    def get_semaphore(self) -> asyncio.Semaphore:
        return asyncio.Semaphore(self.current)
```

#### 2. Increase Convex Store Parallelism

```python
# activities/__init__.py

async def batch_store_scrapes_background(scrapes, url_completion_data):
    # Increase from 10 to 50 (Convex supports 128 concurrent)
    max_concurrent_stores = max(1, min(
        runtime_config.spidercloud_job_details_concurrency,
        50  # Increased from 10
    ))
    semaphore = asyncio.Semaphore(max_concurrent_stores)
    ...
```

#### 3. Pipeline Batch Processing

```python
# Instead of sequential: lease -> scrape -> store
# Use pipeline: pre-lease next while current scrapes

async def process_batches_pipelined():
    """Process batches with overlapped operations."""

    next_batch_task = None

    while True:
        # Start leasing next batch while processing current
        if next_batch_task is None:
            next_batch_task = asyncio.create_task(lease_next_batch())

        # Wait for current batch
        current_batch = await next_batch_task
        if not current_batch:
            break

        # Pre-lease next batch
        next_batch_task = asyncio.create_task(lease_next_batch())

        # Process current batch (scrape + store)
        await process_batch(current_batch)
```

#### 4. Remove Payload Shrinking

```python
# After profiling DBOS limits, likely can remove this:

def _shrink_for_activity(scrape):
    # REMOVED - DBOS doesn't have Temporal's payload limits
    return scrape
```

---

## Phase 6: Activities Module Reorganization

**Goal:** Break 6K-line monolith into focused modules

### Current State
```
activities/
└── __init__.py  (6,006 lines - EVERYTHING)
```

### Target State
```
activities/
├── __init__.py              # Re-exports for backwards compatibility
├── listing/
│   ├── __init__.py
│   ├── lease.py             # lease_scrape_url_batch for listings
│   ├── process.py           # process_listing_batch
│   └── complete.py          # complete_listing_batch
│
├── detail/
│   ├── __init__.py
│   ├── lease.py             # lease_scrape_url_batch for details
│   ├── process.py           # process_spidercloud_job_batch
│   ├── store.py             # batch_store_scrapes_background
│   └── complete.py          # complete_detail_batch
│
├── site/
│   ├── __init__.py
│   ├── management.py        # Site lifecycle operations
│   └── scheduling.py        # Schedule-related activities
│
├── queue/
│   ├── __init__.py
│   ├── operations.py        # Queue management
│   └── recovery.py          # Queue recovery operations
│
└── shared/
    ├── __init__.py
    ├── logging.py           # Logging activities
    ├── convex.py            # Convex mutation helpers
    └── telemetry.py         # Telemetry collection
```

---

## Verification Strategy

### Phase Completion Checklist

Each phase must pass these checks before proceeding:

```bash
# Full test suite
uv run pytest tests/job_scrape_application/workflows/ -v --tb=short

# Lint check
uvx ruff check job_scrape_application/workflows/

# Type check (if using mypy)
uv run mypy job_scrape_application/workflows/

# Import check - no circular imports
python -c "from job_scrape_application.workflows import *"
```

### Test Coverage Requirements

- All existing passing tests must continue to pass
- New modules must have unit tests
- Pipeline steps must have individual tests
- Integration tests must use WorkflowTestRunner

---

## Timeline Overview

| Phase | Description | Files Changed | Risk |
|-------|-------------|---------------|------|
| 1 | Delete Temporal code | -31 files | Low |
| 2 | Create SpiderCloud module | +7 files, -1 file | Medium |
| 3 | Normalization pipeline | +12 files, -1 file | High |
| 4 | Modular test infrastructure | +5 files, modify 3 | Medium |
| 5 | Concurrency improvements | Modify 2 files | Low |
| 6 | Activities reorganization | +12 files, modify 1 | Medium |

**Order:** Phase 1 → 2 → 5 → 6 → 3 → 4

Rationale:
- Phase 1 (cleanup) has no dependencies
- Phase 2 (SpiderCloud) needed before Phase 3
- Phase 5 (concurrency) is isolated
- Phase 6 (activities) needed before Phase 3
- Phase 3 (normalization) is the largest change
- Phase 4 (tests) depends on Phase 3

---

## Summary of Key Changes

| Concept | Before | After |
|---------|--------|-------|
| Heuristics | Opaque "patch" with version numbers | Explicit normalization pipeline |
| SpiderCloud | 5K-line monolith | 7 focused modules |
| Activities | 6K-line monolith | 12 focused modules |
| Tests | Reimplement production logic | Call production code with mocks |
| Concurrency | Fixed semaphores, Temporal limits | Adaptive concurrency, higher parallelism |
| Temporal code | 31 dead files | Deleted |
| Trace output | Single format | Dual: .md (context) + .json (detail) |
