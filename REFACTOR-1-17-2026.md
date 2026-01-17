# Testing Infrastructure and Heuristics Consolidation Plan

## Overview

Refactor testing infrastructure so tests call workflow modules directly, consolidate heuristics into extractors/normalizers, and improve test observability with full strategy traces.

## Key Decisions

- **Fixture format**: Hybrid approach - VCR.py for HTTP recording, custom wrapper for metadata
- **Heuristics**: Delete `heuristics.py` entirely (clean break, update all imports)
- **Test output**: Full strategy trace showing all strategies tried and why each failed/succeeded

---

## Phase 1: Consolidate Shared Helpers

**Goal**: Single source of truth for location/title/compensation utilities.

### Files to Modify

| File | Action |
|------|--------|
| `workflows/helpers/location_normalization.py` | Add `_derive_countries()`, `_derive_location_states()`, `_build_location_search()` from integration.py |
| `workflows/helpers/title_validation.py` | **CREATE** - Move `_should_override_title()` logic here |
| `workflows/normalizers/steps/normalize_step.py` | Remove duplicate location helpers, import from `location_normalization.py` |
| `workflows/extractors/integration.py` | Remove duplicate helpers, import from shared modules |

### Constants Consolidation

Move all geographic constants to `constants.py` (already exists):
- `_CANADIAN_PROVINCE_CODES` / `_CANADIAN_PROVINCE_NAMES`
- `_US_STATE_CODES` / `_US_STATE_NAMES`
- `_UNKNOWN_LOCATION_TOKENS`

---

## Phase 2: Delete heuristics.py

**Goal**: Remove 821 lines of legacy code, establish normalizers/pipeline.py as single entry point.

### Migration Steps

1. **Update imports in:**
   - `workflows/activities/__init__.py` - Change `from .heuristics import` to `from ..normalizers.pipeline import build_job_update`
   - `workflows/workflow/process_pending_heuristics.py`
   - Any files in `workflows/activities/_archive/`

2. **Delete:**
   - `job_scrape_application/workflows/activities/heuristics.py`

3. **Verify** all tests pass after deletion

### Entry Point After Migration

```python
# Single canonical import
from job_scrape_application.workflows.normalizers.pipeline import (
    normalize_job,      # New API - returns NormalizationResult
    build_job_update,   # Backwards-compat - returns (patch, records) tuple
)
```

---

## Phase 3: Enhance Extraction Observability

**Goal**: Full strategy trace for clear test failure messages.

### Enhance NormalizationResult

```python
# In normalizers/types.py
@dataclass
class NormalizationResult:
    job: NormalizedJob | None
    trace: PipelineTrace | None
    success: bool
    errors: list[str]

    # ADD these fields for test observability:
    extraction_results: dict[str, ExtractionResult]  # Full extractor traces per field
    normalization_changes: dict[str, tuple[Any, Any]]  # (before, after) for each normalized field
```

### Files to Modify

| File | Changes |
|------|---------|
| `normalizers/types.py` | Add `extraction_results`, `normalization_changes` to `NormalizationResult` |
| `normalizers/pipeline.py` | Populate new observability fields |
| `normalizers/steps/extract_step.py` | Return full `ExtractionResult` objects, not just values |

### Test Failure Output Format

```
FAILED: title extraction
Expected: "Senior Software Engineer"
Actual:   "Job Description"

Strategy Trace:
  1. structured_data_title (priority=100): SKIPPED - no structured data
  2. handler_provided_title (priority=200): FAILED - handler returned None
  3. hints_title (priority=300): SKIPPED - no title in hints
  4. heading_detection (priority=500): RETURNED "Job Description" - found <h1> tag
  5. cleanup_title (priority=600): ACCEPTED - no cleanup needed

Winning Strategy: heading_detection
Reason: First h1 tag matched, but content was generic placeholder
```

---

## Phase 4: Adopt VCR.py with Custom Wrapper

**Goal**: Hybrid fixture approach - VCR.py records HTTP, custom layer adds metadata.

### VCR.py Integration Points

VCR.py must be configured to hook into all HTTP-making libraries used:

| Library | Usage | VCR.py Hook |
|---------|-------|-------------|
| **httpx** | SpiderCloud API calls | `vcr` has built-in httpx support via `pytest-httpx` or custom stubs |
| **SpiderCloud SDK** | `AsyncSpider.scrape_url()` | Hook at httpx layer (SpiderCloud uses httpx internally) |
| **Convex Python SDK** | `convex_query`, `convex_mutation`, `convex_action` | Hook at httpx layer OR create custom Convex cassette recorder |

**Note**: Convex calls may need special handling since they use a custom protocol. Options:
1. Record at the httpx transport layer (captures raw HTTP)
2. Create a `ConvexCassetteRecorder` that mocks `ConvexClient` directly and stores call/response pairs
3. Continue using the existing mock approach for Convex (`_mock_convex_query`/`_mock_convex_mutation`) and only use VCR.py for SpiderCloud

Recommended approach: **Use VCR.py for SpiderCloud/httpx calls only**, keep existing Convex mocking infrastructure since Convex responses are simpler and don't require real HTTP recording.

### Create Fixture Wrapper

```python
# In workflows/workflow/fixture_recording.py (NEW)

import vcr
from pathlib import Path
from dataclasses import dataclass
from typing import Any

@dataclass
class FixtureMeta:
    """Metadata for test fixtures."""
    generated_at: str
    site_id: str
    ground_truth_file: str | None = None
    source_url: str | None = None

class FixtureRecorder:
    """Records HTTP fixtures with VCR.py and adds custom metadata."""

    def __init__(self, fixture_dir: Path):
        self.fixture_dir = fixture_dir
        self.vcr = vcr.VCR(
            serializer='json',
            record_mode='new_episodes',
            match_on=['method', 'uri'],
            filter_headers=['authorization', 'x-api-key'],
        )

    def record(self, url: str, meta: FixtureMeta) -> Path:
        """Record HTTP interaction and save with metadata."""
        ...

    def load(self, path: Path) -> tuple[Any, FixtureMeta]:
        """Load fixture and parse metadata."""
        ...
```

### Integration with WorkflowTest

```python
# In workflows/workflow/test_utils.py

class WorkflowTest:
    def with_vcr_fixture(self, cassette_path: Path) -> "WorkflowTest":
        """Load a VCR.py cassette as a fixture."""
        fixture, meta = FixtureRecorder.load(cassette_path)
        return self.with_spider_fixture(SpiderFixture.from_vcr(fixture, meta))
```

### Dependencies

Add to `pyproject.toml`:
```toml
[project.optional-dependencies]
test = [
    "vcrpy>=6.0.0",
    # ... existing deps
]
```

---

## Phase 5: Migrate Deprecated Test Helpers

**Goal**: Remove `core/test_helpers.py`, use `workflow/test_utils.py` exclusively.

### Migration Map

| Old (test_helpers.py) | New (test_utils.py or production) |
|-----------------------|-----------------------------------|
| `WorkflowTestHelper` | `WorkflowTest` |
| `SpiderFixture` | `SpiderFixture` (same, consolidated) |
| `CapturedConvexData` | `CapturedStepCalls` |
| `_truncate_description_for_ingest()` | Use `scrape_utils.build_description_preview()` |
| `extract_level_from_title()` | Use `LevelExtractor` directly |
| `parse_posted_at()` | Use `PostedAtExtractor` directly |
| `process_response_item()` | Use `normalize_job()` from pipeline |

### Delete After Migration

- `job_scrape_application/workflows/core/test_helpers.py`

---

## Phase 6: Simplify integration.py

**Goal**: Reduce `extractors/integration.py` to thin bridge role only.

### Keep

- `extract_job_from_scrape()` - Used by scrapers for quick extraction

### Remove

- `build_heuristic_patch_from_extractors()` - Use `normalizers.pipeline.build_job_update()` instead
- All duplicate location/title/compensation helpers

### Final integration.py (~100 lines)

```python
"""Bridge between scrapers and extraction system."""

from ..normalizers.pipeline import normalize_job
from .context import ExtractionContext

def extract_job_from_scrape(
    url: str,
    markdown: str,
    handler: Any,
    structured_data: dict | None = None,
    raw_row: dict | None = None,
    debug: bool = False,
) -> dict | None:
    """Quick extraction for scraper use."""
    result = normalize_job(RawScrapeInput(
        url=url,
        markdown=markdown,
        structured_data=structured_data,
        raw_row=raw_row or {},
    ), trace_enabled=debug)

    if not result.success:
        return None
    return result.job.to_dict()
```

---

## Testing Strategy

### Commands

```bash
# Run all workflow tests
uv run pytest tests/job_scrape_application/workflows/ -v

# Run only extractor tests
uv run pytest tests/job_scrape_application/workflows/extractors/ -v

# Re-run failed tests
uv run pytest --lf

# Lint after changes
uvx ruff check job_scrape_application/workflows/
uv run scripts/lint_dbos_step.py
```

### Validation After Each Phase

1. All existing tests pass
2. No new deprecation warnings (except intentional ones during migration)
3. `ruff check` passes
4. `lint_dbos_step.py` passes

---

## Critical Files Summary

| File | Phase | Action |
|------|-------|--------|
| `workflows/helpers/location_normalization.py` | 1 | Consolidate all location helpers |
| `workflows/helpers/title_validation.py` | 1 | CREATE - title validation logic |
| `workflows/activities/heuristics.py` | 2 | DELETE |
| `workflows/activities/__init__.py` | 2 | Update imports |
| `normalizers/types.py` | 3 | Add observability fields |
| `normalizers/pipeline.py` | 3 | Populate observability fields |
| `workflows/workflow/fixture_recording.py` | 4 | CREATE - VCR.py wrapper |
| `workflows/workflow/test_utils.py` | 4 | Add VCR.py integration |
| `workflows/core/test_helpers.py` | 5 | DELETE (after migration) |
| `workflows/extractors/integration.py` | 6 | Simplify to ~100 lines |

---

## Estimated Impact

- **Lines removed**: ~1,400 (821 heuristics.py + 500+ duplicates + 300+ test_helpers.py)
- **Lines added**: ~200 (new helpers, VCR wrapper, observability)
- **Net reduction**: ~1,200 lines
- **Modules simplified**: 4 (heuristics, integration, normalize_step, test_helpers)
- **Single entry point**: `normalizers/pipeline.py`

---

## Verification

After implementation:
1. `uv run pytest tests/job_scrape_application/` - All tests pass
2. `uvx ruff check .` - No lint errors
3. `uv run scripts/lint_dbos_step.py` - DBOS decorators correct
4. No imports from deleted modules (`heuristics.py`, `test_helpers.py`)
