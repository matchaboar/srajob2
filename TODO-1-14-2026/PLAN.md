# Plan: Unified Workflow Architecture with DRY Production-Test Parity

## Executive Summary

The codebase already has well-designed **extractors** (strategy-based field extraction) and **normalizers** (helpers for location, company, URL, etc.). The problem is that **tests reimplement production logic** instead of calling production workflows. This plan unifies the architecture so tests call the exact same code paths as production.

### Dual-Format Trace Output

Every extraction produces two trace files:

| Format | Purpose | Token Cost | Contains |
|--------|---------|------------|----------|
| `.md` | Claude Code context | LOW (~50 lines) | Summary, failures, sample URLs |
| `.json` | Detailed searching | N/A (not in context) | All data, raw content, full steps |

**Usage Pattern:**
1. Claude reads `.md` file for quick understanding
2. If more detail needed, Claude uses `rg <pattern> *.json` to search detailed data
3. This keeps Claude context token-efficient while preserving all debug info

### Ground Truth Testing Philosophy

**Fixtures** = Input data (SpiderCloud responses)
**Ground Truth** = Expected correct output (what extraction SHOULD produce)

Tests compare workflow output against ground truth to detect:
- Extraction bugs (wrong field values)
- Normalization regressions (changed URL format, location format)
- Handler issues (wrong handler selected, missing URLs)

Ground truth files are **manually verified** human-curated expected values, not auto-generated from production output.

## Current Architecture (What Already Exists)

### 1. Extractors Module (Well-Designed)
Location: `job_scrape_application/workflows/extractors/`

| File | Purpose |
|------|---------|
| `base.py` | `StrategyPriority`, `ExtractionStrategy`, `FieldExtractor`, `StrategyResult` |
| `context.py` | `ExtractionContext` - all input data for extraction |
| `integration.py` | `extract_job_from_scrape()`, `build_heuristic_patch_from_extractors()` |
| `*_extractor.py` | 8 field extractors with 6-9 strategies each |

**Strategy Priority System:**
```
STRUCTURED_DATA (100) → SITE_HANDLER (200) → EXPLICIT_FIELD (300) →
URL_DERIVED (400) → CONTENT_PATTERN (500) → HEURISTIC (600) → FALLBACK (900)
```

### 2. Normalizers/Helpers (Well-Designed)
Location: `job_scrape_application/workflows/helpers/`

| Module | Key Functions |
|--------|---------------|
| `location_normalization.py` | `_normalize_locations()`, dictionary-based resolution |
| `company_normalization.py` | `normalize_company_hint()`, `derive_company_from_url()` |
| `compensation_parsing.py` | `parse_compensation()` - bounds checking, K-suffix |
| `timestamp_parsing.py` | `parse_posted_at()` - ISO, unix, relative times |
| `link_extractors.py` | `normalize_url()`, `normalize_url_list()` |
| `url_handling.py` | URL scoring, filtering, Ashby-specific |

### 3. Test Infrastructure (Good Foundation)
Location: `job_scrape_application/workflows/core/test_helpers.py`

- `WorkflowTestHelper` - mocks data boundaries (SpiderCloud, Convex)
- `SpiderFixture` - loads fixture files
- `CapturedConvexData` - captures queries, mutations, stored data

## The Problem: Tests Reimplement Production Logic + Scattered Heuristics

### Current DRY Violations

| Test File | What It Reimplements | Should Instead Call |
|-----------|---------------------|---------------------|
| `test_listing_extraction_e2e.py` | URL extraction, normalization, filtering | `process_spidercloud_listing_batch()` |
| `test_listing_extraction_e2e.py` | Handler method calls (`get_links_from_raw_html`) | Production workflow |
| `test_listing_extraction_e2e.py` | Fixture response parsing | Shared parsing utilities |
| `test_job_detail_extraction_e2e.py` | Extractor calls for debugging | Production workflow with debug hooks |

### Scattered Heuristic Logic

Currently heuristic/patch logic is spread across multiple files:

| Location | What It Does | Problem |
|----------|--------------|---------|
| `activities/heuristics.py` | `_build_job_detail_heuristic_patch()` | Standalone function, not part of extractor |
| `extractors/integration.py` | `build_heuristic_patch_from_extractors()` | Wrapper but still separate from extractors |
| `helpers/scrape_utils.py` | Various field-specific patches | Duplicates extractor logic |

## Solution: Unified Workflow Module with Debug Hooks

### Key Insight: Don't Mock Logic, Mock Data

```
┌────────────────────────────────────────────────────────────────┐
│                     MOCK SURFACE (Small)                       │
│  SpiderCloud API │ Convex DB │ DBOS Queue │ File Storage      │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│              PRODUCTION CODE (Runs Identically)                │
│  Handlers │ Extractors │ Normalizers │ URL Processing         │
└────────────────────────────────────────────────────────────────┘
```

## Implementation Phases

See TODO.md for checkboxed progress tracking.

## Architecture After Changes

```
┌────────────────────────────────────────────────────────────────────┐
│                        DATA BOUNDARIES (Mocked)                    │
├────────────────────────────────────────────────────────────────────┤
│ SpiderCloud │ Convex Queries │ Convex Mutations │ DBOS Queue      │
└─────────────┴────────────────┴──────────────────┴─────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────────┐
│                    UNIFIED WORKFLOW MODULES                        │
├────────────────────────────────────────────────────────────────────┤
│  ListingWorkflowModule          │  DetailWorkflowModule            │
│    ├─ extract_listing_urls()    │    ├─ extract_job_fields()      │
│    ├─ _filter_urls_with_handler │    ├─ build_heuristic_patch()   │
│    └─ debug trace support       │    └─ debug trace support       │
└────────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────────┐
│                    EXTRACTORS & NORMALIZERS                        │
├────────────────────────────────────────────────────────────────────┤
│ extractors/                     │ helpers/                         │
│  ├─ title_extractor.py         │  ├─ location_normalization.py   │
│  ├─ location_extractor.py      │  ├─ company_normalization.py    │
│  ├─ company_extractor.py       │  ├─ link_extractors.py          │
│  └─ (6 more...)                │  └─ (3 more...)                 │
└────────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────────┐
│                      SITE HANDLERS                                 │
├────────────────────────────────────────────────────────────────────┤
│ GreenhouseHandler │ AshbyHandler │ WorkdayHandler │ NetflixHandler │
└────────────────────────────────────────────────────────────────────┘
```

## Verification Commands

```bash
# Test listing extraction
uv run pytest tests/job_scrape_application/workflows/test_listing_extraction_e2e.py -v

# Verify detail extraction
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py -v --tb=short -x

# Verify debug fixtures
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v --tb=short

# Full workflow test suite
uv run pytest tests/job_scrape_application/workflows/ -v --tb=short
```

## Summary of Changes

| Metric | Before | After |
|--------|--------|-------|
| Test implementations | 2 (production + test reimpl) | 1 (production only) |
| Lines of test extraction logic | ~250 | ~25 |
| Mock surface | Handler methods, URL processing | Data boundaries only |
| Logic drift risk | HIGH | NONE |
| Heuristic logic location | Scattered (3+ files) | In extractor classes |
| Expected values folder | `assertions/` | `ground_truth/` |
| Config override visibility | Unclear | Clear in debug trace |
| Trace format | Single JSON | Dual: concise .md + detailed .json |
