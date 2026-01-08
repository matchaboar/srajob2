# Refactor Plan for job_scrape_application/workflows/activities/__init__.py

Date: 2026-01-07

## Context / Why this file exists
The file `job_scrape_application/workflows/activities/__init__.py` is ~5,000 LOC and mixes many responsibilities:
- Activity orchestration (scrape site, queue, workflow logging)
- Provider-specific logic (SpiderCloud, Firecrawl, FetchFox)
- URL extraction + pagination logic
- Heuristic parsing (job description enrichment, compensation parsing)
- Utilities (normalization, logging, regex helpers)

This makes it hard to navigate, increases coupling, and makes changes riskier. This doc proposes a safe, incremental refactor to improve modularity and readability while preserving the public activity function signatures.

## Goals
- Keep **public activity signatures stable** so workflows don’t break.
- Reduce file size and improve readability for human coders.
- Make logic discoverable by topic (SpiderCloud, Firecrawl, URL extraction, heuristics, etc.).
- Minimize risk by doing **incremental extraction** and re-exporting from `__init__.py`.
- Make it easy for another LLM session to continue the refactor with this file as the source of truth.

## Proposed Module Layout

### 1) `job_scrape_application/workflows/activities/__init__.py`
Purpose: Public API surface only.
- Keep only public activity functions (the ones called externally).
- Import and re-export implementations from modules below.
- Minimal helper logic; no new behavior.

### 2) `job_scrape_application/workflows/activities/_core.py`
Purpose: Shared helpers and internal utilities used across activities.
Likely move:
- `_strip_none_values`
- `_get_activity_worker_id`
- `_summarize_scrape_payload`
- `_apply_workflow_context`
- `_build_log_message`
- `_activity_cancellation_payload`

### 3) `job_scrape_application/workflows/activities/_scrape_orchestration.py`
Purpose: Scrape orchestration and workflow coordination.
Likely move:
- `select_scraper_for_site`
- `fetch_sites`
- `lease_site`
- `scrape_site`
- `scrape_site_fetchfox`
- `scrape_site_firecrawl`
- `start_firecrawl_webhook_scrape`
- `collect_firecrawl_job_result`
- `record_workflow_run`
- `complete_site`, `fail_site`

### 4) `job_scrape_application/workflows/activities/_spidercloud.py`
Purpose: SpiderCloud-specific scraping, batching, and listing logic.
Likely move:
- `_scrape_spidercloud_greenhouse`
- `process_spidercloud_job_batch`
- SpiderCloud listing helpers (if present in `__init__.py`)

### 5) `job_scrape_application/workflows/activities/_firecrawl.py`
Purpose: Firecrawl webhook lifecycle and polling.
Likely move:
- `fetch_pending_firecrawl_webhooks`
- `get_firecrawl_webhook_status`
- `mark_firecrawl_webhook_processed`
- `collect_firecrawl_job_result`
- Any Firecrawl-specific helpers near those functions

### 6) `job_scrape_application/workflows/activities/_fetchfox.py`
Purpose: FetchFox-specific logic.
Likely move:
- `crawl_site_fetchfox`
- `scrape_site_fetchfox`
- FetchFox helper functions

### 7) `job_scrape_application/workflows/activities/_job_urls.py`
Purpose: URL extraction, pagination merge logic, normalization helpers.
Likely move:
- `_extract_job_urls_from_scrape`
- `_normalize_job_url_list`, `_normalize_job_url`
- `_merge_pagination_urls`
- `_extract_pagination_payload`
- `_extract_handler_links`
- The Meta pagination “don’t re-enqueue source url” logic should remain here or call BaseSiteHandler helper.

### 8) `job_scrape_application/workflows/activities/_heuristics.py`
Purpose: Heuristic parsing / enrichment for job details.
Likely move:
- `_build_job_detail_heuristic_patch`
- Compensation parsing helpers (`_parse_comp_*`, `_extract_compensation_from_text`)
- Location parsing helpers (`_normalize_locations`, `_derive_countries`, etc.)

## Minimal Incremental Plan (Recommended)

### Phase 1 (Low risk)
1) Create `_job_urls.py` and move URL extraction + pagination helpers.
2) Update `__init__.py` to import/re-export as needed.
3) Keep tests passing (see test plan below).

### Phase 2 (Medium risk)
1) Create `_spidercloud.py` and move SpiderCloud-specific activities.
2) Create `_firecrawl.py` and move webhook logic.
3) Update imports in `__init__.py`.

### Phase 3 (Optional, more cleanup)
1) Extract heuristic parsing into `_heuristics.py`.
2) Extract shared helpers to `_core.py`.
3) Extract FetchFox to `_fetchfox.py` if still large.

## Readability Improvements
- Each module should start with a short docstring describing its scope.
- Keep function groups together and ordered top-down by call graph.
- Use explicit re-exports in `__init__.py` to show what’s public.
- Avoid circular imports by keeping helpers in `_core.py` and `_job_urls.py`.
- Prefer small modules with a single responsibility.
- Add **short comments only where the logic is non-obvious** (avoid redundant comments).

## Testing Plan

These tests should be run after each refactor phase:
1) Unit tests:
   - `uv run pytest tests/job_scrape_application/sites/test_site_handlers.py`
   - `uv run pytest tests/job_scrape_application/workflows/test_spidercloud_meta_listing_page_links.py`

2) Lint checks (required for Python edits):
   - `uvx ruff check job_scrape_application/workflows/activities/__init__.py`
   - `uvx ruff check job_scrape_application/workflows/activities/_job_urls.py` (or any new module added)

3) If time allows, run any broader workflow tests already used in this repo.

## Notes for the Next LLM Session
- The current code already added a helper in `BaseSiteHandler` to drop re-enqueueing the source listing URL:
  - `BaseSiteHandler.drop_source_listing_url(...)`
- The activities module uses this to prevent pagination loops.
- The refactor must preserve this logic by keeping it in `_job_urls.py` or wherever `_extract_job_urls_from_scrape` lives.

## Files to Create (Phase 1)
- `job_scrape_application/workflows/activities/_job_urls.py`
  - Move URL extraction + pagination helpers from `__init__.py`.
  - Keep function signatures the same.

## Files to Modify (Phase 1)
- `job_scrape_application/workflows/activities/__init__.py`
  - Remove moved helpers and import them from `_job_urls.py`.

## Success Criteria
- All listed tests pass.
- No behavior changes in public activity functions.
- `__init__.py` size reduced and more readable.
- A future engineer/LLM can locate functions by domain quickly.
