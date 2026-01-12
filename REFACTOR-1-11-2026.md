# Refactor Plan: Shrink `activities/__init__.py` (2026-01-11)

## Goals
- [ ] Make `job_scrape_application/workflows/activities/__init__.py` a thin export layer
- [ ] Move logic into cohesive modules + class-based helpers
- [ ] Preserve public API via re-exports

## Task Breakdown

### Inventory & API Preservation
- [ ] Identify all exports in `__all__` and external import usage
- [ ] Map each exported function to a future module
- [ ] Define stable re-export list for `__init__.py`

### Core Utilities
- [ ] Create `activities/core.py` for shared helpers
- [ ] Add `ActivityLogger` class for consistent logging

### Convex Site Lifecycle
- [ ] Create `activities/convex_sites.py`
- [ ] Add `ConvexSiteClient` class to wrap Convex calls
- [ ] Move `fetch_sites`, `lease_site`, `complete_site`, `fail_site`

### Scraper Factory
- [ ] Create `activities/scraper_factory.py`
- [ ] Add `ScraperFactory` class to centralize provider wiring
- [ ] Move `_make_*_scraper` and `select_scraper_for_site`

### Firecrawl Flow
- [ ] Create `activities/firecrawl_jobs.py`
- [ ] Add `FirecrawlJobManager` class for job lifecycle
- [ ] Move Firecrawl webhook start/status/collection helpers

### SpiderCloud Batching
- [ ] Create `activities/spidercloud_batches.py`
- [ ] Add `SpidercloudBatchProcessor` class for batch orchestration
- [ ] Move `lease_scrape_url_batch`, `process_spidercloud_*`, `complete_scrape_urls`

### URL Extraction
- [ ] Create `activities/url_extraction.py`
- [ ] Add `JobUrlExtractor` class for heuristic URL parsing
- [ ] Move `_filter_job_urls`, `_extract_job_urls_from_scrape`

### Location + Compensation Heuristics
- [ ] Create `activities/location_compensation.py`
- [ ] Add `LocationNormalizer` + `CompensationParser` classes
- [ ] Move location/compensation helpers at end of `__init__.py`

### Integration & Validation
- [ ] Update `__init__.py` to re-export public API
- [ ] Adjust internal imports to new modules
- [ ] Run `uvx ruff check .`
