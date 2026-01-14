# DEBUGGING.md

Quick reference for debugging job extraction issues.

## Quick Start

### Debug a Specific Job Extraction

```bash
# Use automated mise task (fetches fixture + runs test)
mise run fix_job_extraction https://srajob.netlify.app/job/JOB_ID

# Or manually run the test
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v
```

### Debug Listing URL Extraction

```bash
DEBUG_EXTRACTION_VERBOSE=1 uv run pytest \
  'tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_listing_extraction[SITE]' -v

# Check trace output
cat ./site-detail-e2e-examples/SITE_listing_extraction.md
```

## Key Paths

| Type | Location |
|------|----------|
| Detail fixtures | `tests/.../fixtures/dbos_schedule/{site}_detail.json` |
| Listing fixtures | `tests/.../fixtures/dbos_schedule/{site}_listing.json` |
| Ground truth | `tests/.../ground_truth/{site}.yml` |
| Trace output | `./site-detail-e2e-examples/` |
| Site handlers | `job_scrape_application/workflows/site_handlers/` |

## Common Commands

```bash
# Regenerate fixture for a site
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-only SITE

# Run detail extraction test
uv run pytest "tests/.../test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[SITE]" -v

# Check extraction result
cat ./site-detail-e2e-examples/SITE_extraction.json

# Verify handler detection
uv run python -c "from job_scrape_application.workflows.site_handlers import get_site_handler; print(get_site_handler('URL'))"
```

## Trace Files

When running with `DEBUG_EXTRACTION_VERBOSE=1` or using debug mode:
- `*.md` - Concise summary for quick review
- `*.json` - Detailed data, searchable with `rg <pattern> *.json`
