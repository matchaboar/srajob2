# Edge Cases Tests

This directory contains company-specific edge case tests that validate specific scenarios, bugs, or unusual behavior for individual sites.

## Test Categories

### Convex Job Fixtures
Tests that validate specific production job extraction issues using Convex job IDs:
- `test_*_convex_job_*.py` - Tests for specific job IDs that had extraction problems

### Site Handler Edge Cases
- `test_*_url_dedup.py` - URL deduplication edge cases
- `test_*_pagination.py` - Pagination edge cases
- `test_*_listing_batch.py` - Listing batch processing edge cases
- `test_*_empty_*.py` - Empty response handling

### SpiderCloud Integration
- `test_spidercloud_*_detail.py` - Detail page scraping edge cases
- `test_spidercloud_*_listing_api.py` - Listing API edge cases

## Running Tests

```bash
# Run all edge case tests
uv run pytest tests/job_scrape_application/workflows/edge_cases/ -v

# Run specific company edge cases
uv run pytest tests/job_scrape_application/workflows/edge_cases/ -k hubspot -v
```

## Adding New Edge Cases

When you encounter a production issue:
1. Generate a fixture using `mise run fix_job_extraction <url>`
2. Create a test file in this directory
3. Use `WorkflowTestModule` from `test_job_detail_extraction_e2e.py` for consistency
