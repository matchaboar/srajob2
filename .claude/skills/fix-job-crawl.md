---
name: fix-job-crawl
description: Debug job listing/crawl issues with automated fixture generation
---

# Fix Job Crawl

Debug and fix job listing page URL extraction issues.

## Usage

```
/fix-job-crawl <company-name-or-url>
```

## What This Does

1. Fetches the listing page from SpiderCloud using production config
2. Saves the response as a debug fixture
3. Extracts URLs using the production handler code
4. Creates assertion file with extracted URLs for validation
5. Provides debugging context and test commands

## Examples

```bash
# Using company name (looks up in site_schedules.yml)
/fix-job-crawl airbnb

# Using listing URL directly
/fix-job-crawl https://api.greenhouse.io/v1/boards/airbnb/jobs

# Using Convex site ID
/fix-job-crawl --site-id k57siteid123

# Dry run (show what would be created)
/fix-job-crawl airbnb --dry-run
```

## Implementation

This skill invokes:
```bash
# With company name
uv run python agent_scripts/core/generate_debug_listing_fixture.py --company "${COMPANY}" --output-format json

# With URL
uv run python agent_scripts/core/generate_debug_listing_fixture.py --url "${URL}" --output-format json
```

## Output

The script outputs JSON with the following structure:
```json
{
  "listing_url": "https://...",
  "fixture_path": "tests/.../fixtures/debug/company/handler_timestamp_listing.json",
  "assertion_path": "tests/.../assertions/debug/company/handler_timestamp_listing.yml",
  "identifier": "company/handler_timestamp",
  "company": "Company Name",
  "handler": "greenhouse",
  "extracted_url_count": 150,
  "extracted_urls": ["https://...", "..."]
}
```

## Workflow

After running this skill:

1. **CRITICAL: Validate extracted URLs** - Review every URL in the output:
   - Is it a valid job detail URL?
   - Is it NOT a listing/search page?
   - Is the format correct (no corruption)?

2. **Update assertion file** - Add `expected_urls` with ONLY valid URLs:
   ```yaml
   expected:
     url_count_min: 50
     url_pattern: "^https://boards-api\\.greenhouse\\.io/v1/boards/[^/]+/jobs/\\d+$"
     expected_urls:
       - "https://valid-url-1"
       - "https://valid-url-2"
   ```

3. **Run the debug test** - Execute with verbose output:
   ```bash
   DEBUG_EXTRACTION_VERBOSE=1 uv run pytest 'tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction[${identifier}]' -v
   ```

4. **Check extraction output** - Review detailed steps:
   ```bash
   cat ./site-detail-e2e-examples/${handler}_listing_extraction.json
   cat ./site-detail-e2e-examples/${handler}_listing_extraction_steps.md
   ```

5. **Fix handler if needed** - Update URL extraction in handler:
   - `get_links_from_json()` for JSON API responses
   - `get_links_from_raw_html()` for HTML scraping
   - `filter_job_urls()` for URL filtering

6. **Verify fix** - Re-run tests to ensure extraction works correctly

## Key Features

- **Per-company organization**: Fixtures saved in `fixtures/debug/{company}/` folders
- **Timestamp-based naming**: ISO format (YYYYMMDDTHHMMSS) for unique identification
- **Production handler code**: Uses actual site handler logic for extraction
- **URL validation**: Lists all extracted URLs for review
- **Handler detection**: Shows which handler was used for the listing page

## Common Issues

### No URLs Extracted
- Check if page rendered properly in fixture
- Verify handler has correct extraction logic
- May need to add URL extraction method

### Wrong URLs Extracted
- Update handler's `filter_job_urls()` method
- Check `_is_probable_listing_url()` detection
- May need URL canonicalization

### Wrong Handler Detected
- Check handler registration in `site_handlers/__init__.py`
- Update URL pattern matching
