---
name: refresh-fixtures
description: Bulk regenerate test fixtures from production schedule
---

# Refresh Fixtures

Bulk regenerate timestamped test fixtures for all sites (or specific sites) from the site schedule.

## Usage

```
/refresh-fixtures [--only site1 site2 ...] [--limit N]
```

## What This Does

1. Loads site schedules from `site_schedules.yml` (dev or prod)
2. For each site:
   - Fetches listing page via SpiderCloud using production workflow code
   - Extracts job URLs from the listing
   - Selects one detail URL and fetches it
   - Saves both listing and detail fixtures with timestamps
   - Generates assertion files for validation
3. Creates NEW fixtures (doesn't overwrite existing ones)
4. Outputs summary JSON with all generated fixtures

## Examples

```bash
# Regenerate all sites from prod schedule
/refresh-fixtures --schedule-env prod

# Regenerate specific sites only
/refresh-fixtures --schedule-env prod --only airbnb purestorage netflix

# Limit to first N sites (for testing)
/refresh-fixtures --schedule-env prod --limit 5

# Use dev schedule
/refresh-fixtures --schedule-env dev
```

## Implementation

This skill invokes:
```bash
uv run python agent_scripts/core/refresh_all_site_fixtures.py \
  --schedule-env prod \
  --output-format json
```

## Output

The script outputs JSON with the following structure:
```json
{
  "timestamp": "20260115T123456",
  "generated_count": 42,
  "fixtures": [
    {
      "company": "Airbnb",
      "company_slug": "airbnb",
      "handler": "greenhouse",
      "identifier": "airbnb/greenhouse_20260115T123456",
      "listing_url": "https://api.greenhouse.io/v1/boards/airbnb/jobs",
      "detail_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/123",
      "fixture_listing_path": "tests/.../airbnb/greenhouse_20260115T123456_listing.json",
      "fixture_detail_path": "tests/.../airbnb/greenhouse_20260115T123456_detail.json",
      "assertion_listing_path": "tests/.../airbnb/greenhouse_20260115T123456_listing.yml",
      "assertion_detail_path": "tests/.../airbnb/greenhouse_20260115T123456_detail.yml",
      "extracted_url_count": 150
    },
    ...
  ]
}
```

## Workflow

After running this skill:

1. **Review generated fixtures** - Check the output summary:
   - How many sites were processed?
   - Any failures or warnings?
   - URL counts look reasonable?

2. **Validate a sample** - Pick a few fixtures to validate:
   ```bash
   # Check listing extraction
   DEBUG_EXTRACTION_VERBOSE=1 uv run pytest 'tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction[airbnb/greenhouse_20260115T123456]' -v

   # Check detail extraction
   uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[greenhouse_20260115T123456] -v
   ```

3. **Review extraction outputs**:
   ```bash
   ls site-detail-e2e-examples/*_20260115T123456_*
   ```

4. **Update assertions if needed** - Fill in TODOs in generated assertion files

5. **Run full test suite** - Ensure all tests pass:
   ```bash
   uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v
   uv run pytest tests/job_scrape_application/workflows/test_listing_extraction_e2e.py -v
   ```

## When to Use This

- **After site handler changes**: Regenerate fixtures to ensure changes work across all sites
- **Before major releases**: Validate all sites still work correctly
- **Adding new sites**: Generate baseline fixtures for comparison
- **Debugging handler issues**: Create fresh fixtures to reproduce problems
- **Testing SpiderCloud changes**: Verify scraping still works after SpiderCloud updates

## Scheduling Environments

### Dev Schedule (`--schedule-env dev`)
- Location: `job_scrape_application/config/dev/site_schedules.yml`
- Use for: Testing new sites, experimental configs
- Smaller set of sites, more frequent updates

### Prod Schedule (`--schedule-env prod`)
- Location: `job_scrape_application/config/prod/site_schedules.yml`
- Use for: Production validation, regression testing
- All active sites, stable configurations

## Timestamp Format

Fixtures use ISO 8601 timestamp format: `YYYYMMDDTHHMMSS`
- Example: `20260115T123456` = January 15, 2026 at 12:34:56
- Ensures unique filenames for each run
- Preserves history (old fixtures not overwritten)
- Sortable chronologically

## Key Features

- **Batch processing**: Handles multiple sites in one run
- **Production workflow code**: Uses actual scraping logic from `fetch_spidercloud_fixtures.py`
- **Timestamped output**: Never overwrites existing fixtures
- **Per-company organization**: Saves fixtures in `debug/{company}/` folders
- **Parallel processing**: Can process multiple sites concurrently
- **Filtering support**: `--only` and `--limit` for selective regeneration

## Performance Notes

- Each site requires 2 SpiderCloud requests (listing + detail)
- Average time: 5-10 seconds per site
- For all sites (~50): Allow 5-10 minutes
- Use `--limit` for faster iteration during development
