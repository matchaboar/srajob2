---
name: fix-job-extraction
description: Debug job extraction issues with automated fixture generation
---

# Fix Job Extraction

Debug and fix job detail extraction issues for production jobs.

## Usage

```
/fix-job-extraction <job-url-or-id>
```

## What This Does

1. Extracts job ID from URL (supports share URLs like https://srajob.netlify.app/job/k57abc123)
2. Fetches job data from Convex production database
3. Generates SpiderCloud fixture for the job detail page
4. Creates assertion file with expected values from Convex data
5. Provides debugging context and test commands

## Examples

```bash
# Using job share URL
/fix-job-extraction https://srajob.netlify.app/job/k57abc123xyz

# Using job ID directly
/fix-job-extraction k57abc123xyz

# With custom detail URL override
/fix-job-extraction k57abc123xyz --url https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/123

# Dry run (show what would be created)
/fix-job-extraction k57abc123xyz --dry-run
```

## Implementation

This skill invokes:
```bash
uv run python agent_scripts/core/generate_debug_fixture.py "${JOB_ID}" --output-format json
```

## Output

The script outputs JSON with the following structure:
```json
{
  "job_id": "k57abc123xyz",
  "detail_url": "https://...",
  "fixture_path": "tests/.../fixtures/debug/company/handler_id_date_detail.json",
  "assertion_path": "tests/.../assertions/debug/company/handler_id_date.yml",
  "identifier": "handler_id_date",
  "company": "Company Name",
  "handler": "greenhouse",
  "remote_override": false
}
```

## Workflow

After running this skill:

1. **Review the assertion file** - Check that expected values match the job posting
2. **Run the debug test** - Execute the test with verbose output:
   ```bash
   DEBUG_EXTRACTION_VERBOSE=1 uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[${identifier}] -v
   ```
3. **Check extraction output** - Review the detailed extraction steps:
   ```bash
   cat ./site-detail-e2e-examples/${handler}_extraction.json
   cat ./site-detail-e2e-examples/${handler}_extraction_steps.md
   ```
4. **Fix handler if needed** - Update site handler in `job_scrape_application/workflows/site_handlers/${handler}.py`
5. **Verify fix** - Re-run tests to ensure extraction works correctly

## Key Features

- **Per-company organization**: Fixtures saved in `fixtures/debug/{company}/` folders
- **Date-based naming**: Preserves history with timestamped filenames
- **URL canonicalization**: Automatically converts marketing URLs to API URLs (e.g., Greenhouse)
- **Remote override detection**: Warns if company is in `remote_companies.yaml`
- **Convex integration**: Pulls actual production data for accurate assertions
