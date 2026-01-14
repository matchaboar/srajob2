# Debug Fixtures

This folder contains fixtures for debugging specific user-submitted job extraction issues. These are separate from the main test suite fixtures to allow rapid iteration on production bugs without affecting automated tests.

## Directory Structure

```
debug/
├── README.md                           # This file
├── netflix_790313551266_detail.json   # Fixture for specific job
└── ...                                 # More debug fixtures

../ground_truth/debug/
├── netflix_790313551266.yml           # Assertions for specific job
└── ...                                 # More debug ground_truth
```

## Quick Start (Automated Workflow)

**Use the mise task for automatic setup:**

```bash
mise run fix_job_extraction https://srajob.netlify.app/job/k57abc123xyz
```

This will:
1. ✅ Fetch job data from Convex prod
2. ✅ Automatically fetch SpiderCloud fixture to debug folder
3. ✅ Create placeholder assertion file with TODOs
4. ✅ Launch Claude Code with all context

**Then Claude just needs to**:
1. Fill in the assertion TODOs (location, level, description requirements)
2. Run the debug test
3. Fix the handler if needed
4. Verify the fix

## Manual Workflow (Alternative)

If you prefer to do it manually or the automated script doesn't work:

## Workflow for Debugging a Production Job

### 1. Identify the Problem Job

From Convex production data, get the job details including:
- Job URL
- Current extracted data (title, description, etc.)
- What's wrong (e.g., missing description, wrong location)

### 2. Create a Fixture

Use the `dump_spidercloud_response.py` script to fetch the job page:

```bash
# Fetch the job detail page
PYTHONPATH=/home/boarcoder/documents/github/srajob2 uv run python agent_scripts/dump_spidercloud_response.py \
  "https://explore.jobs.netflix.net/careers/job/790313551266" \
  --out tests/job_scrape_application/workflows/fixtures/debug/netflix_790313551266_detail.json \
  --use-handler-config
```

The naming convention is: `{site}_{job_id}_detail.json`

### 3. Create Assertions

Create a YAML file in `tests/job_scrape_application/workflows/ground_truth/debug/` with expected values:

```yaml
site_id: netflix
detail_url: https://explore.jobs.netflix.net/careers/job/790313551266
expected:
  title: Software Engineer L5, Ads Campaign Management
  company: Netflix
  location_contains: New York
  is_remote: false
  level: mid
  description_min_words: 300
  description_not_contains: '{"domain":'  # Ensure JSON blocks are stripped
  description_not_contains_2: '{"display_banner":'
  cost_milli_cents_min: 1
  posted_at_not_null: true
```

The naming convention is: `{site}_{job_id}.yml`

### 4. Run the Test

Run the debug test to reproduce the issue:

```bash
# Run all debug tests
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v

# Run specific debug test
uv run pytest "tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[netflix_790313551266]" -v
```

The test will fail if extraction doesn't match ground_truth, helping you identify the problem.

### 5. Fix the Handler

Modify the site handler in `job_scrape_application/workflows/site_handlers/` to fix the extraction issue. Common fixes:

- Implement or update `normalize_markdown()` to clean up the description
- Adjust `extract_location_hint()` for better location extraction
- Update regex patterns for title/company extraction

### 6. Verify the Fix

Re-run the debug test to verify the fix works:

```bash
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v
```

Check the extraction output in `./site-detail-e2e-examples/{site}_extraction.json` to see the extracted data.

### 7. Run Main Tests

Ensure you didn't break existing tests:

```bash
# Run all extraction tests for the site
uv run pytest "tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[{site}]" -v
```

## Supported Assertion Types

See CLAUDE.md for full list of assertion types. Common ones:

- `title` / `title_contains`: Job title validation
- `company` / `company_contains`: Company name validation
- `location` / `location_contains`: Location validation
- `is_remote`: Remote status (true/false)
- `level`: Job level (junior/mid/senior/staff)
- `description_min_words`: Minimum word count for description
- `description_contains`: Content that should be present
- `description_not_contains`: Content that should NOT be present (e.g., JSON blocks)
- `cost_milli_cents_min` / `cost_milli_cents_max`: Cost validation
- `posted_at_not_null`: Check for posted date

## Example: Netflix JSON Block Issue

**Problem**: Netflix job descriptions contained JSON configuration blocks instead of actual content.

**Root Cause**: The base handler's `normalize_markdown` method doesn't strip Netflix's specific JSON blocks.

**Solution**: Implemented `normalize_markdown` in `NetflixHandler` to:
1. Strip initial title line with separators
2. Remove JSON config blocks (backtick lines with JSON)
3. Remove "** All Jobs" metadata
4. Remove trailing JSON blocks
5. Clean up "Apply Now" buttons

**Result**: Description went from 122 words (mostly JSON) to 1010 words (actual content).

## Tips

1. **Name fixtures clearly**: Use `{site}_{job_id}_detail.json` format
2. **Test incrementally**: Create fixture → create ground_truth → run test → fix → verify
3. **Check extraction output**: Always review `./site-detail-e2e-examples/` to see what was extracted
4. **Keep fixtures small**: Debug fixtures are for specific issues, not comprehensive testing
5. **Document fixes**: Add comments in the handler explaining what edge case you're handling
