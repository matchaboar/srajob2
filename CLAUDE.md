# CLAUDE.md

This file provides guidance for Claude Code (claude.ai/claude-code) when working with this repository.

## Repository Overview

This is a job scraping and job board application with two main components:
- `job_scrape_application/` - Python DBOS workflows for scraping job listings via SpiderCloud
- `job_board_application/` - Vite + React UI with Convex backend for displaying jobs

## Key Commands

```bash
# Python (from repo root)
uv run pytest                                    # Run all tests
uv run pytest tests/path/test_file.py           # Run specific test file
uv run pytest -k "pattern"                       # Run tests matching pattern
uv run pytest --lf                               # Re-run only last failed tests
uvx ruff check .                                 # Lint Python code

# Job board UI (from job_board_application/)
pnpm run dev                                     # Dev frontend + backend
pnpm run test                                    # Run Vitest tests

# Convex (from job_board_application/)
npx convex run --prod router:runSiteNow '{"id":"..."}'  # Trigger site scrape
```

## Claude Skills (Recommended)

For common debugging and maintenance tasks, use Claude Skills for guided, automated workflows:

### Available Skills

#### /fix-job-extraction
Debug job detail extraction issues with automated fixture generation.

```bash
/fix-job-extraction https://srajob.netlify.app/job/k57abc123xyz
```

What this does:
1. Fetches job data from Convex production
2. Downloads SpiderCloud fixture to debug folder
3. Creates assertion template with TODOs
4. Provides test commands and debugging context

Use this when a specific job is not extracting correctly from production.

#### /fix-job-crawl
Debug job listing extraction issues (URL discovery problems).

```bash
/fix-job-crawl airbnb
# or
/fix-job-crawl https://api.greenhouse.io/v1/boards/airbnb/jobs
```

What this does:
1. Fetches listing page from SpiderCloud
2. Extracts URLs using production handler
3. Creates listing fixture and assertion
4. Shows extracted URLs for validation

Use this when a site's listing page is not finding job URLs correctly.

#### /add-site
Add a new company site to the scraper system.

```bash
/add-site https://careers.newcompany.com/jobs/12345
```

What this does:
1. Analyzes URL to identify company and handler type
2. Generates both listing and detail fixtures
3. Creates assertion templates
4. Provides schedule entry YAML
5. Guides through testing and deployment

Use this when adding a new company to the scraper.

#### /refresh-fixtures
Bulk regenerate fixtures for multiple sites.

```bash
/refresh-fixtures --schedule-env prod --only airbnb purestorage
# or
/refresh-fixtures --schedule-env prod --limit 5
```

What this does:
1. Fetches fresh fixtures from production schedule
2. Generates new timestamped fixtures
3. Creates assertion templates
4. Provides batch validation commands

Use this when updating fixtures for multiple sites at once.

### Skills vs. Direct Scripts

Claude Skills provide:
- ✅ Automated multi-step workflows
- ✅ Guided interaction with validation
- ✅ Context-aware error handling
- ✅ Best practices enforcement

For one-off operations or scripting, you can also use the underlying scripts directly:
- See `agent_scripts/README.md` for direct script usage
- See `.claude/skills/README.md` for detailed skill documentation

## Testing Job Detail Extraction

### Test Files Location
- `tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py` - E2E tests for job detail extraction
- `tests/job_scrape_application/workflows/fixtures/dbos_schedule/` - Fixture files (`{site}_detail.json`)
- `tests/job_scrape_application/workflows/assertions/` - YAML assertion files for validation

### Running Extraction Tests
```bash
# Run all extraction accuracy tests
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy -v

# Run for a specific site
uv run pytest "tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[purestorage]" -v

# Run all tests for the extraction e2e module
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py -v
```

### Fixture Format
Detail fixtures are JSON files with SpiderCloud response data:
```json
{
  "request": {
    "url": "https://...",
    "params": { ... }
  },
  "response": [
    "{\"content\":{\"commonmark\":\"...\",\"raw\":\"...\"},\"costs\":{...}}"
  ]
}
```

### Assertion Files
YAML files in `assertions/` define expected extraction values:
```yaml
site_id: purestorage
detail_url: https://boards-api.greenhouse.io/v1/boards/purestorage/jobs/7472241
expected:
  title: AI/HPC Pre-Sales Systems Engineer
  company: Pure Storage
  location: Atlanta, Georgia; Austin, Texas; ...
  is_remote: false
  level: senior
  description_min_words: 50
  cost_milli_cents_min: 1
  posted_at_not_null: true
```

### Supported Assertion Types
- `title` / `title_contains` - Exact or partial title match
- `company` / `company_contains` - Exact or partial company match
- `location` / `location_contains` - Exact or partial location match
- `is_remote` - Boolean remote status
- `level` - Exact level (junior/mid/senior/staff)
- `description_min_words` / `description_contains` - Description validation
- `cost_milli_cents_min` / `cost_milli_cents_max` - Cost range
- `posted_at_not_null` - Check posted date presence

### Regenerating Fixtures
Use the fixture generation script to fetch fresh SpiderCloud data:
```bash
# Regenerate fixtures for a specific site (uses production workflow code)
uv run python agent_scripts/core/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-only SITE_NAME

# Regenerate all fixtures from prod schedule
uv run python agent_scripts/core/fetch_spidercloud_fixtures.py --schedule-env prod

# Regenerate from dev schedule
uv run python agent_scripts/core/fetch_spidercloud_fixtures.py --schedule-env dev
```

The script:
1. Reads site config from `job_scrape_application/config/{env}/site_schedules.yml`
2. Fetches listing page via SpiderCloud using production workflow code
3. Extracts job URLs and selects one for detail fixture
4. Fetches detail page and writes both `{site}_listing.json` and `{site}_detail.json`

### Updating Assertions
After regenerating fixtures, update the assertion file if needed:
```bash
# 1. Run the test to generate extraction output
uv run pytest "tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[SITE_NAME]" -v

# 2. View the extraction result
cat ./site-detail-e2e-examples/SITE_NAME_extraction.json

# 3. Update assertions/SITE_NAME.yml with correct expected values
```

Ensure `detail_url` in the assertion file matches `request.url` in the fixture.

## Debugging Production Jobs

For debugging specific user-submitted jobs with extraction issues, **use the Claude Skill (recommended)**:

### Using Claude Skills (Recommended)
```bash
/fix-job-extraction https://srajob.netlify.app/job/k57abc123xyz
```

See the [Claude Skills](#claude-skills-recommended) section above for full details.

### Using Mise Tasks (Alternative)
```bash
mise run fix_job_extraction https://srajob.netlify.app/job/k57abc123xyz
```

Both approaches automatically:
1. ✅ Fetch job data from Convex prod
2. ✅ Download SpiderCloud fixture to debug folder
3. ✅ Create placeholder assertion file
4. ✅ Provide debugging context

**Then you just need to:**
1. Fill in assertion TODOs (location, level, etc.)
2. Run debug test
3. Fix handler if needed
4. Verify fix

### Manual Workflow (if needed)
```bash
# 1. Fetch the job page
PYTHONPATH=/home/boarcoder/documents/github/srajob2 uv run python agent_scripts/core/dump_spidercloud_response.py \
  "https://explore.jobs.netflix.net/careers/job/790313551266" \
  --out tests/job_scrape_application/workflows/fixtures/debug/netflix_790313551266_detail.json \
  --use-handler-config

# 2. Create assertion file at tests/job_scrape_application/workflows/assertions/debug/netflix_790313551266.yml

# 3. Run the debug test
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v

# 4. Check extraction output
cat ./site-detail-e2e-examples/netflix_extraction.json
```

### Why Use Debug Fixtures?
- **Isolated testing**: Test specific production jobs without affecting the main test suite
- **Rapid iteration**: Quickly test fixes for reported issues
- **Clear workflow**: Fixture → Assertions → Test → Fix → Verify
- **Preserved examples**: Keep examples of edge cases for future reference

### Debug Fixture Structure
```
fixtures/debug/
├── README.md                           # Full documentation
├── {site}_{job_id}_detail.json        # Job fixture
└── ...

assertions/debug/
├── {site}_{job_id}.yml                # Job assertions
└── ...
```

See `tests/job_scrape_application/workflows/fixtures/debug/README.md` for detailed documentation.

## Debugging Workflow

See `DEBUGGING.md` for detailed debugging procedures including:
- Generating fixtures from SpiderCloud
- Testing extraction against production data
- Using the WorkflowTestModule for isolated testing
- Tracing normalization issues

## Architecture Notes

### WorkflowTestModule
The test module (`WorkflowTestModule`) simulates the full production workflow:
1. Mocks SpiderCloud client with fixture data
2. Runs actual scraper normalization code
3. Captures Convex mutations without network calls
4. Validates against YAML assertions

Changes to production code (handlers, scrapers, normalizers) are automatically reflected in tests.

### Site Handlers
Located in `job_scrape_application/workflows/site_handlers/`:
- `greenhouse.py` - Greenhouse job boards (API + marketing pages)
- `ashbyhq.py` - AshbyHQ job boards
- `workday.py` - Workday career sites
- etc.

Each handler implements `normalize_markdown()` for extracting job data from scraped content.
