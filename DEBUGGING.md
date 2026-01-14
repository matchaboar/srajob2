# DEBUGGING.md

Detailed debugging procedures for job scraping workflows and extraction issues.

## Table of Contents
- [Quick Reference: Debugging a Specific Job](#quick-reference-debugging-a-specific-job)
- [Quick Reference: Debugging Listing/Crawl Issues](#quick-reference-debugging-listingcrawl-issues)
- [Modular Extractor Debug Output](#modular-extractor-debug-output)
- [Core Module Architecture](#core-module-architecture)
- [Job Detail Extraction Issues](#job-detail-extraction-issues)
- [Listing Page Extraction Issues](#listing-page-extraction-issues)
- [Generating Fixtures](#generating-fixtures)
- [Debug Fixtures Workflow](#debug-fixtures-workflow)
- [Testing with WorkflowTestHelper](#testing-with-workflowtesthelper)
- [Adding/Updating Assertions](#addingupdating-assertions)
- [Validating Against Convex Production](#validating-against-convex-production)
- [Common Issues and Fixes](#common-issues-and-fixes)

---

## Quick Reference: Debugging a Specific Job

### Option 1: Automated (Recommended)

```bash
# Use the mise task - handles everything automatically
mise run fix_job_extraction https://srajob.netlify.app/job/k57abc123xyz
```

### Option 2: Manual Step-by-Step

```bash
# 1. Fetch fixture with handler config (uses --fixture-format for test compatibility)
uv run python agent_scripts/dump_spidercloud_response.py \
  "https://example.com/job/123" \
  --out tests/job_scrape_application/workflows/fixtures/debug/example_123_detail.json \
  --use-handler-config \
  --fixture-format

# 2. Create assertion file at:
#    tests/job_scrape_application/workflows/assertions/debug/example_123.yml

# 3. Run debug test
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v

# 4. Check extraction output
cat ./site-detail-e2e-examples/example_extraction.json

# 5. Fix handler if needed, re-run test
```

---

## Quick Reference: Debugging Listing/Crawl Issues

When a site isn't extracting job URLs correctly from the listing page, use this workflow.

### Option 1: Automated (Recommended)

```bash
# Use the mise task - handles everything automatically
mise run fix_job_crawl airbnb

# Or with a direct URL
mise run fix_job_crawl https://api.greenhouse.io/v1/boards/airbnb/jobs
```

### Option 2: Manual Step-by-Step

```bash
# 1. Generate listing fixture
uv run python agent_scripts/generate_debug_listing_fixture.py --company airbnb

# 2. Create assertion file at:
#    tests/job_scrape_application/workflows/assertions/debug/{company}/{handler}_{timestamp}_listing.yml
#    where timestamp is YYYYMMDDTHHMMSS format (e.g., 20260114T153022)

# 3. Run debug test with verbose output
DEBUG_EXTRACTION_VERBOSE=1 uv run pytest 'tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction[IDENTIFIER]' -v

# 4. Check extraction output
cat ./site-detail-e2e-examples/{handler}_listing_extraction.json
cat ./site-detail-e2e-examples/{handler}_listing_extraction_steps.md

# 5. Fix handler if needed, re-run test
```

### Verbose Listing Extraction Steps

When `DEBUG_EXTRACTION_VERBOSE=1` is set, the test outputs a detailed markdown file showing:

1. **Raw SpiderCloud Response** - HTML/markdown from listing page
2. **Handler Detection** - Which handler was selected
3. **URL Extraction Method** - JSON API, HTML links, or regex fallback
4. **Extracted URLs** - All URLs found
5. **URL Filtering** - What was kept vs rejected
6. **Pagination Detection** - Any pagination URLs found
7. **Queue Enqueue Summary** - URLs to be queued

---

## Modular Extractor Debug Output

The new modular extractor system (`job_scrape_application/workflows/extractors/`) provides detailed strategy-by-strategy tracing for each extracted field.

### Using the Extractors for Debugging

```python
from job_scrape_application.workflows.extractors import (
    ExtractionContext,
    extract_job_fields,
    format_debug_trace,
)

# Create context with debug=True to run ALL strategies
context = ExtractionContext.from_scrape_result(
    url=job_url,
    markdown=scraped_content,
    handler=site_handler,
    raw_row={"job_title": "Engineer", "location": "NYC"},
    debug=True,
)

# Extract all fields
results = extract_job_fields(context, run_all=True)

# View strategy trace for each field
for field, result in results.items():
    print(f"\n{field}: {result.final_value}")
    print(f"  Winner: {result.winning_strategy}")
    for sr in result.all_results:
        status = "VALID" if sr.is_valid else "SKIP"
        print(f"    [{status}] {sr.strategy_name}: {sr.reason}")
```

### Debug Output Format

When `run_all=True`, each field shows all strategies tried:

```
title: Senior Software Engineer
  Winner: raw_row_title
  Strategies tried: 6
    [SKIP] structured_data_title (STRUCTURED_DATA): No structured data available
    [SKIP] site_handler_title (SITE_HANDLER): Handler 'base' did not extract title
    [VALID] raw_row_title (EXPLICIT_FIELD): Valid title
    [VALID] markdown_heading_title (CONTENT_PATTERN): Valid title
    [SKIP] hinted_title (HEURISTIC): No title in hints
    [VALID] first_line_title (FALLBACK): Valid title
```

### Strategy Priority Levels

Strategies are tried in priority order (lower = higher priority):

| Priority | Name | Description |
|----------|------|-------------|
| 100 | STRUCTURED_DATA | JSON-LD, API responses |
| 200 | SITE_HANDLER | Site-specific handlers (Greenhouse, Ashby, etc.) |
| 300 | EXPLICIT_FIELD | Labeled fields (Location: X) |
| 400 | URL_DERIVED | Extracted from URL patterns |
| 500 | CONTENT_PATTERN | Regex patterns in content |
| 600 | HEURISTIC | Fuzzy matching, inference |
| 900 | FALLBACK | Last resort defaults |

### Available Extractors

| Field | Extractor Class | Strategies |
|-------|-----------------|------------|
| title | `JobTitleExtractor` | 6 strategies |
| company | `CompanyExtractor` | 7 strategies |
| location | `LocationExtractor` | 8 strategies |
| remote | `RemoteExtractor` | 7 strategies |
| level | `LevelExtractor` | 6 strategies |
| compensation | `CompensationExtractor` | 5 strategies |
| posted_at | `PostedAtExtractor` | 6 strategies |
| description | `DescriptionExtractor` | 5 strategies |

### Debugging a Specific Field

```python
from job_scrape_application.workflows.extractors import (
    ExtractionContext,
    extract_field,
)

# Extract just one field with full trace
context = ExtractionContext(
    url="https://example.com/job/123",
    raw_markdown="# Senior Engineer\n\nLocation: San Francisco",
    debug=True,
)

result = extract_field(context, "location", run_all=True)
print(f"Location: {result.final_value}")
print(f"Winner: {result.winning_strategy}")
print(f"All strategies: {[r.strategy_name for r in result.all_results]}")
```

### JSON Debug Trace

Get structured JSON output for logging:

```python
from job_scrape_application.workflows.extractors import get_debug_trace

results = extract_job_fields(context, run_all=True)
trace = get_debug_trace(results)

# trace is a dict with this structure:
# {
#   "title": {
#     "field": "title",
#     "final_value": "Senior Software Engineer",
#     "winning_strategy": "raw_row_title",
#     "strategy_results": [
#       {"strategy": "structured_data_title", "priority": "STRUCTURED_DATA", ...},
#       {"strategy": "raw_row_title", "priority": "EXPLICIT_FIELD", ...},
#       ...
#     ]
#   },
#   "location": {...},
#   ...
# }
```

### Adding Custom Strategies

To add a new extraction strategy for a field:

```python
# In extractors/title_extractor.py

class MyCustomTitleStrategy(ExtractionStrategy[str]):
    name = "my_custom_title"
    priority = StrategyPriority.HEURISTIC - 10  # Just before other heuristics

    def extract(self, context: ExtractionContext) -> StrategyResult[str]:
        # Your extraction logic here
        if some_condition:
            return self._make_result(
                extracted_title,
                "Found title via my custom method",
                is_valid=True,
                confidence=0.75,
            )
        return self._make_skip_result("My custom method didn't find a title")

# Add to JobTitleExtractor._register_strategies()
```

---

## Core Module Architecture

The refactored workflow code uses dependency injection for testability. The core module at `job_scrape_application/workflows/core/` provides:

### Key Classes

```python
from job_scrape_application.workflows.core import (
    # Main container
    DependencyContainer,

    # Test helpers
    WorkflowTestHelper,      # Sets up mocks for workflow testing
    SpiderFixture,           # Loads fixtures from JSON files
    CapturedConvexData,      # Container for captured operations

    # Mock implementations
    MockConvexFunctions,     # Returns fixture data for Convex
    MockQueueService,        # In-memory queue for testing
    MockSpiderClient,        # Returns fixture data for SpiderCloud

    # For fixture generation
    CapturingSpiderClient,   # Wraps real client, captures requests/responses
)
```

### DependencyContainer Modes

```python
# Production mode - real services
deps = DependencyContainer.production()

# Testing mode - mocks with fixtures
deps = DependencyContainer.testing(
    query_fixtures={"router:getSiteById": {...}},
    mutation_fixtures={...},
    captured_mutations=my_capture_list,
)

# Capturing mode - real services, captures for fixture generation
deps = DependencyContainer.capturing(
    captured_queries=query_list,
    captured_mutations=mutation_list,
    captured_scrapes=scrape_list,
)
```

---

## Job Detail Extraction Issues

When job titles, descriptions, or other fields aren't extracting correctly, follow this debugging workflow.

### Step 1: Run the Extraction Test

```bash
# Run test for the specific site
uv run pytest "tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[SITE_NAME]" -v

# Check the extraction output
cat ./site-detail-e2e-examples/SITE_NAME_extraction.json
```

### Step 2: Examine the Fixture

```bash
# View the raw fixture (first 2000 chars)
head -c 2000 tests/job_scrape_application/workflows/fixtures/dbos_schedule/SITE_NAME_detail.json

# Parse and inspect the fixture response
uv run python << 'PYEOF'
import json
from job_scrape_application.workflows.core import SpiderFixture
from pathlib import Path

fixture = SpiderFixture.from_file(
    Path('tests/job_scrape_application/workflows/fixtures/dbos_schedule/SITE_NAME_detail.json')
)
print(f"URL: {fixture.url}")
print(f"Is sync mode: {fixture.is_sync}")
print(f"Response type: {type(fixture.response)}")
PYEOF
```

### Step 3: Test Handler Normalization

```bash
uv run python << 'PYEOF'
import json
from job_scrape_application.workflows.site_handlers import get_site_handler

# Load fixture
with open('tests/job_scrape_application/workflows/fixtures/dbos_schedule/SITE_NAME_detail.json') as f:
    fixture = json.load(f)

# Get the URL and handler
url = fixture['request']['url']
handler = get_site_handler(url)
print(f'Handler: {handler.name if handler else None}')

# Parse response and normalize
response = json.loads(fixture['response'][0])
commonmark = response['content']['commonmark']

normalized_markdown, title = handler.normalize_markdown(commonmark)
print(f'Title: {title}')
print(f'Markdown preview: {normalized_markdown[:200] if normalized_markdown else None}')
PYEOF
```

### Step 4: Verify Handler Detection

```bash
uv run python -c "
from job_scrape_application.workflows.site_handlers import get_site_handler
url = 'YOUR_JOB_DETAIL_URL'
handler = get_site_handler(url)
print(f'Handler: {handler.name if handler else None}')
print(f'Is API detail URL: {handler.is_api_detail_url(url) if handler else None}')
"
```

---

## Listing Page Extraction Issues

When a site's listing page isn't extracting job URLs correctly, follow this debugging workflow.

### Understanding URL Extraction Methods

The extraction process tries these methods in order:

1. **JSON API** - Parse structured JSON response (Greenhouse, AshbyHQ, Netflix APIs)
   - Handler's `get_links_from_json()` method
   - Returns array of absolute URLs

2. **HTML Links** - Extract hrefs from anchor tags
   - Handler's `get_links_from_raw_html()` method
   - Uses CSS selectors or tag searching

3. **Regex Fallback** - Search for URL patterns in raw text
   - `_regex_extract_job_urls_from_events()` in scraper
   - Last resort for unusual page formats

### Step 1: Run the Listing Extraction Test

```bash
# Run test for a specific site using verbose output
DEBUG_EXTRACTION_VERBOSE=1 uv run pytest 'tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction[IDENTIFIER]' -v

# Check the extraction output
cat ./site-detail-e2e-examples/{handler}_listing_extraction.json
cat ./site-detail-e2e-examples/{handler}_listing_extraction_steps.md
```

### Step 2: Examine the Fixture Content

```bash
# View raw fixture content type
uv run python << 'PYEOF'
import json
from pathlib import Path

fixture_path = Path('tests/.../fixtures/debug/COMPANY/handler_date_listing.json')
fixture = json.loads(fixture_path.read_text())

response = fixture.get('response', [])
if isinstance(response, list) and response:
    first = response[0]
    if isinstance(first, str):
        parsed = json.loads(first)
        content = parsed.get('content', {})
        print(f"Has commonmark: {bool(content.get('commonmark'))}")
        print(f"Has raw HTML: {bool(content.get('raw'))}")
        print(f"Content preview: {(content.get('commonmark') or content.get('raw', ''))[:500]}")
PYEOF
```

### Step 3: Test Handler URL Extraction

```bash
uv run python << 'PYEOF'
from job_scrape_application.workflows.site_handlers import get_site_handler

url = "https://api.greenhouse.io/v1/boards/airbnb/jobs"
handler = get_site_handler(url)
print(f"Handler: {handler.name if handler else None}")

# For JSON API handlers
if hasattr(handler, 'get_links_from_json'):
    sample_json = {"jobs": [{"absolute_url": "https://..."}]}
    links = handler.get_links_from_json(sample_json)
    print(f"JSON extraction: {links}")

# For HTML handlers
if hasattr(handler, 'get_links_from_raw_html'):
    sample_html = '<a href="/jobs/123">Job</a>'
    links = handler.get_links_from_raw_html(sample_html, url)
    print(f"HTML extraction: {links}")
PYEOF
```

### Step 4: Debug URL Filtering

```bash
uv run python << 'PYEOF'
from job_scrape_application.workflows.activities.url_processing import (
    _filter_job_urls,
    _looks_like_job_detail_url,
    _is_base_listing_page,
)

sample_urls = [
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/123",
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs",  # listing page
    "https://example.com/auth/login",  # auth page
]

for url in sample_urls:
    is_detail = _looks_like_job_detail_url(url)
    is_listing = _is_base_listing_page(url)
    print(f"{url}")
    print(f"  looks_like_detail: {is_detail}")
    print(f"  is_base_listing: {is_listing}")
PYEOF
```

### Common Listing Extraction Issues

#### No URLs Extracted

**Symptoms:** Zero URLs in extraction output

**Causes:**
- Page content not rendered (JavaScript-heavy sites)
- Wrong handler detected
- Handler missing `get_links_from_json()` or `get_links_from_raw_html()`

**Fix:**
1. Check raw content in `_listing_extraction_steps.md`
2. Verify handler detection: `get_site_handler(url)`
3. Add/update URL extraction methods in handler

#### Wrong URLs Extracted

**Symptoms:** URLs are listing pages, not detail pages

**Causes:**
- URL filtering not excluding listing URLs
- Handler extracting wrong href attributes

**Fix:**
1. Update `_is_base_listing_page()` to recognize listing URL patterns
2. Add URL pattern to handler's filtering logic

#### Missing Pagination

**Symptoms:** Only first page of jobs extracted

**Causes:**
- Handler not detecting pagination links
- Site uses infinite scroll (requires different approach)

**Fix:**
1. Implement `get_pagination_urls()` in handler
2. Check `_extract_listing_pagination()` logic

### Test Files Reference

| File | Purpose |
|------|---------|
| `test_listing_extraction_e2e.py` | E2E tests for listing extraction |
| `test_spidercloud_listing_and_detail.py` | Comprehensive listing/detail tests |
| `test_spidercloud_*_listing_*.py` | Site-specific listing tests |

### Key Functions

| Function | Location |
|----------|----------|
| `process_spidercloud_listing_batch()` | `activities/__init__.py:1831` |
| `_extract_listing_job_urls()` | `scrapers/spidercloud_scraper.py:2395` |
| `_extract_listing_links_from_html()` | `scrapers/spidercloud_scraper.py:2464` |
| `_filter_job_urls()` | `activities/url_processing.py` |

---

## Generating Fixtures

### Primary Script: fetch_spidercloud_fixtures.py

Uses the `CapturingSpiderClient` from the core module to capture real SpiderCloud responses.

```bash
# Regenerate fixtures for a specific site
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-only pinterest

# Regenerate fixtures for multiple sites
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-only pinterest purestorage stripe

# Regenerate all fixtures from prod schedule
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod

# Limit number of sites (useful for testing)
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-limit 5
```

### How the Script Works

1. **Reads site config** from `job_scrape_application/config/{env}/site_schedules.yml`
2. **Creates CapturingSpiderClient** wrapper around real SpiderCloud client
3. **Fetches listing page** via actual workflow scraper code
4. **Extracts job URLs** using production `_extract_job_urls_from_scrape()` logic
5. **Selects detail URL** - picks first non-listing URL from extracted jobs
6. **Converts to API URL** - for Greenhouse sites, converts marketing URLs to API URLs
7. **Fetches detail page** and writes captured request/response as fixture

### Debug Script: dump_spidercloud_response.py

For fetching individual pages quickly:

```bash
# Raw response output (default)
uv run python agent_scripts/dump_spidercloud_response.py \
  "https://example.com/jobs/123" \
  --out /tmp/response.json \
  --use-handler-config

# Fixture format output (for tests)
uv run python agent_scripts/dump_spidercloud_response.py \
  "https://example.com/jobs/123" \
  --out tests/.../fixtures/debug/example_123_detail.json \
  --use-handler-config \
  --fixture-format
```

**New `--fixture-format` flag:** Uses `CapturingSpiderClient` to output in the fixture format expected by tests:

```json
{
  "request": {
    "url": "https://...",
    "params": {...},
    "stream": false,
    "contentType": "application/json"
  },
  "response": [...]
}
```

### Fixture Format

Fixtures are stored in `tests/job_scrape_application/workflows/fixtures/`:
- `dbos_schedule/{site}_listing.json` - Listing page response
- `dbos_schedule/{site}_detail.json` - Job detail page response
- `debug/{site}_{job_id}_detail.json` - Debug fixtures for specific jobs

---

## Debug Fixtures Workflow

For debugging specific production jobs that aren't extracting correctly.

### Directory Structure

```
tests/job_scrape_application/workflows/
├── fixtures/debug/
│   ├── README.md
│   ├── {company}/
│   │   ├── {handler}_{short_id}_{timestamp}_detail.json
│   │   └── {handler}_{timestamp}_listing.json
└── assertions/debug/
    ├── {company}/
    │   ├── {handler}_{short_id}_{timestamp}.yml
    │   └── {handler}_{timestamp}_listing.yml
```

### Naming Convention

All debug fixtures use **1:1 mapping** between fixture and assertion files with timestamps for uniqueness:

| Component | Description | Example |
|-----------|-------------|---------|
| `{company}` | Normalized company folder | `airbnb`, `netflix`, `purestorage` |
| `{handler}` | Site handler type | `greenhouse`, `ashbyhq`, `workday` |
| `{short_id}` | Last 8 chars of job ID | `abc12345` |
| `{timestamp}` | ISO timestamp (YYYYMMDDTHHMMSS) | `20260114T153022` |

**Examples:**
- Fixture: `airbnb/greenhouse_abc12345_20260114T153022_detail.json`
- Assertion: `airbnb/greenhouse_abc12345_20260114T153022.yml`

The timestamp ensures:
1. Multiple fixtures for the same job can coexist
2. Each fixture has exactly one matching assertion file
3. Fixtures are traceable to when they were captured

### Step-by-Step Workflow

#### 1. Create Fixture

```bash
uv run python agent_scripts/dump_spidercloud_response.py \
  "https://explore.jobs.netflix.net/careers/job/790313551266" \
  --out tests/job_scrape_application/workflows/fixtures/debug/netflix_790313551266_detail.json \
  --use-handler-config \
  --fixture-format
```

#### 2. Create Assertion File

Create `tests/job_scrape_application/workflows/assertions/debug/netflix_790313551266.yml`:

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
  description_not_contains: '{"domain":'
  cost_milli_cents_min: 1
  posted_at_not_null: true
```

#### 3. Run Debug Test

```bash
# Run all debug tests
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v

# Run specific debug test
uv run pytest "tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[netflix_790313551266]" -v
```

#### 4. Check Extraction Output

```bash
cat ./site-detail-e2e-examples/netflix_extraction.json | jq '.extracted_jobs[0]'
```

#### 5. Fix Handler

Modify the site handler in `job_scrape_application/workflows/site_handlers/` to fix extraction.

#### 6. Verify Fix

```bash
# Re-run debug test
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v

# Ensure main tests still pass
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py -v
```

---

## Testing with WorkflowTestHelper

The `WorkflowTestHelper` class simplifies test setup by managing all mocks internally.

### Basic Usage

```python
from job_scrape_application.workflows.core import SpiderFixture, WorkflowTestHelper
from pathlib import Path

async def test_extraction(tmp_path, monkeypatch):
    # Load fixture
    fixture = SpiderFixture.from_file(Path("fixtures/site_detail.json"))

    # Create helper
    helper = WorkflowTestHelper(
        fixtures={fixture.url: fixture},
        monkeypatch=monkeypatch,
        tmp_path=tmp_path,
        site_id="test-site",
    )
    await helper.setup()

    # Run workflow/activity code
    result = await process_spidercloud_job_batch(...)

    # Check captured data
    assert len(helper.captured.ingested_jobs) > 0
    assert helper.captured.ingested_jobs[0]["title"] == "Expected Title"
```

### What WorkflowTestHelper Provides

- **Environment setup**: DBOS SQLite path, API keys
- **Mock SpiderCloud client**: Returns fixture data as streaming JSONL
- **Mock Convex client**: Captures queries and mutations
- **Captured data access**:
  - `helper.captured.queries` - All Convex queries
  - `helper.captured.mutations` - All Convex mutations
  - `helper.captured.ingested_jobs` - Jobs sent to ingestJobsFromScrape
  - `helper.captured.stored_scrapes` - Scrapes stored
  - `helper.captured.description_uploads` - Description uploads

### Custom Query/Mutation Responses

```python
helper = WorkflowTestHelper(
    fixtures={...},
    monkeypatch=monkeypatch,
    tmp_path=tmp_path,
    query_responses={
        "router:getSiteById": {"paginationLimit": 5},
        "router:listJobDetailConfigs": lambda args: [...],
    },
    mutation_responses={
        "router:ingestJobsFromScrape": {"success": True},
    },
)
```

---

## Adding/Updating Assertions

### Assertion File Format

```yaml
site_id: purestorage
detail_url: https://boards-api.greenhouse.io/v1/boards/purestorage/jobs/7472241
expected:
  title: AI/HPC Pre-Sales Systems Engineer
  title_contains: Engineer           # Alternative: partial match
  company: Pure Storage
  location_contains: Atlanta         # Partial location match
  is_remote: false
  level: senior
  description_min_words: 50
  description_contains: storage      # Check description content
  description_not_contains: '{"'     # Ensure no JSON in description
  cost_milli_cents_min: 1
  posted_at_not_null: true
```

### Supported Assertion Fields

| Field | Description |
|-------|-------------|
| `title` | Exact title match |
| `title_contains` | Partial title match (case-insensitive) |
| `company` | Exact company match |
| `company_contains` | Partial company match |
| `location` | Exact location match |
| `location_contains` | Partial location match |
| `is_remote` | Boolean remote status |
| `level` | Exact level: junior/mid/senior/staff |
| `description_min_words` | Minimum word count |
| `description_contains` | Content that should be present |
| `description_not_contains` | Content that should NOT be present |
| `cost_milli_cents_min` | Minimum SpiderCloud cost |
| `cost_milli_cents_max` | Maximum SpiderCloud cost |
| `posted_at_not_null` | Verify posted date exists |
| `url_contains` | Partial URL match |

### Regenerate Assertions from Extraction Output

```bash
# After running tests, generate assertions from actual extraction
uv run python << 'PYEOF'
import json
import yaml
from pathlib import Path

extraction_file = Path('./site-detail-e2e-examples/SITE_extraction.json')
with open(extraction_file) as f:
    data = json.load(f)

job = data['extracted_jobs'][0]
assertion = {
    'site_id': 'SITE',
    'detail_url': data['detail_url'],
    'expected': {
        'title': job.get('title'),
        'company': job.get('company'),
        'location_contains': job.get('location', '')[:20] if job.get('location') else None,
        'is_remote': job.get('is_remote'),
        'level': job.get('level'),
        'description_min_words': 50,
        'cost_milli_cents_min': 1,
        'posted_at_not_null': job.get('posted_at') is not None,
    }
}
assertion['expected'] = {k: v for k, v in assertion['expected'].items() if v is not None}

print(yaml.dump(assertion, default_flow_style=False, allow_unicode=True))
PYEOF
```

---

## Validating Against Convex Production

### Get Job Details from Convex Prod

```bash
cd job_board_application

# Get a specific job by ID
npx convex run --prod router:getJobById '{"id":"JOB_ID"}'

# Search for jobs by company
npx convex run --prod router:searchJobs '{"query":"COMPANY_NAME","limit":10}'
```

### Compare Extraction vs Production

```bash
# Run extraction test
uv run pytest "tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[purestorage]" -v

# Check extraction result
cat ./site-detail-e2e-examples/purestorage_extraction.json | jq '.extracted_jobs[0] | {title, company, location}'

# Compare with prod (from job_board_application/)
npx convex run --prod router:getJobById '{"id":"..."}'
```

---

## Common Issues and Fixes

### Issue: Title shows as "Untitled"

**Cause:** Handler's `normalize_markdown()` can't parse the job content.

**Debug:**
```bash
uv run python -c "
import json
from job_scrape_application.workflows.site_handlers import get_site_handler

with open('tests/.../SITE_detail.json') as f:
    fixture = json.load(f)

url = fixture['request']['url']
handler = get_site_handler(url)
resp = json.loads(fixture['response'][0])
md = resp['content']['commonmark']

print(f'Handler: {handler.name if handler else None}')
print(f'Starts with code fence: {md.strip().startswith(\"\`\`\`\")}')
print(f'Content preview: {md[:300]}')
"
```

**Fix:** Update handler's `normalize_markdown()` to handle the content format.

### Issue: Handler not detected

**Cause:** URL pattern doesn't match any handler.

**Debug:**
```bash
uv run python -c "
from job_scrape_application.workflows.site_handlers import get_site_handler
print(get_site_handler('YOUR_URL'))
"
```

**Fix:** Check URL patterns in the handler's `_URL_PATTERNS` or `is_*_url()` methods.

### Issue: Description contains JSON blocks

**Cause:** Raw JSON from API not being cleaned.

**Fix:** Implement `normalize_markdown()` in the handler to strip JSON blocks:

```python
def normalize_markdown(self, markdown: str) -> tuple[str | None, str | None]:
    # Strip JSON code blocks
    cleaned = re.sub(r'```json\n.*?\n```', '', markdown, flags=re.DOTALL)
    # Extract title and return cleaned content
    return cleaned, extracted_title
```

### Issue: Wrong fixture format

**Cause:** Fixture missing `request`/`response` structure.

**Fix:** Use `--fixture-format` flag when generating fixtures:
```bash
uv run python agent_scripts/dump_spidercloud_response.py URL --out FILE --fixture-format
```

---

## Output Locations

| Type | Location |
|------|----------|
| **Job Detail Extraction** | |
| Detail extraction results | `./site-detail-e2e-examples/{site}_extraction.json` |
| Detail extraction steps | `./site-detail-e2e-examples/{site}_extraction_steps.md` |
| Detail fixtures (schedule) | `tests/.../fixtures/dbos_schedule/{site}_detail.json` |
| Detail fixtures (debug) | `tests/.../fixtures/debug/{company}/{handler}_{id}_{timestamp}_detail.json` |
| Detail assertions | `tests/.../assertions/{site}.yml` |
| Detail assertions (debug) | `tests/.../assertions/debug/{company}/{handler}_{id}_{timestamp}.yml` |
| **Listing Page Extraction** | |
| Listing extraction results | `./site-detail-e2e-examples/{handler}_listing_extraction.json` |
| Listing extraction steps | `./site-detail-e2e-examples/{handler}_listing_extraction_steps.md` |
| Listing fixtures (schedule) | `tests/.../fixtures/dbos_schedule/{site}_listing.json` |
| Listing fixtures (debug) | `tests/.../fixtures/debug/{company}/{handler}_{timestamp}_listing.json` |
| Listing assertions (debug) | `tests/.../assertions/debug/{company}/{handler}_{timestamp}_listing.yml` |

> **Note:** `{timestamp}` uses ISO format `YYYYMMDDTHHMMSS` (e.g., `20260114T153022`)

---

## Summary: Fix a Production Job Issue

1. **Get fixture**: `dump_spidercloud_response.py URL --out debug/fixture.json --fixture-format --use-handler-config`
2. **Create assertion**: Write YAML with expected values
3. **Run test**: `pytest test_debug_fixtures.py -v`
4. **Check output**: `cat ./site-detail-e2e-examples/SITE_extraction.json`
5. **Fix handler**: Modify `normalize_markdown()` or other handler methods
6. **Verify**: Re-run tests, ensure main test suite still passes

## Summary: Fix a Listing/Crawl Issue

1. **Get fixture**: `mise run fix_job_crawl COMPANY` or use `generate_debug_listing_fixture.py`
2. **Create assertion**: Write YAML with url_count_min and url_pattern
3. **Run test**: `DEBUG_EXTRACTION_VERBOSE=1 pytest test_listing_extraction_e2e.py -v`
4. **Check output**: Review `{handler}_listing_extraction_steps.md` for raw content and URLs
5. **Fix handler**: Update `get_links_from_json()`, `get_links_from_raw_html()`, or URL filtering
6. **Verify**: Re-run tests, ensure main listing tests still pass
