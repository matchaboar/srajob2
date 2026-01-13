# DEBUGGING.md

Detailed debugging procedures for job scraping workflows and extraction issues.

## Table of Contents
- [Job Detail Extraction Issues](#job-detail-extraction-issues)
- [Generating Fixtures](#generating-fixtures)
- [Testing with WorkflowTestModule](#testing-with-workflowtestmodule)
- [Adding/Updating Assertions](#addingupdating-assertions)
- [Validating Against Convex Production](#validating-against-convex-production)
- [Common Issues and Fixes](#common-issues-and-fixes)

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
with open('tests/job_scrape_application/workflows/fixtures/dbos_schedule/SITE_NAME_detail.json') as f:
    fixture = json.load(f)
print(f"Response items: {len(fixture.get('response', []))}")
# Parse first response item
resp = json.loads(fixture['response'][0])
print(f"Content keys: {list(resp.get('content', {}).keys())}")
print(f"Commonmark preview: {resp.get('content', {}).get('commonmark', '')[:500]}")
PYEOF
```

### Step 3: Test Handler Normalization

For Greenhouse sites with JSON-in-markdown issues:

```bash
uv run python << 'PYEOF'
import json
from job_scrape_application.workflows.site_handlers.greenhouse import GreenhouseHandler

with open('tests/job_scrape_application/workflows/fixtures/dbos_schedule/SITE_NAME_detail.json') as f:
    fixture = json.load(f)

response = json.loads(fixture['response'][0])
commonmark = response['content']['commonmark']

handler = GreenhouseHandler()
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

## Generating Fixtures

### Regenerate Fixtures with fetch_spidercloud_fixtures.py

The primary script for fixture generation is `agent_scripts/fetch_spidercloud_fixtures.py`. It uses production workflow code to ensure fixtures match real scraping behavior.

```bash
# Regenerate fixtures for a specific site
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-only pinterest

# Regenerate fixtures for multiple sites
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-only pinterest purestorage stripe

# Regenerate all fixtures from prod schedule
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod

# Regenerate from dev schedule
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env dev

# Limit number of sites (useful for testing)
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-limit 5
```

### How the Script Works

1. **Reads site config** from `job_scrape_application/config/{env}/site_schedules.yml`
2. **Fetches listing page** via SpiderCloud using actual workflow scraper code
3. **Extracts job URLs** using production `_extract_job_urls_from_scrape()` logic
4. **Selects detail URL** - picks first non-listing URL from extracted jobs
5. **Converts to API URL** - for Greenhouse sites, converts marketing URLs to API URLs using `handler.get_api_uri(detail_url, source_url=listing_url)`
6. **Fetches detail page** and writes fixtures

**Important:** The script passes `source_url` when converting URLs, ensuring correct board slug extraction (e.g., `pinterest` not `pinterestcareers`).

### Legacy/Debug Scripts

```bash
# Dump raw SpiderCloud response for debugging
uv run agent_scripts/dump_spidercloud_response.py --url "https://..."
```

### Fixture Format

Fixtures are stored in `tests/job_scrape_application/workflows/fixtures/dbos_schedule/`:
- `{site}_listing.json` - Listing page response
- `{site}_detail.json` - Job detail page response

Each fixture contains:
```json
{
  "request": {
    "url": "https://boards-api.greenhouse.io/v1/boards/COMPANY/jobs/JOB_ID",
    "params": {
      "return_format": ["commonmark", "raw_html"],
      "metadata": true,
      ...
    },
    "stream": true,
    "contentType": "application/jsonl"
  },
  "response": [
    // JSONL lines from SpiderCloud (may be fragmented across multiple items)
    "{\"content\":{\"commonmark\":\"...\",\"raw\":\"...\"},\"costs\":{...}}"
  ]
}
```

**Note:** Response arrays may contain fragmented JSONL lines (split during streaming). The test mock concatenates these before processing.

---

## Testing with WorkflowTestModule

The `WorkflowTestModule` in `test_job_detail_extraction_e2e.py` simulates the full production workflow without network calls.

### How It Works

1. **Mock SpiderCloud Client**: `_FixtureAsyncSpider` returns fixture data as JSONL stream
2. **Real Scraper Code**: Actual `SpiderCloudScraper._scrape_single_url()` processes the response
3. **Real Normalization**: Handler's `normalize_markdown()` extracts job fields
4. **Captured Storage**: Mock Convex mutations capture what would be stored

### Key Code Paths Tested

```
Fixture → _FixtureAsyncSpider.scrape_url()
       → SpiderCloudScraper._consume_chunk()
       → SpiderCloudScraper._extract_markdown()
       → SiteHandler.normalize_markdown()
       → SpiderCloudScraper._normalize_job()
       → Captured in stored_scrapes
```

### Running Isolated Tests

```bash
# Test a single site
uv run pytest "tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[purestorage]" -v

# View extraction output
cat ./site-detail-e2e-examples/purestorage_extraction.json
```

### Adding/Updating Assertions

Assertion files validate extracted job data. After regenerating fixtures, you may need to update assertions.

#### Step-by-Step Assertion Update

```bash
# 1. Regenerate the fixture
uv run python agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-only SITE_NAME

# 2. Run the test to generate extraction output
uv run pytest "tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[SITE_NAME]" -v

# 3. View the extraction result
cat ./site-detail-e2e-examples/SITE_NAME_extraction.json

# 4. Get the detail URL from the fixture
head -5 tests/job_scrape_application/workflows/fixtures/dbos_schedule/SITE_NAME_detail.json
```

#### Create/Edit Assertion File

Create/edit `tests/job_scrape_application/workflows/assertions/{site}.yml`:

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
  cost_milli_cents_min: 1
  posted_at_not_null: true
```

**Critical:** The `detail_url` must match the `request.url` in the fixture file. If the fixture URL changed during regeneration, update the assertion file accordingly.

#### Supported Assertion Fields

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
| `description_contains` | Partial description match |
| `cost_milli_cents_min` | Minimum SpiderCloud cost |
| `cost_milli_cents_max` | Maximum SpiderCloud cost |
| `posted_at_not_null` | Verify posted date exists |
| `url_contains` | Partial URL match |

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

1. Run the extraction test and note the output
2. Query the same job from Convex prod
3. Compare fields: title, company, location, description word count

```bash
# Run extraction test
uv run pytest "tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[purestorage]" -v

# Check extraction result
cat ./site-detail-e2e-examples/purestorage_extraction.json | jq '.extracted_jobs[0] | {title, company, location, description_word_count}'

# Compare with prod (from job_board_application/)
npx convex run --prod router:getJobById '{"id":"..."}'
```

### Regenerate Assertions from Current Extraction

If extraction behavior changed and tests need updating:

```bash
uv run python << 'PYEOF'
import json
import yaml
from pathlib import Path

output_dir = Path('./site-detail-e2e-examples')
assertions_dir = Path('tests/job_scrape_application/workflows/assertions')

for extraction_file in output_dir.glob('*_extraction.json'):
    site_id = extraction_file.stem.replace('_extraction', '')
    with open(extraction_file) as f:
        data = json.load(f)

    jobs = data.get('extracted_jobs', [])
    if not jobs:
        continue

    job = jobs[0]
    assertion = {
        'site_id': site_id,
        'detail_url': data.get('detail_url'),
        'expected': {
            'title': job.get('title'),
            'company': job.get('company'),
            'location': job.get('location'),
            'is_remote': job.get('is_remote'),
            'level': job.get('level'),
            'description_min_words': 50 if job.get('description_word_count', 0) > 50 else 10,
            'cost_milli_cents_min': 1 if job.get('cost_milli_cents') else None,
            'posted_at_not_null': job.get('posted_at') is not None,
        }
    }
    assertion['expected'] = {k: v for k, v in assertion['expected'].items() if v is not None}

    with open(assertions_dir / f'{site_id}.yml', 'w') as f:
        yaml.dump(assertion, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f'Updated {site_id}.yml')
PYEOF
```

---

## Common Issues and Fixes

### Issue: Title shows as "Untitled"

**Cause:** Handler's `normalize_markdown()` can't parse the job content.

**Debug:**
```bash
# Check if markdown has code-fenced JSON (Greenhouse API)
uv run python -c "
import json
with open('tests/.../SITE_detail.json') as f:
    fixture = json.load(f)
resp = json.loads(fixture['response'][0])
md = resp['content']['commonmark']
print('Starts with code fence:', md.strip().startswith('\`\`\`'))
print('Content preview:', md[:300])
"
```

**Fix:** If JSON is wrapped in markdown code fences with escaped characters (`\_`), ensure the handler unescapes before parsing. See `greenhouse.py:normalize_markdown()`.

### Issue: Response fragments not parsed

**Cause:** JSONL responses may be split across multiple response items during streaming.

**Debug:**
```bash
uv run python -c "
import json
with open('tests/.../SITE_detail.json') as f:
    fixture = json.load(f)
print(f'Response items: {len(fixture[\"response\"])}')
for i, item in enumerate(fixture['response']):
    print(f'Item {i}: {len(item)} chars, ends with: {repr(item[-20:])}')
"
```

**Fix:** The test mock concatenates all response items before yielding. This is handled in `_FixtureAsyncSpider.scrape_url()`.

### Issue: Handler not detected

**Cause:** URL pattern doesn't match any handler.

**Debug:**
```bash
uv run python -c "
from job_scrape_application.workflows.site_handlers import get_site_handler
print(get_site_handler('YOUR_URL'))
"
```

**Fix:** Check URL patterns in the relevant handler's `_URL_PATTERNS` or `is_*_url()` methods.

### Issue: Description extraction fails

**Cause:** HTML content not properly converted to markdown/text.

**Debug:**
```bash
# Check raw HTML content
uv run python -c "
import json
with open('tests/.../SITE_detail.json') as f:
    fixture = json.load(f)
resp = json.loads(fixture['response'][0])
raw = resp['content'].get('raw', '')[:1000]
print(raw)
"
```

**Fix:** Check `_html_to_text()` in the handler for encoding/decoding issues.

---

## Output Locations

- **Extraction results:** `./site-detail-e2e-examples/{site}_extraction.json`
- **Fixtures:** `tests/job_scrape_application/workflows/fixtures/dbos_schedule/{site}_detail.json`
- **Assertions:** `tests/job_scrape_application/workflows/assertions/{site}.yml`
