#!/usr/bin/env bash
# [MISE] description="Fix job listing/crawl issues using Claude Code with automated fixture generation"
# [USAGE] arg "<company_or_url>" help="Company name (e.g., airbnb) or listing URL to debug" default=""

set -e

# Check if company/url provided
if [ -z "${usage_company_or_url}" ]; then
  echo "Usage: mise run fix_job_crawl <company_or_url>"
  echo ""
  echo "Examples:"
  echo "  mise run fix_job_crawl airbnb"
  echo "  mise run fix_job_crawl purestorage"
  echo "  mise run fix_job_crawl https://api.greenhouse.io/v1/boards/airbnb/jobs"
  echo ""
  echo "This script will:"
  echo "  1. Fetch the listing page from SpiderCloud"
  echo "  2. Save as a debug fixture"
  echo "  3. Create assertion file with expected values"
  echo "  4. Launch Claude Code to verify extraction and fix any issues"
  echo ""
  echo "Features:"
  echo "  - Per-company folder organization (fixtures/debug/{company}/)"
  echo "  - Date-based filenames to preserve history"
  echo "  - Step-by-step extraction logging"
  echo "  - URL pattern validation"
  exit 1
fi

# Determine if input is URL or company name
INPUT="${usage_company_or_url}"
PYTHON_ARGS=""

if [[ "${INPUT}" == http* ]]; then
  # It's a URL
  PYTHON_ARGS="--url ${INPUT}"
else
  # It's a company name
  PYTHON_ARGS="--company ${INPUT}"
fi

# Create temp file for script output
TEMP_OUTPUT=$(mktemp /tmp/listing_fixture_gen_XXXXXX.json)
trap "rm -f ${TEMP_OUTPUT}" EXIT

# Run the fixture generation script
echo "Running listing fixture generation..."
PYTHONPATH=. uv run python agent_scripts/generate_debug_listing_fixture.py ${PYTHON_ARGS} 2>&1 | tee "${TEMP_OUTPUT}"

# Extract JSON output from the script
JSON_OUTPUT=$(grep -A 100 "=== JSON Output ===" "${TEMP_OUTPUT}" | tail -n +2 | head -20)

if [ -z "${JSON_OUTPUT}" ]; then
  echo "Error: Failed to parse script output"
  exit 1
fi

# Parse JSON fields (requires jq)
FIXTURE_PATH=$(echo "${JSON_OUTPUT}" | jq -r '.fixture_path // empty')
ASSERTION_PATH=$(echo "${JSON_OUTPUT}" | jq -r '.assertion_path // empty')
IDENTIFIER=$(echo "${JSON_OUTPUT}" | jq -r '.identifier // empty')
LISTING_URL=$(echo "${JSON_OUTPUT}" | jq -r '.listing_url // empty')
COMPANY=$(echo "${JSON_OUTPUT}" | jq -r '.company // empty')
HANDLER=$(echo "${JSON_OUTPUT}" | jq -r '.handler // empty')
EXTRACTED_URL_COUNT=$(echo "${JSON_OUTPUT}" | jq -r '.extracted_url_count // "0"')
EXTRACTED_URLS=$(echo "${JSON_OUTPUT}" | jq -r '.extracted_urls // []')

if [ -z "${IDENTIFIER}" ]; then
  echo "Error: Failed to extract identifier from script output"
  exit 1
fi

# If dry-run, exit here
if [ "${usage_dry_run}" = "true" ]; then
  echo ""
  echo "Dry run complete. Re-run without --dry-run to generate files."
  exit 0
fi

echo ""
echo "=== Files Ready ==="
echo "Fixture:    ${FIXTURE_PATH}"
echo "Assertions: ${ASSERTION_PATH}"
echo "Identifier: ${IDENTIFIER}"
echo ""

# Build the Claude Code prompt
# Format extracted URLs as a numbered list for Claude
EXTRACTED_URLS_LIST=$(echo "${EXTRACTED_URLS}" | jq -r 'to_entries | .[] | "  \(.key + 1). \(.value)"')

PROMPT="I need help debugging listing page extraction for this company.

## Company: ${COMPANY}
## Handler: ${HANDLER}

## Files Generated:
- **Fixture**: ${FIXTURE_PATH}
- **Assertions**: ${ASSERTION_PATH}
- **Test identifier**: ${IDENTIFIER}

## Extracted URLs (${EXTRACTED_URL_COUNT} total)
The following URLs were extracted from the fixture using the ${HANDLER} handler:

${EXTRACTED_URLS_LIST}

## Your Tasks:

### 1. CRITICAL: Validate Each Extracted URL
**You MUST validate EVERY URL in the list above.** For each URL, determine:
- Is it a valid job detail URL? (Points to a single job posting)
- Is it NOT a listing/search URL? (Should not be a page that lists multiple jobs)
- Is the URL format correct? (No corruption like \`/_JR\`, proper encoding)

After validation, update the assertion file with \`expected_urls\` containing ONLY valid URLs:
\`\`\`yaml
expected:
  # ... other assertions ...
  expected_urls:
    - \"https://valid-job-url-1\"
    - \"https://valid-job-url-2\"
    # etc - only include VALID job detail URLs
\`\`\`

**Why this matters**: If \`expected_urls\` is set, any future extraction that produces a URL NOT in this list will FAIL the test. This prevents regressions where invalid URLs sneak in.

### 2. Review Assertions
The assertion file was auto-generated with placeholder values. Update it with correct expectations:
\`\`\`bash
cat ${ASSERTION_PATH}
\`\`\`

Fill in:
- \`url_count_min\`: Minimum expected job URLs from this listing
- \`url_pattern\`: Regex pattern matching valid job detail URLs
- \`handler\`: Verify the correct handler is detected
- \`expected_urls\`: **Uncomment and fill in with VALID URLs from the list above**

### 3. Run the Debug Test (with verbose extraction steps)
\`\`\`bash
DEBUG_EXTRACTION_VERBOSE=1 uv run pytest 'tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction[${IDENTIFIER}]' -v
\`\`\`

### 4. Check Extraction Output
\`\`\`bash
# Summary extraction result
cat ./site-detail-e2e-examples/${HANDLER}_listing_extraction.json

# IMPORTANT: Detailed step-by-step extraction log (created when DEBUG_EXTRACTION_VERBOSE=1)
cat ./site-detail-e2e-examples/${HANDLER}_listing_extraction_steps.md
\`\`\`

The \`_listing_extraction_steps.md\` file shows the full extraction pipeline:
1. **Raw SpiderCloud response** - The HTML/markdown content scraped from the listing page
2. **Handler detection** - Which handler was used for this URL
3. **URL extraction method** - JSON API, HTML links, or regex fallback
4. **Extracted URLs** - All URLs found in the listing page
5. **URL filtering** - What was kept vs rejected (and why)
6. **Pagination detection** - Any pagination URLs found
7. **Queue enqueue summary** - URLs to be queued for detail extraction

Look for:
- URL count (should match expected for the site)
- URL patterns (should be job detail URLs, not listing/search pages)
- No auth URLs or other noise
- Correct handler detection

### 5. Fix Issues (if test fails)

#### If no URLs extracted:
- Check raw content in steps.md - is the page rendered properly?
- Check handler's \`get_links_from_json()\` or \`get_links_from_raw_html()\`
- May need to add URL extraction logic for this page format

#### If wrong URLs extracted:
- Check URL filtering in the handler
- May need to update \`_is_probable_listing_url()\` detection
- Check if URLs need canonicalization

#### If invalid URLs in expected_urls:
- If test fails because an unexpected URL was extracted, check if the handler is extracting noise URLs
- Update the handler's \`filter_job_urls()\` to filter out invalid patterns
- Update \`expected_urls\` in assertions if the new URL is actually valid

#### If wrong handler detected:
- Check \`site_handlers/__init__.py\` URL pattern matching
- May need to add/update handler registration

### 6. Verify Fix
\`\`\`bash
# Re-run debug test with verbose output
DEBUG_EXTRACTION_VERBOSE=1 uv run pytest 'tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction[${IDENTIFIER}]' -v

# Review the updated extraction steps
cat ./site-detail-e2e-examples/${HANDLER}_listing_extraction_steps.md

# Ensure main listing tests still pass
uv run pytest tests/job_scrape_application/workflows/test_spidercloud_listing_and_detail.py -v --tb=short
\`\`\`

## Reference Files
- Site handlers: \`job_scrape_application/workflows/site_handlers/\`
- URL processing: \`job_scrape_application/workflows/activities/url_processing.py\`
- Main extraction: \`job_scrape_application/workflows/scrapers/spidercloud_scraper.py\`
- Site schedules: \`job_scrape_application/config/prod/site_schedules.yml\`

## Listing Extraction Methods

The extraction process tries these methods in order:
1. **JSON API** - Parse structured JSON response (Greenhouse, AshbyHQ, Netflix APIs)
2. **HTML Links** - Extract hrefs from anchor tags with handler-specific selectors
3. **Regex Fallback** - Search for URL patterns in raw text

## Field Extraction Debugging (for job detail issues)

If you also need to debug field extraction (title, location, etc.) on the extracted jobs,
use the modular extractors at \`job_scrape_application/workflows/extractors/\`:
\`\`\`python
from job_scrape_application.workflows.extractors import ExtractionContext, extract_job_fields
ctx = ExtractionContext.from_scrape_result(url=job_url, markdown=content, debug=True)
results = extract_job_fields(ctx, run_all=True)
# Shows all strategies tried for each field
\`\`\`
See DEBUGGING.md section 'Modular Extractor Debug Output' for full documentation.

## Debug Folder Structure
Fixtures are organized per-company with date-based naming:
\`\`\`
tests/job_scrape_application/workflows/
├── fixtures/debug/
│   └── {company}/
│       └── {handler}_{date}_listing.json
└── assertions/debug/
    └── {company}/
        └── {handler}_{date}_listing.yml
\`\`\`"

# Launch Claude Code
echo "Launching Claude Code..."
echo ""
claude "${PROMPT}"
