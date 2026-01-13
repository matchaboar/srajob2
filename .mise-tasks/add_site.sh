#!/usr/bin/env bash
# [MISE] description="Add a new site to the job scraper using Claude Code"
# [USAGE] arg "<url>" help="Job listing/detail URL from the site to add (e.g., https://careers.newcompany.com/jobs/12345)" default=""
# [USAGE] option "--listing-url <url>" help="Override the listing URL if auto-detection fails"
# [USAGE] option "--dry-run" help="Show what would be generated without fetching"

set -e

# Check if URL provided
if [ -z "${usage_url}" ]; then
  echo "Usage: mise run add_site <job_url> [--listing-url <listing_url>] [--dry-run]"
  echo ""
  echo "Example:"
  echo "  mise run add_site https://careers.newcompany.com/jobs/software-engineer-12345"
  echo "  mise run add_site https://jobs.newsite.com/engineer --listing-url https://jobs.newsite.com/careers"
  echo ""
  echo "This script will:"
  echo "  1. Analyze the URL to identify the company/site"
  echo "  2. Generate a SpiderCloud fixture for the job detail page"
  echo "  3. Create a placeholder assertion file"
  echo "  4. Launch Claude Code to:"
  echo "     - Fill in the assertion file with correct values"
  echo "     - Add the site to dev and prod site_schedules.yaml"
  echo "     - Run tests and fix any issues with the base handler"
  echo "     - Spot check for regressions on existing fixtures"
  echo ""
  echo "Notes:"
  echo "  - New sites use 'type: general' (default site handler)"
  echo "  - Claude will focus on base.py updates if handler changes are needed"
  exit 1
fi

# Build Python script arguments
PYTHON_ARGS="${usage_url}"
if [ -n "${usage_listing_url}" ]; then
  PYTHON_ARGS="${PYTHON_ARGS} --listing-url ${usage_listing_url}"
fi
if [ "${usage_dry_run}" = "true" ]; then
  PYTHON_ARGS="${PYTHON_ARGS} --dry-run"
fi

# Create temp file for script output
TEMP_OUTPUT=$(mktemp /tmp/new_site_gen_XXXXXX.json)
trap "rm -f ${TEMP_OUTPUT}" EXIT

# Run the fixture generation script
echo "Running new site fixture generation..."
PYTHONPATH=. uv run python agent_scripts/generate_new_site_fixture.py ${PYTHON_ARGS} 2>&1 | tee "${TEMP_OUTPUT}"

# Extract JSON output from the script
JSON_OUTPUT=$(grep -A 100 "=== JSON Output ===" "${TEMP_OUTPUT}" | tail -n +2 | head -30)

if [ -z "${JSON_OUTPUT}" ]; then
  echo "Error: Failed to parse script output"
  exit 1
fi

# Parse JSON fields (requires jq)
FIXTURE_PATH=$(echo "${JSON_OUTPUT}" | jq -r '.fixture_path // empty')
ASSERTION_PATH=$(echo "${JSON_OUTPUT}" | jq -r '.assertion_path // empty')
IDENTIFIER=$(echo "${JSON_OUTPUT}" | jq -r '.identifier // empty')
JOB_URL=$(echo "${JSON_OUTPUT}" | jq -r '.job_url // empty')
LISTING_URL=$(echo "${JSON_OUTPUT}" | jq -r '.listing_url // empty')
COMPANY=$(echo "${JSON_OUTPUT}" | jq -r '.company // empty')
NORMALIZED_COMPANY=$(echo "${JSON_OUTPUT}" | jq -r '.normalized_company // empty')
HANDLER=$(echo "${JSON_OUTPUT}" | jq -r '.handler // empty')
IS_KNOWN_PLATFORM=$(echo "${JSON_OUTPUT}" | jq -r '.is_known_platform // false')
SCHEDULE_ENTRY=$(echo "${JSON_OUTPUT}" | jq -r '.schedule_entry // empty')

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
echo "Company:    ${COMPANY}"
echo "Handler:    ${HANDLER}"
echo ""

# Build known platform note
PLATFORM_NOTE=""
if [ "${IS_KNOWN_PLATFORM}" = "true" ]; then
  PLATFORM_NOTE="
## Known Platform Detected
This URL appears to be from a known job board platform (${HANDLER}). There's likely an existing handler that should work.
Check if the site just needs to be added to site_schedules.yaml with the existing handler type.
"
fi

# Get sample assertions for regression testing
SAMPLE_ASSERTIONS=$(ls tests/job_scrape_application/workflows/assertions/*.yml 2>/dev/null | shuf | head -5 | tr '\n' ' ')

PROMPT="I need help adding a new site to the job scraper.

## New Site Info
- **Company**: ${COMPANY}
- **Job Detail URL**: ${JOB_URL}
- **Suggested Listing URL**: ${LISTING_URL}
- **Handler Type**: ${HANDLER}
${PLATFORM_NOTE}
## Files Generated:
- **Fixture**: ${FIXTURE_PATH}
- **Assertions**: ${ASSERTION_PATH}
- **Test identifier**: ${IDENTIFIER}

## Suggested Site Schedule Entry
\`\`\`yaml
${SCHEDULE_ENTRY}
\`\`\`

## Your Tasks:

### 1. Review the Fixture and Fill In Assertions
First, read the fixture to understand the job data:
\`\`\`bash
cat ${FIXTURE_PATH}
\`\`\`

Then update the assertion file with correct expected values:
\`\`\`bash
cat ${ASSERTION_PATH}
\`\`\`

Fill in all TODO fields:
- \`title\`: The actual job title from the fixture
- \`location_contains\`: A key part of the location (city name, state, etc.)
- \`is_remote\`: true/false based on the job
- \`level\`: junior/mid/senior/staff based on title

### 2. Run the Debug Test
\`\`\`bash
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[${IDENTIFIER}] -v
\`\`\`

### 3. Check Extraction Output
\`\`\`bash
cat ./site-detail-e2e-examples/${HANDLER}_extraction.json
\`\`\`

Look for:
- Word count (should be > 300 for most job descriptions)
- JSON blocks in description (should NOT appear)
- Correct location parsing
- Correct title extraction
- posted_at should not be null

### 4. Fix Base Handler Issues (if test fails)

**IMPORTANT**: New sites should use \`type: general\` and rely on the base handler.
If extraction fails, focus on updating the base handler:

\`\`\`bash
# Main handler to update:
cat job_scrape_application/workflows/site_handlers/base.py
\`\`\`

Common issues to fix in base.py:
- \`normalize_markdown()\` not cleaning up site-specific HTML/JSON properly
- \`extract_posted_at()\` not finding dates in site-specific format
- \`extract_location_hint()\` not extracting location from site structure

### 5. Spot Check for Regressions
After any base handler changes, run a sample of existing tests:
\`\`\`bash
# Run 5 random existing site tests to check for regressions
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py -v --tb=short -k \"${SAMPLE_ASSERTIONS// / or }\"

# Or run all extraction tests
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py -v --tb=short
\`\`\`

### 6. Add Site to Schedules (after tests pass)
Once extraction works correctly:

1. Add to **dev** schedule:
\`\`\`bash
# Edit: job_scrape_application/config/dev/site_schedules.yml
# Add the schedule entry at the appropriate location (alphabetically or by type)
\`\`\`

2. Add to **prod** schedule:
\`\`\`bash
# Edit: job_scrape_application/config/prod/site_schedules.yml
# Add the same entry
\`\`\`

**Schedule Entry to Add:**
\`\`\`yaml
${SCHEDULE_ENTRY}
\`\`\`

Note: You may need to adjust:
- The \`url\` if auto-detection was wrong
- The \`paginationLimit\` if the site has many pages
- Add a \`pattern\` field if job URLs don't match standard patterns

### 7. Final Verification
\`\`\`bash
# Run the new site test one more time
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[${IDENTIFIER}] -v

# Run all e2e tests to ensure nothing is broken
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py -v --tb=short
\`\`\`

## Reference Files
- Base handler: \`job_scrape_application/workflows/site_handlers/base.py\`
- Handler registry: \`job_scrape_application/workflows/site_handlers/__init__.py\`
- Dev schedules: \`job_scrape_application/config/dev/site_schedules.yml\`
- Prod schedules: \`job_scrape_application/config/prod/site_schedules.yml\`
- Scraper logic: \`job_scrape_application/workflows/scrapers/spidercloud_scraper.py\`

## Key Principles
1. **Use \`type: general\`** for new sites - don't create new handlers unless absolutely necessary
2. **Fix base.py** if extraction fails - improvements benefit all general sites
3. **Spot check regressions** after any base handler changes
4. **Test thoroughly** before adding to schedules"

# Launch Claude Code
echo "Launching Claude Code..."
echo ""
claude "${PROMPT}"
