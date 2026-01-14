#!/usr/bin/env bash
# [MISE] description="Regenerate timestamped fixtures for all sites in site_schedules.yml"
# [USAGE] flag "--env" help="Schedule environment (prod or dev)" default="prod"
# [USAGE] flag "--only" help="Comma-separated site names to include (e.g., airbnb,purestorage)"
# [USAGE] flag "--limit" help="Max number of sites to process"
# [USAGE] flag "--claude" help="Launch Claude to fill in assertions after generation" type="bool"

set -e

ENV="${usage_env:-prod}"
ONLY="${usage_only:-}"
LIMIT="${usage_limit:-}"
LAUNCH_CLAUDE="${usage_claude:-false}"

echo "=== Refresh All Fixtures ==="
echo "Environment: ${ENV}"
echo "Only: ${ONLY:-all sites}"
echo "Limit: ${LIMIT:-unlimited}"
echo ""

# Run the Python script to generate fixtures
PYTHON_ARGS="--schedule-env ${ENV}"

if [ -n "${ONLY}" ]; then
  # Convert comma-separated to space-separated for argparse nargs
  PYTHON_ARGS="${PYTHON_ARGS} --only $(echo ${ONLY} | tr ',' ' ')"
fi

if [ -n "${LIMIT}" ]; then
  PYTHON_ARGS="${PYTHON_ARGS} --limit ${LIMIT}"
fi

# Create temp file for script output
TEMP_OUTPUT=$(mktemp /tmp/refresh_fixtures_XXXXXX.json)
trap "rm -f ${TEMP_OUTPUT}" EXIT

echo "Running fixture generation..."
PYTHONPATH=. uv run python agent_scripts/refresh_all_site_fixtures.py ${PYTHON_ARGS} 2>&1 | tee "${TEMP_OUTPUT}"

# Extract JSON output from the script
JSON_OUTPUT=$(grep -A 1000 "=== JSON Output ===" "${TEMP_OUTPUT}" | tail -n +2)

if [ -z "${JSON_OUTPUT}" ]; then
  echo "Error: Failed to parse script output"
  exit 1
fi

# Parse generated files count
GENERATED_COUNT=$(echo "${JSON_OUTPUT}" | jq -r '.generated_count // 0')
echo ""
echo "Generated ${GENERATED_COUNT} fixture sets"

if [ "${LAUNCH_CLAUDE}" = "true" ] && [ "${GENERATED_COUNT}" -gt 0 ]; then
  # Build Claude prompt with all generated fixtures
  FIXTURES_LIST=$(echo "${JSON_OUTPUT}" | jq -r '.fixtures[] | "- \(.company): \(.assertion_listing_path) and \(.assertion_detail_path)"')

  PROMPT="I need help filling in assertions for ${GENERATED_COUNT} newly generated site fixtures.

## Generated Fixtures
${FIXTURES_LIST}

## Your Tasks

For EACH fixture set above:

### 1. Run the listing extraction test
\`\`\`bash
DEBUG_EXTRACTION_VERBOSE=1 uv run pytest 'tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction[IDENTIFIER]' -v
\`\`\`

### 2. Review extracted URLs
Check \`./site-detail-e2e-examples/{handler}_listing_extraction_steps.md\` for:
- Extracted URL count
- URL patterns (should be job detail URLs)
- No invalid/noise URLs

### 3. Update listing assertions
Edit the \`*_listing.yml\` file with:
- \`url_count_min\`: Minimum expected URLs
- \`url_pattern\`: Regex for valid job URLs
- \`expected_urls\`: List of ALL valid URLs (uncomment and validate each one)

### 4. Run the detail extraction test
\`\`\`bash
uv run pytest 'tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_debug_job_detail_extraction[IDENTIFIER]' -v
\`\`\`

### 5. Update detail assertions
Edit the \`*_detail.yml\` file with correct expected values:
- \`title\`: Job title
- \`company\`: Company name
- \`location\`: Location string
- \`is_remote\`: true/false
- \`level\`: junior/mid/senior/staff

### 6. Verify all tests pass
\`\`\`bash
uv run pytest tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction -v
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_debug_job_detail_extraction -v
\`\`\`

## Reference
- Listing assertions: \`tests/job_scrape_application/workflows/assertions/debug/{company}/*_listing.yml\`
- Detail assertions: \`tests/job_scrape_application/workflows/assertions/debug/{company}/*_detail.yml\`
"

  echo ""
  echo "Launching Claude Code..."
  claude "${PROMPT}"
fi
