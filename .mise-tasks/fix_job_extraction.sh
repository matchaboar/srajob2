#!/usr/bin/env bash
# [MISE] description="Fix job extraction issues using Claude Code with automated fixture generation"
# [USAGE] arg "<url>" help="Job share URL from Convex prod (e.g., https://srajob.netlify.app/job/abc123)" default=""

set -e

# Check if URL provided
if [ -z "${usage_url}" ]; then
  echo "Usage: mise run fix_job_extraction <job_share_url> [--url <detail_url>] [--dry-run]"
  echo ""
  echo "Example:"
  echo "  mise run fix_job_extraction https://srajob.netlify.app/job/k57abc123xyz"
  echo "  mise run fix_job_extraction k57abc123xyz --url https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/123"
  echo ""
  echo "This script will:"
  echo "  1. Extract the job ID from the URL"
  echo "  2. Fetch job details from Convex prod"
  echo "  3. Automatically fetch SpiderCloud fixture to per-company debug folder"
  echo "  4. Create assertion file with proper expected values"
  echo "  5. Launch Claude Code to verify assertions and fix any issues"
  echo ""
  echo "Features:"
  echo "  - Per-company folder organization (fixtures/debug/{company}/)"
  echo "  - Date-based filenames to preserve history"
  echo "  - Remote company override awareness"
  echo "  - URL canonicalization (marketing -> API URLs)"
  exit 1
fi

# Build Python script arguments
PYTHON_ARGS="${usage_url}"
if [ -n "${usage_url_2}" ]; then
  # --url option was provided
  PYTHON_ARGS="${PYTHON_ARGS} --url ${usage_url_2}"
fi
if [ "${usage_dry_run}" = "true" ]; then
  PYTHON_ARGS="${PYTHON_ARGS} --dry-run"
fi

# Create temp file for script output
TEMP_OUTPUT=$(mktemp /tmp/fixture_gen_XXXXXX.json)
trap "rm -f ${TEMP_OUTPUT}" EXIT

# Run the improved fixture generation script
echo "Running fixture generation..."
PYTHONPATH=. uv run python agent_scripts/generate_debug_fixture.py ${PYTHON_ARGS} 2>&1 | tee "${TEMP_OUTPUT}"

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
DETAIL_URL=$(echo "${JSON_OUTPUT}" | jq -r '.detail_url // empty')
COMPANY=$(echo "${JSON_OUTPUT}" | jq -r '.company // empty')
HANDLER=$(echo "${JSON_OUTPUT}" | jq -r '.handler // empty')
REMOTE_OVERRIDE=$(echo "${JSON_OUTPUT}" | jq -r '.remote_override // false')
JOB_ID=$(echo "${JSON_OUTPUT}" | jq -r '.job_id // empty')

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
REMOTE_NOTE=""
if [ "${REMOTE_OVERRIDE}" = "true" ]; then
  REMOTE_NOTE="
## Remote Company Override
This company (${COMPANY}) is in \`remote_companies.yaml\`, so all jobs are marked \`remote: true\` regardless of the job description content.
"
fi

PROMPT="I need help verifying and fixing job extraction for this debug fixture.

## Job ID: ${JOB_ID}

## Files Generated:
- **Fixture**: ${FIXTURE_PATH}
- **Assertions**: ${ASSERTION_PATH}
- **Test identifier**: ${IDENTIFIER}
${REMOTE_NOTE}
## Your Tasks:

### 1. Review Assertions
The assertion file was auto-generated with sensible defaults. Review and verify:
\`\`\`bash
cat ${ASSERTION_PATH}
\`\`\`

Check these fields are accurate:
- \`location_contains\`: Verify against actual job posting
- \`level\`: junior/mid/senior/staff based on title
- \`is_remote\`: Should be \`true\` if company is in remote_companies.yaml

### 2. Run the Debug Test (with verbose extraction steps)
\`\`\`bash
DEBUG_EXTRACTION_VERBOSE=1 uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[${IDENTIFIER}] -v
\`\`\`

### 3. Check Extraction Output
\`\`\`bash
# Summary extraction result
cat ./site-detail-e2e-examples/${HANDLER}_extraction.json

# IMPORTANT: Detailed step-by-step extraction log (created when DEBUG_EXTRACTION_VERBOSE=1)
cat ./site-detail-e2e-examples/${HANDLER}_extraction_steps.md
\`\`\`

The \`_extraction_steps.md\` file shows the full extraction pipeline:
1. **Raw SpiderCloud response** - The markdown content scraped from the page
2. **Handler detection** - Which handler was used for this URL
3. **Handler normalization** - Output of \`normalize_markdown()\` including extracted title
4. **Workflow execution** - The batch sent to \`process_spidercloud_job_batch()\`
5. **Extracted job details** - Final fields extracted
6. **Convex mutation payload** - What would be sent to Convex

Look for:
- Word count (should be > 300 for most job descriptions)
- JSON blocks in description (should NOT appear)
- Correct location parsing
- Correct remote status

### 3b. Use Modular Extractor Debug (Advanced)

For detailed per-strategy extraction tracing, use the modular extractors:

\`\`\`bash
uv run python << 'PYEOF'
from job_scrape_application.workflows.extractors import (
    ExtractionContext,
    extract_job_fields,
)
import json
from pathlib import Path

# Load the fixture
fixture_path = Path("${FIXTURE_PATH}")
with open(fixture_path) as f:
    fixture = json.load(f)

# Parse response
response = json.loads(fixture["response"][0])
markdown = response["content"].get("commonmark") or response["content"].get("raw", "")

# Create extraction context with debug mode
ctx = ExtractionContext.from_scrape_result(
    url=fixture["request"]["url"],
    markdown=markdown,
    debug=True,
)

# Extract all fields with full strategy trace
results = extract_job_fields(ctx, run_all=True)

print("=== EXTRACTOR DEBUG TRACE ===")
for field, result in results.items():
    print(f"\\n{field}: {result.final_value}")
    print(f"  Winner: {result.winning_strategy}")
    for sr in result.all_results:
        status = "VALID" if sr.is_valid else "SKIP"
        pname = sr.priority.name if hasattr(sr.priority, "name") else f"CUSTOM_{sr.priority}"
        print(f"    [{status}] {sr.strategy_name} ({pname}): {sr.reason}")
PYEOF
\`\`\`

This shows WHY each field got its value - which strategy won and why others were skipped.

### 4. Fix Issues (if test fails)

#### If location is wrong:
- Check site handler: \`job_scrape_application/workflows/site_handlers/${HANDLER}.py\`
- May need to implement/update \`extract_location_hint()\`

#### If description has JSON blocks:
- Implement/update \`normalize_markdown()\` in the handler

#### If remote status is wrong:
- Check if company should be in \`remote_companies.yaml\`
- Check handler's remote detection logic

### 5. Verify Fix
\`\`\`bash
# Re-run debug test with verbose output
DEBUG_EXTRACTION_VERBOSE=1 uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[${IDENTIFIER}] -v

# Review the updated extraction steps
cat ./site-detail-e2e-examples/${HANDLER}_extraction_steps.md

# Ensure main tests still pass
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py -v --tb=short
\`\`\`

## Reference Files
- Site handlers: \`job_scrape_application/workflows/site_handlers/\`
- Remote companies config: \`job_scrape_application/config/prod/remote_companies.yaml\`
- Main extraction logic: \`job_scrape_application/workflows/scrapers/spidercloud_scraper.py\`

## Debug Folder Structure
Fixtures are organized per-company with date-based naming:
\`\`\`
tests/job_scrape_application/workflows/
├── fixtures/debug/
│   └── {company}/
│       └── {handler}_{short_id}_{date}_detail.json
└── assertions/debug/
    └── {company}/
        └── {handler}_{short_id}_{date}.yml
\`\`\`

This preserves history and allows multiple fixtures per company."

# Launch Claude Code
echo "Launching Claude Code..."
echo ""
claude "${PROMPT}"
