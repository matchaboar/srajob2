#!/usr/bin/env bash
# [MISE] description="Fix job extraction issues using Claude Code with automated fixture generation"
# [USAGE] arg "<url>" help="Job share URL from Convex prod (e.g., https://srajob.netlify.app/job/abc123)" default=""

set -e

# Check if URL provided
if [ -z "${usage_url}" ]; then
  echo "Usage: mise run fix_job_extraction <job_share_url>"
  echo ""
  echo "Example:"
  echo "  mise run fix_job_extraction https://srajob.netlify.app/job/k57abc123xyz"
  echo ""
  echo "This script will:"
  echo "  1. Extract the job ID from the URL"
  echo "  2. Fetch job details from Convex prod"
  echo "  3. Automatically fetch SpiderCloud fixture to debug folder"
  echo "  4. Create placeholder assertion file"
  echo "  5. Launch Claude Code to write assertions and fix the issue"
  exit 1
fi

# Extract job ID from URL
# Supports formats:
#   https://srajob.netlify.app/job/k57abc123xyz
#   https://affable-kiwi-46.convex.site/share/job?id=k57abc123xyz&app=...
#   k57abc123xyz (raw ID)
JOB_ID=""
if [[ "${usage_url}" =~ /job/([a-zA-Z0-9_]+) ]]; then
  JOB_ID="${BASH_REMATCH[1]}"
elif [[ "${usage_url}" =~ [\?\&]id=([a-zA-Z0-9_]+) ]]; then
  JOB_ID="${BASH_REMATCH[1]}"
elif [[ "${usage_url}" =~ ^[a-zA-Z0-9_]+$ ]]; then
  JOB_ID="${usage_url}"
else
  echo "Error: Could not extract job ID from URL: ${usage_url}" >&2
  echo "Expected format: https://srajob.netlify.app/job/<job_id>, share URL with ?id=<job_id>, or just <job_id>" >&2
  exit 1
fi

echo "Job ID: ${JOB_ID}"

# Create temp file for job data
TEMP_FILE=$(mktemp /tmp/job_data_XXXXXX.json)
trap "rm -f ${TEMP_FILE}" EXIT

# Fetch job from Convex prod
echo "Fetching job from Convex prod..."
cd job_board_application

JOB_DATA=$(npx convex run --prod jobs:getJobById "{\"id\":\"${JOB_ID}\"}" 2>/dev/null || echo "null")

if [ "${JOB_DATA}" == "null" ] || [ -z "${JOB_DATA}" ]; then
  echo "Error: Job not found in Convex prod: ${JOB_ID}" >&2
  exit 1
fi

# Save job data to temp file
echo "${JOB_DATA}" > "${TEMP_FILE}"

# Extract key fields for display
TITLE=$(echo "${JOB_DATA}" | jq -r '.title // "N/A"')
COMPANY=$(echo "${JOB_DATA}" | jq -r '.company // "N/A"')
LOCATION=$(echo "${JOB_DATA}" | jq -r '.location // "N/A"')
JOB_URL=$(echo "${JOB_DATA}" | jq -r '.url // "N/A"')
REMOTE=$(echo "${JOB_DATA}" | jq -r '.remote // false')
LEVEL=$(echo "${JOB_DATA}" | jq -r '.level // "N/A"')
DESCRIPTION_LEN=$(echo "${JOB_DATA}" | jq -r '.description | length // 0')

echo ""
echo "=== Job Details ==="
echo "Title:       ${TITLE}"
echo "Company:     ${COMPANY}"
echo "Location:    ${LOCATION}"
echo "Remote:      ${REMOTE}"
echo "Level:       ${LEVEL}"
echo "Desc Length: ${DESCRIPTION_LEN} chars"
echo "Job URL:     ${JOB_URL}"
echo ""

cd ..

# Extract site name from URL (e.g., "netflix" from "explore.jobs.netflix.net")
SITE_NAME=""
if [[ "${JOB_URL}" =~ https?://([^/]+) ]]; then
  DOMAIN="${BASH_REMATCH[1]}"
  # Try to extract company name from domain
  # Examples: explore.jobs.netflix.net -> netflix, boards.greenhouse.io -> greenhouse
  if [[ "${DOMAIN}" =~ jobs\.([a-z0-9]+)\. ]]; then
    SITE_NAME="${BASH_REMATCH[1]}"
  elif [[ "${DOMAIN}" =~ ([a-z0-9]+)\.greenhouse\.io ]]; then
    SITE_NAME="${BASH_REMATCH[1]}"
  elif [[ "${DOMAIN}" =~ ([a-z0-9]+)\.ashbyhq\.com ]]; then
    SITE_NAME="${BASH_REMATCH[1]}"
  elif [[ "${DOMAIN}" =~ careers\.([a-z0-9]+)\. ]]; then
    SITE_NAME="${BASH_REMATCH[1]}"
  else
    # Fallback: use first part of domain
    SITE_NAME=$(echo "${DOMAIN}" | cut -d'.' -f1)
  fi
fi

# If we couldn't extract site name, use company name
if [ -z "${SITE_NAME}" ] && [ "${COMPANY}" != "N/A" ]; then
  SITE_NAME=$(echo "${COMPANY}" | tr '[:upper:]' '[:lower:]' | tr -d ' ')
fi

# Create identifier for debug fixture (site_jobid)
IDENTIFIER="${SITE_NAME}_${JOB_ID}"

echo "=== Automated Setup ==="
echo "Identifier: ${IDENTIFIER}"

# Create debug directories if they don't exist
mkdir -p tests/job_scrape_application/workflows/fixtures/debug
mkdir -p tests/job_scrape_application/workflows/assertions/debug

# Define paths
FIXTURE_PATH="tests/job_scrape_application/workflows/fixtures/debug/${IDENTIFIER}_detail.json"
ASSERTION_PATH="tests/job_scrape_application/workflows/assertions/debug/${IDENTIFIER}.yml"

# Fetch SpiderCloud fixture
echo "Fetching SpiderCloud fixture..."
if PYTHONPATH=. uv run python agent_scripts/dump_spidercloud_response.py "${JOB_URL}" --out "${FIXTURE_PATH}" --use-handler-config 2>&1; then
  echo "✓ Fixture saved to: ${FIXTURE_PATH}"
else
  echo "⚠ Warning: Failed to fetch SpiderCloud fixture. Continuing anyway..."
fi

# Create placeholder assertion file if it doesn't exist
if [ ! -f "${ASSERTION_PATH}" ]; then
  echo "Creating placeholder assertion file..."
  cat > "${ASSERTION_PATH}" << EOF
site_id: ${SITE_NAME}
detail_url: ${JOB_URL}
expected:
  # TODO: Fill in the expected values below by examining the fixture
  # Run: uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v
  # Check: cat ./site-detail-e2e-examples/${SITE_NAME}_extraction.json

  title: "${TITLE}"
  company: "${COMPANY}"
  location_contains: "FILL_THIS"  # e.g., "New York", "San Francisco"
  is_remote: ${REMOTE}
  level: mid  # junior, mid, senior, staff
  description_min_words: 300  # Adjust based on expected content
  description_not_contains: '{"'  # Ensure no JSON blocks
  cost_milli_cents_min: 1
  posted_at_not_null: true
EOF
  echo "✓ Placeholder assertion created: ${ASSERTION_PATH}"
else
  echo "✓ Assertion file already exists: ${ASSERTION_PATH}"
fi

echo ""
echo "=== Files Ready ==="
echo "Fixture:    ${FIXTURE_PATH}"
echo "Assertions: ${ASSERTION_PATH}"
echo ""

# Build the Claude Code prompt
PROMPT="I need help fixing job extraction for this job from Convex prod.

## Job ID: ${JOB_ID}

## Current Extracted Data:
- Title: ${TITLE}
- Company: ${COMPANY}
- Location: ${LOCATION}
- Remote: ${REMOTE}
- Level: ${LEVEL}
- Description Length: ${DESCRIPTION_LEN} chars
- Original Job URL: ${JOB_URL}

## Full Job Data (from Convex):
\$(cat ${TEMP_FILE})

## ✅ Automated Setup Complete

The fixture and placeholder assertion file have been created:
- **Fixture**: ${FIXTURE_PATH}
- **Assertions**: ${ASSERTION_PATH}

## 🎯 Your Tasks:

### 1. Write Assertions (Manual - Required)
The placeholder assertion file needs proper values. Edit:
\`${ASSERTION_PATH}\`

Fill in these fields by examining the fixture:
- \`location_contains\`: Extract actual location from job posting
- \`level\`: Determine correct level (junior/mid/senior/staff)
- \`description_min_words\`: Set appropriate minimum (usually 300+)
- \`description_not_contains\`: Add patterns that should NOT appear (e.g., JSON blocks, metadata)

**Tip**: Look at the fixture to see what content is available:
\`\`\`bash
python -c \"
import json
with open('${FIXTURE_PATH}', 'r') as f:
    data = json.load(f)
    item = data[0][0]
    md = item['content']['commonmark']
    print('Markdown length:', len(md))
    lines = md.split('\\\n')
    print('First 30 lines:')
    for i, line in enumerate(lines[:30]):
        print(f'{i:3d}: {line[:100]}')
\"
\`\`\`

### 2. Run Debug Test
\`\`\`bash
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[${IDENTIFIER}] -v
\`\`\`

This will show what's being extracted and what's failing.

### 3. Check Extraction Output
\`\`\`bash
cat ./site-detail-e2e-examples/${SITE_NAME}_extraction.json
\`\`\`

Look for:
- Word count (too low?)
- JSON blocks in description
- Wrong location parsing
- Missing fields

### 4. Fix the Handler
Edit: \`job_scrape_application/workflows/site_handlers/${SITE_NAME}.py\`

Common fixes:
- Implement/update \`normalize_markdown()\` to clean description
- Add \`extract_location_hint()\` for better location parsing
- Update regex patterns for title/company extraction

Example from Netflix fix:
- **Problem**: Description had JSON blocks (122 words)
- **Solution**: Implemented \`normalize_markdown()\` to strip JSON
- **Result**: Clean 1010-word description

### 5. Verify Fix
\`\`\`bash
# Re-run debug test
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v

# Ensure main test still passes
uv run pytest \"tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[${SITE_NAME}]\" -v
\`\`\`

## 📚 Reference

See \`tests/job_scrape_application/workflows/fixtures/debug/README.md\` for full documentation on the debug workflow."

# Launch Claude Code
echo "Launching Claude Code..."
echo ""
claude "${PROMPT}"
