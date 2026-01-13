#!/usr/bin/env bash
# [MISE] description="Fix job extraction issues using Claude Code"
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
  echo "  3. Launch Claude Code to analyze and fix extraction issues"
  exit 1
fi

# Extract job ID from URL
# Supports formats:
#   https://srajob.netlify.app/job/k57abc123xyz
#   k57abc123xyz (raw ID)
JOB_ID=""
if [[ "${usage_url}" =~ /job/([a-zA-Z0-9_]+) ]]; then
  JOB_ID="${BASH_REMATCH[1]}"
elif [[ "${usage_url}" =~ ^[a-zA-Z0-9_]+$ ]]; then
  JOB_ID="${usage_url}"
else
  echo "Error: Could not extract job ID from URL: ${usage_url}" >&2
  echo "Expected format: https://srajob.netlify.app/job/<job_id> or just <job_id>" >&2
  exit 1
fi

echo "Job ID: ${JOB_ID}"

# Create temp file for job data
TEMP_FILE=$(mktemp /tmp/job_data_XXXXXX.json)
trap "rm -f ${TEMP_FILE}" EXIT

# Fetch job from Convex prod
echo "Fetching job from Convex prod..."
cd job_board_application

JOB_DATA=$(npx convex run --prod router:getJobById "{\"id\":\"${JOB_ID}\"}" 2>/dev/null || echo "null")

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

## What I Need:
1. Identify what's wrong with the extraction (e.g., invalid title, wrong location, missing description)
2. Find or create a test fixture for this job's detail page
3. Run the extraction test to reproduce the issue
4. Fix the relevant site handler or scraper code
5. Verify the fix with the extraction tests

## Debugging Steps:
1. First, determine which site handler is used for this URL
2. Check if we have a fixture for this site in tests/job_scrape_application/workflows/fixtures/dbos_schedule/
3. If not, create one using agent_scripts/fetch_spidercloud_fixtures.py or agent_scripts/dump_spidercloud_response.py
4. Run the extraction test: uv run pytest 'tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[SITE]' -v
5. Check ./site-detail-e2e-examples/SITE_extraction.json for current extraction output
6. Fix the handler code in job_scrape_application/workflows/site_handlers/
7. Update the assertion file in tests/job_scrape_application/workflows/assertions/

See DEBUGGING.md for detailed debugging procedures."

# Launch Claude Code
echo "Launching Claude Code..."
echo ""
claude "${PROMPT}"
