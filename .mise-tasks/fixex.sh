#!/usr/bin/env bash
# [MISE] description="Fix prod extract"
# [USAGE] arg "<url>" help="The URL from share link" default="https://example.com/jobs_list"
# if usage_url is "https://example.com/jobs_list" then skip
if [ "${usage_url}" == "https://example.com/jobs_list" ]; then
  echo "Skipping: usage_url is the default value. Please provide a job listing site URL." >&2
  exit 0
fi

codex exec \
  --model gpt-5.2-codex \
  --sandbox danger-full-access \
  --config model_reasoning_effort="high" \
  --config model_verbosity="medium" \
  "You are an expert software developer. Using agent_scripts, create mulitple unit tests based on data from the
  following job ID from URL in prod convex: ${usage_url}. The unit test should check extraction of data such
  as location (city, state, country, remote=true|false|unknown), job title, company name, min salary, max salary,
  posted_date (or updated_at), job description does not have junk. If anything fails, please improve the base
  site handler class and methods for extracting data, but if it needs to be very site specific, you can update the sub
  classes. Run all unit tests to ensure no regression before completing your task of updating the extraction code and tests."