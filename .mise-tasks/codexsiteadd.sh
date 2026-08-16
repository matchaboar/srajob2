#!/usr/bin/env bash
# [MISE] description="Add a site handler using AI"
# [USAGE] arg "<url>" help="The URL to add" default="https://example.com/jobs_list"
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
  "You are an expert software developer. Add a new site handler to our job
  listing application for the following URL: ${usage_url}. Analyze the website structure,
  identify how job listings are presented, and implement the necessary code to scrape
  job data from this site. Ensure that your implementation follows best practices for
  web scraping, including handling pagination, if applicable. Write clean, maintainable
  code and include comments where necessary. After implementing the site handler, create
  unit tests to verify that the new handler works correctly. Update any relevant
  documentation to reflect the addition of the new site handler.
  As a reminder, you have access to ./agent_scripts in order to help generate test fixtures,
  which can help make unit tests to validate that pagination through pages works,
  job descriptions are accurately captured without junk data, and we also extract
  title, company, location, and posted date (if present) without junk data.
  If you are succesful, please ensure that you add the site url to: 
  job_scrape_application/config/prod/site_schedules.yml
  job_scrape_application/config/dev/site_schedules.yml"