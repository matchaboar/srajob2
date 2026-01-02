#!/usr/bin/env bash
# [MISE] description="Fix job listing site"
# [USAGE] arg "<company>" help="The company name" default="none"
# [USAGE] arg "<additional_context>" help="Any additional context (like what is broken)" default="none"
# if usage_url is "https://example.com/jobs_list" then skip
if [ "${usage_company}" == "none" ]; then
  echo "Skipping: company is none. Please provide a job listing site company name." >&2
  exit 0
fi

codex exec \
  --model gpt-5.2-codex \
  --sandbox danger-full-access \
  --config model_reasoning_effort="high" \
  --config model_verbosity="medium" \
  "You are an expert software developer. Assume that pagination or job extraction is broken for some site.
  Using agent_scripts, create mulitple unit tests based on data from the job site in the configuration file for site_schedules,
  or in convex table. You may use agent_scripts or write additional scripts that help in this task. 
  The unit test should check extraction of jobs, but also if there is pagination, 
  it should ensure that all paginated pages are queued and those jobs get extracted too.
  COMPANY NAME: ${usage_company}
  ADDITIONAL CONTEXT FOR BUGS: ${usage_additional_context}"