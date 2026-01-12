# Next Step Summary

## Job URL Extraction

- Validate that Workday listing URLs now enqueue API detail URLs (Broadcom/Dataminr) and that Ashby job IDs produce detail URLs for Serval/Factory/Lambda.
- Re-run the targeted DBOS workflow test for the five sites:
  - `uv run pytest tests/job_scrape_application/workflows/test_dbos_schedule_workflow.py -q -k "broadcom or dataminr or serval or ashbyhq or lambda" -rs`
- If any still fail, refresh those fixtures with `uv run agent_scripts/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-out tests/job_scrape_application/workflows/fixtures/dbos_schedule --schedule-only broadcom dataminr serval ashbyhq lambda`.

## Datadog Greenhouse Detail

- Confirm the Datadog fixture uses `boards/datadog` (not `datadoghq`) and rerun:
  - `uv run pytest tests/job_scrape_application/workflows/test_dbos_schedule_workflow.py -q -k "datadog" -rs`
- If it still 404s, refresh the Datadog fixtures using the prod schedule and retry.
