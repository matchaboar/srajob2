# Agent Scripts

Quick reference for the helper scripts in `agent_scripts/`. Always run via `uv run` (or `uvx` for modules), never `python` directly.

## Fixtures & Scrape Payloads
- `fetch_spidercloud_fixtures.py`
  - Bulk refreshes the standard SpiderCloud fixtures (Greenhouse, Pinterest, Bloomberg Avature, GoDaddy).
  - Example: `uv run agent_scripts/fetch_spidercloud_fixtures.py`
- `fetch_bloomberg_avature_fixtures.py`
  - Refreshes Bloomberg Avature *SearchJobs* HTML fixtures (3 pages).
  - Example: `uv run agent_scripts/fetch_bloomberg_avature_fixtures.py`
- `fetch_bloomberg_avature_searchjobsdata.py`
  - Fetches Bloomberg Avature *SearchJobsData* JSON (seeds a SearchJobs session first).
  - Example: `uv run agent_scripts/fetch_bloomberg_avature_searchjobsdata.py`
- `dump_spidercloud_response.py`
  - One-off SpiderCloud scrape to inspect raw response/metadata.
  - Example: `uv run agent_scripts/dump_spidercloud_response.py "<url>" --out /tmp/spidercloud.json`

## Validation & Debugging
- `validate_bloomberg_avature_live.py`
  - Validates live Bloomberg Avature pages against the Avature handler (pagination + job links).
  - Run before updating fixtures or logic.
  - Example: `uv run agent_scripts/validate_bloomberg_avature_live.py`
- `test_job_extract_spidercloud_api.py`
  - Diagnoses whether a SpiderCloud payload contains a jobs list (JSON parsing / HTML-embedded JSON).
  - Example: `uv run agent_scripts/test_job_extract_spidercloud_api.py --url "<api_or_html_url>"`
- `diagnose_spidercloud_stalls.py`
  - Summarizes scrape queue health and possible stall causes from Convex.
  - Example: `uv run agent_scripts/diagnose_spidercloud_stalls.py --env prod`
- `measure_spidercloud_batch.py`
  - Samples queue URLs and measures SpiderCloud batch latency/costs.
  - Example: `uv run agent_scripts/measure_spidercloud_batch.py --env prod --provider spidercloud`

## Convex Data Maintenance
- `wipe_comapny_convex.py`
  - Wipes Convex data by domain or company and triggers `runSiteNow` for matches.
  - Examples:
    - `uv run agent_scripts/wipe_comapny_convex.py --env prod --domain bloomberg.avature.net`
    - `uv run agent_scripts/wipe_comapny_convex.py --env prod --company lambda`
- `export_scrape_queue_fixture.py`
  - Exports `scrape_url_queue` rows into a fixture JSON.
  - Example: `uv run agent_scripts/export_scrape_queue_fixture.py --provider spidercloud --statuses pending,processing`
- `export_site_schedules.py`
  - Exports Convex schedules into `job_scrape_application/config/<env>/site_schedules.yml`.
  - Example: `PYTHONPATH=. uv run agent_scripts/export_site_schedules.py --env prod`

## Schedule Config Utilities
- `generate_site_schedule_json.py`
  - Converts `site_schedules.yml` into Convex JSON (`job_board_application/convex/site_schedules.<env>.json`).
  - Example: `uv run agent_scripts/generate_site_schedule_json.py`
