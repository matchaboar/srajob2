---
name: check-latest-job-parsing
description: Audit recent job parsing, add fixtures/tests, and fix extraction logic.
---

You are a Codex agent responsible for validating recent job scraping accuracy.

## Goal
Inspect the latest 25 scraped job descriptions (dev by default), identify parsing issues, create unit tests with SpiderCloud fixtures for problematic jobs, and fix the extraction logic in DBOS workflow steps or site handlers.

## Workflow
1. Ask which environment to target (dev or prod).
2. Run the recent parsing report:
   - `uv run agent_scripts/check_recent_job_parsing.py --env <env> --limit 25 --out tmp/recent_job_parsing.json`
3. Review `problem_jobs` in `tmp/recent_job_parsing.json` to find issues.
4. For each problematic job URL, capture a SpiderCloud fixture:
   - `uv run agent_scripts/dump_spidercloud_response.py "<job-url>" --out tests/job_scrape_application/workflows/fixtures/spidercloud_<slug>_job_detail_commonmark.json --return-format commonmark --use-handler-config`
5. Add or extend workflow tests (see existing fixtures in `tests/job_scrape_application/workflows`).
6. Fix extraction logic in `job_scrape_application/workflows` (site handlers or scraper helpers).
7. If any Python files change, run `uvx ruff check` for linting.
8. After fixes/tests pass, wipe and requeue the broken scrapes for re-extract:
   - Requeue by URL using the report:
     `uv run agent_scripts/requeue_problem_jobs.py --env <env> --input tmp/recent_job_parsing.json --force-refresh --delete-jobs`
   - Requeue by company instead when URLs are too many:
     `uv run agent_scripts/clear_ignored_and_rescrape.py <company...> --env <env>`

## Notes
- Prefer existing fixture and test patterns (`tests/job_scrape_application/workflows/test_*_fixture.py`).
- Keep changes scoped to parsing accuracy issues only.
- If dev has no recent jobs, ask whether to use prod or seed dev data.
