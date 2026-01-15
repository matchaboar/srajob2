---
name: add-site
description: Add new company site to scraper with automated setup
---

# Add Site

Add a new company career site to the job scraper.

## Usage

```
/add-site <job-detail-url>
```

## What This Does

1. Analyzes the URL to identify company, domain, and platform handler
2. Generates SpiderCloud fixture for a job detail page
3. Creates placeholder assertion file for validation
4. Suggests listing URL and provides site schedule YAML entry
5. Guides through testing and deployment process

## Examples

```bash
# Using a job detail URL
/add-site https://careers.newcompany.com/jobs/12345

# With explicit listing URL override
/add-site https://careers.newcompany.com/jobs/12345 --listing-url https://careers.newcompany.com/jobs

# Dry run (show what would be created)
/add-site https://careers.newcompany.com/jobs/12345 --dry-run
```

## Implementation

This skill invokes:
```bash
uv run python agent_scripts/core/generate_new_site_fixture.py "${JOB_URL}" --output-format json
```

## Output

The script outputs JSON with the following structure:
```json
{
  "job_url": "https://...",
  "listing_url": "https://...",
  "fixture_path": "tests/.../fixtures/debug/company/handler_id_date_detail.json",
  "assertion_path": "tests/.../assertions/debug/company/handler_id_date.yml",
  "identifier": "handler_id_date",
  "company": "Company Name",
  "normalized_company": "company_name",
  "handler": "greenhouse",
  "is_known_platform": true,
  "schedule_entry": "- url: ...\n  name: ...\n  ..."
}
```

## Workflow

After running this skill:

1. **Review site detection** - Check identified company, handler, and listing URL
   - Company name detected correctly?
   - Handler type correct (greenhouse, ashby, workday, etc.)?
   - Listing URL looks right?

2. **Fill in assertion TODOs** - Update placeholder values:
   ```yaml
   expected:
     title_contains: "TODO: Add expected title keywords"
     company_contains: "Company Name"
     location_contains: "TODO: Add expected location"
     level: "TODO: junior/mid/senior/staff"
     is_remote: false  # TODO: Update based on job
   ```

3. **Run the debug test**:
   ```bash
   uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[${identifier}] -v
   ```

4. **Fix handler if needed**:
   - For known platforms (Greenhouse, Ashby, Workday): Handler should work out of the box
   - For unknown/custom platforms: May need to create new handler in `site_handlers/`

5. **Add to site schedule** - Use the provided YAML entry:
   - **Dev testing**: Add to `job_scrape_application/config/dev/site_schedules.yml`
   - **Production**: After validation, add to `job_scrape_application/config/prod/site_schedules.yml`

6. **Upload schedule to Convex**:
   ```bash
   # Dev
   mise run devschedule

   # Prod (after validation)
   npx convex run --prod router:uploadSiteSchedules
   ```

7. **Trigger test scrape**:
   ```bash
   # Find site ID in Convex
   npx convex run --prod router:runSiteNow '{"id":"k57siteid123"}'
   ```

8. **Monitor and validate**:
   - Check queue status: `mise run queue-status`
   - Review extracted jobs in Convex
   - Ensure data quality is good

## Site Schedule Entry

The script generates a complete YAML entry for `site_schedules.yml`:
```yaml
- url: https://careers.newcompany.com/jobs
  name: New Company
  enabled: true
  type: greenhouse  # Detected handler type
  scrapeProvider: spidercloud
  paginationLimit: 0
  schedule:
    name: Weekdays every 2 hours @ 09:30
    days: *id001
    startTime: 09:30
    intervalMinutes: 120.0
    timezone: America/Denver
```

## Handler Types

Common handler types automatically detected:
- **greenhouse**: Greenhouse job boards (API + marketing pages)
- **ashby** / **ashbyhq**: AshbyHQ job boards
- **workday**: Workday career sites
- **lever**: Lever job boards
- **kula**: Kula Careers sites
- **netflix**: Netflix careers (GraphQL API)
- **meta**: Meta careers (custom API)
- **custom**: Unknown platforms (requires custom handler)

## Key Features

- **Auto-detection**: Identifies known platforms from URL patterns
- **Schedule generation**: Provides ready-to-use YAML configuration
- **Test framework**: Creates fixtures and assertions for validation
- **Handler guidance**: Shows which handler will be used
- **Listing URL inference**: Suggests listing page URL from detail URL
