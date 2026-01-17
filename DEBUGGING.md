# DEBUGGING.md

## Debug Failing Tests

```bash
DEBUG_EXTRACTION_VERBOSE=1 uv run pytest "path/to/test.py::test_name" -v
```

Output in `./site-detail-e2e-examples/`:
- `{site}_extraction_steps.md` - **Step 4.6** shows all strategies tried per field (winner marked `🏆`)
- `{site}_extraction.json` - Extracted values
- `{handler}_listing_extraction_steps.md` - URL pipeline: raw → filtered → normalized

---

## Quick Reference

### Job Detail Extraction
```bash
mise run fix_job_extraction https://srajob.netlify.app/job/k57abc123xyz
```

### Listing/Crawl Issues
```bash
mise run fix_job_crawl airbnb
```

### Listing Workflow Fixture Generation
```bash
# Generate timestamped fixtures + ground truth for listing workflow integration tests
GENERATE_LISTING_FIXTURES=1 SPIDER_API_KEY=... \
  uv run pytest tests/job_scrape_application/workflows/workflow/test_scrape_listing_batch_integration.py -k airbnb -v
```
Notes:
- Generation is disabled by default; set `GENERATE_LISTING_FIXTURES=1` to enable.
- Fixtures are written under `tests/job_scrape_application/workflows/fixtures/debug/<site>/`.
- Ground truth is written under `tests/job_scrape_application/workflows/ground_truth/debug/<site>/`.

### Generate Fixtures
```bash
# Bulk regenerate
uv run python agent_scripts/core/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-only SITE

# Single page
uv run python agent_scripts/dump_spidercloud_response.py "URL" --out FILE --fixture-format --use-handler-config
```

---

## Strategy Priority Levels

Lower = higher priority. First valid result wins.

| Priority | Name | Description |
|----------|------|-------------|
| 100 | STRUCTURED_DATA | JSON-LD, API responses |
| 200 | SITE_HANDLER | Site-specific handlers (Greenhouse, Ashby, etc.) |
| 300 | EXPLICIT_FIELD | Labeled fields (Location: X) |
| 400 | URL_DERIVED | Extracted from URL patterns |
| 500 | CONTENT_PATTERN | Regex patterns in content |
| 600 | HEURISTIC | Fuzzy matching, inference |
| 900 | FALLBACK | Last resort defaults |

---

## Assertion Format

```yaml
site_id: purestorage
detail_url: https://boards-api.greenhouse.io/v1/boards/purestorage/jobs/7472241
expected:
  title: AI/HPC Pre-Sales Systems Engineer  # or title_contains for partial
  company: Pure Storage                      # or company_contains
  location_contains: Atlanta
  is_remote: false
  level: senior                              # junior/mid/senior/staff
  description_min_words: 50
  description_not_contains: '{"'
  cost_milli_cents_min: 1
  posted_at_not_null: true
```

Fixture locations:
- Schedule: `tests/.../fixtures/dbos_schedule/{site}_{listing|detail}.json`
- Debug: `tests/.../fixtures/debug/{company}/{handler}_{id}_{timestamp}_detail.json`

---

## Common Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| Title shows "Untitled" | Handler can't parse content | Update `normalize_markdown()` |
| Handler not detected | URL pattern doesn't match | Check handler's `_URL_PATTERNS` |
| Description has JSON | Raw API not cleaned | Add JSON stripping to `normalize_markdown()` |
| No URLs extracted | JS-heavy page or wrong handler | Check raw content in trace, verify handler |
| Wrong URLs (listing not detail) | URL filtering issue | Update `_is_base_listing_page()` |

---

## Key Functions

| Function | Purpose |
|----------|---------|
| `get_site_handler(url)` | Detect handler for URL |
| `handler.normalize_markdown(md)` | Clean content, extract title |
| `handler.get_links_from_json(data)` | Extract URLs from JSON API |
| `handler.get_links_from_raw_html(html, url)` | Extract URLs from HTML |

---

## Convex Validation

```bash
cd job_board_application
npx convex run --prod router:getJobById '{"id":"JOB_ID"}'
```
