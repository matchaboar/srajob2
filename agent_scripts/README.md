# Agent Scripts

This directory contains Python scripts for managing the job scraper system. Scripts are organized by function into subdirectories for better maintainability.

## Directory Structure

```
agent_scripts/
├── lib/                    # Shared utility modules (890+ lines)
│   ├── __init__.py        # Exports all utilities
│   ├── site_utils.py      # Site info extraction, URL handling
│   ├── fixtures.py        # Fixture saving/loading utilities
│   ├── assertions.py      # YAML assertion generation
│   ├── convex.py          # Convex database access
│   └── dbos.py            # DBOS queue management
│
├── core/                  # Core fixture generation (6 scripts)
│   ├── generate_debug_fixture.py           # Debug job detail extraction
│   ├── generate_debug_listing_fixture.py   # Debug job listing extraction
│   ├── generate_new_site_fixture.py        # Add new company sites
│   ├── refresh_all_site_fixtures.py        # Bulk regenerate fixtures
│   ├── fetch_spidercloud_fixtures.py       # Fetch production fixtures
│   └── dump_spidercloud_response.py        # Manual fixture capture
│
├── diagnostics/           # Debugging tools (5 scripts)
│   ├── debug_job_extraction.py            # Debug specific job issues
│   ├── diagnose_spidercloud_stalls.py     # Diagnose SpiderCloud timeouts
│   ├── diagnose_ignored_job.py            # Why jobs are ignored
│   ├── check_recent_job_parsing.py        # Recent parsing issues
│   └── check_scrape_blockers.py           # Identify blocking issues
│
├── monitoring/            # Health checks & cost tracking (4 scripts)
│   ├── summarize_scrape_costs.py          # SpiderCloud cost summary
│   ├── check_recent_scrapes_for_site.py   # Recent scrape status
│   ├── check_queue_status.py              # Queue health check
│   └── measure_spidercloud_batch.py       # Batch performance metrics
│
├── maintenance/           # Data cleanup operations (5 scripts)
│   ├── delete_jobs_from_time.py           # Delete jobs by timestamp
│   ├── delete_skipped_jobs_for_company.py # Clean skipped jobs
│   ├── clear_ignored_and_rescrape.py      # Reset ignored status
│   ├── wipe_site_data_ws.py               # Wipe site data
│   └── wipe_company_convex.py             # Wipe company data
│
├── config/                # Site schedule management (3 scripts)
│   ├── update_and_sync_site_schedules.py  # Update schedules
│   ├── clear_and_upload_site_schedules.py # Upload to Convex
│   └── generate_site_schedule_json.py     # Convert YAML to JSON
│
├── inspection/            # Queue inspection tools (3 scripts)
│   ├── inspect_site_queue.py              # View site queue entries
│   ├── inspect_site_activity.py           # Recent site activity
│   └── inspect_netflix_queue.py           # Netflix-specific queue
│
├── export/                # Data export utilities (4 scripts)
│   ├── export_company_jobs_fixture.py     # Export company jobs
│   ├── export_company_salary_caps.py      # Salary cap analysis
│   ├── export_convex_queue_state_fixture.py # Queue state snapshot
│   └── export_scrape_queue_fixture.py     # Scrape queue dump
│
└── site_operations/       # Generic site handler (1 script)
    └── generic_site_handler.py            # Framework for site ops
```

## Usage Patterns

### Using Scripts Directly

All scripts can be run directly with UV:

```bash
# Core fixture generation
uv run python agent_scripts/core/generate_debug_fixture.py <job-id>
uv run python agent_scripts/core/generate_debug_listing_fixture.py --company airbnb

# Monitoring
uv run python agent_scripts/monitoring/check_queue_status.py airbnb purestorage
uv run python agent_scripts/monitoring/summarize_scrape_costs.py --env prod

# Maintenance
uv run python agent_scripts/maintenance/delete_jobs_from_time.py --hours 48 --env prod
```

### Using Claude Skills (Recommended)

For common workflows, use Claude Skills for guided, automated execution:

```bash
# Debug job extraction
/fix-job-extraction https://srajob.netlify.app/job/k57abc123xyz

# Debug listing extraction
/fix-job-crawl airbnb

# Add new site
/add-site https://careers.newcompany.com/jobs/12345

# Bulk refresh fixtures
/refresh-fixtures --schedule-env prod --only airbnb purestorage
```

See `.claude/skills/README.md` for complete skill documentation.

### Using Mise Tasks

Many scripts have mise task wrappers in `.mise-tasks/`:

```bash
# High-level workflows
mise run fix_job_extraction <job-url>
mise run fix_job_crawl <company>
mise run add_site <job-url>

# Maintenance tasks
mise run delete_jobs_from_time <hours>
mise run scrape_costs
mise run wipe_site <company>
```

## Library Modules (lib/)

Shared utilities used across all scripts. Import with:

```python
from agent_scripts.lib import (
    # Site utilities
    SiteInfo,
    extract_site_info_from_url,
    get_canonical_detail_url,

    # Fixture utilities
    save_fixture,
    load_fixture,
    build_fixture_structure,
    get_fixture_paths,

    # Assertion utilities
    generate_assertion_yaml,
    generate_placeholder_assertion_yaml,
    generate_listing_assertion_yaml,

    # Convex utilities
    extract_job_id_from_url,
    fetch_job_by_id,
    fetch_site_by_id,
    run_convex_query,

    # DBOS queue utilities
    get_queue_status,
    clear_site_queue,
    get_queue_summary_for_companies,
    list_queue_entries,
)
```

### Key Library Features

**site_utils.py:**
- `SiteInfo` dataclass for structured site metadata
- Automatic handler detection (Greenhouse, Ashby, Workday, etc.)
- URL canonicalization (marketing → API URLs)
- Listing URL inference from detail URLs

**fixtures.py:**
- Consistent JSON fixture formatting
- Path conventions for debug fixtures
- Fixture validation and loading

**assertions.py:**
- YAML assertion generation with actual values
- Placeholder assertions with TODOs
- Listing assertions with extracted URLs

**convex.py:**
- Job lookup by ID or share URL
- Site lookup by ID
- Generic query execution

**dbos.py:**
- Queue status checking
- Queue entry management
- Summary statistics

## JSON Output Mode

All core scripts support `--output-format json` for programmatic usage:

```bash
# Human-readable output (default)
uv run python agent_scripts/core/generate_debug_fixture.py <job-id>

# JSON output (for scripts/skills)
uv run python agent_scripts/core/generate_debug_fixture.py <job-id> --output-format json
```

JSON mode:
- Outputs pure JSON (no extraneous text)
- Suppresses logging in some scripts
- Ideal for parsing in bash scripts or Claude Skills
- Maintains error reporting via stderr

## Common Operations

### Debug Job Extraction Issue

```bash
# Option 1: Using Claude Skill (recommended)
/fix-job-extraction https://srajob.netlify.app/job/k57abc123xyz

# Option 2: Using mise task
mise run fix_job_extraction https://srajob.netlify.app/job/k57abc123xyz

# Option 3: Direct script execution
uv run python agent_scripts/core/generate_debug_fixture.py k57abc123xyz
```

### Debug Listing Extraction Issue

```bash
# Option 1: Using Claude Skill (recommended)
/fix-job-crawl airbnb

# Option 2: Using mise task
mise run fix_job_crawl airbnb

# Option 3: Direct script execution
uv run python agent_scripts/core/generate_debug_listing_fixture.py --company airbnb
```

### Add New Company Site

```bash
# Option 1: Using Claude Skill (recommended)
/add-site https://careers.newcompany.com/jobs/12345

# Option 2: Using mise task
mise run add_site https://careers.newcompany.com/jobs/12345

# Option 3: Direct script execution
uv run python agent_scripts/core/generate_new_site_fixture.py https://careers.newcompany.com/jobs/12345
```

### Check Queue Status

```bash
uv run python agent_scripts/monitoring/check_queue_status.py airbnb purestorage netflix
```

### Monitor Scrape Costs

```bash
# Last hour
uv run python agent_scripts/monitoring/summarize_scrape_costs.py --env prod

# Custom time window
uv run python agent_scripts/monitoring/summarize_scrape_costs.py --env prod --lookback-minutes 360
```

### Clean Up Old Jobs

```bash
# Delete jobs older than 48 hours (dry run)
uv run python agent_scripts/maintenance/delete_jobs_from_time.py --hours 48 --env prod

# Actually delete
uv run python agent_scripts/maintenance/delete_jobs_from_time.py --hours 48 --env prod --apply
```

## Migration Notes (January 2026)

**Script Location Changes:**
- `generate_debug_fixture.py` → `core/generate_debug_fixture.py`
- `generate_debug_listing_fixture.py` → `core/generate_debug_listing_fixture.py`
- `generate_new_site_fixture.py` → `core/generate_new_site_fixture.py`
- `refresh_all_site_fixtures.py` → `core/refresh_all_site_fixtures.py`
- `debug_job_extraction.py` → `diagnostics/debug_job_extraction.py`
- `summarize_scrape_costs.py` → `monitoring/summarize_scrape_costs.py`
- (and 25+ more - see git log for full list)

**What Changed:**
- ✅ Scripts organized into 7 functional subdirectories
- ✅ Shared code extracted into `lib/` modules (~890 lines)
- ✅ ~440 lines of duplicate code removed
- ✅ All sys.path hacks removed (using UV workspace)
- ✅ 21 obsolete scripts archived to `_archive/`
- ✅ 4 Claude Skills created for common workflows
- ✅ JSON output mode added to core scripts

**What Stayed the Same:**
- All script functionality preserved
- Command-line arguments unchanged
- Output formats unchanged (except new JSON mode)
- Mise tasks updated automatically

## Development Guidelines

### Adding New Scripts

1. **Choose the right directory:**
   - `core/` - Fixture generation workflows
   - `diagnostics/` - Debugging and troubleshooting
   - `monitoring/` - Health checks and metrics
   - `maintenance/` - Data cleanup and management
   - `config/` - Configuration management
   - `inspection/` - Data inspection and queries
   - `export/` - Data export and analysis

2. **Use shared libraries:**
   ```python
   from agent_scripts.lib import (
       extract_site_info_from_url,
       save_fixture,
       generate_assertion_yaml,
   )
   ```

3. **Support JSON output for core scripts:**
   ```python
   parser.add_argument(
       "--output-format",
       choices=["human", "json"],
       default="human",
       help="Output format (default: human)",
   )

   if args.output_format == "human":
       print("Human-readable output")

   # Always output final JSON
   if args.output_format == "json":
       print(json.dumps(result))
   else:
       print("\n=== JSON Output ===")
       print(json.dumps(result, indent=2))
   ```

4. **Add proper documentation:**
   - Script docstring with description and usage
   - Helpful --help text
   - Examples in docstring

### Adding New Claude Skills

1. Create `.claude/skills/skill-name.md`
2. Include frontmatter with name and description
3. Document usage, implementation, and workflow
4. Add to `.claude/settings.local.json` permissions
5. Ensure underlying script supports `--output-format json`

See `.claude/skills/README.md` for detailed guide.

## Testing

Scripts use the shared test infrastructure:

```bash
# Test debug fixtures
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v

# Test listing extraction
uv run pytest tests/job_scrape_application/workflows/test_listing_extraction_e2e.py -v

# Test job detail extraction
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py -v
```

## Support

- **Documentation**: See `CLAUDE.md` and `DEBUGGING.md`
- **Claude Skills**: See `.claude/skills/README.md`
- **Issues**: Report bugs via GitHub issues
