# AGENTS.md

## Repository Overview

Job scraping and board application:
- `job_scrape_application/` - Python DBOS workflows for scraping via SpiderCloud
- `job_board_application/` - Vite + React UI with Convex backend
- `agent_scripts/` - Helper scripts for fixtures and debugging
- `tests/` - pytest suites and fixtures

## Tooling Rules (Required)

| Instead of | Use |
|------------|-----|
| `grep` | `rg` |
| `python` / `python3` | `uv run` |
| `docker` | `podman` |
| `docker-compose` | `podman-compose` |

- Long/infinite-running commands: use 100s timeout
- After editing Python files: run `uvx ruff check`
- Use `--lf` with pytest to re-run only failing tests

## Commands

### Python (repo root)
```bash
uv sync                                         # Install deps
uv run pytest                                   # Run all tests
uv run pytest tests/path/test_file.py           # Run specific test
uv run pytest -k "pattern"                      # Run by pattern
uv run pytest --lf                              # Re-run last failed
uvx ruff check .                                # Lint
uvx pyright                                     # Type check (optional)
```

### Job Board UI (`job_board_application/`)
```bash
pnpm install                                    # Install deps
pnpm run dev                                    # Dev frontend + backend
pnpm run build                                  # Build UI
pnpm run lint                                   # Lint + typecheck
pnpm run test                                   # Run Vitest tests
```

### Convex (`job_board_application/`)
```bash
npx convex run --prod router:runSiteNow '{"id":"..."}'  # Trigger site scrape
npx convex deploy                               # Deploy to prod
```

Note: Use `.convex.site` for HTTP routes, `.convex.cloud` for SDK connections.

## Claude Skills

Use these for common debugging workflows:

| Skill | Use Case |
|-------|----------|
| `/fix-job-extraction <job-url>` | Debug job detail extraction issues |
| `/fix-job-crawl <site-or-url>` | Debug listing page URL discovery |
| `/add-site <job-url>` | Add new company to scraper |
| `/refresh-fixtures --schedule-env prod` | Bulk regenerate fixtures |

See `.claude/skills/README.md` and `agent_scripts/README.md` for details.

## Testing Job Extraction

**Locations:**
- E2E tests: `tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py`
- Fixtures: `tests/job_scrape_application/workflows/fixtures/dbos_schedule/{site}_detail.json`
- Assertions: `tests/job_scrape_application/workflows/assertions/{site}.yml`
- Debug fixtures: `tests/job_scrape_application/workflows/fixtures/debug/`

**Run tests:**
```bash
uv run pytest tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy -v
uv run pytest "...[purestorage]" -v   # Specific site
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v  # Debug fixtures
```

**Regenerate fixtures:**
```bash
uv run python agent_scripts/core/fetch_spidercloud_fixtures.py --schedule-env prod --schedule-only SITE_NAME
```

**Assertion types:** `title`, `title_contains`, `company`, `location`, `is_remote`, `level`, `description_min_words`, `cost_milli_cents_min`, `posted_at_not_null`

## Key Directories

| Directory | Purpose |
|-----------|---------|
| `job_scrape_application/workflows/` | DBOS workflows |
| `job_scrape_application/workflows/site_handlers/` | Per-site handlers (greenhouse, ashbyhq, workday, etc.) |
| `job_scrape_application/services/` | Integrations (Convex, telemetry) |
| `job_scrape_application/config/<env>/site_schedules.yml` | Schedule source of truth |
| `job_board_application/convex/` | Convex schema + functions |

## Code Style

### Python
- Use `from __future__ import annotations`
- Type hints everywhere (`dict[str, Any]` not bare `dict`)
- snake_case functions/vars, PascalCase classes, UPPER_SNAKE constants
- Use logging helpers, not `print`
- Line length: 100 (Ruff config)
- Group imports: stdlib, third-party, local

### TypeScript/React
- Strict TypeScript, avoid `any`
- Functional components with hooks
- Use `@/` path aliases
- 2-space indent, trailing commas

### Convex
- See `job_board_application/AGENTS.md` for specifics
- Include arg/return validators
- Use `internalQuery/Mutation/Action` for private APIs
- Use `v.null()` for null returns (not undefined)

## Agent Scripts

```bash
uv run agent_scripts/core/dump_spidercloud_response.py <url> --out <path>  # Debug SpiderCloud
uv run agent_scripts/diagnose_spidercloud_stalls.py      # Diagnose stalls
uv run agent_scripts/export_company_jobs_fixture.py --company "GitHub" --env prod
```

## Secrets
- Never commit secrets or API keys
- Use env vars, not hardcoded values
- Prod keys in `.env.production`
