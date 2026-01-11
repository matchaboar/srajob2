# AGENTS.md

## Scope and repo layout
- This file applies repo-wide unless a deeper `AGENTS.md` overrides it.
- `job_scrape_application/` is the Python scraping + DBOS workflow code.
- `job_board_application/` is the Vite + React UI + Convex backend.
- `agent_scripts/` contains trusted helper scripts for fixtures and debugging.
- Additional Convex rules live in `job_board_application/AGENTS.md`.

## Key directories
- `job_scrape_application/workflows/` DBOS workflows and orchestration.
- `job_scrape_application/workflows/site_handlers/` per-site scraping handlers.
- `job_scrape_application/services/` integrations (Convex, telemetry, etc.).
- `job_scrape_application/components/` shared models/utilities.
- `job_board_application/src/` UI source.
- `job_board_application/convex/` Convex schema + functions.
- `tests/` pytest suites and fixtures.

## Global tooling rules (must follow)
- Use `pwsh` for PowerShell (PowerShell 7).
- Do not run Python directly. Use `uv run <script.py>`.
- For non-repo Python modules, use `uvx <module>`.
- If you edit any Python file, run `uvx ruff check` and fix issues.
- Long-running commands (e.g., `npm run test`) must use a 100s timeout.
- Infinite-running commands must use a 100s timeout.
- Do not use `docker`; use `podman` instead.
- Do not use `docker-compose`; use `podman-compose` instead.

## Build, lint, and test commands

### Python (repo root)
- Install deps: `uv sync` (from repo root).
- Run all tests: `uv run pytest`.
- Run a single test file: `uv run pytest tests/test_file.py`.
- Run a single test case: `uv run pytest tests/test_file.py::TestClass::test_name`.
- Run tests by keyword: `uv run pytest -k "pattern"`.
- Lint all Python: `uvx ruff check .`.
- Lint a file: `uvx ruff check path/to/file.py`.
- Type check (optional): `uvx pyright`.

### Job board UI + Convex (`job_board_application/`)
- Install deps: `pnpm install`.
- Dev (frontend + backend): `pnpm run dev`.
- Dev backend only: `pnpm run dev:backend`.
- Dev frontend only: `pnpm run dev:frontend`.
- Build UI: `pnpm run build`.
- Lint + typecheck + Convex build: `pnpm run lint`.
- Run tests (Vitest): `pnpm run test`.
- Run a single test file: `pnpm run test -- src/path/to/test.tsx`.
- Run a single test by name: `pnpm run test -- -t "test name"`.

### PowerShell scripts
- Worker entrypoint: `./start_worker.ps1` (use `pwsh` if needed).
- Force full scrape (prod optional): `./start_worker.ps1 -ForceScrapeAll -UseProd`.

## Data, services, and Convex notes
- Convex database lives in `job_board_application/convex`.
- Convex has two domains:
  - `.convex.cloud` for client SDK connections.
  - `.convex.site` for HTTP routes (`httpRouter`, `httpAction`).
- When configuring external POSTs to Convex, always use `.convex.site`.
- Use `npx convex run` without `--args` (it errors).
- From `job_board_application/`, example: `npx convex run --prod router:runSiteNow '{"id":"..."}'`.
- Schedule source of truth lives in `job_scrape_application/config/<env>/site_schedules.yml`.
- Export schedules (requires Convex URL):
  - Dev: `PYTHONPATH=. uv run agent_scripts/export_site_schedules.py --env dev`.
  - Prod: `PYTHONPATH=. uv run agent_scripts/export_site_schedules.py --env prod`.

## Convex prod data debugging
- Always run from `job_board_application` so `npx convex run` can find `package.json`.
- Use `.env.production` for prod keys (do not print or copy keys into logs).
- Prefer `npx convex deploy` when you need immediate prod DB updates.

## Secrets and environments
- Do not commit secrets or API keys.
- Avoid pasting credentials into logs or error messages.
- Prefer env vars over hardcoded URLs or tokens.

## Workflow/runtime guidelines
- Temporal workflows were refactored to DBOS; use DBOS workflows (not Temporal).
- Scrape workflow logic lives under `job_scrape_application/workflows/`.
- Store scraped data in the Convex DB (`job_board_application`).

## Agent scripts and fixtures
- Use `agent_scripts/` for fixture refreshes and debugging.
- Prefer agent scripts + SpiderCloud SDK/API for website content (no `curl`).
- Validate Bloomberg Avature live pages before fixture/workflow updates:
  - `uv run agent_scripts/validate_bloomberg_avature_live.py`.
- Standard fixture refresh: `uv run agent_scripts/fetch_spidercloud_fixtures.py`.
- SpiderCloud debugging: `uv run agent_scripts/dump_spidercloud_response.py`.
- Diagnose stalls/latency:
  - `uv run agent_scripts/diagnose_spidercloud_stalls.py`.
  - `uv run agent_scripts/measure_spidercloud_batch.py`.
- Export company jobs fixture:
  - `uv run agent_scripts/export_company_jobs_fixture.py --company "GitHub" --env prod`.

## Code style guidelines

### General
- Follow existing patterns in each directory first.
- Keep changes minimal and focused; avoid unrelated refactors.
- Prefer descriptive names over abbreviations.
- Add context to errors (include IDs, URLs, and key inputs).

### Python (`job_scrape_application/` and `agent_scripts/`)
- Use `from __future__ import annotations` in new modules.
- Use type hints everywhere; prefer `dict[str, Any]` over bare `dict`.
- Use snake_case for functions/vars, PascalCase for classes, UPPER_SNAKE for constants.
- Favor dataclasses or typed models when passing structured payloads.
- Group imports: standard library, third-party, local.
- Prefer `f"..."` formatting for logs and errors.
- Avoid bare `except`; catch specific exceptions.
- Use `logging` helpers already present (e.g., workflow loggers) instead of `print`.
- Handle `None` explicitly; avoid implicit falsey checks when type matters.
- Line length: 100 (matches Ruff config).

### Logging and telemetry
- Use workflow logger helpers instead of `print`.
- Include site IDs, workflow names, and URLs in logs.
- Prefer structured data payloads over string concatenation.

### TypeScript/React (`job_board_application/`)
- Use TypeScript strictness; avoid `any` unless unavoidable.
- Keep React components functional and hook-based.
- Respect ESLint config (React Hooks + TypeScript rules).
- Use named exports for shared utilities.
- Follow `@/` path aliases defined in `tsconfig.app.json`.
- Keep JSX readable; split long prop lists across lines.
- Keep hooks ordered and avoid conditional hooks.
- Prefer `type` imports for pure types when it improves clarity.
- Use 2-space indentation and trailing commas to match existing files.
- Group imports: external modules, internal aliases, relative paths.

### Convex (inside `job_board_application/`)
- Follow `job_board_application/AGENTS.md` for Convex specifics.
- Always include argument and return validators for Convex functions.
- Use `internalQuery`, `internalMutation`, `internalAction` for private APIs.
- Use `v.null()` for null returns (Convex does not allow `undefined`).

### Error handling
- Raise or rethrow with actionable messages; include identifiers.
- Prefer explicit error types where available (workflow/domain errors).
- Avoid swallowing exceptions; log and rethrow where failures matter.
- When returning error states, prefer structured dicts/objects over strings.

## Testing guidance
- Keep tests deterministic; use fixtures under `tests/fixtures/`.
- Add tests alongside existing suites in `tests/` when changing behavior.
- For new scraping behavior, update fixtures and corresponding tests together.
- For workflow changes, update tests in `tests/job_scrape_application/`.
