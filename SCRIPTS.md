# Scripts

- **Start local Temporal worker + scheduling**
  ```pwsh
  ./start_worker.ps1
  ```
  Runs Temporalite (via Podman/Docker fallback), ensures schedules exist, then launches the Python worker against the configured Convex deployment.

- **Deploy the web app to Netlify**
  ```pwsh
  pwsh ./scripts/deploy-netlify.ps1 -SiteId "<site>" -AuthToken "<token>"
  ```
  Builds `job_board_application` (Vite) and deploys `dist/` to Netlify; expects `NETLIFY_SITE_ID` and `NETLIFY_AUTH_TOKEN` via args or env.

- **Run Convex migrations (backfill schema changes)**
  ```bash
  cd job_board_application && npx convex run migrations:runAll
  ```
  Executes all defined migrations (including scrape metadata backfill) against the configured Convex deployment. Use after pulling schema changes to keep stored data in sync.


## Tests

- **PowerShell unit tests (Pester) for start_worker.ps1**
  ```pwsh
  pwsh -NoLogo -Command "Invoke-Pester -Script ./tests/start_worker.Tests.ps1 -CI"
  ```
  Executes the Pester suite with Pester 5 syntax (`-Script`) so you avoid the legacy warning; `-CI` fails on any test error (useful for CI/local checks).

## Setup

- **Create a fresh .env from template**
  ```pwsh
  pwsh ./scripts/init-env.ps1
  ```
  Copies `.env.example` to `.env` if missing, reminding you to fill in secrets.

## Utilities

- **Run helper tasks (Temporal up/down, health checks)**
  ```pwsh
  pwsh ./scripts/run-tests.ps1 <task> [-RunCheck] [-TimeoutSeconds 300] [...]
  ```
  - `temporal:start` / `temporal:stop`: manage the local Temporal dev stack (Podman).
  - `hc:ephemeral`: run the Temporal health check workflow against the ephemeral dev stack.
  - `hc:real`: health check against a live Temporal endpoint (requires `CONVEX_HTTP_URL`, optional `TEMPORAL_ADDRESS`/`TEMPORAL_NAMESPACE`).
  - `hc:manual`: invoke the manual test workflow.
  Optional switches let you extend timeouts or run extra checks.