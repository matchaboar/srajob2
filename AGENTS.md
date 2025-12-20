# Rules
- Powershell is safely aliased to `pwsh` and is on PowerShell 7.
- If you want to run python command on a module not from this repo, just use `uvx modulename`
- If you make any edits to a python file, you must  lint it and fix any linting errors `uvx ruff check`.
- Any infinite time running command should have a 45 second timeout.
- Long-running commands like `npm run test` should have a 45 second timeout.
- Do not use `docker`, use `podman` instead.
- Do not use `docker-compose`, use `podman-compose` instead.

# Python
- Python packages, use `uv` and not python/pip commands. Example: `uv run` or `uv add`. 
- DO NOT `uv pip`

# Frontend Code Structure
- The UI is in job_board_application.

# Data & Storage
- The database is in job_board_application
- The database is convex and its configuration is in job_board_application/convex
- **IMPORTANT**: Convex has TWO different URL domains:
  - `.convex.cloud` - Used for Convex client SDK connections (e.g., `ConvexReactClient`)
  - `.convex.site` - Used for HTTP routes (e.g., `httpRouter`, `httpAction`)
  - When configuring external services (like Temporal workers) to POST to Convex HTTP endpoints, ALWAYS use the `.convex.site` domain
  - Example: `CONVEX_HTTP_URL=https://elegant-magpie-239.convex.site` (NOT `.convex.cloud`)

# MCP usage
- The Convex MCP server here only exposes tools (status/tables/run/etc.) and does **not** implement `resources/list`; expect “method not found” if you call `list_mcp_resources/templates`. Use the `mcp__convex__*` functions instead.

# Job Scrape Application
- The scrape workflow logic is in job_scrape_application
- This should use temporal for workflows
- Data should be stored in the convex database in job_board_application
