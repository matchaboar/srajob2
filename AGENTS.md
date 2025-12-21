# Rules
- Powershell is safely aliased to `pwsh` and is on PowerShell 7.
- Do not run python directly. Use `uv run x.py`
- If you want to run python command on a module not from this repo, just use `uvx modulename`
- If you make any edits to a python file, you must  lint it and fix any linting errors `uvx ruff check`.
- Any infinite time running command should have a 45 second timeout.
- Long-running commands like `npm run test` should have a 45 second timeout.
- Do not use `docker`, use `podman` instead.
- Do not use `docker-compose`, use `podman-compose` instead.

## Getting test fixtures
- As an agent, you may use my keys and scripts at any time in order to write tests or debug code: `./agent_scripts`
- You may also write useful scripts or edit them as needed in this folder for future use.
- Do use the agent_scripts and spidercloud sdk/api whenever a website contents are needed. DO NOT use curl.
- Use `agent_scripts/validate_bloomberg_avature_live.py` to validate live Bloomberg Avature search pages against the Avature handler (job detail + pagination link extraction) before updating fixtures or workflow logic.

# Exporting site schedule configs
- The schedule sync source-of-truth lives in `job_scrape_application/config/<env>/site_schedules.yml`.
- To export current Convex schedules into those YAML files (requires `CONVEX_URL` or `CONVEX_HTTP_URL`):
  - Dev: `PYTHONPATH=. uv run agent_scripts/export_site_schedules.py --env dev`
  - Prod: `PYTHONPATH=. uv run agent_scripts/export_site_schedules.py --env prod`

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

# Convex and MCP usage
- `convex run` does not need `--args` and that parameter will error, so don't use it. Example: `npx convex run --prod router:runSiteNow '{"id":"kd787xgmvw74bkfqhrmp5he4ed7xnaqy"}'`
- The Convex MCP server here only exposes tools (status/tables/run/etc.) and does **not** implement `resources/list`; expect “method not found” if you call `list_mcp_resources/templates`. Use the `mcp__convex__*` functions instead.

# Convex prod data debugging (read-only)
- Always run from `job_board_application` so `npx convex run` can find `package.json`.
- Use `.env.production` for prod keys (do not print or copy keys into logs).
- Recommended baseline steps and commands (examples include Lambda):

```bash
# 1) Load prod env + confirm the site exists (copy out _id for queue lookups)
cd job_board_application
set -a; source .env.production; set +a
npx convex run --prod router:listSites '{"enabledOnly": false}' \
  | node -e "const fs=require('fs');const data=JSON.parse(fs.readFileSync(0,'utf8')); \
  const site=data.find(s=>s.url==='https://jobs.ashbyhq.com/lambda'); \
  console.log(site?JSON.stringify({ _id:site._id,url:site.url,pattern:site.pattern,enabled:site.enabled,type:site.type,scrapeProvider:site.scrapeProvider,completed:site.completed,failed:site.failed,lastRunAt:site.lastRunAt,lastFailureAt:site.lastFailureAt,lockExpiresAt:site.lockExpiresAt,lockedBy:site.lockedBy,manualTriggerAt:site.manualTriggerAt,scheduleId:site.scheduleId }):'SITE_NOT_FOUND');"

# 2) Scrape URL queue status summary for a siteId
npx convex run --prod router:listQueuedScrapeUrls '{"siteId":"<SITE_ID>","limit":500}' \
  | node -e "const fs=require('fs');const rows=JSON.parse(fs.readFileSync(0,'utf8')); \
  const byStatus={};for(const r of rows){const s=r.status||'unknown';(byStatus[s]??=[]).push(r);} \
  const summarize=(list)=>{if(!list||!list.length)return null;const created=list.map(r=>r.createdAt||0); \
  const updated=list.map(r=>r.updatedAt||0);const attempts=list.map(r=>r.attempts||0); \
  return {count:list.length,createdAtMin:Math.min(...created),createdAtMax:Math.max(...created), \
  updatedAtMin:Math.min(...updated),updatedAtMax:Math.max(...updated),maxAttempts:Math.max(...attempts), \
  sample:list.slice(0,5).map(r=>({url:r.url,status:r.status,attempts:r.attempts,lastError:r.lastError}))};}; \
  const out={total:rows.length};for(const [status,list] of Object.entries(byStatus)){out[status]=summarize(list);} \
  console.log(JSON.stringify(out));"

# 3) Active Temporal workers + task queues (verify job-details workers are alive)
npx convex run --prod temporal:getActiveWorkers '{}' \
  | node -e "const fs=require('fs');const data=JSON.parse(fs.readFileSync(0,'utf8')); \
  console.log(JSON.stringify({count:data.length,taskQueues:[...new Set(data.map(w=>w.taskQueue))]}));"

# 4) Recent workflow runs for a site URL (listing vs job-details workflows)
npx convex run --prod temporal:listWorkflowRunsByUrl '{"url":"https://jobs.ashbyhq.com/lambda","limit":5}' \
  | node -e "const fs=require('fs');const data=JSON.parse(fs.readFileSync(0,'utf8')); \
  console.log(JSON.stringify(data.map(r=>({workflowName:r.workflowName,status:r.status,startedAt:r.startedAt, \
  completedAt:r.completedAt,sitesProcessed:r.sitesProcessed,jobsScraped:r.jobsScraped,taskQueue:r.taskQueue,error:r.error}))));"

# 5) Recent scrape errors (if queue rows are failing)
npx convex run --prod router:listScrapeErrors '{"limit":50}'
```

- Interpreting results:
  - `scrape_url_queue` stuck at all `pending` + `attempts=0` usually means no job-details workers are running.
  - A growing `processing` set without completion usually means workers are running but failing to store/complete.
  - `workflow_runs.jobsScraped` counts scrape records, not job count.

# Job Scrape Application
- The scrape workflow logic is in job_scrape_application
- This should use temporal for workflows
- Data should be stored in the convex database in job_board_application
