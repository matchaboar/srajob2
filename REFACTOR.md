# Temporal to DBOS Workflow Refactor Plan

## Goals
- Replace Temporal workflows with DBOS workflows backed by DBOS built-in SQLite storage.
- Simplify workflow queueing by using DBOS queues instead of Temporal workers + Convex queue tables.
- Preserve current scrape and webhook behavior while simplifying runtime ops.
- Increase modularity by isolating orchestration, activity logic, and storage adapters.

## In-Scope Workflow Replacements
- Scrape orchestration: `job_scrape_application/workflows/scrape_workflow.py`
- Heuristic job details loop: `job_scrape_application/workflows/heuristic_workflow.py`
- Webhook and lease flows: `job_scrape_application/workflows/webhook_workflow.py`
- Greenhouse workflow: `job_scrape_application/workflows/greenhouse_workflow.py`
- Worker bootstrap + routing: `job_scrape_application/workflows/worker.py`

## Proposed DBOS Architecture
- **dbos_runtime/**: DBOS app entrypoint, workflow registrations, SQLite configuration.
- **workflows/**: DBOS workflow definitions mirroring current Temporal workflow boundaries.
- **queues/**: DBOS queue definitions for job listings vs. job detail URLs, including dedupe policies.
- **activities/**: Pure Python activity modules (IO, scraping, Convex calls, Spidercloud), reusing current implementations from `job_scrape_application/workflows/activities/__init__.py`.
- **adapters/**: Storage/queue adapters to isolate DBOS-specific access patterns and preserve current Convex payload shapes.
- **models/**: Typed payloads/dataclasses for workflow inputs/outputs (e.g., site leases, scrape summaries).

## Queue Design
- **Job detail queue**: DBOS queue keyed by normalized job URL (plus site/provider) with dedupe on successful completion.
- **Job listing queue**: DBOS queue keyed by site + listing URL; entries are scheduled on a cadence (DBOS cron or Convex schedule triggers).
- **Deduplication policy**: persist a “completed job detail URL” table in SQLite; enqueue skips when a successful completion exists unless a forced refresh flag is set.
- **Schedule flow (step-by-step)**: 1) Convex retains cadence definitions and fires schedule triggers, 2) trigger handler calls a DBOS enqueue API for the listing queue with site + listing URL, 3) DBOS cron/queue worker picks up the job and runs the listing workflow, 4) listing workflow emits job detail URLs into the job detail queue with dedupe checks, 5) DBOS records completion + next scheduled run metadata in SQLite for reporting.

## Scheduling API + Sequence
- **DBOS enqueue endpoint**: `POST /api/workflows/enqueue-listing` (Convex HTTP action) accepts `{ siteId, listingUrl, cadence, source }` and enqueues into DBOS listing queue.
- **DBOS job-detail enqueue**: `POST /api/workflows/enqueue-detail` or internal helper `enqueue_job_detail(url, siteId, provider)` adds dedupe key before queue insert.
- **Status endpoint**: `GET /api/workflows/status` returns listing queue depth, job detail depth, and last schedule run per site.
- **Naming rationale**: use `/api/workflows/*` to avoid Temporal naming while keeping a compact namespace; if minimizing UI change is critical, alias these under existing `/api/temporal/*` routes during transition.
- **Sequence**: 1) Convex schedule fires, 2) Convex HTTP action calls DBOS enqueue endpoint, 3) DBOS worker pulls listing job, 4) listing workflow runs and enqueues job detail URLs, 5) DBOS job detail workflow scrapes and writes completion state, 6) Convex UI reads DBOS status endpoint for admin views.

## Best Practices + Performance Targets
- **API design**: stable `/api/workflows/*` endpoints with temporary aliases to `/api/temporal/*` for backwards compatibility.
- **Idempotency**: dedupe keys on job detail URLs + explicit “completed” records so retries are safe and fast.
- **Batching**: process listing results in bounded batches and stream job detail enqueues to keep queue latency low.
- **Worker utilization**: configure DBOS queue concurrency per queue type (listing vs. detail) to keep job detail workers saturated.
- **Deadlock avoidance**: keep DBOS transactions short (enqueue + update status only) and move network IO outside of SQLite transaction scopes.
- **Low-latency writes**: post job detail results to Convex immediately on success; avoid multi-step aggregation before write.
- **Observability**: per-queue latency/throughput metrics for detecting backlogs and worker stalls quickly.
- **Job descriptions**: store long job_description bodies in Convex file storage and fetch lazily per-row in the UI.

## Job Description Storage + UI Loading
- **Write path**: job detail workflow uploads description text to Convex file storage, stores file reference on the job record.
- **Read path**: UI displays summary fields only; on job row click, fetch the file contents and render description on-demand.
- **Schema update**: add `descriptionFileId` (or similar) to job records; avoid storing full text in primary tables.

## Verification
- **Storage**: confirm job detail runs write file blobs and only file IDs in job tables.
- **UI**: validate that list view loads without description payloads and fetches on click.
- **Performance**: measure queue latency from detail enqueue to Convex write for P50/P95.
- **Deduplication**: assert repeated job detail URLs do not enqueue if already completed unless forced refresh is set.

## Implementation Plan
1. **Inventory + mapping**
   - Enumerate each Temporal workflow and the activities it calls, starting with `WORKFLOW_CLASSES` in `job_scrape_application/workflows/worker.py`.
   - Map each workflow to a DBOS workflow entry with equivalent inputs/outputs and retry semantics.

2. **DBOS runtime scaffold**
   - Add a DBOS entrypoint that initializes SQLite, registers workflows, and defines queue + cron equivalents.
   - Mirror existing runtime config values in `job_scrape_application/config/runtime_config.py` with DBOS worker settings.

3. **Queue simplification + dedupe**
   - Replace Convex-backed queue tables and Temporal task queues with DBOS queues for job listings and job details.
   - Add deduplication keys for job detail URLs to prevent re-scraping after a successful completion.
   - Keep job listing URLs scheduled via DBOS cron (or Convex-declared schedules that enqueue DBOS jobs).

4. **Modular activity extraction**
   - Split `job_scrape_application/workflows/activities/__init__.py` into smaller modules (lease, scrape, webhook, spidercloud, firecrawl).
   - Keep activity signatures stable so workflow logic can be ported with minimal changes.

5. **Workflow ports**
   - Port `ScrapeWorkflow` and provider-specific workflows first (highest traffic path).
   - Port webhook workflows and the heuristic job-details loop second.
   - Replace Temporal-specific helpers (`workflow.now`, retry policies, timers) with DBOS equivalents.

6. **State + logging cleanup**
   - Replace Temporal run metadata recording with DBOS-native run tracking in SQLite.
   - Keep existing logging formats from `helpers/workflow_logging.py` and `helpers/workflow_debug.py` but remove Temporal interceptors.

7. **Runtime integration**
   - Update `start_worker.ps1` to launch DBOS workers instead of Temporal workers.
   - Remove temporal-specific env vars and config defaults once DBOS paths are stable.

8. **Convex + API updates**
   - Update Convex HTTP endpoints in `job_board_application/convex/router.ts` to read DBOS workflow run metadata instead of Temporal tables.
   - Remove Temporal status tables from `job_board_application/convex/schema.ts` after DBOS metrics are stored.

9. **Validation + rollout**
   - Build parity checks for scrape counts and webhook completion by comparing old vs. new runs.
   - Stage with feature flags to allow fallback to Temporal during early rollout.

## Risks / Dependencies
- DBOS retry/backoff behavior must match Temporal semantics for idempotent activities.
- Long-running batches (e.g., Spidercloud listing/job workflows) need explicit chunking to avoid oversized transactions in SQLite.
- Convex telemetry endpoints must be updated before UI changes in `job_board_application/src/AdminPage.tsx`.
