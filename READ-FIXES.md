# Targeted fixes to reduce Convex read volume (workers)

## 1) Stop schedule audit from running in every worker
- **Problem:** `schedule_audit_logger()` runs in every worker process and reads all `sites` + all `scrape_schedules` every 30 minutes; `listSchedules` itself also scans all `sites` to compute `siteCount`.
- **Fix:** Gate the audit loop to a single worker (e.g., only run when `SCRAPE_WORKER_ROLE=audit`, or when `SCRAPE_WORKER_ID` matches a leader election key).
- **Files:**
  - `job_scrape_application/workflows/worker.py`
  - `job_scrape_application/workflows/schedule_audit.py`
  - `job_board_application/convex/router.ts` (`listSchedules`)

## 2) Avoid full-table scan in `router:leaseSite`
- **Problem:** `leaseSite` does `ctx.db.query("sites").withIndex("by_enabled").collect()` and filters in memory on every lease. This is a hot path.
- **Fix options:**
  - Add an index that supports schedule/lock/failed filtering so you can do narrower queries.
  - Split into two queries: one for scheduled sites (with `scheduleId` index) and one for unscheduled, then take the earliest eligible without scanning all.
  - Cache eligible site IDs in a separate queue table updated by cron or mutations (schedule changes, run completion) and lease from that queue instead of scanning.
- **Files:**
  - `job_board_application/convex/schema.ts` (indexes)
  - `job_board_application/convex/router.ts` (`leaseSite`)

## 3) Cap/segment `listSeenJobUrlsForSite`
- **Problem:** `listSeenJobUrlsForSite` collects *all* rows for a `sourceUrl` and returns the full list. As `seen_job_urls` grows, each scrape loads more.
- **Fix options:**
  - Add `limit` and `recentBefore` args; return only a capped slice (e.g., last N URLs).
  - Store a rolling “seen bloom” per site or a `seen_job_url_buckets` table keyed by day/week to limit reads.
  - Use `pattern` to scope results; right now `pattern` is ignored.
- **Files:**
  - `job_board_application/convex/router.ts` (`listSeenJobUrlsForSite`)
  - `job_board_application/convex/schema.ts` (`seen_job_urls` indexes)
  - Call sites in `job_scrape_application/workflows/helpers/scrape_utils.py` and `job_scrape_application/workflows/scrapers/*.py`

## 4) Batch `findExistingJobUrls`
- **Problem:** `findExistingJobUrls` loops per URL and does a `jobs` index lookup per candidate. Large URL lists -> lots of reads.
- **Fix options:**
  - Normalize candidate URLs client-side and pass a de-duplicated list.
  - Add a `jobs_by_url_hash` table or index to allow multi-get style lookups (store normalized URL + hash, query by hash bucket).
  - Introduce a `jobs_url_lookup` table keyed by normalized URL for faster bulk resolution.
- **Files:**
  - `job_board_application/convex/router.ts` (`findExistingJobUrls`)
  - URL normalization helpers used in the worker before calling the query.

## 5) Reduce `listSchedules` read amplification
- **Problem:** `listSchedules` loads all schedules and *all* sites to compute `siteCount`, even when a worker only needs schedule definitions.
- **Fix:** Split into two queries: one that returns schedules only, and another (admin-only/UI) query that computes counts as needed.
- **Files:**
  - `job_board_application/convex/router.ts` (`listSchedules`)
  - `job_scrape_application/workflows/schedule_audit.py` (use the lighter query)

## 6) Add lightweight read telemetry (optional but useful)
- **Problem:** Hard to attribute read volume to specific queries in workers.
- **Fix:** Wrap `convex_query` calls in workers with timing + result size logging (not full payloads). Push to PostHog/logs to spot hotspots by function name.
- **Files:**
  - `job_scrape_application/services/convex_client.py`
