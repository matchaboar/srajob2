#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from job_scrape_application.services import convex_query  # noqa: E402

try:
    from spider import AsyncSpider  # type: ignore
except Exception:
    AsyncSpider = None  # type: ignore[assignment]


DEFAULT_RUNTIME = {
    "spidercloud_job_details_timeout_minutes": 15,
    "spidercloud_job_details_batch_size": 50,
    "spidercloud_job_details_concurrency": 4,
    "spidercloud_job_details_processing_expire_minutes": 20,
    "spidercloud_http_timeout_seconds": 900,
}


@dataclass
class RuntimeConfig:
    spidercloud_job_details_timeout_minutes: int
    spidercloud_job_details_batch_size: int
    spidercloud_job_details_concurrency: int
    spidercloud_job_details_processing_expire_minutes: int
    spidercloud_http_timeout_seconds: int


def _load_runtime_config(env: str) -> RuntimeConfig:
    path = REPO_ROOT / "job_scrape_application" / "config" / env / "runtime.yaml"
    data: Dict[str, Any] = {}
    if path.exists():
        try:
            raw = yaml.safe_load(path.read_text()) or {}
            if isinstance(raw, dict):
                data = raw
        except Exception:
            data = {}
    return RuntimeConfig(
        spidercloud_job_details_timeout_minutes=int(
            data.get(
                "spidercloud_job_details_timeout_minutes",
                DEFAULT_RUNTIME["spidercloud_job_details_timeout_minutes"],
            )
        ),
        spidercloud_job_details_batch_size=int(
            data.get(
                "spidercloud_job_details_batch_size",
                DEFAULT_RUNTIME["spidercloud_job_details_batch_size"],
            )
        ),
        spidercloud_job_details_concurrency=int(
            data.get(
                "spidercloud_job_details_concurrency",
                DEFAULT_RUNTIME["spidercloud_job_details_concurrency"],
            )
        ),
        spidercloud_job_details_processing_expire_minutes=int(
            data.get(
                "spidercloud_job_details_processing_expire_minutes",
                DEFAULT_RUNTIME["spidercloud_job_details_processing_expire_minutes"],
            )
        ),
        spidercloud_http_timeout_seconds=int(
            data.get(
                "spidercloud_http_timeout_seconds",
                DEFAULT_RUNTIME["spidercloud_http_timeout_seconds"],
            )
        ),
    )


def _fmt_dt(ms: Optional[int]) -> str:
    if not ms:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000))


def _minutes_ago(ms: Optional[int], now_ms: int) -> Optional[int]:
    if not ms:
        return None
    return int((now_ms - ms) / 60000)


def _zoned_parts(now_ms: int, tz_name: str) -> Dict[str, int | str]:
    dt_aware = __import__("datetime").datetime.fromtimestamp(now_ms / 1000, ZoneInfo(tz_name))
    return {
        "year": dt_aware.year,
        "month": dt_aware.month,
        "day": dt_aware.day,
        "hour": dt_aware.hour,
        "minute": dt_aware.minute,
        "second": dt_aware.second,
        "weekday": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"][dt_aware.weekday()],
    }


def _latest_eligible_time(schedule: Dict[str, Any] | None, now_ms: int) -> Optional[int]:
    if not schedule:
        return None

    time_zone = schedule.get("timezone") or "America/Denver"
    parts = _zoned_parts(now_ms, time_zone)
    day_key = parts["weekday"]
    if day_key not in schedule.get("days", []):
        return None

    def _parse_hhmm(val: str) -> int:
        try:
            h, m = val.split(":")
            return int(h) * 60 + int(m)
        except Exception:
            return 0

    minutes_now = int(parts["hour"]) * 60 + int(parts["minute"])
    start_minutes = _parse_hhmm(str(schedule.get("startTime") or "00:00"))
    if minutes_now < start_minutes:
        return None

    interval = max(1, int(schedule.get("intervalMinutes") or 24 * 60))
    steps = (minutes_now - start_minutes) // interval
    minutes_at_slot = start_minutes + steps * interval

    import datetime as _dt

    day_start = _dt.datetime(
        int(parts["year"]),
        int(parts["month"]),
        int(parts["day"]),
        tzinfo=ZoneInfo(time_zone),
    ).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(day_start.timestamp() * 1000) + minutes_at_slot * 60 * 1000


async def _list_queue(status: Optional[str], provider: Optional[str], limit: int) -> List[Dict[str, Any]]:
    args: Dict[str, Any] = {"limit": limit}
    if status:
        args["status"] = status
    if provider:
        args["provider"] = provider
    rows = await convex_query("router:listQueuedScrapeUrls", args)
    return rows or []


def _summarize_queue(rows: List[Dict[str, Any]], now_ms: int, expire_minutes: int) -> Dict[str, Any]:
    if not rows:
        return {"count": 0}
    created = [int(r.get("createdAt") or 0) for r in rows]
    updated = [int(r.get("updatedAt") or 0) for r in rows]
    attempts = [int(r.get("attempts") or 0) for r in rows]
    by_source = Counter([str(r.get("sourceUrl") or "") for r in rows])
    stale_cutoff = now_ms - expire_minutes * 60 * 1000
    stale = [r for r in rows if int(r.get("updatedAt") or 0) and int(r.get("updatedAt") or 0) < stale_cutoff]
    return {
        "count": len(rows),
        "createdAtMin": min(created) if created else None,
        "createdAtMax": max(created) if created else None,
        "updatedAtMin": min(updated) if updated else None,
        "updatedAtMax": max(updated) if updated else None,
        "maxAttempts": max(attempts) if attempts else 0,
        "staleCount": len(stale),
        "topSources": by_source.most_common(5),
        "sample": [
            {
                "url": r.get("url"),
                "status": r.get("status"),
                "attempts": r.get("attempts"),
                "updatedAt": r.get("updatedAt"),
                "lastError": r.get("lastError"),
            }
            for r in rows[:5]
        ],
    }


async def _gather_site_schedule_summary(now_ms: int) -> Dict[str, Any]:
    sites = await convex_query("router:listSites", {"enabledOnly": True}) or []
    schedules = await convex_query("router:listSchedules", {}) or []
    schedule_map = {str(s.get("_id")): s for s in schedules if isinstance(s, dict)}

    summary = Counter()
    due_sites: List[Dict[str, Any]] = []
    for site in sites:
        if not isinstance(site, dict):
            continue
        summary["total"] += 1
        if site.get("failed"):
            summary["failed"] += 1
            continue
        if site.get("lockExpiresAt") and int(site.get("lockExpiresAt") or 0) > now_ms:
            summary["locked"] += 1
            continue
        schedule_id = site.get("scheduleId")
        last_run = int(site.get("lastRunAt") or 0)
        if schedule_id:
            sched = schedule_map.get(str(schedule_id))
            eligible_at = _latest_eligible_time(sched, now_ms)
            if eligible_at and last_run < eligible_at:
                summary["due"] += 1
                due_sites.append(
                    {
                        "url": site.get("url"),
                        "scheduleId": schedule_id,
                        "eligibleAt": eligible_at,
                        "lastRunAt": last_run,
                    }
                )
            else:
                summary["not_due"] += 1
        else:
            summary["unscheduled"] += 1
    return {
        "summary": dict(summary),
        "dueSamples": [
            {
                **row,
                "eligibleAt": _fmt_dt(row.get("eligibleAt")),
                "lastRunAt": _fmt_dt(row.get("lastRunAt")),
            }
            for row in due_sites[:5]
        ],
    }


async def _gather_temporal_status() -> Dict[str, Any]:
    active = await convex_query("temporal:getActiveWorkers", {}) or []
    stale = await convex_query("temporal:getStaleWorkers", {}) or []
    runs = await convex_query("temporal:listWorkflowRuns", {"limit": 50}) or []
    task_queues = sorted({str(row.get("taskQueue")) for row in active if row.get("taskQueue")})
    by_workflow = Counter([str(r.get("workflowName") or "") for r in runs])
    return {
        "activeWorkers": len(active),
        "staleWorkers": len(stale),
        "taskQueues": task_queues,
        "recentWorkflowCounts": dict(by_workflow),
        "recentWorkflowSamples": [
            {
                "workflowName": r.get("workflowName"),
                "status": r.get("status"),
                "startedAt": _fmt_dt(r.get("startedAt")),
                "completedAt": _fmt_dt(r.get("completedAt")),
                "taskQueue": r.get("taskQueue"),
                "error": r.get("error"),
            }
            for r in runs[:8]
        ],
    }


async def _gather_scrape_errors() -> Dict[str, Any]:
    rows = await convex_query("router:listScrapeErrors", {"limit": 50}) or []
    by_event = Counter([str(r.get("event") or "") for r in rows])
    return {
        "count": len(rows),
        "byEvent": dict(by_event),
        "sample": [
            {
                "event": r.get("event"),
                "status": r.get("status"),
                "error": r.get("error"),
                "createdAt": _fmt_dt(r.get("createdAt")),
            }
            for r in rows[:5]
        ],
    }


async def _spidercloud_batch_test(
    urls: List[str],
    *,
    api_key: str,
    concurrency: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    if AsyncSpider is None:
        return {"error": "spider client not available"}
    if not urls:
        return {"error": "no urls to test"}

    semaphore = asyncio.Semaphore(max(1, min(concurrency, len(urls))))
    start = time.time()

    async def _fetch(url: str) -> Tuple[str, Optional[int], Optional[str]]:
        async with semaphore:
            try:
                async with AsyncSpider(api_key=api_key) as client:
                    params = {
                        "return_format": ["commonmark"],
                        "metadata": True,
                        "request": "chrome",
                        "follow_redirects": True,
                        "redirect_policy": "Loose",
                        "external_domains": ["*"],
                        "preserve_host": True,
                        "limit": 1,
                    }
                    coro = client.scrape_url(
                        url,
                        params=params,
                        stream=False,
                        content_type="application/json",
                    )
                    result = await asyncio.wait_for(coro, timeout=timeout_seconds)
                    payload = json.dumps(result)
                    return url, len(payload), None
            except Exception as exc:  # noqa: BLE001
                return url, None, str(exc)

    results = [await _fetch(url) for url in urls]
    elapsed = time.time() - start
    sizes = [r[1] for r in results if r[1]]
    errors = [r for r in results if r[2]]
    return {
        "count": len(results),
        "elapsedSeconds": round(elapsed, 2),
        "avgSizeBytes": int(sum(sizes) / len(sizes)) if sizes else 0,
        "maxSizeBytes": max(sizes) if sizes else 0,
        "minSizeBytes": min(sizes) if sizes else 0,
        "errorCount": len(errors),
        "errorSamples": errors[:5],
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose Convex/Temporal stalls for SpiderCloud job details.")
    parser.add_argument("--env", default="prod", choices=["dev", "prod"])
    parser.add_argument("--provider", default="spidercloud", help="scrape_url_queue provider filter")
    parser.add_argument("--limit", type=int, default=500, help="per-status queue fetch limit (max 500)")
    parser.add_argument("--test-batch", type=int, default=0, help="run spidercloud batch test with N urls")
    parser.add_argument("--test-status", default="pending", help="queue status to source batch test urls from")
    parser.add_argument("--test-timeout", type=int, default=900, help="timeout seconds per spidercloud request")
    args = parser.parse_args()

    now_ms = int(time.time() * 1000)
    runtime = _load_runtime_config(args.env)
    expire_minutes = runtime.spidercloud_job_details_processing_expire_minutes

    pending = await _list_queue("pending", args.provider, args.limit)
    processing = await _list_queue("processing", args.provider, args.limit)
    failed = await _list_queue("failed", args.provider, args.limit)
    completed = await _list_queue("completed", args.provider, min(args.limit, 200))

    report: Dict[str, Any] = {
        "now": _fmt_dt(now_ms),
        "runtime": {
            "processingExpireMinutes": expire_minutes,
            "batchSize": runtime.spidercloud_job_details_batch_size,
            "concurrency": runtime.spidercloud_job_details_concurrency,
        },
        "queue": {
            "pending": _summarize_queue(pending, now_ms, expire_minutes),
            "processing": _summarize_queue(processing, now_ms, expire_minutes),
            "failed": _summarize_queue(failed, now_ms, expire_minutes),
            "completed": _summarize_queue(completed, now_ms, expire_minutes),
        },
        "sites": await _gather_site_schedule_summary(now_ms),
        "temporal": await _gather_temporal_status(),
        "scrapeErrors": await _gather_scrape_errors(),
    }

    if pending:
        oldest_pending = min(int(r.get("createdAt") or now_ms) for r in pending)
        report["queue"]["pending"]["oldestMinutes"] = _minutes_ago(oldest_pending, now_ms)
        report["queue"]["pending"]["oldestCreatedAt"] = _fmt_dt(oldest_pending)
    if processing:
        oldest_processing = min(int(r.get("updatedAt") or now_ms) for r in processing)
        report["queue"]["processing"]["oldestMinutes"] = _minutes_ago(oldest_processing, now_ms)
        report["queue"]["processing"]["oldestUpdatedAt"] = _fmt_dt(oldest_processing)

    if args.test_batch and args.test_batch > 0:
        api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
        if not api_key:
            report["batchTest"] = {"error": "SPIDER_API_KEY (or SPIDER_KEY) is not set"}
        else:
            source_rows = pending if args.test_status == "pending" else processing
            urls = [str(r.get("url")) for r in source_rows if r.get("url")]
            urls = urls[: args.test_batch]
            report["batchTest"] = await _spidercloud_batch_test(
                urls,
                api_key=api_key,
                concurrency=runtime.spidercloud_job_details_concurrency,
                timeout_seconds=args.test_timeout,
            )

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
