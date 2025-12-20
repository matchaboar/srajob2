from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from ..services import telemetry
from ..config import settings
from ..services.convex_client import convex_query


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


def _schedule_decision_for_site(site: Dict[str, Any], schedule_map: Dict[str, Dict[str, Any]], now_ms: int) -> Dict[str, Any]:
    provider = (
        site.get("scrapeProvider")
        or ("spidercloud" if site.get("type") == "greenhouse" else "fetchfox")
    )
    site_id = site.get("_id")
    last_run = int(site.get("lastRunAt") or 0)
    manual_trigger_at = int(site.get("manualTriggerAt") or 0)
    lock_expires_at = int(site.get("lockExpiresAt") or 0)
    schedule_id = site.get("scheduleId")
    completed = bool(site.get("completed"))
    failed = bool(site.get("failed"))
    reason = ""
    due = False
    eligible_at: Optional[int] = None

    if failed:
        reason = "skipped: marked failed"
    elif lock_expires_at and lock_expires_at > now_ms:
        reason = f"skipped: locked until {lock_expires_at}"
    elif completed and not schedule_id:
        reason = "skipped: completed and unscheduled"
    elif manual_trigger_at and manual_trigger_at > now_ms - 15 * 60 * 1000 and manual_trigger_at > last_run:
        due = True
        eligible_at = manual_trigger_at
        reason = "due: manual trigger window active"
    elif schedule_id:
        sched = schedule_map.get(str(schedule_id))
        eligible_at = _latest_eligible_time(sched, now_ms)
        if not eligible_at:
            reason = "skipped: outside schedule window"
        elif last_run >= eligible_at:
            reason = f"skipped: already ran at {last_run} (eligible_at={eligible_at})"
        else:
            due = True
            reason = f"due: scheduled slot {eligible_at}"
    else:
        due = True
        reason = "due: no schedule (always eligible)"

    return {
        "siteId": site_id,
        "url": site.get("url"),
        "provider": provider,
        "scheduleId": schedule_id,
        "lastRunAt": last_run,
        "manualTriggerAt": manual_trigger_at or None,
        "lockExpiresAt": lock_expires_at or None,
        "completed": completed,
        "failed": failed,
        "eligibleAt": eligible_at,
        "due": due,
        "reason": reason,
    }


async def _gather_schedule_audit(worker_id: str, now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
    now_ms = now_ms or int(__import__("time").time() * 1000)
    sites = await convex_query("router:listSites", {"enabledOnly": True})
    schedules = await convex_query("router:listSchedules", {})
    schedule_map = {str(s.get("_id")): s for s in schedules or [] if isinstance(s, dict)}

    if not isinstance(sites, list):
        return []

    entries: List[Dict[str, Any]] = []
    for site in sites:
        if not isinstance(site, dict):
            continue
        decision = _schedule_decision_for_site(site, schedule_map, now_ms)
        entries.append(
            {
                "event": "schedule.audit.site",
                "workflowId": "ScheduleAudit",
                "runId": worker_id,
                "siteId": decision["siteId"],
                "siteUrl": decision["url"],
                "message": (
                    f"[{'DUE' if decision['due'] else 'SKIP'}] {decision['url']} ({decision['provider']}) "
                    f"{decision['reason']}"
                ),
                "data": decision,
                "level": "info",
                "createdAt": now_ms,
            }
        )
    summary = {
        "event": "schedule.audit.summary",
        "workflowId": "ScheduleAudit",
        "runId": worker_id,
        "message": "Schedule audit summary",
        "data": {
            "due": len([e for e in entries if e["data"]["due"]]),
            "skip": len([e for e in entries if not e["data"]["due"]]),
            "total": len(entries),
        },
        "level": "info",
        "createdAt": now_ms,
    }
    return [summary, *entries]


async def schedule_audit_logger(worker_id: str) -> None:
    logger = logging.getLogger("temporal.scheduler.audit")
    try:
        while True:
            try:
                payloads = await _gather_schedule_audit(worker_id)
                if not payloads:
                    await asyncio.sleep(60 * 30)
                    continue

                summary = payloads[0]
                detail_payloads = payloads[1:]

                telemetry.emit_posthog_log(summary)
                logger.info(summary["message"])

                if settings.schedule_audit_verbose:
                    for payload in detail_payloads:
                        telemetry.emit_posthog_log(payload)
                        logger.info(payload["message"])
            except Exception as exc:  # noqa: BLE001
                logger.exception("Schedule audit failed: %s", exc)
            await asyncio.sleep(60 * 30)
    except asyncio.CancelledError:
        logger.info("Schedule audit logger stopped.")
