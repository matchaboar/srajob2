"""DBOS step functions for storing scrape records."""

from __future__ import annotations

import time
from typing import Any, Dict, List

from dbos import DBOS

from ...helpers.compensation_parsing import (
    UNKNOWN_COMPENSATION_REASON,
    parse_compensation,
)
from ...helpers.scrape_utils import build_description_preview, coerce_level, coerce_remote
from ...helpers.timestamp_parsing import parse_posted_at_with_unknown
from ...helpers.url_handling import prefer_apply_url


# Fields allowed by the Convex ingestJobsFromScrape validator
_ALLOWED_OPTIONAL_FIELDS = frozenset({
    "city",
    "compensationReason",
    "compensationUnknown",
    "countries",
    "country",
    "currencyCode",
    "engineer",
    "heuristicAttempts",
    "heuristicLastTried",
    "heuristicVersion",
    "jobTitle",
    "job_title",
    "locationSearch",
    "locationStates",
    "locations",
    "metadata",
    "postedAtUnknown",
    "postingFirstPublishedAt",
    "scrapeUrl",
    "scrapedAt",
    "scrapedCostMilliCents",
    "scrapedWith",
    "state",
    "workflowName",
})


def _normalize_job_payload(job: Dict[str, Any], *, now_ms: int) -> Dict[str, Any] | None:
    title_val = job.get("title") or job.get("job_title") or job.get("jobTitle")
    title = title_val.strip() if isinstance(title_val, str) and title_val.strip() else "Untitled"

    company_val = job.get("company")
    company = company_val.strip() if isinstance(company_val, str) and company_val.strip() else "Unknown"

    description_val = job.get("description")
    description = description_val if isinstance(description_val, str) else ""
    description = build_description_preview(description) if description else ""

    location_val = job.get("location")
    location = location_val.strip() if isinstance(location_val, str) and location_val.strip() else ""
    if not location:
        locations_val = job.get("locations")
        if isinstance(locations_val, list):
            for candidate in locations_val:
                if isinstance(candidate, str) and candidate.strip():
                    location = candidate.strip()
                    break
    if not location:
        location = "Unknown"

    preferred_url = prefer_apply_url(job)
    url = preferred_url.strip() if isinstance(preferred_url, str) and preferred_url.strip() else ""
    raw_url = job.get("url")
    if isinstance(raw_url, str):
        raw_url = raw_url.strip()
    else:
        raw_url = ""
    if not url:
        for key in ("apply_url", "applyUrl", "job_url", "jobUrl", "link", "href"):
            candidate = job.get(key)
            if isinstance(candidate, str) and candidate.strip():
                url = candidate.strip()
                break
    if not raw_url:
        raw_url = url
    if not url:
        return None

    remote = coerce_remote(job.get("remote"), location, title)
    level = coerce_level(job.get("level") or job.get("seniority"), title)

    posted_at_value = job.get("postedAt")
    posted_at_unknown = job.get("postedAtUnknown")
    if isinstance(posted_at_value, (int, float)):
        posted_at = int(posted_at_value)
        if not isinstance(posted_at_unknown, bool):
            posted_at_unknown = job.get("posted_at_unknown")
            if not isinstance(posted_at_unknown, bool):
                posted_at_unknown = job.get("postedAtUnknown")
    else:
        raw_posted = job.get("posted_at") or job.get("postedAt")
        if raw_posted is None:
            raw_posted = job.get("date") or job.get("_timestamp")
        posted_at, parsed_unknown = parse_posted_at_with_unknown(raw_posted, now_ms)
        if isinstance(posted_at_unknown, bool):
            posted_at_unknown = posted_at_unknown
        else:
            posted_at_unknown = parsed_unknown

    total_compensation = job.get("totalCompensation")
    comp_unknown: bool | None = None
    if isinstance(total_compensation, (int, float)):
        total_comp = int(total_compensation)
    else:
        total_compensation = job.get("total_compensation")
        if isinstance(total_compensation, (int, float)):
            total_comp = int(total_compensation)
        else:
            total_comp, comp_unknown = parse_compensation(
                job.get("compensation") or job.get("salary"),
                with_meta=True,
            )

    if comp_unknown is None:
        comp_unknown_val = job.get("compensationUnknown")
        if isinstance(comp_unknown_val, bool):
            comp_unknown = comp_unknown_val
        else:
            comp_unknown_val = job.get("compensation_unknown")
            if isinstance(comp_unknown_val, bool):
                comp_unknown = comp_unknown_val
            else:
                comp_unknown = total_comp <= 0

    # Build output with only allowed fields - required fields first
    output: Dict[str, Any] = {
        "title": title,
        "company": company,
        "description": description,
        "location": location,
        "remote": remote,
        "level": level,
        "url": url,
        "postedAt": posted_at,
        "totalCompensation": total_comp,
    }

    # Add computed optional fields
    if isinstance(posted_at_unknown, bool):
        output["postedAtUnknown"] = posted_at_unknown
    if isinstance(comp_unknown, bool):
        output["compensationUnknown"] = comp_unknown
        if comp_unknown and not isinstance(job.get("compensationReason"), str):
            output["compensationReason"] = UNKNOWN_COMPENSATION_REASON
    if isinstance(raw_url, str) and raw_url.strip():
        output["scrapeUrl"] = raw_url.strip()
    source_url = job.get("sourceUrl") or job.get("source_url")
    if isinstance(source_url, str) and source_url.strip():
        output["sourceUrl"] = source_url.strip()

    # Map cost_milli_cents to scrapedCostMilliCents
    cost_milli_cents = job.get("cost_milli_cents")
    if isinstance(cost_milli_cents, (int, float)):
        output["scrapedCostMilliCents"] = float(cost_milli_cents)

    # Copy allowed optional fields from input
    for field in _ALLOWED_OPTIONAL_FIELDS:
        if field in job and field not in output:
            output[field] = job[field]

    return output


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def insert_scrape_record_step(payload: Dict[str, Any]) -> str:
    """Insert a scrape record into Convex.

    Args:
        payload: The scrape record payload.

    Returns:
        The scrape record ID.
    """
    from ....services.convex_client import convex_mutation

    return convex_mutation("router:insertScrapeRecord", payload)


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def ingest_jobs_from_scrape_step(
    jobs: List[Dict[str, Any]],
    site_id: str | None = None,
) -> None:
    """Ingest a batch of jobs from a scrape into Convex.

    Args:
        jobs: List of job dictionaries to ingest.
        site_id: Optional site ID to associate with jobs.
    """
    from ....services.convex_client import convex_mutation

    prepared_jobs: List[Dict[str, Any]] = []
    now_ms = int(time.time() * 1000)
    for job in jobs:
        if not isinstance(job, dict):
            continue
        job_payload = _normalize_job_payload(job, now_ms=now_ms)
        if job_payload is not None:
            prepared_jobs.append(job_payload)

    payload: Dict[str, Any] = {"jobs": prepared_jobs}
    if site_id is not None:
        payload["siteId"] = site_id

    convex_mutation("router:ingestJobsFromScrape", payload)


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def record_ignored_job_step(
    url: str,
    source_url: str,
    reason: str | None = None,
) -> None:
    """Record an ignored job URL in Convex.

    Args:
        url: The job URL that was ignored.
        source_url: The source/listing URL.
        reason: Optional reason for ignoring.
    """
    from ....services.convex_client import convex_mutation

    payload: Dict[str, Any] = {
        "url": url,
        "sourceUrl": source_url,
    }
    if reason is not None:
        payload["reason"] = reason

    convex_mutation("router:recordIgnoredJobUrl", payload)


@DBOS.step(
    retries_allowed=True,
    max_attempts=3,
    interval_seconds=1.0,
    backoff_rate=2.0,
)
def insert_ignored_job_step(
    url: str,
    source_url: str,
    reason: str | None = None,
    provider: str | None = None,
    workflow_name: str | None = None,
    details: Any = None,
    title: str | None = None,
    description: str | None = None,
) -> None:
    """Insert an ignored job into Convex.

    Args:
        url: The job URL that was ignored.
        source_url: The source/listing URL.
        reason: Optional reason for ignoring.
        provider: Optional provider name.
        workflow_name: Optional workflow name.
        details: Optional details dict.
        title: Optional job title.
        description: Optional job description.
    """
    from ....services.convex_client import convex_mutation

    payload: Dict[str, Any] = {
        "url": url,
        "sourceUrl": source_url,
    }
    if reason is not None:
        payload["reason"] = reason
    if provider is not None:
        payload["provider"] = provider
    if workflow_name is not None:
        payload["workflowName"] = workflow_name
    if details is not None:
        payload["details"] = details
    if title is not None:
        payload["title"] = title
    if description is not None:
        payload["description"] = description

    convex_mutation("router:insertIgnoredJob", payload)
