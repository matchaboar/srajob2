"""Workflow orchestrator for processing pending job details with heuristics.

This module provides a workflow function that coordinates step functions to process
pending job details, applying heuristic parsing for location, compensation, and metadata.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from ..activities.heuristics import (
    _describe_exception,
    _domain_from_url,
    _extract_request_id,
)
from ..activities.step.apply_job_heuristics import (
    count_pending_job_details_step,
    list_job_detail_configs_step,
    list_pending_job_details_step,
    record_job_detail_heuristic_step,
    update_job_with_heuristic_step,
)
from ..normalizers.pipeline import build_job_update as _build_job_detail_heuristic_patch

logger = logging.getLogger(__name__)


def process_pending_job_details_batch(limit: int = 25) -> Dict[str, Any]:
    """Parse pending job descriptions with heuristics and persist learned regex configs.

    This function orchestrates:
    1. Fetching pending job details via Convex
    2. Loading job detail configs for each domain
    3. Building heuristic patches using pattern matching
    4. Recording learned patterns to Convex
    5. Updating jobs with extracted location/compensation/metadata
    6. Reporting remaining pending count

    Args:
        limit: Maximum number of pending jobs to process in this batch.

    Returns:
        Dictionary with:
        - processed: Number of successfully processed jobs
        - updated: List of updated job IDs
        - remaining: Count of remaining pending jobs (or None if unavailable)
        - fetched: Number of jobs fetched from queue
        - errors: List of error details
    """
    pending = list_pending_job_details_step(limit)
    processed = 0
    updated: List[str] = []
    errors: List[Dict[str, Any]] = []
    total = len(pending)
    configs_by_domain: Dict[str, list] = {}
    logger.info("heuristic.batch start fetched=%s limit=%s", total, limit)

    def _attempt_mutation(op_name: str, job_id: str, patch: Dict[str, Any]) -> bool:
        """Run update mutation and capture errors without aborting the batch."""
        try:
            update_job_with_heuristic_step(job_id, patch)
            return True
        except Exception as exc:
            logger.warning(
                "heuristic.error job id=%s op=%s err=%s",
                job_id,
                op_name,
                _describe_exception(exc),
                exc_info=True,
            )
            errors.append(
                {
                    "id": job_id,
                    "op": op_name,
                    "requestId": _extract_request_id(exc),
                    "error": _describe_exception(exc),
                }
            )
            return False

    def _attempt_record(record: Dict[str, Any], job_id: str) -> bool:
        """Record heuristic and capture errors without aborting the batch."""
        try:
            record_job_detail_heuristic_step(record)
            return True
        except Exception as exc:
            logger.warning(
                "heuristic.error job id=%s op=recordJobDetailHeuristic err=%s",
                job_id,
                _describe_exception(exc),
                exc_info=True,
            )
            errors.append(
                {
                    "id": job_id,
                    "op": "router:recordJobDetailHeuristic",
                    "requestId": _extract_request_id(exc),
                    "error": _describe_exception(exc),
                }
            )
            return False

    for idx, row in enumerate(pending):
        current_op = "row:init"
        try:
            job_id = row.get("jobId") or row.get("_id")
            title = (str(row.get("title") or row.get("jobTitle") or "")).strip() or "<untitled>"
            logger.info("heuristic.view job id=%s title=%s", job_id or "<missing>", title)
            url = row.get("url") or ""
            domain = _domain_from_url(url)

            current_op = "router:listJobDetailConfigs"
            if domain in configs_by_domain:
                configs = configs_by_domain[domain]
            else:
                try:
                    configs = list_job_detail_configs_step(domain)
                except Exception as exc:
                    logger.warning(
                        "heuristic.error job id=%s op=%s err=%s",
                        job_id or "<missing>",
                        current_op,
                        _describe_exception(exc),
                        exc_info=True,
                    )
                    errors.append(
                        {
                            "id": job_id,
                            "op": current_op,
                            "requestId": _extract_request_id(exc),
                            "error": _describe_exception(exc),
                        }
                    )
                    continue
                configs_by_domain[domain] = configs

            now_ms = int(time.time() * 1000)
            patch, records = _build_job_detail_heuristic_patch(row, configs, now_ms)

            for rec in records:
                _attempt_record(rec, job_id)

            if not job_id:
                continue

            if patch:
                current_op = "router:updateJobWithHeuristic"
                did_update = _attempt_mutation(current_op, job_id, patch)
                if did_update:
                    update_summary = {
                        key: value
                        for key, value in {
                            "location": patch.get("location"),
                            "totalCompensation": patch.get("totalCompensation"),
                            "currencyCode": patch.get("currencyCode"),
                            "remote": patch.get("remote"),
                            "compensationUnknown": patch.get("compensationUnknown"),
                            "compensationReason": patch.get("compensationReason"),
                        }.items()
                        if value is not None
                    }
                    logger.info(
                        "heuristic.updated job id=%s title=%s changes=%s",
                        job_id or "<missing>",
                        title,
                        update_summary or {"note": "heuristic bookkeeping only"},
                    )
                    updated.append(job_id)
                    processed += 1

        except Exception as exc:
            logger.warning(
                "heuristic.error job id=%s op=%s err=%s",
                row.get("_id"),
                current_op,
                _describe_exception(exc),
                exc_info=True,
            )
            errors.append(
                {
                    "id": row.get("_id"),
                    "op": current_op,
                    "requestId": _extract_request_id(exc),
                    "error": _describe_exception(exc),
                }
            )
            continue

    remaining_after: Optional[int] = None
    try:
        remaining_after = count_pending_job_details_step()
    except Exception as exc:
        logger.debug("heuristic.remaining_count_failed err=%s", exc)

    remaining_label = remaining_after if remaining_after is not None else "unknown"
    logger.info(
        "heuristic.batch processed=%s updated=%s remaining=%s",
        processed,
        len(updated),
        remaining_label,
    )

    return {
        "processed": processed,
        "updated": updated,
        "remaining": remaining_after,
        "fetched": total,
        "errors": errors,
    }
