from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from temporalio import workflow

from .scrape_workflow import ScrapeSummary

# Disable workflow sandbox for this module to allow activity imports that pull in HTTP clients.
__temporal_disable_workflow_sandbox__ = True
ACTIVITY_NAME = "process_pending_job_details_batch"

if TYPE_CHECKING:  # pragma: no cover
    from .activities import process_pending_job_details_batch  # noqa: F401


@workflow.defn(name="HeuristicJobDetails")
class HeuristicJobDetailsWorkflow:
    @workflow.run
    async def run(self) -> ScrapeSummary:  # type: ignore[override]
        processed_total = 0
        try:
            while True:
                res = await workflow.execute_activity(
                    ACTIVITY_NAME,
                    args=[25],
                    start_to_close_timeout=timedelta(seconds=30),
                )
                count = res.get("processed") if isinstance(res, dict) else 0
                processed_total += count or 0
                if not count:
                    break
        except Exception:
            # Best-effort; avoid surfacing heuristic failures as workflow failures.
            pass

        return ScrapeSummary(site_count=0, scrape_ids=[])
