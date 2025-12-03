from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import heuristic_workflow as hw  # noqa: E402


def test_heuristic_workflow_disables_sandbox_and_uses_named_activity():
    assert getattr(hw, "__temporal_disable_workflow_sandbox__", False) is True
    assert hw.ACTIVITY_NAME == "process_pending_job_details_batch"


def test_schedule_yaml_includes_heuristic_job_details():
    from job_scrape_application.workflows.create_schedule import load_schedule_configs

    ids = {cfg.id for cfg in load_schedule_configs()}
    assert "heuristic-job-details" in ids
