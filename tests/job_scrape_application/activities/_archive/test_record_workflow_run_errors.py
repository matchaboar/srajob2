from __future__ import annotations

import asyncio

import pytest


from job_scrape_application.workflows import activities as acts  # noqa: E402


def test_record_workflow_run_swallows_cancelled(monkeypatch):
    payload = {"runId": "r1"}

    def fake_record_run(**_kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.runs.record_run",
        fake_record_run,
    )

    # Should not raise
    acts.record_workflow_run(payload)


def test_record_workflow_run_raises_other_errors(monkeypatch):
    payload = {"runId": "r2"}

    def fake_record_run(**_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.runs.record_run",
        fake_record_run,
    )

    with pytest.raises(RuntimeError):
        acts.record_workflow_run(payload)
