from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import worker as worker_mod  # noqa: E402


def test_select_worker_configs_includes_job_details_queue(monkeypatch):
    monkeypatch.setattr(worker_mod.settings, "worker_role", "all")
    monkeypatch.setattr(worker_mod.settings, "task_queue", "scraper-task-queue")
    monkeypatch.setattr(worker_mod.settings, "job_details_task_queue", "spidercloud-job-details-queue")

    configs = worker_mod._select_worker_configs()

    assert [cfg.task_queue for cfg in configs] == [
        "scraper-task-queue",
        "spidercloud-job-details-queue",
    ]
    assert configs[1].workflows == worker_mod.JOB_DETAILS_WORKFLOWS
    assert configs[1].activities == worker_mod.JOB_DETAILS_ACTIVITIES
    assert configs[1].role == "job-details"


def test_select_worker_configs_no_duplicate_queue(monkeypatch):
    monkeypatch.setattr(worker_mod.settings, "worker_role", "all")
    monkeypatch.setattr(worker_mod.settings, "task_queue", "scraper-task-queue")
    monkeypatch.setattr(worker_mod.settings, "job_details_task_queue", "scraper-task-queue")

    configs = worker_mod._select_worker_configs()

    assert len(configs) == 1
    assert configs[0].task_queue == "scraper-task-queue"


def test_select_worker_configs_job_details_role(monkeypatch):
    monkeypatch.setattr(worker_mod.settings, "worker_role", "job-details")
    monkeypatch.setattr(worker_mod.settings, "task_queue", "scraper-task-queue")
    monkeypatch.setattr(worker_mod.settings, "job_details_task_queue", "spidercloud-job-details-queue")

    configs = worker_mod._select_worker_configs()

    assert len(configs) == 1
    assert configs[0].task_queue == "spidercloud-job-details-queue"
    assert configs[0].workflows == worker_mod.JOB_DETAILS_WORKFLOWS
    assert configs[0].activities == worker_mod.JOB_DETAILS_ACTIVITIES
