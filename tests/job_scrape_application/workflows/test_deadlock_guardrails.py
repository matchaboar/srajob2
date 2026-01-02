from __future__ import annotations

import ast
import os
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402
from job_scrape_application.workflows import greenhouse_workflow as gw  # noqa: E402
from job_scrape_application.workflows import heuristic_workflow as hw  # noqa: E402
from job_scrape_application.workflows import scrape_workflow as sw  # noqa: E402
from job_scrape_application.workflows import webhook_workflow as ww  # noqa: E402


class _Info:
    run_id = "run-1"
    workflow_id = "wf-1"
    task_queue = "test-queue"


def _stub_logger(module) -> None:
    logger = types.SimpleNamespace(
        info=lambda *_a, **_k: None,
        warning=lambda *_a, **_k: None,
        error=lambda *_a, **_k: None,
    )
    module.workflow.logger = logger  # type: ignore[attr-defined]


def _find_convex_references(tree: ast.AST) -> list[str]:
    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if "convex_client" in node.module:
                offenders.append(f"import-from:{node.module}")
        if isinstance(node, ast.Import):
            for alias in node.names:
                if "convex_client" in alias.name:
                    offenders.append(f"import:{alias.name}")
        if isinstance(node, ast.Name) and node.id in {"convex_query", "convex_mutation"}:
            offenders.append(f"name:{node.id}")
        if isinstance(node, ast.Attribute) and node.attr in {"convex_query", "convex_mutation"}:
            offenders.append(f"attr:{node.attr}")
    return offenders


def _assert_no_convex_calls(module) -> None:
    path = Path(module.__file__)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    offenders = _find_convex_references(tree)
    assert not offenders, f"Workflow module {path} should not access Convex directly: {offenders}"


def test_workflow_modules_do_not_call_convex_directly():
    for module in (sw, gw, ww, hw):
        _assert_no_convex_calls(module)


@pytest.mark.asyncio
async def test_scrape_workflow_avoids_store_scrape_when_activity_persists(monkeypatch):
    state = {"leased": False}

    async def fake_execute_activity(activity, *args, **kwargs):
        if activity is acts.lease_site:
            if state["leased"]:
                return None
            state["leased"] = True
            return {"_id": "site-1", "url": "https://example.com"}
        if activity is acts.scrape_site:
            return {"scrapeId": "scr-1", "summary": {"jobs": 1}}
        if activity is acts.complete_site:
            return None
        if activity is acts.record_workflow_run:
            return None
        if activity is acts.store_scrape:
            raise AssertionError("store_scrape should not be called when persist_scrapes_in_activity=True")
        if activity is acts.fail_site:
            return None
        raise AssertionError(f"Unexpected activity {activity}")

    monkeypatch.setattr(sw.settings, "persist_scrapes_in_activity", True)
    monkeypatch.setattr(sw.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(sw.workflow, "now", lambda: datetime.fromtimestamp(0))
    monkeypatch.setattr(sw.workflow, "info", lambda: _Info())
    _stub_logger(sw)

    summary = await sw.ScrapeWorkflow().run()

    assert summary.scrape_ids == ["scr-1"]


@pytest.mark.asyncio
async def test_greenhouse_workflow_avoids_store_scrape_when_activity_persists(monkeypatch):
    state = {"leased": False}
    job_urls = ["https://example.com/jobs/1"]

    async def fake_execute_activity(activity, *args, **kwargs):
        if activity is acts.lease_site:
            if state["leased"]:
                return None
            state["leased"] = True
            return {"_id": "site-1", "url": "https://example.com"}
        if activity is acts.fetch_greenhouse_listing:
            return {"job_urls": job_urls}
        if activity is acts.filter_existing_job_urls:
            return []
        if activity is acts.compute_urls_to_scrape:
            return {"urlsToScrape": job_urls, "existingCount": 0, "totalCount": len(job_urls)}
        if activity is acts.scrape_greenhouse_jobs:
            return {"scrapeId": "scr-1", "jobsScraped": 1}
        if activity is acts.complete_site:
            return None
        if activity is acts.record_workflow_run:
            return None
        if activity is acts.store_scrape:
            raise AssertionError("store_scrape should not be called when persist_scrapes_in_activity=True")
        if activity is acts.fail_site:
            return None
        raise AssertionError(f"Unexpected activity {activity}")

    monkeypatch.setattr(gw.settings, "persist_scrapes_in_activity", True)
    monkeypatch.setattr(gw.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(gw.workflow, "now", lambda: datetime.fromtimestamp(0))
    monkeypatch.setattr(gw.workflow, "info", lambda: _Info())
    _stub_logger(gw)

    summary = await gw.GreenhouseScraperWorkflow().run()

    assert summary.scrape_ids == ["scr-1"]
    assert summary.jobs_scraped == 1
