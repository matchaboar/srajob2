from __future__ import annotations

import json
import os
import sys
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

sys.path.insert(0, os.path.abspath("."))

import job_scrape_application.workflows.scrape_workflow as sw  # noqa: E402


FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/scrape_queue_fixture.json"
)
MIN_FIXTURE_ROWS = 100


def _load_fixture_rows() -> List[Dict[str, Any]]:
    if not FIXTURE_PATH.exists():
        pytest.skip("Missing queue fixture; run agent_scripts/export_scrape_queue_fixture.py")
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    rows = payload.get("rows", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        pytest.skip("Queue fixture rows missing or invalid")
    rows = [row for row in rows if isinstance(row, dict)]
    if len(rows) < MIN_FIXTURE_ROWS:
        pytest.skip("Queue fixture has fewer than 100 rows")
    return rows


class MockQueue:
    def __init__(self, rows: List[Dict[str, Any]], *, normalize_statuses: bool = True) -> None:
        self.rows = [dict(row) for row in rows]
        for row in self.rows:
            if normalize_statuses:
                row["status"] = "pending"
            else:
                row.setdefault("status", "pending")
        self.lease_calls: List[List[str]] = []
        self.complete_calls: List[Dict[str, Any]] = []

    def lease(self, limit: int) -> Dict[str, Any]:
        pending = [row for row in self.rows if row.get("status") == "pending"]
        batch = pending[:limit]
        now_ms = int(time.time() * 1000)
        for row in batch:
            row["status"] = "processing"
            row["attempts"] = int(row.get("attempts") or 0) + 1
            row["updatedAt"] = now_ms
        self.lease_calls.append([row.get("url", "") for row in batch])
        return {
            "urls": [
                {
                    "url": row.get("url"),
                    "sourceUrl": row.get("sourceUrl"),
                    "provider": row.get("provider"),
                    "siteId": row.get("siteId"),
                    "pattern": row.get("pattern"),
                }
                for row in batch
            ]
        }

    def complete(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        urls = payload.get("urls", [])
        status = payload.get("status")
        if not isinstance(urls, list) or not status:
            return {"updated": 0}
        url_set = {u for u in urls if isinstance(u, str)}
        updated = 0
        for row in self.rows:
            if row.get("url") in url_set:
                row["status"] = status
                updated += 1
        self.complete_calls.append(payload)
        return {"updated": updated}

    def status_counts(self) -> Dict[str, int]:
        counts = Counter()
        for row in self.rows:
            counts[str(row.get("status") or "")] += 1
        return dict(counts)


@pytest.mark.asyncio
async def test_spidercloud_job_details_processes_queue_fixture(monkeypatch):
    rows = _load_fixture_rows()
    queue = MockQueue(rows, normalize_statuses=True)
    stored_scrapes: List[Dict[str, Any]] = []

    @activity.defn
    async def lease_scrape_url_batch(provider: str | None = None, limit: int = sw.SPIDERCLOUD_BATCH_SIZE):
        return queue.lease(limit)

    @activity.defn
    async def process_spidercloud_job_batch(batch: Dict[str, Any]):
        scrapes: List[Dict[str, Any]] = []
        for entry in batch.get("urls", []):
            if not isinstance(entry, dict):
                continue
            url = entry.get("url")
            if not isinstance(url, str) or not url:
                continue
            scrapes.append(
                {
                    "sourceUrl": url,
                    "subUrls": [url],
                    "items": {
                        "provider": "spidercloud",
                        "normalized": [{"url": url, "title": "Stub"}],
                    },
                    "provider": "spidercloud",
                }
            )
        return {"scrapes": scrapes}

    @activity.defn
    async def store_scrape(scrape: Dict[str, Any]):
        stored_scrapes.append(scrape)
        return f"scrape-{len(stored_scrapes)}"

    @activity.defn
    async def complete_scrape_urls(payload: Dict[str, Any]):
        return queue.complete(payload)

    @activity.defn
    async def record_workflow_run(payload: Dict[str, Any]):
        return None

    @activity.defn
    async def record_scratchpad(payload: Dict[str, Any]):
        return None

    monkeypatch.setattr(sw, "lease_scrape_url_batch", lease_scrape_url_batch, raising=False)
    monkeypatch.setattr(sw, "process_spidercloud_job_batch", process_spidercloud_job_batch, raising=False)
    monkeypatch.setattr(sw, "store_scrape", store_scrape, raising=False)
    monkeypatch.setattr(sw, "complete_scrape_urls", complete_scrape_urls, raising=False)
    monkeypatch.setattr(sw, "record_workflow_run", record_workflow_run, raising=False)
    monkeypatch.setattr(sw, "record_scratchpad", record_scratchpad, raising=False)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        task_queue = f"queue-fixture-{uuid.uuid4().hex[:6]}"
        worker = Worker(
            env.client,
            task_queue=task_queue,
            workflows=[sw.SpidercloudJobDetailsWorkflow],
            activities=[
                lease_scrape_url_batch,
                process_spidercloud_job_batch,
                store_scrape,
                complete_scrape_urls,
                record_workflow_run,
                record_scratchpad,
            ],
        )
        async with worker:
            summary = await env.client.execute_workflow(
                sw.SpidercloudJobDetailsWorkflow.run,
                id=f"wf-queue-fixture-{uuid.uuid4().hex[:6]}",
                task_queue=task_queue,
            )

    total = len(rows)
    non_empty_calls = [call for call in queue.lease_calls if call]

    assert summary.site_count == 1
    assert len(non_empty_calls) == 1
    leased_urls = non_empty_calls[0]
    assert len(leased_urls) == sw.SPIDERCLOUD_BATCH_SIZE
    assert len(stored_scrapes) == len(leased_urls)

    status_counts = queue.status_counts()
    assert status_counts.get("completed", 0) == len(leased_urls)
    assert status_counts.get("processing", 0) == 0
    assert status_counts.get("pending", 0) == total - len(leased_urls)
