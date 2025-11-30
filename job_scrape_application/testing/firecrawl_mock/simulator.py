from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class MockFirecrawlScenario(str, Enum):
    """Enumerate common Firecrawl behaviours for tests."""

    SUCCESS_WITH_WEBHOOK = "success_with_webhook"
    START_FAILS = "start_fails"
    WEBHOOK_POST_FAILS = "webhook_post_fails"


class MockWebhookQueue:
    """Thread-safe in-memory queue to capture webhook payloads."""

    def __init__(self) -> None:
        self._events: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._event = threading.Event()

    def push(self, payload: Dict[str, Any]) -> None:
        with self._lock:
            self._events.append(payload)
            self._event.set()

    def drain(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        with self._lock:
            if limit is None or limit > len(self._events):
                limit = len(self._events)
            drained = self._events[:limit]
            self._events = self._events[limit:]
            if not self._events:
                self._event.clear()
            return drained

    async def wait_for(self, count: int = 1, timeout: float = 1.0) -> bool:
        """Wait until at least `count` events exist or timeout expires."""

        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if len(self._events) >= count:
                    return True
            await asyncio.sleep(0.01)
        return False


@dataclass
class MockFirecrawlJob:
    """Minimal job object matching Firecrawl's attributes."""

    job_id: str
    kind: str
    status_url: str

    def __init__(self, *, job_id: Optional[str] = None, kind: str = "site_crawl") -> None:
        self.job_id = job_id or f"job-{uuid.uuid4().hex[:10]}"
        self.kind = kind
        self.status_url = f"https://mock.firecrawl/v2/batch/scrape/{self.job_id}"
        # Firecrawl client returns both snake and camel names; mirror that for compatibility.
        self.id = self.job_id
        self.statusUrl = self.status_url
        self.jobId = self.job_id


@dataclass
class MockFirecrawlStatus:
    """Simple status payload that exposes `model_dump` like the SDK."""

    status: str = "completed"
    data: List[Dict[str, Any]] = field(default_factory=list)

    def model_dump(self, *, mode: str = "json", exclude_none: bool = True) -> Dict[str, Any]:
        return {"status": self.status, "data": self.data}


@dataclass
class MockFirecrawlResponse:
    """Lightweight HTTP-style response returned by callable mocks."""

    status_code: int
    payload: Dict[str, Any]

    def json(self) -> Dict[str, Any]:
        return self.payload

    @property
    def text(self) -> str:
        return json.dumps(self.payload, ensure_ascii=False)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            error = self.payload.get("error") or "Mock Firecrawl error"
            raise RuntimeError(f"Mock Firecrawl HTTP {self.status_code}: {error}")


class MockFirecrawl:
    """Drop-in replacement for the Firecrawl client with programmable outcomes."""

    def __init__(
        self,
        *,
        scenario: MockFirecrawlScenario = MockFirecrawlScenario.SUCCESS_WITH_WEBHOOK,
        webhook_delay: float = 0.05,
        webhook_queue: Optional[MockWebhookQueue] = None,
        webhook_handler: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.scenario = scenario
        self.webhook_delay = webhook_delay
        self.webhook_queue = webhook_queue or MockWebhookQueue()
        self.webhook_handler = webhook_handler
        self.started_jobs: List[MockFirecrawlJob] = []
        self.webhooks_sent: List[Dict[str, Any]] = []
        self.webhook_failures: List[Dict[str, Any]] = []

    def __call__(
        self,
        *,
        site_url: str | None = None,
        webhook: Any | None = None,
        kind: str = "site_crawl",
    ) -> MockFirecrawlResponse:
        """Simulate Firecrawl HTTP start endpoints.

        Returns a response-like object with a status code and JSON payload. Success
        responses schedule a webhook delivery according to the configured scenario.
        """

        site_url = site_url or "https://example.com"
        webhook_payload: Any = webhook or {"url": "https://demo.convex.site/api/firecrawl/webhook"}
        if isinstance(webhook_payload, dict):
            metadata = webhook_payload.setdefault("metadata", {})
            metadata.setdefault("siteUrl", site_url)

        try:
            job = self.start_batch_scrape([site_url], webhook=webhook_payload)
        except Exception as exc:  # noqa: BLE001
            return MockFirecrawlResponse(status_code=500, payload={"error": str(exc)})

        payload = {
            "jobId": job.job_id,
            "id": job.job_id,
            "statusUrl": job.status_url,
            "status_url": job.status_url,
            "status": "queued",
            "kind": kind,
        }
        return MockFirecrawlResponse(status_code=200, payload=payload)

    # Public API mirrors the Firecrawl client methods used by activities.py
    def start_crawl(self, url: str, **kwargs: Any) -> MockFirecrawlJob:
        return self._start_job(url=url, kind="site_crawl", webhook=kwargs.get("webhook"))

    def start_batch_scrape(self, urls: List[str], **kwargs: Any) -> MockFirecrawlJob:
        first_url = urls[0] if urls else "https://example.com"
        webhook = kwargs.get("webhook")
        webhook_meta = {}
        if isinstance(webhook, dict):
            webhook_meta = webhook.get("metadata") or {}
        elif hasattr(webhook, "model_dump"):
            try:
                webhook_meta = webhook.model_dump().get("metadata") or {}
            except Exception:
                webhook_meta = {}
        kind = webhook_meta.get("kind") or "site_crawl"
        return self._start_job(
            url=first_url,
            kind=str(kind),
            webhook=webhook,
        )

    def get_crawl_status(self, job_id: str, **_: Any) -> MockFirecrawlStatus:
        # Return a canned completion payload suitable for collect_firecrawl_job_result
        data = [{"results": {"items": [{"job_title": "Mock Engineer", "company": "MockCo"}]}}]
        return MockFirecrawlStatus(status="completed", data=data)

    def get_batch_scrape_status(self, job_id: str, **_: Any) -> MockFirecrawlStatus:
        return self.get_crawl_status(job_id)

    # Internal helpers
    def _start_job(self, *, url: str, kind: str, webhook: Any) -> MockFirecrawlJob:
        if self.scenario == MockFirecrawlScenario.START_FAILS:
            raise RuntimeError("Mock Firecrawl returned 500")

        job = MockFirecrawlJob(kind=kind)
        self.started_jobs.append(job)

        if webhook:
            self._schedule_webhook(job, webhook, url=url)

        return job

    def _schedule_webhook(self, job: MockFirecrawlJob, webhook: Any, *, url: str) -> None:
        payload = self._build_webhook_payload(job, webhook, url=url)

        def _deliver() -> None:
            if self.scenario == MockFirecrawlScenario.WEBHOOK_POST_FAILS:
                self.webhook_failures.append(payload)
                return

            try:
                if self.webhook_handler:
                    self.webhook_handler(payload)
                else:
                    self.webhook_queue.push(payload)
                self.webhooks_sent.append(payload)
            except Exception as exc:  # noqa: BLE001
                self.webhook_failures.append({"payload": payload, "error": str(exc)})

        timer = threading.Timer(self.webhook_delay, _deliver)
        timer.daemon = True
        timer.start()

    def _build_webhook_payload(
        self,
        job: MockFirecrawlJob,
        webhook: Any,
        *,
        url: str,
    ) -> Dict[str, Any]:
        webhook_dict = self._webhook_to_dict(webhook)
        metadata = webhook_dict.get("metadata") or {}

        return {
            "_id": f"wh-{job.job_id}",
            "event": "batch_scrape.completed",
            "type": "batch_scrape.completed",
            "status": "completed",
            "success": True,
            "jobId": job.job_id,
            "statusUrl": job.status_url,
            "status_url": job.status_url,
            "metadata": metadata,
            "siteUrl": metadata.get("siteUrl"),
            "data": [
                {
                    "markdown": f"# Mock scraped {url}",
                    "metadata": {"sourceURL": url, "url": url, "statusCode": 200},
                }
            ],
            "receivedAt": int(time.time() * 1000),
            "targetUrl": webhook_dict.get("url"),
        }

    @staticmethod
    def _webhook_to_dict(webhook: Any) -> Dict[str, Any]:
        if hasattr(webhook, "model_dump"):
            try:
                return webhook.model_dump()
            except TypeError:
                return webhook.model_dump(mode="python")  # type: ignore[arg-type]
        if isinstance(webhook, dict):
            return dict(webhook)

        payload: Dict[str, Any] = {}
        for attr in ("url", "metadata", "headers", "secret"):
            value = getattr(webhook, attr, None)
            if value is not None:
                payload[attr] = value
        return payload
