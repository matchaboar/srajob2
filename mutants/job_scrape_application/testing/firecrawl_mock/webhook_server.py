from __future__ import annotations

from typing import Any, Dict, Optional

from .simulator import MockFirecrawlResponse, MockWebhookQueue


class MockFirecrawlWebhookServer:
    """Minimal in-memory handler to simulate Firecrawl webhook POSTs.

    Tests can call ``post(payload)`` to emulate Firecrawl delivering a webhook.
    Successful requests are pushed onto a ``MockWebhookQueue`` that Temporal
    workflows can drain via the existing mock fetch activity.
    """

    def __init__(self, queue: Optional[MockWebhookQueue] = None) -> None:
        self.queue = queue or MockWebhookQueue()

    def post(self, payload: Dict[str, Any]) -> MockFirecrawlResponse:
        """Handle a simulated POST request.

        Returns a ``MockFirecrawlResponse`` with 200/400 status. Basic validation
        ensures ``jobId`` and ``event`` exist since Temporal relies on them.
        """

        if not isinstance(payload, dict):
            return MockFirecrawlResponse(status_code=400, payload={"error": "invalid json"})

        if not payload.get("jobId"):
            return MockFirecrawlResponse(status_code=400, payload={"error": "jobId required"})

        if not payload.get("event"):
            return MockFirecrawlResponse(status_code=400, payload={"error": "event required"})

        self.queue.push(payload)
        return MockFirecrawlResponse(status_code=200, payload={"status": "ok"})


__all__ = ["MockFirecrawlWebhookServer"]
