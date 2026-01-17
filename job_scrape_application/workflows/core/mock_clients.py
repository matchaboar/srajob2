"""Mock implementations for testing workflows.

This module provides mock/fake implementations of external services
that can be used in tests without making real network calls.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping

from .protocols import ConvexFunctions


@dataclass
class MockConvexFunctions:
    """Mock Convex client that returns fixture data.

    Args:
        query_fixtures: Dict mapping query names to return values
        mutation_fixtures: Dict mapping mutation names to return values
        captured_queries: List to capture all query calls
        captured_mutations: List to capture all mutation calls
    """

    query_fixtures: Dict[str, Any] = field(default_factory=dict)
    mutation_fixtures: Dict[str, Any] = field(default_factory=dict)
    captured_queries: List[Dict[str, Any]] = field(default_factory=list)
    captured_mutations: List[Dict[str, Any]] = field(default_factory=list)

    def query(self, name: str, args: Mapping[str, Any] | None = None) -> Any:
        """Mock query that returns fixture data."""
        self.captured_queries.append({
            "name": name,
            "args": dict(args) if args else {},
        })
        if name in self.query_fixtures:
            fixture = self.query_fixtures[name]
            # Support callable fixtures for dynamic responses
            if callable(fixture):
                return fixture(args)
            return fixture
        return None

    def mutation(self, name: str, args: Mapping[str, Any] | None = None) -> Any:
        """Mock mutation that returns fixture data."""
        self.captured_mutations.append({
            "name": name,
            "args": dict(args) if args else {},
        })
        if name in self.mutation_fixtures:
            fixture = self.mutation_fixtures[name]
            if callable(fixture):
                return fixture(args)
            return fixture
        return None

    def action(self, name: str, args: Mapping[str, Any] | None = None) -> Any:
        """Mock action - currently just returns None."""
        return None

    def as_functions(self) -> ConvexFunctions:
        """Convert to ConvexFunctions for use in DependencyContainer."""
        return ConvexFunctions(
            query=self.query,
            mutation=self.mutation,
            action=self.action,
        )


@dataclass
class MockQueueItem:
    """Mock queue item for testing."""

    id: str
    url: str
    source_url: str | None = None
    provider: str | None = None
    site_id: str | None = None
    pattern: str | None = None
    url_type: str | None = None
    posted_at: int | None = None
    status: str = "pending"
    attempts: int = 0
    dedupe_key: str | None = None
    created_at: int = 0
    run_after: int = 0


@dataclass
class MockLeaseResult:
    """Mock lease result for testing."""

    urls: List[Dict[str, Any]] = field(default_factory=list)
    skipped_urls: List[str] = field(default_factory=list)


class MockQueueService:
    """Mock queue service for testing without SQLite.

    Maintains an in-memory queue that mimics the real queue behavior.
    """

    def __init__(self) -> None:
        self._items: Dict[str, MockQueueItem] = {}
        self._next_id = 1

    def enqueue_scrape_urls(
        self,
        payload: Dict[str, Any],
        *,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """Add URLs to the mock queue."""
        urls = payload.get("urls", [])
        site_id = payload.get("siteId") or payload.get("site_id")
        provider = payload.get("provider")
        source_url = payload.get("sourceUrl") or payload.get("source_url")
        pattern = payload.get("pattern")
        url_type = payload.get("urlType") or payload.get("url_type", "detail")

        added = 0
        skipped = 0

        for url in urls:
            if isinstance(url, str):
                item_url = url
            elif isinstance(url, dict):
                item_url = url.get("url", "")
            else:
                continue

            if not item_url:
                continue

            # Check for duplicates
            dedupe_key = f"{site_id}:{item_url}" if site_id else item_url
            existing = next(
                (item for item in self._items.values() if item.dedupe_key == dedupe_key),
                None,
            )
            if existing and not force_refresh:
                skipped += 1
                continue

            item_id = f"mock_{self._next_id}"
            self._next_id += 1

            self._items[item_id] = MockQueueItem(
                id=item_id,
                url=item_url,
                source_url=source_url,
                provider=provider,
                site_id=site_id,
                pattern=pattern,
                url_type=url_type,
                dedupe_key=dedupe_key,
            )
            added += 1

        return {
            "added": added,
            "skipped": skipped,
            "total": len(urls),
        }

    def lease_scrape_url_batch(
        self,
        *,
        provider: str | None = None,
        limit: int = 50,
        url_type: str | None = None,
    ) -> MockLeaseResult:
        """Lease URLs from the mock queue."""
        leased: List[Dict[str, Any]] = []
        skipped: List[str] = []

        for item in list(self._items.values()):
            if len(leased) >= limit:
                break

            if item.status != "pending":
                continue

            if provider and item.provider != provider:
                continue

            if url_type and item.url_type != url_type:
                continue

            # Mark as processing
            item.status = "processing"
            leased.append({
                "id": item.id,
                "url": item.url,
                "source_url": item.source_url,
                "provider": item.provider,
                "site_id": item.site_id,
                "pattern": item.pattern,
                "url_type": item.url_type,
                "posted_at": item.posted_at,
                "attempts": item.attempts,
            })

        return MockLeaseResult(urls=leased, skipped_urls=skipped)

    def complete_scrape_urls(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Mark URLs as completed."""
        ids = payload.get("ids", [])
        urls = payload.get("urls", [])

        completed = 0
        for item_id in ids:
            if item_id in self._items:
                self._items[item_id].status = "completed"
                completed += 1

        for url in urls:
            for item in self._items.values():
                if item.url == url:
                    item.status = "completed"
                    completed += 1
                    break

        return {"completed": completed}

    def fail_scrape_urls(
        self,
        payload: Dict[str, Any],
        *,
        error: str | None = None,
    ) -> Dict[str, Any]:
        """Mark URLs as failed."""
        ids = payload.get("ids", [])
        urls = payload.get("urls", [])

        failed = 0
        for item_id in ids:
            if item_id in self._items:
                self._items[item_id].status = "failed"
                failed += 1

        for url in urls:
            for item in self._items.values():
                if item.url == url:
                    item.status = "failed"
                    failed += 1
                    break

        return {"failed": failed}

    def queue_status(self) -> Dict[str, Any]:
        """Get mock queue status."""
        pending = sum(1 for item in self._items.values() if item.status == "pending")
        processing = sum(1 for item in self._items.values() if item.status == "processing")
        completed = sum(1 for item in self._items.values() if item.status == "completed")
        failed = sum(1 for item in self._items.values() if item.status == "failed")

        return {
            "pending": pending,
            "processing": processing,
            "completed": completed,
            "failed": failed,
            "total": len(self._items),
        }

    def detail_queue_has_pending(self, *, include_processing: bool = False) -> bool:
        """Check if queue has pending items."""
        for item in self._items.values():
            if item.url_type != "detail":
                continue
            if item.status == "pending":
                return True
            if include_processing and item.status == "processing":
                return True
        return False

    def list_scrape_urls(
        self,
        *,
        provider: str | None = None,
        site_id: str | None = None,
        status: str | None = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """List URLs in the mock queue."""
        result: List[Dict[str, Any]] = []
        for item in self._items.values():
            if len(result) >= limit:
                break
            if provider and item.provider != provider:
                continue
            if site_id and item.site_id != site_id:
                continue
            if status and item.status != status:
                continue
            result.append({
                "id": item.id,
                "url": item.url,
                "source_url": item.source_url,
                "provider": item.provider,
                "site_id": item.site_id,
                "status": item.status,
            })
        return result


@dataclass
class MockSettings:
    """Mock settings for testing."""

    overrides: Dict[str, Any] = field(default_factory=dict)

    @property
    def convex_url(self) -> str | None:
        return self.overrides.get("convex_url", "https://test.convex.cloud")

    @property
    def convex_http_url(self) -> str | None:
        return self.overrides.get("convex_http_url", "https://test.convex.site")

    @property
    def spider_api_key(self) -> str | None:
        return self.overrides.get("spider_api_key", "test_spider_key")

    @property
    def firecrawl_api_key(self) -> str | None:
        return self.overrides.get("firecrawl_api_key", "test_firecrawl_key")

    @property
    def fetchfox_api_key(self) -> str | None:
        return self.overrides.get("fetchfox_api_key", "test_fetchfox_key")

    @property
    def enable_firecrawl(self) -> bool:
        return self.overrides.get("enable_firecrawl", False)

    @property
    def enable_fetchfox(self) -> bool:
        return self.overrides.get("enable_fetchfox", False)


class MockSpiderClient:
    """Mock SpiderCloud client that returns fixture data."""

    def __init__(
        self,
        responses: Dict[str, Any],
        captured_scrapes: List[Dict[str, Any]],
    ) -> None:
        """Initialize mock spider client.

        Args:
            responses: Dict mapping URLs to responses (can be list of JSON strings)
            captured_scrapes: List to capture all scrape calls
        """
        self._responses = responses
        self._captured = captured_scrapes

    async def __aenter__(self) -> "MockSpiderClient":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    async def scrape_url(
        self,
        url: str,
        params: Dict[str, Any] | None = None,
    ) -> Any:
        """Return fixture response for URL."""
        self._captured.append({
            "url": url,
            "params": params or {},
        })

        # Look for exact match first
        if url in self._responses:
            return self._responses[url]

        # Look for partial match
        for key, response in self._responses.items():
            if key in url or url in key:
                return response

        # Return empty response
        return []


class MockSpiderClientFactory:
    """Factory that creates MockSpiderClient instances."""

    def __init__(
        self,
        responses: Dict[str, Any],
        captured_scrapes: List[Dict[str, Any]],
    ) -> None:
        self._responses = responses
        self._captured = captured_scrapes

    def __call__(self) -> MockSpiderClient:
        return MockSpiderClient(self._responses, self._captured)


class CapturingSpiderClient:
    """Spider client wrapper that captures requests/responses for fixture generation.

    Wraps a real AsyncSpider client and records all requests and responses.
    Supports both streaming (JSONL) and non-streaming (JSON) modes.

    Usage:
        captured = []
        client = CapturingSpiderClient(AsyncSpider(api_key), captured)
        async with client:
            response = client.scrape_url(url, params=params, stream=True)
            async for item in response:
                # process item
        # captured now contains request/response data
    """

    def __init__(
        self,
        real_client: Any,
        captured_scrapes: List[Dict[str, Any]],
    ) -> None:
        self._real = real_client
        self._captured = captured_scrapes

    async def __aenter__(self) -> "CapturingSpiderClient":
        await self._real.__aenter__()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self._real.__aexit__(exc_type, exc_val, exc_tb)

    def scrape_url(
        self,
        url: str,
        *,
        params: Dict[str, Any],
        stream: bool = True,
        content_type: str = "application/json",
    ) -> Any:
        """Scrape URL and capture the request/response.

        Returns an async iterator for streaming mode, or an awaitable for non-streaming.
        The capture is updated with the full response after iteration completes.
        """
        response = self._real.scrape_url(
            url,
            params=params,
            stream=stream,
            content_type=content_type,
        )

        capture: Dict[str, Any] = {
            "request": {
                "url": url,
                "params": params,
                "stream": stream,
                "contentType": content_type,
            },
            "response": [],
        }
        self._captured.append(capture)

        if stream:
            # Return a streaming iterator that captures responses
            async def _capturing_iterator():
                items: List[Any] = []
                try:
                    if hasattr(response, "__aiter__"):
                        async for item in response:
                            items.append(item)
                            yield item
                    elif hasattr(response, "__await__"):
                        result = await response
                        if result is not None:
                            items.append(result)
                            yield result
                    elif response is not None:
                        items.append(response)
                        yield response
                finally:
                    capture["response"] = items

            return _capturing_iterator()
        else:
            # Return an awaitable that captures the response
            async def _capturing_awaitable():
                try:
                    # Handle async generators (Spider SDK sometimes returns these even with stream=False)
                    if hasattr(response, "__aiter__"):
                        items = []
                        async for item in response:
                            items.append(item)
                        result = items[0] if len(items) == 1 else items
                        capture["response"] = result
                        return result
                    elif hasattr(response, "__await__"):
                        result = await response
                    else:
                        result = response
                    capture["response"] = result
                    return result
                except Exception as exc:
                    capture["error"] = str(exc)
                    raise

            return _capturing_awaitable()
