"""Dependency container for workflow dependency injection.

The DependencyContainer provides a unified way to inject external dependencies
(Convex, Queue, Settings) into workflows and activities. This enables:

1. Production mode: Use real services
2. Testing mode: Use mock implementations with fixtures
3. Capture mode: Use real services but capture requests/responses for fixture generation

Example usage:
    # Production (default)
    deps = DependencyContainer.production()

    # Testing with fixtures
    deps = DependencyContainer.testing(
        fixtures={"router:listSites": [{"id": "abc", "name": "Test"}]},
        captured_mutations=captured,
    )

    # Use in workflow/activity
    result = await deps.convex.query("router:listSites", {})
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping

from .protocols import ConvexFunctions, QueueServiceProtocol, RuntimeConfigProtocol, SettingsProtocol


@dataclass
class DependencyContainer:
    """Container for all external workflow dependencies.

    This provides a single place to configure whether workflows use:
    - Real production services
    - Mock implementations for testing
    - Capturing wrappers for fixture generation

    Attributes:
        convex: Convex database functions (query/mutation/action)
        queue: Queue service for URL processing
        settings: Application settings
        runtime_config: Runtime configuration values
        spider_client_factory: Factory to create SpiderCloud clients

    """

    convex: ConvexFunctions | None = None
    queue: QueueServiceProtocol | None = None
    settings: SettingsProtocol | None = None
    runtime_config: RuntimeConfigProtocol | None = None
    spider_client_factory: Callable[[], Any] | None = None

    # Capture lists for testing
    _captured_queries: List[Dict[str, Any]] = field(default_factory=list)
    _captured_mutations: List[Dict[str, Any]] = field(default_factory=list)
    _captured_scrapes: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def production(cls) -> "DependencyContainer":
        """Create a container with production dependencies.

        Uses lazy loading for Convex to support Temporal sandbox environments.
        """
        from ...config import settings
        from ...config.runtime_config import runtime_config
        from ...dbos_runtime import queue as dbos_queue

        return cls(
            convex=ConvexFunctions.from_module(),
            queue=_QueueModuleWrapper(dbos_queue),
            settings=settings,
            runtime_config=runtime_config,
            spider_client_factory=_create_production_spider_client,
        )

    @classmethod
    def testing(
        cls,
        *,
        query_fixtures: Dict[str, Any] | None = None,
        mutation_fixtures: Dict[str, Any] | None = None,
        captured_queries: List[Dict[str, Any]] | None = None,
        captured_mutations: List[Dict[str, Any]] | None = None,
        captured_scrapes: List[Dict[str, Any]] | None = None,
        settings_overrides: Dict[str, Any] | None = None,
        spider_responses: Dict[str, Any] | None = None,
    ) -> "DependencyContainer":
        """Create a container with mock dependencies for testing.

        Args:
            query_fixtures: Dict mapping query names to return values
            mutation_fixtures: Dict mapping mutation names to return values
            captured_queries: List to capture query calls
            captured_mutations: List to capture mutation calls
            captured_scrapes: List to capture spider scrape calls
            settings_overrides: Override specific settings values
            spider_responses: Dict mapping URLs to spider responses

        Returns:
            DependencyContainer configured for testing
        """
        from .mock_clients import MockConvexFunctions, MockQueueService, MockSettings, MockSpiderClientFactory

        queries = captured_queries if captured_queries is not None else []
        mutations = captured_mutations if captured_mutations is not None else []
        scrapes = captured_scrapes if captured_scrapes is not None else []

        mock_convex = MockConvexFunctions(
            query_fixtures=query_fixtures or {},
            mutation_fixtures=mutation_fixtures or {},
            captured_queries=queries,
            captured_mutations=mutations,
        )

        return cls(
            convex=mock_convex.as_functions(),
            queue=MockQueueService(),
            settings=MockSettings(overrides=settings_overrides or {}),
            runtime_config=None,  # Use defaults
            spider_client_factory=MockSpiderClientFactory(
                responses=spider_responses or {},
                captured_scrapes=scrapes,
            ),
            _captured_queries=queries,
            _captured_mutations=mutations,
            _captured_scrapes=scrapes,
        )

    @classmethod
    def capturing(
        cls,
        *,
        captured_queries: List[Dict[str, Any]] | None = None,
        captured_mutations: List[Dict[str, Any]] | None = None,
        captured_scrapes: List[Dict[str, Any]] | None = None,
    ) -> "DependencyContainer":
        """Create a container that uses production services but captures all calls.

        Useful for generating test fixtures from production runs.

        Args:
            captured_queries: List to capture query calls and responses
            captured_mutations: List to capture mutation calls and responses
            captured_scrapes: List to capture spider scrape calls and responses

        Returns:
            DependencyContainer with capturing wrappers
        """
        from ...config import settings
        from ...config.runtime_config import runtime_config
        from ...dbos_runtime import queue as dbos_queue

        queries = captured_queries if captured_queries is not None else []
        mutations = captured_mutations if captured_mutations is not None else []
        scrapes = captured_scrapes if captured_scrapes is not None else []

        return cls(
            convex=_create_capturing_convex_functions(queries, mutations),
            queue=_QueueModuleWrapper(dbos_queue),
            settings=settings,
            runtime_config=runtime_config,
            spider_client_factory=_create_capturing_spider_factory(scrapes),
            _captured_queries=queries,
            _captured_mutations=mutations,
            _captured_scrapes=scrapes,
        )

    # Convenience methods for accessing services

    async def query(self, name: str, args: Mapping[str, Any] | None = None) -> Any:
        """Execute a Convex query."""
        if self.convex is None:
            raise RuntimeError("Convex not configured in DependencyContainer")
        return await self.convex.query(name, args)

    async def mutation(self, name: str, args: Mapping[str, Any] | None = None) -> Any:
        """Execute a Convex mutation."""
        if self.convex is None:
            raise RuntimeError("Convex not configured in DependencyContainer")
        return await self.convex.mutation(name, args)

    def enqueue_urls(self, payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        """Add URLs to the scrape queue."""
        if self.queue is None:
            raise RuntimeError("Queue not configured in DependencyContainer")
        return self.queue.enqueue_scrape_urls(payload, force_refresh=force_refresh)

    def get_spider_client(self) -> Any:
        """Get a SpiderCloud client instance."""
        if self.spider_client_factory is None:
            raise RuntimeError("Spider client factory not configured in DependencyContainer")
        return self.spider_client_factory()

    @property
    def captured_queries(self) -> List[Dict[str, Any]]:
        """Get captured Convex queries (for testing/fixture generation)."""
        return self._captured_queries

    @property
    def captured_mutations(self) -> List[Dict[str, Any]]:
        """Get captured Convex mutations (for testing/fixture generation)."""
        return self._captured_mutations

    @property
    def captured_scrapes(self) -> List[Dict[str, Any]]:
        """Get captured spider scrapes (for testing/fixture generation)."""
        return self._captured_scrapes


class _QueueModuleWrapper:
    """Wrapper to make the queue module satisfy QueueServiceProtocol."""

    def __init__(self, queue_module: Any) -> None:
        self._queue = queue_module

    def enqueue_scrape_urls(
        self,
        payload: Dict[str, Any],
        *,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        return self._queue.enqueue_scrape_urls(payload, force_refresh=force_refresh)

    def lease_scrape_url_batch(
        self,
        *,
        provider: str | None = None,
        limit: int = 50,
        url_type: str | None = None,
    ) -> Any:
        return self._queue.lease_scrape_url_batch(provider=provider, limit=limit, url_type=url_type)

    def complete_scrape_urls(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._queue.complete_scrape_urls(payload)

    def fail_scrape_urls(
        self,
        payload: Dict[str, Any],
        *,
        error: str | None = None,
    ) -> Dict[str, Any]:
        return self._queue.fail_scrape_urls(payload, error=error)

    def queue_status(self) -> Dict[str, Any]:
        return self._queue.queue_status()

    def detail_queue_has_pending(self, *, include_processing: bool = False) -> bool:
        return self._queue.detail_queue_has_pending(include_processing=include_processing)

    def list_scrape_urls(
        self,
        *,
        provider: str | None = None,
        site_id: str | None = None,
        status: str | None = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        return self._queue.list_scrape_urls(
            provider=provider,
            site_id=site_id,
            status=status,
            limit=limit,
        )


def _create_production_spider_client() -> Any:
    """Create a production SpiderCloud client."""
    from spider import AsyncSpider

    from ...config import settings

    api_key = settings.spider_api_key
    if not api_key:
        raise RuntimeError("Spider API key not configured")
    return AsyncSpider(api_key=api_key)


def _create_capturing_convex_functions(
    captured_queries: List[Dict[str, Any]],
    captured_mutations: List[Dict[str, Any]],
) -> ConvexFunctions:
    """Create Convex functions that capture calls for fixture generation."""
    from ...services.convex_client import convex_action, convex_mutation, convex_query

    async def capturing_query(name: str, args: Mapping[str, Any] | None = None) -> Any:
        result = await convex_query(name, args)
        captured_queries.append({"name": name, "args": dict(args) if args else {}, "result": result})
        return result

    async def capturing_mutation(name: str, args: Mapping[str, Any] | None = None) -> Any:
        result = await convex_mutation(name, args)
        captured_mutations.append({"name": name, "args": dict(args) if args else {}, "result": result})
        return result

    return ConvexFunctions(
        query=capturing_query,
        mutation=capturing_mutation,
        action=convex_action,
    )


def _create_capturing_spider_factory(captured_scrapes: List[Dict[str, Any]]) -> Callable[[], Any]:
    """Create a spider client factory that captures requests/responses."""
    from .mock_clients import CapturingSpiderClient

    def factory() -> CapturingSpiderClient:
        from spider import AsyncSpider

        from ...config import settings

        api_key = settings.spider_api_key
        if not api_key:
            raise RuntimeError("Spider API key not configured")
        real_client = AsyncSpider(api_key=api_key)
        return CapturingSpiderClient(real_client, captured_scrapes)

    return factory
