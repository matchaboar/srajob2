"""Protocol definitions for dependency injection.

These protocols define the interfaces that can be mocked for testing.
Production implementations use the real services, test implementations
use mock/fake versions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Protocol,
    runtime_checkable,
)

if TYPE_CHECKING:
    from ...dbos_runtime.queue import LeaseResult


@runtime_checkable
class ConvexClientProtocol(Protocol):
    """Protocol for Convex database operations."""

    async def query(self, name: str, args: Mapping[str, Any] | None = None) -> Any:
        """Execute a Convex query function."""
        ...

    async def mutation(self, name: str, args: Mapping[str, Any] | None = None) -> Any:
        """Execute a Convex mutation function."""
        ...

    async def action(self, name: str, args: Mapping[str, Any] | None = None) -> Any:
        """Execute a Convex action function."""
        ...


@runtime_checkable
class QueueServiceProtocol(Protocol):
    """Protocol for DBOS queue operations."""

    def enqueue_scrape_urls(
        self,
        payload: Dict[str, Any],
        *,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """Add URLs to the scrape queue."""
        ...

    def lease_scrape_url_batch(
        self,
        *,
        provider: str | None = None,
        limit: int = 50,
        url_type: str | None = None,
    ) -> "LeaseResult":
        """Lease a batch of URLs from the queue for processing."""
        ...

    def complete_scrape_urls(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Mark URLs as completed in the queue."""
        ...

    def fail_scrape_urls(
        self,
        payload: Dict[str, Any],
        *,
        error: str | None = None,
    ) -> Dict[str, Any]:
        """Mark URLs as failed in the queue."""
        ...

    def queue_status(self) -> Dict[str, Any]:
        """Get current queue status metrics."""
        ...

    def detail_queue_has_pending(
        self,
        *,
        include_processing: bool = False,
    ) -> bool:
        """Check if detail queue has pending items."""
        ...

    def list_scrape_urls(
        self,
        *,
        provider: str | None = None,
        site_id: str | None = None,
        status: str | None = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """List URLs in the queue with optional filters."""
        ...


@runtime_checkable
class SettingsProtocol(Protocol):
    """Protocol for application settings."""

    convex_url: str | None
    convex_http_url: str | None
    spider_api_key: str | None
    firecrawl_api_key: str | None
    fetchfox_api_key: str | None
    enable_firecrawl: bool
    enable_fetchfox: bool


@runtime_checkable
class RuntimeConfigProtocol(Protocol):
    """Protocol for runtime configuration."""

    spidercloud_job_details_timeout_minutes: int
    spidercloud_job_details_batch_size: int
    spidercloud_listing_batch_size: int
    spidercloud_job_details_concurrency: int


@runtime_checkable
class SpiderClientProtocol(Protocol):
    """Protocol for SpiderCloud client (async context manager)."""

    async def __aenter__(self) -> "SpiderClientProtocol":
        ...

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        ...

    async def scrape_url(
        self,
        url: str,
        params: Dict[str, Any] | None = None,
    ) -> Any:
        """Scrape a single URL."""
        ...


# Type aliases for common function signatures
ConvexQueryFn = Callable[[str, Mapping[str, Any] | None], Awaitable[Any]]
ConvexMutationFn = Callable[[str, Mapping[str, Any] | None], Awaitable[Any]]
ConvexActionFn = Callable[[str, Mapping[str, Any] | None], Awaitable[Any]]


@dataclass
class ConvexFunctions:
    """Container for Convex function references.

    This allows passing Convex functions without requiring a full client instance.
    Useful when activities need lazy-loaded Convex access.
    """

    query: ConvexQueryFn
    mutation: ConvexMutationFn
    action: ConvexActionFn | None = None

    @classmethod
    def from_module(cls) -> "ConvexFunctions":
        """Create from the convex_client module (lazy import)."""
        from ...services.convex_client import convex_action, convex_mutation, convex_query

        return cls(query=convex_query, mutation=convex_mutation, action=convex_action)
