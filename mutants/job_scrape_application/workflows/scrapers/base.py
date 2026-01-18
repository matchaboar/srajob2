from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from temporalio.exceptions import ApplicationError

if TYPE_CHECKING:
    from ..activities import Site


class BaseScraper:
    """Common interface for provider-specific scrapers."""

    provider: str = "unknown"

    async def scrape_site(
        self,
        site: Site,
        *,
        skip_urls: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        raise NotImplementedError("scrape_site must be implemented by scraper classes")

    def supports_greenhouse(self) -> bool:
        return False

    async def fetch_greenhouse_listing(self, site: Site) -> Dict[str, Any]:
        raise ApplicationError("Greenhouse scraping not supported", non_retryable=True)

    async def scrape_greenhouse_jobs(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise ApplicationError("Greenhouse scraping not supported", non_retryable=True)
