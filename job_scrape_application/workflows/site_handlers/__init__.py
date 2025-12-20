from __future__ import annotations

from typing import Iterable, List

from .ashby import AshbyHqHandler
from .base import BaseSiteHandler
from .github_careers import GithubCareersHandler
from .greenhouse import GreenhouseHandler

_HANDLER_CLASSES = (AshbyHqHandler, GithubCareersHandler, GreenhouseHandler)


def get_site_handler(url: str | None = None, site_type: str | None = None) -> BaseSiteHandler | None:
    if not url and not site_type:
        return None
    for handler_cls in _HANDLER_CLASSES:
        handler = handler_cls()
        if handler.matches_site(site_type, url or ""):
            return handler
    return None


def get_site_handlers_for_urls(
    urls: Iterable[str], site_type: str | None = None
) -> List[BaseSiteHandler]:
    handlers: List[BaseSiteHandler] = []
    seen: set[str] = set()
    for url in urls:
        handler = get_site_handler(url, site_type)
        if handler and handler.name not in seen:
            seen.add(handler.name)
            handlers.append(handler)
    return handlers


__all__ = [
    "AshbyHqHandler",
    "BaseSiteHandler",
    "GithubCareersHandler",
    "GreenhouseHandler",
    "get_site_handler",
    "get_site_handlers_for_urls",
]
