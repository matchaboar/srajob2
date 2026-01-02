from __future__ import annotations

from typing import Iterable, List

from .adobe_careers import AdobeCareersHandler
from .ashby import AshbyHqHandler
from .avature import AvatureHandler
from .base import BaseSiteHandler
from .cisco_careers import CiscoCareersHandler
from .confluent import ConfluentHandler
from .docusign import DocusignHandler
from .github_careers import GithubCareersHandler
from .greenhouse import GreenhouseHandler
from .netflix import NetflixHandler
from .notion_careers import NotionCareersHandler
from .openai_careers import OpenAICareersHandler
from .paloalto_networks import PaloAltoNetworksHandler
from .uber_careers import UberCareersHandler
from .workday import WorkdayHandler

_HANDLER_CLASSES = (
    AdobeCareersHandler,
    AshbyHqHandler,
    AvatureHandler,
    CiscoCareersHandler,
    ConfluentHandler,
    DocusignHandler,
    GithubCareersHandler,
    GreenhouseHandler,
    NetflixHandler,
    NotionCareersHandler,
    OpenAICareersHandler,
    PaloAltoNetworksHandler,
    UberCareersHandler,
    WorkdayHandler,
)


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
    "AdobeCareersHandler",
    "AshbyHqHandler",
    "AvatureHandler",
    "BaseSiteHandler",
    "CiscoCareersHandler",
    "ConfluentHandler",
    "DocusignHandler",
    "GithubCareersHandler",
    "GreenhouseHandler",
    "NetflixHandler",
    "NotionCareersHandler",
    "OpenAICareersHandler",
    "PaloAltoNetworksHandler",
    "UberCareersHandler",
    "WorkdayHandler",
    "get_site_handler",
    "get_site_handlers_for_urls",
]
