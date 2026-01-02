from __future__ import annotations

from urllib.parse import urlparse

from .base import BaseSiteHandler


class ConfluentHandler(BaseSiteHandler):
    name = "confluent"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host.endswith("confluent.io"):
            return False
        path = (parsed.path or "").lower()
        return path.startswith("/jobs")

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if not host.endswith("confluent.io"):
            return False
        path = (parsed.path or "").lower()
        if not path.startswith("/jobs"):
            return False
        return "/jobs/job/" not in path
