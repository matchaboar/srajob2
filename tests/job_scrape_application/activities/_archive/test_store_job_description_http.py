from __future__ import annotations

import orjson
import sys
import types
from pathlib import Path
from typing import Any, Dict, List

try:
    import firecrawl  # noqa: F401
    import firecrawl.v2.types  # noqa: F401
    import firecrawl.v2.utils.error_handler  # noqa: F401
except Exception:
    firecrawl_mod = types.ModuleType("firecrawl")
    setattr(firecrawl_mod, "Firecrawl", type("Firecrawl", (), {}))
    sys.modules.setdefault("firecrawl", firecrawl_mod)
    firecrawl_v2 = types.ModuleType("firecrawl.v2")
    firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")
    setattr(firecrawl_v2_types, "PaginationConfig", type("PaginationConfig", (), {}))
    setattr(firecrawl_v2_types, "ScrapeOptions", type("ScrapeOptions", (), {}))
    sys.modules.setdefault("firecrawl.v2", firecrawl_v2)
    sys.modules.setdefault("firecrawl.v2.types", firecrawl_v2_types)
    firecrawl_v2_utils = types.ModuleType("firecrawl.v2.utils")
    firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")
    setattr(
        firecrawl_v2_utils_error,
        "PaymentRequiredError",
        type("PaymentRequiredError", (Exception,), {}),
    )
    setattr(
        firecrawl_v2_utils_error,
        "RequestTimeoutError",
        type("RequestTimeoutError", (Exception,), {}),
    )
    sys.modules.setdefault("firecrawl.v2.utils", firecrawl_v2_utils)
    sys.modules.setdefault("firecrawl.v2.utils.error_handler", firecrawl_v2_utils_error)
    setattr(firecrawl_v2_utils, "error_handler", firecrawl_v2_utils_error)
fetchfox_mod = types.ModuleType("fetchfox_sdk")
setattr(fetchfox_mod, "FetchFox", type("FetchFox", (), {}))
sys.modules.setdefault("fetchfox_sdk", fetchfox_mod)

try:
    import temporalio  # noqa: F401
except ImportError:  # pragma: no cover
    temporalio = types.ModuleType("temporalio")
    sys.modules.setdefault("temporalio", temporalio)

    class _Activity:
        def defn(self, fn=None, **kwargs):
            if fn is None:
                def wrapper(func):
                    return func

                return wrapper
            return fn

    setattr(temporalio, "activity", _Activity())
    sys.modules.setdefault("temporalio.activity", temporalio)

    temporalio_exceptions = types.ModuleType("temporalio.exceptions")
    setattr(
        temporalio_exceptions,
        "ApplicationError",
        type("ApplicationError", (Exception,), {}),
    )
    sys.modules.setdefault("temporalio.exceptions", temporalio_exceptions)


from job_scrape_application.workflows import activities as acts  # noqa: E402


def test_store_job_descriptions_via_http_posts_payload(monkeypatch):
    """Test that job descriptions are posted via HTTP step function."""
    calls: List[Dict[str, Any]] = []
    looked_up: List[Dict[str, Any]] = []

    def fake_lookup_job_id_for_url(url: str) -> str | None:
        looked_up.append({"url": url})
        return "job-123"

    def fake_store_job_description_step(base_url: str, job_id: str, description: str) -> bool:
        calls.append({"base_url": base_url, "job_id": job_id, "description": description})
        return True

    monkeypatch.setattr(acts.settings, "convex_http_url", "https://example.convex.site")
    monkeypatch.setattr(acts, "_lookup_job_id_for_url", fake_lookup_job_id_for_url)
    monkeypatch.setattr(acts, "store_job_description_step", fake_store_job_description_step)

    jobs = [
        {
            "url": "https://example.com/job/123/",
            "description": "Full description",
        },
        {
            "url": "https://example.com/job/empty",
            "description": " ",
        },
    ]

    acts._store_job_descriptions_via_http(
        jobs,
        "https://example.com",
        "spidercloud",
        "workflow",
    )

    # Only one job had valid description
    assert len(looked_up) == 1
    assert looked_up[0]["url"] == "https://example.com/job/123"
    assert calls == [
        {
            "base_url": "https://example.convex.site",
            "job_id": "job-123",
            "description": "Full description",
        }
    ]


def test_store_job_descriptions_via_http_skips_truncated_description(monkeypatch):
    """Test that truncated descriptions are skipped."""
    calls: List[Dict[str, Any]] = []
    looked_up: List[Dict[str, Any]] = []

    def fake_lookup_job_id_for_url(url: str) -> str | None:
        looked_up.append({"url": url})
        return "job-123"

    def fake_store_job_description_step(base_url: str, job_id: str, description: str) -> bool:
        calls.append({"base_url": base_url, "job_id": job_id, "description": description})
        return True

    fixture = Path("tests/fixtures/spidercloud_affable_kiwi_job_detail_raw.json")
    payload = orjson.loads(fixture.read_text(encoding="utf-8"))
    entry = payload[0][0] if isinstance(payload[0], list) else payload[0]
    snippet = entry["metadata"]["raw"]["description"]

    monkeypatch.setattr(acts.settings, "convex_http_url", "https://example.convex.site")
    monkeypatch.setattr(acts, "_lookup_job_id_for_url", fake_lookup_job_id_for_url)
    monkeypatch.setattr(acts, "store_job_description_step", fake_store_job_description_step)

    jobs = [
        {
            "url": entry["url"],
            "description": snippet,
        }
    ]

    acts._store_job_descriptions_via_http(
        jobs,
        "https://affable-kiwi-46.convex.site",
        "spidercloud",
        "workflow",
    )

    # Truncated description should be skipped - no lookups, no calls
    assert looked_up == []
    assert calls == []


def test_store_job_descriptions_via_http_uses_greenhouse_api_lookup(monkeypatch):
    """Test that Greenhouse URLs are normalized before lookup."""
    calls: List[Dict[str, Any]] = []
    looked_up: List[str] = []

    def fake_lookup_job_id_for_url(url: str) -> str | None:
        looked_up.append(url)
        # The function receives the normalized URL (trailing slash removed)
        if url == "https://boards.greenhouse.io/stubhubinc/jobs/4716145101":
            return "job-4716145101"
        return None

    def fake_store_job_description_step(base_url: str, job_id: str, description: str) -> bool:
        calls.append({"base_url": base_url, "job_id": job_id, "description": description})
        return True

    monkeypatch.setattr(acts.settings, "convex_http_url", "https://example.convex.site")
    monkeypatch.setattr(acts, "_lookup_job_id_for_url", fake_lookup_job_id_for_url)
    monkeypatch.setattr(acts, "store_job_description_step", fake_store_job_description_step)

    jobs = [
        {
            "url": "https://boards.greenhouse.io/stubhubinc/jobs/4716145101",
            "description": "Full description",
        }
    ]

    acts._store_job_descriptions_via_http(
        jobs,
        "https://boards.greenhouse.io/stubhubinc",
        "spidercloud",
        "workflow",
    )

    # Should have looked up the normalized URL
    assert "https://boards.greenhouse.io/stubhubinc/jobs/4716145101" in looked_up
    assert calls == [
        {
            "base_url": "https://example.convex.site",
            "job_id": "job-4716145101",
            "description": "Full description",
        }
    ]
