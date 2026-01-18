from __future__ import annotations

import orjson
from pathlib import Path
from typing import Any


from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    parse_posted_at_with_unknown,
)
from job_scrape_application.workflows.site_handlers.snapchat_careers import (  # noqa: E402
    SnapchatCareersHandler,
)
from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    SpiderCloudScraper,
    SpidercloudDependencies,
)

FIXTURE_PATH = Path(
    "tests/job_scrape_application/workflows/fixtures/spidercloud_snapchat_job_detail_commonmark.json"
)
JOB_URL = "https://careers.snap.com/job?id=R0042515"
STARTED_AT = 1_700_000_000_000


def _make_scraper() -> SpiderCloudScraper:
    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=lambda payload: payload,
        settings=type("cfg", (), {"spider_api_key": "key"}),
        fetch_seen_urls_for_site=lambda *_args, **_kwargs: [],
    )
    return SpiderCloudScraper(deps)


def _load_spidercloud_fixture(path: Path) -> Any:
    payload = orjson.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        return payload.get("response")
    return payload


def _normalize_snapchat_job() -> dict[str, Any]:
    payload = _load_spidercloud_fixture(FIXTURE_PATH)
    event = payload[0][0]
    markdown = event.get("content", {}).get("commonmark", "")
    scraper = _make_scraper()
    normalized = scraper._normalize_job(JOB_URL, markdown, [event], STARTED_AT, require_keywords=False)
    assert normalized is not None
    return normalized


def test_spidercloud_snapchat_job_detail_parses_relative_posted_at():
    payload = _load_spidercloud_fixture(FIXTURE_PATH)
    event = payload[0][0]
    markdown = event.get("content", {}).get("commonmark", "")
    raw_posted_at = SnapchatCareersHandler().extract_posted_at_from_markdown(markdown)
    assert raw_posted_at is not None, "expected relative posted-at text in fixture"

    normalized = _normalize_snapchat_job()
    expected_posted_at, expected_unknown = parse_posted_at_with_unknown(
        raw_posted_at,
        now_ms=STARTED_AT,
    )

    assert normalized["posted_at"] == expected_posted_at
    assert normalized["posted_at_unknown"] is expected_unknown
