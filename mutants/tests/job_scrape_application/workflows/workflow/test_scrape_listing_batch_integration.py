"""Integration tests for scrape_listing_batch production workflow."""

from __future__ import annotations

import orjson
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

from job_scrape_application.config import runtime_config
from job_scrape_application.dbos_runtime import sqlite as dbos_sqlite
from job_scrape_application.workflows.site_handlers import get_site_handler
from job_scrape_application.workflows.result import Success
from job_scrape_application.workflows.workflow import scrape_listing_batch
from job_scrape_application.workflows.workflow.test_utils import SpiderFixture
from job_scrape_application.workflows.spidercloud.types import UnrenderedHttpException
from job_scrape_application.workflows.workflow.scrape_listing_batch import _extract_job_urls_from_scrape

SCHEDULE_PATH = Path("job_scrape_application/config/prod/site_schedules.yml")
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/debug")
GROUND_TRUTH_DIR = Path("tests/job_scrape_application/workflows/ground_truth/debug")
UNRENDERED_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/unrendered")
ALLOW_FIXTURE_GENERATION = os.getenv("GENERATE_LISTING_FIXTURES", "").lower() in {"1", "true", "yes"}


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_") or "site"


def _load_schedule_entries() -> tuple[dict[str, Any], ...]:
    """Load and cache site schedule entries.

    Returns a tuple (for hashability/caching) of schedule entries.
    """
    if not SCHEDULE_PATH.exists():
        return ()
    payload = yaml.safe_load(SCHEDULE_PATH.read_text(encoding="utf-8"))
    entries = payload if isinstance(payload, list) else payload.get("site_schedules", [])
    return tuple(
        entry
        for entry in entries
        if isinstance(entry, dict) and entry.get("enabled", True)
    )


def _latest_listing_fixture(slug: str) -> Path | None:
    company_dir = FIXTURE_DIR / slug
    if not company_dir.exists():
        return None
    candidates = sorted(company_dir.glob("*_listing.json"), reverse=True)
    return candidates[0] if candidates else None


def _find_fixture_and_ground_truth(slug: str) -> tuple[Path | None, Path | None]:
    company_dir = FIXTURE_DIR / slug
    ground_dir = GROUND_TRUTH_DIR / slug
    if not company_dir.exists():
        return None, None
    for fixture_path in sorted(company_dir.glob("*_listing.json"), reverse=True):
        ground_path = ground_dir / f"{fixture_path.stem}.yml"
        if ground_path.exists():
            return fixture_path, ground_path
    return None, None


async def _generate_listing_fixture(entry: dict[str, Any], slug: str) -> Path | None:
    from agent_scripts.core.fetch_spidercloud_fixtures import (
        _capture_workflow_scrape,
        _extract_listing_job_urls,
    )
    from dotenv import load_dotenv

    listing_url = entry.get("url")
    if not isinstance(listing_url, str) or not listing_url.strip():
        return None

    load_dotenv()
    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        return None

    handler = get_site_handler(listing_url)
    handler_name = handler.name if handler else _slugify(listing_url)
    company_dir = FIXTURE_DIR / slug
    company_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    fixture_path = company_dir / f"{handler_name}_{timestamp}_listing.json"

    object.__setattr__(runtime_config, "spidercloud_single_request_mode", True)
    capture, listing_payload = await _capture_workflow_scrape(
        listing_url,
        source_url=listing_url,
        pattern=entry.get("pattern"),
        label=f"{slug}_listing",
    )
    scraped_urls = _extract_job_urls_from_scrape(listing_payload)
    listing_job_urls = _extract_listing_job_urls(listing_payload, listing_url, entry.get("pattern"))
    if not listing_job_urls:
        raw_response = capture.get("response")
        if raw_response:
            listing_job_urls = _extract_listing_job_urls(
                {"items": {"raw": raw_response}, "sourceUrl": listing_url},
                listing_url,
                entry.get("pattern"),
            )
            if not scraped_urls:
                scraped_urls = _extract_job_urls_from_scrape(
                    {"items": {"raw": raw_response}, "sourceUrl": listing_url}
                )
    apply_urls = _derive_apply_urls(listing_job_urls, listing_url)
    fixture_payload = {
        "scraped_urls": sorted(set(scraped_urls)),
        "normalized_urls": sorted(set(listing_job_urls)),
        "apply_urls": sorted(set(apply_urls)),
        **capture,
    }
    fixture_path.write_text(
        orjson.dumps(fixture_payload, option=orjson.OPT_INDENT_2).decode("utf-8"),
        encoding="utf-8",
    )
    return fixture_path


_schedule_entries = _load_schedule_entries()
_listing_params: list[tuple[str, dict[str, Any]]] = []
for entry in _schedule_entries:
    name = str(entry.get("name") or entry.get("url") or "")
    slug = _slugify(name)
    _listing_params.append((slug, entry))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "slug,entry",
    _listing_params,
    ids=[slug for slug, _ in _listing_params] if _listing_params else ["no_sites"],
)
async def test_scrape_listing_batch_enqueues_detail_urls(
    slug: str,
    entry: dict[str, Any],
    workflow_test,
) -> None:
    listing_url = entry.get("url")
    if not isinstance(listing_url, str) or not listing_url.strip():
        pytest.skip("Missing listing URL in schedule entry")
        return

    fixture_path, ground_truth_path = _find_fixture_and_ground_truth(slug)
    if fixture_path is None or ground_truth_path is None:
        if not ALLOW_FIXTURE_GENERATION:
            pytest.skip(f"Missing fixture/ground truth for {slug}; set GENERATE_LISTING_FIXTURES=1")
            return
        fixture_path = await _generate_listing_fixture(entry, slug)
        if fixture_path is None or not fixture_path.exists():
            pytest.skip(f"No fixture for {slug}; set SPIDER_API_KEY to generate")
            return
        ground_truth_path = GROUND_TRUTH_DIR / slug / f"{fixture_path.stem}.yml"

    fixture = SpiderFixture.from_file(fixture_path)
    workflow_test.with_spider_fixture(fixture)
    workflow_test.with_query_response("router:listSeenJobUrlsForSite", {"urls": []})

    batch = {
        "urls": [
            {
                "url": listing_url,
                "sourceUrl": listing_url,
                "siteId": slug,
                "pattern": entry.get("pattern"),
            }
        ]
    }

    result = await workflow_test.run(scrape_listing_batch, batch=batch)

    assert isinstance(result, Success)
    assert result.value.queued > 0

    enqueued_urls = _load_enqueued_urls()
    normalized_urls = _dedupe_urls(enqueued_urls)
    scraped_urls = _extract_scraped_urls(workflow_test)
    apply_urls = _derive_apply_urls(normalized_urls, listing_url)

    expected = _load_expected_listing(ground_truth_path)
    if expected:
        if ALLOW_FIXTURE_GENERATION:
            handler_name, handler_class = _handler_label(listing_url)
            _write_listing_ground_truth(
                ground_truth_path,
                listing_url=listing_url,
                handler_name=handler_name,
                handler_class=handler_class,
                scraped_urls=scraped_urls,
                normalized_urls=normalized_urls,
                apply_urls=apply_urls,
            )
            return
        expected_normalized = expected.get("normalized_urls") or expected.get("expected_urls")
        if isinstance(expected_normalized, list):
            _assert_expected_urls(normalized_urls, expected_normalized)
        expected_scraped = expected.get("scraped_urls")
        if isinstance(expected_scraped, list):
            _assert_expected_urls(scraped_urls, expected_scraped)
        expected_apply = expected.get("apply_urls")
        if isinstance(expected_apply, list):
            _assert_expected_urls(apply_urls, expected_apply)
        blocked_urls = expected.get("blocked_urls")
        if isinstance(blocked_urls, list) and blocked_urls:
            _assert_blocked_urls(enqueued_urls, blocked_urls)
        return

    if not ALLOW_FIXTURE_GENERATION:
        pytest.skip(f"Missing expected URLs for {slug}; set GENERATE_LISTING_FIXTURES=1")
        return

    handler_name, handler_class = _handler_label(listing_url)
    _write_listing_ground_truth(
        ground_truth_path,
        listing_url=listing_url,
        handler_name=handler_name,
        handler_class=handler_class,
        scraped_urls=scraped_urls,
        normalized_urls=normalized_urls,
        apply_urls=apply_urls,
    )


def _find_unrendered_fixtures() -> list[Path]:
    fixtures: list[Path] = []
    if not UNRENDERED_FIXTURE_DIR.exists():
        return fixtures
    for fixture_path in UNRENDERED_FIXTURE_DIR.glob("*_listing.json"):
        if fixture_path.is_file():
            fixtures.append(fixture_path)
    return sorted(fixtures, reverse=True)


_unrendered_fixtures = _find_unrendered_fixtures()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _unrendered_fixtures,
    reason="No unrendered listing fixtures available",
)
@pytest.mark.parametrize(
    "fixture_path",
    _unrendered_fixtures,
    ids=[path.stem for path in _unrendered_fixtures],
)
async def test_unrendered_listing_fixture_raises(
    fixture_path: Path,
    workflow_test,
) -> None:
    if not fixture_path.exists():
        pytest.skip("Unrendered listing fixture missing on disk")
        return

    # Prefer URL from fixture metadata if present.
    raw_payload = orjson.loads(fixture_path.read_text(encoding="utf-8"))
    request_url = raw_payload.get("request", {}).get("url")
    if not isinstance(request_url, str) or not request_url.strip():
        pytest.skip("Unrendered fixture missing request URL")
        return
    listing_url = request_url
    slug = fixture_path.parent.name

    fixture = SpiderFixture.from_file(fixture_path)
    workflow_test.with_spider_fixture(fixture)
    workflow_test.with_query_response("router:listSeenJobUrlsForSite", {"urls": []})

    batch = {
        "urls": [
            {
                "url": listing_url,
                "sourceUrl": listing_url,
                "siteId": slug,
                "pattern": None,
            }
        ]
    }

    with pytest.raises(UnrenderedHttpException):
        await workflow_test.run(scrape_listing_batch, batch=batch)


def _load_enqueued_urls() -> list[str]:
    try:
        with dbos_sqlite.read_only() as conn:
            rows = conn.execute(
                "SELECT url FROM queue_items WHERE queue_name = ?",
                ("detail",),
            ).fetchall()
    except sqlite3.OperationalError:
        return []
    urls = []
    for row in rows:
        url = row["url"]
        if isinstance(url, str) and url.strip():
            urls.append(url)
    return urls


def _handler_label(listing_url: str) -> tuple[str, str]:
    handler = get_site_handler(listing_url)
    if handler:
        return handler.name, handler.__class__.__name__
    return "unknown", "Handler"


def _load_expected_listing(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    expected = payload.get("expected")
    return expected if isinstance(expected, dict) else None


def _assert_expected_urls(actual: list[str], expected: list[str]) -> None:
    actual_list = [url for url in actual if isinstance(url, str)]
    actual_set = set(actual_list)
    expected_set = {url for url in expected if isinstance(url, str)}
    if len(actual_set) != len(actual_list):
        duplicates = sorted({url for url in actual_list if actual_list.count(url) > 1})
        raise AssertionError(
            "Duplicate URLs were enqueued:\n" + "\n".join(f"  - {url}" for url in duplicates[:20])
        )
    if actual_set != expected_set:
        unexpected = sorted(actual_set - expected_set)
        missing = sorted(expected_set - actual_set)
        message = []
        if unexpected:
            message.append("Unexpected URLs extracted:")
            message.extend(f"  - {url}" for url in unexpected[:20])
        if missing:
            message.append("Missing expected URLs:")
            message.extend(f"  - {url}" for url in missing[:20])
        raise AssertionError("\n".join(message))


def _assert_blocked_urls(actual: list[str], blocked: list[str]) -> None:
    actual_set = {url for url in actual if isinstance(url, str)}
    blocked_set = {url for url in blocked if isinstance(url, str)}
    invalid = sorted(actual_set & blocked_set)
    if invalid:
        raise AssertionError(
            "Blocked URLs were enqueued:\n" + "\n".join(f"  - {url}" for url in invalid)
        )


def _dedupe_urls(urls: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if not isinstance(url, str) or not url.strip():
            continue
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _extract_scraped_urls(workflow_test) -> list[str]:
    scrape_calls = workflow_test.captured.calls.get("scrape_listing_urls", [])
    if not scrape_calls:
        return []
    scrape_result = scrape_calls[0].get("result")
    scrape_payload = scrape_result.get("scrape") if isinstance(scrape_result, dict) else None
    if not isinstance(scrape_payload, dict):
        scrape_payload = scrape_result if isinstance(scrape_result, dict) else {}
    if not scrape_payload:
        return []
    return _extract_job_urls_from_scrape(scrape_payload)


def _derive_apply_urls(urls: list[str], listing_url: str) -> list[str]:
    handler = get_site_handler(listing_url)
    apply_urls: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if not isinstance(url, str) or not url.strip():
            continue
        candidate = None
        if handler and hasattr(handler, "get_company_uri"):
            try:
                candidate = handler.get_company_uri(url)
            except Exception:
                candidate = None
        if candidate and handler and hasattr(handler, "is_listing_url"):
            try:
                if handler.is_listing_url(candidate):
                    candidate = None
            except Exception:
                candidate = None
        final_url = candidate or url
        if final_url in seen:
            continue
        seen.add(final_url)
        apply_urls.append(final_url)
    return apply_urls


def _write_listing_ground_truth(
    path: Path,
    *,
    listing_url: str,
    handler_name: str,
    handler_class: str,
    scraped_urls: list[str],
    normalized_urls: list[str],
    apply_urls: list[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    scraped_urls = sorted({url for url in scraped_urls if isinstance(url, str) and url.strip()})
    normalized_urls = sorted({url for url in normalized_urls if isinstance(url, str) and url.strip()})
    apply_urls = sorted({url for url in apply_urls if isinstance(url, str) and url.strip()})
    payload = {
        "site_id": handler_name,
        "listing_url": listing_url,
        "expected": {
            "url_count": len(normalized_urls),
            "no_listing_urls": True,
            "handler": handler_class,
            "scraped_urls": scraped_urls,
            "normalized_urls": normalized_urls,
            "apply_urls": apply_urls,
            "blocked_urls": [],
        },
    }
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
