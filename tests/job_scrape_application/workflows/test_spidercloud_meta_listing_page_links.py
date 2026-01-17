from __future__ import annotations

import orjson
from pathlib import Path
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

import pytest


# Import store_scrape from workflow module
from job_scrape_application.workflows.workflow.process_spidercloud_job_batch import store_scrape  # noqa: E402

PAGE_LINKS_FIXTURE = (
    Path("tests/job_scrape_application/workflows/fixtures")
    / "spidercloud_meta_careers_listing_page_links.json"
)
LINKS_FIXTURE = (
    Path("tests/job_scrape_application/workflows/fixtures")
    / "spidercloud_meta_careers_listing_links.json"
)
PAGE2_FIXTURE = (
    Path("tests/job_scrape_application/workflows/fixtures")
    / "spidercloud_meta_careers_listing_page2_links.json"
)
PAGE3_FIXTURE = (
    Path("tests/job_scrape_application/workflows/fixtures")
    / "spidercloud_meta_careers_listing_page3_links.json"
)
PAGE4_FIXTURE = (
    Path("tests/job_scrape_application/workflows/fixtures")
    / "spidercloud_meta_careers_listing_page4_links.json"
)
PROD_PAGE1_FIXTURE = (
    Path("tests/job_scrape_application/workflows/fixtures")
    / "spidercloud_meta_careers_prod_listing_page_links.json"
)
PROD_PAGE2_FIXTURE = (
    Path("tests/job_scrape_application/workflows/fixtures")
    / "spidercloud_meta_careers_prod_listing_page2_links.json"
)
PROD_PAGE3_FIXTURE = (
    Path("tests/job_scrape_application/workflows/fixtures")
    / "spidercloud_meta_careers_prod_listing_page3_links.json"
)
PROD_PAGE4_FIXTURE = (
    Path("tests/job_scrape_application/workflows/fixtures")
    / "spidercloud_meta_careers_prod_listing_page4_links.json"
)
GRAPHQL_FIXTURE = (
    Path("tests/job_scrape_application/workflows/fixtures")
    / "spidercloud_meta_careers_listing.json"
)

PROD_SOURCE_URL = (
    "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering"
    "&teams[1]=Research&teams[2]=Enterprise%20Engineering"
    "&teams[3]=Design%20%26%20User%20Experience&teams[4]=Data%20Center"
    "&teams[5]=Data%20%26%20Analytics&teams[6]=Artificial%20Intelligence"
    "&teams[7]=AR%2FVR&offices[0]=Seattle%2C%20WA"
    "&offices[1]=San%20Francisco%2C%20CA&offices[2]=Mesa%2C%20AZ"
    "&offices[3]=Chandler%2C%20AZ"
)


def _load_fixture(path: Path) -> Any:
    payload = orjson.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        return payload.get("response")
    return payload


def _run_store_scrape(
    raw_payload: Any,
    source_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> list[str]:
    calls: list[Dict[str, Any]] = []
    queue_calls: list[Dict[str, Any]] = []

    def fake_mutation(name: str, args: Dict[str, Any]):
        calls.append({"name": name, "args": args})
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            return {"inserted": 0}
        return None

    def fake_enqueue_scrape_urls(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        queue_calls.append(payload)
        return {"queued": len(payload.get("urls", []))}

    monkeypatch.setattr(
        "job_scrape_application.services.convex_client.convex_mutation", fake_mutation
    )
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )

    scrape_payload: Dict[str, Any] = {
        "sourceUrl": source_url,
        "provider": "spidercloud",
        "startedAt": 0,
        "completedAt": 1,
        "items": {"provider": "spidercloud", "raw": raw_payload},
    }

    store_scrape(scrape_payload)

    if not queue_calls:
        return []
    return queue_calls[0]["urls"]


def _has_meta_listing_page(urls: list[str], *, page: int | None) -> bool:
    for url in urls:
        try:
            parsed = urlparse(url)
        except Exception:
            continue
        if (parsed.hostname or "").lower() != "www.metacareers.com":
            continue
        if (parsed.path or "").rstrip("/") != "/jobsearch":
            continue
        params = parse_qs(parsed.query)
        teams = params.get("teams[0]")
        if not teams or teams[0] != "Software Engineering":
            continue
        page_vals = params.get("page")
        if page is None:
            if not page_vals:
                return True
        else:
            if page_vals and page_vals[0] == str(page):
                return True
    return False


def _has_meta_job_detail(urls: list[str], job_id: str) -> bool:
    target = f"https://www.metacareers.com/profile/job_details/{job_id}"
    return any(url == target for url in urls)


def _has_meta_listing_page_for_source(
    urls: list[str],
    base_url: str,
    *,
    page: int | None,
) -> bool:
    try:
        base_parsed = urlparse(base_url)
    except Exception:
        return False
    base_params = parse_qs(base_parsed.query)
    base_params.pop("page", None)
    for url in urls:
        try:
            parsed = urlparse(url)
        except Exception:
            continue
        if (parsed.hostname or "").lower() != "www.metacareers.com":
            continue
        if (parsed.path or "").rstrip("/") != "/jobsearch":
            continue
        params = parse_qs(parsed.query)
        page_vals = params.pop("page", None)
        if page is None:
            if page_vals:
                continue
        else:
            if not page_vals or page_vals[0] != str(page):
                continue
        if set(params.keys()) != set(base_params.keys()):
            continue
        if any(sorted(params.get(key, [])) != sorted(values) for key, values in base_params.items()):
            continue
        return True
    return False


def _contains_non_job_meta_links(urls: list[str]) -> bool:
    blocked = {
        "https://www.instagram.com/lifeatmeta",
        "https://www.linkedin.com/company/meta",
        "https://www.facebook.com/LifeAtMeta",
        "https://www.twitter.com/MetaforBusiness",
        "https://www.meta.com/media-gallery/",
    }
    return any(url in blocked for url in urls)


def _job_detail_ids(urls: list[str]) -> list[str]:
    prefix = "https://www.metacareers.com/profile/job_details/"
    ids: list[str] = []
    for url in urls:
        if not isinstance(url, str) or not url.startswith(prefix):
            continue
        ids.append(url.removeprefix(prefix))
    return ids


def _collect_fixture_links(payload: Any) -> list[str]:
    links: list[str] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            raw_links = node.get("links")
            if isinstance(raw_links, list):
                for link in raw_links:
                    if isinstance(link, str) and link.strip():
                        links.append(link.strip())
            for child in node.values():
                _walk(child)
        elif isinstance(node, list):
            for child in node:
                _walk(child)

    _walk(payload)
    return links


def _job_detail_ids_from_links(links: list[str]) -> set[str]:
    ids: set[str] = set()
    profile_marker = "/profile/job_details/"
    jobs_marker = "/jobs/"
    for link in links:
        if not isinstance(link, str):
            continue
        if profile_marker in link:
            job_id = link.split(profile_marker, 1)[-1].split("/", 1)[0]
        elif jobs_marker in link:
            job_id = link.split(jobs_marker, 1)[-1].split("/", 1)[0]
        else:
            continue
        if job_id.isdigit():
            ids.add(job_id)
    return ids


def test_spidercloud_meta_listing_page_links_include_profile_job_details(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(PAGE_LINKS_FIXTURE)
    source_url = "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering"

    urls = _run_store_scrape(raw_payload, source_url, monkeypatch)

    job_ids = _job_detail_ids(urls)
    assert job_ids, "Expected job detail URLs from listing payload"
    assert not any(_has_meta_listing_page(urls, page=p) for p in (2, 3, 4))
    assert not _contains_non_job_meta_links(urls)


def test_spidercloud_meta_listing_links_include_profile_job_details(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(LINKS_FIXTURE)
    source_url = "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering"

    urls = _run_store_scrape(raw_payload, source_url, monkeypatch)

    job_ids = _job_detail_ids(urls)
    assert job_ids, "Expected job detail URLs from listing payload"
    assert not any(_has_meta_listing_page(urls, page=p) for p in (2, 3, 4))
    assert not _contains_non_job_meta_links(urls)


def test_spidercloud_meta_listing_links_extracts_job_details(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(GRAPHQL_FIXTURE)
    source_url = (
        "https://www.metacareers.com/jobsearch/"
        "?teams[0]=Software%20Engineering&offices[0]=Seattle%2C%20WA"
    )

    urls = _run_store_scrape(raw_payload, source_url, monkeypatch)

    job_ids = _job_detail_ids(urls)
    assert job_ids, "Expected job detail URLs from listing payload"


@pytest.mark.parametrize(
    ("fixture_path", "source_url"),
    [
        (
            PAGE2_FIXTURE,
            "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering&page=2",
        ),
        (
            PAGE3_FIXTURE,
            "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering&page=3",
        ),
        (
            PAGE4_FIXTURE,
            "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering&page=4",
        ),
    ],
)
def test_spidercloud_meta_listing_pages_enqueue_next_pages(
    fixture_path: Path,
    source_url: str,
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(fixture_path)
    urls = _run_store_scrape(raw_payload, source_url, monkeypatch)

    current_page = None
    parsed = urlparse(source_url)
    params = parse_qs(parsed.query)
    if "page" in params:
        try:
            current_page = int(params["page"][0])
        except Exception:
            current_page = None
    if current_page is not None:
        assert not _has_meta_listing_page(urls, page=current_page)

    assert not any(_has_meta_listing_page(urls, page=p) for p in (2, 3, 4))

    job_ids = _job_detail_ids(urls)
    if job_ids:
        assert len(job_ids) == len(set(job_ids)), "Job detail URLs should be unique per page"


def test_spidercloud_meta_page3_links_use_spidercloud_links(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(PAGE3_FIXTURE)
    source_url = "https://www.metacareers.com/jobsearch/?teams[0]=Software%20Engineering&page=3"
    fixture_links = _collect_fixture_links(raw_payload)
    fixture_job_ids = _job_detail_ids_from_links(fixture_links)
    assert fixture_job_ids, "Expected job detail links in SpiderCloud links payload"

    urls = _run_store_scrape(raw_payload, source_url, monkeypatch)
    extracted_ids = set(_job_detail_ids(urls))
    assert extracted_ids & fixture_job_ids, (
        "Expected job detail links sourced from SpiderCloud links payload"
    )


def test_spidercloud_meta_listing_prod_page_links_enqueue_pages(
    monkeypatch: pytest.MonkeyPatch,
):
    raw_payload = _load_fixture(PROD_PAGE1_FIXTURE)
    urls = _run_store_scrape(raw_payload, PROD_SOURCE_URL, monkeypatch)

    assert not any(
        _has_meta_listing_page_for_source(urls, PROD_SOURCE_URL, page=p) for p in (2, 3, 4)
    )

    job_ids = _job_detail_ids(urls)
    assert job_ids, "Expected job detail URLs from prod listing page links"


def test_spidercloud_meta_listing_prod_pages_add_new_urls(
    monkeypatch: pytest.MonkeyPatch,
):
    fixture_sets = [
        (PROD_PAGE1_FIXTURE, PROD_SOURCE_URL),
        (PROD_PAGE2_FIXTURE, f"{PROD_SOURCE_URL}&page=2"),
        (PROD_PAGE3_FIXTURE, f"{PROD_SOURCE_URL}&page=3"),
        (PROD_PAGE4_FIXTURE, f"{PROD_SOURCE_URL}&page=4"),
    ]

    seen: set[str] = set()
    for fixture_path, source_url in fixture_sets:
        raw_payload = _load_fixture(fixture_path)
        urls = _run_store_scrape(raw_payload, source_url, monkeypatch)
        job_ids = _job_detail_ids(urls)
        if not job_ids:
            continue
        new_ids = set(job_ids) - seen
        assert new_ids, f"Expected new job detail URLs for {source_url}"
        seen.update(job_ids)
