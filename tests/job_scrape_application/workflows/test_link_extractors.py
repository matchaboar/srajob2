from __future__ import annotations

from job_scrape_application.workflows.helpers.link_extractors import (
    dedupe_str_list,
    extract_job_urls_from_json_payload,
    extract_links_from_payload,
    normalize_url,
    normalize_url_list,
)


def test_extract_links_from_payload_collects_first_list():
    payload = {
        "outer": {
            "links": ["https://example.com/one", " ", None, "https://example.com/two"],
            "child": {"page_links": ["https://example.com/three"]},
        }
    }

    links = extract_links_from_payload(payload)

    assert links == ["https://example.com/one", "https://example.com/two"]


def test_extract_links_from_payload_collects_all():
    payload = {
        "outer": {
            "links": ["https://example.com/one"],
            "child": {"page_links": ["https://example.com/two"]},
        }
    }

    links = extract_links_from_payload(payload, collect_all=True)

    assert links == ["https://example.com/one", "https://example.com/two"]


def test_extract_job_urls_from_json_payload_walks_nested_jobs():
    payload = {
        "data": {
            "jobs": [
                {"jobUrl": "https://example.com/jobs/1"},
                {"applyUrl": "https://example.com/jobs/2"},
                {"url": "https://example.com/jobs/3"},
            ]
        }
    }

    urls = extract_job_urls_from_json_payload(payload)

    assert urls == [
        "https://example.com/jobs/1",
        "https://example.com/jobs/2",
        "https://example.com/jobs/3",
    ]


def test_dedupe_str_list_keeps_order_and_limits():
    values = [
        " https://example.com/one ",
        "https://example.com/one",
        "",
        "https://example.com/two",
    ]

    deduped = dedupe_str_list(values, limit=1)

    assert deduped == ["https://example.com/one"]


def test_normalize_url_handles_relative_and_scheme_relative():
    assert normalize_url("/jobs/1", base_url="https://example.com/careers") == "https://example.com/jobs/1"
    assert normalize_url("//cdn.example.com/asset", base_url="https://example.com") == "https://cdn.example.com/asset"
    assert normalize_url("mailto:test@example.com", base_url="https://example.com") is None


def test_normalize_url_list_dedupes_and_filters():
    urls = [
        "https://example.com/jobs/1",
        "/jobs/1",
        "",
        "https://example.com/jobs/2",
    ]
    normalized = normalize_url_list(urls, base_url="https://example.com")

    assert normalized == ["https://example.com/jobs/1", "https://example.com/jobs/2"]
