from __future__ import annotations

from job_scrape_application.workflows.activities.step.store_scrape import _normalize_job_payload


def test_normalize_job_payload_prefers_apply_url_and_traces_source() -> None:
    payload = {
        "title": "Software Engineer",
        "company": "Acme",
        "description": "Build systems.",
        "location": "Remote",
        "remote": True,
        "level": "mid",
        "totalCompensation": 0,
        "postedAt": 1_700_000_000_000,
        "url": "https://boards-api.greenhouse.io/v1/boards/acme/jobs/123",
        "apply_url": "https://boards.greenhouse.io/acme/jobs/123",
        "sourceUrl": "https://boards.greenhouse.io/acme",
    }

    normalized = _normalize_job_payload(payload, now_ms=1_700_000_000_000)

    assert normalized is not None
    assert normalized["url"] == "https://boards.greenhouse.io/acme/jobs/123"
    assert normalized["scrapeUrl"] == "https://boards-api.greenhouse.io/v1/boards/acme/jobs/123"
    assert normalized["sourceUrl"] == "https://boards.greenhouse.io/acme"


def test_normalize_job_payload_preserves_url_when_no_apply_url() -> None:
    payload = {
        "title": "Data Engineer",
        "company": "Example",
        "description": "Own pipelines.",
        "location": "Seattle, WA",
        "remote": False,
        "level": "mid",
        "totalCompensation": 0,
        "postedAt": 1_700_000_000_000,
        "url": "https://careers.example.com/jobs/456",
    }

    normalized = _normalize_job_payload(payload, now_ms=1_700_000_000_000)

    assert normalized is not None
    assert normalized["url"] == "https://careers.example.com/jobs/456"
    assert normalized["scrapeUrl"] == "https://careers.example.com/jobs/456"
