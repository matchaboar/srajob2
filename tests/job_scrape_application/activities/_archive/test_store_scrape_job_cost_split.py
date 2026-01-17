from __future__ import annotations

from typing import Any, Dict



from job_scrape_application.workflows import activities as acts  # noqa: E402


def test_store_scrape_splits_cost_per_job(monkeypatch):
    payload = {
        "sourceUrl": "https://example.com",
        "items": {
            "normalized": [
                {"url": "https://example.com/1"},
                {"url": "https://example.com/2"},
            ]
        },
        "costMilliCents": 2000,
    }

    calls: Dict[str, Any] = {"ingest": None}

    def fake_mutation(name: str, args: Dict[str, Any]):
        if name == "router:insertScrapeRecord":
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            calls["ingest"] = args["jobs"]
            return None
        raise AssertionError("unexpected mutation")

    monkeypatch.setattr(acts, "trim_scrape_for_convex", lambda x, **kwargs: x)
    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mutation)

    acts.store_scrape(payload)

    assert calls["ingest"] is not None
    costs = {job["scrapedCostMilliCents"] for job in calls["ingest"]}
    assert costs == {1000}
