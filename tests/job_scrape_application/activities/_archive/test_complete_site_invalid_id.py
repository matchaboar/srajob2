from __future__ import annotations




from job_scrape_application.workflows.activities import complete_site, fail_site


def test_complete_site_ignores_non_convex_id(monkeypatch):
    called = {"mut": False}

    def fake_mut(name: str, args):
        called["mut"] = True
        raise RuntimeError("should not be called")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mut)

    # Should no-op and not raise
    complete_site("manual-site-1")
    assert called["mut"] is False


def test_fail_site_ignores_non_convex_id(monkeypatch):
    called = {"mut": False}

    def fake_mut(name: str, args):
        called["mut"] = True
        raise RuntimeError("should not be called")

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_mut)

    fail_site({"id": "manual-site-1", "error": "boom"})
    assert called["mut"] is False
