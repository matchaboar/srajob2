from __future__ import annotations

import pytest

from job_scrape_application.services import convex_client


@pytest.mark.parametrize(
    ("convex_url", "convex_http_url", "expected"),
    [
        (
            "https://example.convex.cloud",
            "https://legacy.convex.site",
            "https://example.convex.cloud",
        ),
        (
            None,
            "https://elegant-magpie-239.convex.site/",
            "https://elegant-magpie-239.convex.cloud",
        ),
        (
            None,
            "https://acme.convex.cloud/",
            "https://acme.convex.cloud",
        ),
    ],
)
def test_normalize_deployment_url(convex_url, convex_http_url, expected, monkeypatch):
    monkeypatch.setattr(convex_client.settings, "convex_url", convex_url)
    monkeypatch.setattr(convex_client.settings, "convex_http_url", convex_http_url)

    assert convex_client._normalize_deployment_url() == expected


def test_normalize_deployment_url_requires_env(monkeypatch):
    monkeypatch.setattr(convex_client.settings, "convex_url", None)
    monkeypatch.setattr(convex_client.settings, "convex_http_url", None)

    with pytest.raises(RuntimeError, match="CONVEX_URL"):
        convex_client._normalize_deployment_url()
