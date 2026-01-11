from __future__ import annotations

import importlib

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


def test_convex_timeout_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONVEX_REQUEST_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("CONVEX_TOTAL_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("CONVEX_RETRY_ON_TIMEOUT", raising=False)

    module = importlib.reload(convex_client)

    assert module._REQUEST_TIMEOUT_SECONDS == 5.0
    assert module._TOTAL_BUDGET_SECONDS == 12.0
    assert module._RETRY_ON_TIMEOUT is True
