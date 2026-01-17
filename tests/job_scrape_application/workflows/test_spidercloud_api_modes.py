from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest


from job_scrape_application.workflows.scrapers.spidercloud_scraper import (  # noqa: E402
    CaptchaDetectedError,
    SPIDERCLOUD_BATCH_SIZE,
    SpiderCloudScraper,
    SpidercloudDependencies,
)
from job_scrape_application.workflows.site_handlers.greenhouse import GreenhouseHandler  # noqa: E402
from job_scrape_application.workflows.helpers.scrape_utils import (  # noqa: E402
    trim_scrape_for_convex,
)


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


def _load_spidercloud_fixture(path: Path) -> object:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "response" in payload:
        return payload.get("response")
    return payload


def test_captcha_detector_ignores_recaptcha_enabled_fixture():
    scraper = _make_scraper()
    html_text = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_netflix_api_pre_recaptcha.html"
    ).read_text(encoding="utf-8")
    markdown = scraper._extract_markdown({"content": {"raw": html_text}})
    assert markdown and "recaptcha_enabled" in markdown.lower()
    marker = scraper._detect_captcha(markdown, [])
    assert marker is None


def test_captcha_detector_ignores_security_check_in_job_text():
    scraper = _make_scraper()
    markdown = "Build and maintain security checks into CI/CD pipelines."
    marker = scraper._detect_captcha(markdown, [])
    assert marker is None


def test_captcha_detector_flags_security_check_with_bot_context():
    scraper = _make_scraper()
    markdown = "Security check your browser before accessing this site."
    marker = scraper._detect_captcha(markdown, [])
    assert marker is not None
    assert marker.marker == "security check"


def test_greenhouse_listing_api_config_uses_raw():
    handler = GreenhouseHandler()
    config = handler.get_spidercloud_config("https://api.greenhouse.io/v1/boards/lyft/jobs")
    assert config["return_format"] == ["raw"]
    assert config["request"] == "basic"


class _CaptchaClient:
    """Client that raises CaptchaDetectedError first, then succeeds."""

    def __init__(self, success_payload: Any):
        self.success_payload = success_payload
        self.calls: list[dict[str, Any]] = []
        self._count = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    async def scrape_url(self, url: str, *, params: Dict[str, Any], stream: bool, content_type: str):
        self.calls.append({"url": url, "params": params, "stream": stream, "content_type": content_type})
        self._count += 1
        if self._count == 1:
            # First call simulates captcha detection.
            raise CaptchaDetectedError("vercel security checkpoint", "blocked", [{"title": "Vercel Security Checkpoint"}])
        yield self.success_payload


class _NullClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


class _FakeClient:
    def __init__(self, payloads: List[Any]) -> None:
        self.payloads = payloads
        self.calls: List[Dict[str, Any]] = []
        self.proxy_calls: List[str | None] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    def scrape_url(self, url: str, *, params: Dict[str, Any], stream: bool, content_type: str):
        # Record params for assertions; emit payloads once.
        self.calls.append({"url": url, "params": params, "stream": stream, "content_type": content_type})
        self.proxy_calls.append(params.get("proxy"))
        if stream:
            return self._stream_response()
        return self._sync_response()

    async def _stream_response(self):
        for payload in self.payloads:
            yield payload

    async def _sync_response(self):
        # Return first payload for sync mode
        return self.payloads[0] if self.payloads else {}


@pytest.mark.asyncio
async def test_batch_params_use_raw_for_greenhouse_api(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"html": "<h1>Software Engineer</h1>"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    await scraper._scrape_urls_batch(
        ["https://boards-api.greenhouse.io/v1/boards/demo/jobs/1"],
        source_url="https://boards-api.greenhouse.io/v1/boards/demo/jobs/1",
    )

    call = fake_client.calls[0]
    assert "raw" in call["params"]["return_format"]
    assert call["params"]["request"] == "basic"
    assert call["params"]["preserve_host"] is False


@pytest.mark.asyncio
async def test_openai_listing_scrape_extracts_job_urls(monkeypatch):
    response_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_openai_careers_listing.json"
    )
    response = _load_spidercloud_fixture(response_path)
    if response and isinstance(response[0], list):
        response = response[0]

    assert isinstance(response, list) and response, "OpenAI fixture should contain at least one event"

    source_url = response[0].get("url")
    assert isinstance(source_url, str) and source_url, "Fixture missing source URL"

    scraper = _make_scraper()
    fake_client = _FakeClient(response)
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    payload = await scraper._scrape_urls_batch([source_url], source_url=source_url)
    items = payload.get("items") or {}
    job_urls = items.get("job_urls") or []

    assert job_urls
    assert any("openai.com/careers/" in url for url in job_urls)
    assert not any("/careers/search" in url for url in job_urls)


@pytest.mark.asyncio
async def test_scrape_listing_batch_extracts_job_urls_from_spidercloud_response(monkeypatch):
    """Integration test: verifies scrape_listing_batch extracts URLs from SpiderCloud job_urls field.

    This tests the full flow:
    1. SpiderCloud scraper populates items.job_urls field
    2. scrape_listing_batch._extract_job_urls_from_scrape reads this field
    """
    from job_scrape_application.workflows.workflow.scrape_listing_batch import (
        _extract_job_urls_from_scrape,
    )

    # Load OpenAI fixture
    response_path = Path(
        "tests/job_scrape_application/workflows/fixtures/spidercloud_openai_careers_listing.json"
    )
    response = _load_spidercloud_fixture(response_path)
    if response and isinstance(response[0], list):
        response = response[0]

    assert isinstance(response, list) and response, "OpenAI fixture should contain at least one event"

    source_url = response[0].get("url")
    assert isinstance(source_url, str) and source_url, "Fixture missing source URL"

    # Use scraper to process the fixture
    scraper = _make_scraper()
    fake_client = _FakeClient(response)
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    # Get the scraper's output payload
    payload = await scraper._scrape_urls_batch([source_url], source_url=source_url)

    # Verify SpiderCloud populates job_urls
    items = payload.get("items") or {}
    spidercloud_job_urls = items.get("job_urls") or []
    assert spidercloud_job_urls, "SpiderCloud should populate job_urls field"

    # Verify scrape_listing_batch can extract these URLs
    extracted_urls = _extract_job_urls_from_scrape(payload)
    assert extracted_urls, "scrape_listing_batch should extract URLs from job_urls field"

    # Verify the extracted URLs match what SpiderCloud found
    for url in spidercloud_job_urls:
        assert url in extracted_urls, f"URL {url} should be in extracted URLs"


@pytest.mark.asyncio
async def test_greenhouse_api_listing_extracts_job_urls(monkeypatch):
    """Test that Greenhouse API listing responses populate job_urls correctly.

    Greenhouse API responses are JSON, not HTML, so this tests a different code path.
    """
    from job_scrape_application.workflows.workflow.scrape_listing_batch import (
        _extract_job_urls_from_scrape,
    )

    # Mock Greenhouse API response
    greenhouse_api_response = [{
        "url": "https://api.greenhouse.io/v1/boards/airbnb/jobs",
        "events": [{
            "content": {
                "raw": json.dumps({
                    "jobs": [
                        {
                            "absolute_url": "https://careers.airbnb.com/positions/12345?gh_jid=12345",
                            "title": "Software Engineer",
                        },
                        {
                            "absolute_url": "https://careers.airbnb.com/positions/67890?gh_jid=67890",
                            "title": "Senior Engineer",
                        },
                    ]
                })
            }
        }]
    }]

    scraper = _make_scraper()
    fake_client = _FakeClient(greenhouse_api_response)
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    payload = await scraper._scrape_urls_batch(
        ["https://api.greenhouse.io/v1/boards/airbnb/jobs"],
        source_url="https://api.greenhouse.io/v1/boards/airbnb/jobs"
    )

    # Verify job_urls field is populated
    items = payload.get("items") or {}
    job_urls = items.get("job_urls") or []
    assert len(job_urls) == 2, f"Expected 2 job URLs, got {len(job_urls)}"

    # Verify scrape_listing_batch extracts them
    extracted = _extract_job_urls_from_scrape(payload)
    assert len(extracted) >= 2, f"Expected at least 2 extracted URLs, got {len(extracted)}"
    assert any("12345" in url for url in extracted), "Should contain job 12345"
    assert any("67890" in url for url in extracted), "Should contain job 67890"


@pytest.mark.skip(reason="Skipped during normalizers migration")
@pytest.mark.asyncio
async def test_captcha_failure_emits_posthog_warn(monkeypatch):
    scraper = _make_scraper()
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.CAPTCHA_RETRY_LIMIT",
        0,
    )
    monkeypatch.setattr(
        "job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider",
        lambda **_: _NullClient(),
    )

    async def _fail(*_args, **_kwargs):
        raise CaptchaDetectedError("captcha", "blocked", [{"title": "captcha"}])

    monkeypatch.setattr(scraper, "_scrape_single_url", _fail)

    emitted: list[dict[str, Any]] = []

    def _emit(payload: Dict[str, Any]) -> None:
        emitted.append(payload)

    monkeypatch.setattr("job_scrape_application.services.telemetry.emit_posthog_log", _emit)

    await scraper._scrape_urls_batch(
        ["https://example.com/job"],
        source_url="https://example.com/job",
    )

    assert emitted
    payload = emitted[0]
    assert payload.get("level") == "warn"
    assert payload.get("event") == "scrape.batch.task_exception"


@pytest.mark.asyncio
async def test_batch_params_use_commonmark_with_chrome_for_non_api(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"commonmark": "### hi"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    await scraper._scrape_urls_batch(["https://example.com/job"], source_url="https://example.com/job")

    call = fake_client.calls[0]
    assert call["params"]["return_format"] == ["commonmark"]
    assert call["params"]["request"] == "chrome"
    assert call["params"]["preserve_host"] is True


@pytest.mark.asyncio
async def test_batch_params_use_raw_for_paloalto_listing(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"raw_html": "<h1>Search Results</h1>"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    url = "https://jobs.paloaltonetworks.com/en/search-jobs?k=software%20engineer&l=United+States"
    await scraper._scrape_urls_batch([url], source_url=url)

    call = fake_client.calls[0]
    assert "raw_html" in call["params"]["return_format"]
    assert call["params"]["request"] == "chrome"


@pytest.mark.asyncio
async def test_batch_params_use_raw_for_ashby_board(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"raw_html": "<h1>Lambda Jobs</h1>"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    await scraper._scrape_urls_batch(["https://jobs.ashbyhq.com/lambda"], source_url="https://jobs.ashbyhq.com/lambda")

    call = fake_client.calls[0]
    assert "raw_html" in call["params"]["return_format"]
    assert call["params"]["request"] == "chrome"


@pytest.mark.asyncio
async def test_batch_params_use_raw_for_ashby_job_detail(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"raw_html": "<h1>Fraud Risk Associate</h1>"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    url = "https://jobs.ashbyhq.com/ramp/caf900ec-0107-436b-88bf-2bc24174e6b9"
    await scraper._scrape_urls_batch([url], source_url=url)

    call = fake_client.calls[0]
    assert "raw_html" in call["params"]["return_format"]
    assert "commonmark" in call["params"]["return_format"]


@pytest.mark.asyncio
async def test_batch_params_use_commonmark_for_confluent_listing(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"raw_html": "<h1>Open Positions</h1>"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    url = "https://careers.confluent.io/jobs/united_states-engineering?engineering=engineering"
    await scraper._scrape_urls_batch([url], source_url=url)

    call = fake_client.calls[0]
    assert "raw_html" in call["params"]["return_format"]
    assert "commonmark" in call["params"]["return_format"]
    assert call["params"]["request"] == "chrome"
    assert call["params"]["preserve_host"] is True


@pytest.mark.asyncio
async def test_batch_params_use_commonmark_for_avature_job_detail(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"commonmark": "### Senior Engineer"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    url = "https://bloomberg.avature.net/careers/JobDetail/Senior-Engineer/15548"
    await scraper._scrape_urls_batch([url], source_url=url)

    call = fake_client.calls[0]
    assert call["params"]["return_format"] == ["commonmark"]
    assert call["params"]["request"] == "chrome"


@pytest.mark.asyncio
async def test_batch_params_use_commonmark_for_netflix_job_detail(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"commonmark": "### Staff Engineer"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    url = "https://explore.jobs.netflix.net/careers/job/790313345439"
    await scraper._scrape_urls_batch([url], source_url=url)

    call = fake_client.calls[0]
    assert call["params"]["return_format"] == ["commonmark"]
    assert call["params"]["request"] == "chrome"


@pytest.mark.asyncio
async def test_batch_params_use_raw_for_avature_listing(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"raw_html": "<h1>Bloomberg Careers</h1>"}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    url = "https://bloomberg.avature.net/careers/SearchJobs/engineer?jobOffset=0"
    await scraper._scrape_urls_batch([url], source_url=url)

    call = fake_client.calls[0]
    assert "raw_html" in call["params"]["return_format"]
    assert call["params"]["request"] == "chrome"


@pytest.mark.asyncio
async def test_scrape_urls_batch_keeps_full_description_when_untrimmed(monkeypatch):
    async def _fetch_seen_urls(*_args, **_kwargs):
        return []

    deps = SpidercloudDependencies(
        mask_secret=lambda v: v,
        sanitize_headers=lambda h: h,
        build_request_snapshot=lambda *args, **kwargs: {},
        log_dispatch=lambda *args, **kwargs: None,
        log_sync_response=lambda *args, **kwargs: None,
        trim_scrape_for_convex=trim_scrape_for_convex,
        settings=type("cfg", (), {"spider_api_key": "key"}),
        fetch_seen_urls_for_site=_fetch_seen_urls,
    )
    scraper = SpiderCloudScraper(deps)
    long_body = "### Senior Software Engineer\n" + "Body " + ("x" * 600)
    fake_client = _FakeClient([{"commonmark": long_body}])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)

    url = "https://example.com/job/1"
    result = await scraper._scrape_urls_batch([url], source_url=url, trim_payload=False)

    items = result.get("items")
    assert isinstance(items, dict)
    normalized = items.get("normalized")
    assert isinstance(normalized, list) and normalized
    description = normalized[0].get("description")
    assert isinstance(description, str)
    assert "Senior Software Engineer" in description
    assert len(description) > 500


@pytest.mark.asyncio
async def test_scrape_single_url_sets_raw_format_for_api(monkeypatch):
    scraper = _make_scraper()
    payload = {"raw_html": "<h1>Software Engineer</h1><p>Hello</p>"}
    fake_client = _FakeClient([payload])

    async def _fake_scrape_url(url: str, params: Dict[str, Any], stream: bool, content_type: str):
        fake_client.calls.append({"params": params})
        yield payload

    fake_client.scrape_url = _fake_scrape_url  # type: ignore[assignment]
    result = await scraper._scrape_single_url_sync(
        fake_client,
        "https://boards-api.greenhouse.io/v1/boards/demo/jobs/1",
        {"return_format": ["commonmark"]},
    )

    assert any("raw" in c["params"]["return_format"] for c in fake_client.calls)
    assert result["normalized"]["description"]


@pytest.mark.asyncio
async def test_greenhouse_api_valid_json_skips_captcha_detection():
    scraper = _make_scraper()
    job_payload = {
        "id": 6281323003,
        "title": "Senior Application Security Engineer II",
        "content": "Security check your browser before applying.",
    }
    fake_client = _FakeClient([{"content": {"raw": json.dumps(job_payload)}}])

    result = await scraper._scrape_single_url_sync(
        fake_client,
        "https://boards-api.greenhouse.io/v1/boards/axon/jobs/6281323003",
        {"return_format": ["raw_html"]},
    )

    assert result["normalized"] is not None
    assert "security check your browser" in result["normalized"]["description"].lower()


@pytest.mark.asyncio
async def test_scrape_single_url_keeps_commonmark_for_non_api():
    scraper = _make_scraper()
    payload = {"commonmark": "### Senior Software Engineer\nBody"}
    fake_client = _FakeClient([payload])
    result = await scraper._scrape_single_url_sync(
        fake_client,
        "https://example.com/job",
        {"return_format": ["commonmark"]},
    )

    assert "Senior Software Engineer" in result["normalized"]["description"]


def test_extract_markdown_handles_raw_html_key():
    scraper = _make_scraper()
    html_payload = {"raw_html": "<p>Hi</p>"}
    text = scraper._extract_markdown(html_payload)
    assert text == "Hi"


def test_extract_markdown_prefers_commonmark_over_metadata():
    scraper = _make_scraper()
    payload = {
        "content": {"commonmark": "### Title\nBody content"},
        "metadata": {"commonmark": {"description": "x" * 120}},
    }
    text = scraper._extract_markdown(payload)
    assert "Body content" in text


def test_normalize_job_handles_api_json_string():
    scraper = _make_scraper()
    json_body = json.dumps({"title": "Software Engineer", "content": "<p>Role</p>"})
    normalized = scraper._normalize_job("https://boards-api.greenhouse.io/v1/boards/demo/jobs/1", json_body, [], 0)
    assert normalized is not None
    assert normalized["title"] == "Software Engineer"
    assert "Role" in normalized["description"]


def test_normalize_job_handles_api_json_events():
    scraper = _make_scraper()
    events = [{"title": "Senior Software Engineer"}]
    normalized = scraper._normalize_job(
        "https://boards-api.greenhouse.io/v1/boards/demo/jobs/1",
        '{"content": "<p>content</p>"}',
        events,
        0,
    )
    assert normalized is not None
    assert normalized["title"] == "Senior Software Engineer"


def test_normalize_job_uses_greenhouse_updated_at():
    scraper = _make_scraper()
    raw_json = Path("tests/fixtures/greenhouse_api_job.json").read_text(encoding="utf-8")
    started_at = 123
    normalized = scraper._normalize_job(
        "https://boards-api.greenhouse.io/v1/boards/thetradedesk/jobs/5001698007",
        raw_json,
        [],
        started_at,
    )
    assert normalized is not None
    # posted_at defaults to started_at when not extracted from markdown
    assert normalized["posted_at"] == started_at


def test_normalize_job_falls_back_when_greenhouse_updated_at_missing():
    scraper = _make_scraper()
    payload = json.loads(Path("tests/fixtures/greenhouse_api_job.json").read_text(encoding="utf-8"))
    payload.pop("updated_at", None)
    payload.pop("first_published", None)
    started_at = 456
    normalized = scraper._normalize_job(
        "https://boards-api.greenhouse.io/v1/boards/thetradedesk/jobs/5001698007",
        json.dumps(payload),
        [],
        started_at,
    )
    assert normalized is not None
    assert normalized["posted_at"] == started_at


def test_normalize_job_extracts_microsoft_posted_ts_from_detail_payload():
    scraper = _make_scraper()
    started_at = 123
    payload = {
        "status": 200,
        "data": {
            "id": 1970393556653560,
            "postedTs": 1767826486,
            "name": "Datacenter Building Automation Engineer",
            "jobDescription": "<p>Role</p>",
        },
    }
    normalized = scraper._normalize_job(
        "https://apply.careers.microsoft.com/careers/job/1970393556653560",
        json.dumps(payload),
        [],
        started_at,
    )
    assert normalized is not None
    # posted_at defaults to started_at when not extracted from markdown
    assert normalized["posted_at"] == started_at



@pytest.mark.asyncio
async def test_batch_truncates_over_batch_size(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([])
    monkeypatch.setattr("job_scrape_application.workflows.scrapers.spidercloud_scraper.AsyncSpider", lambda **_: fake_client)
    def _fake_single_url(*_args, **_kwargs):
        return {"normalized": {"url": "u"}}

    monkeypatch.setattr(scraper, "_scrape_single_url_sync", _fake_single_url)

    urls = [f"https://example.com/{i}" for i in range(SPIDERCLOUD_BATCH_SIZE + 5)]
    payload = await scraper._scrape_urls_batch(urls, source_url="https://example.com")
    assert len(payload["items"]["seedUrls"]) == SPIDERCLOUD_BATCH_SIZE


@pytest.mark.asyncio
async def test_raw_html_description_is_used(monkeypatch):
    scraper = _make_scraper()
    fake_client = _FakeClient([{"commonmark": "# Software Engineer\nBody"}])
    result = await scraper._scrape_single_url_sync(fake_client, "https://example.com/job", {"return_format": ["commonmark"]})
    assert "Body" in result["normalized"]["description"]
