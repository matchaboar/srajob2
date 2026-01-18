"""
Test that URLs already scraped in Convex prod are properly deduplicated
and not re-scraped by the DBOS workflow.

Target URL: https://boards.greenhouse.io/airbnb/jobs/7527407

BUG IDENTIFIED:
==============
The job exists in Convex prod (ID: k179yqdb18vztc0hs6kt55bmv57yyfkz) but is NOT
recorded in seen_job_urls table. This causes the first dedup layer to miss it.

Root Cause:
-----------
In process_spidercloud_job_batch (activities/__init__.py:1690):
1. Job URLs are queued with siteId in their entries (line 2038-2040)
2. When scrape payloads are built (line 1961), siteId is NOT copied from batch entry
3. store_scrape gets a payload WITHOUT siteId
4. ingestJobsFromScrape has no siteId, so recordSeenJobUrl is NEVER called

Fix Location:
-------------
activities/__init__.py around line 1961-1970 in _scrape_group():
- Need to add siteId from the batch entry to per_url_payload before storing

Impact:
-------
- seen_job_urls table is empty for jobs scraped via process_spidercloud_job_batch
- First dedup layer (fetch_seen_urls_for_site) misses existing jobs
- Second layer (filter_new_job_urls via job_url_keys) still catches duplicates
- But this wastes database queries and is fragile if second layer fails
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest


from job_scrape_application.workflows import activities as acts  # noqa: E402


TARGET_URL = "https://boards.greenhouse.io/airbnb/jobs/7527407"
AIRBNB_SOURCE_URL = "https://api.greenhouse.io/v1/boards/airbnb/jobs"

# The job exists in Convex prod with this ID (verified by user)
CONVEX_JOB_ID = "k179yqdb18vztc0hs6kt55bmv57yyfkz"


class FakeSpiderCloudScraper:
    """Fake scraper that returns the target URL in the listing."""

    provider = "spidercloud"

    async def fetch_greenhouse_listing(self, site: Dict[str, Any]) -> Dict[str, Any]:
        """Return a listing that includes our target URL."""
        return {
            "job_urls": [
                TARGET_URL,
                "https://boards.greenhouse.io/airbnb/jobs/9999999",  # new URL
            ],
            "posted_at_by_url": {},
        }


@pytest.mark.asyncio
async def test_existing_url_is_skipped_via_seen_urls_dedup(monkeypatch: pytest.MonkeyPatch):
    """
    Test that when a URL already exists in Convex (seen_job_urls table),
    it is NOT queued for scraping again.

    This simulates production state where the job at
    https://boards.greenhouse.io/airbnb/jobs/7527407 has already been scraped.
    """

    # Track what URLs get enqueued for scraping
    enqueued_urls: List[str] = []

    def fake_fetch_seen_urls_for_site(
        source_url: str, pattern: str | None, candidate_urls: list[str] | None = None
    ) -> List[str]:
        """
        Simulate Convex returning that TARGET_URL has already been seen.
        This mocks the listSeenJobUrlsForSite query.
        """
        assert source_url == AIRBNB_SOURCE_URL, f"Unexpected source URL: {source_url}"
        # Return TARGET_URL as "already seen" for this site
        return [TARGET_URL]

    def fake_filter_new_job_urls(urls: List[str]) -> List[str]:
        """
        Simulate Convex filterNewJobUrls - returns only URLs NOT in the jobs table.
        Since TARGET_URL exists, it should NOT be in the "new" list.
        """
        # TARGET_URL already exists, so return only URLs that don't exist
        return [u for u in urls if u != TARGET_URL]

    def fake_enqueue_scrape_urls(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        """Capture what URLs get enqueued for scraping."""
        urls = payload.get("urls", [])
        enqueued_urls.extend(urls)
        return {"queued": len(urls)}

    def fake_list_scrape_urls(**kwargs) -> List[Dict[str, Any]]:
        """Return empty queue - no pending URLs."""
        return []

    # Apply mocks
    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen_urls_for_site)
    monkeypatch.setattr(acts, "filter_new_job_urls", fake_filter_new_job_urls)
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.list_scrape_urls",
        fake_list_scrape_urls,
    )

    # Create the site config (simulating Airbnb from prod)
    site = {
        "_id": "a" * 26,  # Fake Convex ID
        "url": AIRBNB_SOURCE_URL,
        "type": "greenhouse",
        "name": "airbnb",
    }

    # Run the greenhouse scraper
    scraper = FakeSpiderCloudScraper()
    result = await acts._scrape_spidercloud_greenhouse(scraper, site, skip_urls=[])

    # Assertions: TARGET_URL should NOT be enqueued for scraping
    assert TARGET_URL not in enqueued_urls, (
        f"DEDUP FAILURE: {TARGET_URL} was enqueued for scraping even though it exists in Convex. "
        f"Enqueued URLs: {enqueued_urls}"
    )

    # The new URL should be enqueued
    assert "https://boards.greenhouse.io/airbnb/jobs/9999999" in enqueued_urls, (
        "New URL should be enqueued for scraping"
    )

    # Verify result structure
    assert "items" in result
    # URLs filtered by seen_urls_for_site don't appear in "existing" - they're filtered at
    # pending_urls stage. The "existing" field only contains URLs that passed seen_urls
    # but were then blocked by filter_new_job_urls (global job dedup).
    # The key assertion is above: TARGET_URL was not enqueued.


@pytest.mark.asyncio
async def test_existing_url_is_skipped_via_filter_new_job_urls(monkeypatch: pytest.MonkeyPatch):
    """
    Test the second dedup layer: filter_new_job_urls (checks job_url_keys table).

    Even if fetch_seen_urls_for_site returns empty (e.g., URL scraped from
    different source), filter_new_job_urls should still block duplicates.
    """

    enqueued_urls: List[str] = []

    def fake_fetch_seen_urls_for_site(
        source_url: str, pattern: str | None, candidate_urls: list[str] | None = None
    ) -> List[str]:
        """
        Simulate that the URL was NOT seen from this specific source.
        (e.g., it was scraped via a different site/source URL)
        """
        return []  # Not seen from this source

    def fake_filter_new_job_urls(urls: List[str]) -> List[str]:
        """
        Simulate Convex filterNewJobUrls - TARGET_URL exists in jobs table
        (from previous scrape via different source).
        """
        # TARGET_URL already exists globally, so filter it out
        return [u for u in urls if u != TARGET_URL]

    def fake_enqueue_scrape_urls(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        urls = payload.get("urls", [])
        enqueued_urls.extend(urls)
        return {"queued": len(urls)}

    def fake_list_scrape_urls(**kwargs) -> List[Dict[str, Any]]:
        return []

    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen_urls_for_site)
    monkeypatch.setattr(acts, "filter_new_job_urls", fake_filter_new_job_urls)
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.list_scrape_urls",
        fake_list_scrape_urls,
    )

    site = {
        "_id": "a" * 26,
        "url": AIRBNB_SOURCE_URL,
        "type": "greenhouse",
        "name": "airbnb",
    }

    scraper = FakeSpiderCloudScraper()
    await acts._scrape_spidercloud_greenhouse(scraper, site, skip_urls=[])

    # TARGET_URL should NOT be enqueued (filtered by filter_new_job_urls)
    assert TARGET_URL not in enqueued_urls, (
        f"DEDUP FAILURE: {TARGET_URL} was enqueued despite existing in job_url_keys. "
        f"Enqueued URLs: {enqueued_urls}"
    )


@pytest.mark.asyncio
async def test_both_dedup_layers_bypass_allows_duplicate_scrape(monkeypatch: pytest.MonkeyPatch):
    """
    This test demonstrates what SHOULD NOT happen:
    If both dedup layers return empty/fail, the URL gets scraped again.

    This test should PASS (showing the bad behavior) if there's a bug.
    """

    enqueued_urls: List[str] = []

    def fake_fetch_seen_urls_for_site(
        source_url: str, pattern: str | None, candidate_urls: list[str] | None = None
    ) -> List[str]:
        """Dedup layer 1 fails to detect the URL."""
        return []

    def fake_filter_new_job_urls(urls: List[str]) -> List[str]:
        """Dedup layer 2 also fails - returns all URLs as "new"."""
        return urls  # BUG: should have filtered out TARGET_URL

    def fake_enqueue_scrape_urls(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        urls = payload.get("urls", [])
        enqueued_urls.extend(urls)
        return {"queued": len(urls)}

    def fake_list_scrape_urls(**kwargs) -> List[Dict[str, Any]]:
        return []

    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen_urls_for_site)
    monkeypatch.setattr(acts, "filter_new_job_urls", fake_filter_new_job_urls)
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.list_scrape_urls",
        fake_list_scrape_urls,
    )

    site = {
        "_id": "a" * 26,
        "url": AIRBNB_SOURCE_URL,
        "type": "greenhouse",
        "name": "airbnb",
    }

    scraper = FakeSpiderCloudScraper()
    await acts._scrape_spidercloud_greenhouse(scraper, site, skip_urls=[])

    # This demonstrates the PROBLEM: if dedup fails, URL gets re-scraped
    assert TARGET_URL in enqueued_urls, (
        "When both dedup layers fail, the URL is incorrectly re-scraped"
    )


@pytest.mark.asyncio
async def test_url_normalization_affects_dedup(monkeypatch: pytest.MonkeyPatch):
    """
    Test that URL normalization doesn't cause dedup misses.

    URLs might be stored with slight variations (trailing slash, etc.)
    and the dedup check must handle this correctly.
    """

    enqueued_urls: List[str] = []

    # URL variations that should all dedupe to the same job
    url_variations = [
        "https://boards.greenhouse.io/airbnb/jobs/7527407",
        "https://boards.greenhouse.io/airbnb/jobs/7527407/",
        "https://boards.greenhouse.io/airbnb/jobs/7527407?gh_jid=7527407",
    ]

    def fake_fetch_seen_urls_for_site(
        source_url: str, pattern: str | None, candidate_urls: list[str] | None = None
    ) -> List[str]:
        """Return the canonical URL form as "seen"."""
        # Convex stores the normalized form
        return ["https://boards.greenhouse.io/airbnb/jobs/7527407"]

    def fake_filter_new_job_urls(urls: List[str]) -> List[str]:
        """Filter based on normalized URL matching."""
        canonical = "https://boards.greenhouse.io/airbnb/jobs/7527407"
        # Should filter all variations of the same URL
        return [u for u in urls if u.rstrip("/").split("?")[0] != canonical]

    def fake_enqueue_scrape_urls(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        urls = payload.get("urls", [])
        enqueued_urls.extend(urls)
        return {"queued": len(urls)}

    def fake_list_scrape_urls(**kwargs) -> List[Dict[str, Any]]:
        return []

    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen_urls_for_site)
    monkeypatch.setattr(acts, "filter_new_job_urls", fake_filter_new_job_urls)
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.list_scrape_urls",
        fake_list_scrape_urls,
    )

    # Test with different URL forms in the listing
    class VariantScraper:
        provider = "spidercloud"

        async def fetch_greenhouse_listing(self, site: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "job_urls": url_variations + ["https://boards.greenhouse.io/airbnb/jobs/NEW123"],
                "posted_at_by_url": {},
            }

    site = {
        "_id": "a" * 26,
        "url": AIRBNB_SOURCE_URL,
        "type": "greenhouse",
        "name": "airbnb",
    }

    scraper = VariantScraper()
    await acts._scrape_spidercloud_greenhouse(scraper, site, skip_urls=[])

    # None of the URL variations should be enqueued
    for variant in url_variations:
        # Check the normalized form
        normalized = variant.rstrip("/").split("?")[0]
        matching = [u for u in enqueued_urls if u.rstrip("/").split("?")[0] == normalized]
        assert not matching, (
            f"URL variation {variant} was enqueued despite existing. "
            f"Enqueued: {enqueued_urls}"
        )

    # Only the NEW URL should be enqueued
    assert any("NEW123" in u for u in enqueued_urls), "New URL should be enqueued"


# Integration tests removed - they require production Convex access
# and test against specific production data that may not exist in all environments


@pytest.mark.asyncio
async def test_source_url_mismatch_causes_dedup_failure(monkeypatch: pytest.MonkeyPatch):
    """
    Test that demonstrates a potential dedup bug: if the sourceUrl stored
    in seen_job_urls doesn't match what the scraper queries with, dedup fails.

    This is a common cause of dedup failures - URL stored with one source,
    queried with a different source.
    """

    enqueued_urls: List[str] = []

    # The URL was stored with a DIFFERENT sourceUrl than what we're querying
    stored_source_url = "https://www.airbnb.com/careers"  # Different from AIRBNB_SOURCE_URL

    def fake_fetch_seen_urls_for_site(
        source_url: str, pattern: str | None, candidate_urls: list[str] | None = None
    ) -> List[str]:
        """
        Simulate the bug: only return seen URLs if sourceUrl matches exactly.
        Since we're querying with AIRBNB_SOURCE_URL but the URL was stored
        with stored_source_url, the query returns empty.
        """
        if source_url == stored_source_url:
            return [TARGET_URL]
        # Wrong source URL - returns empty (the bug!)
        return []

    def fake_filter_new_job_urls(urls: List[str]) -> List[str]:
        """
        Global dedup still works - catches the duplicate.
        """
        return [u for u in urls if u != TARGET_URL]

    def fake_enqueue_scrape_urls(payload: Dict[str, Any], *, force_refresh: bool = False) -> Dict[str, Any]:
        urls = payload.get("urls", [])
        enqueued_urls.extend(urls)
        return {"queued": len(urls)}

    def fake_list_scrape_urls(**kwargs) -> List[Dict[str, Any]]:
        return []

    monkeypatch.setattr(acts, "fetch_seen_urls_for_site", fake_fetch_seen_urls_for_site)
    monkeypatch.setattr(acts, "filter_new_job_urls", fake_filter_new_job_urls)
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.enqueue_scrape_urls",
        fake_enqueue_scrape_urls,
    )
    monkeypatch.setattr(
        "job_scrape_application.dbos_runtime.queue.list_scrape_urls",
        fake_list_scrape_urls,
    )

    site = {
        "_id": "a" * 26,
        "url": AIRBNB_SOURCE_URL,  # Queries with this URL
        "type": "greenhouse",
        "name": "airbnb",
    }

    scraper = FakeSpiderCloudScraper()
    await acts._scrape_spidercloud_greenhouse(scraper, site, skip_urls=[])

    # Even though seen_urls_for_site returned empty (due to sourceUrl mismatch),
    # filter_new_job_urls should catch the duplicate
    assert TARGET_URL not in enqueued_urls, (
        f"Second layer dedup (filter_new_job_urls) should have caught the duplicate. "
        f"Enqueued: {enqueued_urls}"
    )
