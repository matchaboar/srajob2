"""
Test to demonstrate that process_spidercloud_job_batch does not check
for existing job URLs before sending them to SpiderCloud, which can waste API credits.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest


from job_scrape_application.workflows import activities


class _SpiderCloudMock:
    """Mock SpiderCloud scraper to track what URLs are sent to the API."""

    def __init__(self):
        self.provider = "spidercloud"
        self.scrape_calls: List[Dict[str, Any]] = []

    async def scrape_greenhouse_jobs(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Track the scrape call and return a mock response."""
        self.scrape_calls.append(payload)

        # Return a mock response with normalized job data
        urls = payload.get("urls", [])
        normalized = []
        for url in urls:
            normalized.append({
                "url": url,
                "title": "Sample Job",
                "company": "Sample Company",
                "description": "Sample description",
                "location": "Sample location",
            })

        return {
            "scrape": {
                "provider": "spidercloud",
                "items": {
                    "normalized": normalized,
                    "costMilliCents": len(urls) * 100,  # Mock cost
                },
                "costMilliCents": len(urls) * 100,
            }
        }


@pytest.mark.asyncio
async def test_process_job_batch_now_filters_existing_urls_before_spidercloud(monkeypatch):
    """
    Test demonstrating that process_spidercloud_job_batch now correctly filters
    out existing URLs before sending to SpiderCloud, saving API credits.
    """
    # Setup: Create a batch with 3 job URLs
    batch = {
        "urls": [
            {
                "url": "https://boards-api.greenhouse.io/v1/boards/company/jobs/111",
                "sourceUrl": "https://boards.greenhouse.io/company",
                "provider": "spidercloud",
            },
            {
                "url": "https://boards-api.greenhouse.io/v1/boards/company/jobs/222",
                "sourceUrl": "https://boards.greenhouse.io/company",
                "provider": "spidercloud",
            },
            {
                "url": "https://boards-api.greenhouse.io/v1/boards/company/jobs/333",
                "sourceUrl": "https://boards.greenhouse.io/company",
                "provider": "spidercloud",
            },
        ]
    }

    # Mock: Simulate that 2 of these URLs already exist in Convex
    existing_urls = {
        "https://boards-api.greenhouse.io/v1/boards/company/jobs/111",
        "https://boards-api.greenhouse.io/v1/boards/company/jobs/222",
    }

    async def fake_filter_new_job_urls(urls: List[str]) -> List[str]:
        """Mock Convex query that returns only URLs that don't exist."""
        return [u for u in urls if u not in existing_urls]

    # Mock the scraper
    mock_scraper = _SpiderCloudMock()

    def fake_make_spidercloud_scraper():
        return mock_scraper

    async def fake_record_scrape_url_attempts(entries: list[dict[str, Any]]) -> None:
        """Mock attempt recording."""
        return None

    def fake_complete_scrape_urls(payload: dict) -> dict:
        """Mock queue completion."""
        return {"updated": len(payload.get("items", []))}

    # Apply mocks
    monkeypatch.setattr(activities, "filter_new_job_urls", fake_filter_new_job_urls)
    monkeypatch.setattr(activities, "_make_spidercloud_scraper", fake_make_spidercloud_scraper)
    monkeypatch.setattr(activities, "_record_scrape_url_attempts", fake_record_scrape_url_attempts)
    monkeypatch.setattr(activities.dbos_queue, "complete_scrape_urls", fake_complete_scrape_urls)

    # Execute: Call process_spidercloud_job_batch
    await activities.process_spidercloud_job_batch(batch, persist_scrapes=True)

    # Verify: Check what was sent to SpiderCloud
    assert len(mock_scraper.scrape_calls) == 1, "Should make one scrape call"

    scraped_urls = mock_scraper.scrape_calls[0].get("urls", [])

    # FIXED: Only 1 URL should be sent to SpiderCloud (the new one)
    assert len(scraped_urls) == 1, f"Expected 1 URL, got {len(scraped_urls)}"
    assert "https://boards-api.greenhouse.io/v1/boards/company/jobs/333" in scraped_urls
    assert "https://boards-api.greenhouse.io/v1/boards/company/jobs/111" not in scraped_urls
    assert "https://boards-api.greenhouse.io/v1/boards/company/jobs/222" not in scraped_urls

    print("\n✅ DEDUPLICATION WORKING:")
    print("   - URLs in batch: 3")
    print("   - URLs already in database: 2")
    print(f"   - URLs sent to SpiderCloud: {len(scraped_urls)}")
    print("   - API credits saved: 2")


@pytest.mark.asyncio
async def test_all_urls_skipped_when_all_exist(monkeypatch):
    """
    Test that when ALL URLs already exist in the database, no SpiderCloud calls are made.
    """
    batch = {
        "urls": [
            {
                "url": "https://boards-api.greenhouse.io/v1/boards/company/jobs/111",
                "sourceUrl": "https://boards.greenhouse.io/company",
                "provider": "spidercloud",
            },
            {
                "url": "https://boards-api.greenhouse.io/v1/boards/company/jobs/222",
                "sourceUrl": "https://boards.greenhouse.io/company",
                "provider": "spidercloud",
            },
        ]
    }

    # All URLs already exist
    existing_urls = {
        "https://boards-api.greenhouse.io/v1/boards/company/jobs/111",
        "https://boards-api.greenhouse.io/v1/boards/company/jobs/222",
    }

    async def fake_filter_new_job_urls(urls: List[str]) -> List[str]:
        return [u for u in urls if u not in existing_urls]  # Returns empty list

    mock_scraper = _SpiderCloudMock()

    def fake_make_spidercloud_scraper():
        return mock_scraper

    async def fake_record_scrape_url_attempts(entries: list[dict[str, Any]]) -> None:
        return None

    def fake_complete_scrape_urls(payload: dict) -> dict:
        return {"updated": len(payload.get("items", []))}

    monkeypatch.setattr(activities, "filter_new_job_urls", fake_filter_new_job_urls)
    monkeypatch.setattr(activities, "_make_spidercloud_scraper", fake_make_spidercloud_scraper)
    monkeypatch.setattr(activities, "_record_scrape_url_attempts", fake_record_scrape_url_attempts)
    monkeypatch.setattr(activities.dbos_queue, "complete_scrape_urls", fake_complete_scrape_urls)

    result = await activities.process_spidercloud_job_batch(batch, persist_scrapes=True)

    # No SpiderCloud calls should be made
    assert len(mock_scraper.scrape_calls) == 0, "No scrape calls when all URLs exist"

    # Result should indicate skipped URLs
    assert result.get("skippedExisting") == 2, "Should report 2 skipped existing URLs"

    print("\n✅ ALL URLS SKIPPED:")
    print("   - URLs in batch: 2")
    print("   - All already in database")
    print("   - SpiderCloud calls: 0")
    print("   - API credits saved: 2")


@pytest.mark.asyncio
async def test_filter_new_job_urls_is_more_efficient(monkeypatch):
    """
    Test demonstrating that filter_new_job_urls is more efficient than filter_existing_job_urls
    when most URLs already exist, because it transfers less data over the network.
    """

    # Simulate 100 URLs, where 95 already exist (common production scenario)
    all_urls = [f"https://boards-api.greenhouse.io/v1/boards/company/jobs/{i}" for i in range(100)]
    existing_urls_set = set(all_urls[:95])  # First 95 exist
    new_urls_expected = all_urls[95:]  # Last 5 are new

    # Track what gets returned by each Convex query
    convex_calls = []

    async def fake_convex_query(endpoint: str, args: dict) -> dict:
        """Mock Convex query that tracks data transfer."""
        convex_calls.append({"endpoint": endpoint, "args": args})

        urls = args.get("urls", [])

        if endpoint == "router:findExistingJobUrls":
            # Returns existing URLs (95 items = more data)
            existing = [u for u in urls if u in existing_urls_set]
            result = {"existing": existing}
            convex_calls[-1]["returned_count"] = len(existing)
            return result

        elif endpoint == "router:filterNewJobUrls":
            # Returns new URLs (5 items = less data)
            new = [u for u in urls if u not in existing_urls_set]
            result = {"new": new}
            convex_calls[-1]["returned_count"] = len(new)
            return result

        return {}

    # Patch the convex_query function
    import job_scrape_application.services.convex_client as convex_module

    monkeypatch.setattr(convex_module, "convex_query", fake_convex_query)

    # Test OLD approach: filter_existing_job_urls (returns 95 URLs)
    existing = await activities.filter_existing_job_urls(all_urls)
    new_urls_via_filter = [u for u in all_urls if u not in existing]

    # Test NEW approach: filter_new_job_urls (returns 5 URLs)
    new_urls_direct = await activities.filter_new_job_urls(all_urls)

    # Verify correctness: both should identify the same 5 new URLs
    assert set(new_urls_via_filter) == set(new_urls_expected)
    assert set(new_urls_direct) == set(new_urls_expected)

    # Verify efficiency: new approach transfers much less data
    old_approach_call = convex_calls[0]
    new_approach_call = convex_calls[1]

    print("\n📊 EFFICIENCY COMPARISON:")
    print(f"   Input: {len(all_urls)} URLs (95 exist, 5 new)")
    print("\n   OLD approach (filter_existing_job_urls):")
    print(f"     - Query: {old_approach_call['endpoint']}")
    print(f"     - Data returned: {old_approach_call['returned_count']} URLs")
    print("     - Python processing: Build set, filter list")
    print("\n   NEW approach (filter_new_job_urls):")
    print(f"     - Query: {new_approach_call['endpoint']}")
    print(f"     - Data returned: {new_approach_call['returned_count']} URLs")
    print("     - Python processing: None needed, use directly")
    print(f"\n   Network efficiency: {old_approach_call['returned_count'] / new_approach_call['returned_count']:.1f}x more data with old approach")

    assert old_approach_call["returned_count"] == 95
    assert new_approach_call["returned_count"] == 5
    assert old_approach_call["returned_count"] > new_approach_call["returned_count"]


@pytest.mark.asyncio
async def test_filter_new_job_urls_error_raises_for_caller_fallback(monkeypatch):
    """
    Test that when filter_new_job_urls encounters an error, it raises an exception
    so the caller can fall back to treating all URLs as new.

    This is critical because returning [] on error would cause ALL URLs to be
    incorrectly filtered out as "existing".
    """
    import job_scrape_application.services.convex_client as convex_module

    async def failing_convex_query(endpoint: str, args: dict) -> dict:
        """Mock Convex query that always fails."""
        raise RuntimeError("Simulated Convex failure")

    monkeypatch.setattr(convex_module, "convex_query", failing_convex_query)

    test_urls = [
        "https://boards-api.greenhouse.io/v1/boards/company/jobs/111",
        "https://boards-api.greenhouse.io/v1/boards/company/jobs/222",
    ]

    # filter_new_job_urls should raise, not return []
    with pytest.raises(RuntimeError, match="Simulated Convex failure"):
        await activities.filter_new_job_urls(test_urls)

    print("\n✅ ERROR HANDLING CORRECT:")
    print("   - filter_new_job_urls raises on error")
    print("   - Caller can catch and fall back to all URLs as new")
    print("   - Prevents incorrectly filtering out all URLs")
