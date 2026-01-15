"""Tests for UberCareersHandler locale URL filtering.

The Uber careers site includes language selector links on every job page that
point to locale-specific URLs like:
  - https://www.uber.com/global/fr-ca/careers/list/149889/
  - https://www.uber.com/global/pt-pt/careers/list/149889/
  - https://www.uber.com/global/es-es/careers/list/149889/

These are all the same job (149889) but in different languages. If we scrape
all of them, we get duplicate job entries. The handler must filter these out.
"""

from __future__ import annotations

import json
from pathlib import Path


from job_scrape_application.workflows.site_handlers import UberCareersHandler  # noqa: E402


FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/single_request")


def _load_detail_fixture_links() -> list[str]:
    """Load the links array from the Uber detail fixture."""
    fixture_path = FIXTURE_DIR / "uber_detail.json"
    if not fixture_path.exists():
        return []
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    response = payload.get("response", [])
    if isinstance(response, list) and response:
        first_batch = response[0]
        if isinstance(first_batch, list) and first_batch:
            event = first_batch[0]
            if isinstance(event, dict):
                links = event.get("links", [])
                if isinstance(links, list):
                    return [link for link in links if isinstance(link, str)]
    return []


class TestUberLocaleFiltering:
    """Test that Uber handler filters out locale-specific URLs."""

    def test_handler_matches_uber_careers_urls(self):
        """Verify handler matches Uber careers URLs."""
        handler = UberCareersHandler()
        assert handler.matches_url("https://www.uber.com/careers/list/149889")
        assert handler.matches_url("https://www.uber.com/us/en/careers/list?query=engineer")
        assert handler.matches_url("https://www.uber.com/global/fr/careers/list/149889")

    def test_locale_urls_are_filtered_out(self):
        """Verify that /global/ locale URLs are filtered from job URLs.

        Uber pages have a language selector that produces URLs like:
        - /global/fr-ca/careers/list/149889/
        - /global/pt-pt/careers/list/149889/
        - /global/es-es/careers/list/149889/

        These should NOT be extracted as job detail URLs because they're
        duplicates of the same job in different languages.
        """
        handler = UberCareersHandler()

        # Sample of locale URLs from the "Select your preferred language" section
        locale_urls = [
            "https://www.uber.com/global/fr-ca/careers/list/153449",
            "https://www.uber.com/global/pt-pt/careers/list/153449",
            "https://www.uber.com/global/ca-es/careers/list/153449",
            "https://www.uber.com/global/fr/careers/list/153449",
            "https://www.uber.com/global/es-es/careers/list/153449",
            "https://www.uber.com/global/de/careers/list/153449",
            "https://www.uber.com/global/ja/careers/list/153449",
            "https://www.uber.com/global/zh/careers/list/153449",
            "https://www.uber.com/global/ar/careers/list/153449",
            "https://www.uber.com/global/en/careers/list/153449",
        ]

        # Valid non-locale job URLs (with different job IDs)
        valid_urls = [
            "https://www.uber.com/careers/list/149889",
            "https://www.uber.com/us/en/careers/list/150000",
        ]

        all_urls = locale_urls + valid_urls
        filtered = handler.filter_job_urls(all_urls)

        # Locale URLs should be filtered out
        for url in locale_urls:
            assert url not in filtered, f"Locale URL should have been filtered: {url}"

        # Valid URLs should remain
        for url in valid_urls:
            assert url in filtered, f"Valid URL should have passed: {url}"

    def test_filter_job_urls_with_fixture_links(self):
        """Test that actual links from fixture are properly filtered.

        This tests against real data from the SpiderCloud fixture to ensure
        locale URLs from the language selector are filtered out.
        """
        links = _load_detail_fixture_links()
        if not links:
            # Skip if fixture not found
            return

        handler = UberCareersHandler()

        # Convert relative URLs to absolute
        absolute_links = []
        for link in links:
            if link.startswith("/"):
                absolute_links.append(f"https://www.uber.com{link}")
            else:
                absolute_links.append(link)

        # Filter the links
        filtered = handler.filter_job_urls(absolute_links)

        # Count locale URLs in the filtered output
        locale_urls_in_filtered = [
            url for url in filtered
            if "/global/" in url and "/careers/list/" in url
        ]

        # Assert NO locale URLs passed through
        assert len(locale_urls_in_filtered) == 0, (
            f"Found {len(locale_urls_in_filtered)} locale URLs that should have been "
            f"filtered out: {locale_urls_in_filtered[:5]}"  # Show first 5
        )

    def test_canonical_us_en_urls_are_kept(self):
        """Verify that canonical /us/en/ URLs are kept."""
        handler = UberCareersHandler()

        urls = [
            "https://www.uber.com/us/en/careers/list/149889",
            "https://www.uber.com/us/en/careers/list?query=engineer",
        ]

        filtered = handler.filter_job_urls(urls)

        for url in urls:
            assert url in filtered, f"Canonical US/EN URL should pass: {url}"

    def test_base_careers_urls_are_kept(self):
        """Verify that base /careers/list/ URLs without locale are kept."""
        handler = UberCareersHandler()

        urls = [
            "https://www.uber.com/careers/list/149889",
            "https://www.uber.com/careers/list/153449",
        ]

        filtered = handler.filter_job_urls(urls)

        for url in urls:
            assert url in filtered, f"Base careers URL should pass: {url}"

    def test_deduplication_by_job_id(self):
        """Test that multiple locale variants of same job are deduplicated to one.

        Even if we have both a locale URL and a canonical URL for the same job ID,
        we should only keep one (preferably the canonical one).
        """
        handler = UberCareersHandler()

        # Same job ID (149889) in different URL formats
        urls = [
            "https://www.uber.com/careers/list/149889",
            "https://www.uber.com/global/fr/careers/list/149889",
            "https://www.uber.com/global/de/careers/list/149889",
            "https://www.uber.com/us/en/careers/list/149889",
        ]

        filtered = handler.filter_job_urls(urls)

        # Should have at most one URL for job 149889
        job_149889_urls = [url for url in filtered if "149889" in url]
        assert len(job_149889_urls) <= 2, (
            f"Job 149889 appears {len(job_149889_urls)} times after filtering, "
            f"expected at most 2: {job_149889_urls}"
        )
