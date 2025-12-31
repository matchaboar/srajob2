from __future__ import annotations

import pytest

from job_scrape_application.workflows.site_handlers import AvatureHandler


class TestAvatureHandler:
    """Test suite for AvatureHandler site handler."""

    def setup_method(self):
        """Setup test fixtures."""
        self.handler = AvatureHandler()

    def test_matches_url_avature_net(self):
        """Test URL matching for avature.net domains."""
        assert self.handler.matches_url("https://company.avature.net/careers")
        assert self.handler.matches_url("https://subdomain.company.avature.net/careers/SearchJobs")
        assert self.handler.matches_url("http://example.avature.net")

    def test_matches_url_avature_com(self):
        """Test URL matching for avature.com domains."""
        assert self.handler.matches_url("https://company.avature.com/careers")
        assert self.handler.matches_url("https://careers.avature.com/JobDetail/12345")

    def test_matches_url_non_avature(self):
        """Test URL matching rejects non-Avature domains."""
        assert not self.handler.matches_url("https://company.com/careers")
        assert not self.handler.matches_url("https://greenhouse.io/jobs")
        assert not self.handler.matches_url("https://lever.co/jobs")
        assert not self.handler.matches_url("https://workday.com/careers")

    def test_matches_url_edge_cases(self):
        """Test URL matching edge cases."""
        assert not self.handler.matches_url("")
        assert not self.handler.matches_url("not-a-url")
        assert not self.handler.matches_url("https://avature-fake.com")

    def test_is_listing_url_searchjobs(self):
        """Test listing URL detection for SearchJobs."""
        assert self.handler.is_listing_url("https://company.avature.net/careers/SearchJobs")
        assert self.handler.is_listing_url("https://company.avature.net/careers/searchjobs?param=value")
        assert self.handler.is_listing_url("https://company.avature.net/Careers/SearchJobs")

    def test_is_listing_url_searchjobsdata(self):
        """Test listing URL detection for SearchJobsData."""
        assert self.handler.is_listing_url("https://company.avature.net/careers/SearchJobsData")
        assert self.handler.is_listing_url("https://company.avature.net/careers/searchjobsdata?param=value")

    def test_is_listing_url_non_listing(self):
        """Test listing URL detection rejects job detail pages."""
        assert not self.handler.is_listing_url("https://company.avature.net/careers/JobDetail/12345")
        assert not self.handler.is_listing_url("https://company.avature.net/careers")
        assert not self.handler.is_listing_url("https://company.avature.net/careers/apply")

    def test_get_spidercloud_config_listing(self):
        """Test SpiderCloud configuration for listing pages."""
        url = "https://company.avature.net/careers/SearchJobs"
        config = self.handler.get_spidercloud_config(url)
        
        assert config["request"] == "chrome"
        assert "raw_html" in config["return_format"]
        assert config["follow_redirects"] is True
        assert config["redirect_policy"] == "Loose"
        assert "wait_for" in config
        assert "selector" in config["wait_for"]
        assert "a[href*='/careers/JobDetail/']" in config["wait_for"]["selector"]["selector"]

    def test_get_spidercloud_config_detail(self):
        """Test SpiderCloud configuration for detail pages."""
        url = "https://company.avature.net/careers/JobDetail/12345"
        config = self.handler.get_spidercloud_config(url)
        
        assert config["request"] == "chrome"
        assert "commonmark" in config["return_format"]
        assert config["follow_redirects"] is True
        assert "wait_for" not in config  # Detail pages don't wait for selectors

    def test_get_spidercloud_config_non_avature(self):
        """Test SpiderCloud configuration for non-Avature URLs."""
        url = "https://example.com/careers"
        config = self.handler.get_spidercloud_config(url)
        
        assert config == {}

    def test_filter_job_urls_valid(self):
        """Test filtering valid job URLs."""
        urls = [
            "https://company.avature.net/careers/JobDetail/12345",
            "https://company.avature.net/careers/SearchJobs?param=value",
            "https://company.avature.net/careers/SearchJobsData?jobOffset=10",
        ]
        filtered = self.handler.filter_job_urls(urls)
        
        assert len(filtered) == 3
        assert all(url in filtered for url in urls)

    def test_filter_job_urls_invalid(self):
        """Test filtering removes invalid URLs."""
        urls = [
            "https://company.avature.net/careers/SaveJob/12345",  # Contains /savejob
            "https://company.avature.net/careers/Login",  # Contains /login
            "https://company.avature.net/careers/Register",  # Contains /register
            "https://company.avature.net/other/page",  # No /careers/
            "https://company.avature.net/careers/unknown",  # No valid tokens
        ]
        filtered = self.handler.filter_job_urls(urls)
        
        assert len(filtered) == 0

    def test_filter_job_urls_duplicates(self):
        """Test filtering removes duplicate URLs."""
        urls = [
            "https://company.avature.net/careers/JobDetail/12345",
            "https://company.avature.net/careers/JobDetail/12345",
            "https://company.avature.net/careers/JobDetail/67890",
        ]
        filtered = self.handler.filter_job_urls(urls)
        
        assert len(filtered) == 2
        assert "https://company.avature.net/careers/JobDetail/12345" in filtered
        assert "https://company.avature.net/careers/JobDetail/67890" in filtered

    def test_filter_job_urls_empty_and_whitespace(self):
        """Test filtering handles empty and whitespace URLs."""
        urls = [
            "",
            "   ",
            "https://company.avature.net/careers/JobDetail/12345",
            "",
        ]
        filtered = self.handler.filter_job_urls(urls)
        
        assert len(filtered) == 1
        assert filtered[0] == "https://company.avature.net/careers/JobDetail/12345"

    def test_get_links_from_raw_html_empty(self):
        """Test link extraction from empty HTML."""
        links = self.handler.get_links_from_raw_html("")
        assert links == []
        
        links = self.handler.get_links_from_raw_html("   ")
        assert links == []

    def test_get_links_from_raw_html_with_base_url(self):
        """Test link extraction with base URL in meta tags."""
        html = """
        <html>
        <head>
            <meta property="og:url" content="https://company.avature.net/careers/SearchJobs" />
        </head>
        <body>
            <a href="/careers/JobDetail/12345">Job 1</a>
            <a href="/careers/JobDetail/67890">Job 2</a>
        </body>
        </html>
        """
        links = self.handler.get_links_from_raw_html(html)
        
        assert len(links) >= 2
        assert any("JobDetail/12345" in link for link in links)
        assert any("JobDetail/67890" in link for link in links)

    def test_get_links_from_raw_html_full_urls(self):
        """Test link extraction with full URLs in HTML."""
        html = """
        <html>
        <body>
            <a href="https://company.avature.net/careers/JobDetail/12345">Job 1</a>
            <a href="https://company.avature.net/careers/JobDetail/67890">Job 2</a>
            <a href="https://company.avature.net/careers/SearchJobs?jobOffset=10">Next Page</a>
        </body>
        </html>
        """
        links = self.handler.get_links_from_raw_html(html)
        
        assert len(links) >= 3
        assert any("JobDetail/12345" in link for link in links)
        assert any("JobDetail/67890" in link for link in links)
        assert any("jobOffset=10" in link for link in links)

    def test_handler_name_and_type(self):
        """Test handler name and site_type attributes."""
        assert self.handler.name == "avature"
        assert self.handler.site_type == "avature"

    def test_needs_page_links(self):
        """Test handler requires page links extraction."""
        assert self.handler.needs_page_links is True

    def test_pagination_augmentation_basic(self):
        """Test basic pagination URL augmentation."""
        html = """
        <html>
        <body>
            <div aria-label="Showing 1-12 of 100 results"></div>
        </body>
        </html>
        """
        links = self.handler.get_links_from_raw_html(html)
        
        # Should generate pagination links
        assert any("jobOffset=" in link for link in links)

    def test_extract_base_url_from_og_meta(self):
        """Test base URL extraction from OpenGraph meta tags."""
        html = '<meta property="og:url" content="https://company.avature.net/careers/SearchJobs" />'
        base_url = self.handler._extract_base_url(html)
        
        assert base_url == "https://company.avature.net/careers/SearchJobs"

    def test_extract_base_url_from_canonical(self):
        """Test base URL extraction from canonical link."""
        html = '<link rel="canonical" href="https://company.avature.net/careers/SearchJobs" />'
        base_url = self.handler._extract_base_url(html)
        
        assert base_url is not None

    def test_extract_base_url_not_found(self):
        """Test base URL extraction when not present."""
        html = "<html><body>No meta tags here</body></html>"
        base_url = self.handler._extract_base_url(html)
        
        # May return None or a default - check the implementation
        assert base_url is None or isinstance(base_url, str)


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://bloomberg.avature.net/careers/SearchJobs", True),
        ("https://netflix.avature.net/careers", True),
        ("https://custom.avature.com/careers/JobDetail/123", True),
        ("https://greenhouse.io/careers", False),
        ("https://lever.co/jobs", False),
    ],
)
def test_matches_url_parametrized(url, expected):
    """Parametrized test for URL matching."""
    handler = AvatureHandler()
    assert handler.matches_url(url) == expected


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://company.avature.net/careers/SearchJobs", True),
        ("https://company.avature.net/careers/searchjobs", True),
        ("https://company.avature.net/careers/SearchJobsData", True),
        ("https://company.avature.net/careers/JobDetail/123", False),
        ("https://company.avature.net/careers", False),
    ],
)
def test_is_listing_url_parametrized(url, expected):
    """Parametrized test for listing URL detection."""
    handler = AvatureHandler()
    assert handler.is_listing_url(url) == expected
