from __future__ import annotations

from job_scrape_application.workflows.site_handlers import (
    AshbyHqHandler,
    GreenhouseHandler,
    get_site_handler,
)


class TestGreenhouseUrlRewriting:
    """Tests for Greenhouse URL rewriting to API URLs via get_api_uri()."""

    def test_rewrites_marketing_site_with_gh_jid_and_board(self):
        """Marketing site URLs with gh_jid and board params should be rewritten to API URLs."""
        handler = GreenhouseHandler()
        url = "https://coreweave.com/careers/job?4607747006&board=coreweave&gh_jid=4607747006"
        api_url = handler.get_api_uri(url)
        assert api_url == "https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4607747006"

    def test_rewrites_boards_greenhouse_url(self):
        """boards.greenhouse.io URLs should be rewritten to API URLs."""
        handler = GreenhouseHandler()
        url = "https://boards.greenhouse.io/samsara/jobs/1234567"
        api_url = handler.get_api_uri(url)
        assert api_url == "https://boards-api.greenhouse.io/v1/boards/samsara/jobs/1234567"

    def test_rewrites_job_boards_greenhouse_url(self):
        """job-boards.greenhouse.io URLs should be rewritten to API URLs."""
        handler = GreenhouseHandler()
        url = "https://job-boards.greenhouse.io/xai/jobs/5012607007"
        api_url = handler.get_api_uri(url)
        assert api_url == "https://boards-api.greenhouse.io/v1/boards/xai/jobs/5012607007"

    def test_preserves_api_detail_url(self):
        """API detail URLs should be returned unchanged."""
        handler = GreenhouseHandler()
        url = "https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4607747006"
        api_url = handler.get_api_uri(url)
        assert api_url == url

    def test_returns_none_for_non_greenhouse_url(self):
        """Non-Greenhouse URLs without gh_jid should return None."""
        handler = GreenhouseHandler()
        url = "https://example.com/job/123"
        api_url = handler.get_api_uri(url)
        assert api_url is None

    def test_returns_none_for_url_without_job_id(self):
        """URLs without a job ID should return None."""
        handler = GreenhouseHandler()
        # Listing page, no specific job
        url = "https://boards.greenhouse.io/samsara/jobs"
        api_url = handler.get_api_uri(url)
        assert api_url is None

    def test_returns_none_for_ashby_url(self):
        """Ashby URLs should not be handled by GreenhouseHandler."""
        handler = GreenhouseHandler()
        url = "https://jobs.ashbyhq.com/lambda/2d656d6c-733f-4072-8bee-847f142c0938"
        # Greenhouse handler should not match Ashby URLs
        assert not handler.matches_url(url)
        # And get_api_uri should return None
        api_url = handler.get_api_uri(url)
        assert api_url is None


class TestAshbyUrlsUseCorrectHandler:
    """Tests to ensure Ashby URLs are handled by AshbyHqHandler, not Greenhouse."""

    def test_ashby_handler_matches_ashby_url(self):
        """AshbyHqHandler should match Ashby URLs."""
        handler = AshbyHqHandler()
        url = "https://jobs.ashbyhq.com/lambda/2d656d6c-733f-4072-8bee-847f142c0938"
        assert handler.matches_url(url)

    def test_get_site_handler_returns_ashby_for_ashby_url(self):
        """get_site_handler should return AshbyHqHandler for Ashby URLs."""
        url = "https://jobs.ashbyhq.com/lambda/2d656d6c-733f-4072-8bee-847f142c0938"
        handler = get_site_handler(url)
        assert isinstance(handler, AshbyHqHandler)

    def test_ashby_url_not_rewritten_to_greenhouse_api(self):
        """Ashby URLs should never be rewritten to Greenhouse API URLs."""
        url = "https://jobs.ashbyhq.com/lambda/2d656d6c-733f-4072-8bee-847f142c0938"
        handler = get_site_handler(url)

        # Handler should be Ashby, not Greenhouse
        assert isinstance(handler, AshbyHqHandler)

        # Ashby handler doesn't have get_api_uri that returns a different URL
        # The URL stays as-is (Ashby API calls use the posting API separately)
        assert handler.matches_url(url)


class TestGreenhouseHandlerMatching:
    """Tests for GreenhouseHandler.matches_url()."""

    def test_matches_boards_greenhouse_io(self):
        handler = GreenhouseHandler()
        assert handler.matches_url("https://boards.greenhouse.io/company/jobs/123")

    def test_matches_job_boards_greenhouse_io(self):
        handler = GreenhouseHandler()
        assert handler.matches_url("https://job-boards.greenhouse.io/company/jobs/123")

    def test_matches_boards_api_greenhouse_io(self):
        handler = GreenhouseHandler()
        assert handler.matches_url("https://boards-api.greenhouse.io/v1/boards/company/jobs/123")

    def test_matches_url_with_gh_jid(self):
        handler = GreenhouseHandler()
        assert handler.matches_url("https://example.com/careers?gh_jid=4607747006")

    def test_does_not_match_plain_example_url(self):
        handler = GreenhouseHandler()
        assert not handler.matches_url("https://example.com/job/123")

    def test_does_not_match_ashby_url(self):
        handler = GreenhouseHandler()
        assert not handler.matches_url("https://jobs.ashbyhq.com/lambda/123")


class TestGreenhouseCompanyUri:
    """Tests for GreenhouseHandler.get_company_uri() which generates apply links."""

    def test_generates_apply_link_from_api_url(self):
        handler = GreenhouseHandler()
        api_url = "https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4607747006"
        company_url = handler.get_company_uri(api_url)
        assert company_url == "https://boards.greenhouse.io/coreweave/jobs/4607747006"

    def test_generates_apply_link_from_marketing_url(self):
        """Marketing site URL should be convertible to a company apply link."""
        handler = GreenhouseHandler()
        marketing_url = "https://coreweave.com/careers/job?board=coreweave&gh_jid=4607747006"
        company_url = handler.get_company_uri(marketing_url)
        assert company_url == "https://boards.greenhouse.io/coreweave/jobs/4607747006"
