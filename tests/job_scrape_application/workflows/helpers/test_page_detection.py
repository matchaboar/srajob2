"""Tests for page_detection module."""




from job_scrape_application.workflows.helpers.page_detection import (
    _ERROR_LANDING_PHRASES,
    _JOB_DETAIL_MARKERS,
    _LISTING_CARD_APPLY_MARKERS,
    _LISTING_CARD_POSTED_RE,
    _LISTING_FILTER_TERMS,
    _LISTING_URL_TOKENS,
    _description_mentions_listing_url,
    _looks_like_listing_card_snippet,
    _url_is_listing_root,
    _url_suggests_listing,
    is_invalid_job_title,
    is_invalid_job_url,
    looks_like_error_landing,
    looks_like_job_listing_page,
    looks_like_non_job_page,
)


class TestConstants:
    """Tests for page detection constants."""

    def test_error_landing_phrases_includes_not_found(self):
        assert "page not found" in _ERROR_LANDING_PHRASES

    def test_error_landing_phrases_includes_expired(self):
        assert "job has expired" in _ERROR_LANDING_PHRASES

    def test_listing_filter_terms_includes_open_positions(self):
        assert "open positions" in _LISTING_FILTER_TERMS

    def test_listing_card_apply_markers_includes_apply_now(self):
        assert "apply now" in _LISTING_CARD_APPLY_MARKERS

    def test_listing_url_tokens_includes_jobs(self):
        assert "jobs" in _LISTING_URL_TOKENS

    def test_job_detail_markers_includes_responsibilities(self):
        assert "responsibilities" in _JOB_DETAIL_MARKERS

    def test_listing_card_posted_re_matches_posted_ago(self):
        match = _LISTING_CARD_POSTED_RE.search("posted 3 days ago")
        assert match is not None


class TestLooksLikeErrorLanding:
    """Tests for looks_like_error_landing function."""

    def test_detects_404_page(self):
        result = looks_like_error_landing("404 Not Found", "The page could not be found")
        assert result is True

    def test_detects_job_not_found(self):
        result = looks_like_error_landing("Job Not Found", "This job posting was not found")
        assert result is True

    def test_detects_expired_job(self):
        result = looks_like_error_landing("Job Posting", "This job has expired")
        assert result is True

    def test_returns_false_for_valid_job(self):
        result = looks_like_error_landing(
            "Software Engineer",
            "We are looking for a software engineer to join our team. Responsibilities include..."
        )
        assert result is False


class TestUrlIsListingRoot:
    """Tests for _url_is_listing_root function."""

    def test_returns_true_for_jobs_path(self):
        result = _url_is_listing_root("https://example.com/jobs")
        assert result is True

    def test_returns_true_for_careers_path(self):
        result = _url_is_listing_root("https://example.com/careers")
        assert result is True

    def test_returns_false_for_job_id_path(self):
        result = _url_is_listing_root("https://example.com/jobs/12345")
        assert result is False

    def test_returns_false_for_none(self):
        result = _url_is_listing_root(None)
        assert result is False


class TestDescriptionMentionsListingUrl:
    """Tests for _description_mentions_listing_url function."""

    def test_returns_true_for_jobs_url(self):
        result = _description_mentions_listing_url("Check out our openings at https://example.com/jobs")
        assert result is True

    def test_returns_false_for_specific_job_url(self):
        result = _description_mentions_listing_url("Apply at https://example.com/jobs/12345")
        assert result is False

    def test_returns_false_for_empty(self):
        result = _description_mentions_listing_url("")
        assert result is False


class TestLooksLikeJobListingPage:
    """Tests for looks_like_job_listing_page function."""

    def test_detects_listing_page_with_filter_terms(self):
        result = looks_like_job_listing_page(
            "Careers",
            "Open positions\nSearch for opportunities\nSelect department\nSelect location\nView all jobs"
        )
        assert result is True

    def test_returns_false_for_job_detail(self):
        result = looks_like_job_listing_page(
            "Software Engineer",
            "Responsibilities:\n- Write code\n- Review PRs\n\nRequirements:\n- 5 years experience"
        )
        assert result is False

    def test_returns_false_for_empty_description(self):
        result = looks_like_job_listing_page("Title", "")
        assert result is False


class TestUrlSuggestsListing:
    """Tests for _url_suggests_listing function."""

    def test_returns_false_for_none(self):
        result = _url_suggests_listing(None)
        assert result is False

    def test_returns_false_for_job_with_id(self):
        result = _url_suggests_listing("https://example.com/jobs/12345")
        assert result is False


class TestIsInvalidJobUrl:
    """Tests for is_invalid_job_url function."""

    def test_rejects_anchor_fragments(self):
        """Anchor fragments like #sub-title3 should be rejected."""
        assert is_invalid_job_url("#sub-title3") is True
        assert is_invalid_job_url("#content") is True
        assert is_invalid_job_url("#Page Block Perp5Eobrki") is True

    def test_rejects_social_media_urls(self):
        """Social media URLs should be rejected."""
        assert is_invalid_job_url("https://www.instagram.com/robinhoodapp") is True
        assert is_invalid_job_url("https://www.linkedin.com/company/robinhood") is True
        assert is_invalid_job_url("https://twitter.com/robinhood") is True

    def test_rejects_internal_share_links(self):
        """Internal application share links should be rejected."""
        assert is_invalid_job_url("https://affable-kiwi-46.convex.site/share/job?id=abc") is True
        assert is_invalid_job_url("https://elegant-magpie-239.convex.site/share/job?id=abc&app=https%3A%2F%2Flocalhost%3A5173") is True

    def test_rejects_privacy_pages(self):
        """Privacy and policy pages should be rejected."""
        assert is_invalid_job_url("https://privacy.coupang.com/en/notice") is True
        assert is_invalid_job_url("https://careers.robinhood.com/applicantprivacypolicy") is True
        assert is_invalid_job_url("https://example.com/privacy-policy") is True

    def test_rejects_login_pages(self):
        """Login and auth pages should be rejected."""
        assert is_invalid_job_url("https://api.greenhouse.io/en/land/jobsnotice") is True
        assert is_invalid_job_url("https://example.com/login") is True
        assert is_invalid_job_url("https://example.com/signin") is True

    def test_rejects_accommodation_forms(self):
        """Accommodation and HR forms should be rejected."""
        assert is_invalid_job_url("https://robinhood.hracuity.net/webform/123") is True

    def test_rejects_investor_pages(self):
        """Investor relations pages should be rejected."""
        assert is_invalid_job_url("https://investors.robinhood.com/static-files/abc") is True

    def test_rejects_esg_pages(self):
        """ESG pages should be rejected."""
        assert is_invalid_job_url("https://esg.robinhood.com/") is True

    def test_accepts_valid_job_urls(self):
        """Valid job URLs should be accepted."""
        assert is_invalid_job_url("https://boards.greenhouse.io/robinhood/jobs/7155938") is False
        assert is_invalid_job_url("https://careers.robinhood.com/jobs/123") is False
        assert is_invalid_job_url("https://jobs.ashbyhq.com/company/job-id") is False

    def test_rejects_none_and_empty(self):
        """None and empty strings should be rejected."""
        assert is_invalid_job_url(None) is True
        assert is_invalid_job_url("") is True
        assert is_invalid_job_url("   ") is True

    def test_rejects_company_homepages(self):
        """Company homepages (root paths) should be rejected.

        This catches the Coupang bug where http://www.coupang.com was
        scraped as a job posting.
        """
        assert is_invalid_job_url("http://www.coupang.com") is True
        assert is_invalid_job_url("http://www.coupang.com/") is True
        assert is_invalid_job_url("https://coupang.com") is True
        assert is_invalid_job_url("https://example.com/") is True

    def test_allows_job_board_root_urls(self):
        """Job board root URLs should be allowed."""
        # These are valid job board listing pages
        assert is_invalid_job_url("https://jobs.lever.co/company") is False
        assert is_invalid_job_url("https://careers.example.com/") is False

    def test_coupang_privacy_urls_blocked(self):
        """Coupang privacy URLs in job descriptions should be blocked.

        Coupang job descriptions contain links to their privacy policy pages.
        These should not be treated as job URLs.
        """
        assert is_invalid_job_url("https://privacy.coupang.com/en/notice") is True
        assert is_invalid_job_url("https://privacy.coupang.com/en/easy") is True
        assert is_invalid_job_url("https://privacy.coupang.com/en/center") is True
        assert is_invalid_job_url("https://privacy.coupang.com/en/land/jobs") is True
        assert is_invalid_job_url("https://privacy.coupang.com/en/land/jobs/") is True


class TestIsInvalidJobTitle:
    """Tests for is_invalid_job_title function."""

    def test_rejects_markdown_headers(self):
        """Markdown header artifacts should be rejected."""
        assert is_invalid_job_title("#Sub Title3") is True
        assert is_invalid_job_title("#Sub Title5") is True
        assert is_invalid_job_title("#Content") is True
        assert is_invalid_job_title("#Page Block Perp5Eobrki") is True

    def test_rejects_partial_markdown_links(self):
        """Partial markdown link artifacts should be rejected."""
        assert is_invalid_job_title("[6. Personal Information Safeguard Measures") is True

    def test_rejects_generic_page_titles(self):
        """Generic non-job page titles should be rejected."""
        assert is_invalid_job_title("Sign In") is True
        assert is_invalid_job_title("sign in") is True
        assert is_invalid_job_title("Untitled") is True
        assert is_invalid_job_title("Login") is True

    def test_rejects_url_as_title(self):
        """URL-like titles should be rejected."""
        assert is_invalid_job_title("Www.Coupang.Com") is True
        assert is_invalid_job_title("https://example.com") is True

    def test_rejects_members_en(self):
        """Navigation element titles should be rejected."""
        assert is_invalid_job_title("Members En") is True

    def test_accepts_valid_job_titles(self):
        """Valid job titles should be accepted."""
        assert is_invalid_job_title("Software Engineer") is False
        assert is_invalid_job_title("Internal Communications Intern") is False
        assert is_invalid_job_title("AI/HPC Pre-Sales Systems Engineer") is False
        assert is_invalid_job_title("Senior Product Manager, Platform") is False

    def test_rejects_none_and_empty(self):
        """None and empty strings should be rejected."""
        assert is_invalid_job_title(None) is True
        assert is_invalid_job_title("") is True
        assert is_invalid_job_title("   ") is True

    def test_rejects_very_short_titles(self):
        """Very short titles (likely garbage) should be rejected."""
        assert is_invalid_job_title("AB") is True
        assert is_invalid_job_title("X") is True


class TestLooksLikeNonJobPage:
    """Tests for looks_like_non_job_page function."""

    def test_detects_login_page_by_content(self):
        """Login pages should be detected by content analysis."""
        result = looks_like_non_job_page(
            "Sign In",
            "Sign in with Google. Forgot your password? Keep me signed in. Continue to SSO."
        )
        assert result is True

    def test_detects_privacy_page_by_content(self):
        """Privacy pages should be detected by content analysis."""
        result = looks_like_non_job_page(
            "Privacy Notice",
            "This privacy notice explains what personal information collected and how we use it."
        )
        assert result is True

    def test_detects_invalid_url(self):
        """Pages with invalid URLs should be detected."""
        result = looks_like_non_job_page(
            "Robinhood",
            "Some content",
            url="https://www.instagram.com/robinhoodapp"
        )
        assert result is True

    def test_detects_invalid_title(self):
        """Pages with invalid titles should be detected."""
        result = looks_like_non_job_page(
            "#Content",
            "Some description content here",
            url="https://example.com/jobs/123"
        )
        assert result is True

    def test_accepts_valid_job_page(self):
        """Valid job pages should be accepted."""
        result = looks_like_non_job_page(
            "Software Engineer",
            "We are looking for a software engineer. Responsibilities include writing code.",
            url="https://boards.greenhouse.io/company/jobs/123"
        )
        assert result is False

    def test_accepts_job_with_none_url(self):
        """Jobs with None URL should be evaluated on title/content."""
        result = looks_like_non_job_page(
            "Senior Developer",
            "Join our team. Requirements: 5+ years experience.",
            url=None
        )
        assert result is False
