"""Tests for url_handling module."""




from job_scrape_application.workflows.helpers.url_handling import (
    _apply_url_candidates,
    _first_url,
    _score_apply_url,
    _strip_ashby_application_url,
    prefer_apply_url,
)


class TestScoreApplyUrl:
    """Tests for _score_apply_url function."""

    def test_scores_company_domain_highest(self):
        result = _score_apply_url("https://careers.acme.com/jobs/123")
        assert result == 2

    def test_scores_greenhouse_io_medium(self):
        result = _score_apply_url("https://boards.greenhouse.io/acme/jobs/123")
        assert result == 1

    def test_scores_greenhouse_api_lowest(self):
        result = _score_apply_url("https://boards-api.greenhouse.io/v1/jobs")
        assert result == 0

    def test_scores_api_greenhouse_io_lowest(self):
        result = _score_apply_url("https://api.greenhouse.io/jobs")
        assert result == 0

    def test_scores_invalid_url_negative(self):
        result = _score_apply_url("")
        assert result == -1


class TestStripAshbyApplicationUrl:
    """Tests for _strip_ashby_application_url function."""

    def test_strips_application_suffix(self):
        url = "https://jobs.ashbyhq.com/acme/123/application"
        result = _strip_ashby_application_url(url)
        assert result == "https://jobs.ashbyhq.com/acme/123"

    def test_leaves_non_ashby_url_unchanged(self):
        url = "https://example.com/jobs/application"
        result = _strip_ashby_application_url(url)
        assert result == url

    def test_leaves_non_application_url_unchanged(self):
        url = "https://jobs.ashbyhq.com/acme/123"
        result = _strip_ashby_application_url(url)
        assert result == url


class TestApplyUrlCandidates:
    """Tests for _apply_url_candidates function."""

    def test_extracts_apply_url(self):
        row = {"apply_url": "https://example.com/apply"}
        result = _apply_url_candidates(row)
        assert "https://example.com/apply" in result

    def test_extracts_url_field(self):
        row = {"url": "https://example.com/job"}
        result = _apply_url_candidates(row)
        assert "https://example.com/job" in result

    def test_returns_empty_for_empty_row(self):
        result = _apply_url_candidates({})
        assert result == []

    def test_skips_empty_strings(self):
        row = {"apply_url": "", "url": "https://example.com"}
        result = _apply_url_candidates(row)
        assert len(result) == 1
        assert result[0] == "https://example.com"


class TestPreferApplyUrl:
    """Tests for prefer_apply_url function."""

    def test_prefers_company_domain_over_greenhouse(self):
        row = {
            "apply_url": "https://boards.greenhouse.io/acme/jobs/123",
            "url": "https://careers.acme.com/jobs/123",
        }
        result = prefer_apply_url(row)
        assert result == "https://careers.acme.com/jobs/123"

    def test_returns_none_for_empty_row(self):
        result = prefer_apply_url({})
        assert result is None

    def test_deduplicates_urls(self):
        row = {
            "apply_url": "https://example.com/job",
            "url": "https://example.com/job",
        }
        result = prefer_apply_url(row)
        assert result == "https://example.com/job"


class TestFirstUrl:
    """Tests for _first_url function."""

    def test_returns_http_url(self):
        result = _first_url("https://example.com")
        assert result == "https://example.com"

    def test_returns_none_for_non_url(self):
        result = _first_url("not a url")
        assert result is None

    def test_returns_first_url_from_list(self):
        result = _first_url(["https://example.com", "https://other.com"])
        assert result == "https://example.com"

    def test_returns_none_for_empty_list(self):
        result = _first_url([])
        assert result is None

    def test_returns_none_for_none(self):
        result = _first_url(None)
        assert result is None
