"""Tests for the WorkflowTest utility module itself."""

from __future__ import annotations

import pytest

from job_scrape_application.workflows.workflow.test_utils import (
    CapturedStepCalls,
    StepOverride,
    WorkflowTest,
)


class TestCapturedStepCalls:
    """Tests for CapturedStepCalls dataclass."""

    def test_default_values(self) -> None:
        captured = CapturedStepCalls()
        assert captured.calls == {}
        assert captured.convex_queries == []
        assert captured.convex_mutations == []
        assert captured.stored_scrapes == []
        assert captured.queue_operations == []
        assert captured.telemetry_events == []

    def test_calls_default_dict_behavior(self) -> None:
        captured = CapturedStepCalls()
        # Should not raise KeyError
        captured.calls["nonexistent_step"].append({"test": True})
        assert len(captured.calls["nonexistent_step"]) == 1


class TestStepOverride:
    """Tests for StepOverride dataclass."""

    def test_default_values(self) -> None:
        override = StepOverride()
        assert override.return_value is None
        assert override.side_effect is None

    def test_with_return_value(self) -> None:
        override = StepOverride(return_value=["url1", "url2"])
        assert override.return_value == ["url1", "url2"]

    def test_with_side_effect(self) -> None:
        error = ValueError("test error")
        override = StepOverride(side_effect=error)
        assert override.side_effect is error


class TestWorkflowTestInit:
    """Tests for WorkflowTest initialization."""

    def test_init(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)
        assert wt.tmp_path == tmp_path
        assert wt.monkeypatch == monkeypatch
        assert wt._mocks_applied is False

    def test_fluent_api_returns_self(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)

        result1 = wt.with_spidercloud_response("https://example.com", {"jobs": []})
        assert result1 is wt

        result2 = wt.mock_step("filter_new_job_urls", return_value=[])
        assert result2 is wt

        result3 = wt.with_query_response("router:getSiteById", {"id": "test"})
        assert result3 is wt


class TestWorkflowTestSpiderCloudFixtures:
    """Tests for SpiderCloud fixture management."""

    def test_add_fixture(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)
        wt.with_spidercloud_response(
            url="https://boards.greenhouse.io/company",
            response={"jobs": [{"url": "https://company.com/job/123"}]},
        )

        assert "https://boards.greenhouse.io/company" in wt._fixtures
        assert wt._fixtures["https://boards.greenhouse.io/company"]["jobs"][0]["url"] == (
            "https://company.com/job/123"
        )

    def test_multiple_fixtures(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)
        wt.with_spidercloud_response("https://site1.com", {"jobs": []})
        wt.with_spidercloud_response("https://site2.com", {"jobs": [{"url": "test"}]})

        assert len(wt._fixtures) == 2


class TestWorkflowTestStepOverrides:
    """Tests for step override functionality."""

    def test_mock_step_with_return_value(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)
        wt.mock_step("filter_new_job_urls", return_value=["https://new.com/job"])

        assert "filter_new_job_urls" in wt._step_overrides
        assert wt._step_overrides["filter_new_job_urls"].return_value == ["https://new.com/job"]

    def test_mock_step_with_exception(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)
        error = RuntimeError("Convex timeout")
        wt.mock_step("filter_new_job_urls", side_effect=error)

        assert wt._step_overrides["filter_new_job_urls"].side_effect is error


class TestWorkflowTestCallTracking:
    """Tests for step call tracking."""

    def test_call_count_empty(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)
        assert wt.call_count("store_scrape") == 0
        assert wt.call_count("nonexistent") == 0

    def test_step_calls_property(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)
        # Manually add some calls to test the property
        wt.captured.calls["test_step"].append({"arg": "value"})

        assert wt.step_calls["test_step"] == [{"arg": "value"}]

    def test_get_step_call(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)
        wt.captured.calls["test_step"].append({"first": True})
        wt.captured.calls["test_step"].append({"second": True})

        assert wt.get_step_call("test_step", 0) == {"first": True}
        assert wt.get_step_call("test_step", 1) == {"second": True}
        assert wt.get_step_call("test_step", 2) is None
        assert wt.get_step_call("nonexistent") is None


class TestWorkflowTestQueryResponses:
    """Tests for Convex query response configuration."""

    def test_with_query_response_static(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)
        wt.with_query_response("router:getSiteById", {"id": "site-1", "paginationLimit": 5})

        assert wt._query_responses["router:getSiteById"] == {
            "id": "site-1",
            "paginationLimit": 5,
        }

    def test_with_query_response_callable(self, tmp_path, monkeypatch) -> None:
        wt = WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)

        def dynamic_response(payload):
            return {"id": payload.get("id"), "found": True}

        wt.with_query_response("router:getSiteById", dynamic_response)

        # Verify it's stored as callable
        result = wt._query_responses["router:getSiteById"]({"id": "test-123"})
        assert result == {"id": "test-123", "found": True}


class TestWorkflowTestMocking:
    """Tests for mock behavior when run() is called."""

    def test_convex_query_default_response(self, workflow_test) -> None:
        """Test that default Convex query responses work."""
        workflow_test._apply_mocks()

        # Call the mocked query directly
        result = workflow_test._mock_convex_query(
            "router:filterNewJobUrls",
            {"urls": ["https://example.com/job/1", "https://example.com/job/2"]},
        )

        assert result == {"new": ["https://example.com/job/1", "https://example.com/job/2"]}
        assert len(workflow_test.captured.convex_queries) == 1

    def test_convex_query_custom_response(self, workflow_test) -> None:
        """Test that custom query responses are used."""
        workflow_test.with_query_response("router:getSiteById", {"custom": True})
        workflow_test._apply_mocks()

        result = workflow_test._mock_convex_query(
            "router:getSiteById",
            {"id": "test-site"},
        )

        assert result == {"custom": True}

    def test_filter_new_job_urls_default(self, workflow_test) -> None:
        """Test that filter_new_job_urls returns all URLs by default."""
        workflow_test._apply_mocks()

        urls = ["https://example.com/job/1", "https://example.com/job/2"]
        result = workflow_test._mock_filter_new_job_urls(urls)

        assert result == urls
        assert workflow_test.call_count("filter_new_job_urls") == 1

    def test_filter_new_job_urls_override(self, workflow_test) -> None:
        """Test that filter_new_job_urls can be overridden."""
        workflow_test.mock_step("filter_new_job_urls", return_value=[])
        workflow_test._apply_mocks()

        urls = ["https://example.com/job/1"]
        result = workflow_test._mock_filter_new_job_urls(urls)

        assert result == []

    def test_store_scrape_captures_data(self, workflow_test) -> None:
        """Test that store_scrape captures scrape data."""
        workflow_test._apply_mocks()

        scrape = {"items": {"normalized": [{"title": "Engineer"}]}}
        scrape_id = workflow_test._mock_store_scrape(scrape)

        assert scrape_id == "scrape-1"
        assert len(workflow_test.captured.stored_scrapes) == 1
        assert workflow_test.captured.stored_scrapes[0] == scrape

    def test_enqueue_scrape_urls_returns_count(self, workflow_test) -> None:
        """Test that enqueue_scrape_urls returns queued count."""
        workflow_test._apply_mocks()

        payload = {"urls": ["url1", "url2", "url3"]}
        result = workflow_test._mock_enqueue_scrape_urls(payload)

        assert result == {"queued": 3}
        assert workflow_test.call_count("enqueue_scrape_urls") == 1

    def test_step_override_with_exception(self, workflow_test) -> None:
        """Test that step override with exception raises."""
        workflow_test.mock_step("filter_new_job_urls", side_effect=RuntimeError("Test error"))
        workflow_test._apply_mocks()

        with pytest.raises(RuntimeError, match="Test error"):
            workflow_test._mock_filter_new_job_urls(["url"])

    def test_step_override_with_callable(self, workflow_test) -> None:
        """Test that step override with callable invokes it."""

        def custom_filter(urls):
            return [u for u in urls if "keep" in u]

        workflow_test.mock_step("filter_new_job_urls", side_effect=custom_filter)
        workflow_test._apply_mocks()

        result = workflow_test._mock_filter_new_job_urls(
            ["https://keep.com/job", "https://remove.com/job"]
        )

        assert result == ["https://keep.com/job"]
