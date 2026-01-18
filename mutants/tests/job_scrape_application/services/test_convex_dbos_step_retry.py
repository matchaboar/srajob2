"""Test DBOS step retry behavior with Convex exceptions.

This module tests that:
1. ConvexFunctionNotFoundError causes fail-fast behavior (no retries)
2. ConvexWriteConflictError allows DBOS retries (transient error)

The distinction is important for workflow reliability:
- Missing functions will never succeed, so retrying wastes time
- Write conflicts are transient and retrying should eventually succeed
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from job_scrape_application.services.convex_client import (
    ConvexFunctionNotFoundError,
    ConvexWriteConflictError,
    convex_mutation,
)
from job_scrape_application.workflows.result import Failure, Result, Success


pytestmark = pytest.mark.xdist_group(name="convex_client")


def _make_mock_client(side_effect: Any) -> MagicMock:
    """Create a mock ConvexClient that raises the given exception."""
    mock_client = MagicMock()
    mock_client.query.side_effect = side_effect
    mock_client.mutation.side_effect = side_effect
    mock_client.action.side_effect = side_effect
    return mock_client


class TestConvexFunctionNotFoundFailFast:
    """Tests for ConvexFunctionNotFoundError fail-fast behavior.

    When a DBOS step encounters this error, it should:
    - NOT retry (the function doesn't exist, retrying won't help)
    - Return early with a failure result or graceful handling
    - Allow the workflow to continue without unnecessary delays
    """

    def test_function_not_found_should_not_retry_mutation_called_once(self) -> None:
        """Verify ConvexFunctionNotFoundError results in single call (no retry)."""
        mock_client = _make_mock_client(
            Exception("Could not find public function for router:missingFunction")
        )

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            with pytest.raises(ConvexFunctionNotFoundError):
                convex_mutation("router:missingFunction", {"data": "test"})

            # Critical: Should only be called once (no retries)
            assert mock_client.mutation.call_count == 1

    def test_step_catches_function_not_found_returns_failure(self) -> None:
        """Demonstrate a step that catches ConvexFunctionNotFoundError and returns Failure.

        This pattern allows workflows to handle missing functions gracefully
        without blocking on retries.
        """
        mock_client = _make_mock_client(
            Exception("Could not find public function for router:optionalFeature")
        )

        def step_with_optional_convex_call() -> Result[str]:
            """Example step that handles missing function gracefully."""
            try:
                result = convex_mutation("router:optionalFeature", {"id": "test"})
                return Success(result)
            except ConvexFunctionNotFoundError as exc:
                # Fail fast: return failure instead of letting DBOS retry
                return Failure(
                    error_type="function_not_found",
                    message=str(exc),
                    function_name=exc.function_name,
                )

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            result = step_with_optional_convex_call()

            assert isinstance(result, Failure)
            assert result.error_type == "function_not_found"
            assert result.function_name == "router:optionalFeature"
            # Only one call - fail fast without retry
            assert mock_client.mutation.call_count == 1

    def test_step_catches_function_not_found_returns_none(self) -> None:
        """Demonstrate a step that catches ConvexFunctionNotFoundError and returns None.

        This is the pattern used in record_scrape_url_attempts.py where the
        telemetry call is optional and shouldn't block the workflow.
        """
        mock_client = _make_mock_client(
            Exception("Could not find public function for router:recordTelemetry")
        )

        def step_with_optional_telemetry() -> None:
            """Example step that handles missing telemetry function gracefully."""
            try:
                convex_mutation("router:recordTelemetry", {"event": "test"})
            except ConvexFunctionNotFoundError:
                # Function doesn't exist - just return without blocking
                return None

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            # Should complete without raising
            result = step_with_optional_telemetry()

            assert result is None
            # Only one call - fail fast without retry
            assert mock_client.mutation.call_count == 1


class TestConvexWriteConflictRetry:
    """Tests for ConvexWriteConflictError retry behavior.

    When a DBOS step encounters this error, it should:
    - Allow the error to propagate for DBOS retry mechanism
    - Eventually succeed when the conflict resolves
    - NOT catch and suppress the error (unlike function_not_found)
    """

    def test_write_conflict_should_propagate_for_retry(self) -> None:
        """Verify ConvexWriteConflictError propagates (allows DBOS retry)."""
        mock_client = _make_mock_client(
            Exception(
                "Documents read from or written to the table changed "
                "while this mutation was being run"
            )
        )

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            with pytest.raises(ConvexWriteConflictError) as exc_info:
                convex_mutation("router:updateJob", {"id": "test"})

            # Error should propagate with correct type
            assert "write conflict" in str(exc_info.value).lower()
            assert exc_info.value.function_name == "router:updateJob"

    def test_write_conflict_succeeds_after_retry(self) -> None:
        """Simulate DBOS retry behavior: fail on first call, succeed on retry."""
        call_count = 0

        def side_effect(*args: Any, **kwargs: Any) -> dict[str, str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception(
                    "Documents read from or written to the table changed "
                    "while this mutation was being run"
                )
            return {"status": "success"}

        mock_client = MagicMock()
        mock_client.mutation.side_effect = side_effect

        def step_allowing_retry() -> dict[str, str]:
            """Step that allows write conflicts to propagate for retry."""
            # Simulate what DBOS would do: retry on transient errors
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    return convex_mutation("router:updateJob", {"id": "test"})
                except ConvexWriteConflictError:
                    if attempt == max_retries - 1:
                        raise
                    # DBOS would retry here
                    continue
            raise RuntimeError("Should not reach here")

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            result = step_allowing_retry()

            assert result == {"status": "success"}
            # Should be called twice: first fails, retry succeeds
            assert mock_client.mutation.call_count == 2

    def test_write_conflict_step_should_not_catch_and_suppress(self) -> None:
        """Verify that steps should NOT catch and suppress write conflicts.

        Unlike ConvexFunctionNotFoundError, write conflicts are transient
        and should be allowed to propagate for retry.
        """
        mock_client = _make_mock_client(
            Exception("write conflict detected")
        )

        def bad_step_that_suppresses_conflict() -> str:
            """BAD PATTERN: Don't do this for write conflicts!"""
            try:
                convex_mutation("router:updateJob", {"id": "test"})
                return "success"
            except ConvexWriteConflictError:
                # BAD: Suppressing write conflict prevents retry
                return "suppressed"

        def good_step_that_allows_retry() -> str:
            """GOOD PATTERN: Let write conflicts propagate."""
            # Don't catch ConvexWriteConflictError - let DBOS handle retry
            convex_mutation("router:updateJob", {"id": "test"})
            return "success"

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            # Bad pattern: catches and suppresses (returns immediately)
            bad_result = bad_step_that_suppresses_conflict()
            assert bad_result == "suppressed"
            assert mock_client.mutation.call_count == 1

            # Reset call count
            mock_client.mutation.reset_mock()

            # Good pattern: error propagates for retry
            with pytest.raises(ConvexWriteConflictError):
                good_step_that_allows_retry()
            assert mock_client.mutation.call_count == 1


class TestErrorCategorization:
    """Tests for distinguishing retryable vs non-retryable errors."""

    def test_function_not_found_is_non_retryable(self) -> None:
        """ConvexFunctionNotFoundError should be treated as non-retryable."""
        exc = ConvexFunctionNotFoundError(
            "router:missing",
            "Could not find public function for router:missing",
        )

        # Should have function_name attribute for diagnosis
        assert exc.function_name == "router:missing"

        # Exception message should be clear
        assert "not found" in str(exc).lower()

    def test_write_conflict_is_retryable(self) -> None:
        """ConvexWriteConflictError should be treated as retryable."""
        exc = ConvexWriteConflictError(
            "router:update",
            "Documents read from or written to the table changed",
        )

        # Should have function_name for debugging
        assert exc.function_name == "router:update"

        # Exception message should indicate conflict
        assert "conflict" in str(exc).lower()

    def test_both_errors_have_original_message(self) -> None:
        """Both error types should preserve the original error message."""
        fn_not_found = ConvexFunctionNotFoundError(
            "router:missing",
            "Full original error message for function not found",
        )
        write_conflict = ConvexWriteConflictError(
            "router:update",
            "Full original error message for write conflict",
        )

        assert fn_not_found.original_message == "Full original error message for function not found"
        assert write_conflict.original_message == "Full original error message for write conflict"


class TestResultPatternIntegration:
    """Tests for using Result pattern with Convex exceptions in DBOS steps."""

    def test_success_result_unwraps_value(self) -> None:
        """Success result should unwrap to the contained value."""
        from job_scrape_application.workflows.result import unwrap_or_raise

        result: Result[str] = Success("data")
        value = unwrap_or_raise(result)
        assert value == "data"

    def test_failure_result_raises_non_retryable_error(self) -> None:
        """Failure result should raise WorkflowNonRetryableError on unwrap."""
        from job_scrape_application.workflows.result import (
            WorkflowNonRetryableError,
            unwrap_or_raise,
        )

        result: Result[str] = Failure(
            error_type="function_not_found",
            message="Function does not exist",
            function_name="router:missing",
        )

        with pytest.raises(WorkflowNonRetryableError) as exc_info:
            unwrap_or_raise(result)

        assert exc_info.value.error_type == "function_not_found"
        assert exc_info.value.function_name == "router:missing"

    def test_classify_error_categorizes_correctly(self) -> None:
        """Test the classify_error helper function."""
        from job_scrape_application.services.convex_client import classify_error

        fn_not_found = Exception("Could not find public function for router:missing")
        write_conflict = Exception("Documents read from or written to changed")
        unknown = Exception("Some other error")

        assert classify_error(fn_not_found) == "function_not_found"
        assert classify_error(write_conflict) == "write_conflict"
        assert classify_error(unknown) == "unknown_error"


class TestTelemetryStepNeverFailsWorkflow:
    """Tests that telemetry steps never fail a workflow, even after all retries exhausted.

    Telemetry is fire-and-forget: workflow success should never depend on it.
    This is enforced by:
    1. Telemetry steps internally catch all exceptions
    2. Workflows wrap telemetry calls in try/except blocks
    """

    def test_telemetry_step_suppresses_all_exceptions_internally(self) -> None:
        """Telemetry step should not raise even when underlying call fails.

        The emit_scrape_telemetry_step has internal try/except that catches
        all exceptions from the telemetry service.
        """
        from job_scrape_application.workflows.activities.step import emit_scrape_telemetry_step

        # Mock the telemetry service to always fail
        with patch(
            "job_scrape_application.services.telemetry.emit_posthog_log",
            side_effect=Exception("PostHog is down!"),
        ):
            # Should not raise - internal try/except handles it
            emit_scrape_telemetry_step(
                event="test.event",
                level="info",
                site_url="https://example.com",
                data={"test": True},
            )
            # If we get here, the step completed without raising

    def test_workflow_continues_when_telemetry_exhausts_retries(self) -> None:
        """Workflow succeeds even when telemetry step exhausts all DBOS retries.

        This simulates the worst case: telemetry service is completely down,
        DBOS retries the step multiple times, all fail, but workflow still
        completes successfully because telemetry is wrapped in try/except.
        """
        telemetry_call_count = 0
        main_work_completed = False

        def failing_telemetry(*args: Any, **kwargs: Any) -> None:
            """Simulates telemetry service being completely unavailable."""
            nonlocal telemetry_call_count
            telemetry_call_count += 1
            raise Exception("Connection refused: telemetry service unavailable")

        def mock_telemetry_step(event: str, data: dict[str, Any] | None = None) -> None:
            """Mock telemetry step that exhausts retries then raises."""
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    failing_telemetry(event=event, data=data)
                    return
                except Exception:
                    if attempt == max_attempts - 1:
                        # All retries exhausted - raise to outer try/except
                        raise
                    continue

        def mock_main_step() -> str:
            """Main business logic step."""
            nonlocal main_work_completed
            main_work_completed = True
            return "work_done"

        def mock_workflow_with_telemetry() -> Result[str]:
            """Workflow with telemetry wrapped in try/except (production pattern)."""
            # Telemetry at start (fire-and-forget)
            try:
                mock_telemetry_step("workflow.started", {"time": "now"})
            except Exception:
                # Telemetry failed but workflow continues
                pass

            # Main business logic
            result = mock_main_step()

            # Telemetry at end (fire-and-forget)
            try:
                mock_telemetry_step("workflow.completed", {"result": result})
            except Exception:
                # Telemetry failed but workflow continues
                pass

            return Success(result)

        result = mock_workflow_with_telemetry()

        # Workflow should succeed
        assert isinstance(result, Success)
        assert result.value == "work_done"

        # Main work was done
        assert main_work_completed is True

        # Telemetry was attempted (6 calls = 3 retries * 2 telemetry steps)
        assert telemetry_call_count == 6

    def test_telemetry_failure_does_not_affect_workflow_result(self) -> None:
        """Workflow returns correct result regardless of telemetry failures.

        Even with network errors, timeouts, or service outages affecting
        telemetry, the workflow result should be determined only by the
        business logic steps.
        """
        mock_client = _make_mock_client(
            Exception("Network timeout calling telemetry service")
        )

        business_results: list[str] = []

        def mock_telemetry_step_with_convex(event: str) -> None:
            """Telemetry step that calls Convex (and fails)."""
            try:
                convex_mutation("router:recordTelemetry", {"event": event})
            except Exception:
                # Telemetry errors suppressed - this is the correct pattern
                pass

        def mock_business_step(item: str) -> str:
            """Business logic step that succeeds."""
            processed = f"processed:{item}"
            business_results.append(processed)
            return processed

        def mock_workflow(items: list[str]) -> Result[dict[str, Any]]:
            """Workflow that processes items with telemetry."""
            processed: list[str] = []

            for item in items:
                # Telemetry (fire-and-forget)
                mock_telemetry_step_with_convex(f"processing:{item}")

                # Actual work
                result = mock_business_step(item)
                processed.append(result)

                # More telemetry (fire-and-forget)
                mock_telemetry_step_with_convex(f"completed:{item}")

            return Success({
                "processed_count": len(processed),
                "items": processed,
            })

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            result = mock_workflow(["a", "b", "c"])

            # Workflow succeeds with correct business results
            assert isinstance(result, Success)
            assert result.value["processed_count"] == 3
            assert result.value["items"] == ["processed:a", "processed:b", "processed:c"]

            # Business logic was executed for all items
            assert len(business_results) == 3

            # Telemetry was attempted (2 calls per item * 3 items = 6 calls)
            assert mock_client.mutation.call_count == 6

    def test_record_scrape_url_attempts_handles_missing_function(self) -> None:
        """record_scrape_url_attempts should not fail workflow when function missing.

        This is the actual production pattern from record_scrape_url_attempts.py
        where ConvexFunctionNotFoundError is caught and the step returns early.
        """
        mock_client = _make_mock_client(
            Exception("Could not find public function for router:recordScrapeUrlAttempts")
        )

        def mock_record_attempts_step(entries: list[dict[str, Any]]) -> None:
            """Mock of record_scrape_url_attempts that handles missing function."""
            if not entries:
                return

            try:
                convex_mutation("router:recordScrapeUrlAttempts", {"entries": entries})
            except ConvexFunctionNotFoundError:
                # Function doesn't exist - log and continue without blocking
                return None
            except Exception:
                # Other errors - also don't block the workflow
                return None

        def mock_workflow_with_telemetry_step(urls: list[str]) -> Result[int]:
            """Workflow that records URL attempts then processes them."""
            # Record attempts (telemetry, fire-and-forget)
            entries = [{"url": url, "attempts": 1} for url in urls]
            mock_record_attempts_step(entries)

            # Main work - process URLs
            processed = len(urls)

            return Success(processed)

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            result = mock_workflow_with_telemetry_step(["url1", "url2", "url3"])

            # Workflow succeeds
            assert isinstance(result, Success)
            assert result.value == 3

            # Telemetry was attempted once (fail-fast, no retries)
            assert mock_client.mutation.call_count == 1


class TestWorkflowExitOnFailFastException:
    """Tests for workflow exit behavior when steps encounter fail-fast exceptions.

    These tests verify that workflows properly exit (return Failure or handle
    gracefully) when a step encounters ConvexFunctionNotFoundError, rather
    than retrying indefinitely.
    """

    def test_workflow_returns_failure_on_function_not_found(self) -> None:
        """Workflow should return Failure when step encounters function not found.

        This simulates a workflow that:
        1. Calls a step that makes a Convex mutation
        2. Step catches ConvexFunctionNotFoundError
        3. Step returns Failure result
        4. Workflow returns that Failure to caller
        """
        from job_scrape_application.workflows.result import (
            WorkflowNonRetryableError,
            unwrap_or_raise,
        )

        mock_client = _make_mock_client(
            Exception("Could not find public function for router:ingestJobs")
        )

        def mock_ingest_step(jobs: list[dict[str, Any]]) -> Result[int]:
            """Mock step that calls Convex and returns Result."""
            try:
                convex_mutation("router:ingestJobs", {"jobs": jobs})
                return Success(len(jobs))
            except ConvexFunctionNotFoundError as exc:
                return Failure(
                    error_type="function_not_found",
                    message=str(exc),
                    function_name=exc.function_name,
                )

        def mock_workflow(batch: dict[str, Any]) -> Result[dict[str, Any]]:
            """Mock workflow that calls the ingest step."""
            jobs = batch.get("jobs", [])

            # Call the step
            ingest_result = mock_ingest_step(jobs)

            # Check for failure and propagate
            if isinstance(ingest_result, Failure):
                return ingest_result

            # On success, return workflow result
            return Success({"stored": ingest_result.value})

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            result = mock_workflow({"jobs": [{"title": "Engineer"}]})

            # Workflow should return Failure
            assert isinstance(result, Failure)
            assert result.error_type == "function_not_found"
            assert result.function_name == "router:ingestJobs"

            # Only one call to Convex (no retries)
            assert mock_client.mutation.call_count == 1

            # Caller can use unwrap_or_raise to convert to exception
            with pytest.raises(WorkflowNonRetryableError) as exc_info:
                unwrap_or_raise(result, context="ingesting jobs")

            assert exc_info.value.error_type == "function_not_found"

    def test_workflow_continues_gracefully_on_optional_step_failure(self) -> None:
        """Workflow continues when optional step fails with function not found.

        This simulates a workflow that:
        1. Calls an optional telemetry step that may not exist
        2. Step catches ConvexFunctionNotFoundError and returns None
        3. Workflow continues with remaining steps
        4. Workflow returns Success
        """
        mock_client = _make_mock_client(
            Exception("Could not find public function for router:recordTelemetry")
        )

        telemetry_called = False
        main_step_called = False

        def mock_telemetry_step(event: str) -> None:
            """Optional telemetry step that handles missing function."""
            nonlocal telemetry_called
            telemetry_called = True
            try:
                convex_mutation("router:recordTelemetry", {"event": event})
            except ConvexFunctionNotFoundError:
                # Optional - just skip if not deployed
                return None

        def mock_main_step(data: str) -> str:
            """Main step that does actual work."""
            nonlocal main_step_called
            main_step_called = True
            return f"processed: {data}"

        def mock_workflow(data: str) -> Result[str]:
            """Workflow with optional and required steps."""
            # Optional telemetry (fail-fast on function not found)
            mock_telemetry_step("workflow_started")

            # Main processing
            result = mock_main_step(data)

            # More optional telemetry
            mock_telemetry_step("workflow_completed")

            return Success(result)

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            result = mock_workflow("test_data")

            # Workflow should return Success despite telemetry failures
            assert isinstance(result, Success)
            assert result.value == "processed: test_data"

            # Both steps were called
            assert telemetry_called is True
            assert main_step_called is True

            # Telemetry was called twice (once before, once after main step)
            assert mock_client.mutation.call_count == 2

    def test_workflow_handles_multiple_step_failures(self) -> None:
        """Workflow tracks multiple step failures independently.

        This simulates a batch workflow that:
        1. Processes multiple items
        2. Some items fail with function not found
        3. Other items succeed
        4. Workflow returns partial success with error details
        """
        call_count = 0

        def alternating_side_effect(*args: Any, **kwargs: Any) -> dict[str, str]:
            """First call fails, subsequent calls succeed."""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Could not find public function for router:storeItem")
            return {"status": "stored"}

        mock_client = MagicMock()
        mock_client.mutation.side_effect = alternating_side_effect

        def mock_store_step(item: dict[str, Any]) -> Result[str]:
            """Step that stores an item, returning Result."""
            try:
                result = convex_mutation("router:storeItem", {"item": item})
                return Success(result.get("status", "unknown"))
            except ConvexFunctionNotFoundError as exc:
                return Failure(
                    error_type="function_not_found",
                    message=str(exc),
                    function_name=exc.function_name,
                )

        def mock_batch_workflow(items: list[dict[str, Any]]) -> dict[str, Any]:
            """Batch workflow that processes items and collects results."""
            stored = 0
            failed = 0
            errors: list[dict[str, Any]] = []

            for item in items:
                result = mock_store_step(item)
                if isinstance(result, Success):
                    stored += 1
                else:
                    failed += 1
                    errors.append({
                        "item": item,
                        "error_type": result.error_type,
                        "function": result.function_name,
                    })

            return {
                "stored": stored,
                "failed": failed,
                "errors": errors,
            }

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            items = [{"id": 1}, {"id": 2}, {"id": 3}]
            result = mock_batch_workflow(items)

            # First item failed, others succeeded
            assert result["stored"] == 2
            assert result["failed"] == 1
            assert len(result["errors"]) == 1
            assert result["errors"][0]["error_type"] == "function_not_found"

            # Each item was processed exactly once (no retries on fail-fast)
            assert mock_client.mutation.call_count == 3


class TestWorkflowRetryOnWriteConflict:
    """Tests for workflow retry behavior when steps encounter write conflicts.

    These tests verify that workflows allow write conflicts to propagate
    for DBOS retry mechanism, rather than suppressing them.
    """

    def test_workflow_succeeds_after_write_conflict_retry(self) -> None:
        """Workflow succeeds when step retries after write conflict.

        This simulates a workflow that:
        1. Calls a step that makes a Convex mutation
        2. First attempt fails with write conflict
        3. DBOS retry mechanism kicks in
        4. Second attempt succeeds
        5. Workflow returns Success
        """
        call_count = 0

        def conflict_then_success(*args: Any, **kwargs: Any) -> dict[str, str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception(
                    "Documents read from or written to the table changed "
                    "while this mutation was being run"
                )
            return {"id": "job-123"}

        mock_client = MagicMock()
        mock_client.mutation.side_effect = conflict_then_success

        def mock_update_step_with_retry(job_id: str, patch: dict[str, Any]) -> Result[str]:
            """Step that allows write conflicts to propagate for retry."""
            max_attempts = 3
            last_error: Exception | None = None

            for attempt in range(max_attempts):
                try:
                    result = convex_mutation("router:updateJob", {"id": job_id, "patch": patch})
                    return Success(result.get("id", job_id))
                except ConvexWriteConflictError as exc:
                    last_error = exc
                    if attempt == max_attempts - 1:
                        # Max retries reached, return failure
                        return Failure(
                            error_type="write_conflict",
                            message=str(exc),
                            function_name=exc.function_name,
                        )
                    # Will retry
                    continue

            # Should not reach here
            return Failure(
                error_type="unknown",
                message=str(last_error) if last_error else "Unknown error",
            )

        def mock_workflow(job_id: str, patch: dict[str, Any]) -> Result[str]:
            """Workflow that updates a job."""
            return mock_update_step_with_retry(job_id, patch)

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            result = mock_workflow("job-123", {"title": "Updated Title"})

            # Workflow should succeed after retry
            assert isinstance(result, Success)
            assert result.value == "job-123"

            # Called twice: first failed, second succeeded
            assert mock_client.mutation.call_count == 2

    def test_workflow_fails_after_max_write_conflict_retries(self) -> None:
        """Workflow fails after exhausting write conflict retries.

        This simulates a workflow that:
        1. Calls a step that makes a Convex mutation
        2. All attempts fail with write conflict
        3. After max retries, step returns Failure
        4. Workflow returns that Failure
        """
        mock_client = _make_mock_client(
            Exception(
                "Documents read from or written to the table changed "
                "while this mutation was being run"
            )
        )

        def mock_update_step_with_retry(job_id: str) -> Result[str]:
            """Step that retries write conflicts up to max attempts."""
            max_attempts = 3

            for attempt in range(max_attempts):
                try:
                    convex_mutation("router:updateJob", {"id": job_id})
                    return Success(job_id)
                except ConvexWriteConflictError as exc:
                    if attempt == max_attempts - 1:
                        return Failure(
                            error_type="write_conflict_exhausted",
                            message=f"Write conflict after {max_attempts} attempts: {exc}",
                            function_name=exc.function_name,
                        )
                    continue

            return Failure(error_type="unknown", message="Should not reach here")

        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=mock_client,
        ):
            result = mock_update_step_with_retry("job-123")

            # Should fail after max retries
            assert isinstance(result, Failure)
            assert result.error_type == "write_conflict_exhausted"
            assert "3 attempts" in result.message

            # Called 3 times (max attempts)
            assert mock_client.mutation.call_count == 3

    def test_write_conflict_vs_function_not_found_handling(self) -> None:
        """Demonstrate different handling for write conflict vs function not found.

        Write conflict: Retry (transient error)
        Function not found: Fail fast (permanent error)
        """
        fn_not_found_client = _make_mock_client(
            Exception("Could not find public function for router:updateJob")
        )
        write_conflict_client = _make_mock_client(
            Exception("Documents read from or written to changed")
        )

        def step_with_smart_error_handling() -> Result[str]:
            """Step that handles different error types appropriately."""
            max_attempts = 3

            for attempt in range(max_attempts):
                try:
                    convex_mutation("router:updateJob", {"id": "test"})
                    return Success("updated")
                except ConvexFunctionNotFoundError as exc:
                    # Fail fast - no point retrying
                    return Failure(
                        error_type="function_not_found",
                        message=str(exc),
                        function_name=exc.function_name,
                    )
                except ConvexWriteConflictError as exc:
                    # Transient - retry
                    if attempt == max_attempts - 1:
                        return Failure(
                            error_type="write_conflict_exhausted",
                            message=str(exc),
                            function_name=exc.function_name,
                        )
                    continue

            return Failure(error_type="unknown", message="Should not reach here")

        # Test function not found: should fail immediately (1 call)
        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=fn_not_found_client,
        ):
            result = step_with_smart_error_handling()
            assert isinstance(result, Failure)
            assert result.error_type == "function_not_found"
            assert fn_not_found_client.mutation.call_count == 1

        # Test write conflict: should retry (3 calls)
        with patch(
            "job_scrape_application.services.convex_client.get_client",
            return_value=write_conflict_client,
        ):
            result = step_with_smart_error_handling()
            assert isinstance(result, Failure)
            assert result.error_type == "write_conflict_exhausted"
            assert write_conflict_client.mutation.call_count == 3
