"""Test Convex exception handling: error classification and wrapping."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from job_scrape_application.services.convex_client import (
    ArgumentValidationError,
    ConvexApplicationError,
    ConvexFunctionNotFoundError,
    ConvexInternalServerError,
    ConvexReadWriteLimitError,
    ConvexUnknownError,
    ConvexWriteConflictError,
    UnknownConvexException,
    convex_mutation,
    convex_query,
)

# Group all tests in this module to run in the same worker to avoid
# shared state issues with the module-level client
pytestmark = pytest.mark.xdist_group(name="convex_client")


def _make_mock_client(side_effect):
    """Create a mock ConvexClient that raises the given exception."""
    mock_client = MagicMock()
    mock_client.query.side_effect = side_effect
    mock_client.mutation.side_effect = side_effect
    mock_client.action.side_effect = side_effect
    return mock_client


def test_argument_validation_error_raises():
    """Test that ArgumentValidationError is raised for validation errors."""
    mock_client = _make_mock_client(
        Exception("ArgumentValidationError: Invalid value for field .id")
    )

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        with pytest.raises(ArgumentValidationError) as exc_info:
            convex_query("jobs:getJobById", {"id": "invalid"})

        assert "ArgumentValidationError" in str(exc_info.value)
        assert "jobs:getJobById" in str(exc_info.value)
        assert mock_client.query.call_count == 1


def test_function_not_found_error_raises():
    """Test that ConvexFunctionNotFoundError is raised for missing functions."""
    mock_client = _make_mock_client(
        Exception("Could not find public function for router:nonexistentFunction")
    )

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        with pytest.raises(ConvexFunctionNotFoundError) as exc_info:
            convex_mutation("router:nonexistentFunction", {})

        assert "router:nonexistentFunction" in str(exc_info.value)
        assert mock_client.mutation.call_count == 1


def test_unknown_convex_exception_raises():
    """Test that ConvexUnknownError is raised for unrecognized errors."""
    mock_client = _make_mock_client(
        Exception("UncategorizedError: Something went wrong in Convex")
    )

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        with pytest.raises(UnknownConvexException) as exc_info:
            convex_query("jobs:getJobById", {"id": "test"})

        assert "Unknown Convex error" in str(exc_info.value)
        assert mock_client.query.call_count == 1


def test_successful_query_returns_result():
    """Test that successful queries return the result."""
    mock_client = MagicMock()
    mock_client.query.return_value = {"result": "success"}

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        result = convex_query("jobs:getJobById", {"id": "test"})
        assert result == {"result": "success"}


def test_successful_mutation_returns_result():
    """Test that successful mutations return the result."""
    mock_client = MagicMock()
    mock_client.mutation.return_value = {"result": "success"}

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        result = convex_mutation("jobs:createJob", {"url": "test"})
        assert result == {"result": "success"}


def test_argument_validation_error_with_id_field():
    """Test that ArgumentValidationError with .id field is properly categorized."""
    mock_client = _make_mock_client(
        Exception(
            "ArgumentValidationError: Validator error at .id: Expected string, got undefined"
        )
    )

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        with pytest.raises(ArgumentValidationError) as exc_info:
            convex_mutation("router:completeSite", {"id": None})

        assert ".id" in exc_info.value.original_message
        assert "router:completeSite" == exc_info.value.function_name
        assert mock_client.mutation.call_count == 1


def test_convex_error_with_lowercase_convex_keyword():
    """Test that errors containing 'convex' keyword are treated as unknown Convex exceptions."""
    mock_client = _make_mock_client(
        Exception("convex internal error: rate limit exceeded")
    )

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        with pytest.raises(UnknownConvexException) as exc_info:
            convex_query("jobs:getJobById", {"id": "test"})

        assert "Unknown Convex error" in str(exc_info.value)
        assert "rate limit exceeded" in exc_info.value.original_message


def test_write_conflict_error_raises():
    """Test that write conflict (OCC) errors are properly categorized."""
    mock_client = _make_mock_client(
        Exception(
            "Documents read from or written to the table changed while this mutation was being run"
        )
    )

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        with pytest.raises(ConvexWriteConflictError) as exc_info:
            convex_mutation("jobs:updateJob", {"id": "test"})

        assert "write conflict" in str(exc_info.value).lower()
        assert mock_client.mutation.call_count == 1


def test_read_write_limit_error_raises():
    """Test that read/write limit errors are properly categorized."""
    mock_client = _make_mock_client(
        Exception("Query read limit exceeded: tried to read 50000 documents")
    )

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        with pytest.raises(ConvexReadWriteLimitError) as exc_info:
            convex_query("jobs:getAllJobs", {})

        assert "limit" in str(exc_info.value).lower()
        assert mock_client.query.call_count == 1


def test_internal_server_error_raises():
    """Test that internal server errors are properly categorized."""
    mock_client = _make_mock_client(
        Exception("InternalServerError: Database unavailable")
    )

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        with pytest.raises(ConvexInternalServerError) as exc_info:
            convex_mutation("jobs:createJob", {"url": "test"})

        assert "internal server error" in str(exc_info.value).lower()
        assert mock_client.mutation.call_count == 1


def test_application_error_raises():
    """Test that ConvexError (application error) is properly categorized."""
    mock_client = _make_mock_client(
        Exception('ConvexError: {"code": "DUPLICATE_JOB", "message": "Job already exists"}')
    )

    with patch(
        "job_scrape_application.services.convex_client.get_client",
        return_value=mock_client,
    ):
        with pytest.raises(ConvexApplicationError) as exc_info:
            convex_mutation("jobs:createJob", {"url": "test"})

        assert "application error" in str(exc_info.value).lower()
        assert "DUPLICATE_JOB" in exc_info.value.original_message
        assert mock_client.mutation.call_count == 1


def test_convex_unknown_error_alias():
    """Test that ConvexUnknownError and UnknownConvexException are aliases."""
    # Verify they are the same class (backwards compatibility)
    assert ConvexUnknownError is UnknownConvexException
