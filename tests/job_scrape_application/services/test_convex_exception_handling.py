"""Test Convex exception handling: fail-fast vs retry behavior."""

from __future__ import annotations

import asyncio
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


@pytest.mark.asyncio
async def test_argument_validation_error_fails_fast():
    """Test that ArgumentValidationError is raised immediately without retries."""
    mock_client = MagicMock()
    mock_client.query.side_effect = Exception("ArgumentValidationError: Invalid value for field .id")
    
    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        with pytest.raises(ArgumentValidationError) as exc_info:
            await convex_query("jobs:getJobById", {"id": "invalid"})
        
        assert "ArgumentValidationError" in str(exc_info.value)
        assert "jobs:getJobById" in str(exc_info.value)
        # Should only be called once (no retries)
        assert mock_client.query.call_count == 1


@pytest.mark.asyncio
async def test_function_not_found_error_fails_fast():
    """Test that ConvexFunctionNotFoundError is raised immediately without retries."""
    mock_client = MagicMock()
    mock_client.mutation.side_effect = Exception("Could not find public function for router:nonexistentFunction")
    
    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        with pytest.raises(ConvexFunctionNotFoundError) as exc_info:
            await convex_mutation("router:nonexistentFunction", {})
        
        assert "router:nonexistentFunction" in str(exc_info.value)
        # Should only be called once (no retries)
        assert mock_client.mutation.call_count == 1


@pytest.mark.asyncio
async def test_unknown_convex_exception_fails_fast():
    """Test that UnknownConvexException is raised for unrecognized Convex errors."""
    mock_client = MagicMock()
    mock_client.query.side_effect = Exception("UncategorizedError: Something went wrong in Convex")
    
    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        with pytest.raises(UnknownConvexException) as exc_info:
            await convex_query("jobs:getJobById", {"id": "test"})
        
        assert "Unknown Convex error" in str(exc_info.value)
        # Should only be called once (no retries)
        assert mock_client.query.call_count == 1


@pytest.mark.asyncio
async def test_timeout_error_retries():
    """Test that timeout errors are retried according to settings."""
    mock_client = MagicMock()
    call_count = 0
    
    def timeout_then_succeed(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise asyncio.TimeoutError("Request timed out")
        return {"result": "success"}
    
    mock_client.query.side_effect = timeout_then_succeed
    
    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        with patch("job_scrape_application.services.convex_client._RETRY_ON_TIMEOUT", True):
            result = await convex_query("jobs:getJobById", {"id": "test"})
            assert result == {"result": "success"}
            # Should have retried after timeout
            assert call_count == 2


@pytest.mark.asyncio
async def test_generic_error_retries():
    """Test that generic (non-Convex) errors are retried."""
    mock_client = MagicMock()
    call_count = 0
    
    def fail_then_succeed(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise RuntimeError("Network error")
        return {"result": "success"}
    
    mock_client.mutation.side_effect = fail_then_succeed
    
    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        result = await convex_mutation("jobs:createJob", {"url": "test"})
        assert result == {"result": "success"}
        # Should have retried after generic error
        assert call_count == 2


@pytest.mark.asyncio
async def test_argument_validation_error_with_id_field():
    """Test that ArgumentValidationError with .id field is properly categorized."""
    mock_client = MagicMock()
    mock_client.mutation.side_effect = Exception(
        "ArgumentValidationError: Validator error at .id: Expected string, got undefined"
    )
    
    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        with pytest.raises(ArgumentValidationError) as exc_info:
            await convex_mutation("router:completeSite", {"id": None})
        
        assert ".id" in exc_info.value.original_message
        assert "router:completeSite" == exc_info.value.function_name
        # Should only be called once (no retries)
        assert mock_client.mutation.call_count == 1


@pytest.mark.asyncio
async def test_convex_error_with_lowercase_convex_keyword():
    """Test that errors containing 'convex' keyword are treated as unknown Convex exceptions."""
    mock_client = MagicMock()
    mock_client.query.side_effect = Exception("convex internal error: rate limit exceeded")
    
    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        with pytest.raises(UnknownConvexException) as exc_info:
            await convex_query("jobs:getJobById", {"id": "test"})
        
        assert "Unknown Convex error" in str(exc_info.value)
        assert "rate limit exceeded" in exc_info.value.original_message


@pytest.mark.asyncio
async def test_slow_warning_does_not_fail_fast():
    """Test that warnings about slow operations are logged but retried."""
    mock_client = MagicMock()
    call_count = 0

    def slow_warning_then_succeed(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise RuntimeError("Warning: Query is slow")
        return {"result": "success"}

    mock_client.query.side_effect = slow_warning_then_succeed

    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        result = await convex_query("jobs:getJobById", {"id": "test"})
        assert result == {"result": "success"}
        # Should have retried after slow warning
        assert call_count == 2


@pytest.mark.asyncio
async def test_write_conflict_error_fails_fast():
    """Test that write conflict (OCC) errors fail fast."""
    mock_client = MagicMock()
    mock_client.mutation.side_effect = Exception(
        "Documents read from or written to the table changed while this mutation was being run"
    )

    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        with pytest.raises(ConvexWriteConflictError) as exc_info:
            await convex_mutation("jobs:updateJob", {"id": "test"})

        assert "write conflict" in str(exc_info.value).lower()
        assert mock_client.mutation.call_count == 1


@pytest.mark.asyncio
async def test_read_write_limit_error_fails_fast():
    """Test that read/write limit errors fail fast."""
    mock_client = MagicMock()
    mock_client.query.side_effect = Exception(
        "Query read limit exceeded: tried to read 50000 documents"
    )

    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        with pytest.raises(ConvexReadWriteLimitError) as exc_info:
            await convex_query("jobs:getAllJobs", {})

        assert "limit" in str(exc_info.value).lower()
        assert mock_client.query.call_count == 1


@pytest.mark.asyncio
async def test_internal_server_error_fails_fast():
    """Test that internal server errors fail fast."""
    mock_client = MagicMock()
    mock_client.mutation.side_effect = Exception("InternalServerError: Database unavailable")

    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        with pytest.raises(ConvexInternalServerError) as exc_info:
            await convex_mutation("jobs:createJob", {"url": "test"})

        assert "internal server error" in str(exc_info.value).lower()
        assert mock_client.mutation.call_count == 1


@pytest.mark.asyncio
async def test_application_error_fails_fast():
    """Test that ConvexError (application error) fails fast."""
    mock_client = MagicMock()
    mock_client.mutation.side_effect = Exception(
        'ConvexError: {"code": "DUPLICATE_JOB", "message": "Job already exists"}'
    )

    with patch("job_scrape_application.services.convex_client.get_client", return_value=mock_client):
        with pytest.raises(ConvexApplicationError) as exc_info:
            await convex_mutation("jobs:createJob", {"url": "test"})

        assert "application error" in str(exc_info.value).lower()
        assert "DUPLICATE_JOB" in exc_info.value.original_message
        assert mock_client.mutation.call_count == 1


@pytest.mark.asyncio
async def test_convex_unknown_error_alias():
    """Test that ConvexUnknownError and UnknownConvexException are aliases."""
    # Verify they are the same class (backwards compatibility)
    assert ConvexUnknownError is UnknownConvexException
