"""Result type for DBOS steps that need to handle non-retryable errors.

This pattern allows steps to return failures instead of throwing, which:
- Completes the step (DBOS records it, no retry)
- Gives the caller control over error handling
- Works correctly with workflow recovery/replay
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Generic, NoReturn, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Success(Generic[T]):
    """Successful result with a value."""

    value: T


@dataclass(frozen=True, slots=True)
class Failure:
    """Failed result with error info.

    Attributes:
        error_type: Category of error (e.g., "validation_error", "not_found")
        message: Human-readable error message
        function_name: The Convex function that failed (if applicable)
    """

    error_type: str
    message: str
    function_name: str | None = None


# Result is either Success or Failure
Result = Success[T] | Failure


def unwrap_or_raise(result: Result[T], context: str = "") -> T:
    """Unwrap a Result, raising WorkflowNonRetryableError on Failure.

    Use this in workflows to handle failures with a generic error handler.

    Args:
        result: The Result to unwrap
        context: Optional context string for logging (e.g., "filtering job URLs")

    Returns:
        The success value

    Raises:
        WorkflowNonRetryableError: If result is a Failure
    """
    match result:
        case Success(value=value):
            return value
        case Failure(error_type=error_type, message=message, function_name=fn):
            ctx = f" while {context}" if context else ""
            fn_info = f" (function: {fn})" if fn else ""
            logger.error(
                "Non-retryable error%s: [%s]%s %s",
                ctx,
                error_type,
                fn_info,
                message,
            )
            raise WorkflowNonRetryableError(error_type, message, function_name=fn)


class WorkflowNonRetryableError(Exception):
    """Raised when a workflow encounters a non-retryable error.

    This exception signals that the workflow should end without retry.
    Workflows can catch this at the top level to log and exit gracefully.
    """

    def __init__(
        self,
        error_type: str,
        message: str,
        function_name: str | None = None,
    ):
        self.error_type = error_type
        self.function_name = function_name
        super().__init__(f"[{error_type}] {message}")


def handle_workflow_failure(exc: WorkflowNonRetryableError, workflow_name: str) -> NoReturn:
    """Generic handler for non-retryable workflow failures.

    Logs the error and re-raises so the workflow ends with ERROR status.

    Args:
        exc: The WorkflowNonRetryableError
        workflow_name: Name of the workflow for logging

    Raises:
        WorkflowNonRetryableError: Always re-raises after logging
    """
    logger.error(
        "Workflow '%s' failed with non-retryable error [%s]: %s",
        workflow_name,
        exc.error_type,
        str(exc),
    )
    raise exc
