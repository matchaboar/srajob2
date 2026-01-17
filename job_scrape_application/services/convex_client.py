from __future__ import annotations

from typing import Any, Mapping

from convex import ConvexClient

from ..config import settings
from . import telemetry

_client: ConvexClient | None = None
_DESCRIPTION_PREVIEW_WORD_LIMIT_ERROR = "description_preview_word_limit_exceeded"

# =============================================================================
# Convex Exception Classes
# Based on documented error types from Convex:
# - https://docs.convex.dev/functions/error-handling/
# - https://docs.convex.dev/error
# - https://docs.convex.dev/functions/error-handling/application-errors
# =============================================================================


class ConvexFunctionNotFoundError(Exception):
    """Raised when a Convex function doesn't exist.

    Fail-fast: Yes - no point retrying a missing function.
    Docs: Function path doesn't match any public function.
    """

    def __init__(self, function_name: str, original_message: str):
        self.function_name = function_name
        self.original_message = original_message
        super().__init__(f"Convex function not found: {function_name}")


class ConvexArgumentValidationError(Exception):
    """Raised when Convex rejects arguments due to schema validation.

    Fail-fast: Yes - arguments don't match the expected validator schema.
    Docs: https://docs.convex.dev/functions/validation
    Pattern: "ArgumentValidationError" in error message.
    """

    def __init__(self, function_name: str, original_message: str):
        self.function_name = function_name
        self.original_message = original_message
        super().__init__(f"Convex argument validation failed for {function_name}: {original_message}")


class ConvexWriteConflictError(Exception):
    """Raised when a mutation fails due to optimistic concurrency control (OCC).

    Fail-fast: No - indicates concurrent mutations modifying the same documents.
    Docs: https://docs.convex.dev/error (Write Conflict section)
    Pattern: "Documents read from or written to" or "write conflict" in error message.
    """

    def __init__(self, function_name: str, original_message: str):
        self.function_name = function_name
        self.original_message = original_message
        super().__init__(f"Convex write conflict for {function_name}: {original_message}")


class ConvexReadWriteLimitError(Exception):
    """Raised when a function exceeds Convex read/write data limits.

    Fail-fast: Yes - function is trying to read/write too much data.
    Docs: https://docs.convex.dev/production/state/limits
    Pattern: "limit" combined with "read" or "write" in error message.
    """

    def __init__(self, function_name: str, original_message: str):
        self.function_name = function_name
        self.original_message = original_message
        super().__init__(f"Convex read/write limit exceeded for {function_name}: {original_message}")


class ConvexInternalServerError(Exception):
    """Raised when Convex encounters an internal server error.

    Fail-fast: Yes - infrastructure issue that won't resolve with retries.
    Docs: https://docs.convex.dev/functions/error-handling/
    Pattern: "InternalServerError" or "Server Error" in error message.
    """

    def __init__(self, function_name: str, original_message: str):
        self.function_name = function_name
        self.original_message = original_message
        super().__init__(f"Convex internal server error for {function_name}: {original_message}")


class ConvexApplicationError(Exception):
    """Raised when user code throws a ConvexError (application-level error).

    Fail-fast: Yes - intentional error thrown by application logic.
    Docs: https://docs.convex.dev/functions/error-handling/application-errors
    Pattern: "ConvexError" in error message.
    """

    def __init__(self, function_name: str, original_message: str):
        self.function_name = function_name
        self.original_message = original_message
        super().__init__(f"Convex application error for {function_name}: {original_message}")


class ConvexUnknownError(Exception):
    """Raised when Convex returns an unrecognized error type.

    Fail-fast: Yes - unknown errors should surface immediately for investigation.
    """

    def __init__(self, function_name: str, original_message: str):
        self.function_name = function_name
        self.original_message = original_message
        super().__init__(f"Unknown Convex error for {function_name}: {original_message}")


# Backwards compatibility aliases
ArgumentValidationError = ConvexArgumentValidationError
UnknownConvexException = ConvexUnknownError


# =============================================================================
# Error Detection Functions
# =============================================================================


def _is_function_not_found_error(exc: Exception) -> bool:
    """Check if exception indicates a missing Convex function."""
    return "Could not find public function for" in str(exc)


def _is_argument_validation_error(exc: Exception) -> bool:
    """Check if exception indicates argument validation failure."""
    return "ArgumentValidationError" in str(exc)


def _is_write_conflict_error(exc: Exception) -> bool:
    """Check if exception indicates an OCC write conflict."""
    exc_str = str(exc).lower()
    return "documents read from or written to" in exc_str or "write conflict" in exc_str


def _is_read_write_limit_error(exc: Exception) -> bool:
    """Check if exception indicates read/write limit exceeded."""
    exc_str = str(exc).lower()
    return "limit" in exc_str and ("read" in exc_str or "write" in exc_str)


def _is_internal_server_error(exc: Exception) -> bool:
    """Check if exception indicates a Convex internal server error."""
    exc_str = str(exc)
    return "InternalServerError" in exc_str or "Server Error" in exc_str


def _is_application_error(exc: Exception) -> bool:
    """Check if exception is a ConvexError thrown by user code."""
    exc_str = str(exc)
    # Match ConvexError but not other error types that happen to contain "Convex"
    return "ConvexError" in exc_str and "ArgumentValidationError" not in exc_str




def classify_error(exc: Exception) -> str:
    """Classify a Convex error for Result pattern.

    Returns:
        error_type: Category string for the error
    """
    if _is_function_not_found_error(exc):
        return "function_not_found"
    if _is_argument_validation_error(exc):
        return "validation_error"
    if _is_write_conflict_error(exc):
        return "write_conflict"
    if _is_read_write_limit_error(exc):
        return "read_write_limit"
    if _is_internal_server_error(exc):
        return "internal_server_error"
    if _is_application_error(exc):
        return "application_error"
    return "unknown_error"


def _max_description_word_count(args: Mapping[str, Any] | None) -> int | None:
    if not isinstance(args, Mapping):
        return None

    jobs = args.get("jobs")
    if isinstance(jobs, list):
        max_count = 0
        for job in jobs:
            if not isinstance(job, Mapping):
                continue
            description = job.get("description")
            if isinstance(description, str):
                count = len(description.split())
                max_count = max(max_count, count)
        return max_count or None

    description = args.get("description")
    if isinstance(description, str):
        return len(description.split())

    return None


def _wrap_convex_error(exc: Exception, name: str) -> Exception:
    """Wrap a Convex exception in the appropriate typed exception class."""
    exc_str = str(exc)

    if _is_function_not_found_error(exc):
        return ConvexFunctionNotFoundError(name, exc_str)
    if _is_argument_validation_error(exc):
        return ConvexArgumentValidationError(name, exc_str)
    if _is_write_conflict_error(exc):
        return ConvexWriteConflictError(name, exc_str)
    if _is_read_write_limit_error(exc):
        return ConvexReadWriteLimitError(name, exc_str)
    if _is_internal_server_error(exc):
        return ConvexInternalServerError(name, exc_str)
    if _is_application_error(exc):
        return ConvexApplicationError(name, exc_str)
    return ConvexUnknownError(name, exc_str)


def _normalize_deployment_url() -> str:
    """
    Prefer CONVEX_URL (Convex deployment URL).
    Fallback to CONVEX_HTTP_URL by converting .convex.site -> .convex.cloud.
    """

    if settings.convex_url:
        return settings.convex_url

    if settings.convex_http_url:
        url = settings.convex_http_url.rstrip("/")
        if ".convex.site" in url:
            url = url.replace(".convex.site", ".convex.cloud")
        return url

    raise RuntimeError("CONVEX_URL env var is required for Convex client")


def get_client() -> ConvexClient:
    global _client
    if _client is None:
        _client = ConvexClient(_normalize_deployment_url())
    return _client


def convex_query(name: str, args: Mapping[str, Any] | None = None) -> Any:
    """Call a Convex query synchronously."""
    client = get_client()
    try:
        return client.query(name, args)
    except Exception as exc:
        raise _wrap_convex_error(exc, name) from exc


def convex_mutation(name: str, args: Mapping[str, Any] | None = None) -> Any:
    """Call a Convex mutation synchronously."""
    client = get_client()
    try:
        return client.mutation(name, args)
    except Exception as exc:
        try:
            payload: dict[str, Any] = {
                "event": "convex.mutation_failed",
                "name": name,
            }
            if isinstance(args, Mapping):
                payload["argKeys"] = list(args.keys())
            telemetry.emit_posthog_log(payload)
            if _DESCRIPTION_PREVIEW_WORD_LIMIT_ERROR in str(exc):
                violation_payload: dict[str, Any] = {
                    "event": "convex.description_preview_violation",
                    "name": name,
                }
                max_words = _max_description_word_count(args)
                if max_words is not None:
                    violation_payload["maxDescriptionWords"] = max_words
                telemetry.emit_posthog_log(violation_payload)
        except Exception:
            pass
        raise _wrap_convex_error(exc, name) from exc


def convex_action(name: str, args: Mapping[str, Any] | None = None) -> Any:
    """Call a Convex action synchronously."""
    client = get_client()
    try:
        return client.action(name, args)
    except Exception as exc:
        try:
            payload: dict[str, Any] = {
                "event": "convex.action_failed",
                "name": name,
            }
            if isinstance(args, Mapping):
                payload["argKeys"] = list(args.keys())
            telemetry.emit_posthog_log(payload)
        except Exception:
            pass
        raise _wrap_convex_error(exc, name) from exc


# Test helper to inject a mock client
def _set_client_for_tests(client: ConvexClient | None) -> None:
    global _client
    _client = client
