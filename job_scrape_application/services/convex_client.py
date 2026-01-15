from __future__ import annotations

import asyncio
import os
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Mapping

from convex import ConvexClient

from ..config import settings
from . import telemetry

_client: ConvexClient | None = None
_REQUEST_TIMEOUT_SECONDS = float(os.getenv("CONVEX_REQUEST_TIMEOUT_SECONDS", "5.0"))
_TOTAL_BUDGET_SECONDS = float(os.getenv("CONVEX_TOTAL_TIMEOUT_SECONDS", "12.0"))
_MAX_RETRIES = int(os.getenv("CONVEX_MAX_RETRIES", "2"))
_BACKOFF_BASE_SECONDS = 0.5
_BACKOFF_MAX_SECONDS = 4.0
_RETRY_ON_TIMEOUT = os.getenv("CONVEX_RETRY_ON_TIMEOUT", "1") == "1"
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

    Fail-fast: Yes - indicates concurrent mutations modifying the same documents.
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


def _is_timeout_or_warning(exc: Exception) -> bool:
    """Check if exception is a timeout or warning (not fail-fast)."""
    exc_str = str(exc).lower()
    return isinstance(exc, asyncio.TimeoutError) or "timeout" in exc_str or "slow" in exc_str


def _is_retryable_network_error(exc: Exception) -> bool:
    """Check if exception is a transient network error that should be retried."""
    exc_str = str(exc).lower()
    network_patterns = [
        "connection refused",
        "connection reset",
        "connection aborted",
        "network unreachable",
        "network is unreachable",
        "network error",
        "temporary failure",
        "name resolution",
        "dns",
        "socket",
        "eof",
        "broken pipe",
        "ssl",
        "certificate",
    ]
    return any(pattern in exc_str for pattern in network_patterns)


def _is_retryable_error(exc: Exception) -> bool:
    """Check if exception should be retried (timeout, slow, network issues)."""
    return _is_timeout_or_warning(exc) or _is_retryable_network_error(exc)

# Dedicated executor for Convex calls to prevent timeout threads from blocking
# the default executor used by asyncio.to_thread() elsewhere in the application.
# When asyncio.wait_for() times out, the underlying thread continues running -
# using a dedicated pool isolates this behavior from other async operations.
_CONVEX_EXECUTOR_MAX_WORKERS = int(os.getenv("CONVEX_EXECUTOR_MAX_WORKERS", "8"))
_convex_executor: ThreadPoolExecutor | None = None


def _get_convex_executor() -> ThreadPoolExecutor:
    """Get or create the dedicated thread pool executor for Convex calls."""
    global _convex_executor
    if _convex_executor is None:
        _convex_executor = ThreadPoolExecutor(
            max_workers=_CONVEX_EXECUTOR_MAX_WORKERS,
            thread_name_prefix="convex-client-",
        )
    return _convex_executor


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


async def _call_with_retry(fn, name: str, args: Mapping[str, Any] | None) -> Any:
    last_error: Exception | None = None
    loop = asyncio.get_running_loop()
    executor = _get_convex_executor()
    start = loop.time()
    for attempt in range(1, _MAX_RETRIES + 1):
        elapsed = loop.time() - start
        remaining_budget = _TOTAL_BUDGET_SECONDS - elapsed
        if remaining_budget <= 0:
            break
        per_attempt_timeout = min(_REQUEST_TIMEOUT_SECONDS, max(0.0, remaining_budget))
        try:
            # Use dedicated executor to isolate timeout threads from blocking
            # other async operations that use the default executor.
            return await asyncio.wait_for(
                loop.run_in_executor(executor, fn, name, args),
                timeout=per_attempt_timeout,
            )
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            exc_str = str(exc)

            # =================================================================
            # Fail-fast error detection - check specific Convex error types
            # =================================================================

            # Function not found - fail fast
            if _is_function_not_found_error(exc):
                try:
                    telemetry.emit_posthog_log({
                        "event": "convex.function_not_found",
                        "level": "fatal",
                        "functionName": name,
                        "error": exc_str,
                    })
                except Exception:
                    pass
                raise ConvexFunctionNotFoundError(name, exc_str) from exc

            # Argument validation error - fail fast
            if _is_argument_validation_error(exc):
                try:
                    telemetry.emit_posthog_log({
                        "event": "convex.argument_validation_error",
                        "level": "fatal",
                        "functionName": name,
                        "error": exc_str,
                    })
                except Exception:
                    pass
                raise ConvexArgumentValidationError(name, exc_str) from exc

            # Write conflict (OCC) error - fail fast
            if _is_write_conflict_error(exc):
                try:
                    telemetry.emit_posthog_log({
                        "event": "convex.write_conflict",
                        "level": "fatal",
                        "functionName": name,
                        "error": exc_str,
                    })
                except Exception:
                    pass
                raise ConvexWriteConflictError(name, exc_str) from exc

            # Read/write limit exceeded - fail fast
            if _is_read_write_limit_error(exc):
                try:
                    telemetry.emit_posthog_log({
                        "event": "convex.read_write_limit",
                        "level": "fatal",
                        "functionName": name,
                        "error": exc_str,
                    })
                except Exception:
                    pass
                raise ConvexReadWriteLimitError(name, exc_str) from exc

            # Internal server error - fail fast
            if _is_internal_server_error(exc):
                try:
                    telemetry.emit_posthog_log({
                        "event": "convex.internal_server_error",
                        "level": "fatal",
                        "functionName": name,
                        "error": exc_str,
                    })
                except Exception:
                    pass
                raise ConvexInternalServerError(name, exc_str) from exc

            # Application error (ConvexError from user code) - fail fast
            if _is_application_error(exc):
                try:
                    telemetry.emit_posthog_log({
                        "event": "convex.application_error",
                        "level": "fatal",
                        "functionName": name,
                        "error": exc_str,
                    })
                except Exception:
                    pass
                raise ConvexApplicationError(name, exc_str) from exc

            # =================================================================
            # Retryable errors - log at warning level and continue
            # =================================================================

            if _is_retryable_error(exc):
                # Log retryable errors at warning level
                try:
                    event_type = "convex.timeout_or_warning" if _is_timeout_or_warning(exc) else "convex.network_error"
                    telemetry.emit_posthog_log({
                        "event": event_type,
                        "level": "warning",
                        "functionName": name,
                        "error": exc_str,
                        "attempt": attempt,
                    })
                except Exception:
                    pass
                # For timeouts, respect the RETRY_ON_TIMEOUT setting
                if isinstance(exc, asyncio.TimeoutError) and not _RETRY_ON_TIMEOUT:
                    break
                # Continue to retry logic for retryable cases
            else:
                # =============================================================
                # Unknown Convex error - fail fast
                # Any error that isn't recognized should surface immediately
                # =============================================================
                try:
                    telemetry.emit_posthog_log({
                        "event": "convex.unknown_error",
                        "level": "fatal",
                        "functionName": name,
                        "error": exc_str,
                        "errorType": type(exc).__name__,
                    })
                except Exception:
                    pass
                raise ConvexUnknownError(name, exc_str) from exc
            
            if attempt >= _MAX_RETRIES:
                break
            elapsed = loop.time() - start
            remaining_budget = _TOTAL_BUDGET_SECONDS - elapsed
            if remaining_budget <= 0:
                break
            backoff = min(_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), _BACKOFF_MAX_SECONDS)
            jitter = random.uniform(0, 0.25)
            sleep_for = min(backoff + jitter, max(0.0, remaining_budget))
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
    raise last_error if last_error else RuntimeError("Convex call failed")


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


async def convex_query(name: str, args: Mapping[str, Any] | None = None) -> Any:
    client = get_client()
    return await _call_with_retry(client.query, name, args)


async def convex_mutation(name: str, args: Mapping[str, Any] | None = None) -> Any:
    client = get_client()
    try:
        return await _call_with_retry(client.mutation, name, args)
    except Exception as exc:
        try:
            payload = {
                "event": "convex.mutation_failed",
                "name": name,
            }
            if isinstance(args, Mapping):
                payload["argKeys"] = list(args.keys())
            telemetry.emit_posthog_log(payload)
            if _DESCRIPTION_PREVIEW_WORD_LIMIT_ERROR in str(exc):
                violation_payload = {
                    "event": "convex.description_preview_violation",
                    "name": name,
                }
                max_words = _max_description_word_count(args)
                if max_words is not None:
                    violation_payload["maxDescriptionWords"] = max_words
                telemetry.emit_posthog_log(violation_payload)
        except Exception:
            pass
        raise


async def convex_action(name: str, args: Mapping[str, Any] | None = None) -> Any:
    """Call a Convex action (as opposed to mutation or query)."""
    client = get_client()
    try:
        return await _call_with_retry(client.action, name, args)
    except Exception:
        try:
            payload = {
                "event": "convex.action_failed",
                "name": name,
            }
            if isinstance(args, Mapping):
                payload["argKeys"] = list(args.keys())
            telemetry.emit_posthog_log(payload)
        except Exception:
            pass
        raise


# Test helper to inject a mock client
def _set_client_for_tests(client: ConvexClient | None) -> None:
    global _client
    _client = client
