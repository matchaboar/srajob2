from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
import re

try:
    from temporalio.exceptions import ActivityError, ApplicationError
except Exception:  # pragma: no cover - temporal not always available in tests
    ActivityError = None  # type: ignore[assignment]
    ApplicationError = None  # type: ignore[assignment]


SPIDERCLOUD_RETRYABLE_STATUS = {429}
SPIDERCLOUD_FATAL_STATUS = {401, 402}
SPIDERCLOUD_NON_RETRYABLE_STATUS = {400, 403, 404, 413}

SPIDERCLOUD_FATAL_TYPES = {"spidercloud_payment_required", "spidercloud_unauthorized"}
SPIDERCLOUD_RETRYABLE_TYPES = {
    "spidercloud_rate_limit",
}
SPIDERCLOUD_NON_RETRYABLE_TYPES = {
    "spidercloud_bad_request",
    "spidercloud_payload_too_large",
    "spidercloud_timeout",
    "spidercloud_service_unavailable",
    "spidercloud_server_error",
}

_RETRY_AFTER_SECONDS = {
    429: 120,
}
_DEFAULT_RETRY_AFTER_SECONDS = 60
_TIMEOUT_RETRY_AFTER_SECONDS = 120
_RETRY_AFTER_BY_TYPE = {
    "spidercloud_rate_limit": 120,
}


@dataclass(frozen=True)
class SpidercloudErrorContext:
    status_code: int | None
    error_type: str | None
    message: str | None
    source: str | None = None


@dataclass(frozen=True)
class SpidercloudErrorDecision:
    action: Literal["retry", "fail", "halt"]
    error: str
    retry_after_seconds: int | None = None
    status_code: int | None = None
    error_type: str | None = None


class SpidercloudErrorStrategy:
    def decide(self, context: SpidercloudErrorContext) -> SpidercloudErrorDecision:
        raise NotImplementedError


@dataclass(frozen=True)
class RetryableSpidercloudErrorStrategy(SpidercloudErrorStrategy):
    error: str
    retry_after_seconds: int
    error_type: str | None = None
    status_code: int | None = None

    def decide(self, context: SpidercloudErrorContext) -> SpidercloudErrorDecision:
        return SpidercloudErrorDecision(
            action="retry",
            error=self.error,
            retry_after_seconds=self.retry_after_seconds,
            status_code=self.status_code,
            error_type=self.error_type,
        )


@dataclass(frozen=True)
class NonRetryableSpidercloudErrorStrategy(SpidercloudErrorStrategy):
    error: str
    error_type: str | None = None
    status_code: int | None = None

    def decide(self, context: SpidercloudErrorContext) -> SpidercloudErrorDecision:
        return SpidercloudErrorDecision(
            action="fail",
            error=self.error,
            status_code=self.status_code,
            error_type=self.error_type,
        )


@dataclass(frozen=True)
class FatalSpidercloudErrorStrategy(SpidercloudErrorStrategy):
    error: str
    error_type: str | None = None
    status_code: int | None = None

    def decide(self, context: SpidercloudErrorContext) -> SpidercloudErrorDecision:
        return SpidercloudErrorDecision(
            action="halt",
            error=self.error,
            status_code=self.status_code,
            error_type=self.error_type,
        )


def _parse_status_code_from_message(message: str | None) -> int | None:
    if not message:
        return None
    match = re.search(r"\b(\d{3})\b", message)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _reason_for_status(status_code: int | None) -> str | None:
    if status_code is None:
        return None
    return f"http_{status_code}"


def _error_type_for_status(status_code: int | None, *, source: str | None) -> str | None:
    if status_code is None:
        return None
    reason = _reason_for_status(status_code)
    if not reason:
        return None
    prefix = source or "spidercloud"
    return f"{prefix}_{reason}"


def _is_spidercloud_rate_limit(
    status_code: int | None,
    *,
    error_type: str | None,
    source: str | None,
) -> bool:
    if error_type and "rate_limit" in error_type:
        return True
    if status_code != 429:
        return False
    if not source:
        return False
    return source.startswith("spidercloud_api")


def decision_for_status_code(
    status_code: int | None,
    *,
    source: str | None = None,
    message: str | None = None,
    error_type: str | None = None,
) -> SpidercloudErrorDecision:
    inferred_error_type = error_type or _error_type_for_status(status_code, source=source)
    if status_code in SPIDERCLOUD_FATAL_STATUS:
        reason = _reason_for_status(status_code) or "fatal_error"
        return FatalSpidercloudErrorStrategy(
            error=reason,
            error_type=inferred_error_type,
            status_code=status_code,
        ).decide(SpidercloudErrorContext(status_code, inferred_error_type, message, source))
    if status_code in SPIDERCLOUD_RETRYABLE_STATUS and _is_spidercloud_rate_limit(
        status_code,
        error_type=inferred_error_type,
        source=source,
    ):
        reason = _reason_for_status(status_code) or "retryable_error"
        retry_after = _RETRY_AFTER_SECONDS.get(status_code, _DEFAULT_RETRY_AFTER_SECONDS)
        return RetryableSpidercloudErrorStrategy(
            error=reason,
            retry_after_seconds=retry_after,
            error_type=inferred_error_type,
            status_code=status_code,
        ).decide(SpidercloudErrorContext(status_code, inferred_error_type, message, source))
    if status_code in SPIDERCLOUD_NON_RETRYABLE_STATUS:
        reason = _reason_for_status(status_code) or "non_retryable_error"
        return NonRetryableSpidercloudErrorStrategy(
            error=reason,
            error_type=inferred_error_type,
            status_code=status_code,
        ).decide(SpidercloudErrorContext(status_code, inferred_error_type, message, source))
    if status_code is not None:
        return NonRetryableSpidercloudErrorStrategy(
            error=f"http_{status_code}",
            error_type=inferred_error_type,
            status_code=status_code,
        ).decide(SpidercloudErrorContext(status_code, inferred_error_type, message, source))
    return NonRetryableSpidercloudErrorStrategy(error="unknown_error").decide(
        SpidercloudErrorContext(status_code, inferred_error_type, message, source)
    )


def decision_for_exception(exc: BaseException, *, source: str | None = None) -> SpidercloudErrorDecision:
    message = str(exc) if exc else None
    status_code = None
    error_type = None

    if ActivityError is not None and isinstance(exc, ActivityError):
        cause = exc.cause
        if cause is not None:
            return decision_for_exception(cause, source=source)

    if ApplicationError is not None and isinstance(exc, ApplicationError):
        error_type = getattr(exc, "type", None)
        message = str(exc)
        status_code = _parse_status_code_from_message(message)

    if error_type in SPIDERCLOUD_FATAL_TYPES:
        return FatalSpidercloudErrorStrategy(
            error=error_type,
            error_type=error_type,
            status_code=status_code,
        ).decide(SpidercloudErrorContext(status_code, error_type, message, source))

    if error_type in SPIDERCLOUD_RETRYABLE_TYPES:
        retry_after = _RETRY_AFTER_BY_TYPE.get(error_type, _DEFAULT_RETRY_AFTER_SECONDS)
        return RetryableSpidercloudErrorStrategy(
            error=error_type,
            retry_after_seconds=retry_after,
            error_type=error_type,
            status_code=status_code,
        ).decide(SpidercloudErrorContext(status_code, error_type, message, source))

    if error_type in SPIDERCLOUD_NON_RETRYABLE_TYPES:
        return NonRetryableSpidercloudErrorStrategy(
            error=error_type,
            error_type=error_type,
            status_code=status_code,
        ).decide(SpidercloudErrorContext(status_code, error_type, message, source))

    if status_code is None:
        status_code = getattr(exc, "status", None) if exc else None
        if isinstance(status_code, str):
            status_code = _parse_status_code_from_message(status_code)

    if status_code is None:
        status_code = _parse_status_code_from_message(message)

    if message and "timeout" in message.lower():
        return NonRetryableSpidercloudErrorStrategy(
            error="timeout",
            error_type=error_type or "spidercloud_timeout",
            status_code=status_code,
        ).decide(SpidercloudErrorContext(status_code, error_type, message, source))

    return decision_for_status_code(
        status_code,
        source=source,
        message=message,
        error_type=error_type,
    )


def decision_for_failed_entry(entry: dict[str, object], *, source: str | None = None) -> SpidercloudErrorDecision | None:
    url_val = entry.get("url")
    if not isinstance(url_val, str) or not url_val.strip():
        return None
    status_val = entry.get("status") or entry.get("httpStatus")
    status_code = None
    if isinstance(status_val, (int, float)):
        status_code = int(status_val)
    error_type = entry.get("errorType") if isinstance(entry.get("errorType"), str) else None
    message = entry.get("error") if isinstance(entry.get("error"), str) else None
    reason = entry.get("reason") if isinstance(entry.get("reason"), str) else None
    if status_code is None and reason and reason.startswith("http_"):
        status_code = _parse_status_code_from_message(reason)
    decision = decision_for_status_code(
        status_code,
        source=source,
        message=message or reason,
        error_type=error_type,
    )
    return decision
