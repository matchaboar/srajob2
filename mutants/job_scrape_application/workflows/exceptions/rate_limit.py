from .base import RetryableWorkflowError


class RateLimitWorkflowError(RetryableWorkflowError):
    """Provider rate limit hit; safe to retry."""

    pass
