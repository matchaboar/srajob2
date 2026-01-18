from .base import RetryableWorkflowError


class TimeoutWorkflowError(RetryableWorkflowError):
    """Transient timeout from provider or network."""

    pass
