from .base import NonRetryableWorkflowError


class PaymentRequiredWorkflowError(NonRetryableWorkflowError):
    """Firecrawl returned 402 / insufficient credits."""

    pass
