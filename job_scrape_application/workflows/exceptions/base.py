from temporalio.exceptions import ApplicationError


class WorkflowError(ApplicationError):
    """Base error that carries retryability information for Temporal workflows."""

    def __init__(self, message: str, *, retryable: bool) -> None:  # noqa: D401
        super().__init__(message, non_retryable=not retryable)
        self.retryable = retryable


class RetryableWorkflowError(WorkflowError):
    """Errors that should cause Temporal to retry the workflow/activity."""

    def __init__(self, message: str) -> None:  # noqa: D401
        super().__init__(message, retryable=True)


class NonRetryableWorkflowError(WorkflowError):
    """Errors that should fail the workflow/activity without retry."""

    def __init__(self, message: str) -> None:  # noqa: D401
        super().__init__(message, retryable=False)
