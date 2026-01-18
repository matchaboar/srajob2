"""Makes job_scrape_application importable as a package for scripts and tests."""

from __future__ import annotations

import functools
from typing import Callable, ParamSpec, TypeVar

from dbos import DBOS

P = ParamSpec("P")
R = TypeVar("R")


def _pure_func_decorator(
    func: Callable[P, R] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]] | Callable[P, R]:
    """Provide a no-op @DBOS.pure_func decorator if missing from the DBOS SDK."""
    def decorator(target: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(target)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return target(*args, **kwargs)

        return wrapper

    if func is None:
        return decorator
    return decorator(func)


if not hasattr(DBOS, "pure_func"):
    DBOS.pure_func = staticmethod(_pure_func_decorator)
