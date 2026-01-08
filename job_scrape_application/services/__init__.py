from __future__ import annotations

from typing import TYPE_CHECKING

__all__ = ["convex_query", "convex_mutation", "get_client", "_set_client_for_tests"]

if TYPE_CHECKING:
    from .convex_client import convex_mutation, convex_query, get_client, _set_client_for_tests


def __getattr__(name: str):
    if name in __all__:
        from . import convex_client as _convex_client

        return getattr(_convex_client, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(__all__)
