from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pytest


def _disable_fetchfox_firecrawl_env() -> None:
    os.environ.setdefault("ENABLE_FIRECRAWL", "false")
    os.environ.setdefault("ENABLE_FETCHFOX", "false")


def _ensure_fetchfox_stub() -> None:
    if "fetchfox_sdk" in sys.modules:
        return
    fetchfox_mod = types.ModuleType("fetchfox_sdk")

    class _FakeFetchFox:  # minimal stub for import compatibility
        def __init__(self, *args, **kwargs):
            pass

    fetchfox_mod.FetchFox = _FakeFetchFox
    sys.modules["fetchfox_sdk"] = fetchfox_mod


def _ensure_firecrawl_stub() -> None:
    if "firecrawl" in sys.modules:
        return

    firecrawl_mod = types.ModuleType("firecrawl")

    class _FakeFirecrawl:  # minimal stub for import compatibility
        def __init__(self, *args, **kwargs):
            pass

    firecrawl_mod.Firecrawl = _FakeFirecrawl

    firecrawl_v2 = types.ModuleType("firecrawl.v2")
    firecrawl_v2_types = types.ModuleType("firecrawl.v2.types")

    class _FakePaginationConfig:  # noqa: D401
        """Stub class for firecrawl.v2.types.PaginationConfig."""

    class _FakeScrapeOptions:  # noqa: D401
        """Stub class for firecrawl.v2.types.ScrapeOptions."""

    firecrawl_v2_types.PaginationConfig = _FakePaginationConfig
    firecrawl_v2_types.ScrapeOptions = _FakeScrapeOptions

    firecrawl_v2_utils = types.ModuleType("firecrawl.v2.utils")
    firecrawl_v2_utils_error = types.ModuleType("firecrawl.v2.utils.error_handler")

    class _PaymentRequiredError(Exception):
        pass

    class _RequestTimeoutError(Exception):
        pass

    firecrawl_v2_utils_error.PaymentRequiredError = _PaymentRequiredError
    firecrawl_v2_utils_error.RequestTimeoutError = _RequestTimeoutError
    firecrawl_v2_utils.error_handler = firecrawl_v2_utils_error

    firecrawl_v2.types = firecrawl_v2_types
    firecrawl_v2.utils = firecrawl_v2_utils
    firecrawl_mod.v2 = firecrawl_v2

    sys.modules["firecrawl"] = firecrawl_mod
    sys.modules["firecrawl.v2"] = firecrawl_v2
    sys.modules["firecrawl.v2.types"] = firecrawl_v2_types
    sys.modules["firecrawl.v2.utils"] = firecrawl_v2_utils
    sys.modules["firecrawl.v2.utils.error_handler"] = firecrawl_v2_utils_error


def _sync_settings_flags() -> None:
    config_mod = sys.modules.get("job_scrape_application.config")
    if not config_mod:
        return
    settings = getattr(config_mod, "settings", None)
    if not settings:
        return
    settings.enable_firecrawl = False
    settings.enable_fetchfox = False


_disable_fetchfox_firecrawl_env()
_ensure_fetchfox_stub()
_ensure_firecrawl_stub()
_sync_settings_flags()


def pytest_collection_modifyitems(config, items) -> None:  # noqa: ARG001
    skip_marker = pytest.mark.skip(reason="firecrawl/fetchfox workers are disabled")
    file_cache: dict[Path, bool] = {}

    for item in items:
        path = Path(str(item.fspath))
        if path.suffix != ".py":
            continue
        matched = file_cache.get(path)
        if matched is None:
            try:
                contents = path.read_text(encoding="utf-8")
            except OSError:
                matched = False
            else:
                matched = "firecrawl" in contents or "fetchfox" in contents
            file_cache[path] = matched
        if matched:
            item.add_marker(skip_marker)
