from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from .paths import resolve_config_path

@dataclass
class RuntimeConfig:
    spidercloud_job_details_timeout_minutes: int
    spidercloud_job_details_batch_size: int
    spidercloud_listing_batch_size: int
    spidercloud_job_details_concurrency: int
    spidercloud_job_details_processing_expire_minutes: int
    spidercloud_http_timeout_seconds: int
    spidercloud_listing_timeout_seconds: int  # Timeout for listing page fetches (longer than per-URL)
    temporal_general_worker_count: int
    temporal_job_details_worker_count: int
    temporal_listing_worker_count: int
    spidercloud_single_request_mode: bool  # RECOMMENDED: Use synchronous JSON (streaming mode is DEPRECATED)


def _load_runtime_yaml() -> Dict[str, Any]:
    path = resolve_config_path("runtime.yaml")
    if not path.exists():
        return {}
    try:
        import yaml
    except Exception:
        return {}
    try:
        data = yaml.safe_load(path.read_text()) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _coerce_int(config: Dict[str, Any], key: str, default: int) -> int:
    value = config.get(key)
    if isinstance(value, (int, float)):
        return int(value)
    return default


def _coerce_bool(config: Dict[str, Any], key: str, default: bool) -> bool:
    value = config.get(key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return default


_raw_runtime_config = _load_runtime_yaml()

runtime_config = RuntimeConfig(
    spidercloud_job_details_timeout_minutes=_coerce_int(
        _raw_runtime_config,
        "spidercloud_job_details_timeout_minutes",
        4,
    ),
    spidercloud_job_details_batch_size=_coerce_int(
        _raw_runtime_config,
        "spidercloud_job_details_batch_size",
        1,  # Single URL per request in single_request_mode
    ),
    spidercloud_listing_batch_size=_coerce_int(
        _raw_runtime_config,
        "spidercloud_listing_batch_size",
        5,
    ),
    spidercloud_job_details_concurrency=_coerce_int(
        _raw_runtime_config,
        "spidercloud_job_details_concurrency",
        10,  # Increased for single_request_mode
    ),
    spidercloud_job_details_processing_expire_minutes=_coerce_int(
        _raw_runtime_config,
        "spidercloud_job_details_processing_expire_minutes",
        5,
    ),
    spidercloud_http_timeout_seconds=_coerce_int(
        _raw_runtime_config,
        "spidercloud_http_timeout_seconds",
        60,  # Per-URL job detail timeout (60 seconds)
    ),
    spidercloud_listing_timeout_seconds=_coerce_int(
        _raw_runtime_config,
        "spidercloud_listing_timeout_seconds",
        300,  # Listing page timeout (5 minutes)
    ),
    temporal_general_worker_count=_coerce_int(
        _raw_runtime_config,
        "temporal_general_worker_count",
        4,
    ),
    temporal_job_details_worker_count=_coerce_int(
        _raw_runtime_config,
        "temporal_job_details_worker_count",
        4,
    ),
    temporal_listing_worker_count=_coerce_int(
        _raw_runtime_config,
        "temporal_listing_worker_count",
        4,
    ),
    spidercloud_single_request_mode=_coerce_bool(
        _raw_runtime_config,
        "spidercloud_single_request_mode",
        True,  # Default True - streaming mode is DEPRECATED and has hint extraction bugs
    ),
)


def _validate_runtime_config() -> None:
    """Validate runtime configuration and warn about potential issues."""
    import logging
    logger = logging.getLogger(__name__)

    # Check if concurrency × workers might exceed Convex 128 action limit
    # Formula: workers × concurrent_storage × 2 (mutations per store) × 2 (pipeline parallelism)
    max_concurrent_storage = runtime_config.temporal_job_details_worker_count * runtime_config.spidercloud_job_details_concurrency
    estimated_convex_actions = max_concurrent_storage * 2  # 2 mutations per store typically

    # With pipeline parallelism, we might have 2 batches in flight per worker
    peak_convex_actions = estimated_convex_actions * 2

    if peak_convex_actions > 128:
        logger.warning(
            f"Configuration may exceed Convex 128 action limit: "
            f"{runtime_config.temporal_job_details_worker_count} workers × "
            f"{runtime_config.spidercloud_job_details_concurrency} concurrent × "
            f"2 mutations × 2 (pipeline) = ~{peak_convex_actions} peak actions. "
            f"Convex will auto-enqueue excess (adds latency). "
            f"Consider reducing workers or concurrency if you see queue buildup."
        )

    # Warn if HTTP timeout is too high
    if runtime_config.spidercloud_http_timeout_seconds > 600:
        logger.warning(
            f"HTTP timeout is very high: {runtime_config.spidercloud_http_timeout_seconds}s. "
            f"Consider reducing to 300-600s for faster failure detection."
        )

    # Warn if batch size is too small (inefficient)
    if runtime_config.spidercloud_job_details_batch_size < 10:
        logger.info(
            f"Batch size is small: {runtime_config.spidercloud_job_details_batch_size}. "
            f"Consider increasing to 15-20 for better throughput with pipeline parallelism."
        )

    # Calculate expected throughput
    # Rough estimate: workers × (batch_size / estimated_batch_time_minutes)
    # Assume ~30-60s per batch with concurrency and pipeline parallelism
    estimated_batch_time_minutes = 1.0  # Optimistic with good concurrency
    estimated_throughput = (
        runtime_config.temporal_job_details_worker_count *
        runtime_config.spidercloud_job_details_batch_size /
        estimated_batch_time_minutes
    )

    logger.info(
        f"Runtime config loaded: {runtime_config.temporal_job_details_worker_count} workers, "
        f"batch_size={runtime_config.spidercloud_job_details_batch_size}, "
        f"concurrency={runtime_config.spidercloud_job_details_concurrency}. "
        f"Estimated throughput: ~{estimated_throughput:.0f} URLs/min (target: 100 URLs/min)"
    )


# Validate configuration on module load
_validate_runtime_config()
