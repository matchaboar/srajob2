from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass
class RuntimeConfig:
    spidercloud_job_details_timeout_minutes: int
    spidercloud_job_details_batch_size: int
    spidercloud_job_details_processing_expire_minutes: int
    spidercloud_http_timeout_seconds: int


def _load_runtime_yaml() -> Dict[str, Any]:
    path = Path(__file__).with_name("runtime.yaml")
    if not path.exists():
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


_raw_runtime_config = _load_runtime_yaml()

runtime_config = RuntimeConfig(
    spidercloud_job_details_timeout_minutes=_coerce_int(
        _raw_runtime_config,
        "spidercloud_job_details_timeout_minutes",
        15,
    ),
    spidercloud_job_details_batch_size=_coerce_int(
        _raw_runtime_config,
        "spidercloud_job_details_batch_size",
        50,
    ),
    spidercloud_job_details_processing_expire_minutes=_coerce_int(
        _raw_runtime_config,
        "spidercloud_job_details_processing_expire_minutes",
        20,
    ),
    spidercloud_http_timeout_seconds=_coerce_int(
        _raw_runtime_config,
        "spidercloud_http_timeout_seconds",
        900,
    ),
)
