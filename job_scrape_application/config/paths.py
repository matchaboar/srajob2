from __future__ import annotations

import os
from pathlib import Path

_VALID_ENVS = {"dev", "prod"}


def get_config_env() -> str:
    raw = os.getenv("JOB_SCRAPE_ENV") or os.getenv("APP_ENV") or os.getenv("ENV") or "dev"
    env = raw.strip().lower()
    return env if env in _VALID_ENVS else "dev"


def get_config_root() -> Path:
    # Avoid Path.resolve() to keep workflow sandbox imports deterministic.
    return Path(__file__).parent


def get_env_dir(env: str | None = None) -> Path:
    return get_config_root() / (env or get_config_env())


def resolve_config_path(filename: str, env: str | None = None) -> Path:
    env_dir = get_env_dir(env)
    candidate = env_dir / filename
    if candidate.exists():
        return candidate
    legacy = get_config_root() / filename
    if legacy.exists():
        return legacy
    return candidate
