"""Placeholder constants to satisfy scraper-worker scripts."""

from __future__ import annotations

import os

OPENROUTER_API_KEY: str | None = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL: str | None = os.getenv("OPENROUTER_BASE_URL")
MODEL_NAME: str = os.getenv("OPENROUTER_MODEL_NAME", "")
