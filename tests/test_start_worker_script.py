from __future__ import annotations

import re
from pathlib import Path


def _read_start_worker() -> str:
    return Path("start_worker.ps1").read_text(encoding="utf-8")


def _extract_count(script: str, name: str) -> int:
    pattern = rf"\${name}\s*=\s*(\d+)"
    match = re.search(pattern, script)
    assert match, f"Missing {name} assignment in start_worker.ps1"
    return int(match.group(1))


def test_start_worker_defaults_to_multiple_workers():
    script = _read_start_worker()
    general = _extract_count(script, "generalWorkerCount")
    job_details = _extract_count(script, "jobDetailsWorkerCount")

    assert general == 4
    assert job_details == 2
