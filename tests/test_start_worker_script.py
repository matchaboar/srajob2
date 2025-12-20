from __future__ import annotations

import re
from pathlib import Path


def _read_start_worker() -> str:
    return Path("start_worker.ps1").read_text(encoding="utf-8")


def _extract_count(script: str, name: str) -> int:
    patterns = [
        rf"\${name}\s*=\s*(\d+)",
        rf"\$default{name[0].upper()}{name[1:]}\s*=\s*(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, script)
        if match:
            return int(match.group(1))
    assert False, f"Missing {name} assignment in start_worker.ps1"


def test_start_worker_defaults_to_multiple_workers():
    script = _read_start_worker()
    general = _extract_count(script, "generalWorkerCount")
    job_details = _extract_count(script, "jobDetailsWorkerCount")

    assert general == 4
    assert job_details == 4
