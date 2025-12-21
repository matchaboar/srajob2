#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = yaml.safe_load(path.read_text()) or {}
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def main() -> None:
    convex_dir = REPO_ROOT / "job_board_application" / "convex"
    config_dir = REPO_ROOT / "job_scrape_application" / "config"

    for env in ("dev", "prod"):
        yaml_path = config_dir / env / "site_schedules.yml"
        data = _load_yaml(yaml_path)
        payload = {"site_schedules": data.get("site_schedules", [])}
        out_path = convex_dir / f"site_schedules.{env}.json"
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
