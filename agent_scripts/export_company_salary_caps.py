#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import re
import statistics
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

CONVEX_DIR = REPO_ROOT / "job_board_application"

LEVELS: List[str] = ["junior", "mid", "senior", "staff"]


def _round_salary(value: float, step: int = 1000) -> int:
    if step <= 0:
        return int(round(value))
    return int(round(value / step) * step)


def _median(values: Iterable[float]) -> float:
    items = [v for v in values if isinstance(v, (int, float)) and v > 0]
    if not items:
        return 0.0
    return float(statistics.median(items))


def _normalize_caps(value: Mapping[str, Any] | None) -> Dict[str, float]:
    normalized: Dict[str, float] = {}
    if not isinstance(value, Mapping):
        return normalized
    for level in LEVELS:
        raw = value.get(level)
        if isinstance(raw, (int, float)) and raw > 0:
            normalized[level] = float(raw)
    return normalized


def _normalize_company_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.strip().lower())


def _pick_display_name(variants: Iterable[str]) -> str:
    candidates = [name.strip() for name in variants if isinstance(name, str) and name.strip()]
    if not candidates:
        return ""

    def sort_key(name: str) -> tuple[int, int, int, int, str]:
        starts_upper = int(name[:1].isupper())
        upper_count = sum(1 for ch in name if ch.isupper())
        has_upper = int(upper_count > 0)
        return (starts_upper, has_upper, upper_count, -len(name), name.lower())

    chosen = sorted(candidates, key=sort_key, reverse=True)[0]
    if not any(ch.isupper() for ch in chosen):
        chosen = chosen.title()
    return chosen


def _estimate_mid(caps: Dict[str, float], ratios: Dict[str, float], fallback_mid: float) -> float:
    if "mid" in caps:
        return caps["mid"]
    for candidate in ("senior", "staff", "junior"):
        value = caps.get(candidate)
        ratio = ratios.get(candidate) or 0.0
        if value and ratio:
            return value / ratio
    return fallback_mid


def _estimate_level(
    level: str,
    caps: Dict[str, float],
    ratios: Dict[str, float],
    mid_value: float,
    fallback: float,
) -> float:
    if level in caps:
        return caps[level]
    ratio = ratios.get(level) or 0.0
    if mid_value and ratio:
        return mid_value * ratio
    return fallback


async def _fetch_salary_caps(min_compensation: int) -> Dict[str, Any]:
    from job_scrape_application.services import convex_query  # noqa: E402

    return await convex_query("admin:listCompanySalaryMaxima", {"minCompensation": min_compensation})


def _load_env(target_env: str) -> None:
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production")
    else:
        load_dotenv(CONVEX_DIR / ".env")
        load_dotenv(CONVEX_DIR / ".env.local", override=False)


def _build_company_caps(payload: Mapping[str, Any], round_step: int) -> Dict[str, Dict[str, int]]:
    caps_raw = payload.get("caps") if isinstance(payload, Mapping) else None
    all_companies = payload.get("allCompanies") if isinstance(payload, Mapping) else None
    global_max = payload.get("globalMaxByLevel") if isinstance(payload, Mapping) else None

    normalized_caps: Dict[str, Dict[str, float]] = {}
    variants: Dict[str, List[str]] = {}
    if isinstance(caps_raw, Mapping):
        for company, levels in caps_raw.items():
            if not isinstance(company, str):
                continue
            key = _normalize_company_key(company)
            if not key:
                continue
            variants.setdefault(key, []).append(company)
            normalized = _normalize_caps(levels if isinstance(levels, Mapping) else None)
            if not normalized:
                continue
            merged = normalized_caps.get(key, {})
            for level, value in normalized.items():
                existing = merged.get(level, 0.0)
                if value > existing:
                    merged[level] = value
            normalized_caps[key] = merged

    normalized_company_keys: List[str] = []
    if isinstance(all_companies, list):
        normalized_company_keys = []
        for company in all_companies:
            if not isinstance(company, str) or not company.strip():
                continue
            key = _normalize_company_key(company)
            if not key:
                continue
            normalized_company_keys.append(key)
            variants.setdefault(key, []).append(company)
    if not normalized_company_keys:
        normalized_company_keys = sorted(normalized_caps.keys())

    level_values: Dict[str, List[float]] = {level: [] for level in LEVELS}
    for levels in normalized_caps.values():
        for level, value in levels.items():
            level_values[level].append(value)

    medians = {level: _median(values) for level, values in level_values.items()}
    fallback_mid = medians.get("mid", 0.0)

    if fallback_mid <= 0 and isinstance(global_max, Mapping):
        fallback_mid = float(global_max.get("mid") or 0.0)

    ratios: Dict[str, float] = {}
    mid_median = medians.get("mid", 0.0) or 0.0
    for level in LEVELS:
        if level == "mid":
            ratios[level] = 1.0
            continue
        numerator = medians.get(level, 0.0)
        ratios[level] = float(numerator / mid_median) if mid_median > 0 else 1.0

    global_fallbacks: Dict[str, float] = {}
    for level in LEVELS:
        value = medians.get(level, 0.0)
        if value <= 0 and isinstance(global_max, Mapping):
            value = float(global_max.get(level) or 0.0)
        global_fallbacks[level] = value

    output: Dict[str, Dict[str, int]] = {}
    for key in sorted(set(normalized_company_keys)):
        name = _pick_display_name(variants.get(key, [key])) or key
        company_caps = normalized_caps.get(key, {})
        mid_value = _estimate_mid(company_caps, ratios, fallback_mid)
        level_caps: Dict[str, int] = {}
        for level in LEVELS:
            estimate = _estimate_level(
                level,
                company_caps,
                ratios,
                mid_value,
                global_fallbacks.get(level, 0.0),
            )
            level_caps[level] = _round_salary(estimate, round_step)
        last = 0
        for level in LEVELS:
            if level_caps[level] < last:
                level_caps[level] = last
            last = level_caps[level]
        output[name] = level_caps

    return output


async def main() -> None:
    parser = argparse.ArgumentParser(description="Export company salary caps to YAML.")
    parser.add_argument(
        "--output",
        default=str(REPO_ROOT / "job_board_application" / "src" / "config" / "company_salary_caps.yml"),
        help="Output YAML path.",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Convex environment to query (dev or prod).",
    )
    parser.add_argument(
        "--min-compensation",
        type=int,
        default=1,
        help="Minimum compensation value to consider (default: 1).",
    )
    parser.add_argument(
        "--round-step",
        type=int,
        default=1000,
        help="Round salaries to nearest N (default: 1000).",
    )
    args = parser.parse_args()

    _load_env(args.env)
    payload = await _fetch_salary_caps(args.min_compensation)
    company_caps = _build_company_caps(payload or {}, args.round_step)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload_out = {"company_salary_caps": company_caps}
    output_path.write_text(yaml.safe_dump(payload_out, sort_keys=False))
    print(f"Wrote {len(company_caps)} company salary cap entries to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
