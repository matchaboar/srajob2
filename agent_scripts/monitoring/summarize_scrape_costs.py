#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
CONVEX_DIR = REPO_ROOT / "job_board_application"

def _load_env(target_env: str) -> None:
    load_dotenv()
    if target_env == "prod":
        load_dotenv(CONVEX_DIR / ".env.production", override=True)
    else:
        load_dotenv(CONVEX_DIR / ".env", override=False)
        load_dotenv(CONVEX_DIR / ".env.local", override=False)
def _format_time(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
def _format_number(value: Any) -> str:
    if isinstance(value, (int, float)):
        if isinstance(value, bool):
            return str(value)
        if float(value).is_integer():
            return str(int(value))
        return f"{value:.2f}"
    return str(value)
def _table(rows: List[Dict[str, Any]], limit: int | None) -> str:
    columns = [
        ("company", "Company"),
        ("scrapeCount", "Pages"),
        ("avgCostPerPageMilliCents", "Avg/Page (m¢)"),
        ("maxCostPerPageMilliCents", "Max/Page (m¢)"),
        ("totalCostMilliCents", "Total (m¢)"),
    ]
    if limit is not None:
        rows = rows[: max(0, limit)]

    def _clean(value: Any) -> str:
        return " ".join(_format_number(value).split())

    def _ellipsize(value: str, max_len: int) -> str:
        if len(value) <= max_len:
            return value
        if max_len <= 3:
            return value[:max_len]
        return value[: max_len - 3] + "..."

    max_company_width = 80

    widths = []
    for key, label in columns:
        width = len(label)
        for row in rows:
            cell = _clean(row.get(key, ""))
            if key == "company":
                cell = _ellipsize(cell, max_company_width)
            width = max(width, len(cell))
        if key == "company":
            width = min(width, max_company_width)
        widths.append(width)

    header = "  ".join(label.ljust(widths[i]) for i, (_, label) in enumerate(columns))
    sep = "  ".join("-" * widths[i] for i in range(len(columns)))
    lines = [header, sep]

    for row in rows:
        values = []
        for i, (key, _) in enumerate(columns):
            cell = _clean(row.get(key, ""))
            if key == "company":
                cell = _ellipsize(cell, widths[i])
            values.append(cell.ljust(widths[i]))
        lines.append("  ".join(values))

    return "\n".join(lines)
async def _safe_query(convex_query, name: str, args: Dict[str, Any], timeout: int) -> Any:
    try:
        return await asyncio.wait_for(convex_query(name, args), timeout=timeout)
    except asyncio.TimeoutError:
        return {"_error": f"timeout_after_{timeout}s"}
    except Exception as exc:  # noqa: BLE001
        return {"_error": f"{type(exc).__name__}: {exc}"}
async def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize scrape costs per company from Convex.")
    parser.add_argument(
        "--env",
        default="prod",
        choices=("dev", "prod"),
        help="Convex environment to query (default: prod).",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=60,
        help="Minutes of lookback for scrapes (default: 60).",
    )
    parser.add_argument(
        "--max-scrapes",
        type=int,
        default=5000,
        help="Max scrapes to scan (default: 5000).",
    )
    parser.add_argument(
        "--timeout-secs",
        type=int,
        default=30,
        help="Timeout in seconds for the Convex query (default: 30).",
    )
    parser.add_argument(
        "--format",
        default="table",
        choices=("table", "json"),
        help="Output format (default: table).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of rows to print (default: all).",
    )
    args = parser.parse_args()

    _load_env(args.env)

    from job_scrape_application.services import convex_query  # noqa: E402

    payload = await _safe_query(
        convex_query,
        "sites:listScrapeCostSummary",
        {"lookbackMinutes": args.lookback_minutes, "maxScrapes": args.max_scrapes},
        args.timeout_secs,
    )

    if not isinstance(payload, dict) or payload.get("_error"):
        print(
            json.dumps(
                {
                    "env": args.env,
                    "error": payload.get("_error") if isinstance(payload, dict) else "unknown",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    window_start = payload.get("windowStartMs")
    window_end = payload.get("windowEndMs")
    scrapes_checked = payload.get("scrapesChecked")

    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    title_parts = [
        f"env={args.env}",
        f"lookback={payload.get('lookbackMinutes', args.lookback_minutes)}m",
        f"scrapes={scrapes_checked}",
    ]
    if isinstance(window_start, int) and isinstance(window_end, int):
        title_parts.append(f"window={_format_time(window_start)} → {_format_time(window_end)}")

    print("Scrape costs per company (" + ", ".join(title_parts) + ")")
    print(_table(rows, args.limit))
if __name__ == "__main__":
    asyncio.run(main())
