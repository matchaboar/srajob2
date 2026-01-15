#!/usr/bin/env python3
"""
Generate fixture for a new site from a job listing URL.

This script:
1. Takes a job detail URL (or listing URL) for a new site
2. Tries to identify the company/site from the URL
3. Fetches a SpiderCloud fixture for a job detail page
4. Creates a placeholder assertion file
5. Outputs JSON metadata for the mise task to use

Usage:
    python agent_scripts/generate_new_site_fixture.py <job_url>

    # With explicit listing URL
    python agent_scripts/generate_new_site_fixture.py <job_detail_url> --listing-url <listing_url>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

from dotenv import load_dotenv

# Import shared utilities from lib
from agent_scripts.lib import (
    extract_site_info_from_url,
    generate_placeholder_assertion_yaml,
)

ROOT = Path(__file__).resolve().parent.parent


async def fetch_spidercloud_fixture(url: str, output_path: Path) -> bool:
    """Fetch SpiderCloud response and save as fixture."""
    load_dotenv()
    load_dotenv(ROOT / "job_board_application/.env.production", override=False)

    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        print("SPIDER_API_KEY not set", file=sys.stderr)
        return False

    try:
        from job_scrape_application.workflows.site_handlers import get_site_handler
        from spider import AsyncSpider

        handler = get_site_handler(url)
        params: Dict[str, Any] = {}
        if handler:
            params.update(handler.normalize_spidercloud_config(handler.get_spidercloud_config(url)))

        # Set defaults for generic scraping
        if "return_format" not in params:
            params["return_format"] = ["raw_html"]
        params.setdefault("request", "chrome")
        params.setdefault("follow_redirects", True)
        params.setdefault("redirect_policy", "Loose")
        params.setdefault("external_domains", ["*"])
        params.setdefault("preserve_host", True)
        params.setdefault("metadata", True)
        params.setdefault("limit", 1)

        async with AsyncSpider(api_key=api_key) as client:
            response_items = []
            async def collect(resp):
                if hasattr(resp, "__aiter__"):
                    async for item in resp:
                        response_items.append(item)
                elif hasattr(resp, "__await__"):
                    result = await resp
                    if result is not None:
                        response_items.append(result)
                else:
                    response_items.append(resp)

            await collect(client.scrape_url(
                url,
                params=params,
                stream=False,
                content_type="application/json",
            ))

        # Format as fixture
        fixture = {
            "request": {
                "url": url,
                "params": params,
                "stream": False,
            },
            "response": response_items[0] if response_items else {},
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(fixture, ensure_ascii=False, indent=2))
        return True

    except Exception as e:
        print(f"Failed to fetch SpiderCloud fixture: {e}", file=sys.stderr)
        return False


def generate_site_schedule_entry(site_info, listing_url: str) -> str:
    """Generate YAML entry for site_schedules.yml."""
    # site_info is a SiteInfo dataclass
    name = site_info.company.title() if site_info.company else "Unknown"
    site_type = site_info.handler

    entry = f"""- url: {listing_url}
  name: {name}
  enabled: true
  type: {site_type}
  scrapeProvider: spidercloud
  paginationLimit: 0
  schedule:
    name: Weekdays every 2 hours @ 09:30
    days: *id001
    startTime: 09:30
    intervalMinutes: 120.0
    timezone: America/Denver"""

    return entry
async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate fixture for a new site"
    )
    parser.add_argument(
        "job_url",
        help="Job detail URL to use for fixture generation",
    )
    parser.add_argument(
        "--listing-url",
        help="Override the listing URL (if different from auto-detected)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be created without actually fetching",
    )
    parser.add_argument(
        "--output-format",
        choices=["human", "json"],
        default="human",
        help="Output format (default: human)",
    )
    args = parser.parse_args()

    job_url = args.job_url.strip()
    if args.output_format == "human":
        print(f"Job URL: {job_url}")

    # Extract site info
    site_info = extract_site_info_from_url(job_url)
    if args.output_format == "human":
        print("\n=== Site Info ===")
        print(f"Domain:           {site_info.domain}")
        print(f"Company:          {site_info.company}")
        print(f"Suggested Name:   {site_info.company.title() if site_info.company else 'Unknown'}")
        print(f"Suggested Type:   {site_info.handler}")
        print(f"Known Platform:   {site_info.is_known_platform}")

    listing_url = args.listing_url or site_info.suggested_listing_url
    if args.output_format == "human":
        print(f"Listing URL:      {listing_url}")

    # Generate file paths
    date_str = datetime.now().strftime("%Y%m%d")
    company_folder = site_info.normalized_company or "unknown"

    fixture_dir = ROOT / f"tests/job_scrape_application/workflows/fixtures/debug/{company_folder}"
    assertion_dir = ROOT / f"tests/job_scrape_application/workflows/assertions/debug/{company_folder}"

    # Generate short identifier from URL
    parsed = urlparse(job_url)
    path_parts = [p for p in parsed.path.split("/") if p]
    short_id = path_parts[-1][:8] if path_parts else "job"
    short_id = re.sub(r"[^a-zA-Z0-9]", "", short_id)[:8]

    handler_type = site_info.handler
    fixture_path = fixture_dir / f"{handler_type}_{short_id}_{date_str}_detail.json"
    assertion_path = assertion_dir / f"{handler_type}_{short_id}_{date_str}.yml"

    if args.output_format == "human":
        print("\n=== Output Paths ===")
        print(f"Fixture:    {fixture_path}")
        print(f"Assertions: {assertion_path}")

    # Generate site schedule entry
    schedule_entry = generate_site_schedule_entry(site_info, listing_url)
    if args.output_format == "human":
        print("\n=== Site Schedule Entry ===")
        print(schedule_entry)

    if args.dry_run:
        if args.output_format == "human":
            print("\n[DRY RUN] Would create the above files")
        return 0

    # Fetch SpiderCloud fixture
    if args.output_format == "human":
        print(f"\nFetching SpiderCloud fixture from: {job_url}")
    success = await fetch_spidercloud_fixture(job_url, fixture_path)
    if args.output_format == "human":
        if success:
            print(f"✓ Fixture saved to: {fixture_path}")
        else:
            print("⚠️  Failed to fetch fixture (continuing with assertion generation)")

    # Generate assertion file
    company_name = site_info.company.title() if site_info.company else "Unknown"
    assertion_content = generate_placeholder_assertion_yaml(job_url, site_info.handler, company_name)
    assertion_path.parent.mkdir(parents=True, exist_ok=True)
    assertion_path.write_text(assertion_content)
    if args.output_format == "human":
        print(f"✓ Assertions saved to: {assertion_path}")

    # Print test command
    identifier = f"{handler_type}_{short_id}_{date_str}"
    if args.output_format == "human":
        print("\n=== Test Command ===")
        print(f"uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[{identifier}] -v")

    # Output JSON for script integration
    output_info = {
        "job_url": job_url,
        "listing_url": listing_url,
        "fixture_path": str(fixture_path),
        "assertion_path": str(assertion_path),
        "identifier": identifier,
        "company": company_name,
        "normalized_company": site_info.normalized_company,
        "handler": handler_type,
        "is_known_platform": site_info.is_known_platform,
        "schedule_entry": schedule_entry,
    }

    if args.output_format == "json":
        print(json.dumps(output_info, indent=2))
    else:
        print("\n=== JSON Output ===")
        print(json.dumps(output_info, indent=2))

    return 0
if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
