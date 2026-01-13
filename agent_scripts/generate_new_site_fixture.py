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
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv


def extract_site_info_from_url(url: str) -> Dict[str, Any]:
    """Extract site information from a job URL."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    path = parsed.path.lower()

    result = {
        "domain": domain,
        "company": "",
        "normalized_company": "",
        "suggested_name": "",
        "suggested_type": "general",  # Default to general type
        "suggested_listing_url": "",
        "is_known_platform": False,
    }

    # Check for known platforms first
    # Greenhouse
    if "greenhouse.io" in domain:
        result["is_known_platform"] = True
        result["suggested_type"] = "greenhouse"
        match = re.search(r"/boards/([a-z0-9_-]+)", parsed.path, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower().replace("-", "_")
            result["suggested_name"] = result["company"]
            result["suggested_listing_url"] = f"https://api.greenhouse.io/v1/boards/{result['company']}/jobs"
        return result

    # Ashby HQ
    if "ashbyhq.com" in domain:
        result["is_known_platform"] = True
        result["suggested_type"] = "ashby"
        match = re.search(r"ashbyhq\.com/([a-z0-9_-]+)", url, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower().replace("-", "_")
            result["suggested_name"] = result["company"]
            result["suggested_listing_url"] = f"https://jobs.ashbyhq.com/{result['company']}"
        return result

    # Lever
    if "lever.co" in domain:
        result["is_known_platform"] = True
        result["suggested_type"] = "lever"
        match = re.search(r"lever\.co/([a-z0-9_-]+)", url, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower().replace("-", "_")
            result["suggested_name"] = result["company"]
            result["suggested_listing_url"] = f"https://jobs.lever.co/{result['company']}"
        return result

    # Workday
    if "workday.com" in domain or "myworkdayjobs.com" in domain:
        result["is_known_platform"] = True
        result["suggested_type"] = "workday"
        match = re.search(r"([a-z0-9]+)\.wd\d+\.myworkdayjobs\.com", domain, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower()
            result["suggested_name"] = result["company"]
        return result

    # Kula
    if "kula.ai" in domain:
        result["is_known_platform"] = True
        result["suggested_type"] = "kula"
        match = re.search(r"careers\.kula\.ai/([a-z0-9_-]+)", url, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower().replace("-", "_")
            result["suggested_name"] = result["company"]
            result["suggested_listing_url"] = f"https://careers.kula.ai/{result['company']}"
        return result

    # Generic career sites - extract company from domain
    # Try common patterns: careers.company.com, jobs.company.com, company.com/careers
    if domain.startswith("careers.") or domain.startswith("jobs."):
        parts = domain.split(".")
        if len(parts) >= 2:
            result["company"] = parts[1]
            result["normalized_company"] = parts[1].lower().replace("-", "_")
            result["suggested_name"] = result["company"].title()
            # Try to construct a listing URL
            if "/job/" in path or "/jobs/" in path:
                # URL likely has job detail - try to get base
                base_path = re.sub(r"/jobs?/[^/]+.*$", "/jobs", path)
                result["suggested_listing_url"] = f"https://{domain}{base_path}"
            else:
                result["suggested_listing_url"] = f"https://{domain}/careers"
    else:
        # Use domain as company name
        parts = domain.replace("www.", "").split(".")
        result["company"] = parts[0]
        result["normalized_company"] = parts[0].lower().replace("-", "_")
        result["suggested_name"] = result["company"].title()
        # Generic listing URL guess
        if "/careers" in path:
            result["suggested_listing_url"] = f"https://{domain}/careers"
        elif "/jobs" in path:
            result["suggested_listing_url"] = f"https://{domain}/jobs"
        else:
            result["suggested_listing_url"] = f"https://{domain}/careers"

    return result


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


def generate_assertion_yaml(
    detail_url: str,
    site_info: Dict[str, Any],
) -> str:
    """Generate placeholder assertion YAML content."""
    handler = site_info.get("suggested_type", "general")
    company = site_info.get("suggested_name", site_info.get("company", "Unknown"))

    lines = [
        f"site_id: {handler}",
        f"detail_url: {detail_url}",
        "expected:",
        f'  title: "TODO"  # Fill in expected title',
        f'  company: "{company}"',
        '  location_contains: "TODO"  # Fill in expected location',
        "  is_remote: false  # Set to true if remote job",
        "  level: mid  # junior/mid/senior/staff",
        "  description_min_words: 300",
        '  description_not_contains: \'{\"\'  # Ensure no JSON blocks in description',
        "  cost_milli_cents_min: 1",
        "  posted_at_not_null: true",
    ]

    return "\n".join(lines) + "\n"


def generate_site_schedule_entry(site_info: Dict[str, Any], listing_url: str) -> str:
    """Generate YAML entry for site_schedules.yml."""
    name = site_info.get("suggested_name", site_info.get("company", "Unknown"))
    site_type = site_info.get("suggested_type", "general")

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
    args = parser.parse_args()

    job_url = args.job_url.strip()
    print(f"Job URL: {job_url}")

    # Extract site info
    site_info = extract_site_info_from_url(job_url)
    print(f"\n=== Site Info ===")
    print(f"Domain:           {site_info['domain']}")
    print(f"Company:          {site_info['company']}")
    print(f"Suggested Name:   {site_info['suggested_name']}")
    print(f"Suggested Type:   {site_info['suggested_type']}")
    print(f"Known Platform:   {site_info['is_known_platform']}")

    listing_url = args.listing_url or site_info.get("suggested_listing_url", "")
    print(f"Listing URL:      {listing_url}")

    # Generate file paths
    date_str = datetime.now().strftime("%Y%m%d")
    company_folder = site_info["normalized_company"] or "unknown"

    fixture_dir = ROOT / f"tests/job_scrape_application/workflows/fixtures/debug/{company_folder}"
    assertion_dir = ROOT / f"tests/job_scrape_application/workflows/assertions/debug/{company_folder}"

    # Generate short identifier from URL
    parsed = urlparse(job_url)
    path_parts = [p for p in parsed.path.split("/") if p]
    short_id = path_parts[-1][:8] if path_parts else "job"
    short_id = re.sub(r"[^a-zA-Z0-9]", "", short_id)[:8]

    handler_type = site_info.get("suggested_type", "general")
    fixture_path = fixture_dir / f"{handler_type}_{short_id}_{date_str}_detail.json"
    assertion_path = assertion_dir / f"{handler_type}_{short_id}_{date_str}.yml"

    print(f"\n=== Output Paths ===")
    print(f"Fixture:    {fixture_path}")
    print(f"Assertions: {assertion_path}")

    # Generate site schedule entry
    schedule_entry = generate_site_schedule_entry(site_info, listing_url)
    print(f"\n=== Site Schedule Entry ===")
    print(schedule_entry)

    if args.dry_run:
        print("\n[DRY RUN] Would create the above files")
        return 0

    # Fetch SpiderCloud fixture
    print(f"\nFetching SpiderCloud fixture from: {job_url}")
    success = await fetch_spidercloud_fixture(job_url, fixture_path)
    if success:
        print(f"✓ Fixture saved to: {fixture_path}")
    else:
        print("⚠️  Failed to fetch fixture (continuing with assertion generation)")

    # Generate assertion file
    assertion_content = generate_assertion_yaml(job_url, site_info)
    assertion_path.parent.mkdir(parents=True, exist_ok=True)
    assertion_path.write_text(assertion_content)
    print(f"✓ Assertions saved to: {assertion_path}")

    # Print test command
    identifier = f"{handler_type}_{short_id}_{date_str}"
    print(f"\n=== Test Command ===")
    print(f"uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[{identifier}] -v")

    # Output JSON for script integration
    output_info = {
        "job_url": job_url,
        "listing_url": listing_url,
        "fixture_path": str(fixture_path),
        "assertion_path": str(assertion_path),
        "identifier": identifier,
        "company": site_info.get("suggested_name", site_info.get("company", "")),
        "normalized_company": site_info.get("normalized_company", ""),
        "handler": handler_type,
        "is_known_platform": site_info.get("is_known_platform", False),
        "schedule_entry": schedule_entry,
    }

    print(f"\n=== JSON Output ===")
    print(json.dumps(output_info, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
