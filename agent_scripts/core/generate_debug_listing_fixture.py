#!/usr/bin/env python3
"""
Generate debug fixture for listing page extraction issues.

This script helps debug issues with listing page URL extraction by:
1. Fetching the listing page from SpiderCloud
2. Saving the response as a test fixture
3. Creating an assertion file for validation
4. Providing the test command to run

Usage:
    # From site schedule (preferred)
    python agent_scripts/generate_debug_listing_fixture.py --company airbnb

    # From direct URL
    python agent_scripts/generate_debug_listing_fixture.py --url https://api.greenhouse.io/v1/boards/airbnb/jobs

    # From Convex site ID
    python agent_scripts/generate_debug_listing_fixture.py --site-id k57abc123xyz
"""

from __future__ import annotations

import argparse
import asyncio
import orjson
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import yaml

# Import shared utilities from lib
from agent_scripts.lib import (
    extract_site_info_from_url,
    fetch_site_by_id,
    generate_listing_assertion_yaml,
)

ROOT = Path(__file__).resolve().parent.parent


def load_site_schedule(company: str, env: str = "prod") -> Optional[Dict[str, Any]]:
    """Load site schedule entry for a company."""
    schedule_path = ROOT / f"job_scrape_application/config/{env}/site_schedules.yml"
    if not schedule_path.exists():
        print(f"Schedule file not found: {schedule_path}", file=sys.stderr)
        return None

    data = yaml.safe_load(schedule_path.read_text())
    entries = data if isinstance(data, list) else data.get("site_schedules", [])

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name", "").lower()
        url = entry.get("url", "").lower()
        if company.lower() in name or company.lower() in url:
            return entry

    return None


async def fetch_spidercloud_listing(url: str, output_path: Path) -> bool:
    """Fetch SpiderCloud response for listing page and save as fixture."""
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

        # Set defaults for listing pages
        if "return_format" not in params:
            params["return_format"] = ["raw_html"]
        params.setdefault("request", "chrome")
        params.setdefault("follow_redirects", True)
        params.setdefault("redirect_policy", "Loose")
        params.setdefault("external_domains", ["*"])
        params.setdefault("preserve_host", True)
        params.setdefault("metadata", True)
        params.setdefault("limit", 1)

        print(f"Fetching listing page: {url}")
        print(
            f"SpiderCloud params: {orjson.dumps(params, option=orjson.OPT_INDENT_2).decode('utf-8')}"
        )

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
                "source_url": url,  # For listing pages, source = listing
                "params": params,
                "stream": False,
            },
            "response": response_items[0] if response_items else {},
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            orjson.dumps(fixture, option=orjson.OPT_INDENT_2).decode("utf-8"),
            encoding="utf-8",
        )
        return True

    except Exception as e:
        print(f"Failed to fetch SpiderCloud listing: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False
def extract_urls_from_fixture(fixture_path: Path, listing_url: str) -> List[str]:
    """Extract URLs from a saved fixture using the test module.

    This runs the same extraction logic that the test would use.
    """
    try:
        from job_scrape_application.workflows.site_handlers import get_site_handler
        from job_scrape_application.workflows.helpers.link_extractors import normalize_url

        fixture_data = orjson.loads(fixture_path.read_text())
        handler = get_site_handler(listing_url)

        # Extract raw content from fixture
        raw_content = ""
        response = fixture_data.get("response", [])
        if isinstance(response, dict):
            # Single response object
            raw_content = response.get("content", {}).get("commonmark", "") or response.get("content", {}).get("raw", "")
        elif isinstance(response, list) and response:
            first_item = response[0]
            if isinstance(first_item, str):
                try:
                    parsed = orjson.loads(first_item)
                    raw_content = parsed.get("content", {}).get("commonmark", "") or parsed.get("content", {}).get("raw", "")
                except orjson.JSONDecodeError:
                    raw_content = first_item
            elif isinstance(first_item, dict):
                raw_content = first_item.get("content", {}).get("commonmark", "") or first_item.get("content", {}).get("raw", "")

        # Extract URLs using handler
        extracted_urls: List[str] = []
        if handler and hasattr(handler, "get_links_from_raw_html"):
            extracted_urls = handler.get_links_from_raw_html(raw_content)

        # Also check links from response metadata (for JS-rendered SPAs)
        if not extracted_urls and isinstance(response, dict):
            page_links = response.get("links", [])
            if page_links:
                import html as html_lib
                from urllib.parse import urljoin
                listing_url.split("?")[0]
                for link in page_links:
                    href = link.get("href") if isinstance(link, dict) else link
                    if not href:
                        continue
                    href = html_lib.unescape(href)
                    if not href.startswith(("http://", "https://")):
                        parsed = listing_url.split("/")
                        if len(parsed) >= 3:
                            origin = "/".join(parsed[:3])
                            href = urljoin(origin, href)
                    if href and href not in extracted_urls:
                        extracted_urls.append(href)

        # Filter URLs using handler
        if handler and hasattr(handler, "filter_job_urls"):
            extracted_urls = handler.filter_job_urls(extracted_urls)

        # Normalize URLs
        normalized_urls: List[str] = []
        seen: set = set()
        for url in extracted_urls:
            normalized = normalize_url(url, base_url=listing_url)
            if normalized and normalized not in seen:
                seen.add(normalized)
                normalized_urls.append(normalized)

        return normalized_urls
    except Exception as e:
        print(f"Warning: Failed to extract URLs from fixture: {e}", file=sys.stderr)
        return []


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate debug fixture for listing page extraction"
    )
    parser.add_argument(
        "--company",
        help="Company name to look up in site schedules (e.g., airbnb, purestorage)",
    )
    parser.add_argument(
        "--url",
        help="Direct listing URL to fetch",
    )
    parser.add_argument(
        "--site-id",
        help="Convex site ID to look up",
    )
    parser.add_argument(
        "--schedule-env",
        default="prod",
        help="Schedule environment (prod or dev)",
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

    # Determine listing URL
    listing_url = args.url
    company_name = ""

    if args.company and not listing_url:
        entry = load_site_schedule(args.company, args.schedule_env)
        if entry:
            listing_url = entry.get("url", "")
            company_name = entry.get("name", args.company)
            if args.output_format == "human":
                print(f"Found site schedule entry for {args.company}:")
                print(f"  Name: {entry.get('name')}")
                print(f"  URL: {listing_url}")
        else:
            print(f"No site schedule found for: {args.company}", file=sys.stderr)
            return 1

    if args.site_id and not listing_url:
        site_data = fetch_site_by_id(args.site_id, env='prod')
        if site_data:
            listing_url = site_data.get("url", "")
            company_name = site_data.get("name", "")
            if args.output_format == "human":
                print(f"Found Convex site: {company_name}")
                print(f"  URL: {listing_url}")
        else:
            print(f"Site not found in Convex: {args.site_id}", file=sys.stderr)
            return 1

    if not listing_url:
        parser.print_help()
        print("\nError: Must provide --company, --url, or --site-id", file=sys.stderr)
        return 1

    # Extract site info
    site_info = extract_site_info_from_url(listing_url)
    if company_name:
        # Override company info from schedule/convex data
        from dataclasses import replace
        site_info = replace(
            site_info,
            company=company_name,
            normalized_company=re.sub(r"[^a-z0-9]+", "_", company_name.lower()).strip("_")
        )

    if args.output_format == "human":
        print("\n=== Site Info ===")
        print(f"Handler:  {site_info.handler}")
        print(f"Company:  {site_info.company}")
        print(f"Listing:  {listing_url}")

    # Generate file paths with timestamp for uniqueness
    # Use ISO timestamp format: YYYYMMDDTHHMMSS
    timestamp_str = datetime.now().strftime("%Y%m%dT%H%M%S")
    company_folder = site_info.normalized_company or site_info.handler

    fixture_dir = ROOT / f"tests/job_scrape_application/workflows/fixtures/debug/{company_folder}"
    assertion_dir = ROOT / f"tests/job_scrape_application/workflows/assertions/debug/{company_folder}"

    fixture_path = fixture_dir / f"{site_info.handler}_{timestamp_str}_listing.json"
    assertion_path = assertion_dir / f"{site_info.handler}_{timestamp_str}_listing.yml"

    if args.output_format == "human":
        print("\n=== Output Paths ===")
        print(f"Fixture:    {fixture_path}")
        print(f"Assertions: {assertion_path}")

    if args.dry_run:
        if args.output_format == "human":
            print("\n[DRY RUN] Would create the above files")
        return 0

    # Fetch SpiderCloud fixture
    if args.output_format == "human":
        print(f"\nFetching SpiderCloud listing from: {listing_url}")
    success = await fetch_spidercloud_listing(listing_url, fixture_path)
    if args.output_format == "human":
        if success:
            print(f"✓ Fixture saved to: {fixture_path}")
        else:
            print("✗ Failed to fetch fixture")
    if not success:
        return 1

    # Extract URLs from the fixture (for Claude to validate)
    if args.output_format == "human":
        print("\n=== Extracting URLs from Fixture ===")
    extracted_urls = extract_urls_from_fixture(fixture_path, listing_url)
    if args.output_format == "human":
        print(f"✓ Extracted {len(extracted_urls)} URLs using {site_info.handler} handler")

    # Generate assertion file (with extracted URLs as comments)
    assertion_content = generate_listing_assertion_yaml(
        listing_url,
        site_info.handler,
        extracted_urls=extracted_urls,
        company_name=site_info.company
    )
    assertion_path.parent.mkdir(parents=True, exist_ok=True)
    assertion_path.write_text(assertion_content)
    if args.output_format == "human":
        print(f"✓ Assertions saved to: {assertion_path}")

    # Build identifier
    identifier = f"{company_folder}/{site_info.handler}_{timestamp_str}"

    # Print extracted URLs for Claude to review
    if args.output_format == "human":
        print(f"\n=== Extracted URLs ({len(extracted_urls)} total) ===")
        for i, url in enumerate(extracted_urls[:50], 1):
            print(f"  {i}. {url}")
        if len(extracted_urls) > 50:
            print(f"  ... and {len(extracted_urls) - 50} more")

        # Print test command
        print("\n=== Test Command ===")
        print(f"DEBUG_EXTRACTION_VERBOSE=1 uv run pytest 'tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction[{identifier}]' -v")

    # Output JSON for script integration
    output_info = {
        "listing_url": listing_url,
        "fixture_path": str(fixture_path),
        "assertion_path": str(assertion_path),
        "identifier": identifier,
        "company": site_info.company,
        "handler": site_info.handler,
        "extracted_url_count": len(extracted_urls),
        "extracted_urls": extracted_urls,  # Full list for Claude to validate
    }

    if args.output_format == "json":
        print(orjson.dumps(output_info, option=orjson.OPT_INDENT_2).decode("utf-8"))
    else:
        print("\n=== JSON Output ===")
        print(orjson.dumps(output_info, option=orjson.OPT_INDENT_2).decode("utf-8"))

    return 0
if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
