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
import json
import os
import re
import subprocess
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
import yaml


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


def fetch_convex_site(site_id: str) -> Optional[Dict[str, Any]]:
    """Fetch site data from Convex production."""
    try:
        result = subprocess.run(
            ["npx", "convex", "run", "--prod", "sites:getSiteById", json.dumps({"id": site_id})],
            capture_output=True,
            text=True,
            cwd=ROOT / "job_board_application",
            timeout=30,
        )
        if result.returncode != 0:
            print(f"Convex query failed: {result.stderr}", file=sys.stderr)
            return None

        data = json.loads(result.stdout.strip())
        return data if data else None
    except Exception as e:
        print(f"Failed to fetch from Convex: {e}", file=sys.stderr)
        return None


def extract_site_info_from_url(url: str) -> Dict[str, str]:
    """Extract site handler type and company name from listing URL."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    result = {
        "handler": "unknown",
        "company": "",
        "normalized_company": "",
    }

    # Greenhouse API
    if "greenhouse.io" in domain:
        result["handler"] = "greenhouse"
        match = re.search(r"/boards/([a-z0-9_-]+)", parsed.path, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower().replace("-", "_")

    # AshbyHQ
    elif "ashbyhq.com" in domain:
        result["handler"] = "ashbyhq"
        match = re.search(r"ashbyhq\.com/([a-z0-9_-]+)", url, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower().replace("-", "_")

    # Netflix
    elif "netflix" in domain:
        result["handler"] = "netflix"
        result["company"] = "netflix"
        result["normalized_company"] = "netflix"

    # Workday
    elif "workday.com" in domain or "myworkdayjobs.com" in domain:
        result["handler"] = "workday"
        match = re.search(r"([a-z0-9]+)\.wd\d+\.myworkdayjobs\.com", domain, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower()

    # Lever
    elif "lever.co" in domain:
        result["handler"] = "lever"
        match = re.search(r"lever\.co/([a-z0-9_-]+)", url, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower().replace("-", "_")

    # Meta
    elif "metacareers" in domain or "facebook.com/careers" in url:
        result["handler"] = "meta"
        result["company"] = "meta"
        result["normalized_company"] = "meta"

    # Custom career sites
    else:
        if domain.startswith("jobs.") or domain.startswith("careers."):
            parts = domain.split(".")
            if len(parts) >= 2:
                result["company"] = parts[1]
                result["normalized_company"] = parts[1].lower()
        else:
            parts = domain.split(".")
            result["company"] = parts[0]
            result["normalized_company"] = parts[0].lower()

        result["handler"] = result["normalized_company"] or "unknown"

    return result


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
        print(f"SpiderCloud params: {json.dumps(params, indent=2)}")

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
        output_path.write_text(json.dumps(fixture, ensure_ascii=False, indent=2))
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
        from job_scrape_application.workflows.core import SpiderFixture
        from job_scrape_application.workflows.site_handlers import get_site_handler
        from job_scrape_application.workflows.helpers.link_extractors import normalize_url

        fixture_data = json.loads(fixture_path.read_text())
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
                    parsed = json.loads(first_item)
                    raw_content = parsed.get("content", {}).get("commonmark", "") or parsed.get("content", {}).get("raw", "")
                except json.JSONDecodeError:
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
                base_url = listing_url.split("?")[0]
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


def generate_listing_assertion_yaml(
    listing_url: str,
    site_info: Dict[str, str],
    extracted_urls: Optional[List[str]] = None,
) -> str:
    """Generate assertion YAML content for listing page."""
    lines = [
        "# IMPORTANT_NOTE: ASSERTION SHOULD CONTAIN THE CORRECT EXPECTATION, NOT NECESSARILY WHAT IS EXTRACTED.",
        f"site_id: {site_info['handler']}",
        f"listing_url: {listing_url}",
        "expected:",
        f"  url_count_min: {len(extracted_urls) if extracted_urls else 5}  # Minimum expected job URLs",
        f'  url_pattern: "TODO"  # Regex pattern for valid job URLs (e.g., "/jobs/\\d+")',
        "  no_listing_urls: true  # Ensure listing/search URLs are filtered out",
        f'  handler: "{site_info["handler"]}"',
        "  # expected_urls: List of VALID job detail URLs (uncomment and fill in after validation)",
        "  # If expected_urls is provided, any URL extracted that is NOT in this list will FAIL the test",
        "  # This prevents regressions - invalid URLs sneaking in will be caught immediately",
    ]

    if extracted_urls:
        lines.append("  # --- Extracted URLs (REVIEW EACH ONE - remove invalid URLs) ---")
        lines.append("  # expected_urls:")
        for url in extracted_urls:
            lines.append(f"  #   - \"{url}\"")

    return "\n".join(lines) + "\n"


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
    args = parser.parse_args()

    # Determine listing URL
    listing_url = args.url
    company_name = ""

    if args.company and not listing_url:
        entry = load_site_schedule(args.company, args.schedule_env)
        if entry:
            listing_url = entry.get("url", "")
            company_name = entry.get("name", args.company)
            print(f"Found site schedule entry for {args.company}:")
            print(f"  Name: {entry.get('name')}")
            print(f"  URL: {listing_url}")
        else:
            print(f"No site schedule found for: {args.company}", file=sys.stderr)
            return 1

    if args.site_id and not listing_url:
        site_data = fetch_convex_site(args.site_id)
        if site_data:
            listing_url = site_data.get("url", "")
            company_name = site_data.get("name", "")
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
        site_info["company"] = company_name
        site_info["normalized_company"] = re.sub(r"[^a-z0-9]+", "_", company_name.lower()).strip("_")

    print(f"\n=== Site Info ===")
    print(f"Handler:  {site_info['handler']}")
    print(f"Company:  {site_info['company']}")
    print(f"Listing:  {listing_url}")

    # Generate file paths with timestamp for uniqueness
    # Use ISO timestamp format: YYYYMMDDTHHMMSS
    timestamp_str = datetime.now().strftime("%Y%m%dT%H%M%S")
    company_folder = site_info["normalized_company"] or site_info["handler"]

    fixture_dir = ROOT / f"tests/job_scrape_application/workflows/fixtures/debug/{company_folder}"
    assertion_dir = ROOT / f"tests/job_scrape_application/workflows/assertions/debug/{company_folder}"

    fixture_path = fixture_dir / f"{site_info['handler']}_{timestamp_str}_listing.json"
    assertion_path = assertion_dir / f"{site_info['handler']}_{timestamp_str}_listing.yml"

    print(f"\n=== Output Paths ===")
    print(f"Fixture:    {fixture_path}")
    print(f"Assertions: {assertion_path}")

    if args.dry_run:
        print("\n[DRY RUN] Would create the above files")
        return 0

    # Fetch SpiderCloud fixture
    print(f"\nFetching SpiderCloud listing from: {listing_url}")
    success = await fetch_spidercloud_listing(listing_url, fixture_path)
    if success:
        print(f"✓ Fixture saved to: {fixture_path}")
    else:
        print("✗ Failed to fetch fixture")
        return 1

    # Extract URLs from the fixture (for Claude to validate)
    print(f"\n=== Extracting URLs from Fixture ===")
    extracted_urls = extract_urls_from_fixture(fixture_path, listing_url)
    print(f"✓ Extracted {len(extracted_urls)} URLs using {site_info['handler']} handler")

    # Generate assertion file (with extracted URLs as comments)
    assertion_content = generate_listing_assertion_yaml(listing_url, site_info, extracted_urls)
    assertion_path.parent.mkdir(parents=True, exist_ok=True)
    assertion_path.write_text(assertion_content)
    print(f"✓ Assertions saved to: {assertion_path}")

    # Build identifier
    identifier = f"{company_folder}/{site_info['handler']}_{timestamp_str}"

    # Print extracted URLs for Claude to review
    print(f"\n=== Extracted URLs ({len(extracted_urls)} total) ===")
    for i, url in enumerate(extracted_urls[:50], 1):
        print(f"  {i}. {url}")
    if len(extracted_urls) > 50:
        print(f"  ... and {len(extracted_urls) - 50} more")

    # Print test command
    print(f"\n=== Test Command ===")
    print(f"DEBUG_EXTRACTION_VERBOSE=1 uv run pytest 'tests/job_scrape_application/workflows/test_listing_extraction_e2e.py::test_debug_listing_extraction[{identifier}]' -v")

    # Output JSON for script integration
    output_info = {
        "listing_url": listing_url,
        "fixture_path": str(fixture_path),
        "assertion_path": str(assertion_path),
        "identifier": identifier,
        "company": site_info["company"],
        "handler": site_info["handler"],
        "extracted_url_count": len(extracted_urls),
        "extracted_urls": extracted_urls,  # Full list for Claude to validate
    }

    print(f"\n=== JSON Output ===")
    print(json.dumps(output_info, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
