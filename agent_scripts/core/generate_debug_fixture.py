#!/usr/bin/env python3
"""
Generate debug fixture for a specific job URL with improved organization.

Features:
- Per-company folder organization for fixtures and assertions
- Date-based filenames to avoid overwriting
- URL correction to get the canonical detail URL
- Remote override config awareness
- Metadata from Convex production for accurate assertions

Usage:
    python agent_scripts/generate_debug_fixture.py <convex_job_id_or_share_url>

    # With explicit URL override
    python agent_scripts/generate_debug_fixture.py <convex_job_id> --url <detail_url>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

# Import shared utilities from lib
from agent_scripts.lib import (
    extract_job_id_from_url,
    fetch_job_by_id,
    extract_site_info_from_url,
    get_canonical_detail_url,
    generate_assertion_yaml,
)

ROOT = Path(__file__).resolve().parent.parent
def is_remote_company(company_name: str) -> bool:
    """Check if company is in the remote companies config."""
    try:
        from job_scrape_application.constants import is_remote_company as _is_remote
        return _is_remote(company_name)
    except ImportError:
        # Fallback to reading YAML directly
        config_path = ROOT / "job_scrape_application/config/prod/remote_companies.yaml"
        if config_path.exists():
            import yaml
            data = yaml.safe_load(config_path.read_text())
            companies = data.get("companies", []) if isinstance(data, dict) else data or []
            normalized = company_name.lower().strip()
            return any(c.lower().strip() == normalized for c in companies)
        return False
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

        # Set defaults
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


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate debug fixture for a specific job"
    )
    parser.add_argument(
        "job_id_or_url",
        help="Convex job ID or share URL (e.g., k57abc123 or https://srajob.netlify.app/job/k57abc123)",
    )
    parser.add_argument(
        "--url",
        help="Override the detail URL to fetch (use when Convex URL differs from canonical)",
    )
    parser.add_argument(
        "--url-only",
        action="store_true",
        help="Fetch URL directly without looking up in Convex (for one-off testing)",
    )
    parser.add_argument(
        "--company",
        help="Override company name for folder organization",
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

    # Handle --url-only mode (direct URL testing without Convex lookup)
    if args.url_only:
        job_url = args.job_id_or_url
        if not job_url.startswith("http"):
            print(f"--url-only requires a valid URL, got: {job_url}", file=sys.stderr)
            return 1

        # Generate a short ID from URL hash for filename
        import hashlib
        job_id = hashlib.md5(job_url.encode()).hexdigest()[:8]
        job_data = {}  # No Convex data available

        if args.output_format == "human":
            print(f"URL-only mode: {job_url}")
            print(f"Generated ID: {job_id}")
    else:
        # Extract job ID
        job_id = extract_job_id_from_url(args.job_id_or_url)
        if not job_id:
            print(f"Could not extract job ID from: {args.job_id_or_url}", file=sys.stderr)
            return 1

        if args.output_format == "human":
            print(f"Job ID: {job_id}")

        # Fetch job data from Convex
        if args.output_format == "human":
            print("Fetching job from Convex prod...")
        job_data = fetch_job_by_id(job_id, env='prod')
        if not job_data:
            print(f"Job not found in Convex: {job_id}", file=sys.stderr)
            return 1

        # Display job info
        if args.output_format == "human":
            print("\n=== Job Details ===")
            print(f"Title:    {job_data.get('title', 'N/A')}")
            print(f"Company:  {job_data.get('company', 'N/A')}")
            print(f"Location: {job_data.get('location', 'N/A')}")
            print(f"Remote:   {job_data.get('remote', False)}")
            print(f"Level:    {job_data.get('level', 'N/A')}")
            print(f"URL:      {job_data.get('url', 'N/A')}")

        # Determine the detail URL
        job_url = args.url or job_data.get("url", "")

    if not job_url:
        print("No job URL available", file=sys.stderr)
        return 1

    # Extract site info
    site_info = extract_site_info_from_url(job_url)
    if args.output_format == "human":
        print("\n=== Site Info ===")
        print(f"Handler:  {site_info.handler}")
        print(f"Company:  {site_info.company}")

    # Get canonical detail URL
    detail_url = get_canonical_detail_url(job_url)
    if args.output_format == "human" and detail_url != job_url:
        print("\n=== URL Correction ===")
        print(f"Original: {job_url}")
        print(f"Canonical: {detail_url}")

    # Check remote override
    company_name = job_data.get("company", site_info.company or "")
    remote_override = is_remote_company(company_name)
    if args.output_format == "human" and remote_override:
        print(f"\n⚠️  {company_name} is in remote_companies.yaml - all jobs marked remote")

    # Generate file paths with timestamp and per-company organization
    # Use ISO timestamp format: YYYYMMDDTHHMMSS for unique identification
    timestamp_str = datetime.now().strftime("%Y%m%dT%H%M%S")

    # Determine company folder: priority is --company > Convex data > URL parsing
    if args.company:
        company_folder = args.company.lower().replace(" ", "_").replace("-", "_")
    elif job_data.get("company"):
        company_folder = job_data["company"].lower().replace(" ", "_").replace("-", "_")
    else:
        company_folder = site_info.normalized_company or site_info["handler"]

    fixture_dir = ROOT / f"tests/job_scrape_application/workflows/fixtures/debug/{company_folder}"
    assertion_dir = ROOT / f"tests/job_scrape_application/workflows/assertions/debug/{company_folder}"

    # Use shortened job ID for cleaner filenames
    short_id = job_id[-8:] if len(job_id) > 8 else job_id

    fixture_path = fixture_dir / f"{site_info.handler}_{short_id}_{timestamp_str}_detail.json"
    assertion_path = assertion_dir / f"{site_info.handler}_{short_id}_{timestamp_str}.yml"

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
        print(f"\nFetching SpiderCloud fixture from: {detail_url}")
    success = await fetch_spidercloud_fixture(detail_url, fixture_path)
    if args.output_format == "human":
        if success:
            print(f"✓ Fixture saved to: {fixture_path}")
        else:
            print("⚠️  Failed to fetch fixture (continuing with assertion generation)")

    # Generate assertion file
    assertion_content = generate_assertion_yaml(job_data, detail_url, site_info.handler, is_remote_override=remote_override)
    assertion_path.parent.mkdir(parents=True, exist_ok=True)
    assertion_path.write_text(assertion_content)
    if args.output_format == "human":
        print(f"✓ Assertions saved to: {assertion_path}")

    # Print test command
    identifier = f"{site_info.handler}_{short_id}_{timestamp_str}"
    if args.output_format == "human":
        print("\n=== Test Command ===")
        print(f"uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[{identifier}] -v")

    # Output JSON for script integration
    output_info = {
        "job_id": job_id,
        "detail_url": detail_url,
        "fixture_path": str(fixture_path),
        "assertion_path": str(assertion_path),
        "identifier": identifier,
        "company": company_name,
        "handler": site_info.handler,
        "remote_override": remote_override,
    }

    if args.output_format == "json":
        print(json.dumps(output_info, indent=2))
    else:
        print("\n=== JSON Output ===")
        print(json.dumps(output_info, indent=2))

    return 0
if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
