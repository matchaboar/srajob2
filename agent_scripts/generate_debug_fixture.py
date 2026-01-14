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
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv


def extract_job_id_from_url(url: str) -> Optional[str]:
    """Extract Convex job ID from various URL formats."""
    # https://srajob.netlify.app/job/k57abc123xyz
    match = re.search(r"/job/([a-zA-Z0-9_]+)", url)
    if match:
        return match.group(1)

    # https://affable-kiwi-46.convex.site/share/job?id=k57abc123xyz
    match = re.search(r"[\?\&]id=([a-zA-Z0-9_]+)", url)
    if match:
        return match.group(1)

    # Raw ID
    if re.match(r"^[a-zA-Z0-9_]+$", url):
        return url

    return None


def fetch_convex_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Fetch job data from Convex production."""
    try:
        result = subprocess.run(
            ["npx", "convex", "run", "--prod", "jobs:getJobById", json.dumps({"id": job_id})],
            capture_output=True,
            text=True,
            cwd=ROOT / "job_board_application",
            timeout=30,
        )
        if result.returncode != 0:
            print(f"Convex query failed: {result.stderr}", file=sys.stderr)
            return None

        data = json.loads(result.stdout.strip())
        if data is None:
            return None
        return data
    except Exception as e:
        print(f"Failed to fetch from Convex: {e}", file=sys.stderr)
        return None


def extract_site_info_from_url(url: str) -> Dict[str, str]:
    """Extract site handler type and company name from job URL."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    result = {
        "handler": "unknown",
        "company": "",
        "normalized_company": "",
    }

    # Greenhouse boards
    if "greenhouse.io" in domain:
        result["handler"] = "greenhouse"
        # boards-api.greenhouse.io/v1/boards/airbnb/jobs/123
        match = re.search(r"/boards/([a-z0-9_-]+)", parsed.path, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower().replace("-", "_")
        else:
            # boards.greenhouse.io/airbnb/jobs/123 (marketing URL, company is first path segment)
            if domain.startswith("boards."):
                path_parts = [p for p in parsed.path.split("/") if p]
                if path_parts:
                    result["company"] = path_parts[0]
                    result["normalized_company"] = path_parts[0].lower().replace("-", "_")

    # Ashby HQ
    elif "ashbyhq.com" in domain:
        result["handler"] = "ashbyhq"
        # jobs.ashbyhq.com/company or api.ashbyhq.com/posting-api/.../company
        match = re.search(r"ashbyhq\.com/(?:posting-api/[^/]+/)?([a-z0-9_-]+)", url, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower().replace("-", "_")

    # Workday
    elif "workday.com" in domain or "myworkdayjobs.com" in domain:
        result["handler"] = "workday"
        # company.wd5.myworkdayjobs.com
        match = re.search(r"([a-z0-9]+)\.wd\d+\.myworkdayjobs\.com", domain, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower()

    # Lever
    elif "lever.co" in domain:
        result["handler"] = "lever"
        # jobs.lever.co/company/job-id
        match = re.search(r"lever\.co/([a-z0-9_-]+)", url, re.I)
        if match:
            result["company"] = match.group(1)
            result["normalized_company"] = match.group(1).lower().replace("-", "_")

    # Custom career sites - try to extract from domain
    else:
        # Try jobs.company.com or careers.company.com patterns
        if domain.startswith("jobs.") or domain.startswith("careers."):
            parts = domain.split(".")
            if len(parts) >= 2:
                result["company"] = parts[1]
                result["normalized_company"] = parts[1].lower()
        else:
            # Use first part of domain
            parts = domain.split(".")
            result["company"] = parts[0]
            result["normalized_company"] = parts[0].lower()

        result["handler"] = result["normalized_company"] or "unknown"

    return result


def get_canonical_detail_url(job_url: str, site_info: Dict[str, str]) -> str:
    """Convert job URL to canonical detail URL format.

    For Greenhouse jobs, converts:
    - boards.greenhouse.io/company/jobs/123 -> boards-api.greenhouse.io/v1/boards/company/jobs/123
    - Already API URLs stay as-is
    """
    # Greenhouse URL normalization
    if site_info["handler"] == "greenhouse":
        # Already API format
        if "boards-api.greenhouse.io" in job_url:
            return job_url

        # Convert marketing URL to API URL
        match = re.search(r"boards\.greenhouse\.io/([a-z0-9_-]+)/jobs/(\d+)", job_url, re.I)
        if match:
            company = match.group(1)
            job_id = match.group(2)
            return f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs/{job_id}"

    return job_url


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


def generate_assertion_yaml(
    job_data: Dict[str, Any],
    detail_url: str,
    site_info: Dict[str, str],
    is_remote_override: bool,
) -> str:
    """Generate assertion YAML content."""
    title = job_data.get("title", "")
    company = job_data.get("company", "")
    location = job_data.get("location", "")
    remote = job_data.get("remote", False)
    level = job_data.get("level", "mid")

    # If company is in remote override list, always expect remote=true
    if is_remote_override:
        remote = True
        remote_comment = "  # Note: Company is in remote_companies.yaml, so remote is always true"
    else:
        remote_comment = ""

    # Determine level from title if available
    title_lower = title.lower()
    if "staff" in title_lower or "principal" in title_lower:
        level = "staff"
    elif "senior" in title_lower or "sr." in title_lower or "lead" in title_lower:
        level = "senior"
    elif "junior" in title_lower or "jr." in title_lower or "entry" in title_lower or "associate" in title_lower:
        level = "junior"
    else:
        level = "mid"  # Default

    lines = [
        "# IMPORTANT_NOTE: ASSERTION SHOULD CONTAIN THE CORRECT EXPECTATION, NOT NECESSARILY WHAT IS EXTRACTED.",
        f"site_id: {site_info['handler']}",
        f"detail_url: {detail_url}",
        "expected:",
        f'  title: "{title}"',
        f'  company: "{company}"',
    ]

    # Use location_contains for flexibility
    if location:
        # Extract key location part
        location_key = location.split(",")[0].strip()
        lines.append(f'  location_contains: "{location_key}"')
    else:
        lines.append('  location_contains: "TODO"  # Fill in expected location')

    lines.append(f"  is_remote: {str(remote).lower()}{remote_comment}")
    lines.append(f"  level: {level}")
    lines.append("  description_min_words: 300")
    lines.append('  description_not_contains: \'{\"\'  # Ensure no JSON blocks in description')
    lines.append("  cost_milli_cents_min: 1")
    lines.append("  posted_at_not_null: true")

    return "\n".join(lines) + "\n"


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
        "--dry-run",
        action="store_true",
        help="Print what would be created without actually fetching",
    )
    args = parser.parse_args()

    # Extract job ID
    job_id = extract_job_id_from_url(args.job_id_or_url)
    if not job_id:
        print(f"Could not extract job ID from: {args.job_id_or_url}", file=sys.stderr)
        return 1

    print(f"Job ID: {job_id}")

    # Fetch job data from Convex
    print("Fetching job from Convex prod...")
    job_data = fetch_convex_job(job_id)
    if not job_data:
        print(f"Job not found in Convex: {job_id}", file=sys.stderr)
        return 1

    # Display job info
    print(f"\n=== Job Details ===")
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
    print(f"\n=== Site Info ===")
    print(f"Handler:  {site_info['handler']}")
    print(f"Company:  {site_info['company']}")

    # Get canonical detail URL
    detail_url = get_canonical_detail_url(job_url, site_info)
    if detail_url != job_url:
        print(f"\n=== URL Correction ===")
        print(f"Original: {job_url}")
        print(f"Canonical: {detail_url}")

    # Check remote override
    company_name = job_data.get("company", site_info.get("company", ""))
    remote_override = is_remote_company(company_name)
    if remote_override:
        print(f"\n⚠️  {company_name} is in remote_companies.yaml - all jobs marked remote")

    # Generate file paths with timestamp and per-company organization
    # Use ISO timestamp format: YYYYMMDDTHHMMSS for unique identification
    timestamp_str = datetime.now().strftime("%Y%m%dT%H%M%S")
    company_folder = site_info["normalized_company"] or site_info["handler"]

    fixture_dir = ROOT / f"tests/job_scrape_application/workflows/fixtures/debug/{company_folder}"
    assertion_dir = ROOT / f"tests/job_scrape_application/workflows/assertions/debug/{company_folder}"

    # Use shortened job ID for cleaner filenames
    short_id = job_id[-8:] if len(job_id) > 8 else job_id

    fixture_path = fixture_dir / f"{site_info['handler']}_{short_id}_{timestamp_str}_detail.json"
    assertion_path = assertion_dir / f"{site_info['handler']}_{short_id}_{timestamp_str}.yml"

    print(f"\n=== Output Paths ===")
    print(f"Fixture:    {fixture_path}")
    print(f"Assertions: {assertion_path}")

    if args.dry_run:
        print("\n[DRY RUN] Would create the above files")
        return 0

    # Fetch SpiderCloud fixture
    print(f"\nFetching SpiderCloud fixture from: {detail_url}")
    success = await fetch_spidercloud_fixture(detail_url, fixture_path)
    if success:
        print(f"✓ Fixture saved to: {fixture_path}")
    else:
        print("⚠️  Failed to fetch fixture (continuing with assertion generation)")

    # Generate assertion file
    assertion_content = generate_assertion_yaml(job_data, detail_url, site_info, remote_override)
    assertion_path.parent.mkdir(parents=True, exist_ok=True)
    assertion_path.write_text(assertion_content)
    print(f"✓ Assertions saved to: {assertion_path}")

    # Print test command
    identifier = f"{site_info['handler']}_{short_id}_{timestamp_str}"
    print(f"\n=== Test Command ===")
    print(f"uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[{identifier}] -v")

    # Output JSON for script integration
    output_info = {
        "job_id": job_id,
        "detail_url": detail_url,
        "fixture_path": str(fixture_path),
        "assertion_path": str(assertion_path),
        "identifier": identifier,
        "company": company_name,
        "handler": site_info["handler"],
        "remote_override": remote_override,
    }

    print(f"\n=== JSON Output ===")
    print(json.dumps(output_info, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
