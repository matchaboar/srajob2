#!/usr/bin/env python3
"""
Debug job extraction issues for a specific job from Convex prod.

Usage:
    uv run agent_scripts/debug_job_extraction.py <job_url_or_id>

Examples:
    uv run agent_scripts/debug_job_extraction.py https://srajob.netlify.app/job/k57abc123
    uv run agent_scripts/debug_job_extraction.py k57abc123

This script will:
1. Extract the job ID from the URL
2. Fetch job details from Convex prod
3. Identify the site handler
4. Check for existing fixtures
5. Generate a debugging report for Claude Code
"""

from __future__ import annotations

import argparse
import orjson
import re
import subprocess
import sys
from pathlib import Path

# Add repo root to path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
def extract_job_id(url_or_id: str) -> str:
    """Extract job ID from URL or return as-is if already an ID."""
    # Match /job/<id> pattern
    match = re.search(r"/job/([a-zA-Z0-9_]+)", url_or_id)
    if match:
        return match.group(1)

    # Check if it's already a valid ID format
    if re.match(r"^[a-zA-Z0-9_]+$", url_or_id):
        return url_or_id

    raise ValueError(f"Could not extract job ID from: {url_or_id}")
def fetch_job_from_convex(job_id: str) -> dict | None:
    """Fetch job details from Convex prod."""
    try:
        result = subprocess.run(
            [
                "npx",
                "convex",
                "run",
                "--prod",
                "router:getJobById",
                orjson.dumps({"id": job_id}).decode("utf-8"),
            ],
            capture_output=True,
            text=True,
            cwd=ROOT / "job_board_application",
            timeout=30,
        )
        if result.returncode != 0:
            print(f"Error fetching job: {result.stderr}", file=sys.stderr)
            return None

        # Parse the output (npx convex run outputs JSON)
        output = result.stdout.strip()
        if not output or output == "null":
            return None

        return orjson.loads(output)
    except subprocess.TimeoutExpired:
        print("Timeout fetching job from Convex", file=sys.stderr)
        return None
    except orjson.JSONDecodeError as e:
        print(f"Error parsing Convex response: {e}", file=sys.stderr)
        return None
def get_site_handler_info(url: str) -> dict:
    """Get site handler information for a URL."""
    try:
        from job_scrape_application.workflows.site_handlers import get_site_handler

        handler = get_site_handler(url)
        if handler:
            return {
                "name": handler.name,
                "is_api_detail_url": handler.is_api_detail_url(url) if hasattr(handler, "is_api_detail_url") else None,
                "is_listing_url": handler.is_listing_url(url) if hasattr(handler, "is_listing_url") else None,
                "supports_listing_api": getattr(handler, "supports_listing_api", None),
            }
        return {"name": None, "error": "No handler found"}
    except Exception as e:
        return {"name": None, "error": str(e)}
def find_fixture(handler_name: str) -> dict:
    """Check if fixtures exist for this handler."""
    fixture_dir = ROOT / "tests/job_scrape_application/workflows/fixtures/dbos_schedule"
    assertion_dir = ROOT / "tests/job_scrape_application/workflows/assertions"

    # Slugify handler name
    slug = re.sub(r"[^a-z0-9]+", "_", handler_name.lower()).strip("_") if handler_name else None

    result = {
        "slug": slug,
        "listing_fixture": None,
        "detail_fixture": None,
        "assertion_file": None,
    }

    if slug:
        listing_path = fixture_dir / f"{slug}_listing.json"
        detail_path = fixture_dir / f"{slug}_detail.json"
        assertion_path = assertion_dir / f"{slug}.yml"

        result["listing_fixture"] = str(listing_path) if listing_path.exists() else None
        result["detail_fixture"] = str(detail_path) if detail_path.exists() else None
        result["assertion_file"] = str(assertion_path) if assertion_path.exists() else None

    return result
def generate_report(job_id: str, job_data: dict, handler_info: dict, fixture_info: dict) -> str:
    """Generate a debugging report for Claude Code."""

    lines = [
        "# Job Extraction Debug Report",
        "",
        "## Job Information",
        f"- **Job ID:** {job_id}",
        f"- **Title:** {job_data.get('title', 'N/A')}",
        f"- **Company:** {job_data.get('company', 'N/A')}",
        f"- **Location:** {job_data.get('location', 'N/A')}",
        f"- **Remote:** {job_data.get('remote', 'N/A')}",
        f"- **Level:** {job_data.get('level', 'N/A')}",
        f"- **Posted At:** {job_data.get('postedAt', 'N/A')}",
        f"- **Compensation:** {job_data.get('totalCompensation', 'N/A')} (unknown: {job_data.get('compensationUnknown', 'N/A')})",
        "",
        "## Source URL",
        "```",
        f"{job_data.get('url', 'N/A')}",
        "```",
        "",
        "## Site Handler",
        f"- **Handler Name:** {handler_info.get('name', 'None')}",
        f"- **Is API Detail URL:** {handler_info.get('is_api_detail_url', 'N/A')}",
        f"- **Is Listing URL:** {handler_info.get('is_listing_url', 'N/A')}",
    ]

    if handler_info.get("error"):
        lines.append(f"- **Error:** {handler_info['error']}")

    lines.extend([
        "",
        "## Fixtures",
        f"- **Slug:** {fixture_info.get('slug', 'N/A')}",
        f"- **Detail Fixture:** {fixture_info.get('detail_fixture') or 'NOT FOUND'}",
        f"- **Listing Fixture:** {fixture_info.get('listing_fixture') or 'NOT FOUND'}",
        f"- **Assertion File:** {fixture_info.get('assertion_file') or 'NOT FOUND'}",
        "",
        "## Description Preview",
        "```",
    ])

    description = job_data.get("description", "")
    if description:
        # Truncate to first 500 chars
        preview = description[:500] + ("..." if len(description) > 500 else "")
        lines.append(preview)
    else:
        lines.append("(No description)")

    lines.extend([
        "```",
        "",
        "## Debugging Commands",
        "",
        "```bash",
        "# Check handler detection",
        f"uv run python -c \"from job_scrape_application.workflows.site_handlers import get_site_handler; h = get_site_handler('{job_data.get('url', '')}'); print(f'Handler: {{h.name if h else None}}')\"",
        "",
        "# Fetch fresh SpiderCloud scrape",
        f"uv run agent_scripts/dump_spidercloud_response.py --url \"{job_data.get('url', '')}\"",
        "",
    ])

    if fixture_info.get("slug"):
        slug = fixture_info["slug"]
        lines.extend([
            "# Run extraction test",
            f"uv run pytest 'tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[{slug}]' -v",
            "",
            "# Check extraction output",
            f"cat ./site-detail-e2e-examples/{slug}_extraction.json | jq .",
        ])

    lines.extend([
        "```",
        "",
        "## Full Job Data (JSON)",
        "```json",
        orjson.dumps(job_data, option=orjson.OPT_INDENT_2, default=str).decode("utf-8"),
        "```",
    ])

    return "\n".join(lines)
def main():
    parser = argparse.ArgumentParser(
        description="Debug job extraction issues for a specific job from Convex prod"
    )
    parser.add_argument(
        "url_or_id",
        help="Job share URL (https://srajob.netlify.app/job/<id>) or just the job ID",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file for the report (default: stdout)",
        default=None,
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON instead of markdown",
    )

    args = parser.parse_args()

    # Extract job ID
    try:
        job_id = extract_job_id(args.url_or_id)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Job ID: {job_id}", file=sys.stderr)

    # Fetch job from Convex
    print("Fetching job from Convex prod...", file=sys.stderr)
    job_data = fetch_job_from_convex(job_id)

    if not job_data:
        print(f"Error: Job not found: {job_id}", file=sys.stderr)
        sys.exit(1)

    # Get handler info
    job_url = job_data.get("url", "")
    handler_info = get_site_handler_info(job_url) if job_url else {"name": None, "error": "No URL"}

    # Find fixtures
    fixture_info = find_fixture(handler_info.get("name", ""))

    # Generate output
    if args.json:
        output = orjson.dumps(
            {
                "job_id": job_id,
                "job_data": job_data,
                "handler_info": handler_info,
                "fixture_info": fixture_info,
            },
            option=orjson.OPT_INDENT_2,
            default=str,
        ).decode("utf-8")
    else:
        output = generate_report(job_id, job_data, handler_info, fixture_info)

    # Write output
    if args.output:
        Path(args.output).write_text(output)
        print(f"Report written to: {args.output}", file=sys.stderr)
    else:
        print(output)
if __name__ == "__main__":
    main()
