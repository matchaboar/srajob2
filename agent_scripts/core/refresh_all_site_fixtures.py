#!/usr/bin/env python3
"""
Regenerate timestamped fixtures for all sites in site_schedules.yml.

This script generates NEW timestamped fixtures (doesn't overwrite existing ones)
for both listing and detail pages, along with assertion templates.

Uses the same extraction logic as fetch_spidercloud_fixtures.py (production workflow code).

Usage:
    # All sites from prod schedule
    uv run python agent_scripts/refresh_all_site_fixtures.py --schedule-env prod

    # Specific sites only
    uv run python agent_scripts/refresh_all_site_fixtures.py --schedule-env prod --only airbnb purestorage

    # Limited number of sites
    uv run python agent_scripts/refresh_all_site_fixtures.py --schedule-env prod --limit 5
"""

from __future__ import annotations

import argparse
import asyncio
import orjson
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import yaml
from dotenv import load_dotenv

# Import shared utilities from lib
from agent_scripts.lib import (
    generate_listing_assertion_yaml,
    generate_placeholder_assertion_yaml,
)

# Import the existing fixture generation logic
from agent_scripts.core.fetch_spidercloud_fixtures import (
    _capture_workflow_scrape,
    _extract_listing_job_urls,
)
from job_scrape_application.config import get_env_dir
from job_scrape_application.workflows.site_handlers import get_site_handler

ROOT = Path(__file__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEBUG_FIXTURE_DIR = ROOT / "tests/job_scrape_application/workflows/fixtures/debug"
DEBUG_ASSERTIONS_DIR = ROOT / "tests/job_scrape_application/workflows/assertions/debug"
def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return cleaned.strip("_") or "site"
def _load_schedule_entries(env: str) -> List[Dict[str, Any]]:
    schedule_path = get_env_dir(env) / "site_schedules.yml"
    if not schedule_path.exists():
        raise SystemExit(f"Missing site schedule config: {schedule_path}")
    payload = yaml.safe_load(schedule_path.read_text(encoding="utf-8")) or {}
    entries = payload if isinstance(payload, list) else payload.get("site_schedules", [])
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict) and entry.get("enabled", True)]
def _extract_handler_name(url: str) -> str:
    """Extract handler name from URL for file naming."""
    handler = get_site_handler(url)
    if handler:
        return handler.name.lower()

    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    if "greenhouse.io" in domain:
        return "greenhouse"
    elif "ashbyhq.com" in domain:
        return "ashbyhq"
    elif "workday.com" in domain or "myworkdayjobs.com" in domain:
        return "workday"
    elif "lever.co" in domain:
        return "lever"

    # Fallback to first part of domain
    parts = domain.replace("www.", "").split(".")
    return parts[0] if parts else "unknown"


async def process_site(
    entry: Dict[str, Any],
    timestamp: str,
) -> Optional[Dict[str, Any]]:
    """Process a single site - generate listing and detail fixtures.

    Uses the same extraction logic as fetch_spidercloud_fixtures.py.
    """
    url = entry.get("url")
    if not isinstance(url, str) or not url.strip():
        return None

    company_name = str(entry.get("name") or "")
    company_slug = _slugify(company_name or url)
    handler_name = _extract_handler_name(url)
    pattern = entry.get("pattern") if isinstance(entry.get("pattern"), str) else None

    logger.info(f"Processing {company_name or url}...")

    # Create output directories
    fixture_dir = DEBUG_FIXTURE_DIR / company_slug
    assertion_dir = DEBUG_ASSERTIONS_DIR / company_slug
    fixture_dir.mkdir(parents=True, exist_ok=True)
    assertion_dir.mkdir(parents=True, exist_ok=True)

    # Generate paths with timestamp
    listing_fixture_path = fixture_dir / f"{handler_name}_{timestamp}_listing.json"
    detail_fixture_path = fixture_dir / f"{handler_name}_{timestamp}_detail.json"
    listing_assertion_path = assertion_dir / f"{handler_name}_{timestamp}_listing.yml"
    detail_assertion_path = assertion_dir / f"{handler_name}_{timestamp}_detail.yml"

    try:
        # Fetch listing page using the same logic as fetch_spidercloud_fixtures.py
        logger.info(f"  Fetching listing: {url}")
        listing_capture, listing_payload = await _capture_workflow_scrape(
            url,
            source_url=url,
            pattern=pattern,
            label="listing",
        )

        # Save listing fixture
        listing_fixture_path.write_text(
            orjson.dumps(listing_capture, option=orjson.OPT_INDENT_2).decode("utf-8"),
            encoding="utf-8",
        )

        # Extract URLs using the same logic as fetch_spidercloud_fixtures.py
        extracted_urls = _extract_listing_job_urls(listing_payload, url, pattern)
        if not extracted_urls:
            # Try from raw response
            raw_response = listing_capture.get("response")
            if raw_response:
                extracted_urls = _extract_listing_job_urls(
                    {"items": {"raw": raw_response}, "sourceUrl": url},
                    url,
                    pattern,
                )

        # Filter out the listing URL itself
        listing_url_key = url.rstrip("/").lower()
        extracted_urls = [
            job_url
            for job_url in extracted_urls
            if isinstance(job_url, str) and job_url.rstrip("/").lower() != listing_url_key
        ]

        logger.info(f"  Extracted {len(extracted_urls)} URLs from listing")

        # Generate listing assertion
        listing_assertion = generate_listing_assertion_yaml(
            url,
            handler_name,
            extracted_urls=extracted_urls,
            company_name=company_name
        )
        listing_assertion_path.write_text(listing_assertion)

        # Select a detail URL (same logic as fetch_spidercloud_fixtures.py)
        detail_url = None
        handler = get_site_handler(url)

        from job_scrape_application.workflows import activities as workflow_activities

        for candidate in extracted_urls:
            if not isinstance(candidate, str) or not candidate.strip():
                continue
            normalized = (
                candidate
                if candidate.startswith(("http://", "https://"))
                else urljoin(url, candidate)
            )
            candidate_handler = handler or get_site_handler(normalized)
            if candidate_handler:
                if candidate_handler.is_listing_url(normalized):
                    continue
            elif workflow_activities._is_probable_listing_url(normalized):
                continue
            detail_url = normalized
            break

        if detail_url is None and extracted_urls:
            detail_url = extracted_urls[0]
            if not detail_url.startswith(("http://", "https://")):
                detail_url = urljoin(url, detail_url)

        if detail_url:
            # Transform to API URL if needed (e.g., Greenhouse marketing -> API)
            handler = get_site_handler(detail_url) or handler
            if handler and handler.name == "greenhouse":
                api_url = handler.get_api_uri(detail_url, source_url=url)
                if api_url:
                    detail_url = api_url

            # Fetch detail page
            logger.info(f"  Fetching detail: {detail_url}")
            try:
                detail_capture, _ = await _capture_workflow_scrape(
                    detail_url,
                    source_url=url,
                    pattern=pattern,
                    label="detail",
                )
                detail_fixture_path.write_text(
                    orjson.dumps(detail_capture, option=orjson.OPT_INDENT_2).decode("utf-8"),
                    encoding="utf-8",
                )
                # Generate detail assertion
                detail_assertion = generate_placeholder_assertion_yaml(detail_url, handler_name, company_name)
                detail_assertion_path.write_text(detail_assertion)
            except Exception as e:
                logger.warning(f"  Failed to fetch detail page: {e}")
                detail_fixture_path = None
                detail_assertion_path = None
        else:
            logger.warning("  No detail URL found")
            detail_fixture_path = None
            detail_assertion_path = None

    except Exception as e:
        logger.error(f"  Failed to process {company_name}: {e}")
        return None

    identifier = f"{company_slug}/{handler_name}_{timestamp}"

    return {
        "company": company_name,
        "company_slug": company_slug,
        "handler": handler_name,
        "identifier": identifier,
        "listing_url": url,
        "detail_url": detail_url,
        "fixture_listing_path": str(listing_fixture_path),
        "fixture_detail_path": str(detail_fixture_path) if detail_fixture_path else None,
        "assertion_listing_path": str(listing_assertion_path),
        "assertion_detail_path": str(detail_assertion_path) if detail_assertion_path else None,
        "extracted_url_count": len(extracted_urls),
    }
async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Regenerate timestamped fixtures for all sites in site_schedules.yml"
    )
    parser.add_argument(
        "--schedule-env",
        choices=["dev", "prod"],
        required=True,
        help="Schedule environment to use",
    )
    parser.add_argument(
        "--only",
        nargs="*",
        help="Site names/slugs to include (space-separated)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Max number of sites to process",
    )
    parser.add_argument(
        "--output-format",
        choices=["human", "json"],
        default="human",
        help="Output format (default: human)",
    )
    args = parser.parse_args()

    # Disable logging output if JSON mode (keep stdout clean)
    if args.output_format == "json":
        logging.disable(logging.CRITICAL)

    # Load environment variables (needed for SPIDER_API_KEY)
    load_dotenv()
    load_dotenv(ROOT / "job_board_application/.env.production", override=False)

    api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")
    if not api_key:
        raise SystemExit("SPIDER_API_KEY (or SPIDER_KEY) is not set in environment/.env")

    # Load site schedules
    entries = _load_schedule_entries(args.schedule_env)
    logger.info(f"Loaded {len(entries)} sites from {args.schedule_env} schedule")

    # Filter by --only
    if args.only:
        allow = {_slugify(val) for val in args.only}
        entries = [
            entry
            for entry in entries
            if _slugify(str(entry.get("name") or entry.get("url") or "")) in allow
        ]
        logger.info(f"Filtered to {len(entries)} sites")

    # Apply limit
    if args.limit:
        entries = entries[:args.limit]
        logger.info(f"Limited to {len(entries)} sites")

    if not entries:
        logger.warning("No sites to process")
        if args.output_format == "json":
            print(
                orjson.dumps(
                    {"generated_count": 0, "fixtures": []},
                    option=orjson.OPT_INDENT_2,
                ).decode("utf-8")
            )
        else:
            print("\n=== JSON Output ===")
            print(
                orjson.dumps(
                    {"generated_count": 0, "fixtures": []},
                    option=orjson.OPT_INDENT_2,
                ).decode("utf-8")
            )
        return 0

    # Generate timestamp for this batch
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    logger.info(f"Using timestamp: {timestamp}")

    # Process all sites
    results = []
    for entry in entries:
        result = await process_site(entry, timestamp)
        if result:
            results.append(result)

    logger.info(f"\nGenerated {len(results)} fixture sets")

    # Output JSON for script integration
    output = {
        "timestamp": timestamp,
        "generated_count": len(results),
        "fixtures": results,
    }

    if args.output_format == "json":
        print(orjson.dumps(output, option=orjson.OPT_INDENT_2).decode("utf-8"))
    else:
        print("\n=== JSON Output ===")
        print(orjson.dumps(output, option=orjson.OPT_INDENT_2).decode("utf-8"))

    return 0
if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
