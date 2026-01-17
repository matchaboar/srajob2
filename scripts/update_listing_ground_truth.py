#!/usr/bin/env python3
"""Update listing ground_truth files with exact URLs from fixtures.

This script processes all listing fixtures and updates their corresponding
ground_truth files to contain the exact list of valid job detail URLs.

Usage:
    uv run python scripts/update_listing_ground_truth.py
    uv run python scripts/update_listing_ground_truth.py --dry-run
    uv run python scripts/update_listing_ground_truth.py --site airbnb
"""

from __future__ import annotations

import argparse
import html as html_lib
import orjson
import logging
import re
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import yaml

from job_scrape_application.workflows.helpers.link_extractors import normalize_url
from job_scrape_application.workflows.site_handlers import get_site_handler

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Paths
DEBUG_FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures/debug")
DEBUG_GROUND_TRUTH_DIR = Path("tests/job_scrape_application/workflows/ground_truth/debug")


def load_fixture(fixture_path: Path) -> dict[str, Any]:
    """Load a fixture JSON file."""
    return orjson.loads(fixture_path.read_text(encoding="utf-8"))


def extract_content_from_fixture(fixture_data: dict[str, Any]) -> tuple[str, str, str]:
    """Extract raw content from fixture.

    Returns:
        Tuple of (commonmark_content, raw_html_content, content_type)
    """
    commonmark = ""
    raw_html = ""
    content_type = "unknown"

    response = fixture_data.get("response", [])
    if isinstance(response, list) and response:
        first_item = response[0]
        if isinstance(first_item, str):
            # JSONL format - parse the JSON string
            try:
                parsed = orjson.loads(first_item)
                content_dict = parsed.get("content", {})
                commonmark = content_dict.get("commonmark", "")
                raw_html = content_dict.get("raw", "")
                if commonmark:
                    content_type = "commonmark"
                elif raw_html:
                    content_type = "raw_html"
            except orjson.JSONDecodeError:
                commonmark = first_item
                content_type = "raw_string"
        elif isinstance(first_item, dict):
            content_dict = first_item.get("content", {})
            commonmark = content_dict.get("commonmark", "")
            raw_html = content_dict.get("raw", "")
            if commonmark:
                content_type = "commonmark"
            elif raw_html:
                content_type = "raw_html"
        elif isinstance(first_item, list) and first_item:
            nested = first_item[0] if first_item else {}
            if isinstance(nested, dict):
                content_dict = nested.get("content", {})
                commonmark = content_dict.get("commonmark", "")
                raw_html = content_dict.get("raw", "")
                if commonmark:
                    content_type = "commonmark"
                elif raw_html:
                    content_type = "raw_html"

    return commonmark, raw_html, content_type


def extract_spidercloud_links(fixture_data: dict[str, Any], listing_url: str) -> list[str]:
    """Extract links from SpiderCloud response (for JS-rendered SPAs like Workday)."""
    urls = []
    response = fixture_data.get("response", [])
    if isinstance(response, list) and response:
        first_item = response[0]
        if isinstance(first_item, dict):
            page_links = first_item.get("links", [])
            if isinstance(page_links, list):
                for link in page_links:
                    href = link.get("href") if isinstance(link, dict) else link
                    if not href:
                        continue
                    # Unescape HTML entities
                    href = html_lib.unescape(href)
                    # Resolve relative URLs
                    if not href.startswith(("http://", "https://")):
                        parsed = listing_url.split("/")
                        if len(parsed) >= 3:
                            origin = "/".join(parsed[:3])
                            href = urljoin(origin, href)
                    if href and href not in urls:
                        urls.append(href)
    return urls


def extract_urls_from_fixture(fixture_path: Path) -> tuple[list[str], str, str, str]:
    """Extract valid job detail URLs from a listing fixture.

    Returns:
        Tuple of (urls, handler_name, listing_url, source_url)
    """
    fixture_data = load_fixture(fixture_path)
    request = fixture_data.get("request", {})
    listing_url = request.get("url", "")
    source_url = request.get("source_url", listing_url)

    # Get handler
    handler = get_site_handler(listing_url) or get_site_handler(source_url)
    handler_name = type(handler).__name__ if handler else "BaseHandler"

    # Extract raw content
    commonmark, raw_html, content_type = extract_content_from_fixture(fixture_data)

    # Try to extract URLs
    extracted_urls: list[str] = []

    # Check if content is wrapped in markdown code blocks and extract JSON
    content_to_parse = commonmark.strip()
    if content_to_parse.startswith("```"):
        lines = content_to_parse.split("\n")
        json_lines = []
        in_block = False
        for line in lines:
            if line.strip() in ("```", "```json"):
                in_block = not in_block
                continue
            if in_block:
                json_lines.append(line)
        content_to_parse = "\n".join(json_lines).strip()

    # First try parsing as raw JSON (for API responses like Greenhouse, Netflix)
    json_payload: Optional[Any] = None

    # Try commonmark first
    if content_to_parse and content_to_parse.startswith(("{", "[")):
        try:
            json_payload = orjson.loads(content_to_parse)
        except orjson.JSONDecodeError:
            pass

    # Try raw_html if commonmark didn't work - this is where Greenhouse JSON often is
    if not json_payload and raw_html:
        raw_html_stripped = raw_html.strip()
        if raw_html_stripped.startswith(("{", "[")):
            try:
                json_payload = orjson.loads(raw_html_stripped)
            except orjson.JSONDecodeError:
                pass

        # Also try extracting from <pre> tags
        if not json_payload:
            pre_match = re.search(r"<pre>(.+?)</pre>", raw_html, re.DOTALL)
            if pre_match:
                try:
                    json_payload = orjson.loads(pre_match.group(1))
                except orjson.JSONDecodeError:
                    pass

    if json_payload and handler and hasattr(handler, "get_links_from_json"):
        extracted_urls = handler.get_links_from_json(json_payload)

    # Fall back to HTML parsing if JSON didn't work
    if not extracted_urls and handler and hasattr(handler, "get_links_from_raw_html"):
        html_content = raw_html if raw_html else commonmark
        extracted_urls = handler.get_links_from_raw_html(html_content)

    # Also check SpiderCloud-extracted links (for JS-rendered SPAs)
    if not extracted_urls:
        extracted_urls = extract_spidercloud_links(fixture_data, listing_url)

    # Apply handler filters
    urls_to_filter = extracted_urls
    if handler and hasattr(handler, "filter_job_urls_for_site"):
        urls_to_filter = handler.filter_job_urls_for_site(extracted_urls, source_url)

    if handler and hasattr(handler, "filter_job_urls"):
        filtered_urls = handler.filter_job_urls(urls_to_filter)
    else:
        filtered_urls = urls_to_filter

    # Separate detail URLs from pagination URLs
    detail_urls = []
    for url in filtered_urls:
        if handler and hasattr(handler, "is_listing_url") and handler.is_listing_url(url):
            continue  # Skip pagination URLs
        detail_urls.append(url)

    # Apply API transformation for handlers that support it (e.g., Greenhouse)
    should_transform_to_api = (
        handler is not None
        and hasattr(handler, "get_api_uri")
        and hasattr(handler, "supports_detail_api")
        and getattr(handler, "supports_detail_api", False)
    )

    if should_transform_to_api:
        api_urls = []
        for url in detail_urls:
            try:
                api_url = handler.get_api_uri(url, source_url=source_url)
            except TypeError:
                try:
                    api_url = handler.get_api_uri(url)
                except Exception:
                    api_url = url
            api_urls.append(api_url if api_url else url)
        detail_urls = api_urls

    # Normalize URLs
    normalized_urls: list[str] = []
    seen: set[str] = set()
    for url in detail_urls:
        normalized = normalize_url(url, base_url=listing_url)
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_urls.append(normalized)

    return normalized_urls, handler_name, listing_url, source_url


def infer_url_pattern_from_urls(urls: list[str]) -> Optional[str]:
    """Infer a URL pattern regex from the extracted URLs.

    This looks at the actual extracted URLs and creates a pattern that matches them all.
    """
    if not urls:
        return None

    # Analyze the first URL to determine the pattern type
    first_url = urls[0]

    # Greenhouse API URLs
    if "boards-api.greenhouse.io" in first_url:
        return r"https://boards-api\.greenhouse\.io/v1/boards/[^/]+/jobs/\d+"

    # Ashby URLs
    if "jobs.ashbyhq.com" in first_url:
        return r"https://jobs\.ashbyhq\.com/[^/]+/[a-f0-9-]+"

    # Netflix API URLs
    if "explore.jobs.netflix.net/api" in first_url:
        return r"https://explore\.jobs\.netflix\.net/api/apply/v2/jobs/\d+"

    # Netflix careers URLs (explore.jobs.netflix.net/careers/job/...)
    if "explore.jobs.netflix.net/careers/job" in first_url:
        return r"https://explore\.jobs\.netflix\.net/careers/job/\d+"

    # Netflix jobs page URLs
    if "jobs.netflix.com" in first_url:
        return r"https://jobs\.netflix\.com/jobs/\d+"

    # Kula/careers.kula.ai URLs
    if "careers.kula.ai" in first_url:
        return r"https://careers\.kula\.ai/[^/]+/\d+"

    # Kula jobs.kula.app URLs
    if "jobs.kula.app" in first_url:
        return r"https://jobs\.kula\.app/[^/]+/jobs/[a-f0-9-]+"

    # Workday CXS API URLs (the actual job detail URLs)
    if ".myworkdayjobs.com/wday/cxs/" in first_url:
        # Extract the domain pattern
        match = re.match(r"https://([^.]+)\.wd\d+\.myworkdayjobs\.com/", first_url)
        if match:
            company = match.group(1)
            return rf"https://{company}\.wd\d+\.myworkdayjobs\.com/wday/cxs/{company}/[^/]+/job/.+"

    # Standard Workday job URLs
    if ".myworkdayjobs.com" in first_url:
        return r"https://[^/]+\.wd\d+\.myworkdayjobs\.com/[^/]+/job/.+"

    # Pinterest careers (Greenhouse-based but custom domain)
    if "pinterestcareers.com" in first_url:
        return r"https://www\.pinterestcareers\.com/jobs\?gh_jid=\d+"

    # Generic Greenhouse marketing URLs (embedded on company sites)
    if "gh_jid=" in first_url:
        # Extract base URL pattern
        match = re.match(r"(https://[^/]+)/", first_url)
        if match:
            domain = match.group(1).replace(".", r"\.")
            return rf"{domain}/.+\?gh_jid=\d+"

    return None


def get_url_pattern_for_handler(handler_name: str, source_url: str, urls: list[str]) -> Optional[str]:
    """Get the expected URL pattern regex for a handler.

    First tries to infer from actual URLs, then falls back to handler-based patterns.
    """
    # First try to infer from actual URLs
    inferred = infer_url_pattern_from_urls(urls)
    if inferred:
        return inferred

    # Fall back to handler-based patterns
    patterns = {
        "GreenhouseHandler": r"https://boards-api\.greenhouse\.io/v1/boards/[^/]+/jobs/\d+",
        "AshbyHqHandler": r"https://jobs\.ashbyhq\.com/[^/]+/[a-f0-9-]+",
        "KulaCareersHandler": r"https://careers\.kula\.ai/[^/]+/\d+",
        "WorkdayHandler": r"https://[^/]+\.wd\d+\.myworkdayjobs\.com/.+",
        "NetflixHandler": r"https://jobs\.netflix\.com/jobs/\d+",
    }

    return patterns.get(handler_name)


def identify_blocked_urls(all_urls: list[str], valid_urls: list[str], handler_name: str) -> list[str]:
    """Identify URLs that should be blocked (application forms, etc.)."""
    blocked = []

    # Common blocked patterns
    blocked_suffixes = ["/application", "/apply", "/login", "/register"]

    for url in all_urls:
        # Skip if it's in the valid URLs list
        if url in valid_urls:
            continue

        # Check for blocked suffixes
        url_lower = url.lower()
        for suffix in blocked_suffixes:
            if url_lower.endswith(suffix):
                blocked.append(url)
                break

    return blocked


def update_ground_truth_file(
    ground_truth_path: Path,
    urls: list[str],
    handler_name: str,
    listing_url: str,
    source_url: str,
    blocked_urls: list[str],
    dry_run: bool = False,
) -> bool:
    """Update a ground_truth file with the extracted URLs."""
    # Load existing ground_truth if it exists
    existing = {}
    if ground_truth_path.exists():
        try:
            existing = yaml.safe_load(ground_truth_path.read_text(encoding="utf-8")) or {}
        except Exception:
            pass

    # Determine site_id from fixture name or existing
    site_id = existing.get("site_id", "")
    if not site_id:
        # Try to infer from handler or source URL
        if "greenhouse" in handler_name.lower():
            site_id = "greenhouse"
        elif "ashby" in handler_name.lower():
            site_id = "ashby"
        elif "workday" in handler_name.lower():
            site_id = "workday"
        elif "kula" in handler_name.lower():
            site_id = "kula"
        else:
            site_id = handler_name.replace("Handler", "").lower()

    # Get URL pattern
    url_pattern = get_url_pattern_for_handler(handler_name, source_url, urls)

    # Build the ground_truth content
    content = {
        "site_id": site_id,
        "listing_url": listing_url,
        "expected": {
            "url_count": len(urls),
            "no_listing_urls": True,
            "handler": handler_name,
            "expected_urls": sorted(urls),  # Sort for consistent diffs
        },
    }

    # Add URL pattern if available
    if url_pattern:
        content["expected"]["url_pattern"] = url_pattern

    # Add blocked URLs if any
    if blocked_urls:
        content["expected"]["blocked_urls"] = sorted(blocked_urls)

    # Preserve cost_milli_cents_min if it exists
    if existing.get("expected", {}).get("cost_milli_cents_min"):
        content["expected"]["cost_milli_cents_min"] = existing["expected"]["cost_milli_cents_min"]
    else:
        content["expected"]["cost_milli_cents_min"] = 1

    # Generate YAML with custom formatting
    header = (
        f"# These are asserted to be the ground truth based on actual contents of the fixture: "
        f"fixtures/debug/{ground_truth_path.parent.name}/{ground_truth_path.stem.replace('_listing', '')}_listing.json\n"
        "# IMPORTANT_NOTE: ASSERTION SHOULD CONTAIN THE CORRECT EXPECTATION, NOT NECESSARILY WHAT IS EXTRACTED.\n"
    )

    # Custom YAML formatting to keep expected_urls readable
    yaml_content = yaml.dump(content, default_flow_style=False, allow_unicode=True, sort_keys=False)

    final_content = header + yaml_content

    if dry_run:
        logger.info(f"Would write {len(urls)} URLs to {ground_truth_path}")
        return True

    # Create parent directory if needed
    ground_truth_path.parent.mkdir(parents=True, exist_ok=True)

    # Write the file
    ground_truth_path.write_text(final_content, encoding="utf-8")
    logger.info(f"Updated {ground_truth_path} with {len(urls)} URLs")

    return True


def discover_listing_fixtures() -> list[tuple[str, Path, Path]]:
    """Discover all listing fixtures and their ground_truth paths.

    Returns:
        List of (identifier, fixture_path, ground_truth_path) tuples
    """
    fixtures = []

    if not DEBUG_FIXTURE_DIR.exists():
        logger.warning(f"Fixture directory not found: {DEBUG_FIXTURE_DIR}")
        return fixtures

    # Walk through company folders
    for company_dir in sorted(DEBUG_FIXTURE_DIR.iterdir()):
        if not company_dir.is_dir() or company_dir.name.startswith("."):
            continue

        for fixture_path in sorted(company_dir.glob("*_listing.json")):
            # Build identifier from filename
            stem = fixture_path.stem.replace("_listing", "")
            identifier = f"{company_dir.name}/{stem}"

            # Build ground_truth path
            ground_truth_path = DEBUG_GROUND_TRUTH_DIR / company_dir.name / f"{stem}_listing.yml"

            fixtures.append((identifier, fixture_path, ground_truth_path))

    return fixtures


def main():
    parser = argparse.ArgumentParser(description="Update listing ground_truth files")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually write files")
    parser.add_argument("--site", type=str, help="Only process fixtures for this site")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    fixtures = discover_listing_fixtures()
    logger.info(f"Found {len(fixtures)} listing fixtures")

    updated = 0
    errors = 0

    for identifier, fixture_path, ground_truth_path in fixtures:
        # Filter by site if specified
        if args.site and args.site.lower() not in identifier.lower():
            continue

        logger.info(f"Processing {identifier}...")

        try:
            urls, handler_name, listing_url, source_url = extract_urls_from_fixture(fixture_path)

            if not urls:
                logger.warning(f"  No URLs extracted from {identifier}")
                continue

            # Identify blocked URLs (we'd need to load raw extraction for this)
            blocked_urls: list[str] = []

            success = update_ground_truth_file(
                ground_truth_path,
                urls,
                handler_name,
                listing_url,
                source_url,
                blocked_urls,
                dry_run=args.dry_run,
            )

            if success:
                updated += 1
                logger.info(f"  {handler_name}: {len(urls)} URLs")
            else:
                errors += 1

        except Exception as e:
            logger.error(f"  Error processing {identifier}: {e}")
            errors += 1
            if args.verbose:
                import traceback
                traceback.print_exc()

    logger.info(f"\nSummary: Updated {updated} files, {errors} errors")

    if args.dry_run:
        logger.info("(Dry run - no files were actually modified)")


if __name__ == "__main__":
    main()
