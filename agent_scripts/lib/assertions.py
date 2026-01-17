"""Assertion generation utilities for test fixtures.

Provides functions for generating YAML assertion files for both job detail
and listing page tests.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def generate_assertion_yaml(
    job_data: Dict[str, Any],
    detail_url: str,
    handler: str,
    *,
    is_remote_override: bool = False,
) -> str:
    """Generate assertion YAML with actual values from job data.

    Args:
        job_data: Job data dictionary with title, company, location, etc.
        detail_url: Job detail URL
        handler: Handler type (e.g., 'greenhouse', 'ashby')
        is_remote_override: Whether company is in remote_companies.yaml

    Returns:
        YAML assertion content as string
    """
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
        f"site_id: {handler}",
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


def generate_placeholder_assertion_yaml(
    detail_url: str,
    handler: str,
    company_name: str,
) -> str:
    """Generate assertion YAML with TODO placeholders.

    Used when job data is not available (e.g., for new sites).

    Args:
        detail_url: Job detail URL
        handler: Handler type (e.g., 'greenhouse', 'ashby')
        company_name: Company name

    Returns:
        YAML assertion content with TODO placeholders
    """
    lines = [
        "# IMPORTANT_NOTE: Fill in expected values after running extraction test",
        f"site_id: {handler}",
        f"detail_url: {detail_url}",
        "expected:",
        '  title: "TODO"  # Fill in expected title',
        f'  company: "{company_name}"',
        '  location_contains: "TODO"  # Fill in expected location',
        "  is_remote: false  # Set to true if remote job",
        "  level: mid  # junior/mid/senior/staff",
        "  description_min_words: 300",
        '  description_not_contains: \'{\"\'  # Ensure no JSON blocks in description',
        "  cost_milli_cents_min: 1",
        "  posted_at_not_null: true",
    ]

    return "\n".join(lines) + "\n"


def generate_listing_assertion_yaml(
    listing_url: str,
    handler: str,
    *,
    extracted_urls: Optional[List[str]] = None,
    company_name: str = "",
) -> str:
    """Generate listing assertion YAML.

    Args:
        listing_url: Listing page URL
        handler: Handler type (e.g., 'greenhouse', 'ashby')
        extracted_urls: Optional list of extracted job URLs (for documentation)
        company_name: Optional company name

    Returns:
        YAML assertion content for listing page
    """
    handler_class = handler.title() + "Handler" if handler else "Handler"

    lines = [
        "# IMPORTANT_NOTE: ASSERTION SHOULD CONTAIN THE CORRECT EXPECTATION, NOT NECESSARILY WHAT IS EXTRACTED.",
        f"site_id: {handler}",
        f"listing_url: {listing_url}",
        "expected:",
        f"  url_count_min: {len(extracted_urls) if extracted_urls else 5}  # Minimum expected job URLs",
        '  url_pattern: "TODO"  # Regex pattern for valid job URLs (e.g., "/jobs/\\d+")',
        "  no_listing_urls: true  # Ensure listing/search URLs are filtered out",
        f'  handler: "{handler_class}"',
        "  # scraped_urls: Raw URLs extracted from the listing scrape (pre-filter)",
        "  # normalized_urls: Final detail URLs after filtering/normalization",
        "  # apply_urls: Marketing/direct-apply URLs for each normalized URL",
        "  # If normalized_urls is provided, any URL extracted that is NOT in this list will FAIL the test",
        "  # This prevents regressions - invalid URLs sneaking in will be caught immediately",
    ]

    if extracted_urls:
        lines.append("  # --- Extracted URLs (REVIEW EACH ONE - remove invalid URLs) ---")
        lines.append("  # normalized_urls:")
        # Limit to 100 URLs for readability
        for url in extracted_urls[:100]:
            lines.append(f'  #   - "{url}"')
        if len(extracted_urls) > 100:
            lines.append(f"  #   # ... and {len(extracted_urls) - 100} more")
        lines.append("  # apply_urls:")
        for url in extracted_urls[:100]:
            lines.append(f'  #   - "{url}"')
        if len(extracted_urls) > 100:
            lines.append(f"  #   # ... and {len(extracted_urls) - 100} more")

    return "\n".join(lines) + "\n"


def format_yaml_string(value: str) -> str:
    """Format a string value for YAML with proper escaping.

    Args:
        value: String value to format

    Returns:
        Properly escaped YAML string
    """
    # Escape quotes and special characters
    if '"' in value:
        # Use single quotes if double quotes present
        value = value.replace("'", "''")
        return f"'{value}'"

    # Default: use double quotes
    return f'"{value}"'
