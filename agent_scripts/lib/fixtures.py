"""Fixture generation and management utilities.

Provides functions for saving and loading SpiderCloud test fixtures.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


def save_fixture(
    fixture_data: Dict[str, Any],
    output_path: Path,
) -> None:
    """Save fixture data to JSON file with consistent formatting.

    Args:
        fixture_data: Fixture dictionary with 'request' and 'response' keys
        output_path: Path to write fixture file

    Raises:
        ValueError: If fixture_data is missing required keys
    """
    if not isinstance(fixture_data, dict):
        raise ValueError("fixture_data must be a dictionary")

    if "response" not in fixture_data:
        raise ValueError("fixture_data must have 'response' key")

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write with consistent formatting
    output_path.write_text(
        json.dumps(fixture_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_fixture(
    fixture_path: Path,
    *,
    validate: bool = True,
) -> Dict[str, Any]:
    """Load fixture from JSON file with optional validation.

    Args:
        fixture_path: Path to fixture file
        validate: Whether to validate fixture structure

    Returns:
        Fixture data dictionary

    Raises:
        FileNotFoundError: If fixture file doesn't exist
        ValueError: If fixture is invalid and validate=True
        json.JSONDecodeError: If fixture is not valid JSON
    """
    if not fixture_path.exists():
        raise FileNotFoundError(f"Fixture not found: {fixture_path}")

    fixture_data = json.loads(fixture_path.read_text(encoding="utf-8"))

    if validate:
        if not isinstance(fixture_data, dict):
            raise ValueError(f"Fixture must be a dictionary: {fixture_path}")

        if "response" not in fixture_data:
            raise ValueError(f"Fixture missing 'response' key: {fixture_path}")

    return fixture_data


def build_fixture_structure(
    url: str,
    response: Any,
    *,
    source_url: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    endpoint: str = "/scrape",
) -> Dict[str, Any]:
    """Build standard fixture structure.

    Args:
        url: URL that was scraped
        response: SpiderCloud response data
        source_url: Optional source/listing URL
        params: Optional SpiderCloud params used
        endpoint: API endpoint (default: /scrape)

    Returns:
        Fixture dictionary with request and response sections
    """
    request_data: Dict[str, Any] = {
        "endpoint": endpoint,
        "url": url,
    }

    if source_url:
        request_data["source_url"] = source_url

    if params:
        request_data["params"] = params

    return {
        "request": request_data,
        "response": response,
    }


def get_fixture_paths(
    company_slug: str,
    handler: str,
    *,
    timestamp: Optional[str] = None,
    fixture_type: str = "debug",
) -> tuple[Path, Path, Path, Path]:
    """Get standard fixture and assertion paths.

    Args:
        company_slug: Company identifier (e.g., 'pure_storage')
        handler: Handler type (e.g., 'greenhouse')
        timestamp: Optional timestamp for unique fixtures (e.g., '20260115T120000')
        fixture_type: Type of fixture ('debug' or 'dbos_schedule', default: 'debug')

    Returns:
        Tuple of (listing_fixture_path, detail_fixture_path,
                 listing_assertion_path, detail_assertion_path)
    """
    from pathlib import Path as P

    root = P(__file__).resolve().parent.parent.parent

    if fixture_type == "debug":
        fixture_dir = root / f"tests/job_scrape_application/workflows/fixtures/debug/{company_slug}"
        assertion_dir = root / f"tests/job_scrape_application/workflows/assertions/debug/{company_slug}"
    elif fixture_type == "dbos_schedule":
        fixture_dir = root / "tests/job_scrape_application/workflows/fixtures/dbos_schedule"
        assertion_dir = root / "tests/job_scrape_application/workflows/assertions"
    else:
        raise ValueError(f"Unknown fixture_type: {fixture_type}")

    # Build filename components
    if timestamp:
        listing_name = f"{handler}_{timestamp}_listing"
        detail_name = f"{handler}_{timestamp}_detail"
    else:
        listing_name = f"{company_slug}_listing"
        detail_name = f"{company_slug}_detail"

    # Build paths
    listing_fixture = fixture_dir / f"{listing_name}.json"
    detail_fixture = fixture_dir / f"{detail_name}.json"

    if fixture_type == "debug":
        listing_assertion = assertion_dir / f"{listing_name}.yml"
        detail_assertion = assertion_dir / f"{detail_name}.yml"
    else:
        # dbos_schedule assertions use company slug
        listing_assertion = assertion_dir / f"{company_slug}_listing.yml"
        detail_assertion = assertion_dir / f"{company_slug}.yml"

    return listing_fixture, detail_fixture, listing_assertion, detail_assertion
