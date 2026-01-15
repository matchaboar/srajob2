#!/usr/bin/env python3
"""Generic site operation handler framework.

This framework provides common utilities for site-specific operations like:
- Fetching fixtures from custom APIs (Adobe, Uber, Bloomberg, etc.)
- Validating live site data against Convex
- Checking recent data and cleaning up

Rather than having separate scripts for each site, this provides a framework
for common patterns. Site-specific logic is configured via --site parameter.

Usage:
    # Fetch fixtures for a site
    uv run python agent_scripts/site_operations/generic_site_handler.py \\
        --site adobe \\
        --operation fetch \\
        --output-dir tests/fixtures/

    # Validate live data
    uv run python agent_scripts/site_operations/generic_site_handler.py \\
        --site bloomberg \\
        --operation validate

    # Check recent data
    uv run python agent_scripts/site_operations/generic_site_handler.py \\
        --site snapchat \\
        --operation check \\
        --hours 24

NOTE: This is a framework for future site operations. Existing specialized
scripts (fetch_adobe_phenom_fixtures.py, etc.) remain available for now.
To add a new site, extend the SITE_CONFIGS dictionary below.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional



# Site-specific configuration
# Each site can define custom parameters needed for its operations
SITE_CONFIGS: Dict[str, Dict[str, Any]] = {
    "adobe": {
        "name": "Adobe",
        "listing_url": "https://careers.adobe.com/widgets",
        "handler": "AdobePhenomHandler",
        "api_type": "phenom",
        "supported_operations": ["fetch"],
    },
    "uber": {
        "name": "Uber",
        "listing_url": "https://www.uber.com/api/loadSearchJobsResults",
        "handler": "UberCareersHandler",
        "api_type": "custom",
        "supported_operations": ["fetch"],
    },
    "bloomberg": {
        "name": "Bloomberg",
        "listing_url": "https://bloomberg.avature.net/careers/SearchJobs",
        "handler": "AvatureHandler",
        "api_type": "avature",
        "supported_operations": ["fetch", "validate"],
    },
    "netflix": {
        "name": "Netflix",
        "listing_url": "https://explore.jobs.netflix.net/careers",
        "handler": "NetflixHandler",
        "api_type": "graphql",
        "supported_operations": ["fetch", "validate", "compare"],
    },
    "snapchat": {
        "name": "Snapchat",
        "listing_url": "https://www.snap.com/jobs",
        "handler": "GreenhouseHandler",  # Uses Greenhouse
        "api_type": "greenhouse",
        "supported_operations": ["check"],
    },
}


class SiteOperationHandler:
    """Generic handler for site-specific operations."""

    def __init__(self, site: str, config: Dict[str, Any]):
        """Initialize handler with site configuration.

        Args:
            site: Site identifier (e.g., 'adobe', 'netflix')
            config: Site-specific configuration from SITE_CONFIGS
        """
        self.site = site
        self.config = config
        self.root = Path(__file__).resolve().parent.parent.parent

    async def fetch_fixtures(
        self,
        output_dir: Path,
        *,
        keywords: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Fetch fixtures from site's custom API.

        Args:
            output_dir: Directory to save fixtures
            keywords: Optional search keywords
            limit: Max results to fetch

        Returns:
            Dictionary with fetch results and file paths
        """
        print(f"Fetching fixtures for {self.config['name']}...")
        print(f"  API type: {self.config.get('api_type', 'unknown')}")
        print(f"  Listing URL: {self.config['listing_url']}")

        # Load appropriate handler based on site
        api_type = self.config.get("api_type", "")

        if api_type == "phenom":
            return await self._fetch_phenom(output_dir, keywords, limit)
        elif api_type == "avature":
            return await self._fetch_avature(output_dir, keywords, limit)
        elif api_type == "custom":
            return await self._fetch_custom(output_dir, keywords, limit)
        else:
            raise NotImplementedError(
                f"Fetch not implemented for {self.site} (api_type={api_type}). "
                f"See {self.root / 'agent_scripts' / f'fetch_{self.site}_*.py'} for reference."
            )

    async def validate_live_data(
        self,
        *,
        env: str = "prod",
        sample_size: int = 10,
    ) -> Dict[str, Any]:
        """Validate live site data against Convex database.

        Args:
            env: Environment to validate against ('dev' or 'prod')
            sample_size: Number of jobs to sample for validation

        Returns:
            Dictionary with validation results
        """
        print(f"Validating live data for {self.config['name']}...")
        print(f"  Environment: {env}")
        print(f"  Sample size: {sample_size}")

        raise NotImplementedError(
            f"Validate not implemented for {self.site}. "
            f"See {self.root / 'agent_scripts' / f'validate_{self.site}_*.py'} for reference."
        )

    async def check_recent_data(
        self,
        *,
        hours: float = 24,
        env: str = "prod",
        apply_wipe: bool = False,
    ) -> Dict[str, Any]:
        """Check recent data in Convex and optionally wipe.

        Args:
            hours: Check data from last N hours
            env: Environment to check ('dev' or 'prod')
            apply_wipe: Whether to actually wipe data (default: dry-run)

        Returns:
            Dictionary with check results
        """
        print(f"Checking recent data for {self.config['name']}...")
        print(f"  Time window: {hours} hours")
        print(f"  Environment: {env}")
        print(f"  Apply wipe: {apply_wipe}")

        raise NotImplementedError(
            f"Check not implemented for {self.site}. "
            f"See {self.root / 'agent_scripts' / f'check_{self.site}_*.py'} for reference."
        )

    async def compare_convex_vs_live(
        self,
        *,
        env: str = "prod",
    ) -> Dict[str, Any]:
        """Compare Convex database jobs with live site.

        Args:
            env: Environment to compare ('dev' or 'prod')

        Returns:
            Dictionary with comparison results (missing, extra, matched)
        """
        print(f"Comparing Convex vs live for {self.config['name']}...")
        print(f"  Environment: {env}")

        raise NotImplementedError(
            f"Compare not implemented for {self.site}. "
            f"See {self.root / 'agent_scripts' / f'compare_{self.site}_*.py'} for reference."
        )

    # Site-specific fetch implementations (stubs - extend as needed)

    async def _fetch_phenom(
        self, output_dir: Path, keywords: Optional[str], limit: int
    ) -> Dict[str, Any]:
        """Fetch from Phenom People API (Adobe)."""
        raise NotImplementedError(
            "Phenom fetch not implemented. "
            "See agent_scripts/fetch_adobe_phenom_fixtures.py for implementation."
        )

    async def _fetch_avature(
        self, output_dir: Path, keywords: Optional[str], limit: int
    ) -> Dict[str, Any]:
        """Fetch from Avature API (Bloomberg)."""
        raise NotImplementedError(
            "Avature fetch not implemented. "
            "See agent_scripts/fetch_bloomberg_avature_fixtures.py for implementation."
        )

    async def _fetch_custom(
        self, output_dir: Path, keywords: Optional[str], limit: int
    ) -> Dict[str, Any]:
        """Fetch from custom API."""
        raise NotImplementedError(
            f"Custom API fetch not implemented for {self.site}. "
            f"See site-specific scripts for implementation."
        )


async def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generic site operation handler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch Adobe fixtures
  uv run python %(prog)s --site adobe --operation fetch

  # Validate Bloomberg live data
  uv run python %(prog)s --site bloomberg --operation validate --env prod

  # Check recent Snapchat data
  uv run python %(prog)s --site snapchat --operation check --hours 24

Available sites: {sites}
        """.format(sites=", ".join(sorted(SITE_CONFIGS.keys()))),
    )

    parser.add_argument(
        "--site",
        required=True,
        choices=list(SITE_CONFIGS.keys()),
        help="Site identifier",
    )
    parser.add_argument(
        "--operation",
        required=True,
        choices=["fetch", "validate", "check", "compare"],
        help="Operation to perform",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory for fixtures (fetch operation)",
    )
    parser.add_argument(
        "--keywords",
        help="Search keywords (fetch operation)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max results to fetch (default: 100)",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="prod",
        help="Environment (default: prod)",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=24,
        help="Time window in hours (default: 24)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (default: dry-run)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="Sample size for validation (default: 10)",
    )

    args = parser.parse_args()

    # Get site configuration
    config = SITE_CONFIGS[args.site]

    # Check if operation is supported for this site
    supported_ops = config.get("supported_operations", [])
    if args.operation not in supported_ops:
        print(
            f"Error: Operation '{args.operation}' not supported for {config['name']}",
            file=sys.stderr,
        )
        print(f"Supported operations: {', '.join(supported_ops)}", file=sys.stderr)
        return 1

    # Create handler
    handler = SiteOperationHandler(args.site, config)

    # Execute operation
    try:
        if args.operation == "fetch":
            output_dir = args.output_dir or Path.cwd() / "fixtures"
            result = await handler.fetch_fixtures(
                output_dir,
                keywords=args.keywords,
                limit=args.limit,
            )
        elif args.operation == "validate":
            result = await handler.validate_live_data(
                env=args.env,
                sample_size=args.sample_size,
            )
        elif args.operation == "check":
            result = await handler.check_recent_data(
                hours=args.hours,
                env=args.env,
                apply_wipe=args.apply,
            )
        elif args.operation == "compare":
            result = await handler.compare_convex_vs_live(env=args.env)
        else:
            print(f"Unknown operation: {args.operation}", file=sys.stderr)
            return 1

        # Output results
        print("\n=== Results ===")
        print(json.dumps(result, indent=2))
        return 0

    except NotImplementedError as e:
        print(f"\nNot implemented: {e}", file=sys.stderr)
        print("\nFor now, use the existing site-specific script.", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
