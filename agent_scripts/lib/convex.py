"""Convex database client utilities.

Provides functions for querying Convex production database for job and site data.
"""

from __future__ import annotations

import orjson
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional


def extract_job_id_from_url(url_or_id: str) -> str:
    """Extract Convex job ID from share URL or return ID as-is.

    Args:
        url_or_id: Share URL (e.g., https://srajob.netlify.app/job/k57abc123)
                  or direct job ID (e.g., k57abc123)

    Returns:
        Convex job ID (e.g., k57abc123)

    Examples:
        >>> extract_job_id_from_url("https://srajob.netlify.app/job/k57abc123")
        'k57abc123'
        >>> extract_job_id_from_url("k57abc123")
        'k57abc123'
    """
    # If it looks like a URL, extract ID from path
    if url_or_id.startswith("http://") or url_or_id.startswith("https://"):
        match = re.search(r"/job/([a-z0-9]+)", url_or_id, re.I)
        if match:
            return match.group(1)
        # Fallback: try to get last path segment
        parts = url_or_id.rstrip("/").split("/")
        if parts:
            return parts[-1]

    # Already an ID
    return url_or_id


def fetch_job_by_id(
    job_id: str,
    *,
    env: str = "prod",
) -> Optional[Dict[str, Any]]:
    """Fetch job data from Convex by job ID.

    Args:
        job_id: Convex job ID (e.g., k57abc123)
        env: Environment ('prod' or 'dev', default: 'prod')

    Returns:
        Job data dictionary, or None if job not found or error occurred

    Raises:
        RuntimeError: If convex CLI not available
    """
    root = Path(__file__).resolve().parent.parent.parent
    convex_dir = root / "job_board_application"

    if not convex_dir.exists():
        raise RuntimeError(f"Convex directory not found: {convex_dir}")

    # Build convex command
    cmd = ["npx", "convex", "run"]

    if env == "prod":
        cmd.append("--prod")
    # For dev, no flag needed (uses local deployment)

    cmd.extend([
        "jobs:getJobById",
        orjson.dumps({"id": job_id}).decode("utf-8"),
    ])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=convex_dir,
            timeout=30,
        )

        if result.returncode != 0:
            print(f"Convex query failed: {result.stderr}", file=sys.stderr)
            return None

        # Parse JSON output
        output = result.stdout.strip()
        if not output or output == "null":
            print(f"Job not found: {job_id}", file=sys.stderr)
            return None

        data = orjson.loads(output)
        return data if isinstance(data, dict) else None

    except subprocess.TimeoutExpired:
        print(f"Convex query timed out for job: {job_id}", file=sys.stderr)
        return None
    except orjson.JSONDecodeError as e:
        print(f"Failed to parse Convex response: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Failed to fetch job from Convex: {e}", file=sys.stderr)
        return None


def fetch_site_by_id(
    site_id: str,
    *,
    env: str = "prod",
) -> Optional[Dict[str, Any]]:
    """Fetch site data from Convex by site ID.

    Args:
        site_id: Convex site ID
        env: Environment ('prod' or 'dev', default: 'prod')

    Returns:
        Site data dictionary, or None if site not found or error occurred

    Raises:
        RuntimeError: If convex CLI not available
    """
    root = Path(__file__).resolve().parent.parent.parent
    convex_dir = root / "job_board_application"

    if not convex_dir.exists():
        raise RuntimeError(f"Convex directory not found: {convex_dir}")

    # Build convex command
    cmd = ["npx", "convex", "run"]

    if env == "prod":
        cmd.append("--prod")

    cmd.extend([
        "sites:getSiteById",
        orjson.dumps({"id": site_id}).decode("utf-8"),
    ])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=convex_dir,
            timeout=30,
        )

        if result.returncode != 0:
            print(f"Convex query failed: {result.stderr}", file=sys.stderr)
            return None

        # Parse JSON output
        output = result.stdout.strip()
        if not output or output == "null":
            print(f"Site not found: {site_id}", file=sys.stderr)
            return None

        data = orjson.loads(output)
        return data if isinstance(data, dict) else None

    except subprocess.TimeoutExpired:
        print(f"Convex query timed out for site: {site_id}", file=sys.stderr)
        return None
    except orjson.JSONDecodeError as e:
        print(f"Failed to parse Convex response: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Failed to fetch site from Convex: {e}", file=sys.stderr)
        return None


def run_convex_query(
    function: str,
    args: Optional[Dict[str, Any]] = None,
    *,
    env: str = "prod",
) -> Any:
    """Run arbitrary Convex query.

    Args:
        function: Convex function name (e.g., 'jobs:getJobById')
        args: Function arguments as dictionary
        env: Environment ('prod' or 'dev', default: 'prod')

    Returns:
        Query result (parsed JSON)

    Raises:
        RuntimeError: If query fails
    """
    root = Path(__file__).resolve().parent.parent.parent
    convex_dir = root / "job_board_application"

    if not convex_dir.exists():
        raise RuntimeError(f"Convex directory not found: {convex_dir}")

    # Build convex command
    cmd = ["npx", "convex", "run"]

    if env == "prod":
        cmd.append("--prod")

    cmd.append(function)

    if args:
        cmd.append(orjson.dumps(args).decode("utf-8"))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=convex_dir,
            timeout=30,
        )

        if result.returncode != 0:
            raise RuntimeError(f"Convex query failed: {result.stderr}")

        # Parse JSON output
        output = result.stdout.strip()
        if not output:
            return None

        return orjson.loads(output)

    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"Convex query timed out: {function}") from e
    except orjson.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse Convex response: {e}") from e
