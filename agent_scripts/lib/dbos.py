"""DBOS queue management utilities.

Provides functions for querying and managing the DBOS scrape queue.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional


def get_queue_status(
    site_id: str,
    *,
    limit: int = 500,
) -> Dict[str, Any]:
    """Get queue status summary for a site.

    Args:
        site_id: Convex site ID
        limit: Maximum number of queue rows to fetch

    Returns:
        Dictionary with queue statistics:
        {
            "total_rows": int,
            "status_counts": {"pending": N, "processing": M, ...},
            "rows": [...]  # Raw queue rows
        }
    """
    from job_scrape_application.dbos_runtime import queue as dbos_queue

    rows = dbos_queue.list_scrape_urls(site_id=site_id, limit=limit) or []
    if not isinstance(rows, list):
        rows = []

    # Count by status
    status_counts: Dict[str, int] = defaultdict(int)
    for row in rows:
        if isinstance(row, dict):
            status = row.get("status") or "unknown"
            status_counts[str(status)] += 1

    return {
        "total_rows": len(rows),
        "status_counts": dict(status_counts),
        "rows": rows,
    }


def clear_site_queue(
    site_id: str,
    *,
    status_filter: Optional[str] = None,
) -> int:
    """Clear queue entries for a site.

    Args:
        site_id: Convex site ID
        status_filter: Optional status to filter (e.g., 'pending', 'failed')

    Returns:
        Number of rows deleted
    """
    from job_scrape_application.dbos_runtime import queue as dbos_queue

    # Get all rows for site
    rows = dbos_queue.list_scrape_urls(site_id=site_id, limit=10000) or []
    if not isinstance(rows, list):
        return 0

    deleted_count = 0

    for row in rows:
        if not isinstance(row, dict):
            continue

        # Apply status filter if provided
        if status_filter:
            row_status = row.get("status")
            if row_status != status_filter:
                continue

        # Delete the row
        row_id = row.get("id")
        if row_id:
            try:
                dbos_queue.delete_scrape_url(row_id)
                deleted_count += 1
            except Exception:
                # Continue on error
                pass

    return deleted_count


def get_queue_summary_for_companies(
    companies: List[str],
    *,
    limit_per_site: int = 500,
) -> Dict[str, Any]:
    """Get queue summary for multiple companies.

    Args:
        companies: List of company names to match
        limit_per_site: Max queue rows per site

    Returns:
        Dictionary with summary data for each matching site
    """
    from job_scrape_application.services import convex_query
    from job_scrape_application.dbos_runtime import queue as dbos_queue
    import asyncio

    async def _fetch() -> Dict[str, Any]:
        # Fetch all sites
        sites = await convex_query("router:listSites", {"enabledOnly": False})
        if not isinstance(sites, list):
            return {"error": "Failed to fetch sites"}

        # Match sites by company name
        matched_sites = []
        for site in sites:
            if not isinstance(site, dict):
                continue

            # Check if any company matches this site
            for company in companies:
                token = company.strip().lower()
                for key in ("name", "url", "pattern", "type"):
                    value = site.get(key)
                    if isinstance(value, str) and token in value.lower():
                        matched_sites.append(site)
                        break

        # Get queue status for each site
        site_summaries = []
        for site in matched_sites:
            site_id = site.get("_id")
            if not isinstance(site_id, str):
                continue

            rows = dbos_queue.list_scrape_urls(site_id=site_id, limit=limit_per_site) or []
            if not isinstance(rows, list):
                rows = []

            # Count by status
            status_counts: Dict[str, int] = defaultdict(int)
            for row in rows:
                if isinstance(row, dict):
                    status = row.get("status") or "unknown"
                    status_counts[str(status)] += 1

            site_summaries.append({
                "id": site_id,
                "name": site.get("name"),
                "url": site.get("url"),
                "type": site.get("type"),
                "queue_rows": len(rows),
                "status_counts": dict(status_counts),
            })

        return {
            "companies": companies,
            "matched_sites": len(matched_sites),
            "sites": site_summaries,
        }

    return asyncio.run(_fetch())


def list_queue_entries(
    site_id: str,
    *,
    limit: int = 500,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """List queue entries for a site.

    Args:
        site_id: Convex site ID
        limit: Maximum number of entries to return
        status: Optional status filter

    Returns:
        List of queue entry dictionaries
    """
    from job_scrape_application.dbos_runtime import queue as dbos_queue

    rows = dbos_queue.list_scrape_urls(site_id=site_id, limit=limit) or []
    if not isinstance(rows, list):
        return []

    # Filter by status if provided
    if status:
        rows = [row for row in rows if isinstance(row, dict) and row.get("status") == status]

    return rows
