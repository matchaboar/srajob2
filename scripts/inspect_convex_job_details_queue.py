from __future__ import annotations

import json
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

from convex import ConvexClient
from dotenv import load_dotenv


def _load_env() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env_path = repo_root / "job_board_application" / ".env.production"
    if env_path.exists():
        load_dotenv(env_path)
    # Normalize Convex URL for the Python client
    if not os.getenv("CONVEX_URL"):
        if os.getenv("VITE_CONVEX_URL"):
            os.environ["CONVEX_URL"] = os.environ["VITE_CONVEX_URL"]
        elif os.getenv("CONVEX_HTTP_URL"):
            os.environ["CONVEX_URL"] = os.environ["CONVEX_HTTP_URL"].replace(
                ".convex.site", ".convex.cloud"
            )


def _domain(url: str) -> str:
    try:
        return url.split("//", 1)[1].split("/", 1)[0].lower()
    except Exception:
        return ""


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    now = int(time.time() * 1000)
    domains = Counter(_domain(row.get("url", "")) for row in rows if row.get("url"))
    oldest = None
    newest = None
    for row in rows:
        updated = row.get("updatedAt") or row.get("createdAt")
        if isinstance(updated, (int, float)):
            if oldest is None or updated < oldest:
                oldest = int(updated)
            if newest is None or updated > newest:
                newest = int(updated)
    return {
        "count": len(rows),
        "top_domains": domains.most_common(10),
        "oldest_age_min": (now - oldest) / 60000 if oldest else None,
        "newest_age_min": (now - newest) / 60000 if newest else None,
    }


def _sample(rows: List[Dict[str, Any]], domain: str, limit: int = 5) -> List[Dict[str, Any]]:
    sample = []
    for row in rows:
        if _domain(row.get("url", "")) == domain:
            sample.append(
                {
                    "url": row.get("url"),
                    "status": row.get("status"),
                    "attempts": row.get("attempts"),
                    "updatedAt": row.get("updatedAt"),
                    "createdAt": row.get("createdAt"),
                    "lastError": row.get("lastError"),
                }
            )
            if len(sample) >= limit:
                break
    return sample


def _safe_query(client: ConvexClient, name: str, args: Optional[Dict[str, Any]] = None) -> Any:
    return client.query(name, args or {})


def main() -> None:
    _load_env()
    url = os.getenv("CONVEX_URL")
    if not url:
        raise SystemExit("CONVEX_URL is not set (check .env.production)")

    client = ConvexClient(url)

    statuses = ["pending", "processing", "failed", "completed"]
    queue_by_status: Dict[str, List[Dict[str, Any]]] = {}
    for status in statuses:
        rows = _safe_query(
            client,
            "router:listQueuedScrapeUrls",
            {"status": status, "provider": "spidercloud", "limit": 500},
        )
        queue_by_status[status] = rows if isinstance(rows, list) else []

    rate_limits = _safe_query(client, "router:listJobDetailRateLimits", {})
    rate_limits = rate_limits if isinstance(rate_limits, list) else []

    active_workers = _safe_query(client, "temporal:getActiveWorkers", {})
    active_workers = active_workers if isinstance(active_workers, list) else []

    summary = {
        "convex_url": url,
        "queue": {status: _summarize(rows) for status, rows in queue_by_status.items()},
        "rate_limits": [
            {
                "domain": row.get("domain"),
                "maxPerMinute": row.get("maxPerMinute"),
                "sentInWindow": row.get("sentInWindow"),
                "lastWindowStart": row.get("lastWindowStart"),
            }
            for row in rate_limits
        ],
        "active_workers": [
            {
                "workerId": row.get("workerId"),
                "taskQueue": row.get("taskQueue"),
                "lastHeartbeat": row.get("lastHeartbeat"),
                "workflowCount": len(row.get("workflows", [])) if isinstance(row.get("workflows"), list) else 0,
            }
            for row in active_workers
        ],
    }

    github_domain = "www.github.careers"
    summary["github_samples"] = {
        status: _sample(rows, github_domain, limit=10)
        for status, rows in queue_by_status.items()
    }

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
