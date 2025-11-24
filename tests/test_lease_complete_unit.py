from __future__ import annotations

from typing import Any, Dict

import pytest

# Target module
import os
import sys

sys.path.insert(0, os.path.abspath('.'))
from job_scrape_application.workflows import activities as acts
from job_scrape_application.workflows import convex_client


class FakeConvex:
    def __init__(self):
        self.sites: Dict[str, Dict[str, Any]] = {}
        self._id_counter = 0

    def _insert_site(self, name: str) -> str:
        self._id_counter += 1
        sid = f"site_{self._id_counter}"
        self.sites[sid] = {
            "_id": sid,
            "name": name,
            "url": f"https://example.com/{name}",
            "enabled": True,
            "completed": False,
            "failed": False,
            "lockExpiresAt": None,
            "lockedBy": None,
            "lastRunAt": None,
            "type": "general",
        }
        return sid

    async def query(self, name: str, args: Dict[str, Any] | None = None):
        if name == "router:listSites":
            enabled_only = (args or {}).get("enabledOnly", False)
            sites = list(self.sites.values())
            if enabled_only:
                sites = [s for s in sites if s["enabled"]]
            return sites
        raise RuntimeError(f"Unexpected query {name}")

    async def mutation(self, name: str, args: Dict[str, Any] | None = None):
        args = args or {}
        if name == "router:leaseSite":
            now = 0
            for s in self.sites.values():
                if s.get("completed") or s.get("failed"):
                    continue
                if s.get("lockExpiresAt") and s["lockExpiresAt"] > now:
                    continue
                s["lockedBy"] = args.get("workerId")
                s["lockExpiresAt"] = now + int(args.get("lockSeconds") or 300) * 1000
                return s
            return None
        if name == "router:completeSite":
            sid = args.get("id")
            s = self.sites.get(sid)
            assert s
            s["completed"] = True
            s["lockedBy"] = None
            s["lockExpiresAt"] = None
            return {"success": True}
        raise RuntimeError(f"Unexpected mutation {name}")


@pytest.mark.asyncio
async def test_lease_complete_sequence(monkeypatch):
    fake = FakeConvex()

    # Pre-seed two sites
    sid1 = fake._insert_site("A")
    sid2 = fake._insert_site("B")

    async def fake_query(name: str, args: Dict[str, Any] | None = None):
        return await fake.query(name, args)

    async def fake_mutation(name: str, args: Dict[str, Any] | None = None):
        return await fake.mutation(name, args)

    monkeypatch.setattr(convex_client, "convex_query", fake_query)
    monkeypatch.setattr(convex_client, "convex_mutation", fake_mutation)

    # Lease one
    leased1 = await acts.lease_site("worker-x", 60)
    assert leased1 is not None
    first_id = leased1["_id"]
    assert first_id in (sid1, sid2)

    # Complete it
    await acts.complete_site(first_id)

    # Lease again => different id
    leased2 = await acts.lease_site("worker-x", 60)
    assert leased2 is not None
    second_id = leased2["_id"]
    assert second_id != first_id
