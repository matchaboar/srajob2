from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows import activities as acts  # noqa: E402


@pytest.mark.asyncio
async def test_store_scrape_accepts_spidercloud_batch_fixture(monkeypatch):
    """Ensure spidercloud batch payloads pass type validation when stored."""

    fixture_path = Path("tests/fixtures/spidercloud_store_scrape_input.json")
    # The fixture contains control characters and unescaped backslashes; sanitize for JSON parser.
    raw_text = fixture_path.read_text(encoding="utf-8")

    def _load_json(text: str) -> Dict[str, Any]:
        try:
            return json.loads(text, strict=False)
        except json.JSONDecodeError:
            pass
        try:
            cleaned = re.sub(r"\\(?![\"\\/bfnrtu])", "", text)
            cleaned = cleaned.replace("\r", " ").replace("\n", " ")
            return json.loads(cleaned, strict=False)
        except Exception:
            pass
        try:
            stripped = text.replace("\\", "").replace("\r", " ").replace("\n", " ")
            return json.loads(stripped, strict=False)
        except Exception:
            pass
        try:
            decoded = text.encode("utf-8").decode("unicode_escape")
            return json.loads(decoded, strict=False)
        except Exception:
            pass
        try:
            import yaml  # type: ignore

            return yaml.safe_load(text)
        except Exception:
            pass
        # Fallback: treat as JSONL and return the first parsed object
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                return json.loads(stripped, strict=False)
            except json.JSONDecodeError:
                continue
        raise

    payload: Dict[str, Any] = _load_json(raw_text)
    scrape = payload.get("scrape") if isinstance(payload, dict) else {}
    assert scrape, "fixture must contain scrape payload"
    assert payload.get("jobsScraped") == 34
    items = scrape.get("items") if isinstance(scrape, dict) else {}
    normalized = items.get("normalized") if isinstance(items, dict) else []
    assert isinstance(normalized, list) and normalized, "normalized jobs should not be empty"
    first_url = normalized[0].get("url") if isinstance(normalized[0], dict) else ""
    assert "boards.greenhouse.io" in first_url
    markdown = normalized[0].get("description") if isinstance(normalized[0], dict) else ""
    assert isinstance(markdown, str) and len(markdown) > 100
    assert "# " in markdown or "## " in markdown
    scrape["provider"] = "spidercloud"

    seen: Dict[str, Any] = {}

    async def fake_convex_mutation(name: str, args: Dict[str, Any] | None = None):
        args = args or {}
        if name == "router:insertScrapeRecord":
            seen["insert"] = args
            assert isinstance(args.get("sourceUrl"), str)
            assert isinstance(args.get("startedAt"), (int, float))
            assert isinstance(args.get("completedAt"), (int, float))
            assert isinstance(args.get("items"), dict)
            return "scrape-id"
        if name == "router:ingestJobsFromScrape":
            seen["ingest"] = args
            jobs = args.get("jobs") or []
            assert isinstance(jobs, list) and jobs, "jobs must be a non-empty list"
            for job in jobs:
                assert isinstance(job.get("title"), str)
                assert isinstance(job.get("company"), str)
                assert isinstance(job.get("description"), str)
                assert isinstance(job.get("url"), str)
                assert isinstance(job.get("postedAt"), (int, float))
            return {"inserted": len(jobs)}
        return None

    monkeypatch.setattr("job_scrape_application.services.convex_client.convex_mutation", fake_convex_mutation)

    await acts.store_scrape(scrape)

    assert "insert" in seen
    assert "ingest" in seen
