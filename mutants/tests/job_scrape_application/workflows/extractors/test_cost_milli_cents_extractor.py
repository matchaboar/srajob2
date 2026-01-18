from __future__ import annotations

from typing import Any

from job_scrape_application.workflows.extractors import ExtractionContext, extract_job_fields


def _extract_cost(raw_row: dict[str, Any]) -> int | None:
    context = ExtractionContext.from_scrape_result(
        url="https://example.com/jobs/123",
        markdown="",
        raw_row=raw_row,
    )
    results = extract_job_fields(context, fields=["cost_milli_cents"])
    return results["cost_milli_cents"].final_value


def test_cost_milli_cents_from_total_cost() -> None:
    raw_row = {"costs": {"total_cost": 0.00123}}
    assert _extract_cost(raw_row) == 123


def test_cost_milli_cents_from_explicit_field() -> None:
    raw_row = {"costMilliCents": 2500}
    assert _extract_cost(raw_row) == 2500


def test_cost_milli_cents_from_credits_used() -> None:
    raw_row = {"costs": {"credits_used": 2}}
    assert _extract_cost(raw_row) == 20
