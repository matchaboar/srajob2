"""
Cost (milli-cents) extraction strategies and extractor.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from .base import (
    ExtractionStrategy,
    FieldExtractor,
    StrategyPriority,
    StrategyResult,
)

if TYPE_CHECKING:
    from .context import ExtractionContext


def _coerce_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _normalize_milli_cents(value: Any) -> int | None:
    number = _coerce_number(value)
    if number is None or number <= 0:
        return None
    return int(number)


def _normalize_cents(value: Any) -> int | None:
    number = _coerce_number(value)
    if number is None or number <= 0:
        return None
    return int(number * 1_000)


def _normalize_dollars(value: Any) -> int | None:
    number = _coerce_number(value)
    if number is None or number <= 0:
        return None
    return int(number * 100_000)


def _normalize_credits(value: Any) -> int | None:
    number = _coerce_number(value)
    if number is None or number <= 0:
        return None
    return int(number * 10)


def _get_items_block(raw_row: Any) -> dict[str, Any] | None:
    if not isinstance(raw_row, dict):
        return None
    items = raw_row.get("items")
    return items if isinstance(items, dict) else None


def _get_costs_block(raw_row: Any) -> dict[str, Any] | None:
    if not isinstance(raw_row, dict):
        return None
    costs = raw_row.get("costs")
    if isinstance(costs, dict):
        return costs
    items = _get_items_block(raw_row)
    if isinstance(items, dict):
        costs = items.get("costs")
        if isinstance(costs, dict):
            return costs
    return None


def _is_valid_cost(value: int | None) -> tuple[bool, str]:
    if value is None:
        return False, "No cost value"
    if value <= 0:
        return False, f"Cost must be positive: {value}"
    return True, f"Valid cost: {value}"


class RawRowCostMilliCentsStrategy(ExtractionStrategy[int]):
    """Extract cost milli-cents from explicit raw row fields."""

    name = "raw_row_cost_milli_cents"
    priority = StrategyPriority.STRUCTURED_DATA

    def extract(self, context: ExtractionContext) -> StrategyResult[int]:
        raw_value = context.get_raw_field(
            "cost_milli_cents",
            "costMilliCents",
            "scrapedCostMilliCents",
        )

        if raw_value is None:
            items = _get_items_block(context.raw_row)
            if isinstance(items, dict):
                raw_value = items.get("cost_milli_cents") or items.get("costMilliCents")
                if raw_value is None:
                    raw_value = items.get("scrapedCostMilliCents")

        if raw_value is None:
            return self._make_skip_result("No explicit cost field")

        normalized = _normalize_milli_cents(raw_value)
        if normalized is None:
            return self._make_skip_result("Invalid cost milli-cents value")

        is_valid, reason = _is_valid_cost(normalized)
        return self._make_result(
            normalized if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.95,
            debug_info={"raw_value": raw_value},
        )

    def validate(self, value: int) -> tuple[bool, str]:
        return _is_valid_cost(value)


class RawRowCostCentsStrategy(ExtractionStrategy[int]):
    """Extract cost in cents and convert to milli-cents."""

    name = "raw_row_cost_cents"
    priority = StrategyPriority.STRUCTURED_DATA + 10

    def extract(self, context: ExtractionContext) -> StrategyResult[int]:
        raw_value = context.get_raw_field("cost_cents", "costCents")

        if raw_value is None:
            items = _get_items_block(context.raw_row)
            if isinstance(items, dict):
                raw_value = items.get("cost_cents") or items.get("costCents")

        if raw_value is None:
            return self._make_skip_result("No cost cents field")

        normalized = _normalize_cents(raw_value)
        if normalized is None:
            return self._make_skip_result("Invalid cost cents value")

        is_valid, reason = _is_valid_cost(normalized)
        return self._make_result(
            normalized if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.9,
            debug_info={"raw_value": raw_value},
        )

    def validate(self, value: int) -> tuple[bool, str]:
        return _is_valid_cost(value)


class CostsBlockStrategy(ExtractionStrategy[int]):
    """Extract cost from SpiderCloud costs block."""

    name = "costs_block"
    priority = StrategyPriority.STRUCTURED_DATA + 20

    def extract(self, context: ExtractionContext) -> StrategyResult[int]:
        costs = _get_costs_block(context.raw_row)
        if not isinstance(costs, dict):
            return self._make_skip_result("No costs block")

        total_cost = costs.get("total_cost") or costs.get("totalCost")
        normalized = _normalize_dollars(total_cost)
        if normalized is not None:
            is_valid, reason = _is_valid_cost(normalized)
            return self._make_result(
                normalized if is_valid else None,
                reason,
                is_valid=is_valid,
                confidence=0.9,
                debug_info={"raw_value": total_cost, "source": "total_cost"},
            )

        credits_used = costs.get("credits_used") or costs.get("creditsUsed")
        normalized = _normalize_credits(credits_used)
        if normalized is not None:
            is_valid, reason = _is_valid_cost(normalized)
            return self._make_result(
                normalized if is_valid else None,
                reason,
                is_valid=is_valid,
                confidence=0.85,
                debug_info={"raw_value": credits_used, "source": "credits_used"},
            )

        return self._make_skip_result("Costs block missing total_cost or credits_used")

    def validate(self, value: int) -> tuple[bool, str]:
        return _is_valid_cost(value)


class RawRowCreditsUsedStrategy(ExtractionStrategy[int]):
    """Extract cost from raw creditsUsed fields."""

    name = "raw_row_credits_used"
    priority = StrategyPriority.STRUCTURED_DATA + 30

    def extract(self, context: ExtractionContext) -> StrategyResult[int]:
        raw_value = context.get_raw_field("credits_used", "creditsUsed")

        if raw_value is None:
            items = _get_items_block(context.raw_row)
            if isinstance(items, dict):
                raw_value = items.get("credits_used") or items.get("creditsUsed")

        if raw_value is None:
            return self._make_skip_result("No creditsUsed field")

        normalized = _normalize_credits(raw_value)
        if normalized is None:
            return self._make_skip_result("Invalid creditsUsed value")

        is_valid, reason = _is_valid_cost(normalized)
        return self._make_result(
            normalized if is_valid else None,
            reason,
            is_valid=is_valid,
            confidence=0.8,
            debug_info={"raw_value": raw_value},
        )

    def validate(self, value: int) -> tuple[bool, str]:
        return _is_valid_cost(value)


class CostMilliCentsExtractor(FieldExtractor[int]):
    """Extract cost in milli-cents from scrape metadata."""

    field_name = "cost_milli_cents"

    def _register_strategies(self) -> list[ExtractionStrategy[int]]:
        return [
            RawRowCostMilliCentsStrategy(),
            RawRowCostCentsStrategy(),
            CostsBlockStrategy(),
            RawRowCreditsUsedStrategy(),
        ]
