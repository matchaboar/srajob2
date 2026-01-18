"""
Base classes for the modular extractor system.

This module provides the foundation for field extraction with explicit
priorities and comprehensive debug tracing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from .context import ExtractionContext

T = TypeVar("T")


class StrategyPriority(IntEnum):
    """
    Strategy execution order. Lower values = higher priority = tried first.

    If a strategy returns a valid result, lower-priority strategies are skipped
    (unless debug mode is enabled, in which case all strategies run).
    """

    STRUCTURED_DATA = 100  # Schema.org JSON-LD, API responses
    SITE_HANDLER = 200  # Site-specific handlers (Greenhouse, Ashby, etc.)
    EXPLICIT_FIELD = 300  # Explicit labeled fields (Location: X)
    URL_DERIVED = 400  # Extracted from URL patterns
    CONTENT_PATTERN = 500  # Regex patterns in content
    HEURISTIC = 600  # Fuzzy matching, inference
    FALLBACK = 900  # Last resort defaults


@dataclass
class StrategyResult(Generic[T]):
    """Result from a single strategy execution."""

    value: T | None
    is_valid: bool
    confidence: float  # 0.0 to 1.0
    strategy_name: str
    priority: StrategyPriority | int  # Can be enum or int (for custom priorities)
    reason: str  # Human-readable explanation
    raw_input: str | None = None  # What the strategy operated on (for debugging)
    debug_info: dict[str, Any] = field(default_factory=dict)

    @property
    def should_use(self) -> bool:
        """Whether this result should be used as the final value."""
        return self.is_valid and self.value is not None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        # Handle both StrategyPriority enum and plain int values
        priority_name = (
            self.priority.name
            if isinstance(self.priority, StrategyPriority)
            else f"CUSTOM_{self.priority}"
        )
        return {
            "strategy": self.strategy_name,
            "priority": priority_name,
            "priority_value": int(self.priority),
            "value": self.value,
            "is_valid": self.is_valid,
            "confidence": self.confidence,
            "reason": self.reason,
            **({"raw_input": self.raw_input[:200]} if self.raw_input else {}),
            **({"debug_info": self.debug_info} if self.debug_info else {}),
        }


@dataclass
class ExtractionResult(Generic[T]):
    """Complete extraction result with debug trace."""

    field_name: str
    final_value: T | None
    winning_strategy: str | None
    all_results: list[StrategyResult[T]]  # All strategies that ran

    def to_debug_dict(self) -> dict[str, Any]:
        """Format for debug output."""
        return {
            "field": self.field_name,
            "final_value": self.final_value,
            "winning_strategy": self.winning_strategy,
            "strategy_results": [r.to_dict() for r in self.all_results],
        }


class ExtractionStrategy(ABC, Generic[T]):
    """
    Base class for extraction strategies.

    Each strategy implements a specific extraction approach for a field.
    Strategies are tried in priority order until a valid result is found.
    """

    name: str  # Unique identifier for this strategy
    priority: StrategyPriority | int  # Execution order (lower = higher priority)

    @abstractmethod
    def extract(self, context: ExtractionContext) -> StrategyResult[T]:
        """
        Attempt to extract the field value.

        Args:
            context: Full extraction context with all input data

        Returns:
            StrategyResult with value (or None) and metadata explaining the result
        """
        raise NotImplementedError

    def validate(self, value: T) -> tuple[bool, str]:
        """
        Validate the extracted value.

        Override in subclasses for field-specific validation.

        Returns:
            (is_valid, reason) tuple
        """
        if value is None:
            return False, "Value is None"
        return True, "Valid"

    def _make_result(
        self,
        value: T | None,
        reason: str,
        *,
        is_valid: bool | None = None,
        confidence: float = 0.0,
        raw_input: str | None = None,
        debug_info: dict[str, Any] | None = None,
    ) -> StrategyResult[T]:
        """
        Helper to create a StrategyResult with common fields filled in.

        If is_valid is None, it will be determined by validate().
        """
        if is_valid is None:
            is_valid, validation_reason = self.validate(value)
            if not is_valid:
                reason = validation_reason

        return StrategyResult(
            value=value if is_valid else None,
            is_valid=is_valid,
            confidence=confidence if is_valid else 0.0,
            strategy_name=self.name,
            priority=self.priority,
            reason=reason,
            raw_input=raw_input,
            debug_info=debug_info or {},
        )

    def _make_skip_result(self, reason: str) -> StrategyResult[T]:
        """Helper to create a result indicating this strategy doesn't apply."""
        return StrategyResult(
            value=None,
            is_valid=False,
            confidence=0.0,
            strategy_name=self.name,
            priority=self.priority,
            reason=reason,
        )


class FieldExtractor(ABC, Generic[T]):
    """
    Base class for field extractors.

    Each extractor manages multiple strategies for a single field.
    Strategies are executed in priority order until a valid result is found
    (or all strategies run in debug mode).
    """

    field_name: str
    strategies: list[ExtractionStrategy[T]]

    def __init__(self) -> None:
        self.strategies = self._register_strategies()
        # Sort by priority (lower = higher priority)
        self.strategies.sort(key=lambda s: s.priority)

    @abstractmethod
    def _register_strategies(self) -> list[ExtractionStrategy[T]]:
        """
        Register all strategies for this field.

        Override in subclasses to define the strategies.
        """
        raise NotImplementedError

    def extract(
        self,
        context: ExtractionContext,
        *,
        run_all: bool = False,
    ) -> ExtractionResult[T]:
        """
        Extract field value using registered strategies.

        Args:
            context: Extraction context with all input data
            run_all: If True, run ALL strategies even after finding valid result
                     (for debug mode). If False, stop at first valid result.

        Returns:
            ExtractionResult with final value and trace of all strategy results
        """
        results: list[StrategyResult[T]] = []
        winning_result: StrategyResult[T] | None = None

        for strategy in self.strategies:
            try:
                result = strategy.extract(context)
            except Exception as e:
                result = StrategyResult(
                    value=None,
                    is_valid=False,
                    confidence=0.0,
                    strategy_name=strategy.name,
                    priority=strategy.priority,
                    reason=f"Exception: {type(e).__name__}: {e}",
                )

            results.append(result)

            # Track winning result (first valid)
            if result.should_use and winning_result is None:
                winning_result = result

                # Stop early unless in debug mode
                if not run_all:
                    break

        return ExtractionResult(
            field_name=self.field_name,
            final_value=winning_result.value if winning_result else None,
            winning_strategy=winning_result.strategy_name if winning_result else None,
            all_results=results,
        )
