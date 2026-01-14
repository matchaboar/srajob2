"""
Unified listing extraction workflow - used by BOTH production and tests.

Debug mode generates trace output automatically - no separate scripts needed.

This module provides:
- ListingExtractionTrace: Debug trace of listing extraction steps
- ListingWorkflowModule: Core listing extraction logic with debug support

Usage:
    # Production (no trace)
    workflow = ListingWorkflowModule()
    urls = workflow.extract_listing_urls(scrape_payload, source_url, site_id)

    # Debug/Test (with trace)
    workflow = ListingWorkflowModule(debug=True)
    urls = workflow.extract_listing_urls(scrape_payload, source_url, site_id)
    trace = workflow.trace  # Contains all debug info
    # Files auto-written to ./site-detail-e2e-examples/
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..helpers.link_extractors import normalize_url, normalize_url_list
from ..site_handlers import get_site_handler
from ..site_handlers.base import BaseSiteHandler

OUTPUT_DIR = Path("./site-detail-e2e-examples")


@dataclass
class ListingExtractionTrace:
    """Debug trace of listing extraction steps.

    Captures all intermediate state during listing URL extraction
    for debugging and test verification.
    """

    site_id: str = ""
    handler_name: Optional[str] = None
    listing_url: str = ""
    source_url: str = ""
    raw_content: Optional[str] = None
    content_type: Optional[str] = None

    # URL extraction
    extracted_urls: List[str] = field(default_factory=list)
    extraction_method: Optional[str] = None

    # Handler filtering
    filtered_urls: List[str] = field(default_factory=list)
    rejected_urls: List[Tuple[str, str]] = field(default_factory=list)
    transformations: List[Tuple[str, str, str]] = field(default_factory=list)

    # Normalization
    normalized_urls: List[str] = field(default_factory=list)
    url_transformations: List[Tuple[str, str]] = field(default_factory=list)

    # Pagination
    pagination_urls: List[str] = field(default_factory=list)

    # Step-by-step log
    steps: List[Dict[str, Any]] = field(default_factory=list)

    def to_json(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dict (summary view)."""
        return {
            "site_id": self.site_id,
            "handler_name": self.handler_name,
            "listing_url": self.listing_url,
            "source_url": self.source_url,
            "content_type": self.content_type,
            "extraction_method": self.extraction_method,
            "extracted_url_count": len(self.extracted_urls),
            "filtered_url_count": len(self.filtered_urls),
            "normalized_url_count": len(self.normalized_urls),
            "pagination_url_count": len(self.pagination_urls),
            "sample_urls": self.normalized_urls[:10],
            "rejected_urls": [{"url": u, "reason": r} for u, r in self.rejected_urls[:10]],
            "url_transformations": [{"orig": o, "norm": n} for o, n in self.url_transformations[:10]],
        }

    def to_markdown_concise(self) -> str:
        """
        Generate CONCISE markdown for Claude Code context.

        Design goals:
        - Minimize tokens (Claude context is expensive)
        - Show key results and any failures
        - Reference JSON file for detailed data
        """
        json_file = f"{self.site_id}_listing_extraction.json"
        lines = [
            f"# Listing Extraction: {self.site_id}",
            "",
            f"Handler: `{self.handler_name}` | Content: `{self.content_type}`",
            f"URLs: {len(self.extracted_urls)} extracted → {len(self.filtered_urls)} filtered → {len(self.normalized_urls)} final",
            "",
        ]

        # Only show failures/issues (success is the default)
        if self.rejected_urls:
            lines.append(f"**Rejected:** {len(self.rejected_urls)} URLs")
            for url, reason in self.rejected_urls[:3]:
                truncated = url[:60] + "..." if len(url) > 60 else url
                lines.append(f"  - `{truncated}`: {reason}")
            if len(self.rejected_urls) > 3:
                lines.append(f"  - ... {len(self.rejected_urls) - 3} more (see JSON)")
            lines.append("")

        if self.url_transformations:
            lines.append(f"**Transformations:** {len(self.url_transformations)} URLs normalized")
            lines.append("")

        # Sample URLs (just 3)
        if self.normalized_urls:
            lines.append("**Sample URLs:**")
            for url in self.normalized_urls[:3]:
                lines.append(f"  - `{url}`")
            lines.append("")

        # Reference to detailed JSON
        lines.extend([
            "---",
            f"**Full details:** `rg <pattern> {json_file}` for strategy results, raw content, etc.",
        ])

        return "\n".join(lines)

    def to_json_detailed(self) -> Dict[str, Any]:
        """
        Generate DETAILED JSON for searching with ripgrep.

        Contains all data - use `rg` to find specific fields.
        """
        return {
            "site_id": self.site_id,
            "handler_name": self.handler_name,
            "listing_url": self.listing_url,
            "source_url": self.source_url,
            "content_type": self.content_type,
            "extraction_method": self.extraction_method,
            "raw_content_preview": (self.raw_content or "")[:2000],
            "extracted_urls": self.extracted_urls,
            "filtered_urls": self.filtered_urls,
            "normalized_urls": self.normalized_urls,
            "rejected_urls": [{"url": u, "reason": r} for u, r in self.rejected_urls],
            "url_transformations": [{"original": o, "normalized": n} for o, n in self.url_transformations],
            "pagination_urls": self.pagination_urls,
            "steps": self.steps,
        }

    def write_output(self, site_id: str) -> Tuple[Path, Path]:
        """
        Write dual-format output:
        - .md: Concise for Claude context (token-efficient)
        - .json: Detailed for ripgrep searching
        """
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        json_path = OUTPUT_DIR / f"{site_id}_listing_extraction.json"
        md_path = OUTPUT_DIR / f"{site_id}_listing_extraction.md"
        json_path.write_text(json.dumps(self.to_json_detailed(), indent=2))
        md_path.write_text(self.to_markdown_concise())
        return json_path, md_path


def _parse_scrape_content(scrape_payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Parse raw content and content type from scrape payload.

    Returns:
        Tuple of (raw_content, content_type)
    """
    items = scrape_payload.get("items", {})
    if not isinstance(items, dict):
        return None, None

    # Check for raw payload (from SpiderCloud)
    raw = items.get("raw")
    if isinstance(raw, list) and raw:
        first_batch = raw[0] if isinstance(raw[0], list) else [raw[0]]
        if first_batch and isinstance(first_batch[0], dict):
            content = first_batch[0].get("content", {})
            if isinstance(content, dict):
                # Prefer commonmark, then raw HTML
                if "commonmark" in content:
                    return content.get("commonmark"), "commonmark"
                if "raw" in content:
                    return content.get("raw"), "raw_html"

    # Check for pre-normalized content
    normalized = items.get("normalized")
    if isinstance(normalized, list):
        return None, "normalized"

    return None, None


def _filter_urls_with_handler(
    urls: List[str],
    handler: Optional[BaseSiteHandler],
    *,
    debug: bool = False,
) -> Tuple[List[str], List[Tuple[str, str]], List[Tuple[str, str, str]]]:
    """Filter URLs using handler's filter_job_urls method.

    Args:
        urls: List of extracted URLs
        handler: Site handler or None
        debug: Whether to track rejections and transformations

    Returns:
        Tuple of (filtered_urls, rejected_urls, transformations)
        - rejected_urls: List of (url, reason) tuples
        - transformations: List of (original, transformed, reason) tuples
    """
    if not handler:
        return list(urls), [], []

    filtered = handler.filter_job_urls(urls)
    if not debug:
        return filtered, [], []

    # Track what was rejected
    filtered_set = set(filtered)
    rejected: List[Tuple[str, str]] = []
    transformations: List[Tuple[str, str, str]] = []

    for url in urls:
        if url not in filtered_set:
            # Check if it was transformed
            normalized = normalize_url(url)
            if normalized and normalized in filtered_set:
                transformations.append((url, normalized, "normalized"))
            else:
                rejected.append((url, "filtered_by_handler"))

    return filtered, rejected, transformations


class ListingWorkflowModule:
    """
    Core listing extraction logic used by production and tests.

    Production: debug=False, just returns URL entries
    Tests/Debug: debug=True, generates trace + writes output files automatically
    """

    def __init__(self, *, debug: bool = False, write_output: bool = True):
        """Initialize the workflow module.

        Args:
            debug: Enable debug tracing and output
            write_output: Auto-write debug files when debug=True
        """
        self.debug = debug
        self.write_output = write_output
        self.trace: Optional[ListingExtractionTrace] = None

    def _log_step(self, name: str, description: str, data: Optional[Dict[str, Any]] = None) -> None:
        """Log a step to the trace (only in debug mode)."""
        if self.debug and self.trace:
            self.trace.steps.append({
                "name": name,
                "description": description,
                "data": data,
            })

    def extract_listing_urls(
        self,
        scrape_payload: Dict[str, Any],
        source_url: str,
        site_id: str,
        *,
        extract_urls_fn: Any = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract job URLs from a listing page scrape.

        This is the core extraction logic used by both production and tests.

        Args:
            scrape_payload: The scrape response payload
            source_url: The source listing URL
            site_id: Site identifier
            extract_urls_fn: Optional custom URL extraction function

        Returns:
            List of URL entry dicts ready for enqueue

        In debug mode:
        - Populates self.trace with all intermediate state
        - Auto-writes JSON and markdown output files
        """
        if self.debug:
            self.trace = ListingExtractionTrace(site_id=site_id, source_url=source_url)

        # 1. Get handler
        handler = get_site_handler(source_url)
        if self.debug and self.trace:
            self.trace.handler_name = type(handler).__name__ if handler else None
            self.trace.listing_url = source_url
            self._log_step(
                "Handler Detection",
                f"Detected handler: {self.trace.handler_name}",
                {"url": source_url, "handler": self.trace.handler_name}
            )

        # 2. Parse response
        raw_content, content_type = _parse_scrape_content(scrape_payload)
        if self.debug and self.trace:
            self.trace.raw_content = raw_content[:5000] if raw_content else None
            self.trace.content_type = content_type
            self._log_step(
                "Content Parsing",
                f"Parsed {len(raw_content or '')} chars of {content_type}",
                {"content_type": content_type, "length": len(raw_content or "")}
            )

        # 3. Extract URLs
        if extract_urls_fn:
            extracted_urls = extract_urls_fn(scrape_payload)
            extraction_method = "custom_function"
        else:
            # Import here to avoid circular imports
            from ..activities import _extract_job_urls_from_scrape
            extracted_urls = _extract_job_urls_from_scrape(scrape_payload)
            extraction_method = "production._extract_job_urls_from_scrape"

        if self.debug and self.trace:
            self.trace.extracted_urls = list(extracted_urls)
            self.trace.extraction_method = extraction_method
            self._log_step(
                "URL Extraction",
                f"Extracted {len(extracted_urls)} URLs",
                {"count": len(extracted_urls), "sample": extracted_urls[:5]}
            )

        # 4. Filter using handler
        filtered_urls, rejected, transformations = _filter_urls_with_handler(
            extracted_urls, handler, debug=self.debug
        )
        if self.debug and self.trace:
            self.trace.filtered_urls = filtered_urls
            self.trace.rejected_urls = rejected
            self.trace.transformations = transformations
            self._log_step(
                "Handler Filtering",
                f"Filtered to {len(filtered_urls)} URLs, rejected {len(rejected)}",
                {"kept": len(filtered_urls), "rejected": len(rejected)}
            )

        # 5. Normalize URLs
        normalized_urls = normalize_url_list(filtered_urls, base_url=source_url)
        if self.debug and self.trace:
            self.trace.normalized_urls = normalized_urls
            self.trace.url_transformations = [
                (o, n) for o, n in zip(filtered_urls, normalized_urls) if o != n
            ]
            self._log_step(
                "URL Normalization",
                f"Normalized {len(normalized_urls)} URLs, {len(self.trace.url_transformations)} transformed",
                {"transformations": self.trace.url_transformations[:5]}
            )

        # 6. Separate pagination URLs (simple heuristic)
        detail_urls: List[str] = []
        pagination_urls: List[str] = []
        for url in normalized_urls:
            if handler and handler.is_listing_url(url):
                pagination_urls.append(url)
            else:
                detail_urls.append(url)

        if self.debug and self.trace:
            self.trace.pagination_urls = pagination_urls
            self._log_step(
                "Pagination Separation",
                f"{len(detail_urls)} detail URLs, {len(pagination_urls)} pagination URLs",
                {"detail_sample": detail_urls[:3], "pagination_sample": pagination_urls[:3]}
            )

            # AUTO-WRITE OUTPUT FILES
            if self.write_output:
                json_path, md_path = self.trace.write_output(site_id)
                self._log_step(
                    "Output Written",
                    f"Wrote debug output to {json_path} and {md_path}",
                    None
                )

        # 7. Build enqueue entries
        return [
            {"url": url, "sourceUrl": source_url, "siteId": site_id, "urlType": "detail"}
            for url in detail_urls
        ]

    def get_trace(self) -> Optional[ListingExtractionTrace]:
        """Get the debug trace if available."""
        return self.trace
