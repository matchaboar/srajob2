"""
Shared fixtures and utilities for extractor unit tests.

These tests validate that extractors produce the CORRECT expected values
from the assertion files, not just that they "work". If an extractor
produces wrong values, these tests should FAIL.
"""

from __future__ import annotations

import orjson
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from job_scrape_application.workflows.extractors import (
    ExtractionContext,
    extract_job_fields,
)
from job_scrape_application.workflows.site_handlers import get_site_handler

logger = logging.getLogger(__name__)

# Directories
FIXTURE_DIR = Path("tests/job_scrape_application/workflows/fixtures")
GROUND_TRUTH_DIR = Path("tests/job_scrape_application/workflows/ground_truth")


@dataclass
class ExtractorTestCase:
    """A test case with fixture data and expected values from assertion."""

    identifier: str
    fixture_path: Path
    assertion_path: Path
    url: str = ""
    raw_markdown: str = ""
    raw_row: dict[str, Any] = field(default_factory=dict)
    structured_data: dict[str, Any] | None = None
    expected: dict[str, Any] = field(default_factory=dict)
    site_id: str = ""

    @classmethod
    def from_paths(cls, fixture_path: Path, assertion_path: Path) -> "ExtractorTestCase":
        """Load test case from fixture and assertion files."""
        identifier = assertion_path.stem

        # Load fixture
        fixture_data = orjson.loads(fixture_path.read_text(encoding="utf-8"))

        # Extract URL from fixture
        url = ""
        raw_markdown = ""
        raw_row: dict[str, Any] = {}
        structured_data: dict[str, Any] | None = None

        # Handle list-format fixtures (e.g., [[{...}]])
        if isinstance(fixture_data, list):
            if fixture_data and isinstance(fixture_data[0], list) and fixture_data[0]:
                first_item = fixture_data[0][0]
                if isinstance(first_item, dict):
                    url = first_item.get("url", "")
                    content = first_item.get("content", {})
                    raw_markdown = content.get("commonmark", "")
                    raw_content = content.get("raw", "")

                    def _is_trivial_commonmark(value: str) -> bool:
                        stripped = value.strip()
                        if not stripped:
                            return True
                        if len(stripped) < 200 and len(stripped.splitlines()) <= 3:
                            return True
                        return False
                    # Extract metadata as structured data
                    # Note: Do NOT use metadata.description when raw_markdown is available,
                    # as metadata.description is typically just a short SEO summary,
                    # not the full job description.
                    metadata = first_item.get("metadata", {})
                    if isinstance(metadata, dict):
                        # Try commonmark metadata first (Netflix-style)
                        # But only use it if raw_markdown is NOT available
                        # (metadata.description is usually just a summary)
                        cm_meta = metadata.get("commonmark", {})
                        if isinstance(cm_meta, dict):
                            # Always use non-description fields from metadata
                            # Copy metadata to structured_data but don't let it override
                            # raw_markdown content for description extraction
                            structured_data = cm_meta
                            raw_row = cm_meta
                        # Fall back to raw metadata
                        elif metadata.get("raw") and isinstance(metadata.get("raw"), dict):
                            structured_data = metadata.get("raw")
                            raw_row = metadata.get("raw", {})

                    # Handle raw JSON responses (e.g., Greenhouse API wrapped in HTML)
                    if raw_content:
                        # Check if raw content is JSON
                        if raw_content.strip().startswith("{"):
                            try:
                                parsed_raw = orjson.loads(raw_content)
                                if isinstance(parsed_raw, dict):
                                    data_obj = parsed_raw.get("data")
                                    if isinstance(data_obj, dict):
                                        structured_data = data_obj
                                        raw_row = data_obj
                                    else:
                                        structured_data = parsed_raw
                                        raw_row = parsed_raw
                                    # Extract description from Greenhouse API format
                                    if "content" in parsed_raw and (
                                        not raw_markdown or _is_trivial_commonmark(raw_markdown)
                                    ):
                                        import html
                                        html_content = parsed_raw.get("content", "")
                                        unescaped = html.unescape(html_content)
                                        raw_markdown = unescaped
                                    elif isinstance(data_obj, dict) and (
                                        not raw_markdown or _is_trivial_commonmark(raw_markdown)
                                    ):
                                        job_desc = data_obj.get("jobDescription", "")
                                        if job_desc:
                                            import html
                                            raw_markdown = html.unescape(job_desc)
                            except (orjson.JSONDecodeError, Exception):
                                if not raw_markdown or _is_trivial_commonmark(raw_markdown):
                                    raw_markdown = raw_content
                        # Handle HTML-wrapped JSON (e.g., <html>...<pre>{...}</pre>...</html>)
                        elif "<pre>" in raw_content:
                            import re
                            pre_match = re.search(r"<pre>(.*?)</pre>", raw_content, re.DOTALL)
                            if pre_match:
                                json_str = pre_match.group(1)
                                try:
                                    parsed_raw = orjson.loads(json_str)
                                    if isinstance(parsed_raw, dict):
                                        data_obj = parsed_raw.get("data")
                                        if isinstance(data_obj, dict):
                                            structured_data = data_obj
                                            raw_row = data_obj
                                        else:
                                            structured_data = parsed_raw
                                            raw_row = parsed_raw
                                        # Extract description from Greenhouse API format
                                        if "content" in parsed_raw and (
                                            not raw_markdown or _is_trivial_commonmark(raw_markdown)
                                        ):
                                            import html
                                            html_content = parsed_raw.get("content", "")
                                            unescaped = html.unescape(html_content)
                                            raw_markdown = unescaped
                                        elif isinstance(data_obj, dict) and (
                                            not raw_markdown or _is_trivial_commonmark(raw_markdown)
                                        ):
                                            job_desc = data_obj.get("jobDescription", "")
                                            if job_desc:
                                                import html
                                                raw_markdown = html.unescape(job_desc)
                                except (orjson.JSONDecodeError, Exception):
                                    pass
                        else:
                            if not raw_markdown or _is_trivial_commonmark(raw_markdown):
                                raw_markdown = raw_content
                            # Try to extract JSON-LD from HTML (Ashby pages with Schema.org data)
                            import re
                            jsonld_match = re.search(
                                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                                raw_content,
                                re.DOTALL | re.IGNORECASE,
                            )
                            if jsonld_match:
                                try:
                                    jsonld = orjson.loads(jsonld_match.group(1))
                                    if isinstance(jsonld, dict):
                                        structured_data = jsonld
                                        raw_row = jsonld
                                except (orjson.JSONDecodeError, Exception):
                                    pass

        elif isinstance(fixture_data, dict):
            request = fixture_data.get("request", {})
            url = request.get("url", "")
            response = fixture_data.get("response", [])

            # Parse response content
            if isinstance(response, list) and response:
                first_line = response[0]
                if isinstance(first_line, list) and first_line:
                    first_line = first_line[0]
                if isinstance(first_line, str):
                    parsed = orjson.loads(first_line)
                    content = parsed.get("content", {})
                    commonmark = content.get("commonmark", "")
                    raw_content = content.get("raw", "")
                    raw_markdown = commonmark or raw_content
                    # Try to extract structured data from JSON code blocks
                    # Handle both ```json and plain ``` code blocks
                    if "```" in raw_markdown:
                        try:
                            import re
                            # Try ```json first, then plain ```
                            json_match = re.search(r"```(?:json)?\s*\n(.*?)\n```", raw_markdown, re.DOTALL)
                            if json_match:
                                json_text = json_match.group(1)
                                # Check if it looks like JSON
                                if json_text.strip().startswith("{"):
                                    # Clean markdown-escaped underscores (common in Greenhouse API responses)
                                    cleaned_json = json_text.replace("\\_", "_")
                                    structured_data = orjson.loads(cleaned_json)
                                    if isinstance(structured_data, dict):
                                        raw_row = structured_data
                        except (orjson.JSONDecodeError, Exception):
                            pass
                    # Handle HTML-wrapped JSON (e.g., <pre>{...}</pre>)
                    if structured_data is None and raw_content and "<pre>" in raw_content:
                        try:
                            import re
                            pre_match = re.search(r"<pre>(.*?)</pre>", raw_content, re.DOTALL)
                            if pre_match:
                                parsed_raw = orjson.loads(pre_match.group(1))
                                if isinstance(parsed_raw, dict):
                                    structured_data = parsed_raw
                                    raw_row = parsed_raw
                        except (orjson.JSONDecodeError, Exception):
                            pass
                    # Handle direct JSON in raw field (e.g., Microsoft API)
                    elif not commonmark and raw_content.strip().startswith("{"):
                        try:
                            parsed_raw = orjson.loads(raw_content)
                            if isinstance(parsed_raw, dict):
                                structured_data = parsed_raw
                                raw_row = parsed_raw
                                data = parsed_raw.get("data")
                                if isinstance(data, dict) and "jobDescription" in data:
                                    raw_markdown = data.get("jobDescription", "")
                        except (orjson.JSONDecodeError, Exception):
                            pass
                elif isinstance(first_line, dict):
                    content = first_line.get("content", first_line)
                    if not isinstance(content, dict):
                        content = {}
                    commonmark_content = content.get("commonmark", "")
                    raw_html_content = content.get("raw", "")

                    def _is_trivial_commonmark(value: str) -> bool:
                        stripped = value.strip()
                        if not stripped:
                            return True
                        if len(stripped) < 200 and len(stripped.splitlines()) <= 3:
                            return True
                        return False

                    parsed_raw: dict[str, Any] | None = None
                    if raw_html_content:
                        if raw_html_content.strip().startswith("{"):
                            try:
                                parsed_raw = orjson.loads(raw_html_content)
                            except (orjson.JSONDecodeError, Exception):
                                parsed_raw = None
                        elif "<pre>" in raw_html_content:
                            try:
                                import re
                                pre_match = re.search(r"<pre>(.*?)</pre>", raw_html_content, re.DOTALL)
                                if pre_match:
                                    parsed_raw = orjson.loads(pre_match.group(1))
                            except (orjson.JSONDecodeError, Exception):
                                parsed_raw = None

                    if isinstance(parsed_raw, dict):
                        data_obj = parsed_raw.get("data")
                        if isinstance(data_obj, dict):
                            structured_data = data_obj
                            raw_row = data_obj
                        else:
                            structured_data = parsed_raw
                            raw_row = parsed_raw
                        if "content" in parsed_raw:
                            import html
                            raw_markdown = html.unescape(parsed_raw.get("content", ""))
                        elif isinstance(data_obj, dict):
                            job_desc = data_obj.get("jobDescription", "")
                            if job_desc:
                                import html
                                raw_markdown = html.unescape(job_desc)
                        if not raw_markdown:
                            raw_markdown = commonmark_content or raw_html_content
                    else:
                        # Prefer commonmark unless it's just a short title.
                        if commonmark_content and not _is_trivial_commonmark(commonmark_content):
                            raw_markdown = commonmark_content
                        elif raw_html_content and len(raw_html_content) > len(commonmark_content):
                            raw_markdown = raw_html_content
                        else:
                            raw_markdown = commonmark_content

                        # Try to extract JSON-LD from raw HTML (Ashby pages with Schema.org data)
                        if raw_html_content and not structured_data:
                            import re
                            jsonld_match = re.search(
                                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                                raw_html_content,
                                re.DOTALL | re.IGNORECASE,
                            )
                            if jsonld_match:
                                try:
                                    jsonld = orjson.loads(jsonld_match.group(1))
                                    if isinstance(jsonld, dict):
                                        structured_data = jsonld
                                        raw_row = jsonld
                                except (orjson.JSONDecodeError, Exception):
                                    pass

                    # Extract metadata as structured data (for Ashby, etc.)
                    metadata = first_line.get("metadata", {})
                    if isinstance(metadata, dict):
                        raw_meta = metadata.get("raw", {})
                        if isinstance(raw_meta, dict):
                            # Use metadata.raw as structured data if available
                            if not structured_data:
                                structured_data = raw_meta
                                raw_row = raw_meta

                    # Handle raw JSON responses (e.g., Greenhouse API, Microsoft API)
                    if not raw_markdown and content.get("raw"):
                        raw_content = content.get("raw", "")
                        # Check if raw content is JSON
                        if raw_content.strip().startswith("{"):
                            try:
                                parsed_raw = orjson.loads(raw_content)
                                if isinstance(parsed_raw, dict):
                                    structured_data = parsed_raw
                                    raw_row = parsed_raw
                                    # Extract description from Greenhouse API format
                                    if "content" in parsed_raw:
                                        # Greenhouse API returns HTML in 'content' field
                                        import html
                                        html_content = parsed_raw.get("content", "")
                                        # Unescape HTML entities
                                        unescaped = html.unescape(html_content)
                                        raw_markdown = unescaped
                                    # Handle Microsoft/Eightfold API format
                                    # Job description is in data.jobDescription
                                    elif "data" in parsed_raw and isinstance(parsed_raw["data"], dict):
                                        data_obj = parsed_raw["data"]
                                        job_desc = data_obj.get("jobDescription", "")
                                        if job_desc:
                                            import html
                                            raw_markdown = html.unescape(job_desc)
                                        # Use data object as structured_data for other fields
                                        structured_data = data_obj
                                        raw_row = data_obj
                            except (orjson.JSONDecodeError, Exception):
                                raw_markdown = raw_content
                        else:
                            raw_markdown = raw_content
            elif isinstance(response, dict):
                content = response.get("content", {})
                raw_markdown = content.get("commonmark", "") or content.get("raw", "")
                raw_content = content.get("raw", "")
                if raw_content:
                    if raw_content.strip().startswith("{"):
                        try:
                            parsed_raw = orjson.loads(raw_content)
                            if isinstance(parsed_raw, dict):
                                structured_data = parsed_raw
                                raw_row = parsed_raw
                        except (orjson.JSONDecodeError, Exception):
                            pass
                    elif "<pre>" in raw_content:
                        try:
                            import re
                            pre_match = re.search(r"<pre>(.*?)</pre>", raw_content, re.DOTALL)
                            if pre_match:
                                parsed_raw = orjson.loads(pre_match.group(1))
                                if isinstance(parsed_raw, dict):
                                    structured_data = parsed_raw
                                    raw_row = parsed_raw
                        except (orjson.JSONDecodeError, Exception):
                            pass

        # Load assertion
        assertion_data = yaml.safe_load(assertion_path.read_text(encoding="utf-8"))
        expected = assertion_data.get("expected", {})
        site_id = assertion_data.get("site_id", "")

        return cls(
            identifier=identifier,
            fixture_path=fixture_path,
            assertion_path=assertion_path,
            url=url,
            raw_markdown=raw_markdown,
            raw_row=raw_row,
            structured_data=structured_data,
            expected=expected,
            site_id=site_id,
        )


@dataclass
class ExtractorTestResult:
    """Result of running an extractor test."""

    field_name: str
    expected_value: Any
    actual_value: Any
    winning_strategy: str | None
    all_strategies: list[dict[str, Any]]
    passed: bool
    failure_reason: str = ""

    def format_failure(self) -> str:
        """Format a detailed failure message."""
        lines = [
            f"EXTRACTOR FAILURE: {self.field_name}",
            f"  Expected: {self.expected_value!r}",
            f"  Actual:   {self.actual_value!r}",
            f"  Winning Strategy: {self.winning_strategy or 'None'}",
            "",
            "  Strategy Trace (all strategies that ran):",
        ]
        for s in self.all_strategies:
            is_winner = s.get("strategy") == self.winning_strategy
            marker = " [WIN]" if is_winner else ""
            valid = "OK" if s.get("is_valid") else "FAIL"
            lines.append(
                f"    [{valid}] {s.get('strategy')}{marker}: "
                f"value={s.get('value')!r}, reason={s.get('reason')}"
            )
        return "\n".join(lines)


def run_extractor_test(
    test_case: ExtractorTestCase,
    field_name: str,
    expected_key: str | None = None,
    match_type: str = "exact",
) -> ExtractorTestResult:
    """
    Run a single extractor and compare against expected value.

    Args:
        test_case: The test case with fixture and expected values
        field_name: Field to extract (e.g., "title", "location")
        expected_key: Key in expected dict (defaults to field_name)
        match_type: How to compare - "exact", "contains", "not_null"

    Returns:
        ExtractorTestResult with detailed trace information
    """
    expected_key = expected_key or field_name

    # Get expected value from assertion
    expected_value = test_case.expected.get(expected_key)
    if expected_value is None and f"{expected_key}_contains" in test_case.expected:
        expected_value = test_case.expected[f"{expected_key}_contains"]
        match_type = "contains"
    if f"{expected_key}_not_null" in test_case.expected:
        match_type = "not_null"
        expected_value = True

    # Create extraction context
    handler = get_site_handler(test_case.url)
    context = ExtractionContext.from_scrape_result(
        url=test_case.url,
        markdown=test_case.raw_markdown,
        handler=handler,
        raw_row=test_case.raw_row,
        structured_data=test_case.structured_data,
        debug=True,  # Always run all strategies
    )

    # Run extraction
    results = extract_job_fields(context, fields=[field_name], run_all=True)
    result = results.get(field_name)

    if result is None:
        return ExtractorTestResult(
            field_name=field_name,
            expected_value=expected_value,
            actual_value=None,
            winning_strategy=None,
            all_strategies=[],
            passed=False,
            failure_reason=f"Extractor returned no result for field {field_name}",
        )

    actual_value = result.final_value
    all_strategies = [r.to_dict() for r in result.all_results]

    # Compare values based on match type
    if match_type == "exact":
        passed = actual_value == expected_value
    elif match_type == "contains":
        if actual_value is None or expected_value is None:
            passed = False
        else:
            passed = str(expected_value).lower() in str(actual_value).lower()
    elif match_type == "not_null":
        passed = actual_value is not None
    else:
        passed = actual_value == expected_value

    failure_reason = ""
    if not passed:
        failure_reason = f"Expected {match_type} match: {expected_value!r} vs actual: {actual_value!r}"

    return ExtractorTestResult(
        field_name=field_name,
        expected_value=expected_value,
        actual_value=actual_value,
        winning_strategy=result.winning_strategy,
        all_strategies=all_strategies,
        passed=passed,
        failure_reason=failure_reason,
    )


def discover_test_cases() -> list[ExtractorTestCase]:
    """
    Discover all test cases with matching fixture and ground truth files.

    Searches:
    - fixtures/dbos_schedule/*_detail.json with ground_truth/*.yml
    - fixtures/debug/**/*_detail.json with ground_truth/debug/**/*.yml
    """
    test_cases = []

    # DBOS schedule fixtures
    dbos_schedule_dir = FIXTURE_DIR / "dbos_schedule"
    if dbos_schedule_dir.exists():
        for fixture_path in dbos_schedule_dir.glob("*_detail.json"):
            identifier = fixture_path.stem.replace("_detail", "")
            assertion_path = GROUND_TRUTH_DIR / f"{identifier}.yml"
            if assertion_path.exists():
                try:
                    test_cases.append(ExtractorTestCase.from_paths(fixture_path, assertion_path))
                except Exception as e:
                    logger.warning(f"Failed to load test case {identifier}: {e}")

    # Debug fixtures (nested in company folders)
    debug_fixture_dir = FIXTURE_DIR / "debug"
    if debug_fixture_dir.exists():
        for fixture_path in debug_fixture_dir.rglob("*_detail.json"):
            identifier = fixture_path.stem.replace("_detail", "")
            # Check assertion in same company folder
            parent_name = fixture_path.parent.name
            assertion_path = GROUND_TRUTH_DIR / "debug" / parent_name / f"{identifier}.yml"
            if not assertion_path.exists():
                # Check flat structure
                assertion_path = GROUND_TRUTH_DIR / "debug" / f"{identifier}.yml"
            if assertion_path.exists():
                try:
                    test_cases.append(ExtractorTestCase.from_paths(fixture_path, assertion_path))
                except Exception as e:
                    logger.warning(f"Failed to load debug test case {identifier}: {e}")

    return test_cases


# Pre-discover test cases for parametrization
ALL_TEST_CASES = discover_test_cases()


def get_test_ids(test_cases: list[ExtractorTestCase]) -> list[str]:
    """Get test IDs for parametrization."""
    return [tc.identifier for tc in test_cases]
