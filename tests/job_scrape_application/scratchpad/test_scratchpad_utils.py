from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath("."))

from job_scrape_application.workflows.scratchpad_utils import _shrink_for_log, extract_http_exchange


def test_shrink_for_log_truncates_long_string() -> None:
    value = "a" * 500
    truncated = _shrink_for_log(value, max_chars=100)
    assert isinstance(truncated, str)
    assert truncated.startswith("a" * 100)
    assert truncated.endswith("... (+400 chars)")


def test_shrink_for_log_summarizes_large_dict() -> None:
    payload = {f"k{i}": i for i in range(10)}
    preview = _shrink_for_log(payload, max_items=3)
    assert preview == {"_type": "dict", "size": 10, "keys": ["k0", "k1", "k2"]}


def test_shrink_for_log_keeps_small_dict_with_truncation() -> None:
    payload = {"url": "x" * 50, "count": 1}
    preview = _shrink_for_log(payload, max_chars=10)
    assert preview["count"] == 1
    assert preview["url"].startswith("x" * 10)
    assert preview["url"].endswith("... (+40 chars)")


def test_extract_http_exchange_summarizes_request_response() -> None:
    scrape_result = {
        "provider": "generic",
        "jobId": "job-1",
        "statusUrl": "https://status.example",
        "request": {"headers": {"X-Test": "ok"}, "body": "b" * 20},
        "response": {f"key{i}": i for i in range(8)},
    }

    payload = extract_http_exchange(scrape_result)

    assert payload is not None
    assert payload["provider"] == "generic"
    assert payload["jobId"] == "job-1"
    assert payload["statusUrl"] == "https://status.example"
    assert payload["request"]["headers"]["X-Test"] == "ok"
    assert payload["response"]["_type"] == "dict"
    assert payload["response"]["size"] == 8
