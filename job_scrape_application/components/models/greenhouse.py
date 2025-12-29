from __future__ import annotations

import html
import json
import re
from typing import Any, Iterable, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from ...constants import title_matches_required_keywords


class GreenhouseJobLocation(BaseModel):
    name: Optional[str] = None

    model_config = ConfigDict(extra="allow")


class GreenhouseJob(BaseModel):
    absolute_url: str = Field(alias="absolute_url")
    id: int
    title: Optional[str] = None
    requisition_id: Optional[str] = Field(default=None, alias="requisition_id")
    company_name: Optional[str] = Field(default=None, alias="company_name")
    updated_at: Optional[str] = Field(default=None, alias="updated_at")
    first_published: Optional[str] = Field(default=None, alias="first_published")
    language: Optional[str] = None
    location: Optional[GreenhouseJobLocation] = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class GreenhouseBoardResponse(BaseModel):
    jobs: List[GreenhouseJob] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True, extra="allow")


_PRE_TAG_PATTERN = re.compile(r"<pre[^>]*>(.*?)</pre>", flags=re.IGNORECASE | re.DOTALL)


def _find_jobs_payload(node: Any) -> Optional[dict[str, Any]]:
    if isinstance(node, dict):
        jobs = node.get("jobs")
        if isinstance(jobs, list):
            return node
        positions = node.get("positions")
        if isinstance(positions, list):
            return node
        for child in node.values():
            found = _find_jobs_payload(child)
            if found is not None:
                return found
    elif isinstance(node, list):
        for child in node:
            found = _find_jobs_payload(child)
            if found is not None:
                return found
    return None


def _scan_json_candidates(text: str) -> Iterable[Any]:
    decoder = json.JSONDecoder()
    for match in re.finditer(r"[{[]", text):
        idx = match.start()
        try:
            parsed, _ = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, str):
            try:
                parsed = json.loads(parsed)
            except Exception:
                pass
        yield parsed


def _extract_jobs_payload_from_text(text: str) -> Optional[dict[str, Any]]:
    candidates: list[str] = []
    for match in _PRE_TAG_PATTERN.finditer(text):
        candidate = match.group(1)
        if candidate:
            candidates.append(candidate)
    candidates.append(text)

    for candidate in candidates:
        cleaned = html.unescape(candidate).strip()
        if not cleaned:
            continue
        for parsed in _scan_json_candidates(cleaned):
            found = _find_jobs_payload(parsed)
            if found is not None:
                return found
    return None


def load_greenhouse_board(raw_payload: Any) -> GreenhouseBoardResponse:
    """Normalize raw FetchFox/http payload into a typed board response.

    Accepts raw JSON string, bytes, or already-parsed mapping/list structures.
    """

    if isinstance(raw_payload, (bytes, bytearray)):
        raw_payload = raw_payload.decode()

    if isinstance(raw_payload, str):
        try:
            data = json.loads(raw_payload)
        except json.JSONDecodeError:
            extracted = _extract_jobs_payload_from_text(raw_payload)
            if extracted is None:
                raise ValueError("Greenhouse board payload was not valid JSON")
            data = extracted
    else:
        data = raw_payload

    return GreenhouseBoardResponse.model_validate(data)


def extract_greenhouse_job_urls(
    board: GreenhouseBoardResponse, required_keywords: Iterable[str] | None = None
) -> list[str]:
    """Return unique, non-empty job URLs from a board response that match title filters."""

    urls = []
    for job in board.jobs:
        if not job.absolute_url:
            continue
        if not title_matches_required_keywords(job.title, keywords=required_keywords):
            continue
        urls.append(job.absolute_url)
    # Preserve order while deduping
    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped
