from __future__ import annotations

import json
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class GreenhouseJobLocation(BaseModel):
    name: Optional[str] = None

    model_config = ConfigDict(extra="allow")


class GreenhouseJob(BaseModel):
    absolute_url: str = Field(alias="absolute_url")
    id: int
    title: str
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
            raise ValueError("Greenhouse board payload was not valid JSON")
    else:
        data = raw_payload

    return GreenhouseBoardResponse.model_validate(data)


def extract_greenhouse_job_urls(board: GreenhouseBoardResponse) -> list[str]:
    """Return unique, non-empty job URLs from a board response."""

    urls = [job.absolute_url for job in board.jobs if job.absolute_url]
    # Preserve order while deduping
    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped
