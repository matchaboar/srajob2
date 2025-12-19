from __future__ import annotations

import json
import re
import html as html_lib
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from .base import BaseSiteHandler

JOB_ID_PATH_PATTERN = r"/jobs/(\\d+)"
HTML_LINE_BREAK_PATTERN = r"<br\\s*/?>"
HTML_PARAGRAPH_CLOSE_PATTERN = r"</p\\s*>"
HTML_PARAGRAPH_OPEN_PATTERN = r"<p[^>]*>"
HTML_LIST_ITEM_OPEN_PATTERN = r"<li[^>]*>"
HTML_SCRIPT_OR_STYLE_BLOCK_PATTERN = r"<(script|style)[^>]*>.*?</\\1>"
HTML_TAG_PATTERN = r"<[^>]+>"
HORIZONTAL_WHITESPACE_PATTERN = r"[ \t]+"
LINE_WRAPPED_WHITESPACE_PATTERN = r"\\s*\\n\\s*"
MULTI_NEWLINE_PATTERN = r"\\n{3,}"


class GreenhouseHandler(BaseSiteHandler):
    name = "greenhouse"
    site_type = "greenhouse"

    @classmethod
    def matches_url(cls, url: str) -> bool:
        if "gh_jid" in url:
            return True
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return "greenhouse.io" in host

    def _extract_slug_from_url(self, url: str) -> Optional[str]:
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        query = parse_qs(parsed.query)
        board_param = query.get("board")
        if board_param:
            slug = board_param[0].strip()
            if slug:
                return slug
        parts = [p for p in parsed.path.split("/") if p]
        if "boards" in parts:
            idx = parts.index("boards")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        if len(parts) >= 2 and parts[0] == "v1" and parts[1] == "boards":
            if len(parts) >= 3:
                return parts[2]
        host = (parsed.hostname or "").lower()
        host_parts = host.split(".")
        if len(host_parts) >= 3 and host_parts[-2] != "greenhouse":
            return host_parts[-2]
        return None

    def _extract_job_id_from_url(self, url: str) -> Optional[str]:
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        query = parse_qs(parsed.query)
        gh_jid = query.get("gh_jid", [])
        if gh_jid:
            return gh_jid[0]
        match = re.search(JOB_ID_PATH_PATTERN, parsed.path)
        if match:
            return match.group(1)
        return None

    def is_api_detail_url(self, uri: str) -> bool:
        try:
            parsed = urlparse(uri)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        return "boards-api.greenhouse.io" in host and "/jobs/" in parsed.path

    def get_api_uri(self, uri: str) -> Optional[str]:
        if self.is_api_detail_url(uri):
            return uri
        job_id = self._extract_job_id_from_url(uri)
        if not job_id:
            return None
        slug = self._extract_slug_from_url(uri)
        if not slug:
            return None
        return f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}"

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        slug = self._extract_slug_from_url(uri)
        if not slug:
            return None
        return f"https://boards.greenhouse.io/v1/boards/{slug}/jobs"

    def get_company_uri(self, uri: str) -> Optional[str]:
        try:
            parsed = urlparse(uri)
        except Exception:
            parsed = None
        if parsed and "boards-api.greenhouse.io" in (parsed.hostname or "").lower():
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) >= 5 and parts[0] == "v1" and parts[1] == "boards":
                slug = parts[2]
                job_id = parts[4]
                return f"https://boards.greenhouse.io/{slug}/jobs/{job_id}"
        api_url = self.get_api_uri(uri)
        if api_url and api_url != uri:
            return self.get_company_uri(api_url)
        return None

    def get_links_from_json(self, payload: Any) -> List[str]:
        if not isinstance(payload, dict):
            return []
        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            return []
        urls: List[str] = []
        seen: set[str] = set()
        for job in jobs:
            if not isinstance(job, dict):
                continue
            url = job.get("absolute_url")
            if isinstance(url, str) and url.strip():
                cleaned = url.strip()
                if cleaned not in seen:
                    seen.add(cleaned)
                    urls.append(cleaned)
        return urls

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        if self.is_api_detail_url(uri):
            return {
                "request": "chrome",
                "return_format": ["raw_html"],
                "follow_redirects": True,
                "redirect_policy": "Loose",
                "external_domains": ["*"],
                "preserve_host": False,
            }
        return {}

    def normalize_markdown(self, markdown: str) -> tuple[str, Optional[str]]:
        """
        Parse SpiderCloud commonmark that wraps Greenhouse job JSON in a ``` block.
        Returns plain-text description and title when possible.
        """

        if not markdown:
            return "", None

        content = markdown.strip()
        if content.startswith("```") and content.endswith("```"):
            content = content.strip("`\n ")

        def _html_to_text(html_body: str) -> str:
            html_body = html_lib.unescape(html_body or "")
            html_body = re.sub(HTML_LINE_BREAK_PATTERN, "\n", html_body, flags=re.IGNORECASE)
            html_body = re.sub(HTML_PARAGRAPH_CLOSE_PATTERN, "\n\n", html_body, flags=re.IGNORECASE)
            html_body = re.sub(HTML_PARAGRAPH_OPEN_PATTERN, "", html_body, flags=re.IGNORECASE)
            html_body = re.sub(HTML_LIST_ITEM_OPEN_PATTERN, "- ", html_body, flags=re.IGNORECASE)
            html_body = re.sub(
                HTML_SCRIPT_OR_STYLE_BLOCK_PATTERN,
                " ",
                html_body,
                flags=re.DOTALL | re.IGNORECASE,
            )
            html_body = re.sub(HTML_TAG_PATTERN, " ", html_body)
            html_body = re.sub(HORIZONTAL_WHITESPACE_PATTERN, " ", html_body)
            html_body = re.sub(LINE_WRAPPED_WHITESPACE_PATTERN, "\n", html_body)
            html_body = re.sub(MULTI_NEWLINE_PATTERN, "\n\n", html_body)
            return html_body.strip()

        try:
            data = json.loads(content)
            title = data.get("title") if isinstance(data, dict) else None
            desc = _html_to_text(data.get("content") or "") if isinstance(data, dict) else ""
            if title and desc:
                return f"{title}\n\n{desc}".strip(), title
            if title:
                return title, title
        except Exception:
            return markdown, None

        return markdown, None
