from __future__ import annotations

import json
import re
import html as html_lib
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from .base import BaseSiteHandler
from ..helpers.link_extractors import normalize_url, strip_wrapping_url
from ...components.models.greenhouse import (
    extract_greenhouse_job_urls,
    load_greenhouse_board,
)
from ..helpers.regex_patterns import (
    HTML_LINE_BREAK_PATTERN,
    HTML_LIST_ITEM_OPEN_PATTERN,
    HTML_PARAGRAPH_CLOSE_PATTERN,
    HTML_PARAGRAPH_OPEN_PATTERN,
    HTML_SCRIPT_OR_STYLE_BLOCK_PATTERN,
    HTML_TAG_PATTERN,
    HORIZONTAL_WHITESPACE_PATTERN,
    JOB_ID_PATH_PATTERN,
    LINE_WRAPPED_WHITESPACE_PATTERN,
    MULTI_NEWLINE_PATTERN,
)



class GreenhouseHandler(BaseSiteHandler):
    name = "greenhouse"
    site_type = "greenhouse"

    _BOARD_SLUG_OVERRIDES = {
        "datadoghq": "datadog",
        "hioscar": "oscar",
    }

    def _normalize_slug(self, slug: Optional[str]) -> Optional[str]:
        if not isinstance(slug, str):
            return None
        cleaned = slug.strip()
        if not cleaned:
            return None
        override = self._BOARD_SLUG_OVERRIDES.get(cleaned.lower())
        return override or cleaned

    @classmethod
    def matches_url(cls, url: str) -> bool:
        if "gh_jid" in url:
            return True
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return "greenhouse.io" in host

    def is_listing_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if self._extract_job_id_from_url(url):
            return False
        if self.is_api_detail_url(url) or self.get_api_uri(url):
            return False
        host = (parsed.hostname or "").lower()
        if not host:
            return False
        path = (parsed.path or "").lower().rstrip("/")
        if host.endswith("greenhouse.io") and path.endswith("/jobs"):
            return True
        if host.endswith("boards.greenhouse.io"):
            parts = [p for p in path.split("/") if p]
            return len(parts) <= 2
        return False

    # Non-job Greenhouse subdomains that should be filtered out during listing extraction
    _INVALID_GREENHOUSE_HOSTS = frozenset({
        "learn.greenhouse.io",
        "support.greenhouse.io",
        "my.greenhouse.io",
        "www.greenhouse.io",
        "www.greenhouse.com",
        "greenhouse.com",
        "greenhouse.io",
        "job-seekers.cdn.greenhouse.io",
        "api.greenhouse.io",  # API listing URLs, not job detail URLs
    })

    def _is_valid_job_url(self, url: str) -> bool:
        """Check if a URL is a valid Greenhouse job detail URL.

        Filters out:
        - Greenhouse marketing/support/learning subdomains (.io and .com)
        - CDN asset URLs (.css, .js files)
        - Authentication and login URLs
        - Non-job board greenhouse.io subdomains
        """
        try:
            parsed = urlparse(url)
        except Exception:
            return False

        host = (parsed.hostname or "").lower()
        path = (parsed.path or "").lower()

        # Check for gh_jid parameter - this is always a valid job URL
        if "gh_jid" in url:
            return True

        # Filter out non-job Greenhouse hosts (marketing, support, learning, etc.)
        if host in self._INVALID_GREENHOUSE_HOSTS:
            return False

        # Filter out greenhouse.com entirely - it's the marketing site
        if host.endswith(".greenhouse.com") or host == "greenhouse.com":
            return False

        # Filter out CDN asset URLs (CSS, JS, images, etc.)
        if host.endswith("cdn.greenhouse.io"):
            return False

        # Filter out URLs ending in asset extensions
        if path.endswith((".css", ".js", ".png", ".jpg", ".svg", ".gif", ".woff", ".woff2", ".ttf")):
            return False

        # Filter out authentication/login URLs on any greenhouse domain
        if "greenhouse.io" in host:
            auth_paths = ("/auth/", "/login", "/sign_in", "/sign_up", "/password", "/checkout/")
            if any(auth_path in path for auth_path in auth_paths):
                return False

        # For greenhouse.io subdomains, require valid job board patterns
        if host.endswith("greenhouse.io"):
            # boards.greenhouse.io/company/jobs/id or job-boards.greenhouse.io/company/jobs/id
            if host.startswith(("boards.", "job-boards.")):
                parts = [p for p in path.split("/") if p]
                # Need at least company/jobs/id pattern
                if len(parts) >= 3 and parts[1] == "jobs":
                    return True
                return False

            # boards-api.greenhouse.io/v1/boards/company/jobs/id
            if host.startswith("boards-api."):
                parts = [p for p in path.split("/") if p]
                if len(parts) >= 5 and parts[3] == "jobs":
                    return True
                return False

            # Other greenhouse.io subdomains are likely not job URLs
            return False

        # Non-greenhouse URLs are allowed (they might be company career sites with gh_jid param checked above,
        # or they may be completely unrelated to Greenhouse)
        return True

    def filter_job_urls(self, urls: List[str]) -> List[str]:
        filtered: List[str] = []
        seen: set[str] = set()
        for url in urls:
            if not isinstance(url, str):
                continue
            cleaned = strip_wrapping_url(url)
            if not cleaned:
                continue
            normalized = normalize_url(cleaned) or cleaned.strip()
            if not normalized:
                continue
            normalized = self._canonicalize_gh_jid_url(normalized)
            if normalized in seen:
                continue
            # Check if this is a valid job URL before adding
            if not self._is_valid_job_url(normalized):
                continue
            seen.add(normalized)
            filtered.append(normalized)
        return filtered

    def _canonicalize_gh_jid_url(self, url: str) -> str:
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        if not parsed.query:
            return url
        params = parse_qs(parsed.query, keep_blank_values=True)
        gh_jid_value: Optional[str] = None
        gh_keys: list[str] = []
        for key, values in params.items():
            normalized_key = re.sub(r"_+", "_", key.replace("/", "_")).lower()
            if normalized_key == "gh_jid":
                gh_keys.append(key)
                if values and gh_jid_value is None:
                    gh_jid_value = str(values[0]).strip().strip("/")
        if gh_jid_value is None:
            return url
        for key in gh_keys:
            params.pop(key, None)
        params["gh_jid"] = [gh_jid_value]
        query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=query))

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
                return self._normalize_slug(slug)
        parts = [p for p in parsed.path.split("/") if p]
        if "boards" in parts:
            idx = parts.index("boards")
            if idx + 1 < len(parts):
                return self._normalize_slug(parts[idx + 1])
        if len(parts) >= 2 and parts[0] == "v1" and parts[1] == "boards":
            if len(parts) >= 3:
                return self._normalize_slug(parts[2])
        if "job-board" in parts:
            idx = parts.index("job-board")
            if idx + 1 < len(parts):
                candidate = parts[idx + 1]
                if candidate and candidate not in {"job", "jobs"}:
                    return self._normalize_slug(candidate)
        host = (parsed.hostname or "").lower()
        if host and "greenhouse.io" not in host:
            host_parts = host.split(".")
            if len(host_parts) >= 2 and host_parts[-2]:
                return self._normalize_slug(host_parts[-2])
        if len(parts) >= 2 and parts[1] == "jobs":
            return self._normalize_slug(parts[0])
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

    def _is_listing_api_url(self, uri: str) -> bool:
        try:
            parsed = urlparse(uri)
        except Exception:
            return False
        host = (parsed.hostname or "").lower()
        if "api.greenhouse.io" not in host and "boards-api.greenhouse.io" not in host:
            return False
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) < 4:
            return False
        if parts[0] != "v1" or parts[1] != "boards":
            return False
        return parts[3] == "jobs" and len(parts) == 4

    def get_api_uri(self, uri: str, source_url: Optional[str] = None) -> Optional[str]:
        if self.is_api_detail_url(uri):
            return uri
        job_id = self._extract_job_id_from_url(uri)
        if not job_id:
            return None
        slug = self._extract_slug_from_url(uri)
        # If source_url is an API URL, prefer its board slug over domain-derived slug
        if source_url:
            source_slug = self._extract_slug_from_url(source_url)
            if source_slug:
                try:
                    parsed = urlparse(uri)
                    uri_host = (parsed.hostname or "").lower()
                    # Only override if the uri slug came from a non-API domain
                    if uri_host and "greenhouse.io" not in uri_host:
                        slug = source_slug
                except Exception:
                    pass
        if not slug:
            return None
        return f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}"

    def get_listing_api_uri(self, uri: str) -> Optional[str]:
        slug = self._extract_slug_from_url(uri)
        if not slug:
            return None
        try:
            parsed = urlparse(uri)
        except Exception:
            parsed = None
        host = (parsed.hostname or "").lower() if parsed else ""
        scheme = "https"
        if parsed and parsed.scheme:
            scheme = parsed.scheme
        if host.startswith("api.greenhouse.io"):
            return f"{scheme}://api.greenhouse.io/v1/boards/{slug}/jobs"
        if "boards-api.greenhouse.io" in host:
            return f"{scheme}://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
        return f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"

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
        try:
            board = load_greenhouse_board(payload)
        except Exception:
            board = None
        if board is not None:
            urls = extract_greenhouse_job_urls(board)
            if urls:
                return urls
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

    def _pick_date_value(self, node: Any, keys: tuple[str, ...]) -> Any | None:
        """Extract a date value from a node by checking the given keys in order."""
        if not isinstance(node, dict):
            return None
        for key in keys:
            value = node.get(key)
            if isinstance(value, str):
                cleaned = value.strip()
                if cleaned:
                    return cleaned
            elif isinstance(value, (int, float)):
                return value
        return None

    def _find_job_node(self, payload: Any, url: str | None) -> Any | None:
        """Find the matching job node in a jobs list based on URL or job_id."""
        jobs = payload.get("jobs")
        if not isinstance(jobs, list) or not url:
            return None
        job_id = self._extract_job_id_from_url(url)
        for job in jobs:
            if not isinstance(job, dict):
                continue
            if job_id is not None:
                candidate_id = job.get("id") or job.get("job_id") or job.get("internal_job_id")
                if candidate_id is not None and str(candidate_id) == str(job_id):
                    return job
            absolute_url = job.get("absolute_url")
            if isinstance(absolute_url, str) and absolute_url.strip() == url:
                return job
        return None

    def extract_posted_at(self, payload: Any, url: str | None = None) -> Any | None:
        """Extract posted_at, prioritizing updated_at over first_published.

        This ensures the "posted" date reflects when the job was last updated,
        which is more relevant for job seekers.
        """
        # Priority: updated_at > first_published > created_at
        keys = (
            "updated_at",
            "updatedAt",
            "first_published",
            "firstPublished",
            "created_at",
            "createdAt",
        )

        if not isinstance(payload, dict):
            return None

        direct = self._pick_date_value(payload, keys)
        if direct is not None:
            return direct

        matched_job = self._find_job_node(payload, url)
        if matched_job is not None:
            return self._pick_date_value(matched_job, keys)

        return None

    def extract_first_published(self, payload: Any, url: str | None = None) -> Any | None:
        """Extract the first_published date for preserving original posting date.

        Returns the first_published date if present, or None if not available.
        This is used to populate postingFirstPublishedAt in the database.
        """
        keys = (
            "first_published",
            "firstPublished",
        )

        if not isinstance(payload, dict):
            return None

        direct = self._pick_date_value(payload, keys)
        if direct is not None:
            return direct

        matched_job = self._find_job_node(payload, url)
        if matched_job is not None:
            return self._pick_date_value(matched_job, keys)

        return None

    def extract_company(self, payload: Any, url: str | None = None) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        for key in ("company_name", "companyName", "company"):
            value = payload.get(key)
            if isinstance(value, str):
                cleaned = value.strip()
                if cleaned:
                    return cleaned
        return None

    def get_spidercloud_config(self, uri: str) -> Dict[str, Any]:
        if not self.matches_url(uri):
            return {}
        if self._is_listing_api_url(uri):
            return self._apply_page_links_config(
                {
                "request": "basic",
                "return_format": ["raw"],
                "follow_redirects": True,
                "redirect_policy": "Loose",
                "external_domains": ["*"],
                "preserve_host": True,
                }
            )
        if self.is_api_detail_url(uri):
            # API detail URLs return raw JSON, use "raw" format (same as listing API)
            return self._apply_page_links_config(
                {
                "request": "basic",
                "return_format": ["raw"],
                "follow_redirects": True,
                "redirect_policy": "Loose",
                "external_domains": ["*"],
                "preserve_host": False,
                }
            )
        return self._apply_page_links_config(
            {
            "request": "chrome",
            "return_format": ["commonmark", "raw_html"],
            "follow_redirects": True,
            "redirect_policy": "Loose",
            "external_domains": ["*"],
            "preserve_host": True,
            }
        )

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
            # Handle literal \uXXXX escape sequences in the content
            # These appear when the Greenhouse API returns encoded content
            if "\\u00" in html_body:
                try:
                    # Decode literal unicode escapes (e.g., \u0026 -> &)
                    html_body = html_body.encode("utf-8").decode("unicode_escape")
                    # Then re-decode UTF-8 multi-byte sequences that were split
                    html_body = html_body.encode("latin-1").decode("utf-8")
                except (UnicodeDecodeError, UnicodeEncodeError):
                    # Fallback: just decode unicode escapes
                    try:
                        html_body = html_body.encode("utf-8").decode("unicode_escape")
                    except (UnicodeDecodeError, UnicodeEncodeError):
                        pass
            else:
                # Handle improperly decoded UTF-8 bytes (chars in 0x80-0xff range)
                try:
                    html_body = html_body.encode("latin-1").decode("utf-8")
                except (UnicodeDecodeError, UnicodeEncodeError):
                    pass
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
            # Unescape markdown backslash escapes (e.g., \_ -> _) before parsing JSON
            # SpiderCloud returns commonmark format which escapes special characters
            unescaped = content.replace("\\_", "_").replace("\\#", "#").replace("\\*", "*")
            data = json.loads(unescaped)
            title = data.get("title") if isinstance(data, dict) else None
            desc = _html_to_text(data.get("content") or "") if isinstance(data, dict) else ""
            if title and desc:
                return f"{title}\n\n{desc}".strip(), title
            if title:
                return title, title
        except Exception:
            return markdown, None

        return markdown, None

    def extract_location_hint(self, markdown: str) -> Optional[str]:
        """
        Extract location from Greenhouse API JSON response.
        The JSON contains location.name and offices[].name fields.
        Handles both:
        - Raw JSON wrapped in triple backticks (commonmark format)
        - Raw HTML with JSON in <pre> tags
        """
        if not markdown:
            return None

        content = markdown.strip()

        # Handle triple backticks (commonmark format)
        if content.startswith("```") and content.endswith("```"):
            content = content.strip("`\n ")

        # Handle raw HTML with <pre> tags containing JSON
        if "<pre>" in content and "</pre>" in content:
            pre_match = re.search(r"<pre>({.+})</pre>", content, flags=re.DOTALL)
            if pre_match:
                content = html_lib.unescape(pre_match.group(1))

        # Try to parse as JSON
        try:
            unescaped = content.replace("\\_", "_").replace("\\#", "#").replace("\\*", "*")
            data = json.loads(unescaped)
            if not isinstance(data, dict):
                return None

            # Try location.name first (Greenhouse API format)
            location = data.get("location")
            if isinstance(location, dict):
                name = location.get("name")
                if isinstance(name, str) and name.strip():
                    return name.strip()

            # Fall back to offices[].name
            offices = data.get("offices")
            if isinstance(offices, list) and offices:
                for office in offices:
                    if isinstance(office, dict):
                        name = office.get("name") or office.get("location")
                        if isinstance(name, str) and name.strip():
                            return name.strip()
        except Exception:
            pass

        return None
