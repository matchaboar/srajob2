from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from typing import Any

from .queue import enqueue_scrape_urls, queue_status


def _json_response(handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class WorkflowApiHandler(BaseHTTPRequestHandler):
    server_version = "DBOSWorkflowAPI/1.0"

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/api/workflows/enqueue-listing":
            self._handle_enqueue(url_type="listing")
            return
        if self.path == "/api/workflows/enqueue-detail":
            self._handle_enqueue(url_type="detail")
            return
        _json_response(self, {"error": "Not found"}, status=404)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/api/workflows/status":
            _json_response(self, queue_status())
            return
        _json_response(self, {"error": "Not found"}, status=404)

    def _handle_enqueue(self, *, url_type: str) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            _json_response(self, {"error": "Invalid JSON"}, status=400)
            return

        if url_type == "listing":
            listing_url = payload.get("listingUrl") or payload.get("url")
            listing_urls = payload.get("listingUrls") or payload.get("urls")
            urls = []
            if isinstance(listing_url, str):
                urls.append(listing_url)
            if isinstance(listing_urls, list):
                urls.extend([u for u in listing_urls if isinstance(u, str)])
        else:
            detail_url = payload.get("detailUrl") or payload.get("url")
            detail_urls = payload.get("detailUrls") or payload.get("urls")
            urls = []
            if isinstance(detail_url, str):
                urls.append(detail_url)
            if isinstance(detail_urls, list):
                urls.extend([u for u in detail_urls if isinstance(u, str)])

        if not urls:
            _json_response(self, {"error": "No URLs provided"}, status=400)
            return

        delays = payload.get("delaysMs") if isinstance(payload.get("delaysMs"), list) else None
        url_types = [url_type for _ in urls]
        enqueue_payload = {
            "urls": urls,
            "sourceUrl": payload.get("sourceUrl") or payload.get("listingUrl") or payload.get("detailUrl"),
            "provider": payload.get("provider"),
            "siteId": payload.get("siteId"),
            "pattern": payload.get("pattern"),
            "delaysMs": delays,
            "urlTypes": url_types,
        }
        force_refresh = bool(payload.get("forceRefresh"))
        res = enqueue_scrape_urls(enqueue_payload, force_refresh=force_refresh)
        _json_response(self, res)


def serve(host: str = "0.0.0.0", port: int = 8080) -> None:
    ThreadingHTTPServer.allow_reuse_address = True
    server = ThreadingHTTPServer((host, port), WorkflowApiHandler)
    server.serve_forever()
