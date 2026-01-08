from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.site_handlers import (  # noqa: E402
    MicrosoftCareersHandler,
    get_site_handler,
)


def test_microsoft_handler_matches_and_builds_api_url():
    handler = MicrosoftCareersHandler()
    listing_url = (
        "https://apply.careers.microsoft.com/careers?"
        "query=software%20engineer&start=0&location=United%20States,%20Washington,%20Redmond&"
        "pid=1970393556656795&sort_by=timestamp&filter_distance=160&filter_include_remote=1"
    )
    detail_url = "https://apply.careers.microsoft.com/careers/job/1970393556656795"
    api_detail_url = (
        "https://apply.careers.microsoft.com/api/pcsx/position_details"
        "?position_id=1970393556656795&domain=microsoft.com&hl=en"
    )

    assert handler.matches_url(listing_url)
    assert handler.is_listing_url(listing_url)
    assert handler.matches_url(detail_url)
    assert not handler.is_listing_url(detail_url)
    assert handler.matches_url(api_detail_url)
    assert isinstance(get_site_handler(listing_url), MicrosoftCareersHandler)

    api_url = handler.get_listing_api_uri(listing_url)
    assert api_url is not None
    assert "/api/pcsx/search" in api_url
    assert "domain=microsoft.com" in api_url
    assert "start=0" in api_url
    assert "pid=" not in api_url

    detail_api_url = handler.get_api_uri(detail_url)
    assert detail_api_url is not None
    assert "/api/pcsx/position_details" in detail_api_url
    assert "position_id=1970393556656795" in detail_api_url


def test_microsoft_handler_extracts_links_and_pagination():
    handler = MicrosoftCareersHandler()
    api_url = (
        "https://apply.careers.microsoft.com/api/pcsx/search?"
        "domain=microsoft.com&query=engineer&start=0"
    )
    payload = {
        "positions": [
            {"id": 111, "positionUrl": "/careers/job/111", "postedTs": 1700000000},
            {"id": 222, "positionUrl": "/careers/job/222"},
            {"id": 333, "positionUrl": "/careers/job/333"},
            {"id": 444, "positionUrl": "/careers/job/444"},
            {"id": 555, "positionUrl": "/careers/job/555"},
            {"id": 666, "positionUrl": "/careers/job/666"},
            {"id": 777, "positionUrl": "/careers/job/777"},
            {"id": 888, "positionUrl": "/careers/job/888"},
            {"id": 999, "positionUrl": "/careers/job/999"},
            {"id": 1010, "positionUrl": "/careers/job/1010"},
        ],
        "count": 25,
    }

    links = handler.get_links_from_json(payload)
    assert "https://apply.careers.microsoft.com/careers/job/111" in links
    assert "https://apply.careers.microsoft.com/careers/job/1010" in links
    assert all(link.startswith("https://apply.careers.microsoft.com/careers/job/") for link in links)

    pagination = handler.get_pagination_urls_from_json(payload, api_url)
    assert any("start=10" in url for url in pagination)
    assert any("start=20" in url for url in pagination)


def test_microsoft_handler_extracts_links_from_html_and_posted_ts():
    handler = MicrosoftCareersHandler()
    html = (
        '<a href="/careers/job/1970393556656795">Software Engineer II</a>'
        '<a href="https://apply.careers.microsoft.com/careers/job/1970393556657166">Senior Engineer</a>'
    )
    links = handler.get_links_from_raw_html(html)
    assert "https://apply.careers.microsoft.com/careers/job/1970393556656795" in links
    assert "https://apply.careers.microsoft.com/careers/job/1970393556657166" in links

    payload = {
        "positions": [
            {"id": 1970393556656795, "postedTs": 1767823358},
            {"id": 1970393556657166, "postedTs": 1767822813},
        ]
    }
    posted = handler.extract_posted_at(payload, "https://apply.careers.microsoft.com/careers/job/1970393556656795")
    assert posted == 1767823358


def test_microsoft_handler_spidercloud_config():
    handler = MicrosoftCareersHandler()
    listing_url = "https://apply.careers.microsoft.com/careers?query=engineer"
    api_url = "https://apply.careers.microsoft.com/api/pcsx/search?domain=microsoft.com&query=engineer"
    detail_url = "https://apply.careers.microsoft.com/careers/job/1970393556656795"
    detail_api_url = (
        "https://apply.careers.microsoft.com/api/pcsx/position_details"
        "?position_id=1970393556656795&domain=microsoft.com&hl=en"
    )

    listing_config = handler.get_spidercloud_config(listing_url)
    assert listing_config.get("return_format") == ["raw_html"]
    assert listing_config.get("request") == "chrome"

    api_config = handler.get_spidercloud_config(api_url)
    assert api_config.get("return_format") == ["raw_html"]
    assert api_config.get("request") == "standard"

    detail_config = handler.get_spidercloud_config(detail_url)
    assert detail_config.get("return_format") == ["raw_html"]
    assert detail_config.get("request") == "chrome"
    assert "execution_scripts" not in detail_config

    detail_api_config = handler.get_spidercloud_config(detail_api_url)
    assert detail_api_config.get("return_format") == ["raw_html"]
    assert detail_api_config.get("request") == "standard"
