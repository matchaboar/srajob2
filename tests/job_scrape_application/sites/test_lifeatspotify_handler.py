from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(".")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from job_scrape_application.workflows.site_handlers.lifeatspotify import (  # noqa: E402
    LifeAtSpotifyHandler,
)


def test_lifeatspotify_handler_matches_and_config():
    handler = LifeAtSpotifyHandler()
    listing_url = "https://www.lifeatspotify.com/jobs?l=new-york&c=backend"
    detail_url = "https://www.lifeatspotify.com/jobs/senior-machine-learning-engineer-ads-rd"
    assert handler.matches_url(listing_url)
    assert handler.matches_url(detail_url)
    assert handler.is_listing_url(listing_url)
    config = handler.get_spidercloud_config(listing_url)
    assert config.get("return_format") == ["raw_html"]
    assert "execution_scripts" in config
    assert config.get("wait_for", {}).get("selector", {}).get("selector") == "#spotify-jobs"


def test_lifeatspotify_handler_extracts_links_from_html():
    handler = LifeAtSpotifyHandler()
    html = """
    <div data-info="gm-surfaces-personalization"></div>
    <div data-info="senior-machine-learning-engineer-ads-rd"></div>
    """
    links = handler.get_links_from_raw_html(html)
    assert links == [
        "https://www.lifeatspotify.com/jobs/gm-surfaces-personalization",
        "https://www.lifeatspotify.com/jobs/senior-machine-learning-engineer-ads-rd",
    ]


def test_lifeatspotify_handler_filters_lever_payload():
    handler = LifeAtSpotifyHandler()
    payload = {
        "__source_url": "https://www.lifeatspotify.com/jobs?l=new-york&c=backend",
        "jobs": [
            {
                "text": "Senior Machine Learning Engineer - Ads R&amp;D",
                "categories": {
                    "department": "Engineering",
                    "location": "New York, NY",
                    "allLocations": ["New York, NY"],
                },
            },
            {
                "text": "Workday Payroll Specialist",
                "categories": {
                    "department": "People",
                    "location": "New York, NY",
                    "allLocations": ["New York, NY"],
                },
            },
        ],
    }
    links = handler.get_links_from_json(payload)
    assert links == [
        "https://www.lifeatspotify.com/jobs/senior-machine-learning-engineer-ads-rd"
    ]


def test_lifeatspotify_handler_normalizes_markdown():
    handler = LifeAtSpotifyHandler()
    markdown = """# Senior Machine Learning Engineer

Link copied to clipboard.

We are hiring machine learning engineers.

## Similar jobs
### Some other role

Application Senior Machine Learning Engineer
Please upload your resume.
"""
    cleaned, title = handler.normalize_markdown(markdown)
    assert title == "Senior Machine Learning Engineer"
    assert "Similar jobs" not in cleaned
    assert "Application" not in cleaned
