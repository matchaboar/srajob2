"""DBOS workflows and activities for scraping jobs.

This package provides:
- Activity functions for scraping, storing, and processing jobs
- Site handlers for extracting job data from various career sites
- Core dependency injection infrastructure for testing

Key modules:
- activities/: Activity functions for scraping, storage, queue management
- site_handlers/: Site-specific extraction handlers
- scrapers/: Provider-specific scraper implementations (SpiderCloud, etc.)
- helpers/: Shared utilities for extraction and normalization
- core/: Dependency injection infrastructure

Archived (in _archive/):
- Temporal workflow code (replaced by DBOS runner in dbos_runtime/)
"""
