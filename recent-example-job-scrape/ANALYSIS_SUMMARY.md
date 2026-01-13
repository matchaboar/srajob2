# Job Scrape Data Quality Analysis

## Analysis Date: 2026-01-12

## Overview

Analyzed 40 most recent scraped jobs from Convex production to identify data quality issues in the job scraping pipeline.

## Issues Identified

### 1. Non-Job URLs Being Scraped

**Problem**: The scraper was treating non-job pages as job postings, including:
- Anchor fragments (`#sub-title3`, `#content`)
- Social media pages (Instagram, LinkedIn company pages)
- Privacy policy pages
- Login/authentication pages
- ESG and investor relations pages
- Accommodation/HR forms

**Examples from Production**:
- `#Sub Title3` - Anchor fragment as job title
- `https://www.instagram.com/robinhoodapp` - Instagram profile
- `https://www.linkedin.com/company/robinhood` - LinkedIn company page
- `https://privacy.coupang.com/en/notice` - Privacy page
- `https://api.greenhouse.io/en/land/jobsnotice` - Greenhouse login page
- `https://esg.robinhood.com/` - ESG page
- `https://robinhood.hracuity.net/webform/...` - HR accommodation form

### 2. Invalid Job Titles

**Problem**: Scraped pages with garbage or non-job titles:
- Markdown headers (`#Sub Title3`, `#Content`)
- Partial markdown links (`[6. Personal Information Safeguard Measures`)
- Generic page titles (`Sign In`, `Untitled`, `Login`)
- URL-as-title (`Www.Coupang.Com`)
- Navigation elements (`Members En`)

### 3. Incorrect Company Extraction

**Problem**: Language codes extracted as company names from Greenhouse API URLs:
- `company: "En"` from `/en/land/jobsnotice`
- `company: "Ko"` from `/ko/land/jobs`
- `company: "Instagram"` from Instagram URLs
- `company: "Linkedin"` from LinkedIn URLs

**Root Cause**: The `derive_company_from_url` function extracted the first path segment from Greenhouse URLs, which in API landing pages contains the language code instead of the company slug.

### 4. Suspicious Compensation Extraction

**Problem**: Non-job pages extracted compensation values from unrelated content:
- Instagram page: `totalCompensation: 290000`
- Privacy policy: `totalCompensation: 100000`

## Fixes Implemented

### 1. URL Validation (`page_detection.py`)

Added `is_invalid_job_url()` function to detect and reject:
- Anchor fragments (`#...`)
- Social media domains (instagram.com, linkedin.com, twitter.com, etc.)
- Privacy/policy pages
- Login/authentication pages
- ESG, investor, and accommodation pages
- Greenhouse API landing pages with language codes

```python
def is_invalid_job_url(url: str | None) -> bool:
    """Check if a URL is definitely not a valid job posting URL."""
```

### 2. Title Validation (`page_detection.py`)

Added `is_invalid_job_title()` function to detect and reject:
- Markdown artifacts (`#`, `[`)
- Generic page titles (`Sign In`, `Untitled`, `Login`)
- URL-like titles (`Www.`, `http://`)
- Very short titles (< 3 chars)
- Pure emoji titles

```python
def is_invalid_job_title(title: str | None) -> bool:
    """Check if a title is definitely not a valid job title."""
```

### 3. Page Type Detection (`page_detection.py`)

Added `looks_like_non_job_page()` function combining URL, title, and content analysis:
- Login page detection (by content indicators)
- Privacy page detection (by content indicators)
- Social media profile detection

```python
def looks_like_non_job_page(title, description, url) -> bool:
    """Heuristically detect pages that are not job postings."""
```

### 4. Company Extraction Fix (`company_normalization.py`)

Added `_INVALID_COMPANY_TOKENS` set containing:
- ISO 639-1 language codes (en, ko, ja, zh, de, fr, etc.)
- URL segment tokens (api, land, www, cdn, etc.)
- Generic terms (unknown, untitled, null)

Updated `is_generic_company_name()` and `derive_company_from_url()` to reject these tokens as company names.

### 5. Workflow Integration (`spidercloud_scraper.py`)

Integrated validation into `_normalize_job()`:
1. Early rejection of invalid URLs at function entry
2. Final validation before returning normalized job data:
   - Invalid title check
   - Non-job page detection

## Files Modified

1. `job_scrape_application/workflows/helpers/page_detection.py`
   - Added constants: `_NON_JOB_URL_PATTERNS`, `_NON_JOB_DOMAINS`, `_INVALID_TITLE_PATTERNS`
   - Added functions: `is_invalid_job_url()`, `is_invalid_job_title()`, `looks_like_non_job_page()`

2. `job_scrape_application/workflows/helpers/company_normalization.py`
   - Added constant: `_INVALID_COMPANY_TOKENS`
   - Updated `is_generic_company_name()` to check language codes
   - Updated `derive_company_from_url()` to skip language codes in Greenhouse paths

3. `job_scrape_application/workflows/helpers/scrape_utils.py`
   - Updated imports to include new validation functions

4. `job_scrape_application/workflows/scrapers/spidercloud_scraper.py`
   - Added imports for new validation functions
   - Added validation checks in `_normalize_job()`

## Tests Added

1. `tests/job_scrape_application/workflows/helpers/test_page_detection.py`
   - `TestIsInvalidJobUrl` - 9 test cases
   - `TestIsInvalidJobTitle` - 8 test cases
   - `TestLooksLikeNonJobPage` - 6 test cases

2. `tests/job_scrape_application/workflows/helpers/test_company_normalization.py`
   - Tests for language code rejection
   - Tests for URL segment rejection
   - Tests for Greenhouse API URL handling

## Test Results

All 204 helper module tests pass:
- 47 page detection tests
- 50 company normalization tests
- 18 link extractor tests
- 89 other helper tests

## Impact

These fixes will prevent the following issues in future scrapes:
- No more anchor fragments as job URLs
- No more social media profiles stored as jobs
- No more privacy/login pages as jobs
- No more language codes as company names
- Better filtering of non-job content before storage
