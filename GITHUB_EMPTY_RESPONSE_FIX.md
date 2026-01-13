# GitHub Careers Empty Response & Error Classification Fix

## Summary

Fixed two critical bugs in the job scraping workflow related to GitHub careers scraping failures:

1. **Empty list responses treated as invalid_response** - SpiderCloud returning `[[]]` was incorrectly rejected
2. **Wrong error classification** - URLs that were all skipped (already exist in DB) were incorrectly marked as "zero_urls" errors

## Original Issue

GitHub careers scrape failed with:
```json
{
  "event": "scrape.batch.failures",
  "data": {
    "failedCount": 1,
    "failedReasonCounts": {"invalid_response": 1},
    "failedSampleUrls": ["https://www.github.careers/careers-home/jobs?keywords=engineer&sortBy=relevance&limit=100"]
  }
}
```

## Root Causes

### Bug 1: Empty List Handling
**Location:** `job_scrape_application/workflows/scrapers/spidercloud_scraper.py:3372`

When SpiderCloud returns `[[]]` (empty nested list):
1. Unwrapping logic produces `[]`
2. Code checks `if not isinstance(raw_result, dict)` → True
3. Returns `{"failed": {"reason": "invalid_response"}}`

**Problem:** Empty list is a valid "no results" response, not an invalid format.

### Bug 2: Error Classification
**Location:** `job_scrape_application/workflows/activities/__init__.py:2750`

```python
# BEFORE (bug):
return 0, should_warn_zero_urls

# AFTER (fix):
return 0, should_warn_zero_urls and not all_seen
```

**Problem:** Returned `should_warn_zero_urls` (whether URL looks like listing page) instead of considering `all_seen` (whether all URLs were skipped because they exist). This caused listings with all existing URLs to be marked as "zero_urls" errors.

## Fixes Applied

### Fix 1: Empty List Response Handling
**File:** `spidercloud_scraper.py:3368-3392`

```python
# Handle nested list responses (SpiderCloud may return [[{...}]] or [{...}])
while isinstance(raw_result, list) and raw_result:
    raw_result = raw_result[0]

# If we end up with an empty list or non-dict, treat as empty response
if not isinstance(raw_result, dict):
    # If it's an empty list, this is a valid "no results" response, not an error
    if isinstance(raw_result, list):
        raw_result = {
            "content": {"commonmark": "", "raw": ""},
            "status": 200,
            "url": original_url,
        }
    else:
        # Truly invalid response (string, number, etc.)
        return {
            "normalized": None,
            "raw": {"url": original_url, "events": [], "markdown": ""},
            "job_urls": [],
            "costMilliCents": None,
            "startedAt": started_at,
            "failed": {"url": original_url, "reason": "invalid_response"},
        }
```

**Result:** Empty list responses now continue through normal flow, properly returning 0 job URLs instead of failing as "invalid_response".

### Fix 2: Error Classification Logic
**File:** `activities/__init__.py:2750-2752`

```python
# Only mark as failed if this is a listing page AND not all URLs were just skipped
# If all URLs were skipped because they already exist, don't treat as error
return 0, should_warn_zero_urls and not all_seen
```

**Result:** When all extracted URLs already exist in database, they're logged as WARN (skip_all_seen_urls) instead of ERROR (zero_urls).

## Test Coverage

Created comprehensive test suite: `tests/job_scrape_application/workflows/test_github_empty_and_valid_response.py`

### Tests Included

1. **test_github_empty_list_response** - Verifies `[[]]` is handled as valid empty result
2. **test_github_valid_response_with_jobs** - Verifies 43 jobs from GitHub API are extracted correctly
3. **test_error_classification_all_seen_logic** - Verifies all_seen scenario doesn't cause zero_urls error
4. **test_error_classification_truly_zero_urls** - Verifies truly zero URLs still fails appropriately
5. **test_error_classification_partial_skipped** - Verifies partial skipping still fails
6. **test_error_classification_invalid_urls_present** - Verifies invalid URLs presence causes failure

### Test Results
```bash
$ uv run pytest tests/job_scrape_application/workflows/test_github_empty_and_valid_response.py -v
============================= test session starts ==============================
collected 6 items

test_github_empty_list_response PASSED [ 16%]
test_github_valid_response_with_jobs PASSED [ 33%]
test_error_classification_all_seen_logic PASSED [ 50%]
test_error_classification_truly_zero_urls PASSED [ 66%]
test_error_classification_partial_skipped PASSED [ 83%]
test_error_classification_invalid_urls_present PASSED [100%]

============================== 6 passed in 0.36s
```

## Verification

Fresh fixture from GitHub careers shows the fix works:
- **URL:** `https://www.github.careers/api/jobs?keywords=engineer`
- **Result:** 43 jobs extracted successfully
- **Fixture:** `tests/job_scrape_application/workflows/fixtures/debug/github_api_listing.json`

## Impact

### Before Fix
- ❌ Empty responses fail as "invalid_response"
- ❌ All-existing URLs fail as "zero_urls" error
- ❌ Misleading error logs and failed scrape entries

### After Fix
- ✅ Empty responses return 0 URLs cleanly
- ✅ All-existing URLs logged as WARN (skip_all_seen_urls)
- ✅ Clear distinction between genuine errors and normal scenarios
- ✅ Proper error classification aligns with user expectations

## Files Changed

1. `job_scrape_application/workflows/scrapers/spidercloud_scraper.py` - Empty list handling
2. `job_scrape_application/workflows/activities/__init__.py` - Error classification logic
3. `tests/job_scrape_application/workflows/test_github_empty_and_valid_response.py` - New test suite

## User Requirement Met

> "I don't want zero_urls if there are urls but they are all just skipped. that should give skipped error instead."

✅ **Implemented:** When URLs exist but are all skipped (already in DB), the system now:
- Logs as `scrape.listing.skip_all_seen_urls` (WARN level)
- Does NOT mark as failed with "zero_urls" error
- Distinguishes between "no URLs extracted" vs "all URLs already exist"
