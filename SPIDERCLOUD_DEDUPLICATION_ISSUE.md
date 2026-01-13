# SpiderCloud Deduplication Issue & Solution

## Problem Summary

The DBOS scraper workflow **wastes SpiderCloud API credits** by sending job URLs to SpiderCloud even when they already exist in the Convex database.

## Current Behavior

### ✅ Listing Processing (Works Correctly)
**Location:** `process_spidercloud_listing_batch` (lines 2534-2542)

```python
# Check which URLs already exist in Convex
existing_jobs = await filter_existing_job_urls(job_urls)
existing_set = {u for u in existing_jobs if isinstance(u, str)}

# Filter them out before enqueueing
if existing_set:
    job_urls = [u for u in job_urls if u not in existing_set]
```

**Result:** Only new URLs are enqueued to the `scrape_urls` queue.

### ❌ Detail Processing (Missing Check)
**Location:** `process_spidercloud_job_batch` (lines 1747-1793)

```python
# Lease URLs from queue
for row in batch.get("urls", []):
    groups.setdefault(key, []).append(normalized_url)

# Send ALL URLs directly to SpiderCloud - NO CHECK!
result = await scraper.scrape_greenhouse_jobs(payload)
```

**Result:** ALL URLs are sent to SpiderCloud, even if they already exist in Convex.

## Why This Happens

Between when URLs are:
1. **Filtered and enqueued** (listing processing)
2. **Leased and scraped** (detail processing)

URLs could have been inserted by:
- Another concurrent workflow processing a different listing page
- Manual insertion via Convex mutations
- Duplicate URLs from different sites
- Retry/re-enqueue logic

## Proof of Issue

Tests in `tests/job_scrape_application/activities/test_spidercloud_duplicate_url_scraping.py`:

```
⚠️  CREDIT WASTE DETECTED:
   - URLs sent to SpiderCloud: 3
   - URLs already in database: 2
   - URLs that should be scraped: 1
   - Wasted SpiderCloud calls: 2
   - Estimated wasted cost: 200 milli-cents
```

## Solution

### Part 1: New Convex Function (More Efficient!)

**Created:** `filterNewJobUrls` in `job_board_application/convex/router.ts`

Instead of returning URLs that exist (old approach), return URLs that **don't** exist (new approach).

**Efficiency gain:** 19x less network transfer in typical production scenario (95% existing URLs):

```
📊 EFFICIENCY COMPARISON:
   Input: 100 URLs (95 exist, 5 new)

   OLD approach (filter_existing_job_urls):
     - Data returned: 95 URLs
     - Python processing: Build set, filter list

   NEW approach (filter_new_job_urls):
     - Data returned: 5 URLs
     - Python processing: None needed, use directly

   Network efficiency: 19.0x more data with old approach
```

### Part 2: Python Wrapper

**Created:** `filter_new_job_urls()` in `job_scrape_application/workflows/activities/__init__.py`

```python
async def filter_new_job_urls(urls: List[str]) -> List[str]:
    """
    Return only URLs that do NOT exist in Convex jobs table.

    More efficient than filter_existing_job_urls when most URLs already exist.
    """
    cleaned = [u for u in urls if isinstance(u, str) and u.strip()]
    if not cleaned:
        return []

    from ...services.convex_client import convex_query

    data = await convex_query("router:filterNewJobUrls", {"urls": cleaned})
    new_urls = data.get("new", []) if isinstance(data, dict) else []

    return [u for u in new_urls if isinstance(u, str)]
```

### Part 3: Update Detail Processing (TODO)

**Needed in:** `process_spidercloud_job_batch` (around line 1774)

```python
# After extracting URLs from batch, before sending to SpiderCloud:

# Extract all URLs from batch
all_urls = []
for row in batch.get("urls", []):
    url_val = row.get("url")
    if isinstance(url_val, str) and url_val.strip():
        all_urls.append(url_val)

# Filter to only new URLs (efficient approach!)
new_urls = await filter_new_job_urls(all_urls)
new_url_set = set(new_urls)

# Only process URLs that don't exist
for row in batch.get("urls", []):
    url_val = row.get("url")
    if url_val not in new_url_set:
        continue  # Skip existing URLs

    # ... rest of processing
```

## Benefits

1. **Reduced API costs** - Stop paying for duplicate scrapes
2. **Faster processing** - Skip existing URLs immediately
3. **Network efficiency** - 19x less data transfer in typical scenarios
4. **Clearer semantics** - Ask for what you actually want (new URLs)

## Testing

Run the test suite to verify:

```bash
uv run pytest tests/job_scrape_application/activities/test_spidercloud_duplicate_url_scraping.py -v -s
```

All 3 tests demonstrate:
1. Current credit waste issue
2. Expected behavior with deduplication
3. Efficiency improvement with new Convex function

## Next Steps

1. ✅ Create `filterNewJobUrls` Convex function (DONE)
2. ✅ Create `filter_new_job_urls()` Python wrapper (DONE)
3. ✅ Write tests demonstrating issue and solution (DONE)
4. ⏳ Update `process_spidercloud_job_batch` to use deduplication
5. ⏳ Deploy and monitor SpiderCloud API cost reduction
