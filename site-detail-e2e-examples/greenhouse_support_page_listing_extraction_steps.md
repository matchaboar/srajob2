# Listing Extraction Steps: greenhouse_support_page

**Listing URL:** `https://support.greenhouse.io/hc/en-us`
**Source URL:** `https://support.greenhouse.io/hc/en-us`
**Handler:** `GreenhouseHandler`
**Content Type:** `Unknown`

---

## Step 1: SpiderCloud Response

Raw content from SpiderCloud scrape:

```markdown
(No raw content captured)
```

---

## Step 2: Handler Detection

**Detected Handler:** `GreenhouseHandler`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's listing page format
- Extract job URLs from JSON API responses or HTML
- Identify pagination links
- Filter out non-job URLs

---

## Step 3: URL Extraction Method

**Method Used:** `production_workflow`

URL extraction methods (in priority order):
1. **JSON API**: Parse structured JSON response with job array
2. **HTML Links**: Extract href attributes from anchor tags
3. **Regex Fallback**: Search for URL patterns in raw text

---

## Step 4: Detailed Extraction Log

### Handler Detection (Production)

Detected handler: GreenhouseHandler

```json
{
  "url": "https://support.greenhouse.io/hc/en-us",
  "source_url": "https://support.greenhouse.io/hc/en-us",
  "handler": "GreenhouseHandler"
}
```

### Workflow Setup

Set up WorkflowTestHelper with mocked dependencies

```json
{
  "sync_mode": false,
  "listing_url": "https://support.greenhouse.io/hc/en-us"
}
```

### Workflow Execution

Calling process_spidercloud_listing_batch()

```json
{
  "urls": [
    {
      "url": "https://support.greenhouse.io/hc/en-us",
      "sourceUrl": "https://support.greenhouse.io/hc/en-us",
      "provider": "spidercloud",
      "siteId": "greenhouse_support_page",
      "urlType": "listing"
    }
  ]
}
```

### Workflow Complete

Workflow returned, enqueued 1 URLs

```json
{
  "response": {
    "queued": 1,
    "listingCompleted": 1,
    "sourceUrl": "https://support.greenhouse.io/hc/en-us"
  },
  "enqueued_count": 1,
  "completed_count": 1
}
```

### Extraction Complete (Production)

Extracted 1 URLs via production workflow

```json
{
  "extracted_count": 1,
  "sample_urls": [
    "https://boards-api.greenhouse.io/v1/boards/stripe/jobs/7654321"
  ]
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 1
**URLs After Filtering:** 1
**URLs After Normalization:** 1
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://boards-api.greenhouse.io/v1/boards/stripe/jobs/7654321`

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 1

