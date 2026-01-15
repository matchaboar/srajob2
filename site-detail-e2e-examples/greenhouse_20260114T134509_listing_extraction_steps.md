# Listing Extraction Steps: greenhouse_20260114T134509

**Listing URL:** `https://api.greenhouse.io/v1/boards/togetherai/jobs`
**Source URL:** `https://api.greenhouse.io/v1/boards/togetherai/jobs`
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
  "url": "https://api.greenhouse.io/v1/boards/togetherai/jobs",
  "source_url": "https://api.greenhouse.io/v1/boards/togetherai/jobs",
  "handler": "GreenhouseHandler"
}
```

### Workflow Setup

Set up WorkflowTestHelper with mocked dependencies

```json
{
  "sync_mode": true,
  "listing_url": "https://api.greenhouse.io/v1/boards/togetherai/jobs"
}
```

### Workflow Execution

Calling process_spidercloud_listing_batch()

```json
{
  "urls": [
    {
      "url": "https://api.greenhouse.io/v1/boards/togetherai/jobs",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/togetherai/jobs",
      "provider": "spidercloud",
      "siteId": "greenhouse_20260114T134509",
      "urlType": "listing"
    }
  ]
}
```

### Workflow Complete

Workflow returned, enqueued 29 URLs

```json
{
  "response": {
    "queued": 29,
    "listingCompleted": 1,
    "sourceUrl": "https://api.greenhouse.io/v1/boards/togetherai/jobs"
  },
  "enqueued_count": 29,
  "completed_count": 1
}
```

### Extraction Complete (Production)

Extracted 29 URLs via production workflow

```json
{
  "extracted_count": 29,
  "sample_urls": [
    "https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4745173007",
    "https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4840844007",
    "https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/5004498007",
    "https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4710363007",
    "https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4903661007"
  ]
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 29
**URLs After Filtering:** 29
**URLs After Normalization:** 29
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4745173007`
2. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4840844007`
3. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/5004498007`
4. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4710363007`
5. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4903661007`
6. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4687884007`
7. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4626694007`
8. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4385540007`
9. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4835988007`
10. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4987660007`
11. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4967737007`
12. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/5012785007`
13. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4622781007`
14. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4835763007`
15. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4990627007`
16. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4737079007`
17. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4774159007`
18. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4949454007`
19. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4998021007`
20. `https://boards-api.greenhouse.io/v1/boards/togetherai/jobs/4187998007`
... and 9 more

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 29

