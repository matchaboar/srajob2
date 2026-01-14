# Extraction Steps: greenhouse

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/coupang/jobs/7355561`
**Source URL:** `https://boards-api.greenhouse.io/v1/boards/coupang/jobs/7355561`
**Handler:** `greenhouse`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
(No raw markdown captured)
```

---

## Step 2: Handler Detection

**Detected Handler:** `greenhouse`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
(No normalized markdown captured - handler may not implement normalize_markdown)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: greenhouse

```json
{
  "url": "https://boards-api.greenhouse.io/v1/boards/coupang/jobs/7355561",
  "handler": "greenhouse"
}
```

### Raw Content Capture

Captured 0 chars of commonmark content

```json
{
  "length": 0,
  "content_type": "commonmark"
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://boards-api.greenhouse.io/v1/boards/coupang/jobs/7355561",
      "sourceUrl": "https://boards-api.greenhouse.io/v1/boards/coupang/jobs/7355561",
      "provider": "spidercloud",
      "siteId": "greenhouse",
      "pattern": null,
      "urlType": "detail"
    }
  ]
}
```

### Workflow Complete

Workflow returned, captured 1 scrapes, 0 ingested jobs

```json
{
  "stored_scrapes": 1,
  "ingested_jobs": 0,
  "description_uploads": 0
}
```

---

## Step 5: Extracted Job Details

*No jobs extracted*

---

## Step 6: Convex Mutation Payload

**Ingested Jobs Count:** 0
**Stored Scrapes Count:** 1
**Description Uploads Count:** 0

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": "https://boards-api.greenhouse.io/v1/boards/coupang/jobs/7355561",
  "provider": "spidercloud",
  "costMilliCents": 4,
  "items_keys": [
    "normalized",
    "page_links",
    "provider",
    "costMilliCents",
    "workflowName",
    "requestedFormat",
    "seedUrls",
    "ignored",
    "ignoredCount",
    "job_urls",
    "raw",
    "request"
  ],
  "normalized_count": 0
}
```
