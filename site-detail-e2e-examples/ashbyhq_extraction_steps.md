# Extraction Steps: ashbyhq

**Detail URL:** `https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd`
**Source URL:** `https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd`
**Handler:** `ashby`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
<html lang="en">
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <meta content="#483fad" name="theme-color">
    <meta content="Oo0LI3Pb89o-WiGK8d9clw6687Hq0BVwCHCC-g0tt78" id="csp-nonce" name="csp-nonce">
    <link href="https://cdn.ashbyprd.com/cdn_assets/f60311c15b1c9db3843dfe8f1790884afa6cf89f/favicon.png" rel="icon" sizes="192x192" type="image/png">
    <link href="https://cdn.ashbyprd.com/cdn_assets/f60311c15b1c9db38...
```

---

## Step 2: Handler Detection

**Detected Handler:** `ashby`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
<html lang="en">
    
    
    
    
    
    <link href="https://cdn.ashbyprd.com/cdn_assets/f60311c15b1c9db38...
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: ashby

```json
{
  "url": "https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd",
  "handler": "ashby"
}
```

### Raw Content Capture

Captured 503 chars of raw_html content

```json
{
  "length": 503,
  "content_type": "raw_html"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 114 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 114
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd",
      "sourceUrl": "https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd",
      "provider": "spidercloud",
      "siteId": "ashbyhq",
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
  "sourceUrl": "https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd",
  "provider": "spidercloud",
  "costMilliCents": null,
  "items_keys": [
    "normalized",
    "page_links",
    "provider",
    "workflowName",
    "requestedFormat",
    "seedUrls",
    "failed",
    "failedCount",
    "job_urls",
    "raw",
    "request"
  ],
  "normalized_count": 0
}
```
