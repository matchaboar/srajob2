# Extraction Steps: axon

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/axon/jobs/4322399003`
**Source URL:** `https://api.greenhouse.io/v1/boards/axon/jobs`
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
  "url": "https://boards-api.greenhouse.io/v1/boards/axon/jobs/4322399003",
  "handler": "greenhouse"
}
```

### Raw Content Capture

Captured 0 chars of unknown content

```json
{
  "length": 0,
  "content_type": "unknown"
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://boards-api.greenhouse.io/v1/boards/axon/jobs/4322399003",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/axon/jobs",
      "provider": "spidercloud",
      "siteId": "axon",
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

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Backend Software Engineer I/ II`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768379588216,
  "heuristicVersion": 4,
  "locations": [
    "re explorers, pursuing society"
  ],
  "location": "re explorers, pursuing society",
  "locationStates": [
    "pursuing society"
  ],
  "locationSearch": "pursuing explorers society re",
  "countries": [
    "pursuing society"
  ],
  "country": "pursuing society",
  "description": "Backend Software Engineer I/ II\n\nJoin Axon and be a Force for Good.\nAt Axon, we\u2019re on a mission to Protect Life. We\u2019re explorers, pursuing society\u2019s most critical safety and justice issues with our ecosystem of devices and cloud software. Like our products, we work better together. We connect with candor and care, seeking out diverse perspectives from our customers, communities and each other.\nLife at Axon is fast-paced, challenging and meaningful. Here, you\u2019ll take ownership and drive real change. Constantly grow as you work hard for a mission that matters at a company w
```

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Backend Software Engineer I/ II` |
| Company | `Axon` |
| Location | `re explorers, pursuing society` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1767909861000` |
| Description Words | `981` |
| Cost (milli-cents) | `2` |
| URL | `https://boards-api.greenhouse.io/v1/boards/axon/jobs/4322399003` |

**Description Preview (first 200 words):**

```
Backend Software Engineer I/ II Join Axon and be a Force for Good. At Axon, we’re on a mission to Protect Life. We’re explorers, pursuing society’s most critical safety and justice issues with our ecosystem of devices and cloud software. Like our products, we work better together. We connect with candor and care, seeking out diverse perspectives from our customers, communities and each other. Life at Axon is fast-paced, challenging and meaningful. Here, you’ll take ownership and drive real change. Constantly grow as you work hard for a mission that matters at a company where you matter. Your Impact As one of the software engineers on the team, you will make key design decisions that will shape this product. You’ll create and maintain the data integration, management, and analytics platform alongside Axon’s newest public safety technology products. This platform will offer law enforcement administrators and crime analysts flexible access to key crime data, decision support, state and federal crime reports, and criminal investigation insights. Axon has led the global effort to protect life and through electric weapons, body cameras, a number of real-time sensors, public safety software products and AI tools. Axon is uniquely positioned to tie together every aspect...
```

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
  "sourceUrl": "https://api.greenhouse.io/v1/boards/axon/jobs",
  "provider": "spidercloud",
  "costMilliCents": 2,
  "items_keys": [
    "normalized",
    "normalizedCount",
    "normalizedSample",
    "page_links",
    "provider",
    "costMilliCents",
    "workflowName",
    "job_urls",
    "raw",
    "request"
  ],
  "normalized_count": 1
}
```
