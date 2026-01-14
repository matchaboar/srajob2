# Extraction Steps: paloaltonetworks

**Detail URL:** `https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944`
**Source URL:** `https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944`
**Handler:** `paloalto_networks`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
(No raw markdown captured)
```

---

## Step 2: Handler Detection

**Detected Handler:** `paloalto_networks`

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

Detected handler: paloalto_networks

```json
{
  "url": "https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944",
  "handler": "paloalto_networks"
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
      "url": "https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944",
      "sourceUrl": "https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944",
      "provider": "spidercloud",
      "siteId": "paloaltonetworks",
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

**Title unchanged:** `Named Account Manager - SLED`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768377808115,
  "heuristicVersion": 4,
  "locations": [
    "Los Angeles"
  ],
  "location": "Los Angeles",
  "locationStates": [],
  "locationSearch": "Angeles Los",
  "countries": [
    "Los Angeles"
  ],
  "country": "Los Angeles",
  "remote": false,
  "description": "Our Mission\n\nAt Palo Alto Networks\u00ae everything starts and ends with our mission:\n\nBeing the cybersecurity partner of choice, protecting our digital way of life.\n\nWe have the vision of a world where each day is safer and more secure than the one before. These aren\u2019t easy goals to accomplish \u2013 but we\u2019re not here for easy. We\u2019re here for better. We are a company built on the foundation of challenging and disrupting the way things are done, and we\u2019re looking for innovators who are as committed to shaping the future of cybersecurity as we are.\n\nWe\u2019re changing the nature of work. Palo Alto Networks is evolving to meet the needs of
```

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Named Account Manager - SLED` |
| Company | `Palo Alto Networks` |
| Location | `Los Angeles` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1767744000000` |
| Description Words | `1041` |
| Cost (milli-cents) | `74` |
| URL | `https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944` |

**Description Preview (first 200 words):**

```
Our Mission At Palo Alto Networks® everything starts and ends with our mission: Being the cybersecurity partner of choice, protecting our digital way of life. We have the vision of a world where each day is safer and more secure than the one before. These aren’t easy goals to accomplish – but we’re not here for easy. We’re here for better. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are. We’re changing the nature of work. Palo Alto Networks is evolving to meet the needs of our employees now and in the future through FLEXWORK, our approach to how we work. From benefits to learning, location to leadership, we’ve rethought and recreated every aspect of the employee experience at Palo Alto Networks. And because it FLEXes around each individual employee based on their individual choices, employees are empowered to push boundaries and help us all evolve, together. Your Career The Named Account Manager - SLED is a significant driver of company revenue and growth. As an experienced and dynamic sales professional, you’re responsible for...
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
  "sourceUrl": "https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944",
  "provider": "spidercloud",
  "costMilliCents": 74,
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
