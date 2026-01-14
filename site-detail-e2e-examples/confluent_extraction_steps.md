# Extraction Steps: confluent

**Detail URL:** `https://careers.confluent.io/jobs/job/9f38c542-fe09-4fb1-bae2-09e3b789119b`
**Source URL:** `https://careers.confluent.io/jobs/united_states-engineering?engineering=engineering`
**Handler:** `confluent`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
(No raw markdown captured)
```

---

## Step 2: Handler Detection

**Detected Handler:** `confluent`

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

Detected handler: confluent

```json
{
  "url": "https://careers.confluent.io/jobs/job/9f38c542-fe09-4fb1-bae2-09e3b789119b",
  "handler": "confluent"
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
      "url": "https://careers.confluent.io/jobs/job/9f38c542-fe09-4fb1-bae2-09e3b789119b",
      "sourceUrl": "https://careers.confluent.io/jobs/united_states-engineering?engineering=engineering",
      "provider": "spidercloud",
      "siteId": "confluent",
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

**Title unchanged:** `Staff Software Engineer II - Kora Storage`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768379596797,
  "heuristicVersion": 4,
  "locations": [
    "Job"
  ],
  "location": "Job",
  "locationStates": [],
  "locationSearch": "Job",
  "countries": [
    "Job"
  ],
  "country": "Job",
  "totalCompensation": 277000,
  "compensationReason": "parsed from description",
  "compensationUnknown": false,
  "metadata": "Confluent Careers\n[](https://careers.confluent.io/)\n[Life at confluent](https://careers.confluent.io/life-at-confluent)[Belonging](https://careers.confluent.io/belonging)\nOur Teams\n[early talent](https://careers.confluent.io/early-talent)\n[Open Positions](https://careers.confluent.io/jobs)\n[Life at confluent](https://careers.confluent.io/life-at-confluent)[Belonging](https://careers.confluent.io/belonging)\nOur Teams\n[early talent](https://careers.confluent.io/early-talent)\n[Open Positions](https://careers.confluent.io/jobs)[&lt; Back to Confluent.io](https://www.confluent.io/)\n# Staff Software Engineer II 
```

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Staff Software Engineer II - Kora Storage` |
| Company | `Confluent` |
| Location | `Job` |
| Is Remote | `False` |
| Level | `staff` |
| Posted At | `1768379593032` |
| Description Words | `955` |
| Cost (milli-cents) | `32` |
| URL | `https://careers.confluent.io/jobs/job/9f38c542-fe09-4fb1-bae2-09e3b789119b` |

**Description Preview (first 200 words):**

```
We’re not just building better tech. We’re rewriting how data moves and what the world can do with it. With Confluent, data doesn’t sit still. Our platform puts information in motion, streaming in near real-time so companies can react faster, build smarter, and deliver experiences as dynamic as the world around them. It takes a certain kind of person to join this team. Those who ask hard questions, give honest feedback, and show up for each other. No egos, no solo acts. Just smart, curious humans pushing toward something bigger, together. One Confluent. One Team. One Data Streaming Platform. ## **About the Role:** We are a team of passionate engineers who love solving complex distributed systems and infrastructure problems. We are building Kora from the ground up to be a true managed service for clients who demand high levels of availability and performance at the lowest cost of ownership. ## **What You Will Do:** * **Storage Engine Development:** Design and develop a highly available, performant, reliable, durable, scalable, and multi-tenant storage engine for Kora. * **Engineering Leadership: **Set the standard for engineering excellence. Be instrumental in driving the technical solutions end-to-end, working closely with the team and key stakeholders to...
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
  "sourceUrl": "https://careers.confluent.io/jobs/united_states-engineering?engineering=engineering",
  "provider": "spidercloud",
  "costMilliCents": 32,
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
