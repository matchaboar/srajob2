# Extraction Steps: serval

**Detail URL:** `https://jobs.ashbyhq.com/Serval/77514f30-6a54-4bb3-b9aa-28b86f6ce7c6`
**Source URL:** `https://jobs.ashbyhq.com/Serval`
**Handler:** `ashby`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
(No raw markdown captured)
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
(No normalized markdown captured - handler may not implement normalize_markdown)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: ashby

```json
{
  "url": "https://jobs.ashbyhq.com/Serval/77514f30-6a54-4bb3-b9aa-28b86f6ce7c6",
  "handler": "ashby"
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
      "url": "https://jobs.ashbyhq.com/Serval/77514f30-6a54-4bb3-b9aa-28b86f6ce7c6",
      "sourceUrl": "https://jobs.ashbyhq.com/Serval",
      "provider": "spidercloud",
      "siteId": "serval",
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

### Heuristic Title Override

Title changed from 'Mid-Market Account Executive' to 'Role Overview'

```json
{
  "original_title": "Mid-Market Account Executive",
  "patched_title": "Role Overview",
  "patch": {
    "heuristicAttempts": 1,
    "heuristicLastTried": 1768379601486,
    "heuristicVersion": 4,
    "title": "Role Overview",
    "jobTitle": "Role Overview",
    "locations": [
      "San Francisco, CA"
    ],
    "location": "San Francisco, CA",
    "locationStates": [
      "CA"
    ],
    "locationSearch": "San Francisco CA",
    "countries": [
      "United States"
    ],
    "country": "United States",
    "totalCompensation": 250000,
    "compensationReason": "parsed from description",
    "compensationUnknown": false,
    "description": "Who We Are\n\nAt Serval, we're building the AI platform for IT teams. Our goal is to take on legacy players like ServiceNow, a $230+ bn company, by deploying AI agents to resolve IT issues instead of humans.\n\nServal \u201cautomates the automation,\u201d using a natural language-to-code workflow builder and AI agents that discover and deliver automations for tedious IT workflows.\n\nOur mission is to free IT departments from the #helpdesk channel by creating the simplest way to automate employee onboarding/offboarding, software access management, and the long tail of employee requests. Long term, our vision extends to developing a universal workflow automation platform for all business functions.\n\nServal was founded by product and engineering leaders from Verkada and is backed by industry-leading investors like First Round, Genera
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**⚠️ TITLE CHANGED:**
- Original: `Mid-Market Account Executive`
- After Heuristics: `Role Overview`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768379601486,
  "heuristicVersion": 4,
  "title": "Role Overview",
  "jobTitle": "Role Overview",
  "locations": [
    "San Francisco, CA"
  ],
  "location": "San Francisco, CA",
  "locationStates": [
    "CA"
  ],
  "locationSearch": "San Francisco CA",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "totalCompensation": 250000,
  "compensationReason": "parsed from description",
  "compensationUnknown": false,
  "description": "Who We Are\n\nAt Serval, we're building the AI platform for IT teams. Our goal is to take on legacy players like ServiceNow, a $230+ bn company, by deploying AI agents to resolve IT issues instead of humans.\n\nServal \u201cautomates the automation,\u201d using a natural language-to-code workflow builder and AI agents that discover and deliver automations for tedious IT workflows.\n\nOur mission is to free IT departments from the #helpdesk channel by creating the simplest way to auto
```

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Role Overview` |
| Company | `Serval` |
| Location | `San Francisco, CA` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1761058800000` |
| Description Words | `552` |
| Cost (milli-cents) | `9` |
| URL | `https://jobs.ashbyhq.com/Serval/77514f30-6a54-4bb3-b9aa-28b86f6ce7c6` |

**Description Preview (first 200 words):**

```
Who We Are At Serval, we're building the AI platform for IT teams. Our goal is to take on legacy players like ServiceNow, a $230+ bn company, by deploying AI agents to resolve IT issues instead of humans. Serval “automates the automation,” using a natural language-to-code workflow builder and AI agents that discover and deliver automations for tedious IT workflows. Our mission is to free IT departments from the #helpdesk channel by creating the simplest way to automate employee onboarding/offboarding, software access management, and the long tail of employee requests. Long term, our vision extends to developing a universal workflow automation platform for all business functions. Serval was founded by product and engineering leaders from Verkada and is backed by industry-leading investors like First Round, General Catalyst, Alt Capital, and Box Group. Role Overview As a Mid-Market Account Executive, you’ll join our founding go-to-market team and own the full sales cycle — from outbound prospecting through close — for multi-stakeholder IT and Security deals. You’ll partner closely with our founders, product, and engineering teams to shape Serval’s commercial motion and define how AI transforms IT operations. This role is ideal for a high-performing seller who’s thrived in fast-growing SaaS environments,...
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
  "sourceUrl": "https://jobs.ashbyhq.com/Serval",
  "provider": "spidercloud",
  "costMilliCents": 9,
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
