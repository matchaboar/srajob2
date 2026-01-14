# Extraction Steps: stripe

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/stripe/jobs/7374078`
**Source URL:** `https://api.greenhouse.io/v1/boards/stripe/jobs`
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
  "url": "https://boards-api.greenhouse.io/v1/boards/stripe/jobs/7374078",
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
      "url": "https://boards-api.greenhouse.io/v1/boards/stripe/jobs/7374078",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/stripe/jobs",
      "provider": "spidercloud",
      "siteId": "stripe",
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

**Title unchanged:** `Account Executive, AI Sales`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768379591007,
  "heuristicVersion": 4,
  "locations": [
    "Account Executive, AI Sales"
  ],
  "location": "Account Executive, AI Sales",
  "locationStates": [
    "AI Sales"
  ],
  "locationSearch": "AI Account Sales Executive",
  "countries": [
    "AI Sales"
  ],
  "country": "AI Sales",
  "description": "Account Executive, AI Sales\n\nWho we are\nAbout Stripe\nStripe is a financial infrastructure platform for businesses. Millions of companies - from the world\u2019s largest enterprises to the most ambitious startups - use Stripe to accept payments, grow their revenue, and accelerate new business opportunities. Our mission is to increase the GDP of the internet, and we have a staggering amount of work ahead. That means you have an unprecedented opportunity to put the global economy within everyone's reach while doing the most important work of your career.\nAbout the team\nThe AI team is focused on one of Stripe\u2019s most stra
```

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Account Executive, AI Sales` |
| Company | `Stripe` |
| Location | `Account Executive, AI Sales` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1768255104000` |
| Description Words | `473` |
| Cost (milli-cents) | `2` |
| URL | `https://boards-api.greenhouse.io/v1/boards/stripe/jobs/7374078` |

**Description Preview (first 200 words):**

```
Account Executive, AI Sales Who we are About Stripe Stripe is a financial infrastructure platform for businesses. Millions of companies - from the world’s largest enterprises to the most ambitious startups - use Stripe to accept payments, grow their revenue, and accelerate new business opportunities. Our mission is to increase the GDP of the internet, and we have a staggering amount of work ahead. That means you have an unprecedented opportunity to put the global economy within everyone's reach while doing the most important work of your career. About the team The AI team is focused on one of Stripe’s most strategic growth areas: enabling the monetization and scaling of AI-native and AI-enabled businesses. We’re in a unique position – partnering with the world’s most ambitious AI companies (the likes of OpenAI, Anthropic, NVIDIA, etc) building on the frontier of artificial intelligence -- across infrastructure, foundation models, agents, and applications -- to help them grow and commercialize globally using Stripe’s full financial stack. As part of Stripe’s GTM / Sales organization, this team works closely with Product, Engineering, and Marketing to shape Stripe’s AI GTM strategy and ensure that the world’s leading AI companies -- from early-stage innovators to the...
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
  "sourceUrl": "https://api.greenhouse.io/v1/boards/stripe/jobs",
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
