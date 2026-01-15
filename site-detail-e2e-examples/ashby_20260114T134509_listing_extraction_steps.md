# Listing Extraction Steps: ashby_20260114T134509

**Listing URL:** `https://api.ashbyhq.com/posting-api/job-board/ramp`
**Source URL:** `https://api.ashbyhq.com/posting-api/job-board/ramp`
**Handler:** `AshbyHqHandler`
**Content Type:** `Unknown`

---

## Step 1: SpiderCloud Response

Raw content from SpiderCloud scrape:

```markdown
(No raw content captured)
```

---

## Step 2: Handler Detection

**Detected Handler:** `AshbyHqHandler`

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

Detected handler: AshbyHqHandler

```json
{
  "url": "https://api.ashbyhq.com/posting-api/job-board/ramp",
  "source_url": "https://api.ashbyhq.com/posting-api/job-board/ramp",
  "handler": "AshbyHqHandler"
}
```

### Workflow Setup

Set up WorkflowTestHelper with mocked dependencies

```json
{
  "sync_mode": true,
  "listing_url": "https://api.ashbyhq.com/posting-api/job-board/ramp"
}
```

### Workflow Execution

Calling process_spidercloud_listing_batch()

```json
{
  "urls": [
    {
      "url": "https://api.ashbyhq.com/posting-api/job-board/ramp",
      "sourceUrl": "https://api.ashbyhq.com/posting-api/job-board/ramp",
      "provider": "spidercloud",
      "siteId": "ashby_20260114T134509",
      "urlType": "listing"
    }
  ]
}
```

### Workflow Complete

Workflow returned, enqueued 126 URLs

```json
{
  "response": {
    "queued": 126,
    "listingCompleted": 1,
    "sourceUrl": "https://api.ashbyhq.com/posting-api/job-board/ramp"
  },
  "enqueued_count": 126,
  "completed_count": 1
}
```

### Extraction Complete (Production)

Extracted 126 URLs via production workflow

```json
{
  "extracted_count": 126,
  "sample_urls": [
    "https://jobs.ashbyhq.com/ramp/63df0ffc-bdc6-40ba-906f-fe03378536b0",
    "https://jobs.ashbyhq.com/ramp/caf900ec-0107-436b-88bf-2bc24174e6b9",
    "https://jobs.ashbyhq.com/ramp/f564dcf9-9390-4a3f-896f-8047a5086040",
    "https://jobs.ashbyhq.com/ramp/1e077eec-dcae-4be5-a446-b3dd089777c6",
    "https://jobs.ashbyhq.com/ramp/4e64ab86-4e30-403b-b1b9-41dc052570ce"
  ]
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 126
**URLs After Filtering:** 126
**URLs After Normalization:** 126
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://jobs.ashbyhq.com/ramp/63df0ffc-bdc6-40ba-906f-fe03378536b0`
2. `https://jobs.ashbyhq.com/ramp/caf900ec-0107-436b-88bf-2bc24174e6b9`
3. `https://jobs.ashbyhq.com/ramp/f564dcf9-9390-4a3f-896f-8047a5086040`
4. `https://jobs.ashbyhq.com/ramp/1e077eec-dcae-4be5-a446-b3dd089777c6`
5. `https://jobs.ashbyhq.com/ramp/4e64ab86-4e30-403b-b1b9-41dc052570ce`
6. `https://jobs.ashbyhq.com/ramp/d64609dd-c391-45a2-bfdb-c1bb34e8f93c`
7. `https://jobs.ashbyhq.com/ramp/4745807e-82f4-4b1a-857c-dc8dadc73076`
8. `https://jobs.ashbyhq.com/ramp/707d5f91-bcf7-42b2-a0e6-8130bf13e56b`
9. `https://jobs.ashbyhq.com/ramp/4859cd5e-f2a9-44d7-81f7-8bfc0e62369f`
10. `https://jobs.ashbyhq.com/ramp/6e7b0226-d806-4efb-972c-1e7d0e1690cf`
11. `https://jobs.ashbyhq.com/ramp/a7b5b128-c024-433a-b5d7-4e7b7ed7a49d`
12. `https://jobs.ashbyhq.com/ramp/b55447c0-4adc-42eb-9ca2-f88fd44e0e5b`
13. `https://jobs.ashbyhq.com/ramp/b9568fb8-a47e-4738-87fb-a6d88c3a505f`
14. `https://jobs.ashbyhq.com/ramp/eca54d0e-232a-4c3e-bfcc-d6c6add393f5`
15. `https://jobs.ashbyhq.com/ramp/20b3fd61-d517-4c05-bca6-6f6563737072`
16. `https://jobs.ashbyhq.com/ramp/a808d519-404a-4aee-9189-2b1ad62d0330`
17. `https://jobs.ashbyhq.com/ramp/84d4a0a3-f629-4fac-acaa-63437029043f`
18. `https://jobs.ashbyhq.com/ramp/fc971889-db1d-4a20-a25e-f282f9296936`
19. `https://jobs.ashbyhq.com/ramp/17ad9012-2545-4403-8e81-0775075a4fa3`
20. `https://jobs.ashbyhq.com/ramp/d84bbf19-572a-499c-9c87-0c154ce85caf`
... and 106 more

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 126

