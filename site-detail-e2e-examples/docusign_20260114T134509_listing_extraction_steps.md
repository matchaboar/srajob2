# Listing Extraction Steps: docusign_20260114T134509

**Listing URL:** `https://careers.docusign.com/api/jobs?categories=Engineering%7CIT%20Infrastructure%20%26%20Operations&page=1&limit=100&locations=San%20Francisco,California,United%20States%7CSeattle,Washington,United%20States&sortBy=relevance&descending=false&internal=false`
**Source URL:** `https://careers.docusign.com/api/jobs?categories=Engineering%7CIT%20Infrastructure%20%26%20Operations&page=1&limit=100&locations=San%20Francisco,California,United%20States%7CSeattle,Washington,United%20States&sortBy=relevance&descending=false&internal=false`
**Handler:** `DocusignHandler`
**Content Type:** `Unknown`

---

## Step 1: SpiderCloud Response

Raw content from SpiderCloud scrape:

```markdown
(No raw content captured)
```

---

## Step 2: Handler Detection

**Detected Handler:** `DocusignHandler`

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

Detected handler: DocusignHandler

```json
{
  "url": "https://careers.docusign.com/api/jobs?categories=Engineering%7CIT%20Infrastructure%20%26%20Operations&page=1&limit=100&locations=San%20Francisco,California,United%20States%7CSeattle,Washington,United%20States&sortBy=relevance&descending=false&internal=false",
  "source_url": "https://careers.docusign.com/api/jobs?categories=Engineering%7CIT%20Infrastructure%20%26%20Operations&page=1&limit=100&locations=San%20Francisco,California,United%20States%7CSeattle,Washington,United%20States&sortBy=relevance&descending=false&internal=false",
  "handler": "DocusignHandler"
}
```

### Workflow Setup

Set up WorkflowTestHelper with mocked dependencies

```json
{
  "sync_mode": true,
  "listing_url": "https://careers.docusign.com/api/jobs?categories=Engineering%7CIT%20Infrastructure%20%26%20Operations&page=1&limit=100&locations=San%20Francisco,California,United%20States%7CSeattle,Washington,United%20States&sortBy=relevance&descending=false&internal=false"
}
```

### Workflow Execution

Calling process_spidercloud_listing_batch()

```json
{
  "urls": [
    {
      "url": "https://careers.docusign.com/api/jobs?categories=Engineering%7CIT%20Infrastructure%20%26%20Operations&page=1&limit=100&locations=San%20Francisco,California,United%20States%7CSeattle,Washington,United%20States&sortBy=relevance&descending=false&internal=false",
      "sourceUrl": "https://careers.docusign.com/api/jobs?categories=Engineering%7CIT%20Infrastructure%20%26%20Operations&page=1&limit=100&locations=San%20Francisco,California,United%20States%7CSeattle,Washington,United%20States&sortBy=relevance&descending=false&internal=false",
      "provider": "spidercloud",
      "siteId": "docusign_20260114T134509",
      "urlType": "listing"
    }
  ]
}
```

### Workflow Complete

Workflow returned, enqueued 10 URLs

```json
{
  "response": {
    "queued": 10,
    "listingCompleted": 1,
    "sourceUrl": "https://careers.docusign.com/api/jobs?categories=Engineering%7CIT%20Infrastructure%20%26%20Operations&page=1&limit=100&locations=San%20Francisco,California,United%20States%7CSeattle,Washington,United%20States&sortBy=relevance&descending=false&internal=false"
  },
  "enqueued_count": 10,
  "completed_count": 1
}
```

### Extraction Complete (Production)

Extracted 10 URLs via production workflow

```json
{
  "extracted_count": 10,
  "sample_urls": [
    "https://careers.docusign.com/jobs/27215?lang=en-us",
    "https://careers.docusign.com/jobs/27441?lang=en-us",
    "https://careers.docusign.com/jobs/28171?lang=en-us",
    "https://careers.docusign.com/jobs/28259?lang=en-us",
    "https://careers.docusign.com/jobs/28395?lang=en-us"
  ]
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 10
**URLs After Filtering:** 10
**URLs After Normalization:** 10
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://careers.docusign.com/jobs/27215?lang=en-us`
2. `https://careers.docusign.com/jobs/27441?lang=en-us`
3. `https://careers.docusign.com/jobs/28171?lang=en-us`
4. `https://careers.docusign.com/jobs/28259?lang=en-us`
5. `https://careers.docusign.com/jobs/28395?lang=en-us`
6. `https://careers.docusign.com/jobs/28380?lang=en-us`
7. `https://careers.docusign.com/jobs/28412?lang=en-us`
8. `https://careers.docusign.com/jobs/28351?lang=en-us`
9. `https://careers.docusign.com/jobs/28249?lang=en-us`
10. `https://careers.docusign.com/jobs/28248?lang=en-us`

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 10

