# Listing Extraction Steps: greenhouse_20260114T134509

## URL Pipeline
- **Input URL:** `https://api.greenhouse.io/v1/boards/airbnb/jobs`
- **Scrape URL:** `https://api.greenhouse.io/v1/boards/airbnb/jobs`

**Listing URL:** `https://api.greenhouse.io/v1/boards/airbnb/jobs`
**Source URL:** `https://api.greenhouse.io/v1/boards/airbnb/jobs`
**Handler:** `GreenhouseHandler`
**Content Type:** `raw_html`

## Detail URL Pipeline Counts
- **Raw Extracted:** 143
- **Handler Filtered:** 143
- **API Transformed:** 143

---

## Step 1: SpiderCloud Response

Raw raw_html from SpiderCloud scrape:

```html
{"jobs":[{"absolute_url":"https://careers.airbnb.com/positions/7434393?gh_jid=7434393","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"education":"education_required","internal_job_id":3307403,"location":{"name":"Canada"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7434393,"updated_at":"2026-01-09T14:59:55-05:00","requisition_id":"ONE","title":"Account Executive, Airbnb for Business","company_name":"Airbnb","first_published":"2025-12-02T09:49:28-05:00","language":"en"},{"absolute_url":"https://careers.airbnb.com/positions/7467432?gh_jid=7467432","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":3316949,"location":{"name":"Mexico"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7467432,"updated_at":"2025-12-15T16:11:12-05:00","requisition_id":"ONE","title":"Acquisition Lead, Experiences, Mexico City (12 month contract)","company_name":"Airbnb","first_published":"2025-12-12T16:01:08-05:00","language":"en"},{"absolute_url":"https://careers.airbnb.com/positions/7467455?gh_jid=7467455","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":3316960,"location":{"name":"Mexico"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7467455,"updated_at":"2025-12-15T16:11:12-05:00","requisition_id":"MULTI","title":"Acquisition Manager, Experiences, Mexico City (12 month contract)","company_name":"Airbnb","first_published":"2025-12-12T16:00:49-05:00","language":"en"},{"absolute_url":"https://careers.airbnb.com/positions/7403612?gh_jid=7403612","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":3298810,"location":{"name":"Canada"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7403612,"updated_at":"2025-12-19T10:29:17-05:00","requisition_id":"ONE","title":"AirCover UX Enablement Manager","company_name":"Airbnb","first_published":"2025-11-19T11:55:23-05:00","language":"en"},{"absolute_url":"https://careers.airbnb.com/positions/7535699?gh_jid=7535699","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"education":"education_required","internal_job_id":3336769,"location":{"name":"Canada"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7535699,"updated_at":"2026-01-13T16:33:23-05:00","requisition_id":"ONE","title":"Analyste en intelligence de la fraude","company_name":"Airbnb","first_published":"2026-01-13T16:33:23-05:00","language":"fr"},{"absolute_url":"https://careers.airbnb.com/positions/7456094?gh_jid=7456094","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"education":"education_required","internal_job_id":3313803,"location":{"name":"United States "},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7456094,"updated_at":"2026-01-13T19:50:53-05:00","requisition_id":"ONE","title":"Analyst, Strategic Finance and Analytics, Technology","company_name":"Airbnb","first_published":"2025-12-12T09:49:19-05:00","language":"en"},{"absolute_url":"https://careers.airbnb.com/positions/7469518?gh_jid=7469518","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":3317245,"location":{"name":"São Paulo, Brazil"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Wor

... (truncated, 157699 total chars)
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

**Method Used:** `handler.get_links_from_json`

URL extraction methods (in priority order):
1. **JSON API**: Parse structured JSON response with job array
2. **HTML Links**: Extract href attributes from anchor tags
3. **Regex Fallback**: Search for URL patterns in raw text

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: GreenhouseHandler

```json
{
  "url": "https://api.greenhouse.io/v1/boards/airbnb/jobs",
  "source_url": "https://api.greenhouse.io/v1/boards/airbnb/jobs",
  "handler": "GreenhouseHandler"
}
```

### Raw Content Capture

Captured 157699 chars of raw_html content

```json
{
  "length": 157699,
  "content_type": "raw_html"
}
```

### Parsed raw JSON

Parsed raw JSON content, extracted 143 URLs

```json
{
  "url_count": 143
}
```

### API URL Transformation

Transformed 143 URLs to API format

```json
{
  "transformation_count": 143,
  "sample_transformations": [
    {
      "original": "https://careers.airbnb.com/positions/7467432?gh_jid=7467432",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7467432"
    },
    {
      "original": "https://careers.airbnb.com/positions/7467455?gh_jid=7467455",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7467455"
    },
    {
      "original": "https://careers.airbnb.com/positions/7403612?gh_jid=7403612",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7403612"
    },
    {
      "original": "https://careers.airbnb.com/positions/7469518?gh_jid=7469518",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7469518"
    },
    {
      "original": "https://careers.airbnb.com/positions/7455067?gh_jid=7455067",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7455067"
    },
    {
      "original": "https://careers.airbnb.com/positions/7288960?gh_jid=7288960",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7288960"
    },
    {
      "original": "https://careers.airbnb.com/positions/7439746?gh_jid=7439746",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7439746"
    },
    {
      "original": "https://careers.airbnb.com/positions/7256581?gh_jid=7256581",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7256581"
    },
    {
      "original": "https://careers.airbnb.com/positions/7522011?gh_jid=7522011",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7522011"
    },
    {
      "original": "https://careers.airbnb.com/positions/7525334?gh_jid=7525334",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7525334"
    }
  ]
}
```

### Extraction Complete

Extracted 143 URLs, filtered to 143 detail + 0 pagination, normalized to 143 final

```json
{
  "extracted_count": 143,
  "detail_count": 143,
  "normalized_count": 143,
  "pagination_count": 0,
  "sample_normalized_urls": [
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7467432",
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7467455",
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7403612",
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7469518",
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7455067"
  ],
  "sample_pagination_urls": []
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 143
**URLs After Filtering:** 143
**URLs After Normalization:** 143
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7467432`
2. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7467455`
3. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7403612`
4. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7469518`
5. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7455067`
6. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7288960`
7. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7439746`
8. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7256581`
9. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7522011`
10. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7525334`
11. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7402675`
12. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7521998`
13. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7277976`
14. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7434498`
15. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7481629`
16. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7535582`
17. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7336167`
18. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7438414`
19. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7443212`
20. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7256544`
... and 123 more

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 143

