# Listing Extraction Steps: greenhouse_20260117T014458

## URL Pipeline
- **Input URL:** `https://api.greenhouse.io/v1/boards/airbnb/jobs`
- **Scrape URL:** `https://api.greenhouse.io/v1/boards/airbnb/jobs`

**Listing URL:** `https://api.greenhouse.io/v1/boards/airbnb/jobs`
**Source URL:** `https://api.greenhouse.io/v1/boards/airbnb/jobs`
**Handler:** `GreenhouseHandler`
**Content Type:** `raw_html`

## Detail URL Pipeline Counts
- **Raw Extracted:** 146
- **Handler Filtered:** 146
- **API Transformed:** 146

---

## Step 1: SpiderCloud Response

Raw raw_html from SpiderCloud scrape:

```html
{"jobs":[{"absolute_url":"https://careers.airbnb.com/positions/7434393?gh_jid=7434393","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"education":"education_required","internal_job_id":3307403,"location":{"name":"Canada"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7434393,"updated_at":"2026-01-09T14:59:55-05:00","requisition_id":"ONE","title":"Account Executive, Airbnb for Business","company_name":"Airbnb","first_published":"2025-12-02T09:49:28-05:00","language":"en"},{"absolute_url":"https://careers.airbnb.com/positions/7467432?gh_jid=7467432","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":3316949,"location":{"name":"Mexico"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7467432,"updated_at":"2025-12-15T16:11:12-05:00","requisition_id":"ONE","title":"Acquisition Lead, Experiences, Mexico City (12 month contract)","company_name":"Airbnb","first_published":"2025-12-12T16:01:08-05:00","language":"en"},{"absolute_url":"https://careers.airbnb.com/positions/7467455?gh_jid=7467455","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":3316960,"location":{"name":"Mexico"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7467455,"updated_at":"2025-12-15T16:11:12-05:00","requisition_id":"MULTI","title":"Acquisition Manager, Experiences, Mexico City (12 month contract)","company_name":"Airbnb","first_published":"2025-12-12T16:00:49-05:00","language":"en"},{"absolute_url":"https://careers.airbnb.com/positions/7403612?gh_jid=7403612","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":3298810,"location":{"name":"Canada"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7403612,"updated_at":"2025-12-19T10:29:17-05:00","requisition_id":"ONE","title":"AirCover UX Enablement Manager","company_name":"Airbnb","first_published":"2025-11-19T11:55:23-05:00","language":"en"},{"absolute_url":"https://careers.airbnb.com/positions/7535699?gh_jid=7535699","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"education":"education_required","internal_job_id":3336769,"location":{"name":"Canada"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7535699,"updated_at":"2026-01-13T16:33:23-05:00","requisition_id":"ONE","title":"Analyste en intelligence de la fraude","company_name":"Airbnb","first_published":"2026-01-13T16:33:23-05:00","language":"fr"},{"absolute_url":"https://careers.airbnb.com/positions/7540205?gh_jid=7540205","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":3338221,"location":{"name":"United States"},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote","value_type":"single_select"}],"id":7540205,"updated_at":"2026-01-14T22:55:26-05:00","requisition_id":"ONE","title":"Analyst, Revenue Forecasting","company_name":"Airbnb","first_published":"2026-01-14T22:55:26-05:00","language":"en"},{"absolute_url":"https://careers.airbnb.com/positions/7456094?gh_jid=7456094","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"education":"education_required","internal_job_id":3313803,"location":{"name":"United States "},"metadata":[{"id":9245691,"name":"Is this job part of ACC?","value":false,"value_type":"yes_no"},{"id":10216612,"name":"Workplace Type","value":"Remote

... (truncated, 165778 total chars)
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

Captured 165778 chars of raw_html content

```json
{
  "length": 165778,
  "content_type": "raw_html"
}
```

### Parsed raw JSON

Parsed raw JSON content, extracted 146 URLs

```json
{
  "url_count": 146
}
```

### API URL Transformation

Transformed 146 URLs to API format

```json
{
  "transformation_count": 146,
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
    },
    {
      "original": "https://careers.airbnb.com/positions/7402675?gh_jid=7402675",
      "api_url": "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7402675"
    }
  ]
}
```

### Extraction Complete

Extracted 146 URLs, filtered to 146 detail + 0 pagination, normalized to 146 final

```json
{
  "extracted_count": 146,
  "detail_count": 146,
  "normalized_count": 146,
  "pagination_count": 0,
  "sample_normalized_urls": [
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7467432",
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7467455",
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7403612",
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7455067",
    "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7288960"
  ],
  "sample_pagination_urls": []
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 146
**URLs After Filtering:** 146
**URLs After Normalization:** 146
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7467432`
2. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7467455`
3. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7403612`
4. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7455067`
5. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7288960`
6. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7439746`
7. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7256581`
8. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7522011`
9. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7525334`
10. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7402675`
11. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7521998`
12. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7277976`
13. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7434498`
14. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7481629`
15. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7535582`
16. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7336167`
17. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7438414`
18. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7443212`
19. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7256544`
20. `https://boards-api.greenhouse.io/v1/boards/airbnb/jobs/7481945`
... and 126 more

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 146

