# Extraction Steps: palo_alto_networks

**Detail URL:** `https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520`
**Source URL:** `https://jobs.paloaltonetworks.com/en/search-jobs?k=software%20engineer&l=United+States`
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
  "url": "https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520",
  "handler": "paloalto_networks"
}
```

### Raw Content Capture

Failed to capture raw content: unexpected end of data: line 1 column 32702 (char 32701)

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520",
      "sourceUrl": "https://jobs.paloaltonetworks.com/en/search-jobs?k=software%20engineer&l=United+States",
      "provider": "spidercloud",
      "siteId": "palo_alto_networks",
      "pattern": null,
      "urlType": "detail"
    }
  ]
}
```

### Workflow Complete

Workflow returned, captured 1 scrapes, 1 ingested jobs

```json
{
  "stored_scrapes": 1,
  "ingested_jobs": 1,
  "description_uploads": 0
}
```

### Extractor Debug Trace

Ran 10 extractors with all strategies

```json
{
  "title": {
    "winner": "raw_row_title",
    "value": "Software Engineering Manager (NetSec)"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Palo Alto Networks"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Santa Clara, CA, US"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": true
  },
  "level": {
    "winner": "explicit_level_field",
    "value": "senior"
  },
  "compensation": {
    "winner": "unknown_compensation",
    "value": 0
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 48
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T13:43:12.039000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Our Mission At Palo Alto Networks® everything starts and ends with our mission: Being the cybersecurity partner of choice, protecting our digital way of life. Our vision is a world where each day is safer and more secure than the one before. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are. Who We Are We believe collaboration thrives in person. That’s why most of our teams work from the office full time, with flexibility..."
  }
}
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Software Engineering Manager (NetSec)`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768596192138,
  "heuristicVersion": 5,
  "location": "Santa Clara, CA, US",
  "locationSearch": "Santa Clara, CA, US"
}
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `raw_row_title` | `Software Engineering Manager (NetSec)` |
| company | `raw_row_company` | `Palo Alto Networks` |
| location | `raw_row_location` | `Santa Clara, CA, US` |
| remote | `explicit_remote_flag` | `True` |
| level | `explicit_level_field` | `senior` |
| compensation | `unknown_compensation` | `(none)` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `48` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 13:43:12.039000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Our Mission At Palo Alto Networks® everything star` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Software Engineering Manager (NetSec)`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Software Engineering Manager (` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ❌ | `` | No title in hints |
| first_line_title | FALLBACK | ❌ | `` | No valid title found in first lines |

#### COMPANY

**Final Value:** `Palo Alto Networks`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Palo Alto Networks` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Palo Alto Networks` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ❌ | `` | No company pattern matched |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Santa Clara, CA, US`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' returned no location h |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Santa Clara, CA, US` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ✅ | `Santa Clara` | Extracted from URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Santa Clara, CA, US` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `Palo Alto, CA` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `True`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `True` | Explicit boolean remote=True |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Santa Clara, CA, US' present but not inf |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Palo Alto Networks' not in remote company |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `senior`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `senior` | Explicit level field: senior -> senior |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `senior` | Level from title: 'manager' -> senior |
| content_pattern_level | CUSTOM_550 | ❌ | `` | No level pattern in content |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `0`
**Winning Strategy:** `unknown_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| hinted_compensation | CUSTOM_450 | ❌ | `` | No compensation in hints |
| content_pattern_compensation | CONTENT_PATTERN | ❌ | `` | No compensation pattern in content |
| **unknown_compensation** 🏆 | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `48`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `48` | Valid cost: 48 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 13:43:12.039000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 13:43:12.039000` | Valid date: 2026-01-16T13:43:12.039000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 13:43:12.144660` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' returned no first_publ |

#### DESCRIPTION

**Final Value:** `Our Mission At Palo Alto Networks® everything starts and ends with our mission: Being the cybersecurity partner of choice, protecting our digital way of life. Our vision is a world where each day is safer and more secure than the one before. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are. Who We Are We believe collaboration thrives in person. That’s why most of our teams work from the office full time, with flexibility...`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Our Mission At Palo Alto Netwo` | Valid description (577 chars, 100 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Our Mission At Palo Alto Netwo` | Valid description (577 chars, 100 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Our Mission At Palo Alto Netwo` | Valid description (577 chars, 100 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Software Engineering Manager (NetSec)` |
| Company | `Palo Alto Networks` |
| Location | `Santa Clara, CA, US` |
| Is Remote | `True` |
| Level | `senior` |
| Posted At | `1768596192039` |
| Description Words | `100` |
| Cost (milli-cents) | `48` |
| URL | `https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520` |

**Description Preview (first 200 words):**

```
Our Mission At Palo Alto Networks® everything starts and ends with our mission: Being the cybersecurity partner of choice, protecting our digital way of life. Our vision is a world where each day is safer and more secure than the one before. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are. Who We Are We believe collaboration thrives in person. That’s why most of our teams work from the office full time, with flexibility...
```

---

## Step 6: Convex Mutation Payload

**Ingested Jobs Count:** 1
**Stored Scrapes Count:** 1
**Description Uploads Count:** 0

### Sample Ingested Job Payload

This is what gets sent to `router:ingestJobsFromScrape`:

```json
{
  "title": "Software Engineering Manager (NetSec)",
  "company": "Palo Alto Networks",
  "location": "Santa Clara, CA, US",
  "description": "Our Mission At Palo Alto Networks® everything starts and ends with our mission: Being the cybersecurity partner of choice, protecting our digital way of life. Our vision is a world where each day is safer and more secure than the one before. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are. Who We Are We believe collaboration thrives in person. Tha...",
  "url": "https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520",
  "posted_at": 1768596192039,
  "level": "senior",
  "remote": true,
  "cost_milli_cents": 48,
  "_full_description_word_count": 100
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 48,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
