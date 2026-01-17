# Extraction Steps: thetradedesk

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/thetradedesk/jobs/4828165007`
**Source URL:** `https://api.greenhouse.io/v1/boards/thetradedesk/jobs`
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
  "url": "https://boards-api.greenhouse.io/v1/boards/thetradedesk/jobs/4828165007",
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
      "url": "https://boards-api.greenhouse.io/v1/boards/thetradedesk/jobs/4828165007",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/thetradedesk/jobs",
      "provider": "spidercloud",
      "siteId": "thetradedesk",
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

Ran 8 extractors with all strategies

```json
{
  "title": {
    "winner": "raw_row_title",
    "value": "2026 China Data Science Internship - Deep Learning Focus (Based in Shanghai)"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "The Trade Desk"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Shanghai"
  },
  "remote": {
    "winner": "default_remote",
    "value": false
  },
  "level": {
    "winner": "explicit_level_field",
    "value": "mid"
  },
  "compensation": {
    "winner": "unknown_compensation",
    "value": 0
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2025-08-14T00:50:10"
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "&lt;p&gt;The Trade Desk is a global technology company with a mission to create a better, more open internet for everyone through principled, intelligent advertising. Handling over 1 trillion queries per day, our platform operates at an unprecedented scale. We have also built something even stronger and more valuable: an award-winning culture based on trust, ownership, empathy, and collaboration. We value the unique experiences and perspectives that each person brings to The Trade Desk, and we are committed to fostering inclusive spaces where everyone can bring their authentic selves to work every day.&lt;/p&gt; &lt;p&gt;Do you have a passion for solving hard..."
  }
}
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `2026 China Data Science Internship - Deep Learning Focus (Based in Shanghai)`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768590810468,
  "heuristicVersion": 5,
  "locations": [
    "Shanghai, China"
  ],
  "location": "Shanghai, China",
  "locationStates": [
    "China"
  ],
  "locationSearch": "China Shanghai",
  "countries": [
    "China"
  ],
  "country": "China",
  "remote": false
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
| title | `raw_row_title` | `2026 China Data Science Internship - Deep Learning` |
| company | `raw_row_company` | `The Trade Desk` |
| location | `raw_row_location` | `Shanghai` |
| remote | `default_remote` | `(none)` |
| level | `explicit_level_field` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| posted_at | `explicit_posted_at_field` | `2025-08-14 00:50:10` |
| description | `normalized_markdown_description` | `&lt;p&gt;The Trade Desk is a global technology com` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `2026 China Data Science Internship - Deep Learning Focus (Based in Shanghai)`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `2026 China Data Science Intern` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ❌ | `` | No title in hints |
| first_line_title | FALLBACK | ❌ | `` | No valid title found in first lines |

#### COMPANY

**Final Value:** `The Trade Desk`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `The Trade Desk` | Valid company name |
| url_company | URL_DERIVED | ✅ | `The Trade Desk` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ❌ | `` | No company pattern matched |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Shanghai`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Shanghai` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Shanghai` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ❌ | `` | No location in hints |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `default_remote`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_remote_flag | STRUCTURED_DATA | ❌ | `` | No remote field in raw row |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Shanghai' present but not inferring remo |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | No remote in hints |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'The Trade Desk' not in remote company lis |
| **default_remote** 🏆 | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `mid`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `mid` | Explicit level field: mid -> mid |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `junior` | Level from title: 'internship' -> junior |
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

#### POSTED_AT

**Final Value:** `2025-08-14 00:50:10`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-08-14 00:50:10` | Valid date: 2025-08-14T00:50:10 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 12:13:30.473892` | Using current time as fallback (date unknown) |

#### DESCRIPTION

**Final Value:** `&lt;p&gt;The Trade Desk is a global technology company with a mission to create a better, more open internet for everyone through principled, intelligent advertising. Handling over 1 trillion queries per day, our platform operates at an unprecedented scale. We have also built something even stronger and more valuable: an award-winning culture based on trust, ownership, empathy, and collaboration. We value the unique experiences and perspectives that each person brings to The Trade Desk, and we are committed to fostering inclusive spaces where everyone can bring their authentic selves to work every day.&lt;/p&gt; &lt;p&gt;Do you have a passion for solving hard...`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `&lt;p&gt;The Trade Desk is a g` | Valid description (670 chars, 100 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `&lt;p&gt;The Trade Desk is a g` | Valid description (670 chars, 100 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `&lt;p&gt;The Trade Desk is a g` | Valid description (670 chars, 100 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `2026 China Data Science Internship - Deep Learning Focus (Based in Shanghai)` |
| Company | `The Trade Desk` |
| Location | `Shanghai, China` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1755157810000` |
| Description Words | `100` |
| Cost (milli-cents) | `4` |
| URL | `https://job-boards.greenhouse.io/thetradedesk/jobs/4828165007` |

**Description Preview (first 200 words):**

```
&lt;p&gt;The Trade Desk is a global technology company with a mission to create a better, more open internet for everyone through principled, intelligent advertising. Handling over 1 trillion queries per day, our platform operates at an unprecedented scale. We have also built something even stronger and more valuable: an award-winning culture based on trust, ownership, empathy, and collaboration. We value the unique experiences and perspectives that each person brings to The Trade Desk, and we are committed to fostering inclusive spaces where everyone can bring their authentic selves to work every day.&lt;/p&gt; &lt;p&gt;Do you have a passion for solving hard...
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
  "title": "2026 China Data Science Internship - Deep Learning Focus (Based in Shanghai)",
  "company": "The Trade Desk",
  "location": "Shanghai",
  "description": "&lt;p&gt;The Trade Desk is a global technology company with a mission to create a better, more open internet for everyone through principled, intelligent advertising. Handling over 1 trillion queries per day, our platform operates at an unprecedented scale. We have also built something even stronger and more valuable: an award-winning culture based on trust, ownership, empathy, and collaboration. We value the unique experiences and perspectives that each person brings to The Trade Desk, and we a...",
  "url": "https://job-boards.greenhouse.io/thetradedesk/jobs/4828165007",
  "apply_url": "https://job-boards.greenhouse.io/thetradedesk/jobs/4828165007",
  "posted_at": 1755157810000,
  "level": "mid",
  "cost_milli_cents": 4
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 4,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
