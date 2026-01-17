# Extraction Steps: stubhub

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/stubhubinc/jobs/4704159101`
**Source URL:** `https://api.greenhouse.io/v1/boards/stubhubinc/jobs`
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
  "url": "https://boards-api.greenhouse.io/v1/boards/stubhubinc/jobs/4704159101",
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
      "url": "https://boards-api.greenhouse.io/v1/boards/stubhubinc/jobs/4704159101",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/stubhubinc/jobs",
      "provider": "spidercloud",
      "siteId": "stubhub",
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
    "value": "Analytics Engineer II"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "StubHub"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Los Angeles, California, United States"
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
    "value": "2025-11-02T16:27:31"
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "&lt;div class=&quot;content-intro&quot;&gt;&lt;p&gt;StubHub is on a mission to redefine the live event experience on a global scale. Whether someone is looking to attend their first event or their hundredth, we’re here to delight them all the way from the moment they start looking for a ticket until they step through the gate. The same goes for our sellers. From fans selling a single ticket to the promoters of a worldwide stadium tour, we want StubHub to be the safest, most convenient way to offer a ticket to the millions of fans who browse our platform around the world.&lt;/p&gt;&lt;/div&gt;&lt;p&gt;We are seeking talented..."
  }
}
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Analytics Engineer II `

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768590796427,
  "heuristicVersion": 5,
  "locations": [
    "Los Angeles, CA"
  ],
  "location": "Los Angeles, CA",
  "locationStates": [
    "CA"
  ],
  "locationSearch": "Los Angeles CA",
  "countries": [
    "United States"
  ],
  "country": "United States",
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
| title | `raw_row_title` | `Analytics Engineer II` |
| company | `raw_row_company` | `StubHub` |
| location | `raw_row_location` | `Los Angeles, California, United States` |
| remote | `default_remote` | `(none)` |
| level | `explicit_level_field` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| posted_at | `explicit_posted_at_field` | `2025-11-02 16:27:31` |
| description | `normalized_markdown_description` | `&lt;div class=&quot;content-intro&quot;&gt;&lt;p&g` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Analytics Engineer II`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Analytics Engineer II` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ❌ | `` | No title in hints |
| first_line_title | FALLBACK | ❌ | `` | No valid title found in first lines |

#### COMPANY

**Final Value:** `StubHub`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `StubHub` | Valid company name |
| url_company | URL_DERIVED | ✅ | `StubHub` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ❌ | `` | No company pattern matched |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Los Angeles, California, United States`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Los Angeles, California, Unite` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Los Angeles, California, Unite` | Country-only fallback: Valid location |
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
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Los Angeles, California, United States'  |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | No remote in hints |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'StubHub' not in remote company list |
| **default_remote** 🏆 | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `mid`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `mid` | Explicit level field: mid -> mid |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ❌ | `` | No level indicator in title |
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

**Final Value:** `2025-11-02 16:27:31`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-11-02 16:27:31` | Valid date: 2025-11-02T16:27:31 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 12:13:16.432851` | Using current time as fallback (date unknown) |

#### DESCRIPTION

**Final Value:** `&lt;div class=&quot;content-intro&quot;&gt;&lt;p&gt;StubHub is on a mission to redefine the live event experience on a global scale. Whether someone is looking to attend their first event or their hundredth, we’re here to delight them all the way from the moment they start looking for a ticket until they step through the gate. The same goes for our sellers. From fans selling a single ticket to the promoters of a worldwide stadium tour, we want StubHub to be the safest, most convenient way to offer a ticket to the millions of fans who browse our platform around the world.&lt;/p&gt;&lt;/div&gt;&lt;p&gt;We are seeking talented...`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `&lt;div class=&quot;content-in` | Valid description (634 chars, 100 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `&lt;div class=&quot;content-in` | Valid description (634 chars, 100 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `&lt;div class=&quot;content-in` | Valid description (634 chars, 100 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Analytics Engineer II ` |
| Company | `StubHub` |
| Location | `Los Angeles, CA` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1762126051000` |
| Description Words | `100` |
| Cost (milli-cents) | `2` |
| URL | `https://job-boards.eu.greenhouse.io/stubhubinc/jobs/4704159101` |

**Description Preview (first 200 words):**

```
&lt;div class=&quot;content-intro&quot;&gt;&lt;p&gt;StubHub is on a mission to redefine the live event experience on a global scale. Whether someone is looking to attend their first event or their hundredth, we’re here to delight them all the way from the moment they start looking for a ticket until they step through the gate. The same goes for our sellers. From fans selling a single ticket to the promoters of a worldwide stadium tour, we want StubHub to be the safest, most convenient way to offer a ticket to the millions of fans who browse our platform around the world.&lt;/p&gt;&lt;/div&gt;&lt;p&gt;We are seeking talented...
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
  "title": "Analytics Engineer II ",
  "company": "StubHub",
  "location": "Los Angeles, California, United States",
  "description": "&lt;div class=&quot;content-intro&quot;&gt;&lt;p&gt;StubHub is on a mission to redefine the live event experience on a global scale. Whether someone is looking to attend their first event or their hundredth, we’re here to delight them all the way from the moment they start looking for a ticket until they step through the gate. The same goes for our sellers. From fans selling a single ticket to the promoters of a worldwide stadium tour, we want StubHub to be the safest, most convenient way to off...",
  "url": "https://job-boards.eu.greenhouse.io/stubhubinc/jobs/4704159101",
  "apply_url": "https://job-boards.eu.greenhouse.io/stubhubinc/jobs/4704159101",
  "posted_at": 1762126051000,
  "level": "mid",
  "cost_milli_cents": 2
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 2,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
