# Extraction Steps: coreweave

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4595172006`
**Source URL:** `https://api.greenhouse.io/v1/boards/coreweave/jobs`
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
  "url": "https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4595172006",
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
      "url": "https://boards-api.greenhouse.io/v1/boards/coreweave/jobs/4595172006",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/coreweave/jobs",
      "provider": "spidercloud",
      "siteId": "coreweave",
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
    "value": "Accounting Manager - M&A"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "CoreWeave"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Livingston, NJ/ Sunnyvale, CA / Bellevue, WA"
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
    "value": "2025-08-21T16:36:59"
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "&lt;div class=&quot;content-intro&quot;&gt;&lt;div&gt; &lt;div&gt; &lt;div class=&quot;gmail_quote&quot;&gt; &lt;div&gt; &lt;div&gt;&lt;span id=&quot;m_1770241969069985273m_-2746164444908759431gmail-docs-internal-guid-131e4fb0-7fff-b4e9-ff50-e8cf32449b1b&quot;&gt;CoreWeave is The Essential Cloud for AI™. Built for pioneers by pioneers, CoreWeave delivers a platform of technology, tools, and teams that enables innovators to build and scale AI with confidence. Trusted by leading AI labs, startups, and global enterprises, CoreWeave combines superior infrastructure performance with deep technical expertise to accelerate breakthroughs and turn compute into capability. Founded in 2017, CoreWeave became a publicly traded company (Nasdaq: CRWV) in March 2025. Learn more at&amp;nbsp;&lt;a href=&quot;http://www
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Accounting Manager - M&A`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768590842394,
  "heuristicVersion": 5,
  "locations": [
    "Livingston, NJ",
    "Sunnyvale, CA",
    "Bellevue, WA"
  ],
  "location": "Livingston, NJ",
  "locationStates": [
    "NJ",
    "CA",
    "WA"
  ],
  "locationSearch": "Livingston NJ WA Sunnyvale Bellevue CA",
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
| title | `raw_row_title` | `Accounting Manager - M&A` |
| company | `raw_row_company` | `CoreWeave` |
| location | `raw_row_location` | `Livingston, NJ/ Sunnyvale, CA / Bellevue, WA` |
| remote | `default_remote` | `(none)` |
| level | `explicit_level_field` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| posted_at | `explicit_posted_at_field` | `2025-08-21 16:36:59` |
| description | `normalized_markdown_description` | `&lt;div class=&quot;content-intro&quot;&gt;&lt;div` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Accounting Manager - M&A`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Accounting Manager - M&A` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ❌ | `` | No title in hints |
| first_line_title | FALLBACK | ❌ | `` | No valid title found in first lines |

#### COMPANY

**Final Value:** `CoreWeave`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `CoreWeave` | Valid company name |
| url_company | URL_DERIVED | ✅ | `CoreWeave` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ❌ | `` | No company pattern matched |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Livingston, NJ/ Sunnyvale, CA / Bellevue, WA`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Livingston, NJ/ Sunnyvale, CA ` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Livingston, NJ/ Sunnyvale, CA ` | Country-only fallback: Valid location |
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
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Livingston, NJ/ Sunnyvale, CA / Bellevue |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | No remote in hints |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'CoreWeave' not in remote company list |
| **default_remote** 🏆 | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `mid`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `mid` | Explicit level field: mid -> mid |
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

#### POSTED_AT

**Final Value:** `2025-08-21 16:36:59`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-08-21 16:36:59` | Valid date: 2025-08-21T16:36:59 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 12:14:02.400081` | Using current time as fallback (date unknown) |

#### DESCRIPTION

**Final Value:** `&lt;div class=&quot;content-intro&quot;&gt;&lt;div&gt; &lt;div&gt; &lt;div class=&quot;gmail_quote&quot;&gt; &lt;div&gt; &lt;div&gt;&lt;span id=&quot;m_1770241969069985273m_-2746164444908759431gmail-docs-internal-guid-131e4fb0-7fff-b4e9-ff50-e8cf32449b1b&quot;&gt;CoreWeave is The Essential Cloud for AI™. Built for pioneers by pioneers, CoreWeave delivers a platform of technology, tools, and teams that enables innovators to build and scale AI with confidence. Trusted by leading AI labs, startups, and global enterprises, CoreWeave combines superior infrastructure performance with deep technical expertise to accelerate breakthroughs and turn compute into capability. Founded in 2017, CoreWeave became a publicly traded company (Nasdaq: CRWV) in March 2025. Learn more at&amp;nbsp;&lt;a href=&quot;http://www.coreweave.com/&quot; target=&quot;_blank&quot; data-saferedirecturl=&quot;https://www.google.com/url?q=http://www.coreweave.com&amp;amp;source=gmail&amp;amp;ust=1762613132717000&amp;amp;usg=AOvVaw3D-UOhNaqEvF5BEWxjYyAU&quot;&gt;www.coreweave.com&lt;/a&gt;.&lt;/span&gt;&lt;/div&gt; &lt;/div&gt; &lt;/div&gt; &lt;/div&gt; &lt;/div&gt;&lt;/div&gt;&lt;p&gt;&lt;strong&gt;What You’ll Do:&lt;/strong&gt;&lt;/p&gt; &lt;p&gt;The Accounting Manager-M&amp;amp;A will report directly to the Financial Close...`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `&lt;div class=&quot;content-in` | Valid description (1312 chars, 100 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `&lt;div class=&quot;content-in` | Valid description (1312 chars, 100 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `&lt;div class=&quot;content-in` | Valid description (1312 chars, 100 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Accounting Manager - M&A` |
| Company | `CoreWeave` |
| Location | `Livingston, NJ` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1755819419000` |
| Description Words | `100` |
| Cost (milli-cents) | `3` |
| URL | `https://coreweave.com/careers/job?4595172006&board=coreweave&gh_jid=4595172006` |

**Description Preview (first 200 words):**

```
&lt;div class=&quot;content-intro&quot;&gt;&lt;div&gt; &lt;div&gt; &lt;div class=&quot;gmail_quote&quot;&gt; &lt;div&gt; &lt;div&gt;&lt;span id=&quot;m_1770241969069985273m_-2746164444908759431gmail-docs-internal-guid-131e4fb0-7fff-b4e9-ff50-e8cf32449b1b&quot;&gt;CoreWeave is The Essential Cloud for AI™. Built for pioneers by pioneers, CoreWeave delivers a platform of technology, tools, and teams that enables innovators to build and scale AI with confidence. Trusted by leading AI labs, startups, and global enterprises, CoreWeave combines superior infrastructure performance with deep technical expertise to accelerate breakthroughs and turn compute into capability. Founded in 2017, CoreWeave became a publicly traded company (Nasdaq: CRWV) in March 2025. Learn more at&amp;nbsp;&lt;a href=&quot;http://www.coreweave.com/&quot; target=&quot;_blank&quot; data-saferedirecturl=&quot;https://www.google.com/url?q=http://www.coreweave.com&amp;amp;source=gmail&amp;amp;ust=1762613132717000&amp;amp;usg=AOvVaw3D-UOhNaqEvF5BEWxjYyAU&quot;&gt;www.coreweave.com&lt;/a&gt;.&lt;/span&gt;&lt;/div&gt; &lt;/div&gt; &lt;/div&gt; &lt;/div&gt; &lt;/div&gt;&lt;/div&gt;&lt;p&gt;&lt;strong&gt;What You’ll Do:&lt;/strong&gt;&lt;/p&gt; &lt;p&gt;The Accounting Manager-M&amp;amp;A will report directly to the Financial Close...
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
  "title": "Accounting Manager - M&A",
  "company": "CoreWeave",
  "location": "Livingston, NJ/ Sunnyvale, CA / Bellevue, WA",
  "description": "&lt;div class=&quot;content-intro&quot;&gt;&lt;div&gt; &lt;div&gt; &lt;div class=&quot;gmail_quote&quot;&gt; &lt;div&gt; &lt;div&gt;&lt;span id=&quot;m_1770241969069985273m_-2746164444908759431gmail-docs-internal-guid-131e4fb0-7fff-b4e9-ff50-e8cf32449b1b&quot;&gt;CoreWeave is The Essential Cloud for AI™. Built for pioneers by pioneers, CoreWeave delivers a platform of technology, tools, and teams that enables innovators to build and scale AI with confidence. Trusted by leading AI labs, startups,...",
  "url": "https://coreweave.com/careers/job?4595172006&board=coreweave&gh_jid=4595172006",
  "apply_url": "https://coreweave.com/careers/job?4595172006&board=coreweave&gh_jid=4595172006",
  "posted_at": 1755819419000,
  "level": "mid",
  "cost_milli_cents": 3
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 3,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
