# Extraction Steps: spotify

**Detail URL:** `https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing`
**Source URL:** `https://www.lifeatspotify.com/jobs?l=new-york&l=boston&l=toronto&l=united-states-of-america-home-mix&c=backend&c=client-c&c=data&c=developer-tools-infrastructure&c=engineering-leadership&c=machine-learning&c=mobile&c=network-engineering-it&c=security&c=tech-research&c=web&c=data-insights-leadership&c=data-science&c=machine-learning-data-research-insights&c=tech-research-data-research-insights&c=user-research&c=product&c=design-ops&c=editorial-design&c=internal-tools-design&c=product-design&c=ux-writing`
**Handler:** `lifeatspotify`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Data Scientist - Growth Analytics (Performance Marketing) | Life at Spotify
[![](data:image/svg+xml,%3csvg%20xmlns=%27http://www.w3.org/2000/svg%27%20version=%271.1%27%20width=%2740%27%20height=%2740%27/%3e)![Spotify Logo](https://www.lifeatspotify.com/_next/static/media/spotify-logo-light.113c7bc8.svg)](https://www.lifeatspotify.com/)
[All Jobs](https://www.lifeatspotify.com/jobs)
[![](data:image/svg+xml,%3csvg%20xmlns=%27http://www.w3.org/2000/svg%27%20version=%271.1%27%20width=%2740%27%20height=%2740%27/%3e)![Spotify Logo](https://www.lifeatspotify.com/_next/static/media/spotify-logo-light.113c7bc8.svg)](https://www.lifeatspotify.com/)
[Locations](https://www.lifeatspotify.com/locations)
[Being here](https://www.lifeatspotify.com/being-here)
[Equity, Diversity &amp; Impact](https://www.lifeatspotify.com/equity-diversity-impact)
[Students](https://www.lifeatspotify.com/students)[How We Hire](https://www.lifeatspotify.com/how-we-hire)[Latest](https://www.lifeatspotify.com/latest)
[All Jobs](https://www.lifeatspotify.com/jobs)
# Data Scientist
### Growth Analytics (Performance Marketing)
Link copied to clipboard.
[
Apply now
Apply
](https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing#)
[
Data, Research &amp; Insights
](https://www.lifeatspotify.com/job-categories/data-research-insights)
Data
Permanent
New York
We are looking for a Data Scientist to join the Business Analytics team at Spotify. In this role, you’ll shape Spotify’s performance marketing and growth strategy by developing models, designing experiments and measuring the impact of marketing investments. Partnering with the Growth Analytics Lead, you’ll turn data into strategies that guide budget allocation, optimize spend and accelerate efficient growth across paid media channels. You’ll collaborate with a global, cross-functional team of analysts, data scientists, marketers, business leaders, and engineers to scale insights and drive efficient growth. Learning and improving is part of our daily routine, and you will get a platform to develop your data skills and carve out efficient ways of working.
The Business Analytics team is part of Spotify’s core business strategy organization. You’ll play a crucial role in the growth and direction of Spotify as we grow to 700M+ users around the globe. At your fingertips, you’ll have access to all of the data Spotify has to offer, and the opportunity to be creative with how you use it to derive insights and strategies. Above all, your work will impact the way the world experiences audio!
What You'll Do
* Develop data-driven strategies to drive the growth of Spotify users and subscribers, with a focus on performance marketing
* Refine attribution practices and define robust, data-backed methods to measure the incremental impact of marketing spend
* Partner with third-party advertising platforms and agencies to design and implement comprehensive experiments and lift studies
* Contribute to annual and quarterly pl

... (truncated, 10012 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `lifeatspotify`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `Data Scientist`

Normalized markdown after handler processing:

```markdown
# Data Scientist
### Growth Analytics (Performance Marketing)
Link copied to clipboard.
[
](https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing#)
[
Data, Research &amp; Insights
](https://www.lifeatspotify.com/job-categories/data-research-insights)
Data
Permanent
New York
We are looking for a Data Scientist to join the Business Analytics team at Spotify. In this role, you’ll shape Spotify’s performance marketing and growth strategy by developing models, designing experiments and measuring the impact of marketing investments. Partnering with the Growth Analytics Lead, you’ll turn data into strategies that guide budget allocation, optimize spend and accelerate efficient growth across paid media channels. You’ll collaborate with a global, cross-functional team of analysts, data scientists, marketers, business leaders, and engineers to scale insights and drive efficient growth. Learning and improving is part of our daily routine, and you will get a platform to develop your data skills and carve out efficient ways of working.
The Business Analytics team is part of Spotify’s core business strategy organization. You’ll play a crucial role in the growth and direction of Spotify as we grow to 700M+ users around the globe. At your fingertips, you’ll have access to all of the data Spotify has to offer, and the opportunity to be creative with how you use it to derive insights and strategies. Above all, your work will impact the way the world experiences audio!
What You'll Do
* Develop data-driven strategies to drive the growth of Spotify users and subscribers, with a focus on performance marketing
* Refine attribution practices and define robust, data-backed methods to measure the incremental impact of marketing spend
* Partner with third-party advertising platforms and agencies to design and implement comprehensive experiments and lift studies
* Contribute to annual and quarterly planning through impact forecasting, budget allocation and sc

... (truncated, 5448 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: lifeatspotify

```json
{
  "url": "https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing",
  "handler": "lifeatspotify"
}
```

### Raw Content Capture

Captured 10012 chars of commonmark content

```json
{
  "length": 10012,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='Data Scientist', 5448 chars of normalized content

```json
{
  "title": "Data Scientist",
  "normalized_length": 5448
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing",
      "sourceUrl": "https://www.lifeatspotify.com/jobs?l=new-york&l=boston&l=toronto&l=united-states-of-america-home-mix&c=backend&c=client-c&c=data&c=developer-tools-infrastructure&c=engineering-leadership&c=machine-learning&c=mobile&c=network-engineering-it&c=security&c=tech-research&c=web&c=data-insights-leadership&c=data-science&c=machine-learning-data-research-insights&c=tech-research-data-research-insights&c=user-research&c=product&c=design-ops&c=editorial-design&c=internal-tools-design&c=product-design&c=ux-writing",
      "provider": "spidercloud",
      "siteId": "spotify",
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
    "value": "Data Scientist"
  },
  "company": {
    "winner": "site_handler_company",
    "value": "Spotify"
  },
  "location": {
    "winner": "content_pattern_location",
    "value": "Equity, Diversity"
  },
  "remote": {
    "winner": "explicit_remote_flag",
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
    "winner": "now_fallback_posted_at",
    "value": "2026-01-16T13:14:35.365496"
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Data Scientist - Growth Analytics (Performance Marketing) | Life at Spotify [![](data:image/svg+xml,%3csvg%20xmlns=%27http://www.w3.org/2000/svg%27%20version=%271.1%27%20width=%2740%27%20height=%2740%27/%3e)![Spotify Logo](https://www.lifeatspotify.com/_next/static/media/spotify-logo-light.113c7bc8.svg)](https://www.lifeatspotify.com/) [All Jobs](https://www.lifeatspotify.com/jobs) [![](data:image/svg+xml,%3csvg%20xmlns=%27http://www.w3.org/2000/svg%27%20version=%271.1%27%20width=%2740%27%20height=%2740%27/%3e)![Spotify Logo](https://www.lifeatspotify.com/_next/static/media/spotify-logo-light.113c7bc8.svg)](https://www.lifeatspotify.com/) [Locations](https://www.lifeatspotify.com/locations) [Being here](https://www.lifeatspotify.com/being-here) [Equity, Diversity &amp; Impact](https://www.lifeatspotify.com/equity-di
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Data Scientist`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768594475358,
  "heuristicVersion": 5,
  "location": "Equity, Diversity",
  "locationSearch": "Equity, Diversity"
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
| title | `raw_row_title` | `Data Scientist` |
| company | `site_handler_company` | `Spotify` |
| location | `content_pattern_location` | `Equity, Diversity` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| posted_at | `now_fallback_posted_at` | `2026-01-16 13:14:35.365496` |
| description | `normalized_markdown_description` | `Data Scientist - Growth Analytics (Performance Mar` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Data Scientist`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'lifeatspotify' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Data Scientist` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ✅ | `Data Scientist - Growth Analyt` | Valid title |
| first_line_title | FALLBACK | ❌ | `` | No valid title found in first lines |

#### COMPANY

**Final Value:** `Spotify`
**Winning Strategy:** `site_handler_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_company** 🏆 | SITE_HANDLER | ✅ | `Spotify` | Valid company name |
| raw_row_company | EXPLICIT_FIELD | ✅ | `Lifeatspotify` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Lifeatspotify` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `the Business Analytics team at` | Found 'Join Company' pattern |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Equity, Diversity`
**Winning Strategy:** `content_pattern_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'lifeatspotify' returned no location hint |
| raw_row_location | EXPLICIT_FIELD | ❌ | `` | Location too short and not a known format: Us |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| **content_pattern_location** 🏆 | CONTENT_PATTERN | ✅ | `Equity, Diversity` | Matched pattern LOCATION_FULL |
| country_only_fallback_location | CUSTOM_550 | ❌ | `` | Location invalid even with country-only allowed: L |
| hinted_location | HEURISTIC | ✅ | `New York, NY` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Equity, Diversity' present but not infer |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Spotify' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

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

**Final Value:** `2026-01-16 13:14:35.365496`
**Winning Strategy:** `now_fallback_posted_at`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_posted_at_field | STRUCTURED_DATA | ❌ | `` | No posted_at field in raw row |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'lifeatspotify' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| **now_fallback_posted_at** 🏆 | FALLBACK | ✅ | `2026-01-16 13:14:35.365496` | Using current time as fallback (date unknown) |

#### DESCRIPTION

**Final Value:** `Data Scientist - Growth Analytics (Performance Marketing) | Life at Spotify [![](data:image/svg+xml,%3csvg%20xmlns=%27http://www.w3.org/2000/svg%27%20version=%271.1%27%20width=%2740%27%20height=%2740%27/%3e)![Spotify Logo](https://www.lifeatspotify.com/_next/static/media/spotify-logo-light.113c7bc8.svg)](https://www.lifeatspotify.com/) [All Jobs](https://www.lifeatspotify.com/jobs) [![](data:image/svg+xml,%3csvg%20xmlns=%27http://www.w3.org/2000/svg%27%20version=%271.1%27%20width=%2740%27%20height=%2740%27/%3e)![Spotify Logo](https://www.lifeatspotify.com/_next/static/media/spotify-logo-light.113c7bc8.svg)](https://www.lifeatspotify.com/) [Locations](https://www.lifeatspotify.com/locations) [Being here](https://www.lifeatspotify.com/being-here) [Equity, Diversity &amp; Impact](https://www.lifeatspotify.com/equity-diversity-impact) [Students](https://www.lifeatspotify.com/students)[How We Hire](https://www.lifeatspotify.com/how-we-hire)[Latest](https://www.lifeatspotify.com/latest) [All Jobs](https://www.lifeatspotify.com/jobs) # Data Scientist ### Growth Analytics (Performance Marketing) Link copied to clipboard. [ Apply now Apply ](https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing#) [ Data, Research &amp; Insights ](https://www.lifeatspotify.com/job-categories/data-research-insights) Data Permanent New York We are looking for a Data Scientist to join the Business Analytics team at Spotify. In this role, you’ll shape Spotify’s performance marketing and growth strategy by developing models, designing experiments and measuring the impact of marketing investments. Partnering with the Growth Analytics Lead,...`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Data Scientist - Growth Analyt` | Valid description (1672 chars, 100 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Data Scientist - Growth Analyt` | Valid description (1672 chars, 100 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Data Scientist - Growth Analyt` | Valid description (1672 chars, 100 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Data Scientist` |
| Company | `Lifeatspotify` |
| Location | `Equity, Diversity` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `None` |
| Description Words | `100` |
| Cost (milli-cents) | `126` |
| URL | `https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing` |

**Description Preview (first 200 words):**

```
Data Scientist - Growth Analytics (Performance Marketing) | Life at Spotify [![](data:image/svg+xml,%3csvg%20xmlns=%27http://www.w3.org/2000/svg%27%20version=%271.1%27%20width=%2740%27%20height=%2740%27/%3e)![Spotify Logo](https://www.lifeatspotify.com/_next/static/media/spotify-logo-light.113c7bc8.svg)](https://www.lifeatspotify.com/) [All Jobs](https://www.lifeatspotify.com/jobs) [![](data:image/svg+xml,%3csvg%20xmlns=%27http://www.w3.org/2000/svg%27%20version=%271.1%27%20width=%2740%27%20height=%2740%27/%3e)![Spotify Logo](https://www.lifeatspotify.com/_next/static/media/spotify-logo-light.113c7bc8.svg)](https://www.lifeatspotify.com/) [Locations](https://www.lifeatspotify.com/locations) [Being here](https://www.lifeatspotify.com/being-here) [Equity, Diversity &amp; Impact](https://www.lifeatspotify.com/equity-diversity-impact) [Students](https://www.lifeatspotify.com/students)[How We Hire](https://www.lifeatspotify.com/how-we-hire)[Latest](https://www.lifeatspotify.com/latest) [All Jobs](https://www.lifeatspotify.com/jobs) # Data Scientist ### Growth Analytics (Performance Marketing) Link copied to clipboard. [ Apply now Apply ](https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing#) [ Data, Research &amp; Insights ](https://www.lifeatspotify.com/job-categories/data-research-insights) Data Permanent New York We are looking for a Data Scientist to join the Business Analytics team at Spotify. In this role, you’ll shape Spotify’s performance marketing and growth strategy by developing models, designing experiments and measuring the impact of marketing investments. Partnering with the Growth Analytics Lead,...
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
  "title": "Data Scientist",
  "company": "Lifeatspotify",
  "location": "Us",
  "description": "Data Scientist - Growth Analytics (Performance Marketing) | Life at Spotify [![](data:image/svg+xml,%3csvg%20xmlns=%27http://www.w3.org/2000/svg%27%20version=%271.1%27%20width=%2740%27%20height=%2740%27/%3e)![Spotify Logo](https://www.lifeatspotify.com/_next/static/media/spotify-logo-light.113c7bc8.svg)](https://www.lifeatspotify.com/) [All Jobs](https://www.lifeatspotify.com/jobs) [![](data:image/svg+xml,%3csvg%20xmlns=%27http://www.w3.org/2000/svg%27%20version=%271.1%27%20width=%2740%27%20heig...",
  "url": "https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing",
  "level": "mid",
  "remote": false,
  "cost_milli_cents": 126,
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
  "costMilliCents": 126,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
