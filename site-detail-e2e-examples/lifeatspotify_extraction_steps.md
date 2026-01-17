# Extraction Steps: lifeatspotify

**Detail URL:** `https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing`
**Source URL:** `https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing`
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

... (truncated, 10041 total chars)
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

Captured 10041 chars of commonmark content

```json
{
  "length": 10041,
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
      "sourceUrl": "https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing",
      "provider": "spidercloud",
      "siteId": "lifeatspotify",
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
  "description_uploads": 1
}
```

### Extractor Debug Trace

Ran 10 extractors with all strategies

```json
{
  "title": {
    "winner": "site_handler_title",
    "value": "Data Scientist"
  },
  "company": {
    "winner": "site_handler_company",
    "value": "Spotify"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "New York, NY"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": false
  },
  "level": {
    "winner": "default_level",
    "value": "mid"
  },
  "compensation": {
    "winner": "hinted_compensation",
    "value": 133593
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 126
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:33:48.458000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "# Data Scientist\n### Growth Analytics (Performance Marketing)\nLink copied to clipboard.\n](https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing#)\nData, Research &amp; Insights\n](https://www.lifeatspotify.com/job-categories/data-research-insights)\nData\nPermanent\nNew York\nWe are looking for a Data Scientist to join the Business Analytics team at Spotify. In this role, you’ll shape Spotify’s performance marketing and growth strategy by developing models, designing experiments and measuring the impact of marketing investments. Partnering with the Growth Analytics Lead, you’ll turn data into strategies that guide budget allocation, 
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
  "heuristicLastTried": 1768599228486,
  "heuristicVersion": 5,
  "locations": [
    "New York, NY"
  ],
  "location": "New York, NY",
  "locationStates": [
    "NY"
  ],
  "locationSearch": "New York NY",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "totalCompensation": 133593,
  "compensationUnknown": false,
  "compensationReason": "extractor:hinted_compensation"
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
| title | `site_handler_title` | `Data Scientist` |
| company | `site_handler_company` | `Spotify` |
| location | `raw_row_location` | `New York, NY` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `default_level` | `mid` |
| compensation | `hinted_compensation` | `133593` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `126` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:33:48.458000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `# Data Scientist
### Growth Analytics (Performance` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Data Scientist`
**Winning Strategy:** `site_handler_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_title** 🏆 | SITE_HANDLER | ✅ | `Data Scientist` | Valid title |
| raw_row_title | EXPLICIT_FIELD | ✅ | `Data Scientist` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `Data Scientist` | Valid title |
| hinted_title | HEURISTIC | ✅ | `Data Scientist` | Valid title |
| first_line_title | FALLBACK | ✅ | `Data Scientist` | Valid title |

#### COMPANY

**Final Value:** `Spotify`
**Winning Strategy:** `site_handler_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_company** 🏆 | SITE_HANDLER | ✅ | `Spotify` | Valid company name |
| raw_row_company | EXPLICIT_FIELD | ✅ | `Spotify` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Lifeatspotify` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `the Business Analytics team at` | Found 'Join Company' pattern |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `New York, NY`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'lifeatspotify' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `New York, NY` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `New York` | Matched pattern BASED_IN |
| country_only_fallback_location | CUSTOM_550 | ✅ | `New York, NY` | Country-only fallback: Valid location |
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
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'New York, NY' present but not inferring  |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Spotify' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `mid`
**Winning Strategy:** `default_level`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_level_field | STRUCTURED_DATA | ❌ | `` | Skipping mid-level default: mid |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ❌ | `` | No level indicator in title |
| content_pattern_level | CUSTOM_550 | ❌ | `` | No level pattern in content |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| **default_level** 🏆 | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `133593`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `133593` | Compensation from hint range: $110,018-$157,169 -> |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `133593` | Compensation range pattern: $110,018-$157,169 -> $ |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `126`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `126` | Valid cost: 126 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:33:48.458000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:33:48.458000` | Valid date: 2026-01-16T14:33:48.458000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'lifeatspotify' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:48.494906` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'lifeatspotify' returned no first_publishe |

#### DESCRIPTION

**Final Value:** `# Data Scientist
### Growth Analytics (Performance Marketing)
Link copied to clipboard.
](https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing#)
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
* Contribute to annual and quarterly planning through impact forecasting, budget allocation and scenario modelling
* Build scalable data pipelines and dashboards to track marketing efficiency and Global business performance
* Collaborate closely with business partners to understand growth drivers and guide strategic marketing decisions
* Present insights and recommendations to senior stakeholders, influencing the course of our business
Who You Are
* 3+ years synthesizing insights from data using tools such as Python/R and SQL
* Hands-on experience supporting marketing organizations with their paid media strategy, including running and analyzing incrementality tests on platforms like Meta, Google and TikTok
* Proven experience building advanced models to understand and optimise paid media campaigns
* Skilled at collaborating with business partners to measure the impact of marketing initiatives and presenting those findings in coherent recommendations
* Intellectually curious, creative, and diligent - you enjoy thinking about the business as much as about the data
* Have a background in Computer Science, Statistics, Engineering or other relevant field
* Comfortable working on a globally distributed team (with occasional international travel)
Where You'll Be
* This role is based in New York
* We offer you the flexibility to work where you work best! There will be some in person meetings, but still allows for flexibility to work from home.
Learn about life at Spotify
The United States base range for this position is$110,018 - $157,169, plus equity. The benefits available for this position include health insurance, six month paid parental leave, 401(k) retirement plan, monthly meal allowance, 23 paid days off, paid flexible holidays, paid sick leave. These ranges may be modified in the future.
Spotify is an equal opportunity employer. You are welcome at Spotify for who you are, no matter where you come from, what you look like, or what’s playing in your headphones. Our platform is for everyone, and so is our workplace. The more voices we have represented and amplified in our business, the more we will all thrive, contribute, and be forward-thinking! So bring us your personal experience, your perspectives, and your background. It’s in our differences that we will find the power to keep revolutionizing the way the world listens.
At Spotify, we are passionate about inclusivity and making sure our entire recruitment process is accessible to everyone. We have ways to request reasonable accommodations during the interview process and help assist in what you need. If you need accommodations at any stage of the application or interview process, please let us know - we’re here to support you in any way we can.
Spotify transformed music listening forever when we launched in 2008. Our mission is to unlock the potential of human creativity by giving a million creative artists the opportunity to live off their art and billions of fans the chance to enjoy and be passionate about these creators. Everything we do is driven by our love for music and podcasting. Today, we are the world’s most popular audio streaming subscription service.
Our global benefits
Extensive learning opportunities, through our dedicated team, GreenHouse.
Flexible share incentives letting you choose how you share in our success.
Global parental leave, six months off - for all new parents.
All The Feels, our employee assistance program and self-care hub.
Flexible public holidays, swap days off according to your values and beliefs.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `# Data Scientist
### Growth An` | Valid description (5444 chars, 821 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `# Data Scientist
### Growth An` | Valid description (5444 chars, 821 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `# Data Scientist
### Growth An` | Valid description (5444 chars, 821 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Data Scientist` |
| Company | `Spotify` |
| Location | `New York, NY` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1768599228458` |
| Description Words | `821` |
| Cost (milli-cents) | `126` |
| URL | `https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing` |

**Description Preview (first 200 words):**

```
# Data Scientist ### Growth Analytics (Performance Marketing) Link copied to clipboard. ](https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing#) Data, Research &amp; Insights ](https://www.lifeatspotify.com/job-categories/data-research-insights) Data Permanent New York We are looking for a Data Scientist to join the Business Analytics team at Spotify. In this role, you’ll shape Spotify’s performance marketing and growth strategy by developing models, designing experiments and measuring the impact of marketing investments. Partnering with the Growth Analytics Lead, you’ll turn data into strategies that guide budget allocation, optimize spend and accelerate efficient growth across paid media channels. You’ll collaborate with a global, cross-functional team of analysts, data scientists, marketers, business leaders, and engineers to scale insights and drive efficient growth. Learning and improving is part of our daily routine, and you will get a platform to develop your data skills and carve out efficient ways of working. The Business Analytics team is part of Spotify’s core business strategy organization. You’ll play a crucial role in the growth and direction of Spotify as we grow to 700M+ users around the globe. At your fingertips, you’ll have access to all of the data Spotify has to offer, and the opportunity to be creative with how you use it to derive insights and strategies. Above...
```

---

## Step 6: Convex Mutation Payload

**Ingested Jobs Count:** 1
**Stored Scrapes Count:** 1
**Description Uploads Count:** 1

### Sample Ingested Job Payload

This is what gets sent to `router:ingestJobsFromScrape`:

```json
{
  "title": "Data Scientist",
  "company": "Spotify",
  "location": "New York, NY",
  "description": "# Data Scientist\n### Growth Analytics (Performance Marketing)\nLink copied to clipboard.\n](https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing#)\nData, Research &amp; Insights\n](https://www.lifeatspotify.com/job-categories/data-research-insights)\nData\nPermanent\nNew York\nWe are looking for a Data Scientist to join the Business Analytics team at Spotify. In this role, you’ll shape Spotify’s performance marketing and growth strategy by developing models, designing...",
  "url": "https://www.lifeatspotify.com/jobs/data-scientist-growth-analytics-performance-marketing",
  "posted_at": 1768599228458,
  "level": "mid",
  "remote": false,
  "cost_milli_cents": 126,
  "_full_description_word_count": 821
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
