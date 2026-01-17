# Extraction Steps: dataminr

**Detail URL:** `https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/Melbourne-AU/Customer-Success-Associate_JR1945`
**Source URL:** `https://dataminr.wd12.myworkdayjobs.com/en-US/Dataminr?q=engineer`
**Handler:** `workday`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
<html><meta name="color-scheme" content="light dark"><meta charset="utf-8"><pre>{"jobPostingInfo":{"id":"b4a4c1a1722b100161f703a736cc0000","title":"Customer Success Associate","jobDescription":"&lt;p style=\"text-align:inherit\"&gt;&lt;/p&gt;&lt;p style=\"text-align:left\"&gt;&lt;b&gt;See yourself at Dataminr&lt;/b&gt;&lt;/p&gt;As a Customer Success Associate, you will drive Dataminr product adoption, renewal, and usage among APAC customers, as well as build a strategy to foster account growth i...
```

---

## Step 2: Handler Detection

**Detected Handler:** `workday`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
<html><meta name="color-scheme" content="light dark"><meta charset="utf-8"><pre>{"jobPostingInfo":{"id":"b4a4c1a1722b100161f703a736cc0000","title":"Customer Success Associate","jobDescription":"&lt;p style=\"text-align:inherit\"&gt;&lt;/p&gt;&lt;p style=\"text-align:left\"&gt;&lt;b&gt;See yourself at Dataminr&lt;/b&gt;&lt;/p&gt;As a Customer Success Associate, you will drive Dataminr product adoption, renewal, and usage among APAC customers, as well as build a strategy to foster account growth i...
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: workday

```json
{
  "url": "https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/Melbourne-AU/Customer-Success-Associate_JR1945",
  "handler": "workday"
}
```

### Raw Content Capture

Captured 503 chars of raw_html content

```json
{
  "length": 503,
  "content_type": "raw_html"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 503 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 503
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/Melbourne-AU/Customer-Success-Associate_JR1945",
      "sourceUrl": "https://dataminr.wd12.myworkdayjobs.com/en-US/Dataminr?q=engineer",
      "provider": "spidercloud",
      "siteId": "dataminr",
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
    "value": "Customer Success Associate"
  },
  "company": {
    "winner": "site_handler_company",
    "value": "Dataminr"
  },
  "location": {
    "winner": "site_handler_location_hint",
    "value": ": Melbourne, AU"
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
    "winner": "unknown_compensation",
    "value": 0
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 3
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-08T08:00:00"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "# Customer Success Associate\n\nLocation: Melbourne, AU\n\n>See yourself at Dataminr>As a Customer Success Associate, you will drive Dataminr product adoption, renewal, and usage among APAC customers, as well as build a strategy to foster account growth in Defense, Public Sector and Corporate Risk. You are excited to combine your interests in technology, government, national security, and customer success to transform the way our customers adopt and use our products. This role is based in the Eastern Australian time zone.>>AI Innovation at Dataminr>>>Working at Dataminr you’ll have the opportunity to tackle the most exciting trends in AI on a daily basis to 
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Customer Success Associate`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599081163,
  "heuristicVersion": 5,
  "locations": [
    "Melbourne, Australia"
  ],
  "location": "Melbourne, Australia",
  "locationStates": [
    "Australia"
  ],
  "locationSearch": "Melbourne Australia",
  "countries": [
    "Australia"
  ],
  "country": "Australia"
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
| title | `site_handler_title` | `Customer Success Associate` |
| company | `site_handler_company` | `Dataminr` |
| location | `site_handler_location_hint` | `: Melbourne, AU` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `default_level` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `3` |
| posted_at | `explicit_posted_at_field` | `2026-01-08 08:00:00` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `# Customer Success Associate

Location: Melbourne,` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Customer Success Associate`
**Winning Strategy:** `site_handler_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_title** 🏆 | SITE_HANDLER | ✅ | `Customer Success Associate` | Valid title |
| raw_row_title | EXPLICIT_FIELD | ✅ | `Customer Success Associate` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `Customer Success Associate` | Valid title |
| hinted_title | HEURISTIC | ✅ | `Customer Success Associate` | Valid title |
| first_line_title | FALLBACK | ✅ | `Customer Success Associate` | Valid title |

#### COMPANY

**Final Value:** `Dataminr`
**Winning Strategy:** `site_handler_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_company** 🏆 | SITE_HANDLER | ✅ | `Dataminr` | Valid company name |
| raw_row_company | EXPLICIT_FIELD | ✅ | `Dataminr` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Dataminr` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `Dataminr` | Found 'Work at Company' pattern |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `: Melbourne, AU`
**Winning Strategy:** `site_handler_location_hint`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_location_hint** 🏆 | SITE_HANDLER | ✅ | `: Melbourne, AU` | Valid location |
| raw_row_location | EXPLICIT_FIELD | ✅ | `Melbourne, Australia` | Valid location |
| explicit_label_location | CUSTOM_350 | ✅ | `Melbourne, AU` | Found 'Location:' label |
| url_location | URL_DERIVED | ✅ | `Melbourne Au` | Extracted from URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `Melbourne, AU` | Matched pattern LOCATION_CITY_STATE |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Melbourne, Australia` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `Melbourne, Australia` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location ': Melbourne, AU' present but not inferri |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Dataminr' not in remote company list |
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

**Final Value:** `3`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `3` | Valid cost: 3 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-08 08:00:00`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-08 08:00:00` | Valid date: 2026-01-08T08:00:00 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'workday' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:31:21.176860` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'workday' returned no first_published |

#### DESCRIPTION

**Final Value:** `# Customer Success Associate

Location: Melbourne, AU

>See yourself at Dataminr>As a Customer Success Associate, you will drive Dataminr product adoption, renewal, and usage among APAC customers, as well as build a strategy to foster account growth in Defense, Public Sector and Corporate Risk. You are excited to combine your interests in technology, government, national security, and customer success to transform the way our customers adopt and use our products. This role is based in the Eastern Australian time zone.>>AI Innovation at Dataminr>>>Working at Dataminr you’ll have the opportunity to tackle the most exciting trends in AI on a daily basis to power a revolutionary product that uncovers critical events around the world as they unfold.>>>>Regenerative AI>>>:> our AI technology, ReGenAI, is a new form of generative AI that automatically regenerates real-time Live Event Briefs as events unfold. Learn more >>>here>>>.>>>>Agentic AI>>:> >we recently launched our Agentic AI capability, what we’re calling our Intel Agents, that autonomously generates critical context for our clients on real-time events, threats, and risks allowing them to see the clearest, most accurate view of what’s happening on the ground. Learn more >>>here>>>Multimodal AI:>> our platform detects events from many different types of data (images, video, sensor data, audio, and text in over 150 languages). Learn more >>>here>>>.>>The opportunity>>Expand the scope and scale of Dataminr products (First Alert and Pulse) adoption, while driving successful customer outcomes within your assigned portfolio including product adoption, renewal and high customer satisfaction.>Design and implement scalable programs that seamlessly integrate First Alert and Pulse into customer workflows, and align with customer’s enterprise-level objectives.>Develop and maintain relationships across your account portfolio through capability briefings, product demonstrations, training, and execution of playbooks / regular check-ins.>Serve as a First Alert and Pulse platform expert ensuring customer engagements support organisational goals and drive positive outcomes for the customer beyond basic platform features and functionality.>Collect customer feedback and clearly articulate recommendations for First Alert and Pulse product development to our engineering and product teams.>Successfully leverage data-driven metrics and reporting to understand and stay ahead of risks and opportunities that impact retention and growth.>>>What you bring>At Dataminr, we value you for who you are. We encourage you to apply for this role, even if you don&'t meet every qualification. Our candidates are reviewed on the basis of their skill and potential to succeed.>Bachelor&'s degree or equivalent relevant experience in a related field AND 3 years of enterprise software or SaaS customer success/account management experience supporting Corporate Risk as well as Defense and the public sector, including but not limited to Federal, State, and Local government organizations.>Knowledge and/or experience with publicly available information and the real-time alerting needs of public and private sector organizations, including but not limited to Defense, Corporate Security, law enforcement, and emergency management.>Outstanding ability to communicate both orally and written complex concepts to a wide range of audiences, including technical and non-technical customers within the Software/SaaS space or with senior leaders and decision makers in the form of a presentation.>High level of accountability and the ability to execute independently on multiple and competing projects and deadlines.>Proficiency in other APAC Regional Languages (Japanese, Singaporean Mandarin) is desirable but not required.>>#LI-WC1>About Dataminr>>At Dataminr, we are a mission driven team of talented builders, creators and visionaries who have real-world impact on how organizations are able to respond to events. Dataminr’s groundbreaking, AI-powered, intelligence platform provides organizations with the earliest signals of emerging risks, events, and threats before they unfold. Trusted by two-thirds of the Fortune 50 and half of the Fortune 100, Dataminr’s platform analyzes billions of public data inputs spanning text, image, video, audio and sensor data across 150&+ languages, empowering our clients to stay one step ahead in an increasingly complex world where every second counts.>>Founded in 2009, we have pioneered the world’s first real-time event detection platform, long before the recent Gen AI ‘boom.’ Dataminr operates all around the world united by our passion to use AI for the greater good, be agents of positive change and put our technology into the hands of clients charged with the responsibility to keep organizations running and keep people safe.>>As our employees focus on developing our revolutionary technology, we focus on our employees. Dataminr is proud to offer a variety of flexible work arrangements, offices all over the world to foster collaboration, generous PTO and sick leave, and more, as part of our competitive benefits package aimed at keeping all our employees happy and healthy. Explore all our benefits> >>>>>here>>>>.>>We believe our differences give us strength. Our employees are empowered to be their best, authentic selves through various opportunities, such as our robust employee resource group (ERG) network, manager development programming, professional development funds, and more.>>We serve a global community made up of many cultures and strive to reflect the world and clients we serve, with a workforce built on merit and equity. We actively condemn racism and discrimination in any form. We stand for social good, fostering a culture of allyship, and standing up for those who face systemic barriers to equality. We lead with empathy and strive to be agents of positive change in our company and in our communities.>>>Dataminr is an equal opportunity and affirmative action employer. Individuals seeking employment at Dataminr are considered without regards to race, sex, colour, creed, religion, national origin, age, disability, genetics, marital status, pregnancy, unemployment status, sexual orientation, citizenship status or veteran status.>>>>Dataminr will collect and process your personal data. All personal data will be processed in accordance with applicable data protection laws. Please see Dataminr&'s candidate privacy notice available >here. By providing your details and applying via our careers website, you acknowledge that you have read our candidate privacy notice. If you have any queries, please contact the People Team at hr&@dataminr.com or >>>privacy&@dataminr.com>>>.>>`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `# Customer Success Associate

` | Valid description (6716 chars, 957 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `# Customer Success Associate

` | Valid description (6716 chars, 957 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `# Customer Success Associate

` | Valid description (6716 chars, 957 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Customer Success Associate` |
| Company | `Dataminr` |
| Location | `Melbourne, Australia` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1767884400000` |
| Description Words | `957` |
| Cost (milli-cents) | `3` |
| URL | `https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/Melbourne-AU/Customer-Success-Associate_JR1945` |

**Description Preview (first 200 words):**

```
# Customer Success Associate Location: Melbourne, AU >See yourself at Dataminr>As a Customer Success Associate, you will drive Dataminr product adoption, renewal, and usage among APAC customers, as well as build a strategy to foster account growth in Defense, Public Sector and Corporate Risk. You are excited to combine your interests in technology, government, national security, and customer success to transform the way our customers adopt and use our products. This role is based in the Eastern Australian time zone.>>AI Innovation at Dataminr>>>Working at Dataminr you’ll have the opportunity to tackle the most exciting trends in AI on a daily basis to power a revolutionary product that uncovers critical events around the world as they unfold.>>>>Regenerative AI>>>:> our AI technology, ReGenAI, is a new form of generative AI that automatically regenerates real-time Live Event Briefs as events unfold. Learn more >>>here>>>.>>>>Agentic AI>>:> >we recently launched our Agentic AI capability, what we’re calling our Intel Agents, that autonomously generates critical context for our clients on real-time events, threats, and risks allowing them to see the clearest, most accurate view of what’s happening on the ground. Learn more >>>here>>>Multimodal AI:>> our platform detects events from many different types of data (images, video,...
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
  "title": "Customer Success Associate",
  "company": "Dataminr",
  "location": "Melbourne, Australia",
  "description": "# Customer Success Associate\n\nLocation: Melbourne, AU\n\n>See yourself at Dataminr>As a Customer Success Associate, you will drive Dataminr product adoption, renewal, and usage among APAC customers, as well as build a strategy to foster account growth in Defense, Public Sector and Corporate Risk. You are excited to combine your interests in technology, government, national security, and customer success to transform the way our customers adopt and use our products. This role is based in the Easter...",
  "url": "https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/Melbourne-AU/Customer-Success-Associate_JR1945",
  "posted_at": 1767884400000,
  "level": "mid",
  "remote": false,
  "cost_milli_cents": 3,
  "_full_description_word_count": 957
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
