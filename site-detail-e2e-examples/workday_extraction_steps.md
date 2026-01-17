# Extraction Steps: workday

**Detail URL:** `https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/San-Antonio-TX/Field-Services-Engineer--USAF--Onsite-in-San-Antonio--TX-_JR1954`
**Source URL:** `https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/San-Antonio-TX/Field-Services-Engineer--USAF--Onsite-in-San-Antonio--TX-_JR1954`
**Handler:** `workday`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
```
{"jobPostingInfo":{"id":"b4a4c1a1722b1001f487f85a3c880000","title":"Field Services Engineer, USAF (Onsite in San Antonio, TX)","jobDescription":"&lt;&lt;p style=\\"text-align:inherit\\"&gt;&gt;&lt;&lt;/p&gt;&gt;&lt;&lt;p style=\\"text-align:left\\"&gt;&gt;&lt;&lt;b&gt;&gt;See yourself at Dataminr&lt;&lt;/b&gt;&gt;&lt;&lt;/p&gt;&gt;We are seeking a hands-on, customer-facing facing and mission-focused Field Services Engineer to support the deployment and growth of a large US Air Force system. This is an on-site role in San Antonio, TX. &lt;&lt;br&gt;&gt;&lt;&lt;br&gt;&gt;This role is ideal for a hands-on keyboard engineer who thrives in operational environments where real-world missions rely on your work. If you enjoy direct customer interaction and can translate complex system behaviour into actionable deployment steps, and troubleshooting paths. If you are motivated by improving how users execute cyber operations rather than simply maintaining systems. If you enjoy solving problems that emerge in dynamic environments - whether that involves diagnosing connectivity failures, understanding permissions and access flows, or adapting to new tools coming online. &lt;&lt;br&gt;&gt;&lt;&lt;br&gt;&gt;If you are comfortable being the technical point person for field operators, mission teams, and leadership, and you naturally earn trust by being responsive, competent, and composed. If your passion is for enabling teams, not just implementing technology, but ensuring people understand it and can rely on it with confidence, then this role is for you. &lt;&lt;br&gt;&gt;Active TS or TS/SCI or obtainable clearance, or experience working in classified settings, is required for this role.&lt;&lt;p style=\\"text-align:inherit\\"&gt;&gt;&lt;&lt;/p&gt;&gt;&lt;&lt;p style=\\"text-align:left\\"&gt;&gt;&lt;&lt;b&gt;&gt;&lt;&lt;b&gt;&gt;AI Innovation at Dataminr&lt;&lt;/b&gt;&gt;&lt;&lt;/b&gt;&gt;&lt;&lt;/p&gt;&gt;&lt;&lt;p style=\\"text-align:left\\"&gt;&gt;&lt;&lt;span&gt;&gt;Working at Dataminr you’ll have the opportunity to tackle the most exciting trends in AI on a daily basis to power a revolutionary product that uncovers critical events around the world as they unfold.&lt;&lt;/span&gt;&gt;&lt;&lt;/p&gt;&gt;&lt;&lt;p style=\\"text-align:left\\"&gt;&gt;&lt;&lt;b&gt;&gt;&lt;&lt;a href=\\"https://www.dataminr.com/press/dataminr-unveils-regenai-the-first-generative-ai-that-automatically-regenerates-in-real-time/\\" target=\\"\_blank\\"&gt;&gt;&lt;&lt;u&gt;&gt;Regenerative AI&lt;&lt;/u&gt;&gt;&lt;&lt;/a&gt;&gt;:&lt;&lt;/b&gt;&gt;&lt;&lt;span&gt;&gt; our AI technology, ReGenAI, is a new form of generative AI that automatically regenerates real-time Live Event Briefs as events unfold. Learn more &lt;&lt;/span&gt;&gt;&lt;&lt;a href=\\"https://www.dataminr.com/press/dataminr-ceo-discusses-future-of-real-time-information-at-imagination-in-action\\" target=\\"\_blank\\"&gt;&gt;&lt;&lt;u&gt;&gt;here&lt;&lt;/u&gt;&gt;&lt;&lt;/a&gt;&gt;&lt;&lt;span&gt;&gt;.&lt;&lt;/span&gt;&

... (truncated, 14874 total chars)
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
```
{"jobPostingInfo":{"id":"b4a4c1a1722b1001f487f85a3c880000","title":"Field Services Engineer, USAF (Onsite in San Antonio, TX)","jobDescription":"&lt;&lt;p style=\\"text-align:inherit\\"&gt;&gt;&lt;&lt;/p&gt;&gt;&lt;&lt;p style=\\"text-align:left\\"&gt;&gt;&lt;&lt;b&gt;&gt;See yourself at Dataminr&lt;&lt;/b&gt;&gt;&lt;&lt;/p&gt;&gt;We are seeking a hands-on, customer-facing facing and mission-focused Field Services Engineer to support the deployment and growth of a large US Air Force system. This is an on-site role in San Antonio, TX. &lt;&lt;br&gt;&gt;&lt;&lt;br&gt;&gt;This role is ideal for a hands-on keyboard engineer who thrives in operational environments where real-world missions rely on your work. If you enjoy direct customer interaction and can translate complex system behaviour into actionable deployment steps, and troubleshooting paths. If you are motivated by improving how users execute cyber operations rather than simply maintaining systems. If you enjoy solving problems that emerge in dynamic environments - whether that involves diagnosing connectivity failures, understanding permissions and access flows, or adapting to new tools coming online. &lt;&lt;br&gt;&gt;&lt;&lt;br&gt;&gt;If you are comfortable being the technical point person for field operators, mission teams, and leadership, and you naturally earn trust by being responsive, competent, and composed. If your passion is for enabling teams, not just implementing technology, but ensuring people understand it and can rely on it with confidence, then this role is for you. &lt;&lt;br&gt;&gt;Active TS or TS/SCI or obtainable clearance, or experience working in classified settings, is required for this role.&lt;&lt;p style=\\"text-align:inherit\\"&gt;&gt;&lt;&lt;/p&gt;&gt;&lt;&lt;p style=\\"text-align:left\\"&gt;&gt;&lt;&lt;b&gt;&gt;&lt;&lt;b&gt;&gt;AI Innovation at Dataminr&lt;&lt;/b&gt;&gt;&lt;&lt;/b&gt;&gt;&lt;&lt;/p&gt;&gt;&lt;&lt;p style=\\"text-align:left\\"&gt;&gt;&lt;&lt;span&gt;&gt;Working 

... (truncated, 14874 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: workday

```json
{
  "url": "https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/San-Antonio-TX/Field-Services-Engineer--USAF--Onsite-in-San-Antonio--TX-_JR1954",
  "handler": "workday"
}
```

### Raw Content Capture

Captured 14874 chars of commonmark content

```json
{
  "length": 14874,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 14874 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 14874
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/San-Antonio-TX/Field-Services-Engineer--USAF--Onsite-in-San-Antonio--TX-_JR1954",
      "sourceUrl": "https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/San-Antonio-TX/Field-Services-Engineer--USAF--Onsite-in-San-Antonio--TX-_JR1954",
      "provider": "spidercloud",
      "siteId": "workday",
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
    "winner": "raw_row_title",
    "value": "Field Services Engineer, USAF (Onsite in San Antonio, TX)"
  },
  "company": {
    "winner": "site_handler_company",
    "value": "Dataminr"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "San Antonio, TX"
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
    "value": 118950
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 3
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-09T08:00:00"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "See yourself at Dataminr We are seeking a hands-on, customer-facing facing and mission-focused Field Services Engineer to support the deployment and growth of a large US Air Force system. This is an on-site role in San Antonio, TX. This role is ideal for a hands-on keyboard engineer who thrives in operational environments where real-world missions rely on your work. If you enjoy direct customer interaction and can translate complex system behaviour into actionable deployment steps, and troubleshooting paths. If you are motivated by improving how users execute cyber operations rather than simply maintaining systems. If you enjoy solving pr
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Field Services Engineer, USAF (Onsite in San Antonio, TX)`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599248806,
  "heuristicVersion": 5,
  "locations": [
    "San Antonio, TX"
  ],
  "location": "San Antonio, TX",
  "locationStates": [
    "TX"
  ],
  "locationSearch": "TX San Antonio",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "totalCompensation": 118950,
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
| title | `raw_row_title` | `Field Services Engineer, USAF (Onsite in San Anton` |
| company | `site_handler_company` | `Dataminr` |
| location | `raw_row_location` | `San Antonio, TX` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `default_level` | `mid` |
| compensation | `hinted_compensation` | `118950` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `3` |
| posted_at | `explicit_posted_at_field` | `2026-01-09 08:00:00` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `See yourself at Dataminr We are seeking a hands-on` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Field Services Engineer, USAF (Onsite in San Antonio, TX)`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'workday' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Field Services Engineer, USAF ` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ❌ | `` | No title in hints |
| first_line_title | FALLBACK | ❌ | `` | No valid title found in first lines |

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

**Final Value:** `San Antonio, TX`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'workday' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `San Antonio, TX` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ✅ | `San Antonio Tx` | Extracted from URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `San Antonio, TX` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `San Antonio, TX` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'San Antonio, TX' present but not inferri |
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

**Final Value:** `118950`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `118950` | Compensation from hint range: $96,400-$141,500 ->  |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `118950` | Compensation range pattern: $96,400-$141,500 -> $1 |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

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

**Final Value:** `2026-01-09 08:00:00`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-09 08:00:00` | Valid date: 2026-01-09T08:00:00 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'workday' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:34:08.821302` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'workday' returned no first_published |

#### DESCRIPTION

**Final Value:** `See yourself at Dataminr We are seeking a hands-on, customer-facing facing and mission-focused Field Services Engineer to support the deployment and growth of a large US Air Force system. This is an on-site role in San Antonio, TX. This role is ideal for a hands-on keyboard engineer who thrives in operational environments where real-world missions rely on your work. If you enjoy direct customer interaction and can translate complex system behaviour into actionable deployment steps, and troubleshooting paths. If you are motivated by improving how users execute cyber operations rather than simply maintaining systems. If you enjoy solving problems that emerge in dynamic environments - whether that involves diagnosing connectivity failures, understanding permissions and access flows, or adapting to new tools coming online. If you are comfortable being the technical point person for field operators, mission teams, and leadership, and you naturally earn trust by being responsive, competent, and composed. If your passion is for enabling teams, not just implementing technology, but ensuring people understand it and can rely on it with confidence, then this role is for you. Active TS or TS/SCI or obtainable clearance, or experience working in classified settings, is required for this role. AI Innovation at Dataminr Working at Dataminr you’ll have the opportunity to tackle the most exciting trends in AI on a daily basis to power a revolutionary product that uncovers critical events around the world as they unfold. Regenerative AI : our AI technology, ReGenAI, is a new form of generative AI that automatically regenerates real-time Live Event Briefs as events unfold. Learn more here . Agentic AI : we recently launched our Agentic AI capability, what we’re calling our Intel Agents, that autonomously generates critical context for our clients on real-time events, threats, and risks allowing them to see the clearest, most accurate view of what’s happening on the ground. Learn more here Multimodal AI: our platform detects events from many different types of data (images, video, sensor data, audio, and text in over 150 languages). Learn more here . The opportunity Deploy and configure USAF cyber kits across multiple hardware platforms Troubleshoot live issues under operational conditions Support threat hunting teams, cyber protection teams, and mission operators directly Train analysts, operators, and engineers on best-use practices for Polarity and related tools Serve as the “train-the-trainer” lead, building repeatable onboarding and instructional content Work with platforms such as Docker, Kubernetes, Linux, secure enclaves, and classified infrastructures Contribute insights that shape future features, SOP documentation, and operational workflows Support expansion into multiple theaters and operational units as the deployment footprint scales to 550&#43; kits What you bring At Dataminr, we value you for who you are. We encourage you to apply for this role, even if you don&#39;t meet every qualification. Our candidates are reviewed on the basis of their skill and potential to succeed. Hands-on Linux administration experience Strong troubleshooting and research ability in complex environments, knowledge of computer networking, routing, and connectivity workflows Experience with Docker and/or Kubernetes deployment Experience working with mission-aligned operators or cyber organizations. Ability to teach, onboard, and guide users of varying technical experience Experience with cyber hunt teams, IR teams, or network defense operations. Familiarity with secure enclave operations (NIPR/JWICS/SIPR or equivalent). Exposure to automation or scripting for configuration scaling Experience authoring SOPs or structured training documentation Ability and willingness to travel (~30%) LI-JF About Dataminr At Dataminr, we are a mission driven team of talented builders, creators and visionaries who have real-world impact on how organizations are able to respond to events. Dataminr’s groundbreaking, AI-powered, intelligence platform provides organizations with the earliest signals of emerging risks, events, and threats before they unfold. Trusted by two-thirds of the Fortune 50 and half of the Fortune 100, Dataminr’s platform analyzes billions of public data inputs spanning text, image, video, audio and sensor data across 150&#43; languages, empowering our clients to stay one step ahead in an increasingly complex world where every second counts. Founded in 2009, we have pioneered the world’s first real-time event detection platform, long before the recent Gen AI ‘boom.’ Dataminr operates all around the world united by our passion to use AI for the greater good, be agents of positive change and put our technology into the hands of clients charged with the responsibility to keep organizations running and keep people safe. As our employees focus on developing our revolutionary technology, we focus on our employees. Dataminr is proud to offer a variety of flexible work arrangements, offices all over the world to foster collaboration, generous PTO and sick leave, and more, as part of our competitive benefits package aimed at keeping all our employees happy and healthy. Explore all our benefits here . We believe our differences give us strength. Our employees are empowered to be their best, authentic selves through various opportunities, such as our robust employee resource group (ERG) network, manager development programming, professional development funds, and more. We serve a global community made up of many cultures and strive to reflect the world and clients we serve, with a workforce built on merit and equity. We actively condemn racism and discrimination in any form. We stand for social good, fostering a culture of allyship, and standing up for those who face systemic barriers to equality. We lead with empathy and strive to be agents of positive change in our company and in our communities. The annual base salary range for this position is $96,400 - $141,500. You will also be eligible to receive a discretionary bonus and Company equity. Actual salary will be based on a number of factors including, but not limited to, geographic location, applicant skills, and prior relevant experience. Dataminr is an equal opportunity and affirmative action employer. Individuals seeking employment at Dataminr are considered without regards to race, sex, color, creed, religion, national origin, age, disability, genetics, marital status, pregnancy, unemployment status, sexual orientation, citizenship status or veteran status. Dataminr will collect and process your personal data. All personal data will be processed in accordance with applicable data protection laws. Please see Dataminr&#39;s candidate privacy notice available here . By providing your details and applying via our careers website, you acknowledge that you have read our candidate privacy notice. If you have any queries, please contact the People Team at hr&#64;dataminr.com or privacy&#64;dataminr.com .`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `See yourself at Dataminr We ar` | Valid description (7047 chars, 1049 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `See yourself at Dataminr We ar` | Valid description (7047 chars, 1049 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `See yourself at Dataminr We ar` | Valid description (7047 chars, 1049 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Field Services Engineer, USAF (Onsite in San Antonio, TX)` |
| Company | `Dataminr` |
| Location | `San Antonio, TX` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1767970800000` |
| Description Words | `1049` |
| Cost (milli-cents) | `3` |
| URL | `https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/San-Antonio-TX/Field-Services-Engineer--USAF--Onsite-in-San-Antonio--TX-_JR1954` |

**Description Preview (first 200 words):**

```
See yourself at Dataminr We are seeking a hands-on, customer-facing facing and mission-focused Field Services Engineer to support the deployment and growth of a large US Air Force system. This is an on-site role in San Antonio, TX. This role is ideal for a hands-on keyboard engineer who thrives in operational environments where real-world missions rely on your work. If you enjoy direct customer interaction and can translate complex system behaviour into actionable deployment steps, and troubleshooting paths. If you are motivated by improving how users execute cyber operations rather than simply maintaining systems. If you enjoy solving problems that emerge in dynamic environments - whether that involves diagnosing connectivity failures, understanding permissions and access flows, or adapting to new tools coming online. If you are comfortable being the technical point person for field operators, mission teams, and leadership, and you naturally earn trust by being responsive, competent, and composed. If your passion is for enabling teams, not just implementing technology, but ensuring people understand it and can rely on it with confidence, then this role is for you. Active TS or TS/SCI or obtainable clearance, or experience working in classified settings, is required for this role. AI Innovation at...
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
  "title": "Field Services Engineer, USAF (Onsite in San Antonio, TX)",
  "company": "Dataminr",
  "location": "San Antonio, TX",
  "description": "See yourself at Dataminr We are seeking a hands-on, customer-facing facing and mission-focused Field Services Engineer to support the deployment and growth of a large US Air Force system. This is an on-site role in San Antonio, TX. This role is ideal for a hands-on keyboard engineer who thrives in operational environments where real-world missions rely on your work. If you enjoy direct customer interaction and can translate complex system behaviour into actionable deployment steps, and troublesh...",
  "url": "https://dataminr.wd12.myworkdayjobs.com/wday/cxs/dataminr/Dataminr/job/San-Antonio-TX/Field-Services-Engineer--USAF--Onsite-in-San-Antonio--TX-_JR1954",
  "posted_at": 1767970800000,
  "level": "mid",
  "remote": false,
  "cost_milli_cents": 3,
  "_full_description_word_count": 1049
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
