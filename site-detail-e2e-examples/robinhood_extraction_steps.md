# Extraction Steps: robinhood

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/robinhood/jobs/5702135`
**Source URL:** `https://api.greenhouse.io/v1/boards/robinhood/jobs`
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
  "url": "https://boards-api.greenhouse.io/v1/boards/robinhood/jobs/5702135",
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
      "url": "https://boards-api.greenhouse.io/v1/boards/robinhood/jobs/5702135",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/robinhood/jobs",
      "provider": "spidercloud",
      "siteId": "robinhood",
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
    "value": "Android Developer"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Robinhood"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Toronto, Canada"
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
    "value": 129500
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 3
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-05T15:04:03"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Android Developer\n\nJoin us in building the future of finance.\nOur mission is to democratize finance for all. An estimated $124 trillion of assets will be inherited by younger generations in the next two decades. The largest transfer of wealth in human history. If you’re ready to be at the epicenter of this historic cultural and financial shift, keep reading.\nAbout the team + role\nWe’re excited to invite talented and motivated Android Developers to join our award-winning team to help expand Robinhood’s mobile experience for our customers.\nWe’re looking for passionate and skilled developers who lead by example, building up the product and people around them through hard work, m
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Android Developer`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599175289,
  "heuristicVersion": 5,
  "locations": [
    "Toronto, Canada"
  ],
  "location": "Toronto, Canada",
  "locationStates": [
    "Canada"
  ],
  "locationSearch": "Canada Toronto",
  "countries": [
    "Canada"
  ],
  "country": "Canada",
  "totalCompensation": 129500,
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
| title | `raw_row_title` | `Android Developer` |
| company | `raw_row_company` | `Robinhood` |
| location | `raw_row_location` | `Toronto, Canada` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `default_level` | `mid` |
| compensation | `hinted_compensation` | `129500` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `3` |
| posted_at | `explicit_posted_at_field` | `2026-01-05 15:04:03` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Android Developer

Join us in building the future ` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Android Developer`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Android Developer` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ✅ | `Android Developer` | Valid title |
| first_line_title | FALLBACK | ✅ | `Android Developer` | Valid title |

#### COMPANY

**Final Value:** `Robinhood`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Robinhood` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Robinhood` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `us in building the future of f` | Found 'Join Company' pattern |
| hinted_company | HEURISTIC | ✅ | `the team + role` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Toronto, Canada`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Toronto, Canada` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Toronto, Canada` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `Toronto, Canada` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Toronto, Canada' present but not inferri |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Robinhood' not in remote company list |
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

**Final Value:** `129500`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `129500` | Compensation from hint range: $119,000-$140,000 -> |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `124000` | Single compensation pattern: $124,000 |
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

**Final Value:** `2026-01-05 15:04:03`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-05 15:04:03` | Valid date: 2026-01-05T15:04:03 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:32:55.333410` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no first_published |

#### DESCRIPTION

**Final Value:** `Android Developer

Join us in building the future of finance.
Our mission is to democratize finance for all. An estimated $124 trillion of assets will be inherited by younger generations in the next two decades. The largest transfer of wealth in human history. If you’re ready to be at the epicenter of this historic cultural and financial shift, keep reading.
About the team + role
We’re excited to invite talented and motivated Android Developers to join our award-winning team to help expand Robinhood’s mobile experience for our customers.
We’re looking for passionate and skilled developers who lead by example, building up the product and people around them through hard work, mentorship, and proven best practices.
Learn more about some of our available roles below!
Support Products: The Support Products team owns the end to end customer facing support experience including chat and phone channel, user triage experience, support entry point, help center and more.
Crypto: As a member of our Crypto team you will help ship Robinhood's new standalone wallet app, while also improving the crypto experience within Robinhhood's flagship application. Learn more what the crypto team is up to here
Activation: Sherwood is focused on creating the best in class news experience in the RH trading app. As a member of the Sherwood team you will&nbsp; help deliver actionable, accessible financial news to retail traders at any point in their investing journey.
What you’ll do
- Work with a fast-growing team to revolutionize finance at speed and scale
- Build smooth, stable, and elegant products with Kotlin and the newest Android APIs
- Own features from inception to design, implementation, and launch
- Work cross-functionally with Product Designers, Product Managers, Backend Developers, and Data team
- Work with Kotlin, Android SDK &amp; Jetpack Compose, MVVM, Coroutines &amp; Flow (preferred) OR RxJava, and Retrofit &amp; Room
What you bring
- 1.5+ years of professional Android development experience building consumer Android applications at scale, preferably in Kotlin and Compose
- Attention to detail, passion for writing clean, readable and&nbsp; maintainable code
- Proficiency in Kotlin or Java, and Jetpack
- Motivation to improve Robinhood’s app and codebase to ensure the highest quality for our customers
- Strong product sense and ability to collaborate with Product Managers and Designers
- Excellent communication skills and passion for solving technical challenges
- Bachelor’s degree in Computer Science or a related field preferred; equivalent training or work experience also acceptable
&nbsp;
What we offer
- Market competitive and pay equity-focused compensation structure
- 100% paid health insurance for employees with 90% coverage for dependents
- Annual lifestyle wallet for personal wellness, learning and development, and more!
- Lifetime maximum benefit for family forming and fertility benefits
- Dedicated mental health support for employees and eligible dependents
- Generous time away including company holidays, paid time off, sick time, parental leave, and more!
- Lively office environment with catered meals, fully stocked kitchens, and geo-specific commuter benefits
Our team is committed to providing an inclusive and welcoming interview experience for all candidates. If you require a specific accommodation during the application or interview process due to a physical or mental condition, please complete this&nbsp; Applicant Accommodation Form to notify our team. The form should only be completed if you need a specific accommodation.
AI Usage Disclosure: Robinhood uses artificial intelligence (AI) tools to support parts of our recruiting process. These tools enhance the efficiency and consistency of our hiring process; however, all hiring decisions are made by our hiring teams.
Vacancy Notice: This job posting represents an existing vacancy that we are actively seeking to fill.
In addition to the base pay range listed below, this role is also eligible for bonus opportunities + equity + benefits.
Base pay for the successful applicant will depend on a variety of job-related factors, which may include education, training, experience, location, business needs, or market demands. The expected base pay range for this role is based on the location where the work will be performed.
Base Pay Range:
Toronto, ON $119,000 &mdash; $140,000 CAD Click here to learn more about our Total Rewards, which vary by region and entity.
If our mission energizes you and you’re ready to build the future of finance, we look forward to seeing your application.
Robinhood provides equal opportunity for all applicants, offers reasonable accommodations upon request, and complies with applicable equal employment and privacy laws. Inclusion is built into how we hire and work—welcoming different backgrounds, perspectives, and experiences so everyone can do their best. Please review the&nbsp; Privacy Policy for your country of application.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Android Developer

Join us in ` | Valid description (4984 chars, 762 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Android Developer

Join us in ` | Valid description (4984 chars, 762 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Android Developer

Join us in ` | Valid description (4984 chars, 762 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Android Developer` |
| Company | `Robinhood` |
| Location | `Toronto, Canada` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1767650643000` |
| Description Words | `762` |
| Cost (milli-cents) | `3` |
| URL | `https://boards-api.greenhouse.io/v1/boards/robinhood/jobs/5702135` |

**Description Preview (first 200 words):**

```
Android Developer Join us in building the future of finance. Our mission is to democratize finance for all. An estimated $124 trillion of assets will be inherited by younger generations in the next two decades. The largest transfer of wealth in human history. If you’re ready to be at the epicenter of this historic cultural and financial shift, keep reading. About the team + role We’re excited to invite talented and motivated Android Developers to join our award-winning team to help expand Robinhood’s mobile experience for our customers. We’re looking for passionate and skilled developers who lead by example, building up the product and people around them through hard work, mentorship, and proven best practices. Learn more about some of our available roles below! Support Products: The Support Products team owns the end to end customer facing support experience including chat and phone channel, user triage experience, support entry point, help center and more. Crypto: As a member of our Crypto team you will help ship Robinhood's new standalone wallet app, while also improving the crypto experience within Robinhhood's flagship application. Learn more what the crypto team is up to here Activation: Sherwood is focused on creating the best in...
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
  "title": "Android Developer",
  "company": "Robinhood",
  "location": "Toronto, Canada",
  "description": "Android Developer\n\nJoin us in building the future of finance.\nOur mission is to democratize finance for all. An estimated $124 trillion of assets will be inherited by younger generations in the next two decades. The largest transfer of wealth in human history. If you’re ready to be at the epicenter of this historic cultural and financial shift, keep reading.\nAbout the team + role\nWe’re excited to invite talented and motivated Android Developers to join our award-winning team to help expand Robin...",
  "url": "https://boards-api.greenhouse.io/v1/boards/robinhood/jobs/5702135",
  "apply_url": "https://boards.greenhouse.io/robinhood/jobs/5702135",
  "posted_at": 1767650643000,
  "level": "mid",
  "remote": false,
  "cost_milli_cents": 3,
  "_full_description_word_count": 762
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
