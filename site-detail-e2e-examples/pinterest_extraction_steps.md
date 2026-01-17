# Extraction Steps: pinterest

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/pinterest/jobs/7444414`
**Source URL:** `https://api.greenhouse.io/v1/boards/pinterest/jobs`
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
  "url": "https://boards-api.greenhouse.io/v1/boards/pinterest/jobs/7444414",
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
      "url": "https://boards-api.greenhouse.io/v1/boards/pinterest/jobs/7444414",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/pinterest/jobs",
      "provider": "spidercloud",
      "siteId": "pinterest",
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
    "value": "Business Development Lead, Data Partnerships"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Pinterest"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "San Francisco, CA, US"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": true
  },
  "level": {
    "winner": "explicit_level_field",
    "value": "staff"
  },
  "compensation": {
    "winner": "hinted_compensation",
    "value": 202337
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 2
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2025-12-11T17:05:42"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Business Development Lead, Data Partnerships\n\nAbout Pinterest:\nMillions of people around the world come to our platform to find creative ideas, dream about new possibilities and plan for memories that will last a lifetime. At Pinterest, we’re on a mission to bring everyone the inspiration to create a life they love, and that starts with the people behind the product.\nDiscover a career where you ignite innovation for millions, transform passion into growth opportunities, celebrate each other’s unique experiences and embrace the&nbsp; flexibility to do your best work. Creating a career you love? It’s Possible.\nThe Business Development tea
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Business Development Lead, Data Partnerships`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599198860,
  "heuristicVersion": 5,
  "locations": [
    "San Francisco, CA"
  ],
  "location": "San Francisco, CA",
  "locationStates": [
    "CA"
  ],
  "locationSearch": "San Francisco CA",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "remote": true,
  "totalCompensation": 202337,
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
| title | `raw_row_title` | `Business Development Lead, Data Partnerships` |
| company | `raw_row_company` | `Pinterest` |
| location | `raw_row_location` | `San Francisco, CA, US` |
| remote | `explicit_remote_flag` | `True` |
| level | `explicit_level_field` | `staff` |
| compensation | `hinted_compensation` | `202337` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `2` |
| posted_at | `explicit_posted_at_field` | `2025-12-11 17:05:42` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Business Development Lead, Data Partnerships

Abou` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Business Development Lead, Data Partnerships`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Business Development Lead, Dat` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ✅ | `Business Development Lead, Dat` | Valid title |
| first_line_title | FALLBACK | ✅ | `Business Development Lead, Dat` | Valid title |

#### COMPANY

**Final Value:** `Pinterest`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Pinterest` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Pinterest` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `Pinterest` | Found 'Company is a' pattern |
| hinted_company | HEURISTIC | ✅ | `Pinterest` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `San Francisco, CA, US`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `San Francisco, CA, US` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `San Francisco, CA, US; Remote,` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ❌ | `` | No location in hints |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `True`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `True` | Explicit boolean remote=True |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'San Francisco, CA, US' present but not i |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | No remote in hints |
| remote_company | CUSTOM_650 | ✅ | `True` | Company 'Pinterest' is known remote-first |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `staff`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `staff` | Explicit level field: staff -> staff |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `staff` | Level from title: 'lead' -> staff |
| content_pattern_level | CUSTOM_550 | ✅ | `senior` | Level from experience: 6+ years -> senior |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `202337`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `202337` | Compensation from hint range: $132,298-$272,377 -> |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `132298` | Single compensation pattern: $132,298 |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `2`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `2` | Valid cost: 2 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2025-12-11 17:05:42`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-12-11 17:05:42` | Valid date: 2025-12-11T17:05:42 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:18.875024` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no first_published |

#### DESCRIPTION

**Final Value:** `Business Development Lead, Data Partnerships

About Pinterest:
Millions of people around the world come to our platform to find creative ideas, dream about new possibilities and plan for memories that will last a lifetime. At Pinterest, we’re on a mission to bring everyone the inspiration to create a life they love, and that starts with the people behind the product.
Discover a career where you ignite innovation for millions, transform passion into growth opportunities, celebrate each other’s unique experiences and embrace the&nbsp; flexibility to do your best work. Creating a career you love? It’s Possible.
The Business Development team at Pinterest is responsible for developing partner strategy, as well as sourcing, evaluating, executing and managing partnerships that are a critical lever for growth and progress of the company against key strategic initiatives. The team works closely with senior leadership, product, marketing, legal and revenue teams to deliver these partnerships.&nbsp;
&nbsp;
The Business Development Lead for Data Partnerships will own and grow a portfolio of privacy-centric data partnerships (e.g., clean rooms, identity, and targeting partners) that enable Pinterest and its advertisers to activate, measure, and optimize performance in a privacy first world.&nbsp; This position will report to the Director of BD for Measurement and Data.&nbsp; The role will be hybrid with some time in the office and remote.&nbsp;&nbsp;
&nbsp;
What you’ll do:
- Develop the partner strategy for privacy centric data partnerships at Pinterest, including helping the company understand the industry landscape to identify opportunities, trends and risks that are relevant to Pinterest.&nbsp;
- Lead the process of identifying partnerships to pursue, including building the business case through strategic/financial analysis and due diligence across relevant teams including product, engineering, legal, marketing and sales&nbsp;
- Negotiate deals and develop partnerships by working across relevant teams including product, engineering, legal, marketing and sales&nbsp;
- Manage the portfolio of partnerships and drive growth from initial launch through scaled adoption, ensuring partners are integrated effectively, supported by strong joint value propositions, and embedded into how advertisers measure and optimize on Pinterest.
&nbsp;
What we’re looking for:
- You have a Bachelor’s Degree or equivalent practical experience and 6+ years of work experience in business development, ideally with demonstrated experience in online advertising partnerships focused on privacy enhancing technology, identity, and targeting.&nbsp;&nbsp;
- You are strategic, driven, and analytical with a passion for the online advertising industry and technology
- You are an expert in partner and program management and know how to manage and optimize complex partnerships&nbsp;
- You are experienced in performing broad quantitative analysis and negotiating complex deals
- You have experience and knowledge of the online advertising industry and partner ecosystem&nbsp;&nbsp;
- You have the ability to work with, and effectively influence, cross-functional senior executives in a rapidly changing environment&nbsp;
&nbsp;
Relocation Statement:
- &nbsp;This position is not eligible for relocation assistance. Visit our PinFlex page to learn more about our working model.
&nbsp;
In-Office Requirement Statement:
- We let the type of work you do guide the collaboration style. That means we're not always working in an office, but we continue to gather for key moments of collaboration and connection.
- This role will need to be in the office for in-person collaboration 1-2 times/quarter and therefore can be situated anywhere in the country.
#LI-REMOTE&nbsp;
#LI-EP4
At Pinterest we believe the workplace should be equitable, inclusive, and inspiring for every employee. In an effort to provide greater transparency, we are sharing the base salary range for this position. The position is also eligible for equity. Final salary is based on a number of factors including location, travel, relevant prior experience, or particular skills and expertise.
Information regarding the culture at Pinterest and benefits available for this position can be found here .
US based applicants only $132,298 &mdash; $272,377 USD Our Commitment to Inclusion:
Pinterest is an equal opportunity employer and makes employment decisions on the basis of merit. We want to have the best qualified people in every job. All qualified applicants will receive consideration for employment without regard to race, color, ancestry, national origin, religion or religious creed, sex (including pregnancy, childbirth, or related medical conditions), sexual orientation, gender, gender identity, gender expression, age, marital status, status as a protected veteran, physical or mental disability, medical condition, genetic information or characteristics (or those of a family member) or any other consideration made unlawful by applicable federal, state or local laws. We also consider qualified applicants regardless of criminal histories, consistent with legal requirements. If you require a medical or religious accommodation during the job application process, please complete&nbsp; this form &nbsp;for support.
&nbsp;`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Business Development Lead, Dat` | Valid description (5302 chars, 764 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Business Development Lead, Dat` | Valid description (5302 chars, 764 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Business Development Lead, Dat` | Valid description (5302 chars, 764 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Business Development Lead, Data Partnerships` |
| Company | `Pinterest` |
| Location | `San Francisco, CA` |
| Is Remote | `True` |
| Level | `staff` |
| Posted At | `1765497942000` |
| Description Words | `764` |
| Cost (milli-cents) | `2` |
| URL | `https://boards-api.greenhouse.io/v1/boards/pinterest/jobs/7444414` |

**Description Preview (first 200 words):**

```
Business Development Lead, Data Partnerships About Pinterest: Millions of people around the world come to our platform to find creative ideas, dream about new possibilities and plan for memories that will last a lifetime. At Pinterest, we’re on a mission to bring everyone the inspiration to create a life they love, and that starts with the people behind the product. Discover a career where you ignite innovation for millions, transform passion into growth opportunities, celebrate each other’s unique experiences and embrace the&nbsp; flexibility to do your best work. Creating a career you love? It’s Possible. The Business Development team at Pinterest is responsible for developing partner strategy, as well as sourcing, evaluating, executing and managing partnerships that are a critical lever for growth and progress of the company against key strategic initiatives. The team works closely with senior leadership, product, marketing, legal and revenue teams to deliver these partnerships.&nbsp; &nbsp; The Business Development Lead for Data Partnerships will own and grow a portfolio of privacy-centric data partnerships (e.g., clean rooms, identity, and targeting partners) that enable Pinterest and its advertisers to activate, measure, and optimize performance in a privacy first world.&nbsp; This position will report to the Director of BD...
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
  "title": "Business Development Lead, Data Partnerships",
  "company": "Pinterest",
  "location": "San Francisco, CA, US; Remote, US",
  "description": "Business Development Lead, Data Partnerships\n\nAbout Pinterest:\nMillions of people around the world come to our platform to find creative ideas, dream about new possibilities and plan for memories that will last a lifetime. At Pinterest, we’re on a mission to bring everyone the inspiration to create a life they love, and that starts with the people behind the product.\nDiscover a career where you ignite innovation for millions, transform passion into growth opportunities, celebrate each other’s un...",
  "url": "https://boards-api.greenhouse.io/v1/boards/pinterest/jobs/7444414",
  "apply_url": "https://boards.greenhouse.io/pinterest/jobs/7444414",
  "posted_at": 1765497942000,
  "level": "staff",
  "remote": true,
  "cost_milli_cents": 2,
  "_full_description_word_count": 764
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
