# Extraction Steps: uber

**Detail URL:** `https://www.uber.com/careers/list/149889`
**Source URL:** `https://www.uber.com/careers/list/149889`
**Handler:** `uber_careers`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Business Development Lead, US Regional Grocery - Chicago, Illinois | Uber Careers
[Skip to main content](#main)
[](https://www.uber.com/careers/list?query=)
# Business Development Lead, US Regional Grocery
Sales, Sales &amp; Account Management
Los Angeles, California |
New York, New York |
Seattle, Washington |
Miami, Florida |
Chicago, Illinois |
San Francisco, California
[Apply Now](https://www.uber.com/careers/apply/interstitial/149889)
**About the Role**
As a Business Development Lead, you will be responsible for engaging with senior-level executives, the negotiation process, and ultimately signing key strategic regional grocery brands in the United States.
Who are we looking for? Someone who has proven they can build best-in-class partnerships with large brands, and thrives in a fast-paced, cross-functional environment. Your primary focus will be to form new partnerships, including account outreach, pitching, negotiation, and onboarding
**What You’ll Do**
* Run an end-to-end sales process: sales discovery and outreach, pitching, negotiation, and onboarding
* Engage with potential partners: From C-suite to individual contributors, while using data to tell a compelling story
* Use internal and external data to understand potential and influence outcomes
* continuously improve our sales capabilities as an organization
**Basic Qualifications**
* **Experience:** 4+ years of experience in Enterprise sales, consulting and/or partnerships with large, complex enterprise brands and organizations
* **Data Driven:** Ability to tell a data-driven story and work across large sets of data
* **Collaborative leadership:** Passion for digging in tactically, leveraging analytical skills where needed, and equally as comfortable leading a conversation with an Engineer, Product Manager, or C-level executive to secure buy-in
* **Self-Sufficient:** Speed, resourcefulness, and a go-getter mentality. You are comfortable working in a fast-paced environment and navigating ambiguity
* **Business Accument:** understand both strategic and technical aspects of a deal from the point of view of Uber and the Merchant
* **Project Management:** Comfortable concurrently managing many opportunities on short time horizons
**Preferred Qualifications**
* Experience with food delivery, retail and/or third-party marketplaces
* Financial modeling
* SQL
* Salesforce
For Chicago, IL-based roles: The base salary range for this role is USD$131,000 per year - USD$145,000 per year.
For Los Angeles, CA-based roles: The base salary range for this role is USD$131,000 per year - USD$145,000 per year.
For Miami, FL-based roles: The base salary range for this role is USD$131,000 per year - USD$145,000 per year.
For New York, NY-based roles: The base salary range for this role is USD$145,000 per year - USD$161,000 per year.
For San Francisco, CA-based roles: The base salary range for this role is USD$145,000 per year - USD$161,000 per year.
For Seattle, WA-based roles: The base salary range for thi

... (truncated, 28779 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `uber_careers`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
Business Development Lead, US Regional Grocery - Chicago, Illinois | Uber Careers
[Skip to main content](#main)
[](https://www.uber.com/careers/list?query=)
# Business Development Lead, US Regional Grocery
Sales, Sales &amp; Account Management
Los Angeles, California |
New York, New York |
Seattle, Washington |
Miami, Florida |
Chicago, Illinois |
San Francisco, California
[Apply Now](https://www.uber.com/careers/apply/interstitial/149889)
**About the Role**
As a Business Development Lead, you will be responsible for engaging with senior-level executives, the negotiation process, and ultimately signing key strategic regional grocery brands in the United States.
Who are we looking for? Someone who has proven they can build best-in-class partnerships with large brands, and thrives in a fast-paced, cross-functional environment. Your primary focus will be to form new partnerships, including account outreach, pitching, negotiation, and onboarding
**What You’ll Do**
* Run an end-to-end sales process: sales discovery and outreach, pitching, negotiation, and onboarding
* Engage with potential partners: From C-suite to individual contributors, while using data to tell a compelling story
* Use internal and external data to understand potential and influence outcomes
* continuously improve our sales capabilities as an organization
**Basic Qualifications**
* **Experience:** 4+ years of experience in Enterprise sales, consulting and/or partnerships with large, complex enterprise brands and organizations
* **Data Driven:** Ability to tell a data-driven story and work across large sets of data
* **Collaborative leadership:** Passion for digging in tactically, leveraging analytical skills where needed, and equally as comfortable leading a conversation with an Engineer, Product Manager, or C-level executive to secure buy-in
* **Self-Sufficient:** Speed, resourcefulness, and a go-getter mentality. You are comfortable working in a fast-paced environment and navigating ambiguity
* **Bu

... (truncated, 28779 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: uber_careers

```json
{
  "url": "https://www.uber.com/careers/list/149889",
  "handler": "uber_careers"
}
```

### Raw Content Capture

Captured 28779 chars of commonmark content

```json
{
  "length": 28779,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 28779 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 28779
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://www.uber.com/careers/list/149889",
      "sourceUrl": "https://www.uber.com/careers/list/149889",
      "provider": "spidercloud",
      "siteId": "uber",
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
    "value": "Business Development Lead, US Regional Grocery"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Uber"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "San Francisco, CA"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": false
  },
  "level": {
    "winner": "explicit_level_field",
    "value": "staff"
  },
  "compensation": {
    "winner": "hinted_compensation",
    "value": 161000
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 129
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:34:09.843000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Business Development Lead, US Regional Grocery - Chicago, Illinois | Uber Careers\n[Skip to main content](#main)\n[](https://www.uber.com/careers/list?query=)\n# Business Development Lead, US Regional Grocery\nSales, Sales &amp; Account Management\nLos Angeles, California |\nNew York, New York |\nSeattle, Washington |\nMiami, Florida |\nChicago, Illinois |\nSan Francisco, California\n[Apply Now](https://www.uber.com/careers/apply/interstitial/149889)\n**About the Role**\nAs a Business Development Lead, you will be responsible for engaging with senior-level executives, the negotiation process, and ultimately signing key strategic regional 
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Business Development Lead, US Regional Grocery`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599249918,
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
  "totalCompensation": 161000,
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
| title | `raw_row_title` | `Business Development Lead, US Regional Grocery` |
| company | `raw_row_company` | `Uber` |
| location | `raw_row_location` | `San Francisco, CA` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `staff` |
| compensation | `hinted_compensation` | `161000` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `129` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:34:09.843000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Business Development Lead, US Regional Grocery - C` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Business Development Lead, US Regional Grocery`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'uber_careers' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Business Development Lead, US ` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `Business Development Lead, US ` | Valid title |
| hinted_title | HEURISTIC | ✅ | `Business Development Lead, US ` | Valid title |
| first_line_title | FALLBACK | ✅ | `Business Development Lead, US ` | Valid title |

#### COMPANY

**Final Value:** `Uber`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'uber_careers' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Uber` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Uber` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ❌ | `` | No company pattern matched |
| hinted_company | HEURISTIC | ✅ | `the Role` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `San Francisco, CA`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'uber_careers' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `San Francisco, CA` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `San Francisco, California` | Matched pattern SIMPLE_LOCATION_LINE |
| country_only_fallback_location | CUSTOM_550 | ✅ | `San Francisco, CA` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `San Francisco, CA` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'San Francisco, CA' present but not infer |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Uber' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `staff`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `staff` | Explicit level field: staff -> staff |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `staff` | Level from title: 'lead' -> staff |
| content_pattern_level | CUSTOM_550 | ✅ | `mid` | Level from experience: 4+ years -> mid |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `161000`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `161000` | Compensation from hints: $161,000 |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `131000` | Single compensation pattern: $131,000 |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `129`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `129` | Valid cost: 129 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:34:09.843000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:34:09.843000` | Valid date: 2026-01-16T14:34:09.843000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'uber_careers' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:34:09.953065` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'uber_careers' returned no first_published |

#### DESCRIPTION

**Final Value:** `Business Development Lead, US Regional Grocery - Chicago, Illinois | Uber Careers
[Skip to main content](#main)
[](https://www.uber.com/careers/list?query=)
# Business Development Lead, US Regional Grocery
Sales, Sales &amp; Account Management
Los Angeles, California |
New York, New York |
Seattle, Washington |
Miami, Florida |
Chicago, Illinois |
San Francisco, California
[Apply Now](https://www.uber.com/careers/apply/interstitial/149889)
**About the Role**
As a Business Development Lead, you will be responsible for engaging with senior-level executives, the negotiation process, and ultimately signing key strategic regional grocery brands in the United States.
Who are we looking for? Someone who has proven they can build best-in-class partnerships with large brands, and thrives in a fast-paced, cross-functional environment. Your primary focus will be to form new partnerships, including account outreach, pitching, negotiation, and onboarding
**What You’ll Do**
* Run an end-to-end sales process: sales discovery and outreach, pitching, negotiation, and onboarding
* Engage with potential partners: From C-suite to individual contributors, while using data to tell a compelling story
* Use internal and external data to understand potential and influence outcomes
* continuously improve our sales capabilities as an organization
**Basic Qualifications**
* **Experience:** 4+ years of experience in Enterprise sales, consulting and/or partnerships with large, complex enterprise brands and organizations
* **Data Driven:** Ability to tell a data-driven story and work across large sets of data
* **Collaborative leadership:** Passion for digging in tactically, leveraging analytical skills where needed, and equally as comfortable leading a conversation with an Engineer, Product Manager, or C-level executive to secure buy-in
* **Self-Sufficient:** Speed, resourcefulness, and a go-getter mentality. You are comfortable working in a fast-paced environment and navigating ambiguity
* **Business Accument:** understand both strategic and technical aspects of a deal from the point of view of Uber and the Merchant
* **Project Management:** Comfortable concurrently managing many opportunities on short time horizons
**Preferred Qualifications**
* Experience with food delivery, retail and/or third-party marketplaces
* Financial modeling
* SQL
* Salesforce
For Chicago, IL-based roles: The base salary range for this role is USD$131,000 per year - USD$145,000 per year.
For Los Angeles, CA-based roles: The base salary range for this role is USD$131,000 per year - USD$145,000 per year.
For Miami, FL-based roles: The base salary range for this role is USD$131,000 per year - USD$145,000 per year.
For New York, NY-based roles: The base salary range for this role is USD$145,000 per year - USD$161,000 per year.
For San Francisco, CA-based roles: The base salary range for this role is USD$145,000 per year - USD$161,000 per year.
For Seattle, WA-based roles: The base salary range for this role is USD$131,000 per year - USD$145,000 per year.
For all US locations, you will be eligible to participate in Uber's bonus program, and may be offered an equity award, sales bonuses &amp; other types of comp. You will also be eligible for various benefits. More details can be found at the following link [https://www.uber.com/careers/benefits](https://www.uber.com/careers/benefits).
Uber's mission is to reimagine the way the world moves for the better. Here, bold ideas create real-world impact, challenges drive growth, and speed fuels progress. What moves us, moves the world - let's move it forward, together.
Uber is proud to be an Equal Opportunity employer. All qualified applicants will receive consideration for employment without regard to sex, gender identity, sexual orientation, race, color, religion, national origin, disability, protected Veteran status, age, or any other characteristic protected by law. We also consider qualified applicants regardless of criminal histories, consistent with legal requirements. If you have a disability or special need that requires accommodation, please let us know by completing [this form](https://forms.gle/aDWTk9k6xtMU25Y5A).
Offices continue to be central to collaboration and Uber's cultural identity. Unless formally approved to work fully remotely, Uber expects employees to spend at least half of their work time in their assigned office. For certain roles, such as those based at green-light hubs, employees are expected to be in-office for 100% of their time. Please speak with your recruiter to better understand in-office expectations for this role.
[Apply Now](https://www.uber.com/careers/apply/interstitial/149889)
See our Candidate Privacy Statement
](https://www.uber.com/legal/document/?name=candidate-privacy-notice)
Uber is proud to be an equal opportunity workplace. We are committed to equal employment opportunity regardless of race, color, ancestry, religion, sex, national origin, sexual orientation, age, citizenship, marital status, disability, gender identity, Veteran Status, or any other characteristic protected bylaw.
## Select your preferred language
[العربية](https://www.uber.com/global/ar/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[български език](https://www.uber.com/global/bg/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[বাংলা](https://www.uber.com/global/bn/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Catalan (Spain)](https://www.uber.com/global/ca-es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Čeština](https://www.uber.com/global/cs/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Dansk](https://www.uber.com/global/da/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Deutsch](https://www.uber.com/global/de/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[ελληνικά](https://www.uber.com/global/el/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[English](https://www.uber.com/global/en/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Internacional)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Argentina)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Chile)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Colombia)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Costa Rica)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Castellano](https://www.uber.com/global/es-es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Honduras)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (México)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Uruguay)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Eesti Keel](https://www.uber.com/global/et/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Suomi](https://www.uber.com/global/fi/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Français (Canada)](https://www.uber.com/global/fr-ca/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Français (France)](https://www.uber.com/global/fr/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[עברית](https://www.uber.com/global/he/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[हिन्दी](https://www.uber.com/global/hi/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Hrvatski](https://www.uber.com/global/hr/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Magyar](https://www.uber.com/global/hu/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Italiano](https://www.uber.com/global/it/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[日本語](https://www.uber.com/global/ja/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[ಕನ್ನಡ](https://www.uber.com/global/kn/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[한국어](https://www.uber.com/global/ko/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Kurdish Sorani](https://www.uber.com/global/ku/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Lietuvių](https://www.uber.com/global/lt/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[मराठी](https://www.uber.com/global/mr/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Norsk Bokmål](https://www.uber.com/global/nb/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Nederlands](https://www.uber.com/global/nl/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Polski](https://www.uber.com/global/pl/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Português (Brasil)](https://www.uber.com/global/pt-br/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Português (Portugal)](https://www.uber.com/global/pt-pt/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Română](https://www.uber.com/global/ro/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Pусский](https://www.uber.com/global/ru/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[සිංහල](https://www.uber.com/global/si-lk/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Slovenčina](https://www.uber.com/global/sk/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Slovenščina](https://www.uber.com/global/sl-si/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Svenska](https://www.uber.com/global/sv/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Kiswahili](https://www.uber.com/global/sw/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[தமிழ்](https://www.uber.com/global/ta/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[తెలుగు](https://www.uber.com/global/te/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Türkçe](https://www.uber.com/global/tr/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[українська мова](https://www.uber.com/global/uk/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[اردو](https://www.uber.com/global/ur/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[简体中文](https://www.uber.com/global/zh/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[香港中文版](https://www.uber.com/global/zh-hk/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[繁體中文](https://www.uber.com/global/zh-tw/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)
*
### Teams
*
Departments
*
[Business Development &amp; Sales](https://www.uber.com/careers/teams/business-development/)
*
[Communications](https://www.uber.com/careers/teams/communications/)
*
[Community Operations](https://www.uber.com/careers/teams/community-operations/)
*
[Data Science &amp; Analytics](https://www.uber.com/careers/teams/data-science/)
*
[Design](https://www.uber.com/careers/teams/design/)
*
[Engineering](https://www.uber.com/careers/teams/engineering/)
*
[Finance](https://www.uber.com/careers/teams/finance-and-accounting/)
*
[Legal](https://www.uber.com/careers/teams/legal/)
*
[Marketing](https://www.uber.com/careers/teams/marketing/)
*
[Operations &amp; Launch](https://www.uber.com/careers/teams/operations-and-launch/)
*
[People &amp; Places](https://www.uber.com/careers/teams/people-and-places/)
*
[Product](https://www.uber.com/careers/teams/product/)
*
[Public Policy](https://www.uber.com/careers/teams/public-policy/)
*
[Safety, Security &amp; Insurance](https://www.uber.com/careers/teams/safety-and-insurance/)
*
Offerings
*
[Ridesharing](https://www.uber.com/careers/teams/ridesharing/)
*
[Uber Eats](https://www.uber.com/careers/teams/eats/)
*
[Uber for Business](https://www.uber.com/careers/teams/uber-for-business/)
*
[Uber Freight](https://www.uberfreight.com/careers/)
*
### Locations
*
Asia Pacific
*
[Bengaluru](https://www.uber.com/careers/locations/bangalore/)
*
[Gurugram](https://www.uber.com/careers/locations/gurgaon/)
*
[Manila &amp; Pampanga](https://www.uber.com/careers/locations/manila/)
*
[Hong Kong](https://www.uber.com/careers/locations/hong-kong/)
*
[Hyderabad](https://www.uber.com/careers/locations/hyderabad/)
*
[Singapore](https://www.uber.com/careers/locations/singapore/)
*
[Seoul ](https://www.uber.com/careers/locations/seoul)
*
[Sydney](https://www.uber.com/careers/locations/sydney/)
*
[Tokyo](https://www.uber.com/careers/locations/japan/)
*
[Taipei](https://www.uber.com/careers/locations/taiwan/?nocache=true)
*
[Visakhapatnam](< https://www.uber.com/careers/visakhapatnam/>)
*
Europe, Middle East &amp; Africa
*
[Aarhus](https://www.uber.com/careers/locations/aarhus/)
*
[Amsterdam](https://www.uber.com/careers/locations/amsterdam/)
*
[Berlin](https://www.uber.com/careers/berlin/)
*
[London](https://www.uber.com/careers/locations/london/)
*
[Paris](https://www.uber.com/careers/locations/paris/)
*
Latin America
*
[Bogotá](https://www.uber.com/careers/locations/bogota/)
*
[Mexico City](https://www.uber.com/careers/locations/mexico-city/)
*
[Santiago](https://www.uber.com/careers/locations/santiago/)
*
[Sao Paulo](https://www.uber.com/careers/locations/sao-paulo)
*
United States &amp; Canada
*
[Chicago](https://www.uber.com/careers/locations/chicago/)
*
[Dallas](https://www.uber.com/careers/locations/dallas/)
*
[New York City](https://www.uber.com/careers/locations/new-york/)
*
[San Francisco Bay Area](https://www.uber.com/careers/locations/san-francisco-bay-area/)
*
[Seattle](https://www.uber.com/careers/locations/seattle/)
*
[Toronto](https://www.uber.com/careers/locations/toronto/)
*
[University](https://www.uber.com/careers/teams/university/)
*
### Inside Uber
*
[Blog](https://www.uber.com/blog/careers/)
*
[Benefits](https://www.uber.com/careers/benefits)
*
[Driving opportunity](https://www.uber.com/careers/drivingopportunity/)
*
[Grow at Uber](https://www.uber.com/careers/grow/)
*
[How we hire](https://www.uber.com/careers/interviewing)
*
[Programs](https://www.uber.com/careers/dei/)
*
[Life at Uber](https://www.uber.com/careers/lifeatuber/)
*
[Values](https://www.uber.com/careers/values/)
*
Products
EN
## Select your preferred language
[العربية](https://www.uber.com/global/ar/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[български език](https://www.uber.com/global/bg/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[বাংলা](https://www.uber.com/global/bn/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Catalan (Spain)](https://www.uber.com/global/ca-es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Čeština](https://www.uber.com/global/cs/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Dansk](https://www.uber.com/global/da/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Deutsch](https://www.uber.com/global/de/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[ελληνικά](https://www.uber.com/global/el/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[English](https://www.uber.com/global/en/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Internacional)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Argentina)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Chile)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Colombia)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Costa Rica)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Castellano](https://www.uber.com/global/es-es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Honduras)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (México)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Español (Uruguay)](https://www.uber.com/global/es/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Eesti Keel](https://www.uber.com/global/et/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Suomi](https://www.uber.com/global/fi/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Français (Canada)](https://www.uber.com/global/fr-ca/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Français (France)](https://www.uber.com/global/fr/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[עברית](https://www.uber.com/global/he/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[हिन्दी](https://www.uber.com/global/hi/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Hrvatski](https://www.uber.com/global/hr/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Magyar](https://www.uber.com/global/hu/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Italiano](https://www.uber.com/global/it/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[日本語](https://www.uber.com/global/ja/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[ಕನ್ನಡ](https://www.uber.com/global/kn/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[한국어](https://www.uber.com/global/ko/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Kurdish Sorani](https://www.uber.com/global/ku/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Lietuvių](https://www.uber.com/global/lt/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[मराठी](https://www.uber.com/global/mr/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Norsk Bokmål](https://www.uber.com/global/nb/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Nederlands](https://www.uber.com/global/nl/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Polski](https://www.uber.com/global/pl/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Português (Brasil)](https://www.uber.com/global/pt-br/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Português (Portugal)](https://www.uber.com/global/pt-pt/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Română](https://www.uber.com/global/ro/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Pусский](https://www.uber.com/global/ru/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[සිංහල](https://www.uber.com/global/si-lk/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Slovenčina](https://www.uber.com/global/sk/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Slovenščina](https://www.uber.com/global/sl-si/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Svenska](https://www.uber.com/global/sv/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Kiswahili](https://www.uber.com/global/sw/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[தமிழ்](https://www.uber.com/global/ta/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[తెలుగు](https://www.uber.com/global/te/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[Türkçe](https://www.uber.com/global/tr/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[українська мова](https://www.uber.com/global/uk/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[اردو](https://www.uber.com/global/ur/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[简体中文](https://www.uber.com/global/zh/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[香港中文版](https://www.uber.com/global/zh-hk/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)[繁體中文](https://www.uber.com/global/zh-tw/careers/list/149889/?iis=Job%20Board%20-%20Recruitment%20Marketing&amp;iisn=LinkedIn&amp;jClickId=94da018c-b5ac-443e-933e-26a5f7a04835)
*
Home
Home
](https://www.uber.com/)[
*
Car front
Ride
](https://www.uber.com/ride/)[
*
Steering wheel
Drive
](https://www.uber.com/drive/)[
*
Restaurant
Eat
](https://www.ubereats.com/)[
*
Wine
Merchants
](https://merchants.ubereats.com/)[
*
Truck
Freight
](https://www.uberfreight.com/)[
*
Train,
Transit
](https://www.uber.com/transit/)[
*
Bike jump
Bike &amp; scoot
](https://www.uber.com/ride/how-it-works/scooters-and-bikes/)[
*
Briefcase
Business
](https://www.uber.com/business/)[
*
Money
Money
](https://www.uber.com/money/)
# We use cookies`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Business Development Lead, US ` | Valid description (28354 chars, 989 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Business Development Lead, US ` | Valid description (28354 chars, 989 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Business Development Lead, US ` | Valid description (28354 chars, 989 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Business Development Lead, US Regional Grocery` |
| Company | `Uber` |
| Location | `San Francisco, CA` |
| Is Remote | `False` |
| Level | `staff` |
| Posted At | `1768599249843` |
| Description Words | `989` |
| Cost (milli-cents) | `129` |
| URL | `https://www.uber.com/careers/list/149889` |

**Description Preview (first 200 words):**

```
Business Development Lead, US Regional Grocery - Chicago, Illinois | Uber Careers [Skip to main content](#main) [](https://www.uber.com/careers/list?query=) # Business Development Lead, US Regional Grocery Sales, Sales &amp; Account Management Los Angeles, California | New York, New York | Seattle, Washington | Miami, Florida | Chicago, Illinois | San Francisco, California [Apply Now](https://www.uber.com/careers/apply/interstitial/149889) **About the Role** As a Business Development Lead, you will be responsible for engaging with senior-level executives, the negotiation process, and ultimately signing key strategic regional grocery brands in the United States. Who are we looking for? Someone who has proven they can build best-in-class partnerships with large brands, and thrives in a fast-paced, cross-functional environment. Your primary focus will be to form new partnerships, including account outreach, pitching, negotiation, and onboarding **What You’ll Do** * Run an end-to-end sales process: sales discovery and outreach, pitching, negotiation, and onboarding * Engage with potential partners: From C-suite to individual contributors, while using data to tell a compelling story * Use internal and external data to understand potential and influence outcomes * continuously improve our sales capabilities as an organization **Basic Qualifications** * **Experience:** 4+ years of experience in Enterprise sales, consulting and/or partnerships with large, complex enterprise brands and...
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
  "title": "Business Development Lead, US Regional Grocery",
  "company": "Uber",
  "location": "San Francisco, CA",
  "description": "Business Development Lead, US Regional Grocery - Chicago, Illinois | Uber Careers\n[Skip to main content](#main)\n[](https://www.uber.com/careers/list?query=)\n# Business Development Lead, US Regional Grocery\nSales, Sales &amp; Account Management\nLos Angeles, California |\nNew York, New York |\nSeattle, Washington |\nMiami, Florida |\nChicago, Illinois |\nSan Francisco, California\n[Apply Now](https://www.uber.com/careers/apply/interstitial/149889)\n**About the Role**\nAs a Business Development Lead, you w...",
  "url": "https://www.uber.com/careers/list/149889",
  "posted_at": 1768599249843,
  "level": "staff",
  "remote": false,
  "cost_milli_cents": 129,
  "_full_description_word_count": 989
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 129,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
