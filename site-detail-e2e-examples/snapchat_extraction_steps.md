# Extraction Steps: snapchat

**Detail URL:** `https://careers.snap.com/job?id=R0043117`
**Source URL:** `https://careers.snap.com/job?id=R0043117`
**Handler:** `snapchat`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Manager, Client Partner
[![logo](https://images.ctfassets.net/jwenq9l5fmib/5Am8vNzwNAXm2asqiP9SKP/1a7a6d315963c88cc318b96258c06135/Snap.svg?q=40)](https://careers.snap.com/)[](https://careers.snap.com/)[View Openings](https://careers.snap.com/jobs)
![](https://images.ctfassets.net/jwenq9l5fmib/4EFkiKP5Y9QZnRukcN7YTC/c2cb774cc6697a90ae890f3ca2651c59/Maureen_CareerSite_Learning.jpg?q=40&amp;h=800)
Sales
# Manager, Client Partner
Austin
Full time
Posted 58 days ago
R0043117
[Apply Now](https://wd1.myworkdaysite.com/recruiting/snapchat/snap/job/Austin-Texas/Manager--Client-Partner_R0043117-1/apply?source=Career+Site)
[Snap Inc](https://www.snap.com/en-US/?lang=en-US) is a technology company. We believe the camera presents the greatest opportunity to improve the way people live and communicate. Snap contributes to human progress by empowering people to express themselves, live in the moment, learn about the world, and have fun together. The Company’s three core products are [Snapchat](https://www.snapchat.com/?lang=en-US), a visual messaging app that enhances your relationships with friends, family, and the world; [Lens Studio](https://ar.snap.com/lens-studio?lang=en-US), an augmented reality platform that powers AR across Snapchat and other services; and its AR glasses, [Spectacles](https://www.spectacles.com/?lang=en-US).
We’re looking for a Manager, Client Partner to join Snap Inc! As a Manager, you will manage a team of Client Partners within one category. You will need to have natural leadership skills; the ability to coach, develop, and challenge your direct reports; and strong client-facing skills. You should have a deep understanding of the performance of top accounts at Snap and a point of view on how they would accelerate growth in their vertical over the next year.
What you’ll do:
*
Have command of and be responsible for the growth of your and subsequent accounts at Snap and be able to inform client teams on trends in that vertical
*
Manage a team of Client Partners
*
Build and manage relationships with key clients and agency partners
*
Hold your team accountable to revenue quotas and KPIs
*
Define, execute, and deliver KPI-driven measurement strategies
*
Understand broad vertical trends and translate into strategic areas and opportunities to drive growth of the business
*
Assist Sales leadership with determining individual quotas and account lists for team members
Knowledge, Skills &amp; Abilities:
*
Demonstrated ability to communicate, present and influence credibly and effectively at all levels of the organization (internally and externally)
*
Proven ability to drive the sales process from plan to close
*
Strong business sense and industry expertise across the verticals
*
Excellent mentoring, coaching and people management skills
*
Experience working effectively with cross-functional teams and all levels of management
*
Ability to travel as needed
Minimum Qualifications:
*
Bachelor’s degree in business, communications, marketing, or equi

... (truncated, 10655 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `snapchat`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `Manager, Client Partner`

Normalized markdown after handler processing:

```markdown
# Manager, Client Partner
Austin
Full time
Posted 58 days ago
[Snap Inc](https://www.snap.com/en-US/?lang=en-US) is a technology company. We believe the camera presents the greatest opportunity to improve the way people live and communicate. Snap contributes to human progress by empowering people to express themselves, live in the moment, learn about the world, and have fun together. The Company’s three core products are [Snapchat](https://www.snapchat.com/?lang=en-US), a visual messaging app that enhances your relationships with friends, family, and the world; [Lens Studio](https://ar.snap.com/lens-studio?lang=en-US), an augmented reality platform that powers AR across Snapchat and other services; and its AR glasses, [Spectacles](https://www.spectacles.com/?lang=en-US).
We’re looking for a Manager, Client Partner to join Snap Inc! As a Manager, you will manage a team of Client Partners within one category. You will need to have natural leadership skills; the ability to coach, develop, and challenge your direct reports; and strong client-facing skills. You should have a deep understanding of the performance of top accounts at Snap and a point of view on how they would accelerate growth in their vertical over the next year.
What you’ll do:
*
Have command of and be responsible for the growth of your and subsequent accounts at Snap and be able to inform client teams on trends in that vertical
*
Manage a team of Client Partners
*
Build and manage relationships with key clients and agency partners
*
Hold your team accountable to revenue quotas and KPIs
*
Define, execute, and deliver KPI-driven measurement strategies
*
Understand broad vertical trends and translate into strategic areas and opportunities to drive growth of the business
*
Assist Sales leadership with determining individual quotas and account lists for team members
Knowledge, Skills &amp; Abilities:
*
Demonstrated ability to communicate, present and influence credibly and effectively at all levels of the org

... (truncated, 5657 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: snapchat

```json
{
  "url": "https://careers.snap.com/job?id=R0043117",
  "handler": "snapchat"
}
```

### Raw Content Capture

Captured 10655 chars of commonmark content

```json
{
  "length": 10655,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='Manager, Client Partner', 5657 chars of normalized content

```json
{
  "title": "Manager, Client Partner",
  "normalized_length": 5657
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://careers.snap.com/job?id=R0043117",
      "sourceUrl": "https://careers.snap.com/job?id=R0043117",
      "provider": "spidercloud",
      "siteId": "snapchat",
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
    "value": "Manager, Client Partner"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Snap"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Austin, TX"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": false
  },
  "level": {
    "winner": "explicit_level_field",
    "value": "senior"
  },
  "compensation": {
    "winner": "hinted_compensation",
    "value": 286000
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 102
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2025-11-19T14:33:52.719000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "# Manager, Client Partner\nAustin\nFull time\nPosted 58 days ago\n[Snap Inc](https://www.snap.com/en-US/?lang=en-US) is a technology company. We believe the camera presents the greatest opportunity to improve the way people live and communicate. Snap contributes to human progress by empowering people to express themselves, live in the moment, learn about the world, and have fun together. The Company’s three core products are [Snapchat](https://www.snapchat.com/?lang=en-US), a visual messaging app that enhances your relationships with friends, family, and the world; [Lens Studio](https://ar.snap.com/lens-studio?lang=en-US), an augmented reality platform that powe
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Manager, Client Partner`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599232763,
  "heuristicVersion": 5,
  "locations": [
    "Austin, TX"
  ],
  "location": "Austin, TX",
  "locationStates": [
    "TX"
  ],
  "locationSearch": "Austin TX",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "totalCompensation": 286000,
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
| title | `site_handler_title` | `Manager, Client Partner` |
| company | `raw_row_company` | `Snap` |
| location | `raw_row_location` | `Austin, TX` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `senior` |
| compensation | `hinted_compensation` | `286000` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `102` |
| posted_at | `explicit_posted_at_field` | `2025-11-19 14:33:52.719000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `# Manager, Client Partner
Austin
Full time
Posted ` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Manager, Client Partner`
**Winning Strategy:** `site_handler_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_title** 🏆 | SITE_HANDLER | ✅ | `Manager, Client Partner` | Valid title |
| raw_row_title | EXPLICIT_FIELD | ✅ | `Manager, Client Partner` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `Manager, Client Partner` | Valid title |
| hinted_title | HEURISTIC | ✅ | `Manager, Client Partner` | Valid title |
| first_line_title | FALLBACK | ✅ | `Manager, Client Partner` | Valid title |

#### COMPANY

**Final Value:** `Snap`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'snapchat' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Snap` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Snap` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `Snap` | Found company in markdown link |
| hinted_company | HEURISTIC | ✅ | `Snap` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Austin, TX`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'snapchat' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Austin, TX` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Austin, TX` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `Austin, TX` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Austin, TX' present but not inferring re |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Snap' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `senior`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `senior` | Explicit level field: senior -> senior |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `senior` | Level from title: 'manager' -> senior |
| content_pattern_level | CUSTOM_550 | ✅ | `senior` | Level from experience: 10+ years -> senior |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `286000`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `286000` | Compensation from hint range: $229,000-$343,000 -> |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `286000` | Compensation range pattern: $229,000-$343,000 -> $ |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `102`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `102` | Valid cost: 102 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2025-11-19 14:33:52.719000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-11-19 14:33:52.719000` | Valid date: 2025-11-19T14:33:52.719000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'snapchat' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ✅ | `2025-11-19 14:33:52.780000` | Date from markdown via handler |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:52.780025` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'snapchat' returned no first_published |

#### DESCRIPTION

**Final Value:** `# Manager, Client Partner
Austin
Full time
Posted 58 days ago
[Snap Inc](https://www.snap.com/en-US/?lang=en-US) is a technology company. We believe the camera presents the greatest opportunity to improve the way people live and communicate. Snap contributes to human progress by empowering people to express themselves, live in the moment, learn about the world, and have fun together. The Company’s three core products are [Snapchat](https://www.snapchat.com/?lang=en-US), a visual messaging app that enhances your relationships with friends, family, and the world; [Lens Studio](https://ar.snap.com/lens-studio?lang=en-US), an augmented reality platform that powers AR across Snapchat and other services; and its AR glasses, [Spectacles](https://www.spectacles.com/?lang=en-US).
We’re looking for a Manager, Client Partner to join Snap Inc! As a Manager, you will manage a team of Client Partners within one category. You will need to have natural leadership skills; the ability to coach, develop, and challenge your direct reports; and strong client-facing skills. You should have a deep understanding of the performance of top accounts at Snap and a point of view on how they would accelerate growth in their vertical over the next year.
What you’ll do:
*
Have command of and be responsible for the growth of your and subsequent accounts at Snap and be able to inform client teams on trends in that vertical
*
Manage a team of Client Partners
*
Build and manage relationships with key clients and agency partners
*
Hold your team accountable to revenue quotas and KPIs
*
Define, execute, and deliver KPI-driven measurement strategies
*
Understand broad vertical trends and translate into strategic areas and opportunities to drive growth of the business
*
Assist Sales leadership with determining individual quotas and account lists for team members
Knowledge, Skills &amp; Abilities:
*
Demonstrated ability to communicate, present and influence credibly and effectively at all levels of the organization (internally and externally)
*
Proven ability to drive the sales process from plan to close
*
Strong business sense and industry expertise across the verticals
*
Excellent mentoring, coaching and people management skills
*
Experience working effectively with cross-functional teams and all levels of management
*
Ability to travel as needed
Minimum Qualifications:
*
Bachelor’s degree in business, communications, marketing, or equivalent experience
*
10+ years experience in sales in digital media space
Preferred Qualifications:
*
Can facilitate meetings with multiple senior stakeholders
*
Pre-existing relationships with brand marketers within the vertical and advertising agencies
If you have a disability or special need that requires accommodation, please don’t be shy and provide us some [information](https://docs.google.com/forms/d/e/1FAIpQLScV7t31iR3yYR9ztGDHJpbvL63svWpb6s0afkBkLEjGnDx4Kg/viewform).
"Default Together" Policy at Snap: At Snap Inc. we believe that being together in person helps us build our culture faster, reinforce our values, and serve our community, customers and partners better through dynamic collaboration. To reflect this, we practice a “default together” approach and expect our team members to work in an office 4+ days per week.
At Snap, we believe that having a team of diverse backgrounds and voices working together will enable us to create innovative products that improve the way people live and communicate. Snap is proud to be an equal opportunity employer, and committed to providing employment opportunities regardless of race, religious creed, color, national origin, ancestry, physical disability, mental disability, medical condition, genetic information, marital status, sex, gender, gender identity, gender expression, pregnancy, childbirth and breastfeeding, age, sexual orientation, military or veteran status, or any other protected classification, in accordance with applicable federal, state, and local laws. EOE, including disability/vets.
We are an Equal Opportunity Employer and will consider qualified applicants with criminal histories in a manner consistent with applicable law (by example, the requirements of the San Francisco Fair Chance Ordinance and the Los Angeles Fair Chance Initiative for Hiring, where applicable).
[Our Benefits](https://careers.snap.com/benefits): Snap Inc. is its own community, so we’ve got your back! We do our best to make sure you and your loved ones have everything you need to be happy and healthy, on your own terms. Our benefits are built around your needs and include paid parental leave, comprehensive medical coverage, emotional and mental health support programs, and compensation packages that let you share in Snap’s long-term success!
Compensation
In the United States, work locations are assigned a pay zone which determines the salary range for the position. The successful candidate’s starting pay will be determined based on job-related skills, experience, qualifications, work location, and market conditions. The starting pay may be negotiable within the salary range for the position. These pay zones may be modified in the future.
[Zone A (CA, WA, NYC)](https://careers.snap.com/us-payzones):
The base salary range for this position is $229,000-$343,000 annually.
[Zone B](https://careers.snap.com/us-payzones):
The base salary range for this position is $218,000-$326,000 annually.
[Zone C](https://careers.snap.com/us-payzones):
The base salary range for this position is $195,000-$292,000 annually.
This position is eligible to participate in a sales incentive program.
This position is eligible for equity in the form of RSUs.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `# Manager, Client Partner
Aust` | Valid description (5657 chars, 802 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `# Manager, Client Partner
Aust` | Valid description (5657 chars, 802 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `# Manager, Client Partner
Aust` | Valid description (5657 chars, 802 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Manager, Client Partner` |
| Company | `Snap` |
| Location | `Austin, TX` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1763588032719` |
| Description Words | `802` |
| Cost (milli-cents) | `102` |
| URL | `https://careers.snap.com/job?id=R0043117` |

**Description Preview (first 200 words):**

```
# Manager, Client Partner Austin Full time Posted 58 days ago [Snap Inc](https://www.snap.com/en-US/?lang=en-US) is a technology company. We believe the camera presents the greatest opportunity to improve the way people live and communicate. Snap contributes to human progress by empowering people to express themselves, live in the moment, learn about the world, and have fun together. The Company’s three core products are [Snapchat](https://www.snapchat.com/?lang=en-US), a visual messaging app that enhances your relationships with friends, family, and the world; [Lens Studio](https://ar.snap.com/lens-studio?lang=en-US), an augmented reality platform that powers AR across Snapchat and other services; and its AR glasses, [Spectacles](https://www.spectacles.com/?lang=en-US). We’re looking for a Manager, Client Partner to join Snap Inc! As a Manager, you will manage a team of Client Partners within one category. You will need to have natural leadership skills; the ability to coach, develop, and challenge your direct reports; and strong client-facing skills. You should have a deep understanding of the performance of top accounts at Snap and a point of view on how they would accelerate growth in their vertical over the next year. What you’ll do: * Have command of and be responsible for the growth of your and subsequent accounts at Snap and be able to inform...
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
  "title": "Manager, Client Partner",
  "company": "Snap",
  "location": "Austin, TX",
  "description": "# Manager, Client Partner\nAustin\nFull time\nPosted 58 days ago\n[Snap Inc](https://www.snap.com/en-US/?lang=en-US) is a technology company. We believe the camera presents the greatest opportunity to improve the way people live and communicate. Snap contributes to human progress by empowering people to express themselves, live in the moment, learn about the world, and have fun together. The Company’s three core products are [Snapchat](https://www.snapchat.com/?lang=en-US), a visual messaging app th...",
  "url": "https://careers.snap.com/job?id=R0043117",
  "posted_at": 1763588032719,
  "level": "senior",
  "remote": false,
  "cost_milli_cents": 102,
  "_full_description_word_count": 802
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 102,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
