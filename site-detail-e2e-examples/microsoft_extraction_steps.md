# Extraction Steps: microsoft

**Detail URL:** `https://apply.careers.microsoft.com/api/pcsx/position_details?position_id=1970393556651308&domain=microsoft.com&hl=en`
**Source URL:** `https://apply.careers.microsoft.com/api/pcsx/position_details?position_id=1970393556651308&domain=microsoft.com&hl=en`
**Handler:** `microsoft_careers`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
(No raw markdown captured)
```

---

## Step 2: Handler Detection

**Detected Handler:** `microsoft_careers`

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

Detected handler: microsoft_careers

```json
{
  "url": "https://apply.careers.microsoft.com/api/pcsx/position_details?position_id=1970393556651308&domain=microsoft.com&hl=en",
  "handler": "microsoft_careers"
}
```

### Raw Content Capture

Captured 0 chars of commonmark content

```json
{
  "length": 0,
  "content_type": "commonmark"
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://apply.careers.microsoft.com/api/pcsx/position_details?position_id=1970393556651308&domain=microsoft.com&hl=en",
      "sourceUrl": "https://apply.careers.microsoft.com/api/pcsx/position_details?position_id=1970393556651308&domain=microsoft.com&hl=en",
      "provider": "spidercloud",
      "siteId": "microsoft",
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
    "value": "Senior Software Engineer"
  },
  "company": {
    "winner": "site_handler_company",
    "value": "Microsoft"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Redmond, WA"
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
    "value": 208200
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 3
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-14T13:21:21"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Senior Software Engineer in Redmond, WA, US | Microsoft\n\nWork Location: onsite\n\nOverview\n\nSenior Software Engineer – Auction Logic – Monetize Platform\n\nWe built a platform to provide an open, transparent marketplace and powerful enterprise technology enabling marketers to connect and engage consumers on thousands of high-quality websites and apps.\n\nThe Monetize Platform is an advanced buy-side and sell-side advertising technology platform that enables buyers and sellers to access and trade premium inventory in a transparent and streamlined environment.\n\nMicrosoft is hiring an experienced Senior Software Engineer to join the Auction Logic team, specific
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Senior Software Engineer`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599221077,
  "heuristicVersion": 5,
  "locations": [
    "Redmond, WA"
  ],
  "location": "Redmond, WA",
  "locationStates": [
    "WA"
  ],
  "locationSearch": "WA Redmond",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "totalCompensation": 208200,
  "compensationUnknown": false,
  "compensationReason": "extractor:hinted_compensation",
  "description": "Senior Software Engineer in Redmond, WA, US | Microsoft\n\nWork Location: onsite\n\nOverview\n\nSenior Software Engineer – Auction Logic – Monetize Platform\n\nWe built a platform to provide an open, transparent marketplace and powerful enterprise technology enabling marketers to connect and engage consumers on thousands of high-quality websites and apps.\n\nThe Monetize Platform is an advanced buy-side and sell-side advertising technology platform that enables buyers and sellers to access and trade premium inventory in a transparent and streamlined env
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `raw_row_title` | `Senior Software Engineer` |
| company | `site_handler_company` | `Microsoft` |
| location | `raw_row_location` | `Redmond, WA` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `senior` |
| compensation | `hinted_compensation` | `208200` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `3` |
| posted_at | `explicit_posted_at_field` | `2026-01-14 13:21:21` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Senior Software Engineer in Redmond, WA, US | Micr` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Senior Software Engineer`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'microsoft_careers' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Senior Software Engineer` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ✅ | `Senior Software Engineer` | Valid title |
| first_line_title | FALLBACK | ✅ | `Senior Software Engineer in Re` | Valid title |

#### COMPANY

**Final Value:** `Microsoft`
**Winning Strategy:** `site_handler_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_company** 🏆 | SITE_HANDLER | ✅ | `Microsoft` | Valid company name |
| raw_row_company | EXPLICIT_FIELD | ✅ | `Microsoft` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Microsoft` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `The Monetize Platform` | Found 'Company is a' pattern |
| hinted_company | HEURISTIC | ✅ | `Microsoft` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Redmond, WA`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'microsoft_careers' returned no location h |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Redmond, WA` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Redmond, WA` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `Redmond, WA` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Redmond, WA' present but not inferring r |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Microsoft' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `senior`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `senior` | Explicit level field: senior -> senior |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `senior` | Level from title: 'senior' -> senior |
| content_pattern_level | CUSTOM_550 | ✅ | `senior` | Level from content: 'Senior' -> senior |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `208200`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `208200` | Compensation from hint range: $158,400-$258,000 -> |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `177250` | Compensation range pattern: $119,800-$234,700 -> $ |
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

**Final Value:** `2026-01-14 13:21:21`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-14 13:21:21` | Valid date: 2026-01-14T13:21:21 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'microsoft_careers' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:41.093582` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'microsoft_careers' returned no first_publ |

#### DESCRIPTION

**Final Value:** `Senior Software Engineer in Redmond, WA, US | Microsoft

Work Location: onsite

Overview

Senior Software Engineer – Auction Logic – Monetize Platform

We built a platform to provide an open, transparent marketplace and powerful enterprise technology enabling marketers to connect and engage consumers on thousands of high-quality websites and apps.

The Monetize Platform is an advanced buy-side and sell-side advertising technology platform that enables buyers and sellers to access and trade premium inventory in a transparent and streamlined environment.

Microsoft is hiring an experienced Senior Software Engineer to join the Auction Logic team, specifically focused on build, test, deploy, and monitoring.

As a Senior Software Engineer, you will

- Lead the cross-team engineering efforts to build business critical products for publishers and buyers on the Monetize platform.
- You will collaborate with a team of smart, passionate engineers.
- You will own significant areas of the Monetize real-time platforms and function as the architect.
- You will participate in setting the roadmap and direction for your areas of responsibility.

About the team:

The mission of the Auction Logic team is to create the digital marketplace with a purpose of accurately connecting advertisers (buyers) and publishers (sellers) in the most effective and efficient way possible. The team is responsible for building and operating a set of core components of Monetize real-time platform, specifically the distributed set of servers that runs an auction for every ad impression, delivers the ad content, and performs all the necessary accounting. The Monetize real-time platform is a large-scale distributed platform operating under strict real-time constraints, responding to and executing real-time, dynamic auctions in less than 100 milliseconds. In the past few years, we've grown from processing 300k QPS to over 5 million QPS for an approximate total of 100 billion requests per day. These events generate more than 150 TB of new data every day. Engineers work on the real-time components to optimize our software for performance, space, and algorithmic efficiency. The application that processes all those requests, impression bus, is among the responsibilities of the Auction Logic team. You will provide solutions to help connect advertisers and publishers in the digital marketplace as well as resolve complex technical issues that arise because of the immense scale of our platform.

About the job:

As a technical leader you’ll design and implement solutions that span multiple applications on the real time platform, are high quality, require hard real time constraints, are easy to operate and maintain, and make the lives of product engineers easier. You will own or influence significant parts of the platform and allied areas. You will work closely with other engineers and our product teams, to not only make these systems go faster and handle more and more data, but also to build revolutionary new products for our clients. responsibilities will include:

- Designing and implementing scalable solutions including new features and enhancements of our Platform.
- Participating in and helping lead planning sessions with Product Management and Engineering team leads to constructing feature requirements and specifications.
- Produce prototype features quickly and participate in gathering feedback from clients.
- Bring innovation to the team in new features, improving build processes and product development lifecycle.
- Participate in code reviews and provide feedback to increase code efficiency, maintainability and robustness.
- Support and debug production level issues and provide fixes in an expedient manner.
- Mentor junior engineers on best practices in software development

More about you:

- You are focused on building high-quality, intuitive products.
- You are passionate about learning and teaching.
- You love challenging yourself to constantly improve and sharing your knowledge to empower others.
- You view processes as a means and not an end, preferring lean or automated approaches to ensure quality and productivity.
- You are not satisfied with the status quo and are always looking to improve how things are done and what is built

Responsibilities

- Works with appropriate stakeholders to determine user requirements for a set of features.
- Contributes to the identification of dependencies, and the development of design documents for a product area with little oversight.
- Creates and implements code for a product, service, or feature, reusing code as applicable.
- Contributes to efforts to break down larger work items into smaller work items and provides estimation.
- Acts as a Designated Responsible Individual (DRI) working on-call to monitor system/product feature/service for degradation, downtime, or interruptions and gains approval to restore system/product/service for simple problems.
- Remains current in skills by investing time and effort into staying abreast of current developments that will improve the availability, reliability, efficiency, observability, and performance of products while also driving consistency in monitoring and operations at scale.

Qualifications

Required Qualifications:

- Bachelor's Degree in Computer Science or related technical field AND 4+ years technical engineering experience with coding in languages including, but not limited to, C, C++, C#, Java, JavaScript, or Python OR equivalent experience.

Preferred Qualifications:

- Master's Degree in Computer Science or related technical field AND 6+ years technical engineering experience with coding in languages including, but not limited to, C, C++, C#, Java, JavaScript, or Python
- OR Bachelor's Degree in Computer Science or related technical field AND 8+ years technical engineering experience with coding in languages including, but not limited to, C, C++, C#, Java, JavaScript, or Python
- OR equivalent experience.
- 5+ years of experience as a professional software developer.
- Experience building high performance, multi-threaded, distributed systems and applications, preferably in a Linux environment.
- Experience practicing advanced optimization techniques.
- Solid fundamental understanding of generalized architecture patterns and service-oriented architecture (SOA).
- Excellent Computer Science fundamentals with regards to data structures, algorithms, time complexity, etc.
- Excellent and creative problem-solving abilities. We will consider extensive experience with other languages as well, e.g., Rust, Go, etc.

Other Requirements:
Ability to meet Microsoft, customer and/or government security screening requirements are required for this role. These requirements include but are not limited to the following specialized security screenings:

- Microsoft Cloud Background Check: This position will be required to pass the Microsoft Cloud background check upon hire/transfer and every two years thereafter.

#MicrosoftAI

Software Engineering IC4 - The typical base pay range for this role across the U.S. is USD $119,800 - $234,700 per year. There is a different range applicable to specific work locations, within the San Francisco Bay area and New York City metropolitan area, and the base pay range for this role in those locations is USD $158,400 - $258,000 per year.

Certain roles may be eligible for benefits and other compensation. Find additional benefits and pay information here:
https://careers.microsoft.com/us/en/us-corporate-pay

This position will be open for a minimum of 5 days, with applications accepted on an ongoing basis until the position is filled.

Microsoft is an equal opportunity employer. All qualified applicants will receive consideration for employment without regard to age, ancestry, citizenship, color, family or medical care leave, gender identity or expression, genetic information, immigration status, marital status, medical condition, national origin, physical or mental disability, political affiliation, protected veteran or military status, race, ethnicity, religion, sex (including pregnancy), sexual orientation, or any other characteristic protected by applicable local laws, regulations and ordinances. If you need assistance with religious accommodations and/or a reasonable accommodation due to a disability during the application process, read more about requesting accommodations.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Senior Software Engineer in Re` | Valid description (8410 chars, 1221 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Senior Software Engineer in Re` | Valid description (8410 chars, 1221 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Senior Software Engineer in Re` | Valid description (8410 chars, 1221 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Senior Software Engineer` |
| Company | `Microsoft` |
| Location | `Redmond, WA` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1768422081000` |
| Description Words | `1221` |
| Cost (milli-cents) | `3` |
| URL | `https://apply.careers.microsoft.com/api/pcsx/position_details?position_id=1970393556651308&domain=microsoft.com&hl=en` |

**Description Preview (first 200 words):**

```
Senior Software Engineer in Redmond, WA, US | Microsoft Work Location: onsite Overview Senior Software Engineer – Auction Logic – Monetize Platform We built a platform to provide an open, transparent marketplace and powerful enterprise technology enabling marketers to connect and engage consumers on thousands of high-quality websites and apps. The Monetize Platform is an advanced buy-side and sell-side advertising technology platform that enables buyers and sellers to access and trade premium inventory in a transparent and streamlined environment. Microsoft is hiring an experienced Senior Software Engineer to join the Auction Logic team, specifically focused on build, test, deploy, and monitoring. As a Senior Software Engineer, you will - Lead the cross-team engineering efforts to build business critical products for publishers and buyers on the Monetize platform. - You will collaborate with a team of smart, passionate engineers. - You will own significant areas of the Monetize real-time platforms and function as the architect. - You will participate in setting the roadmap and direction for your areas of responsibility. About the team: The mission of the Auction Logic team is to create the digital marketplace with a purpose of accurately connecting advertisers (buyers) and publishers (sellers) in the most effective...
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
  "title": "Senior Software Engineer",
  "company": "Microsoft",
  "location": "Redmond, WA",
  "description": "Senior Software Engineer in Redmond, WA, US | Microsoft\n\nWork Location: onsite\n\nOverview\n\n\nSenior Software Engineer – Auction Logic – Monetize Platform\n\n\nWe built a platform to provide an open, transparent marketplace and powerful enterprise technology enabling marketers to connect and engage consumers on thousands of high-quality websites and apps.\n\n\nThe Monetize Platform is an advanced buy-side and sell-side advertising technology platform that enables buyers and sellers to access and trade pr...",
  "url": "https://apply.careers.microsoft.com/api/pcsx/position_details?position_id=1970393556651308&domain=microsoft.com&hl=en",
  "posted_at": 1768422081000,
  "level": "senior",
  "remote": false,
  "cost_milli_cents": 3,
  "_full_description_word_count": 1221
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
