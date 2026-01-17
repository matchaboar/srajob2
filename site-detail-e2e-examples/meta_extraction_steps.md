# Extraction Steps: meta

**Detail URL:** `https://www.metacareers.com/profile/job_details/727671609895617`
**Source URL:** `https://www.metacareers.com/profile/job_details/727671609895617`
**Handler:** `meta_careers`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Meta Careers
[
Skip to main content
](#mdc-main-content)
# Software Engineer, Product
Sunnyvale, CA
+9 locations
Engineering
+1 more
Apply now
Meta is seeking talented engineers to join our teams in building cutting-edge products that connect billions of people around the world. As a member of our team, you will have the opportunity to work on complex technical problems, build new features, and improve existing products across various platforms, including mobile devices and web applications. Our teams are constantly pushing the boundaries of user experience, and we're looking for passionate individuals who can help us advance the way people connect globally. If you're interested in joining a world-class team of industry veterans and working on exciting projects that have a significant impact, we encourage you to apply.
## Software Engineer, Product Responsibilities
* Collaborate with cross-functional teams (product, design, operations, infrastructure) to build innovative application experiences
* Implement custom user interfaces using latest programming techniques and technologies
* Develop reusable software components for interfacing with back-end platforms
* Analyze and optimize code for quality, efficiency, and performance
* Lead complex technical or product efforts and provide technical guidance to peers
* Architect efficient and scalable systems that drive complex applications
* Identify and resolve performance and scalability issues
* Work on a variety of coding languages and technologies
* Establish ownership of components, features, or systems with expert end-to-end understanding
## Minimum Qualifications
* Currently has, or is in the process of obtaining a Bachelor's degree in Computer Science, Computer Engineering, relevant technical field, or equivalent practical experience. Degree must be completed prior to joining Meta
* 2+ years of programming experience in a relevant language OR a PhD + 9 months programming experience in a relevant language
* Track record of setting technical direction for a team, driving consensus and successful cross-functional partnerships
* Experience building maintainable and testable code bases, including API design and unit testing techniques
## Preferred Qualifications
* Exposure to architectural patterns of large scale software applications
* Experience improving quality through thoughtful code reviews, appropriate testing, proper rollout, monitoring, and proactive changes
* Experience with scripting languages such as Python, Javascript or Hack
* 2+ years of relevant experience building large-scale applications or similar experience
* Experience completing projects at large scope
* Experience in programming languages such as C, C++, Java, Swift, or Kotlin
* Experience as an owner of a particular component, feature or system
* 1+ years of experience identifying, designing and completing medium to large features independently without guidance
## About Meta
Meta builds technologies that help people connect, f

... (truncated, 5766 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `meta_careers`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
Meta Careers
[
Skip to main content
](#mdc-main-content)
# Software Engineer, Product
Sunnyvale, CA
+9 locations
Engineering
+1 more
Apply now
Meta is seeking talented engineers to join our teams in building cutting-edge products that connect billions of people around the world. As a member of our team, you will have the opportunity to work on complex technical problems, build new features, and improve existing products across various platforms, including mobile devices and web applications. Our teams are constantly pushing the boundaries of user experience, and we're looking for passionate individuals who can help us advance the way people connect globally. If you're interested in joining a world-class team of industry veterans and working on exciting projects that have a significant impact, we encourage you to apply.
## Software Engineer, Product Responsibilities
* Collaborate with cross-functional teams (product, design, operations, infrastructure) to build innovative application experiences
* Implement custom user interfaces using latest programming techniques and technologies
* Develop reusable software components for interfacing with back-end platforms
* Analyze and optimize code for quality, efficiency, and performance
* Lead complex technical or product efforts and provide technical guidance to peers
* Architect efficient and scalable systems that drive complex applications
* Identify and resolve performance and scalability issues
* Work on a variety of coding languages and technologies
* Establish ownership of components, features, or systems with expert end-to-end understanding
## Minimum Qualifications
* Currently has, or is in the process of obtaining a Bachelor's degree in Computer Science, Computer Engineering, relevant technical field, or equivalent practical experience. Degree must be completed prior to joining Meta
* 2+ years of programming experience in a relevant language OR a PhD + 9 months programming experience in a relevant language
* Track re

... (truncated, 5766 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: meta_careers

```json
{
  "url": "https://www.metacareers.com/profile/job_details/727671609895617",
  "handler": "meta_careers"
}
```

### Raw Content Capture

Captured 5766 chars of commonmark content

```json
{
  "length": 5766,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 5766 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 5766
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://www.metacareers.com/profile/job_details/727671609895617",
      "sourceUrl": "https://www.metacareers.com/profile/job_details/727671609895617",
      "provider": "spidercloud",
      "siteId": "meta",
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
    "value": "Software Engineer, Product"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Meta"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Sunnyvale, CA"
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
    "value": 365
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2025-06-10T15:21:19"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Meta is seeking talented engineers to join our teams in building cutting-edge products that connect billions of people around the world. As a member of our team, you will have the opportunity to work on complex technical problems, build new features, and improve existing products across various platforms, including mobile devices and web applications. Our teams are constantly pushing the boundaries of user experience, and we're looking for passionate individuals who can help us advance the way people connect globally. If you're interested in joining a world-class team of industry veterans and working on exciting projects that have a significant impact, we encourage you to apply.\n\
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Software Engineer, Product`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599233834,
  "heuristicVersion": 5,
  "locations": [
    "Sunnyvale, CA"
  ],
  "location": "Sunnyvale, CA",
  "locationStates": [
    "CA"
  ],
  "locationSearch": "Sunnyvale CA",
  "countries": [
    "United States"
  ],
  "country": "United States"
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
| title | `raw_row_title` | `Software Engineer, Product` |
| company | `raw_row_company` | `Meta` |
| location | `raw_row_location` | `Sunnyvale, CA` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `default_level` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `365` |
| posted_at | `explicit_posted_at_field` | `2025-06-10 15:21:19` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Meta is seeking talented engineers to join our tea` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Software Engineer, Product`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'meta_careers' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Software Engineer, Product` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ✅ | `Implement custom user interfac` | Valid title |
| first_line_title | FALLBACK | ✅ | `Collaborate with cross-functio` | Valid title |

#### COMPANY

**Final Value:** `Meta`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'meta_careers' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Meta` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Meta` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ❌ | `` | No company pattern matched |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Sunnyvale, CA`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'meta_careers' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Sunnyvale, CA` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Sunnyvale, CA` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ❌ | `` | No location in hints |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Sunnyvale, CA' present but not inferring |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | No remote in hints |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Meta' not in remote company list |
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

**Final Value:** `365`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `365` | Valid cost: 365 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2025-06-10 15:21:19`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-06-10 15:21:19` | Valid date: 2025-06-10T15:21:19 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'meta_careers' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:53.845118` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'meta_careers' returned no first_published |

#### DESCRIPTION

**Final Value:** `Meta is seeking talented engineers to join our teams in building cutting-edge products that connect billions of people around the world. As a member of our team, you will have the opportunity to work on complex technical problems, build new features, and improve existing products across various platforms, including mobile devices and web applications. Our teams are constantly pushing the boundaries of user experience, and we're looking for passionate individuals who can help us advance the way people connect globally. If you're interested in joining a world-class team of industry veterans and working on exciting projects that have a significant impact, we encourage you to apply.

Responsibilities
Collaborate with cross-functional teams (product, design, operations, infrastructure) to build innovative application experiences
Implement custom user interfaces using latest programming techniques and technologies
Develop reusable software components for interfacing with back-end platforms
Analyze and optimize code for quality, efficiency, and performance
Lead complex technical or product efforts and provide technical guidance to peers
Architect efficient and scalable systems that drive complex applications
Identify and resolve performance and scalability issues
Work on a variety of coding languages and technologies
Establish ownership of components, features, or systems with expert end-to-end understanding

Qualifications
Currently has, or is in the process of obtaining a Bachelor's degree in Computer Science, Computer Engineering, relevant technical field, or equivalent practical experience. Degree must be completed prior to joining Meta
2+ years of programming experience in a relevant language OR a PhD + 9 months programming experience in a relevant language
Track record of setting technical direction for a team, driving consensus and successful cross-functional partnerships
Experience building maintainable and testable code bases, including API design and unit testing techniques Exposure to architectural patterns of large scale software applications
Experience improving quality through thoughtful code reviews, appropriate testing, proper rollout, monitoring, and proactive changes
Experience with scripting languages such as Python, Javascript or Hack
2+ years of relevant experience building large-scale applications or similar experience
Experience completing projects at large scope
Experience in programming languages such as C, C++, Java, Swift, or Kotlin
Experience as an owner of a particular component, feature or system
1+ years of experience identifying, designing and completing medium to large features independently without guidance`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Meta is seeking talented engin` | Valid description (2681 chars, 368 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Meta is seeking talented engin` | Valid description (2681 chars, 368 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Meta is seeking talented engin` | Valid description (2681 chars, 368 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Software Engineer, Product` |
| Company | `Meta` |
| Location | `Sunnyvale, CA` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1749594079000` |
| Description Words | `368` |
| Cost (milli-cents) | `365` |
| URL | `https://www.metacareers.com/profile/job_details/727671609895617` |

**Description Preview (first 200 words):**

```
Meta is seeking talented engineers to join our teams in building cutting-edge products that connect billions of people around the world. As a member of our team, you will have the opportunity to work on complex technical problems, build new features, and improve existing products across various platforms, including mobile devices and web applications. Our teams are constantly pushing the boundaries of user experience, and we're looking for passionate individuals who can help us advance the way people connect globally. If you're interested in joining a world-class team of industry veterans and working on exciting projects that have a significant impact, we encourage you to apply. Responsibilities Collaborate with cross-functional teams (product, design, operations, infrastructure) to build innovative application experiences Implement custom user interfaces using latest programming techniques and technologies Develop reusable software components for interfacing with back-end platforms Analyze and optimize code for quality, efficiency, and performance Lead complex technical or product efforts and provide technical guidance to peers Architect efficient and scalable systems that drive complex applications Identify and resolve performance and scalability issues Work on a variety of coding languages and technologies Establish ownership of components, features, or systems with expert end-to-end understanding Qualifications Currently has, or...
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
  "title": "Software Engineer, Product",
  "company": "Meta",
  "location": "Sunnyvale, CA",
  "description": "Meta is seeking talented engineers to join our teams in building cutting-edge products that connect billions of people around the world. As a member of our team, you will have the opportunity to work on complex technical problems, build new features, and improve existing products across various platforms, including mobile devices and web applications. Our teams are constantly pushing the boundaries of user experience, and we're looking for passionate individuals who can help us advance the way p...",
  "url": "https://www.metacareers.com/profile/job_details/727671609895617",
  "posted_at": 1749594079000,
  "level": "mid",
  "remote": false,
  "cost_milli_cents": 365,
  "_full_description_word_count": 368
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 365,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
