# Extraction Steps: github

**Detail URL:** `https://www.github.careers/careers-home/jobs/4764?lang=en-us`
**Source URL:** `https://www.github.careers/careers-home/jobs/4764?lang=en-us`
**Handler:** `github_careers`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Software Engineer III in United States | GitHub, Inc.
[ Back ](<javascript: history.go(-1)>)
# Software Engineer III
JOB\_DESCRIPTION.SHARE.HTML
CAROUSEL\_PARAGRAPH
JOB\_DESCRIPTION.SHARE.HTML
* United States
* Engineering
* Experienced Professional
* Individual Contributor
* Yes
* 4764
* Full Time
mail\_outline
Get future jobs matching this search
LoginorRegister
## Job Description
**About GitHub**
GitHub is the world’s leading platform for agentic software development — powered by Copilot to build, scale, and deliver secure software. Over 180 million developers, including more than 90% of the Fortune 100 companies, use GitHub to collaborate, and more than 77,000 organisations have adopted GitHub Copilot.
**Locations**
In this role you can work from Remote, United States
**Overview**
GitHub is changing the way the world builds software and we want you to help lead this effort.
The Compute Foundation team owns and operates the core runtime layers that powers GitHub’s internal compute platform. Our team is responsible for the hypervisor-based GC2 VM platform, OS and container base images, fleet-wide configuration management, and secure, automated reboot orchestration across GitHub’s global data centers. Our mission is to provide a reliable, scalable, and low-toil platform that enables internal engineering teams to ship features, migrate to Azure, and meet security SLAs without worrying about infrastructure complexity.
As a Software Engineer III, you will contribute to the engineering foundations that keep GitHub’s services running smoothly. You’ll collaborate with a distributed team of engineers to improve the reliability, safety, and automation of our compute platform, covering everything from hypervisor lifecycle and VM runtime workflows to image pipelines and configuration tooling.
You’ll work in an environment optimized for asynchronous work and written communication, partnering with teams across the company to help them adopt platform best practices and build resilient services on top of our infrastructure. You’ll have opportunities to take well-scoped ownership areas, implement improvements that reduce operational toil, and build systems that directly support GitHub’s ability to scale.
This role provides the opportunity to solve complex infrastructure problems at scale, strengthen the platform that hundreds of internal teams rely on, and help shape the future of GitHub’s compute ecosystem.
**Responsibilities**
* Contribute to the design and implementation of reliable, performant, and secure systems that support GitHub’s compute platform (e.g. hypervisor, lifecycle, VM runtime, automation, image pipelines, and configuration tooling)
* Build features and automation that reduce operational toil and increase the predictability and safety of platform operations.
* Maintain and improve existing compute and lifecycle services, including GC2 components, fleet management workflows, and base OS/container image pipelines.
* Write, review, and maintain 

... (truncated, 7151 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `github_careers`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
Software Engineer III in United States | GitHub, Inc.
[ Back ](<javascript: history.go(-1)>)
# Software Engineer III
JOB\_DESCRIPTION.SHARE.HTML
CAROUSEL\_PARAGRAPH
JOB\_DESCRIPTION.SHARE.HTML
* United States
* Engineering
* Experienced Professional
* Individual Contributor
* Yes
* 4764
* Full Time
mail\_outline
Get future jobs matching this search
LoginorRegister
## Job Description
**About GitHub**
GitHub is the world’s leading platform for agentic software development — powered by Copilot to build, scale, and deliver secure software. Over 180 million developers, including more than 90% of the Fortune 100 companies, use GitHub to collaborate, and more than 77,000 organisations have adopted GitHub Copilot.
**Locations**
In this role you can work from Remote, United States
**Overview**
GitHub is changing the way the world builds software and we want you to help lead this effort.
The Compute Foundation team owns and operates the core runtime layers that powers GitHub’s internal compute platform. Our team is responsible for the hypervisor-based GC2 VM platform, OS and container base images, fleet-wide configuration management, and secure, automated reboot orchestration across GitHub’s global data centers. Our mission is to provide a reliable, scalable, and low-toil platform that enables internal engineering teams to ship features, migrate to Azure, and meet security SLAs without worrying about infrastructure complexity.
As a Software Engineer III, you will contribute to the engineering foundations that keep GitHub’s services running smoothly. You’ll collaborate with a distributed team of engineers to improve the reliability, safety, and automation of our compute platform, covering everything from hypervisor lifecycle and VM runtime workflows to image pipelines and configuration tooling.
You’ll work in an environment optimized for asynchronous work and written communication, partnering with teams across the company to help them adopt platform best practices and build re

... (truncated, 7151 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: github_careers

```json
{
  "url": "https://www.github.careers/careers-home/jobs/4764?lang=en-us",
  "handler": "github_careers"
}
```

### Raw Content Capture

Captured 7151 chars of commonmark content

```json
{
  "length": 7151,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 7151 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 7151
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://www.github.careers/careers-home/jobs/4764?lang=en-us",
      "sourceUrl": "https://www.github.careers/careers-home/jobs/4764?lang=en-us",
      "provider": "spidercloud",
      "siteId": "github",
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
    "value": "Software Engineer III"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "GitHub"
  },
  "location": {
    "winner": "country_only_fallback_location",
    "value": "United States"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": true
  },
  "level": {
    "winner": "default_level",
    "value": "mid"
  },
  "compensation": {
    "winner": "hinted_compensation",
    "value": 196800
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 418
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2025-12-05T14:11:00"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "About GitHub\n\nGitHub is the world’s leading platform for agentic software development — powered by Copilot to build, scale, and deliver secure software.  Over 180 million developers, including more than 90% of the Fortune 100 companies, use GitHub to collaborate, and more than 77,000 organisations have adopted GitHub Copilot.\n\nLocations\n\nIn this role you can work from Remote,  United States\n\nOverview\n\nGitHub is changing the way the world builds software and we want you to help lead this effort.\n\nThe Compute Foundation team owns and operates the core runtime layers that powers GitHub’s internal compute platform. Our team is responsible for the hypervisor-ba
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Software Engineer III`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599247769,
  "heuristicVersion": 5,
  "locations": [
    "United States"
  ],
  "location": "United States",
  "locationStates": [],
  "locationSearch": "United States",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "remote": true,
  "totalCompensation": 196800,
  "compensationUnknown": false,
  "compensationReason": "extractor:hinted_compensation",
  "description": "About GitHub\n\nGitHub is the world’s leading platform for agentic software development — powered by Copilot to build, scale, and deliver secure software.  Over 180 million developers, including more than 90% of the Fortune 100 companies, use GitHub to collaborate, and more than 77,000 organisations have adopted GitHub Copilot.\n\nLocations\n\nIn this role you can work from Remote,  United States\n\nOverview\n\nGitHub is changing the way the world builds software and we want you to help lead this effort.\n\nThe Compute Foundation team owns 
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `raw_row_title` | `Software Engineer III` |
| company | `raw_row_company` | `GitHub` |
| location | `country_only_fallback_location` | `United States` |
| remote | `explicit_remote_flag` | `True` |
| level | `default_level` | `mid` |
| compensation | `hinted_compensation` | `196800` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `418` |
| posted_at | `explicit_posted_at_field` | `2025-12-05 14:11:00` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `About GitHub

GitHub is the world’s leading platfo` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Software Engineer III`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'github_careers' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Software Engineer III` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ✅ | `About GitHub` | Valid title |
| first_line_title | FALLBACK | ✅ | `About GitHub` | Valid title |

#### COMPANY

**Final Value:** `GitHub`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'github_careers' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `GitHub` | Valid company name |
| url_company | URL_DERIVED | ✅ | `GitHub` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `About GitHub GitHub` | Found 'Company is a' pattern |
| hinted_company | HEURISTIC | ✅ | `GitHub` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `United States`
**Winning Strategy:** `country_only_fallback_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'github_careers' returned no location hint |
| raw_row_location | EXPLICIT_FIELD | ❌ | `` | Country-only location too generic: United States |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| **country_only_fallback_location** 🏆 | CUSTOM_550 | ✅ | `United States` | Country-only fallback: Country-only location accep |
| hinted_location | HEURISTIC | ✅ | `United States` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ✅ | `Remote` | Job marked as remote, using 'Remote' as location |

#### REMOTE

**Final Value:** `True`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `True` | Explicit boolean remote=True |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'United States' present but not inferring |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ✅ | `True` | Content contains remote pattern at position 372 |
| hinted_remote | HEURISTIC | ✅ | `True` | Remote from hints: True |
| remote_company | CUSTOM_650 | ✅ | `True` | Company 'GitHub' is known remote-first |
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

**Final Value:** `196800`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `196800` | Compensation from hint range: $107,700-$285,900 -> |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `107700` | Single compensation pattern: $107,700 |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `418`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `418` | Valid cost: 418 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2025-12-05 14:11:00`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-12-05 14:11:00` | Valid date: 2025-12-05T14:11:00 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'github_careers' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:34:07.784848` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'github_careers' returned no first_publish |

#### DESCRIPTION

**Final Value:** `About GitHub

GitHub is the world’s leading platform for agentic software development — powered by Copilot to build, scale, and deliver secure software.  Over 180 million developers, including more than 90% of the Fortune 100 companies, use GitHub to collaborate, and more than 77,000 organisations have adopted GitHub Copilot.

Locations

In this role you can work from Remote,  United States

Overview

GitHub is changing the way the world builds software and we want you to help lead this effort.

The Compute Foundation team owns and operates the core runtime layers that powers GitHub’s internal compute platform. Our team is responsible for the hypervisor-based GC2 VM platform, OS and container base images, fleet-wide configuration management, and secure, automated reboot orchestration across GitHub’s global data centers. Our mission is to provide a reliable, scalable, and low-toil platform that enables internal engineering teams to ship features, migrate to Azure, and meet security SLAs without worrying about infrastructure complexity.

As a Software Engineer III, you will contribute to the engineering foundations that keep GitHub’s services running smoothly. You’ll collaborate with a distributed team of engineers to improve the reliability, safety, and automation of our compute platform, covering everything from hypervisor lifecycle and VM runtime workflows to image pipelines and configuration tooling.

You’ll work in an environment optimized for asynchronous work and written communication, partnering with teams across the company to help them adopt platform best practices and build resilient services on top of our infrastructure. You’ll have opportunities to take well-scoped ownership areas, implement improvements that reduce operational toil, and build systems that directly support GitHub’s ability to scale.

This role provides the opportunity to solve complex infrastructure problems at scale, strengthen the platform that hundreds of internal teams rely on, and help shape the future of GitHub’s compute ecosystem.

Responsibilities

Contribute to the design and implementation of reliable, performant, and secure systems that support GitHub’s compute platform (e.g. hypervisor, lifecycle, VM runtime, automation, image pipelines, and configuration tooling)

Build features and automation that reduce operational toil and increase the predictability and safety of platform operations.

Maintain and improve existing compute and lifecycle services, including GC2 components, fleet management workflows, and base OS/container image pipelines.

Write, review, and maintain high-quality code while following GitHub engineering best practices.

Collaborate with partner engineering teams to help them adopt platform capabilities, debug issues, and integrate with Platform APIs and workflows.

Participate in on-call rotations for the services and systems owned by the organization, contributing to incident response, reliability improvements, and follow-up work.

Contribute to clear, thoughtful documentation and to asynchronous communication patterns that support a distributed engineering environment.

Qualifications

Required/Minimum Qualifications:

4+ years’ technical experience in infrastructure domains (e.g., container orchestration engineering, platform engineering, database engineering, software engineering, network engineering, systems administration, or related field),

OR bachelor's degree in computer science, Information Technology, or related field AND 2+ years’ technical experience in infrastructure domains (e.g., container orchestration engineering, platform engineering, database engineering, software engineering, network engineering, systems administration, or related field),

OR equivalent experience.

2+ years building and supporting large, high traffic applications at scale within platform/infrastructure domains

2+ years supporting and building cloud native workloads in Azure, AWS or Google Cloud

Preferred Qualifications:

4+ years’ experience with Azure, or any other Cloud Provider

Experience building or maintaining planetary scale engineering systems

Experience working with a remote, distributed team

Strong written and verbal communication skills

Demonstrated expertise in working with cloud environments and Cloud Native Compute Foundation (CNCF) concepts, which is beneficial for managing and optimizing cloud-based infrastructure.

Compensation Range

The base salary range for this job is USD $107,700.00 - USD $285,900.00 /Yr.

These pay ranges are intended to cover roles based across the United States. An individual's base pay depends on various factors including geographical location and review of experience, knowledge, skills, abilities of the applicant. At GitHub certain roles are eligible for benefits and additional rewards, including annual bonus and stock. These rewards are allocated based on individual impact in role. In addition, certain roles also have the opportunity to earn sales incentives based on revenue or utilization, depending on the terms of the plan and the employee's role.
GitHub values

Customer-obsessed

Ship to learn

Growth mindset

Own the outcome

Better together

Diverse and inclusive

Manager fundamentals

Model

Coach

Care

Leadership principles

Create clarity

Generate energy

Deliver success

Who We Are

GitHub is the world’s leading AI-powered developer platform with 150 million developers and counting. We’re also home to the biggest open-source community on earth (and 99% of the world’s software has open-source code in its DNA). Many of the apps and programs you use every day are built on GitHub.

Our teams are dreamers, doers, and pioneers, leading the way in AI, driving humanitarian efforts around the globe, and even sending open source to Mars (and beyond!).
At GitHub, our goal is to create the space you need to do your best work. We’re remote-first and offer competitive pay, generous learning and growth opportunities, and excellent benefits to support you, wherever you are—because we know that people flourish when they can work on their own terms.

Join us, and let’s change the world, together.

EEO Statement

GitHub is made up of people from a wide variety of backgrounds and lifestyles. We embrace diversity and invite applications from people of all walks of life. We don't discriminate against employees or applicants based on gender identity or expression, sexual orientation, race, religion, age, national origin, citizenship, disability, pregnancy status, veteran status, or any other differences. Also, if you have a disability, please let us know if there's any way we can make the interview process better for you; we're happy to accommodate!`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `About GitHub

GitHub is the wo` | Valid description (6710 chars, 937 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `About GitHub

GitHub is the wo` | Valid description (6710 chars, 937 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `About GitHub

GitHub is the wo` | Valid description (6710 chars, 937 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Software Engineer III` |
| Company | `GitHub` |
| Location | `United States` |
| Is Remote | `True` |
| Level | `mid` |
| Posted At | `1764969060000` |
| Description Words | `937` |
| Cost (milli-cents) | `418` |
| URL | `https://www.github.careers/careers-home/jobs/4764?lang=en-us` |

**Description Preview (first 200 words):**

```
About GitHub GitHub is the world’s leading platform for agentic software development — powered by Copilot to build, scale, and deliver secure software. Over 180 million developers, including more than 90% of the Fortune 100 companies, use GitHub to collaborate, and more than 77,000 organisations have adopted GitHub Copilot. Locations In this role you can work from Remote, United States Overview GitHub is changing the way the world builds software and we want you to help lead this effort. The Compute Foundation team owns and operates the core runtime layers that powers GitHub’s internal compute platform. Our team is responsible for the hypervisor-based GC2 VM platform, OS and container base images, fleet-wide configuration management, and secure, automated reboot orchestration across GitHub’s global data centers. Our mission is to provide a reliable, scalable, and low-toil platform that enables internal engineering teams to ship features, migrate to Azure, and meet security SLAs without worrying about infrastructure complexity. As a Software Engineer III, you will contribute to the engineering foundations that keep GitHub’s services running smoothly. You’ll collaborate with a distributed team of engineers to improve the reliability, safety, and automation of our compute platform, covering everything from hypervisor lifecycle and VM runtime...
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
  "title": "Software Engineer III",
  "company": "GitHub",
  "location": "United States",
  "description": "About GitHub\n\nGitHub is the world’s leading platform for agentic software development — powered by Copilot to build, scale, and deliver secure software.  Over 180 million developers, including more than 90% of the Fortune 100 companies, use GitHub to collaborate, and more than 77,000 organisations have adopted GitHub Copilot.\n \nLocations\n\nIn this role you can work from Remote,  United States\n \nOverview\n\nGitHub is changing the way the world builds software and we want you to help lead this effort...",
  "url": "https://www.github.careers/careers-home/jobs/4764?lang=en-us",
  "posted_at": 1764969060000,
  "level": "mid",
  "remote": true,
  "cost_milli_cents": 418,
  "_full_description_word_count": 937
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 418,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
