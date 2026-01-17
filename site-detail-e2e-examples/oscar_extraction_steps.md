# Extraction Steps: oscar

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/oscar/jobs/7297858`
**Source URL:** `https://api.greenhouse.io/v1/boards/oscar/jobs`
**Handler:** `greenhouse`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
<html><meta content="light dark" name="color-scheme"><meta charset="utf-8"><pre>{"absolute_url":"http://www.hioscar.com/careers/7297858?gh_jid=7297858","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":3264575,"location":{"name":"New York, New York, United States"},"metadata":[{"id":157143,"name":"Job Description","value":null,"value...
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
<html><meta content="light dark" name="color-scheme"><meta charset="utf-8"><pre>{"absolute_url":"http://www.hioscar.com/careers/7297858?gh_jid=7297858","data_compliance":[{"type":"gdpr","requires_consent":false,"requires_processing_consent":false,"requires_retention_consent":false,"retention_period":null,"demographic_data_consent_applies":false}],"internal_job_id":3264575,"location":{"name":"New York, New York, United States"},"metadata":[{"id":157143,"name":"Job Description","value":null,"value...
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: greenhouse

```json
{
  "url": "https://boards-api.greenhouse.io/v1/boards/oscar/jobs/7297858",
  "handler": "greenhouse"
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
      "url": "https://boards-api.greenhouse.io/v1/boards/oscar/jobs/7297858",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/oscar/jobs",
      "provider": "spidercloud",
      "siteId": "oscar",
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
    "value": "Senior Security Engineer I, Platform Security"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Oscar Health"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "New York, New York, United States"
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
    "value": 183150
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 2
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2025-11-19T07:04:48"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Senior Security Engineer I, Platform Security\n\nHi, we're Oscar. We're hiring a Senior Security Engineer I, Platform Security to join our Security team.\nOscar is the first health insurance company built around a full stack technology platform and a relentless focus on serving our members. We started Oscar in 2012 to create the kind of health insurance company we would want for ourselves—one that behaves like a doctor in the family.\nAbout the role:\nThe Senior Security Engineer I takes on a leadership role in enhancing Oscar's security procedures, spearheading initiatives to elevate the organization's security posture. Th
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Senior Security Engineer I, Platform Security`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599221163,
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
  "totalCompensation": 183150,
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
| title | `raw_row_title` | `Senior Security Engineer I, Platform Security` |
| company | `raw_row_company` | `Oscar Health` |
| location | `raw_row_location` | `New York, New York, United States` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `senior` |
| compensation | `hinted_compensation` | `183150` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `2` |
| posted_at | `explicit_posted_at_field` | `2025-11-19 07:04:48` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Senior Security Engineer I, Platform Security

Hi,` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Senior Security Engineer I, Platform Security`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Senior Security Engineer I, Pl` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ✅ | `Senior Security Engineer I, Pl` | Valid title |
| first_line_title | FALLBACK | ✅ | `Senior Security Engineer I, Pl` | Valid title |

#### COMPANY

**Final Value:** `Oscar Health`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Oscar Health` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Oscar Health` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `our Security team` | Found 'Join Company' pattern |
| hinted_company | HEURISTIC | ✅ | `the role` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `New York, New York, United States`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `New York, New York, United Sta` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `New York` | Matched pattern BASED_IN |
| country_only_fallback_location | CUSTOM_550 | ✅ | `New York, New York, United Sta` | Country-only fallback: Valid location |
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
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'New York, New York, United States' prese |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Oscar Health' not in remote company list |
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

**Final Value:** `183150`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `183150` | Compensation from hint range: $158,400-$207,900 -> |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `183150` | Compensation range pattern: $158,400-$207,900 -> $ |
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

**Final Value:** `2025-11-19 07:04:48`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-11-19 07:04:48` | Valid date: 2025-11-19T07:04:48 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:41.175291` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no first_published |

#### DESCRIPTION

**Final Value:** `Senior Security Engineer I, Platform Security

Hi, we're Oscar. We're hiring a Senior Security Engineer I, Platform Security to join our Security team.
Oscar is the first health insurance company built around a full stack technology platform and a relentless focus on serving our members. We started Oscar in 2012 to create the kind of health insurance company we would want for ourselves—one that behaves like a doctor in the family.
About the role:
The Senior Security Engineer I takes on a leadership role in enhancing Oscar's security procedures, spearheading initiatives to elevate the organization's security posture. This individual not only identifies innovative perspectives on security but also pioneers solutions for intricate challenges. The Senior Security Engineer I collaborates extensively with diverse teams, offering strategic insights and counsel to ensure the alignment of Oscar's security objectives with overarching business goals. Furthermore, this role involves shaping and driving Oscar's security strategies, fostering a secure and resilient company environment through proactive measures and continuous improvement.
You will report into the Associate Director, Platform Security Engineering.
Work Location: This position is based in our New York City office, requiring a hybrid work schedule with 3 days of in-office work per week. Thursdays are a required in-office day for team meetings and events, while your other two office days are flexible to suit your schedule. #LI-Hybrid
Pay Transparency: The base pay for this role is: $158,400 - $207,900 per year. You are also eligible for employee benefits, participation in Oscar's unlimited vacation program, company equity grants, and annual performance bonuses.
Responsibilities:
- Collaborate closely with cross-functional teams to proactively identify, address, and resolve security concerns across Oscar, including proposing enhanced controls and procedural strategies to mitigate technical risks.&nbsp;
- Lead internal workshops with cross functional teams to discuss outcomes from technical reviews and develop a plan for mitigating identified risks.
- Exhibit a deep understanding of Oscar’s technology footprint, how our systems work and how they may be attacked or abused.
- Collaborate effectively with Security Leadership, providing insights into technical issues and their potential impacts.
- Collaborate with non-technical teams to propose control and process enhancements to mitigate technical risk.
- Engage in multiple-layers of oscars Technology stack to design security measures around protecting Oscars systems.
- Simplify intricate security concerns into actionable steps for effective remediation or risk mitigation.
- Define hardening and secure design standards and use them to perform application security reviews in partnership with developer teams.
- Compliance with all applicable laws and regulations.
- Other duties as assigned.
Requirements:
- 4+ years experience in Technology related field&nbsp;
- 4+ years experience in Security Engineering
Bonus points:
- Familiarity with industry standards and compliance frameworks (such as SOC, SOX, NIST, HIPAA) and experience in ensuring organizational adherence to these standards
- Certifications such as CISSP, CISM, CISA, CEH, or vendor-specific certifications
- Proficiency in managing security projects, including planning, execution, and successful delivery within timelines and budgets
- Hands-on experience in Cloud Engineering
- Experience with database data access/management including SQL and BigQuery
- Experience writing scripts with JavaScript, Go, or Python
- Experience implementing modern cloud infrastructure services in AWS or GCP with Terraform
- Experience using containers and container orchestration technology (Mesos and Kubernetes)
- Experience with Terraform or ArgoCD
This is an authentic Oscar Health job opportunity. Learn more about how you can safeguard yourself from recruitment fraud here .&nbsp;
At Oscar, being an Equal Opportunity Employer means more than upholding discrimination-free hiring practices. It means that we cultivate an environment where people can be their most authentic selves and find both belonging and support. We're on a mission to change health care -- an experience made whole by our unique backgrounds and perspectives.
Pay Transparency:&nbsp; Final offer amounts, within the base pay set forth above, are determined by factors including your relevant skills, education, and experience. Full-time employees are eligible for benefits including: medical, dental, and vision benefits, 11 paid holidays, paid sick time, paid parental leave, 401(k) plan participation, life and disability insurance, and paid wellness time and reimbursements.
Artificial Intelligence (AI): Our AI Guidelines outline the acceptable use of artificial intelligence for candidates and detail how we use AI to support our recruiting efforts.
Reasonable Accommodation: Oscar applicants are considered solely based on their qualifications, without regard to applicant’s disability or need for accommodation. Any Oscar applicant who requires reasonable accommodations during the application process should contact the Oscar Benefits Team (accommodations@hioscar.com) to make the need for an accommodation known.
California Residents: For information about our collection, use, and disclosure of applicants’ personal information as well as applicants’ rights over their personal information, please see our Privacy Policy .`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Senior Security Engineer I, Pl` | Valid description (5504 chars, 776 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Senior Security Engineer I, Pl` | Valid description (5504 chars, 776 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Senior Security Engineer I, Pl` | Valid description (5504 chars, 776 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Senior Security Engineer I, Platform Security` |
| Company | `Oscar Health` |
| Location | `New York, NY` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1763561088000` |
| Description Words | `776` |
| Cost (milli-cents) | `2` |
| URL | `https://boards-api.greenhouse.io/v1/boards/oscar/jobs/7297858` |

**Description Preview (first 200 words):**

```
Senior Security Engineer I, Platform Security Hi, we're Oscar. We're hiring a Senior Security Engineer I, Platform Security to join our Security team. Oscar is the first health insurance company built around a full stack technology platform and a relentless focus on serving our members. We started Oscar in 2012 to create the kind of health insurance company we would want for ourselves—one that behaves like a doctor in the family. About the role: The Senior Security Engineer I takes on a leadership role in enhancing Oscar's security procedures, spearheading initiatives to elevate the organization's security posture. This individual not only identifies innovative perspectives on security but also pioneers solutions for intricate challenges. The Senior Security Engineer I collaborates extensively with diverse teams, offering strategic insights and counsel to ensure the alignment of Oscar's security objectives with overarching business goals. Furthermore, this role involves shaping and driving Oscar's security strategies, fostering a secure and resilient company environment through proactive measures and continuous improvement. You will report into the Associate Director, Platform Security Engineering. Work Location: This position is based in our New York City office, requiring a hybrid work schedule with 3 days of in-office work per week. Thursdays are...
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
  "title": "Senior Security Engineer I, Platform Security",
  "company": "Oscar Health",
  "location": "New York, New York, United States",
  "description": "Senior Security Engineer I, Platform Security\n\nHi, we're Oscar. We're hiring a Senior Security Engineer I, Platform Security to join our Security team.\nOscar is the first health insurance company built around a full stack technology platform and a relentless focus on serving our members. We started Oscar in 2012 to create the kind of health insurance company we would want for ourselves—one that behaves like a doctor in the family.\nAbout the role:\nThe Senior Security Engineer I takes on a leaders...",
  "url": "https://boards-api.greenhouse.io/v1/boards/oscar/jobs/7297858",
  "apply_url": "https://boards.greenhouse.io/oscar/jobs/7297858",
  "posted_at": 1763561088000,
  "level": "senior",
  "remote": false,
  "cost_milli_cents": 2,
  "_full_description_word_count": 776
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
