# Extraction Steps: greenhouse

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/twilio/jobs/7312780`
**Source URL:** `https://boards-api.greenhouse.io/v1/boards/twilio/jobs/7312780`
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
  "url": "https://boards-api.greenhouse.io/v1/boards/twilio/jobs/7312780",
  "handler": "greenhouse"
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
      "url": "https://boards-api.greenhouse.io/v1/boards/twilio/jobs/7312780",
      "sourceUrl": "https://boards-api.greenhouse.io/v1/boards/twilio/jobs/7312780",
      "provider": "spidercloud",
      "siteId": "greenhouse",
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
    "value": "Application Integration Engineer"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Twilio"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Remote - Colombia"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": true
  },
  "level": {
    "winner": "content_pattern_level",
    "value": "mid"
  },
  "compensation": {
    "winner": "unknown_compensation",
    "value": 0
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 1
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-13T12:10:26"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Application Integration Engineer\n\nWho we are&nbsp;\nAt Twilio, we’re shaping the future of communications, all from the comfort of our homes. We deliver innovative solutions to &nbsp;hundreds of thousands of businesses&nbsp; and empower millions of developers worldwide to craft personalized customer experiences.\nOur dedication to remote-first work , and strong culture of connection and global inclusion means that no matter your location, you’re part of a vibrant team with diverse experiences making a global impact each day. As we continue to revolutionize how the world interacts, we’re acquiring new skills and experiences that make work feel truly rewarding. You
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Application Integration Engineer`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599259454,
  "heuristicVersion": 5,
  "location": "Remote - Colombia",
  "locationSearch": "Remote - Colombia"
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
| title | `raw_row_title` | `Application Integration Engineer` |
| company | `raw_row_company` | `Twilio` |
| location | `raw_row_location` | `Remote - Colombia` |
| remote | `explicit_remote_flag` | `True` |
| level | `content_pattern_level` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `1` |
| posted_at | `explicit_posted_at_field` | `2026-01-13 12:10:26` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Application Integration Engineer

Who we are&nbsp;` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Application Integration Engineer`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Application Integration Engine` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ✅ | `Application Integration Engine` | Valid title |
| first_line_title | FALLBACK | ✅ | `Application Integration Engine` | Valid title |

#### COMPANY

**Final Value:** `Twilio`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Twilio` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Twilio` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `Twilio` | Found 'Work at Company' pattern |
| hinted_company | HEURISTIC | ✅ | `the job` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Remote - Colombia`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Remote - Colombia` | Valid location |
| explicit_label_location | CUSTOM_350 | ✅ | `&nbsp` | Found 'Location:' label |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `Mulesoft, Workato` | Matched pattern LOCATION_FULL |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Remote - Colombia` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `Los Angeles, CA` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ✅ | `Remote` | Job marked as remote, using 'Remote' as location |

#### REMOTE

**Final Value:** `True`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `True` | Explicit boolean remote=True |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ✅ | `True` | Location contains 'remote': Remote - Colombia |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ✅ | `True` | Content contains remote pattern at position 331 |
| hinted_remote | HEURISTIC | ✅ | `True` | Remote from hints: True |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Twilio' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `mid`
**Winning Strategy:** `content_pattern_level`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_level_field | STRUCTURED_DATA | ❌ | `` | Skipping mid-level default: mid |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ❌ | `` | No level indicator in title |
| **content_pattern_level** 🏆 | CUSTOM_550 | ✅ | `mid` | Level from experience: 3+ years -> mid |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

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

**Final Value:** `1`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `1` | Valid cost: 1 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-13 12:10:26`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-13 12:10:26` | Valid date: 2026-01-13T12:10:26 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:34:19.464665` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no first_published |

#### DESCRIPTION

**Final Value:** `Application Integration Engineer

Who we are&nbsp;
At Twilio, we’re shaping the future of communications, all from the comfort of our homes. We deliver innovative solutions to &nbsp;hundreds of thousands of businesses&nbsp; and empower millions of developers worldwide to craft personalized customer experiences.
Our dedication to remote-first work , and strong culture of connection and global inclusion means that no matter your location, you’re part of a vibrant team with diverse experiences making a global impact each day. As we continue to revolutionize how the world interacts, we’re acquiring new skills and experiences that make work feel truly rewarding. Your career at Twilio is in your hands.
We use Artificial Intelligence (AI) technologies to maintain an efficient, fair and transparent hiring process. Our hiring process is never completely automated, and uses AI in conjunction with our recruiting professionals.
See yourself at Twilio
Join the team as Twilio’s next Application Integration Engineer – Informatica IDMC
About the job
This position is needed to bridge business and technology by delivering reliable, high-quality integrations across multiple platforms, enabling smooth data and process flow across the enterprise.&nbsp;
Join our Center for Enablement (C4E), a mature team managing 100s of enterprise integrations across Finance, HR, CRM, and more. You’ll design and implement solutions on Informatica IDMC (IICS CDI/CAI), with exposure to Mulesoft and Workato, applying cloud, and CI/CD expertise. Beyond building pipelines, you’ll own best practices, advise stakeholders, and drive operational improvements while ensuring secure, high-quality, and scalable integrations across distributed teams.&nbsp;
&nbsp;
Responsibilities
In this role, you’ll:
- Design and implement integrations using Informatica IICS (CDI/CAI) and Mulesoft, Workato, leveraging 3+ years of experience.
- Develop mappings, workflows, service tasks, event triggers, and secure agent deployments, while handling long-running processes, errors, and connectors.
- Generate robust API/data integrations, transforming and mapping data across multiple formats, handling schema changes, ensuring quality, and balancing streaming vs. batch ingestion.
- Apply strong SQL and data modeling skills to optimize queries, perform format conversions, and manage source/target systems effectively.
- Leverage cloud platforms like AWS, Azure, GCP to design integrations mindful of networking, storage, and serverless/PaaS considerations.
- Implement version control and CI/CD pipelines for deploying, rolling back, and promoting mappings/workflows across environments.
- Debug integration failures and performance bottlenecks, ensuring resilience and high availability.
- Collaborate with stakeholders across time zones, gathering requirements, defining SLAs, and addressing error handling expectations.
- Own best practices for integration engineering, driving technical excellence and organizational influence within the Center for Enablement team.
- Identify blockers, dependencies, and risks; communicate proactively with stakeholders on status and updates.
- Prioritize MVPs and iterative product delivery, making thoughtful trade-offs among competing demands.
- Document requirements, BRDs, functional/technical specifications, and UAT scripts; analyze system integrations and provide data quality assessments.
Qualifications&nbsp;
Twilio values diverse experiences from all kinds of industries, and we encourage everyone who meets the required qualifications to apply. If your career is just starting or hasn't followed a traditional path, don't let that stop you from considering Twilio. We are always looking for people who will bring something new to the table!
&nbsp;
*Required:
- 3+ years experience designing and building integrations using Informatica IICS (CDI/CAI).
- Experience developing mappings, workflows, and service tasks with strong error handling and agent management.
- API, SQL and data modeling skills to optimize data flows, transformations &amp; application integrations.
- Collaboration with cross-functional teams across timezones to gather requirements, drive designs and define SLAs.
Desired:
- Experience with Mulesoft, Workato, Kafka, Snowflake, Salesforce alongside Informatica for hybrid integration.
- Familiarity with AWS, Azure, or GCP for cloud-native integration design.
- Knowledge of CI/CD, version control, and automation for deployment pipelines.
- Strong documentation and communication skills for BRDs, functional specs, and UAT scripts.
Location
&nbsp; This role will be remote, and based in Colombia.
Travel&nbsp;
We prioritize connection and opportunities to build relationships with our customers and each other. For this role, you may be required to travel occasionally to participate in project or team in-person meetings.
What We Offer
Working at Twilio offers many benefits, including competitive pay, generous time off, ample parental and wellness leave, healthcare, a retirement savings program, and much more. Offerings vary by location.
Twilio thinks big. Do you?
We like to solve problems, take initiative, pitch in when needed, and are always up for trying new things. That's why we seek out colleagues who embody our values — something we call Twilio Magic . Additionally, we empower employees to build positive change in their communities by supporting their volunteering and donation efforts.
So, if you're ready to unleash your full potential, do your best work, and be the best version of yourself, apply now! If this role isn't what you're looking for, please consider other open positions.
Twilio is proud to be an equal opportunity employer. We do not discriminate based upon race, religion, color, national origin, sex (including pregnancy, childbirth, reproductive health decisions, or related medical conditions), sexual orientation, gender identity, gender expression, age, status as a protected veteran, status as an individual with a disability, genetic information, political views or activity, or other applicable legally protected characteristics. We also consider qualified applicants with criminal histories, consistent with applicable federal, state and local law. Qualified applicants with arrest or conviction records will be considered for employment in accordance with the Los Angeles County Fair Chance Ordinance for Employers and the California Fair Chance Act. Additionally, Twilio participates in the E-Verify program in certain locations, as required by law.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Application Integration Engine` | Valid description (6537 chars, 910 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Application Integration Engine` | Valid description (6537 chars, 910 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Application Integration Engine` | Valid description (6537 chars, 910 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Application Integration Engineer` |
| Company | `Twilio` |
| Location | `Remote - Colombia` |
| Is Remote | `True` |
| Level | `mid` |
| Posted At | `1768331426000` |
| Description Words | `910` |
| Cost (milli-cents) | `1` |
| URL | `https://boards-api.greenhouse.io/v1/boards/twilio/jobs/7312780` |

**Description Preview (first 200 words):**

```
Application Integration Engineer Who we are&nbsp; At Twilio, we’re shaping the future of communications, all from the comfort of our homes. We deliver innovative solutions to &nbsp;hundreds of thousands of businesses&nbsp; and empower millions of developers worldwide to craft personalized customer experiences. Our dedication to remote-first work , and strong culture of connection and global inclusion means that no matter your location, you’re part of a vibrant team with diverse experiences making a global impact each day. As we continue to revolutionize how the world interacts, we’re acquiring new skills and experiences that make work feel truly rewarding. Your career at Twilio is in your hands. We use Artificial Intelligence (AI) technologies to maintain an efficient, fair and transparent hiring process. Our hiring process is never completely automated, and uses AI in conjunction with our recruiting professionals. See yourself at Twilio Join the team as Twilio’s next Application Integration Engineer – Informatica IDMC About the job This position is needed to bridge business and technology by delivering reliable, high-quality integrations across multiple platforms, enabling smooth data and process flow across the enterprise.&nbsp; Join our Center for Enablement (C4E), a mature team managing 100s of enterprise integrations across Finance, HR, CRM,...
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
  "title": "Application Integration Engineer",
  "company": "Twilio",
  "location": "Remote - Colombia",
  "description": "Application Integration Engineer\n\nWho we are&nbsp;\nAt Twilio, we’re shaping the future of communications, all from the comfort of our homes. We deliver innovative solutions to &nbsp;hundreds of thousands of businesses&nbsp; and empower millions of developers worldwide to craft personalized customer experiences.\nOur dedication to remote-first work , and strong culture of connection and global inclusion means that no matter your location, you’re part of a vibrant team with diverse experiences maki...",
  "url": "https://boards-api.greenhouse.io/v1/boards/twilio/jobs/7312780",
  "apply_url": "https://boards.greenhouse.io/twilio/jobs/7312780",
  "posted_at": 1768331426000,
  "level": "mid",
  "remote": true,
  "cost_milli_cents": 1,
  "_full_description_word_count": 910
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 1,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
