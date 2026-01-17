# Extraction Steps: nexhealth

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/nexhealth/jobs/5709065004`
**Source URL:** `https://api.greenhouse.io/v1/boards/nexhealth/jobs`
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
  "url": "https://boards-api.greenhouse.io/v1/boards/nexhealth/jobs/5709065004",
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
      "url": "https://boards-api.greenhouse.io/v1/boards/nexhealth/jobs/5709065004",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/nexhealth/jobs",
      "provider": "spidercloud",
      "siteId": "nexhealth",
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
    "value": "Demand Generation Lead"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "NexHealth"
  },
  "location": {
    "winner": "content_pattern_location",
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
    "winner": "content_pattern_compensation",
    "value": 177000
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 3
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2025-12-01T12:43:26"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Demand Generation Lead\n\nAbout NexHealth\nOur healthcare system remains frustratingly analog. When you live in a world of one-tap car rides, instant meal delivery, and unlimited streaming, why do you still have to call to schedule a doctor’s appointment and fill out a clipboard in the waiting room?\nNexHealth’s mission is to accelerate innovation in healthcare by connecting patients, providers, and developers.&nbsp;We’re building the infrastructure layer for modern healthcare, connecting thousands of fragmented, on-premise, and closed EHR systems into a single, modern platform that powers software, APIs, payments, and patient experiences across the
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Demand Generation Lead`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599238538,
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
  "totalCompensation": 177000,
  "compensationUnknown": false,
  "compensationReason": "extractor:content_pattern_compensation"
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
| title | `raw_row_title` | `Demand Generation Lead` |
| company | `raw_row_company` | `NexHealth` |
| location | `content_pattern_location` | `San Francisco, CA` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `staff` |
| compensation | `content_pattern_compensation` | `177000` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `3` |
| posted_at | `explicit_posted_at_field` | `2025-12-01 12:43:26` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Demand Generation Lead

About NexHealth
Our health` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Demand Generation Lead`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Demand Generation Lead` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ✅ | `Demand Generation Lead` | Valid title |
| first_line_title | FALLBACK | ✅ | `Demand Generation Lead` | Valid title |

#### COMPANY

**Final Value:** `NexHealth`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `NexHealth` | Valid company name |
| url_company | URL_DERIVED | ✅ | `NexHealth` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `This` | Found 'Company is a' pattern |
| hinted_company | HEURISTIC | ✅ | `NexHealth` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `San Francisco, CA`
**Winning Strategy:** `content_pattern_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no location hint |
| raw_row_location | EXPLICIT_FIELD | ❌ | `` | Country-only location too generic: United States |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| **content_pattern_location** 🏆 | CONTENT_PATTERN | ✅ | `San Francisco, CA` | Matched pattern LOCATION_CITY_STATE |
| country_only_fallback_location | CUSTOM_550 | ✅ | `United States` | Country-only fallback: Country-only location accep |
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
| remote_company | CUSTOM_650 | ❌ | `` | Company 'NexHealth' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `staff`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `staff` | Explicit level field: staff -> staff |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `staff` | Level from title: 'lead' -> staff |
| content_pattern_level | CUSTOM_550 | ❌ | `` | No level pattern in content |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `177000`
**Winning Strategy:** `content_pattern_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| hinted_compensation | CUSTOM_450 | ❌ | `` | No compensation in hints |
| **content_pattern_compensation** 🏆 | CONTENT_PATTERN | ✅ | `177000` | Single compensation pattern: $177,000 |
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

**Final Value:** `2025-12-01 12:43:26`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-12-01 12:43:26` | Valid date: 2025-12-01T12:43:26 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:58.558101` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no first_published |

#### DESCRIPTION

**Final Value:** `Demand Generation Lead

About NexHealth
Our healthcare system remains frustratingly analog. When you live in a world of one-tap car rides, instant meal delivery, and unlimited streaming, why do you still have to call to schedule a doctor’s appointment and fill out a clipboard in the waiting room?
NexHealth’s mission is to accelerate innovation in healthcare by connecting patients, providers, and developers.&nbsp;We’re building the infrastructure layer for modern healthcare, connecting thousands of fragmented, on-premise, and closed EHR systems into a single, modern platform that powers software, APIs, payments, and patient experiences across the ecosystem.
- Founded: 2017
- Headquarters: San Francisco, CA
- Funding: $177M Series C&nbsp;
- Employees: 200+
- Trusted by tens of thousands of providers and hundreds of health-tech developers — forging the infrastructure layer that modern healthcare needs
About the Role
As a&nbsp; Demand Generation Lead , you will architect and own our full-funnel marketing strategy, from high-funnel brand awareness to mid-funnel education and bottom-funnel lead capture. You’ll design scalable, repeatable acquisition and expansion programs that deepen engagement and accelerate revenue — partnering closely with Sales, Customer Success, Product, and RevOps.
This is a hands-on builder role for someone who thrives in fast-paced, high-growth environments. You love using data and product signals to surface the right accounts to engage, and you know how to translate insights into personalized, multi-channel experiences. You’ll lead both strategy and execution for your first 9–12 months, with a clear path to team leadership as you “nail and scale” our growth formula.
You’ll be joining a lean, talented, and highly collaborative marketing team where your work will have meaningful, immediate impact. This role gives you the opportunity to shape and scale the future of our full-funnel marketing engine, experiment with cutting-edge AI-powered workflows, and help drive the revenue growth that fuels NexHealth’s mission. You’ll also benefit from competitive compensation, stock options, flexible work arrangements, and comprehensive health coverage—all while doing high-impact work that accelerates innovation in healthcare.
What You'll Do
- Demand Gen Strategy &amp; Ownership: Develop, execute, and optimize full-funnel demand gen strategies that drive awareness, engage prospects, and drive action across the buyer’s journey. This includes lead scoring, lifecycle management, and account-based marketing. Build nurture and lifecycle campaigns that guide prospects from first touch through activation and expansion.
- Channel &amp; Budget Management: Manage the demand gen budget across channels and campaigns, ensuring spend efficiency and alignment with goals (cost per demo performed, ROAS). Leverage our agencies that provide creative, website, and distribution support.
- Cross-Functional Collaboration: Partner with Revenue Operations, Sales, Customer Success, and Finance to align on lead definitions, pipeline goals, and performance metrics. Serve as a builder who thrives in ambiguity, drives projects from conception to execution, and shares learnings with the broader team.
- Data-Driven Experimentation &amp; Optimization: Identify bottlenecks, propose hypothesis-driven optimizations, and continually refine channel mix through testing and analysis. Create repeatable campaign playbooks that can be scaled across segments and regions.
- AI and Automation as a Force Multiplier: Leverage AI for research, analytics and reporting, campaign ideation, and prospect engagement. Partner with Revenue Operations to evolve processes, workflows, and data hygiene that enable scaling.
- Drive Measurable Pipeline Growth: Increase the number of qualified sales demos and bookings sourced from new and existing prospects while maintaining or reducing acquisition cost.
- Grow and Engage Marketable Database: Deliver month-over-month increases in the size of our engaged prospect database while improving engagement rates and converting more of our audience into MQLs and sales-qualified opportunities.
- Improve Full-Funnel Performance: Show continuous optimization across the buyer journey, from first touch to post-sale expansion, improving conversion rates at every stage.
- Establish Operational Clarity &amp; Excellence: Create clear visibility into ROI across channels and campaigns, supported by actionable insights and reporting.
- Built Scalable Systems &amp; AI- Driven Efficiency: Implement processes, reporting structures, and martech integrations that support long-term growth. Demonstrate measurable time-savings and/or performance improvements through AI-enhanced workflows.
What You'll Bring:
- 8+ years of B2B SaaS growth/demand gen experience with ownership of end-to-end demand strategies
- Strong analytical and data-driven skillset with comfort diving into data independently
- Hands-on technical experience with modern marketing tools such as HubSpot and Visual Website Optimizer
- Curiosity and practical experience incorporating AI into marketing workflows
- Strong collaboration and communication skills with the ability to turn metrics into narrative
- A builder mentality, thriving in fast-paced, ambiguous environments and excited to architect systems that scale
Benefits
- Full Medical, Dental, and Vision (up to 100% covered)
- 401K and commuter benefits
- Flexible PTO
- High-impact work that directly improves the healthcare experience for millions
Our Values
- Solve the customer’s problems, not yours
When making decisions, think from the perspective of the customer. It’s easy to make decisions that make our lives simpler, but not the customers.
- Do the things others are not willing to do
As a Nexer, always go after the hardest problems. Pursue things at the highest quality. Move at the fastest pace.&nbsp;
- Take ownership
Act like a founder. Own your roles, destinies, mistakes, behavior, and our mission. The buck stops with each of us - no blaming or excuses.
- Say what’s on your mind, with positive intent
Be direct, proactive, transparent, and frequent in your communication.
- Default trust
As a Nexer, you do not have to earn trust, trust is given to you by default. If we by default trust each other, our speed of communication, feedback, information sharing, and overall improvements will be a lot faster.
- Think in first principles
We first identify the problem and then break it down to its fundamentals before diving into solutions. We constantly ask “why” to validate our assumptions.
We are an equal opportunity employer and value diversity at our company. We do not discriminate on the basis of race, religion, color, national origin, sex, gender expression, sexual orientation, age, marital status, veteran status, or disability status. We provide reasonable accommodation for individuals with disabilities to participate in the application or interview process. Contact talent@nexhealth.com to request assistance.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Demand Generation Lead

About ` | Valid description (7022 chars, 1007 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Demand Generation Lead

About ` | Valid description (7022 chars, 1007 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Demand Generation Lead

About ` | Valid description (7022 chars, 1007 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Demand Generation Lead` |
| Company | `NexHealth` |
| Location | `San Francisco, CA` |
| Is Remote | `False` |
| Level | `staff` |
| Posted At | `1764618206000` |
| Description Words | `1007` |
| Cost (milli-cents) | `3` |
| URL | `https://boards-api.greenhouse.io/v1/boards/nexhealth/jobs/5709065004` |

**Description Preview (first 200 words):**

```
Demand Generation Lead About NexHealth Our healthcare system remains frustratingly analog. When you live in a world of one-tap car rides, instant meal delivery, and unlimited streaming, why do you still have to call to schedule a doctor’s appointment and fill out a clipboard in the waiting room? NexHealth’s mission is to accelerate innovation in healthcare by connecting patients, providers, and developers.&nbsp;We’re building the infrastructure layer for modern healthcare, connecting thousands of fragmented, on-premise, and closed EHR systems into a single, modern platform that powers software, APIs, payments, and patient experiences across the ecosystem. - Founded: 2017 - Headquarters: San Francisco, CA - Funding: $177M Series C&nbsp; - Employees: 200+ - Trusted by tens of thousands of providers and hundreds of health-tech developers — forging the infrastructure layer that modern healthcare needs About the Role As a&nbsp; Demand Generation Lead , you will architect and own our full-funnel marketing strategy, from high-funnel brand awareness to mid-funnel education and bottom-funnel lead capture. You’ll design scalable, repeatable acquisition and expansion programs that deepen engagement and accelerate revenue — partnering closely with Sales, Customer Success, Product, and RevOps. This is a hands-on builder role for someone who thrives in fast-paced, high-growth environments. You...
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
  "title": "Demand Generation Lead",
  "company": "NexHealth",
  "location": "United States",
  "description": "Demand Generation Lead\n\nAbout NexHealth\nOur healthcare system remains frustratingly analog. When you live in a world of one-tap car rides, instant meal delivery, and unlimited streaming, why do you still have to call to schedule a doctor’s appointment and fill out a clipboard in the waiting room?\nNexHealth’s mission is to accelerate innovation in healthcare by connecting patients, providers, and developers.&nbsp;We’re building the infrastructure layer for modern healthcare, connecting thousands ...",
  "url": "https://boards-api.greenhouse.io/v1/boards/nexhealth/jobs/5709065004",
  "apply_url": "https://boards.greenhouse.io/nexhealth/jobs/5709065004",
  "posted_at": 1764618206000,
  "level": "staff",
  "remote": false,
  "cost_milli_cents": 3,
  "_full_description_word_count": 1007
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
