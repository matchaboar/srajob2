# Extraction Steps: paloaltonetworks

**Detail URL:** `https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944`
**Source URL:** `https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944`
**Handler:** `paloalto_networks`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
(No raw markdown captured)
```

---

## Step 2: Handler Detection

**Detected Handler:** `paloalto_networks`

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

Detected handler: paloalto_networks

```json
{
  "url": "https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944",
  "handler": "paloalto_networks"
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
      "url": "https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944",
      "sourceUrl": "https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944",
      "provider": "spidercloud",
      "siteId": "paloaltonetworks",
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
    "value": "Named Account Manager - SLED"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Palo Alto Networks"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Los Angeles, CA, US"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": false
  },
  "level": {
    "winner": "title_level",
    "value": "mid"
  },
  "compensation": {
    "winner": "unknown_compensation",
    "value": 0
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 74
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:33:47.399000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Our Mission\n\nAt Palo Alto Networks® everything starts and ends with our mission:\n\nBeing the cybersecurity partner of choice, protecting our digital way of life.\n\nWe have the vision of a world where each day is safer and more secure than the one before. These aren’t easy goals to accomplish – but we’re not here for easy. We’re here for better. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are.\n\nWe’re changing the nature of work. Palo Alto Networks is evolving to meet the needs of our employees now and i
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Named Account Manager - SLED`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599227521,
  "heuristicVersion": 5,
  "locations": [
    "Los Angeles, CA"
  ],
  "location": "Los Angeles, CA",
  "locationStates": [
    "CA"
  ],
  "locationSearch": "Los Angeles CA",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "level": "mid",
  "description": "Our Mission\n\nAt Palo Alto Networks® everything starts and ends with our mission:\n\nBeing the cybersecurity partner of choice, protecting our digital way of life.\n\nWe have the vision of a world where each day is safer and more secure than the one before. These aren’t easy goals to accomplish – but we’re not here for easy. We’re here for better. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are.\n\nWe’re changing the nature of work. Palo Alto Networks is evolving to meet the needs of our empl
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `raw_row_title` | `Named Account Manager - SLED` |
| company | `raw_row_company` | `Palo Alto Networks` |
| location | `raw_row_location` | `Los Angeles, CA, US` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `title_level` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `74` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:33:47.399000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Our Mission

At Palo Alto Networks® everything sta` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Named Account Manager - SLED`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Named Account Manager - SLED` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ❌ | `` | No title in hints |
| first_line_title | FALLBACK | ✅ | `Our Mission` | Valid title |

#### COMPANY

**Final Value:** `Palo Alto Networks`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Palo Alto Networks` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Palo Alto Networks` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `The Named Account Manager - SL` | Found 'Company is a' pattern |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Los Angeles, CA, US`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' returned no location h |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Los Angeles, CA, US` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ✅ | `Los Angeles` | Extracted from URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Los Angeles, CA, US` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `Palo Alto, CA` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Los Angeles, CA, US' present but not inf |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Palo Alto Networks' not in remote company |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `mid`
**Winning Strategy:** `title_level`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_level_field | STRUCTURED_DATA | ❌ | `` | Skipping explicit senior level for account manager |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| **title_level** 🏆 | CONTENT_PATTERN | ✅ | `mid` | Account manager title maps to mid level: Named Acc |
| content_pattern_level | CUSTOM_550 | ❌ | `` | No level pattern in content |
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

**Final Value:** `74`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `74` | Valid cost: 74 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:33:47.399000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:33:47.399000` | Valid date: 2026-01-16T14:33:47.399000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:47.534247` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' returned no first_publ |

#### DESCRIPTION

**Final Value:** `Our Mission

At Palo Alto Networks® everything starts and ends with our mission:

Being the cybersecurity partner of choice, protecting our digital way of life.

We have the vision of a world where each day is safer and more secure than the one before. These aren’t easy goals to accomplish – but we’re not here for easy. We’re here for better. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are.

We’re changing the nature of work. Palo Alto Networks is evolving to meet the needs of our employees now and in the future through FLEXWORK, our approach to how we work. From benefits to learning, location to leadership, we’ve rethought and recreated every aspect of the employee experience at Palo Alto Networks.  And because it FLEXes around each individual employee based on their individual choices, employees are empowered to push boundaries and help us all evolve, together.

Your Career

The Named Account Manager - SLED is a significant driver of company revenue and growth. As an experienced and dynamic sales professional, you’re responsible for leading and driving sales engagements. You’re motivated by the desire to solve critical challenges facing our customer’s secure environment, so you’re prepared to connect them with a solution for every stage of threat prevention.

You’ll be responsible for meeting and exceeding your quota by crafting and implementing strategic territory plans targeting deployments of the Palo Alto Networks Next Generation Security Platform. This is a unique opportunity for a closer with a go-getter mentality to win business and market share by actively displacing competing technologies.

Palo Alto Networks is leading the charge in platformization, offering best-in-breed solutions that enable customers to build a truly zero-trust security architecture and navigate critical transformations. To ensure our sales team is equipped to guide customers, we've developed FLIGHT, an immersive onboarding program. Flight blends virtual and in-person learning at our headquarters, where new sales hires will participate in dynamic cohorts, fully dedicated to their training without customer distractions. This focused approach ensures they emerge as well-prepared sales professionals, ready to help customers leverage our comprehensive portfolio.

Your Impact

As a Named Account Manager,  you will drive and orchestrate complex sales cycles and work with our internal partners and teams to best serve the customer

Bring your experience and consultative selling skills to initiate long-standing relationships with prospective customers and executive sponsors

Your focus will be to create and implement strategic account plans focused on attaining enterprise-wide deployments

Understanding of the strategic competitive landscape and customer needs so you can effectively position Palo Alto Networks

Engage a programmatic approach to demand to generate, develop, and expand your territory

Leverage prospect stories to create a compelling value proposition with insights into value for that specific account.

Stay updated on industry news and trends, and how they affect Palo Alto Networks products and services

Travel as necessary within your territory, and to company-wide meetings

Your Experience

Experience and knowledge of SaaS-based architectures, ideally in a networking and/or security context

Experience cultivating mutually beneficial relationships with our channel partners to bring channel-centric go-to-market approach for our customers

Have and able to lead all aspects of the sales cycle with the ability to uncover, qualifying, developing, and closing new, white-space territories and accounts

Possess a successful track record selling complex-solutions

Excellent time management skills, and work with high levels of autonomy and self-direction

Highly competitive, ramp quickly, extremely adaptive, and pride yourself on exceeding production goals

The Team

Our sales team members work hand-in-hand with large organizations around the world to keep their digital environments protected. We educate, inspire, and empower our potential clients in their journey to security.

As part of our sales team, you are empowered with unmatched systems and tools, constantly updated research and sales libraries, and a team built on joint success. You won’t find someone at Palo Alto Networks that isn’t committed to your success – with everyone pitching in to assist when it comes to solutions selling, learning, and development. As a member of our sales team, you are motivated by a solutions-focused sales environment and find fulfillment in working with clients to resolve incredibly complex cyberthreats.

Our Commitment

We’re trailblazers that dream big, take risks, and challenge cybersecurity’s status quo. It’s simple: we can’t accomplish our mission without diverse teams innovating, together.

We are committed to providing reasonable accommodations for all qualified individuals with a disability. If you require assistance or accommodation due to a disability or special need, please contact us at accommodations@paloaltonetworks.com.

Palo Alto Networks is an equal opportunity employer. We celebrate diversity in our workplace, and all qualified applicants will receive consideration for employment without regard to age, ancestry, color, family or medical care leave, gender identity or expression, genetic information, marital status, medical condition, national origin, physical or mental disability, political affiliation, protected veteran status, race, religion, sex (including pregnancy), sexual orientation, or other legally protected characteristics.

Our Commitment

 We’re problem solvers that take risks and challenge cybersecurity’s status quo. It’s simple: we can’t accomplish our mission without diverse teams innovating, together.

We are committed to providing reasonable accommodations for all qualified individuals with a disability. If you require assistance or accommodation due to a disability or special need, please contact us at  accommodations@paloaltonetworks.com.

Palo Alto Networks is an equal opportunity employer. We celebrate diversity in our workplace, and all qualified applicants will receive consideration for employment without regard to age, ancestry, color, family or medical care leave, gender identity or expression, genetic information, marital status, medical condition, national origin, physical or mental disability, political affiliation, protected veteran status, race, religion, sex (including pregnancy), sexual orientation, or other legally protected characteristics.

All your information will be kept confidential according to EEO guidelines.

 Is role eligible for Immigration Sponsorship? No. Please note that we will not sponsor applicants for work visas for this position.

 Motor-Vehicle Requirement:
 This role may require travel to and from Palo Alto Networks, Inc. business meetings and events and requires reliable transportation to do so.  If a hire chooses to drive in connection with company business, the hire for this role must maintain a valid driver’s license.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Our Mission

At Palo Alto Netw` | Valid description (7245 chars, 1041 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Our Mission

At Palo Alto Netw` | Valid description (7245 chars, 1041 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Our Mission

At Palo Alto Netw` | Valid description (7245 chars, 1041 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Named Account Manager - SLED` |
| Company | `Palo Alto Networks` |
| Location | `Los Angeles, CA` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1768599227399` |
| Description Words | `1041` |
| Cost (milli-cents) | `74` |
| URL | `https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944` |

**Description Preview (first 200 words):**

```
Our Mission At Palo Alto Networks® everything starts and ends with our mission: Being the cybersecurity partner of choice, protecting our digital way of life. We have the vision of a world where each day is safer and more secure than the one before. These aren’t easy goals to accomplish – but we’re not here for easy. We’re here for better. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are. We’re changing the nature of work. Palo Alto Networks is evolving to meet the needs of our employees now and in the future through FLEXWORK, our approach to how we work. From benefits to learning, location to leadership, we’ve rethought and recreated every aspect of the employee experience at Palo Alto Networks. And because it FLEXes around each individual employee based on their individual choices, employees are empowered to push boundaries and help us all evolve, together. Your Career The Named Account Manager - SLED is a significant driver of company revenue and growth. As an experienced and dynamic sales professional, you’re responsible for...
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
  "title": "Named Account Manager - SLED",
  "company": "Palo Alto Networks",
  "location": "Los Angeles, CA, US",
  "description": "Our Mission\n\nAt Palo Alto Networks® everything starts and ends with our mission:\n\nBeing the cybersecurity partner of choice, protecting our digital way of life.\n\nWe have the vision of a world where each day is safer and more secure than the one before. These aren’t easy goals to accomplish – but we’re not here for easy. We’re here for better. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to sh...",
  "url": "https://jobs.paloaltonetworks.com/en/job/los-angeles/named-account-manager-sled/47263/90533764944",
  "posted_at": 1768599227399,
  "level": "senior",
  "remote": false,
  "cost_milli_cents": 74,
  "_full_description_word_count": 1041
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 74,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
