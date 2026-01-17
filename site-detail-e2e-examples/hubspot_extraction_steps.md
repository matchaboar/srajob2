# Extraction Steps: hubspot

**Detail URL:** `https://www.hubspot.com/careers/jobs/5986323`
**Source URL:** `https://www.hubspot.com/careers/jobs/5986323`
**Handler:** `hubspot_careers`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
HubSpot Careers | All Openings
Logo - Full (Color)
Emerging Talent North America Roles
[
Explore Roles
](https://app.ripplematch.com/v2/public/company/hubspot?tl=cd866a5f)
# All Open Positions
However you identify or whatever your path here, please apply if you see a position that makes your heart skip a beat. Come join us and help us build a global company where we're all proud to belong.
Careers Menu
[← Back to all openings](https://www.hubspot.com/careers/jobs/?page=1)
## Account Executive, Corporate - Benelux
### Sales
### Remote - Netherlands
**HubSpot Philosophy and Product**
We believe the world has changed. We see businesses using more kinds of software, not less, that all needs to work together. To help our customers grow better in this environment, HubSpot is evolving from an “all-in-one” suite into an “all-on-one” platform.
HubSpot is a set of tools that supports a companies customer facing teams including Marketing, Sales and Service helping them to optimize each stage of the buyer’s journey and grow their business.
[HubSpot’s flywheel philosophy ](https://www.hubspot.com/flywheel)uses the momentum of happy customers to drive referrals and repeat sales for our customers. We’ve also invested in an integrations ecosystem that helps customers do more with HubSpot and creates real value for people who adopt our suite of software.
We are actively hiring for a Corporate Account Executive; **candidates are eligible to be office, flex or remotely located in the Netherlands based on individual preference! **Please check out this article for more context: [The Future of Work at HubSpot: How We're Building a Hybrid Company.](https://www.hubspot.com/careers-blog/future-of-work-hybrid)
**Your Role at HubSpot**
As a Corporate Account Executive at HubSpot you will be engaging directly with medium to corporate sized businesses, helping them to grow. You will use proactive and inbound selling strategies to find and close new business, and increase the customer's usage of the HubSpot platform over time. You will use your knowledge of digital transformation and change management to act as a trusted advisor and business consultant to the customer, running the sales process end to end with them
**What are the responsibilities of a Corporate Account Executive?**
* Develop and be responsible for your own annual, quarterly and monthly territory business plan
* Find new prospects from both inbound and self-sourced leads
* Run qualification calls with C- level executives and department leaders
* Close both new business and install base at or above quota on a monthly cadence
* Sell through internal champions to multiple stakeholders, as well as directly to C -level
* Work collaboratively with HubSpot's marketing and technology departments to evolve our sales strategy when new features and products are introduced
* Run online and occasionally in person product demonstrations
* Answer Legal, Security and Procurement questionnaires and RFP
* Sell the full growth p

... (truncated, 10625 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `hubspot_careers`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `Account Executive, Corporate - Benelux`

Normalized markdown after handler processing:

```markdown
## Account Executive, Corporate - Benelux
### Sales
### Remote - Netherlands
**HubSpot Philosophy and Product**
We believe the world has changed. We see businesses using more kinds of software, not less, that all needs to work together. To help our customers grow better in this environment, HubSpot is evolving from an “all-in-one” suite into an “all-on-one” platform.
HubSpot is a set of tools that supports a companies customer facing teams including Marketing, Sales and Service helping them to optimize each stage of the buyer’s journey and grow their business.
[HubSpot’s flywheel philosophy ](https://www.hubspot.com/flywheel)uses the momentum of happy customers to drive referrals and repeat sales for our customers. We’ve also invested in an integrations ecosystem that helps customers do more with HubSpot and creates real value for people who adopt our suite of software.
We are actively hiring for a Corporate Account Executive; **candidates are eligible to be office, flex or remotely located in the Netherlands based on individual preference! **Please check out this article for more context: [The Future of Work at HubSpot: How We're Building a Hybrid Company.](https://www.hubspot.com/careers-blog/future-of-work-hybrid)
**Your Role at HubSpot**
As a Corporate Account Executive at HubSpot you will be engaging directly with medium to corporate sized businesses, helping them to grow. You will use proactive and inbound selling strategies to find and close new business, and increase the customer's usage of the HubSpot platform over time. You will use your knowledge of digital transformation and change management to act as a trusted advisor and business consultant to the customer, running the sales process end to end with them
**What are the responsibilities of a Corporate Account Executive?**
* Develop and be responsible for your own annual, quarterly and monthly territory business plan
* Find new prospects from both inbound and self-sourced leads
* Run qualification calls 

... (truncated, 6908 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: hubspot_careers

```json
{
  "url": "https://www.hubspot.com/careers/jobs/5986323",
  "handler": "hubspot_careers"
}
```

### Raw Content Capture

Captured 10625 chars of commonmark content

```json
{
  "length": 10625,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='Account Executive, Corporate - Benelux', 6908 chars of normalized content

```json
{
  "title": "Account Executive, Corporate - Benelux",
  "normalized_length": 6908
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://www.hubspot.com/careers/jobs/5986323",
      "sourceUrl": "https://www.hubspot.com/careers/jobs/5986323",
      "provider": "spidercloud",
      "siteId": "hubspot",
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
    "value": "Account Executive, Corporate - Benelux"
  },
  "company": {
    "winner": "site_handler_company",
    "value": "HubSpot"
  },
  "location": {
    "winner": "site_handler_location_hint",
    "value": "Remote - Netherlands"
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
    "winner": "unknown_compensation",
    "value": 0
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 141
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:34:14.104000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "## Account Executive, Corporate - Benelux\n### Sales\n### Remote - Netherlands\n**HubSpot Philosophy and Product**\nWe believe the world has changed. We see businesses using more kinds of software, not less, that all needs to work together. To help our customers grow better in this environment, HubSpot is evolving from an “all-in-one” suite into an “all-on-one” platform.\nHubSpot is a set of tools that supports a companies customer facing teams including Marketing, Sales and Service helping them to optimize each stage of the buyer’s journey and grow their business.\n[HubSpot’s flywheel philosophy ](https://www.hubspot.com/flywheel)use
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Account Executive, Corporate - Benelux`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599254138,
  "heuristicVersion": 5,
  "location": "Remote - Netherlands",
  "locationSearch": "Remote - Netherlands"
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
| title | `site_handler_title` | `Account Executive, Corporate - Benelux` |
| company | `site_handler_company` | `HubSpot` |
| location | `site_handler_location_hint` | `Remote - Netherlands` |
| remote | `explicit_remote_flag` | `True` |
| level | `default_level` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `141` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:34:14.104000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `## Account Executive, Corporate - Benelux
### Sale` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Account Executive, Corporate - Benelux`
**Winning Strategy:** `site_handler_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_title** 🏆 | SITE_HANDLER | ✅ | `Account Executive, Corporate -` | Valid title |
| raw_row_title | EXPLICIT_FIELD | ✅ | `Account Executive, Corporate -` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `Account Executive, Corporate -` | Valid title |
| hinted_title | HEURISTIC | ✅ | `Account Executive, Corporate -` | Valid title |
| first_line_title | FALLBACK | ✅ | `Account Executive, Corporate -` | Valid title |

#### COMPANY

**Final Value:** `HubSpot`
**Winning Strategy:** `site_handler_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_company** 🏆 | SITE_HANDLER | ✅ | `HubSpot` | Valid company name |
| raw_row_company | EXPLICIT_FIELD | ✅ | `HubSpot` | Valid company name |
| url_company | URL_DERIVED | ✅ | `HubSpot` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `HubSpot` | Found 'Work at Company' pattern |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Remote - Netherlands`
**Winning Strategy:** `site_handler_location_hint`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_location_hint** 🏆 | SITE_HANDLER | ✅ | `Remote - Netherlands` | Valid location |
| raw_row_location | EXPLICIT_FIELD | ✅ | `Remote - Netherlands` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Remote - Netherlands` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `Netherlands` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ✅ | `Remote` | Job marked as remote, using 'Remote' as location |

#### REMOTE

**Final Value:** `True`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `True` | Explicit boolean remote=True |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ✅ | `True` | Location contains 'remote': Remote - Netherlands |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ✅ | `True` | Content contains remote pattern at position 56 |
| hinted_remote | HEURISTIC | ✅ | `True` | Remote from hints: True |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'HubSpot' not in remote company list |
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

**Final Value:** `141`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `141` | Valid cost: 141 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:34:14.104000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:34:14.104000` | Valid date: 2026-01-16T14:34:14.104000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'hubspot_careers' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:34:14.150037` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'hubspot_careers' returned no first_publis |

#### DESCRIPTION

**Final Value:** `## Account Executive, Corporate - Benelux
### Sales
### Remote - Netherlands
**HubSpot Philosophy and Product**
We believe the world has changed. We see businesses using more kinds of software, not less, that all needs to work together. To help our customers grow better in this environment, HubSpot is evolving from an “all-in-one” suite into an “all-on-one” platform.
HubSpot is a set of tools that supports a companies customer facing teams including Marketing, Sales and Service helping them to optimize each stage of the buyer’s journey and grow their business.
[HubSpot’s flywheel philosophy ](https://www.hubspot.com/flywheel)uses the momentum of happy customers to drive referrals and repeat sales for our customers. We’ve also invested in an integrations ecosystem that helps customers do more with HubSpot and creates real value for people who adopt our suite of software.
We are actively hiring for a Corporate Account Executive; **candidates are eligible to be office, flex or remotely located in the Netherlands based on individual preference! **Please check out this article for more context: [The Future of Work at HubSpot: How We're Building a Hybrid Company.](https://www.hubspot.com/careers-blog/future-of-work-hybrid)
**Your Role at HubSpot**
As a Corporate Account Executive at HubSpot you will be engaging directly with medium to corporate sized businesses, helping them to grow. You will use proactive and inbound selling strategies to find and close new business, and increase the customer's usage of the HubSpot platform over time. You will use your knowledge of digital transformation and change management to act as a trusted advisor and business consultant to the customer, running the sales process end to end with them
**What are the responsibilities of a Corporate Account Executive?**
* Develop and be responsible for your own annual, quarterly and monthly territory business plan
* Find new prospects from both inbound and self-sourced leads
* Run qualification calls with C- level executives and department leaders
* Close both new business and install base at or above quota on a monthly cadence
* Sell through internal champions to multiple stakeholders, as well as directly to C -level
* Work collaboratively with HubSpot's marketing and technology departments to evolve our sales strategy when new features and products are introduced
* Run online and occasionally in person product demonstrations
* Answer Legal, Security and Procurement questionnaires and RFP
* Sell the full growth platform, heavily weighted towards the Enterprise license
* Liaise with internal HubSpot stakeholders such as Legal, Finance and Security to solve for the more complex contract requirements of our Corporate customers
**What are the role requirements?**
* 4+ years of**Closing**Salesexperience
* **Fluency in English &amp; Dutch**
* Unmatched consultative selling and closing skills
* Accurate forecasting and pipeline management
* Track record of being a high performer (e.g. over quota, President's Club)
* A sharp focus on your goals and a strong approach for achieving them
**Who excels in this role?**
Top performers in the Account Executive position usually have:
* Strong communication, time management and adaptability in order to be set up for success remotely
* Experience working in a high-growth, "scale up" environment
* Passion forhelping businesses grow and curiosity about the tech industry
* Humility and enthusiasm in their work
**What are some of the benefits of working at HubSpot?**
* Generous remuneration and stock units
* Interactive employee training and onboarding
* An education allowance up to €4,250 per annum
* Pension
* Health Insurance
* Life Assurance
* 20 days holidays and Flexible time off
* Amazing colleagues to learn from and enjoy company social outings, parties, and events
***Interested in learning more about our Remote Program?**Learn more [here!](https://www.hubspot.com/careers/remote)*
*We know the** [confidence gap](https://www.theatlantic.com/magazine/archive/2014/05/the-confidence-gap/359815/)**and**[ impostor syndrome](https://blog.hubspot.com/marketing/impostor-syndrome-tips)** can get in the way of meeting spectacular candidates, so please don’t hesitate to apply — we’d love to hear from you.*
***If you need accommodations or assistance due to a disability, please reach out to us [using this form](https://form.asana.com/?k=Xr9-j19kRaY5T5NjIeyx4Q&amp;d=8587152060687).***
**If you require an accommodation due to travel limitations or other reasons, please inform your recruiter during the hiring process. We are committed to supporting candidates who may need alternative arrangements**
***Massachusetts Applicants: **It is unlawful in Massachusetts to require or administer a lie detector test as a condition of employment or continued employment. An employer who violates this law shall be subject to criminal penalties and civil liability.*
***Germany Applicants:** (m/f/d) - link to HubSpot's Career Diversitypage[here](https://www.hubspot.com/careers/diversity).*
***India**** Applicants:** link to HubSpot India's equal opportunity policy [here](https://drive.google.com/file/d/1fTZ0ht2chl1WRI7Tgrbzh9ytZUhL2jG9/view?__hstc=20629287.8bedd818fefb24c6303ec98fcf9dcfff.1724281309795.1724281309795.1724281309795.1&amp;__hssc=20629287.1.1724281309796&amp;__hsfp=1818362978).*
**About HubSpot**
HubSpot (NYSE: HUBS) is an AI-powered customer platform with all the software, integrations, and resources customers need to connect marketing, sales, and service. HubSpot's connected platform enables businesses to grow faster by focusing on what matters most: customers.
At HubSpot, bold is our baseline. Our employees around the globe move fast, stay customer-obsessed, and win together. Our culture is grounded in four commitments: Solve for the Customer, Be Bold, Learn Fast, Align, Adapt &amp; Go!, and Deliver with HEART. These commitments shape how we work, lead, and grow.
We’re building a company[ where people can do their best work](https://www.hubspot.com/careers/hybrid-work). We focus on brilliant work, not badge swipes. By combining clarity, ownership, and trust, we create space for big thinking and meaningful progress. And we know that when our employees grow, our customers do too.
Explore more:
* *[HubSpot Careers](https://www.hubspot.com/careers)*
* *[Life at HubSpot on Instagram](https://www.instagram.com/lifeathubspot)*
*HubSpot may use AI to help screen or assess candidates, but all hiring decisions are always human. More information can be found [here](https://www.hubspot.com/careers/hiring-ai). By submitting your application, you agree that HubSpot may collect your personal data for recruiting, global organization planning, and related purposes. Refer to HubSpot's [Recruiting Privacy Notice](https://legal.hubspot.com/recruiting-privacy-notice) for details on data processing and your rights.*`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `## Account Executive, Corporat` | Valid description (6908 chars, 937 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `## Account Executive, Corporat` | Valid description (6908 chars, 937 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `## Account Executive, Corporat` | Valid description (6908 chars, 937 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Account Executive, Corporate - Benelux` |
| Company | `HubSpot` |
| Location | `Remote - Netherlands` |
| Is Remote | `True` |
| Level | `mid` |
| Posted At | `1768599254104` |
| Description Words | `937` |
| Cost (milli-cents) | `141` |
| URL | `https://www.hubspot.com/careers/jobs/5986323` |

**Description Preview (first 200 words):**

```
## Account Executive, Corporate - Benelux ### Sales ### Remote - Netherlands **HubSpot Philosophy and Product** We believe the world has changed. We see businesses using more kinds of software, not less, that all needs to work together. To help our customers grow better in this environment, HubSpot is evolving from an “all-in-one” suite into an “all-on-one” platform. HubSpot is a set of tools that supports a companies customer facing teams including Marketing, Sales and Service helping them to optimize each stage of the buyer’s journey and grow their business. [HubSpot’s flywheel philosophy ](https://www.hubspot.com/flywheel)uses the momentum of happy customers to drive referrals and repeat sales for our customers. We’ve also invested in an integrations ecosystem that helps customers do more with HubSpot and creates real value for people who adopt our suite of software. We are actively hiring for a Corporate Account Executive; **candidates are eligible to be office, flex or remotely located in the Netherlands based on individual preference! **Please check out this article for more context: [The Future of Work at HubSpot: How We're Building a Hybrid Company.](https://www.hubspot.com/careers-blog/future-of-work-hybrid) **Your Role at HubSpot** As a Corporate Account Executive at HubSpot you will be engaging directly with medium to corporate...
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
  "title": "Account Executive, Corporate - Benelux",
  "company": "HubSpot",
  "location": "Remote - Netherlands",
  "description": "## Account Executive, Corporate - Benelux\n### Sales\n### Remote - Netherlands\n**HubSpot Philosophy and Product**\nWe believe the world has changed. We see businesses using more kinds of software, not less, that all needs to work together. To help our customers grow better in this environment, HubSpot is evolving from an “all-in-one” suite into an “all-on-one” platform.\nHubSpot is a set of tools that supports a companies customer facing teams including Marketing, Sales and Service helping them to o...",
  "url": "https://www.hubspot.com/careers/jobs/5986323",
  "posted_at": 1768599254104,
  "level": "mid",
  "remote": true,
  "cost_milli_cents": 141,
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
  "costMilliCents": 141,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
