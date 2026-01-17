# Extraction Steps: paloalto

**Detail URL:** `https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520`
**Source URL:** `https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520`
**Handler:** `paloalto_networks`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Software Engineering Manager (NetSec) at Palo Alto Networks
We use cookies and other tracking technologies to support navigation, to improve our products and services, to support our marketing activities and to provide content from third parties.To manage your preferences, select "Manage Settings" or choose "Accept" to consent to the use of Cookies.
AcceptManage Settings
* [Saved Jobs
(0)
](https://jobs.paloaltonetworks.com/en/saved-jobs)
* [Job Alerts](#section28)
* EN
* [Deutsch
(German)
](https://jobs.paloaltonetworks.com/de/stellenbeschreibung/santa-clara/software-engineering-manager-netsec/47263/89086617520)
* [English
(English)
](https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520)
* [日本語
(Japanese)
](https://jobs.paloaltonetworks.com/ja/%E3%82%B8%E3%83%A7%E3%83%96/santa-clara/software-engineering-manager-netsec/47263/89086617520)
[
![homepage](https://tbcdn.talentbrew.com/company/47263/v1_0/images/icons/pan-logo-header-m.svg)
](https://jobs.paloaltonetworks.com/en)
Close Button
Security Awareness! Read more to learn about how we're keeping job seekers safe.
**Palo Alto Networks is on a mission to keep the digital world safe, and this extends to job seekers as well. Please be mindful of a current bad actor practice of recruiters impersonating us. If you receive an email from someone who does not have a [@paloaltonetworks.com](http://paloaltonetworks.com/) email address, please do not respond or engage. **
![Two women reviewing a computer screen](https://tbcdn.talentbrew.com/company/47263/v1_0/images/banner-job-details-m.webp)
Job Details
# Revolutionizing protection.
## Define what’s next in cybersecurity.
## Software Engineering Manager (NetSec)
Santa Clara, California, United States
Engineering
Full-time
Ref ID: JR-012570
[Apply](https://jobs.smartrecruiters.com/PaloAltoNetworks2/744000101959216-software-engineering-manager-netsec-?oga=true)
Save Job
Current Employees, [apply here](https://www.smartrecruiters.com/app/employee-portal/5f0bfc5b150d9a317eac65ea/jobs/fa9ba42d-65bc-444f-a627-705544bd1f87)
**Our Mission**
At Palo Alto Networks® everything starts and ends with our mission:
Being the cybersecurity partner of choice, protecting our digital way of life.
Our vision is a world where each day is safer and more secure than the one before. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are.
**Who We Are**
We believe collaboration thrives in person. That’s why most of our teams work from the office full time, with flexibility when it’s needed. This model supports real-time problem-solving, stronger relationships, and the kind of precision that drives great outcomes.
**Your Career**
Palo Alto Networks® is shaping the future with technology that is transforming the way people and organizations operate in the cloud, at the network edge, and e

... (truncated, 12077 total chars)
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
Software Engineering Manager (NetSec) at Palo Alto Networks
We use cookies and other tracking technologies to support navigation, to improve our products and services, to support our marketing activities and to provide content from third parties.To manage your preferences, select "Manage Settings" or choose "Accept" to consent to the use of Cookies.
AcceptManage Settings
* [Saved Jobs
(0)
](https://jobs.paloaltonetworks.com/en/saved-jobs)
* [Job Alerts](#section28)
* EN
* [Deutsch
(German)
](https://jobs.paloaltonetworks.com/de/stellenbeschreibung/santa-clara/software-engineering-manager-netsec/47263/89086617520)
* [English
(English)
](https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520)
* [日本語
(Japanese)
](https://jobs.paloaltonetworks.com/ja/%E3%82%B8%E3%83%A7%E3%83%96/santa-clara/software-engineering-manager-netsec/47263/89086617520)
[
![homepage](https://tbcdn.talentbrew.com/company/47263/v1_0/images/icons/pan-logo-header-m.svg)
](https://jobs.paloaltonetworks.com/en)
Close Button
Security Awareness! Read more to learn about how we're keeping job seekers safe.
**Palo Alto Networks is on a mission to keep the digital world safe, and this extends to job seekers as well. Please be mindful of a current bad actor practice of recruiters impersonating us. If you receive an email from someone who does not have a [@paloaltonetworks.com](http://paloaltonetworks.com/) email address, please do not respond or engage. **
![Two women reviewing a computer screen](https://tbcdn.talentbrew.com/company/47263/v1_0/images/banner-job-details-m.webp)
Job Details
# Revolutionizing protection.
## Define what’s next in cybersecurity.
## Software Engineering Manager (NetSec)
Santa Clara, California, United States
Engineering
Full-time
Ref ID: JR-012570
[Apply](https://jobs.smartrecruiters.com/PaloAltoNetworks2/744000101959216-software-engineering-manager-netsec-?oga=true)
Save Job
Current Employees, [apply here](https://www.smartrecru

... (truncated, 8784 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: paloalto_networks

```json
{
  "url": "https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520",
  "handler": "paloalto_networks"
}
```

### Raw Content Capture

Captured 12077 chars of commonmark content

```json
{
  "length": 12077,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 8784 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 8784
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520",
      "sourceUrl": "https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520",
      "provider": "spidercloud",
      "siteId": "paloalto",
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
    "value": "Software Engineering Manager (NetSec)"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Palo Alto Networks"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Santa Clara, CA, US"
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
    "value": 267500
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 39
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:33:37.732000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Our Mission\n\nAt Palo Alto Networks® everything starts and ends with our mission:\n\nBeing the cybersecurity partner of choice, protecting our digital way of life.\n Our vision is a world where each day is safer and more secure than the one before. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are.\n\nWho We Are\n\nWe believe collaboration thrives in person. That’s why most of our teams work from the office full time, with flexibility when it’s needed. This model supports real-time pr
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Software Engineering Manager (NetSec)`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599217774,
  "heuristicVersion": 5,
  "location": "Santa Clara, CA, US",
  "locationSearch": "Santa Clara, CA, US",
  "totalCompensation": 267500,
  "compensationUnknown": false,
  "compensationReason": "extractor:hinted_compensation",
  "description": "Our Mission\n\nAt Palo Alto Networks® everything starts and ends with our mission:\n\nBeing the cybersecurity partner of choice, protecting our digital way of life.\n Our vision is a world where each day is safer and more secure than the one before. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are.\n\nWho We Are\n\nWe believe collaboration thrives in person. That’s why most of our teams work from the office full time, with flexibility when it’s needed. This model supports real-time problem-solving, stronger relationships, and the kind
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `raw_row_title` | `Software Engineering Manager (NetSec)` |
| company | `raw_row_company` | `Palo Alto Networks` |
| location | `raw_row_location` | `Santa Clara, CA, US` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `senior` |
| compensation | `hinted_compensation` | `267500` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `39` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:33:37.732000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Our Mission

At Palo Alto Networks® everything sta` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Software Engineering Manager (NetSec)`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Software Engineering Manager (` | Valid title |
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
| content_pattern_company | CONTENT_PATTERN | ✅ | `Our vision` | Found 'Company is a' pattern |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Santa Clara, CA, US`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' returned no location h |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Santa Clara, CA, US` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ✅ | `Santa Clara` | Extracted from URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `PAC, DNS handling` | Matched pattern LOCATION_FULL |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Santa Clara, CA, US` | Country-only fallback: Valid location |
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
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Santa Clara, CA, US' present but not inf |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Palo Alto Networks' not in remote company |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `senior`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `senior` | Explicit level field: senior -> senior |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `senior` | Level from title: 'manager' -> senior |
| content_pattern_level | CUSTOM_550 | ✅ | `mid` | Level from experience: 3+ years -> mid |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `267500`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `267500` | Compensation from hints: $267,500 |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `165000` | Single compensation pattern: $165,000 |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `39`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `39` | Valid cost: 39 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:33:37.732000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:33:37.732000` | Valid date: 2026-01-16T14:33:37.732000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'paloalto_networks' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:37.784193` | Using current time as fallback (date unknown) |

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
 Our vision is a world where each day is safer and more secure than the one before. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are.

Who We Are

We believe collaboration thrives in person. That’s why most of our teams work from the office full time, with flexibility when it’s needed. This model supports real-time problem-solving, stronger relationships, and the kind of precision that drives great outcomes.

Your Career

Palo Alto Networks® is shaping the future with technology that is transforming the way people and organizations operate in the cloud, at the network edge, and everywhere in between. Our mission is to be the cybersecurity partner of choice, protecting our digital way of life.

We are looking for an Engineering Manager to lead the Explicit Proxy team, one of the most technically complex and cross-functional areas within Prisma Access. This role combines hands-on technical depth (L4-L7 proxy, multi-cloud environments) with managerial excellence, guiding team members who build scalable network security services running at massive cloud scale.

You’ll drive innovation at the core of our Secure Access Service Edge (SASE) solution, enabling enterprises to securely connect users to the internet, SaaS, and private applications — with scale, performance, and zero-trust principles at the foundation.

Your Impact

Lead, mentor, and grow an engineering team within Prisma Access

Partner closely with Product Managers and cross-functional groups to define priorities and build roadmaps

Align stakeholders across business units and communicate tradeoffs, risks, and execution plans with clarity

Build a culture of accountability, ownership, and continuous improvement.

Guide teams on building highly scalable cloud-native microservices.

Ensure efficient deployment, observability, and runtime stability in production environments.

Your Experience

3-4 years of experience managing a software engineering team in a large enterprise organization.

5+ years of experience as a hands-on software engineer.

Expertise in Explicit Proxy technologies: L4-L7 Proxy,  TCP/IP, SSL/TLS interception, authentication flows, PAC, DNS handling, and content filtering.

Background in network security and cloud-delivered SWG/SASE services.

Knowledge of building multi-tenant, cloud-native architectures (Kubernetes, microservices, distributed data pipelines, observability stacks)

Strong collaboration skills; able to align cross-disciplinary teams around a shared goal.

Demonstrated ability to plan, execute, and deliver roadmaps with high predictability.

Proven track record of delivering complex, distributed cloud products end-to-end.

Experience with large-scale cloud architectures (GCP, AWS, or Azure)

The Team

Our engineering team is at the core of our products – connected directly to the mission of preventing cyberattacks. We are constantly innovating – challenging the way we, and the industry, think about cybersecurity. Our engineers don’t shy away from building products to solve problems no one has pursued before.

We define the industry, instead of waiting for directions. We need individuals who feel comfortable in ambiguity, excited by the prospect of a challenge, and empowered by the unknown risks facing our everyday lives that are only enabled by a secure digital environment.

Compensation Disclosure

The compensation offered for this position will depend on qualifications, experience, and work location. For candidates who receive an offer at the posted level, the starting base salary (for non-sales roles) or base salary + commission target (for sales/commissioned roles) is expected to be between $165000/YR - $267500/YR. The offered compensation may also include restricted stock units and a bonus. A description of our employee benefits may be found here.

Our Commitment

 We’re problem solvers that take risks and challenge cybersecurity’s status quo. It’s simple: we can’t accomplish our mission without diverse teams innovating, together.

We are committed to providing reasonable accommodations for all qualified individuals with a disability. If you require assistance or accommodation due to a disability or special need, please contact us at  accommodations@paloaltonetworks.com.

Palo Alto Networks is an equal opportunity employer. We celebrate diversity in our workplace, and all qualified applicants will receive consideration for employment without regard to age, ancestry, color, family or medical care leave, gender identity or expression, genetic information, marital status, medical condition, national origin, physical or mental disability, political affiliation, protected veteran status, race, religion, sex (including pregnancy), sexual orientation, or other legally protected characteristics.

All your information will be kept confidential according to EEO guidelines.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Our Mission

At Palo Alto Netw` | Valid description (5157 chars, 730 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Our Mission

At Palo Alto Netw` | Valid description (5157 chars, 730 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Our Mission

At Palo Alto Netw` | Valid description (5157 chars, 730 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Software Engineering Manager (NetSec)` |
| Company | `Palo Alto Networks` |
| Location | `Santa Clara, CA, US` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1768599217732` |
| Description Words | `730` |
| Cost (milli-cents) | `39` |
| URL | `https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520` |

**Description Preview (first 200 words):**

```
Our Mission At Palo Alto Networks® everything starts and ends with our mission: Being the cybersecurity partner of choice, protecting our digital way of life. Our vision is a world where each day is safer and more secure than the one before. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are. Who We Are We believe collaboration thrives in person. That’s why most of our teams work from the office full time, with flexibility when it’s needed. This model supports real-time problem-solving, stronger relationships, and the kind of precision that drives great outcomes. Your Career Palo Alto Networks® is shaping the future with technology that is transforming the way people and organizations operate in the cloud, at the network edge, and everywhere in between. Our mission is to be the cybersecurity partner of choice, protecting our digital way of life. We are looking for an Engineering Manager to lead the Explicit Proxy team, one of the most technically complex and cross-functional areas within Prisma Access. This role combines hands-on technical depth (L4-L7 proxy, multi-cloud...
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
  "title": "Software Engineering Manager (NetSec)",
  "company": "Palo Alto Networks",
  "location": "Santa Clara, CA, US",
  "description": "Our Mission\n\nAt Palo Alto Networks® everything starts and ends with our mission:\n\nBeing the cybersecurity partner of choice, protecting our digital way of life.\n Our vision is a world where each day is safer and more secure than the one before. We are a company built on the foundation of challenging and disrupting the way things are done, and we’re looking for innovators who are as committed to shaping the future of cybersecurity as we are.\n\nWho We Are\n\nWe believe collaboration thrives in person...",
  "url": "https://jobs.paloaltonetworks.com/en/job/santa-clara/software-engineering-manager-netsec/47263/89086617520",
  "posted_at": 1768599217732,
  "level": "senior",
  "remote": false,
  "cost_milli_cents": 39,
  "_full_description_word_count": 730
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 39,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
