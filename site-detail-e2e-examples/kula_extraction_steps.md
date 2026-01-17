# Extraction Steps: kula

**Detail URL:** `https://careers.kula.ai/voltagepark/19780`
**Source URL:** `https://careers.kula.ai/voltagepark/19780`
**Handler:** `kula`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Data Center Site Operations Manager - Voltage Park
[
Return to jobs list
](https://careers.kula.ai/voltagepark)
![Job Details Logo](https://assets.kula.ai/images/career/6kirob1z7khxgvfsuezvaeyzn3ub)
Data Center Site Operations Manager
Job type: Full Time · Department: Operations · Work type: On-Site · USD 110000 -
130000 / year
Lisle, Illinois, United States
Apply for this position
Voltage Park is your enterprise AI factory. We offer scalable compute power, on-demand and reserved bare metal AI infrastructure using NVIDIA GPUs, with world-class service, performance, and value. Founded with the mission of making accessible AI computing for all, our flexible, affordable GPU solutions power everyone from builders to enterprises.
As part of this effort, we’re hiring a **Data Center Site Operations Manager** to lead operations at our new Chicago facility. You’ll oversee the tenant fit-out of our space, stand up our operational environment, and build a high-performance team responsible for day-to-day data center operations. This is a hands-on leadership role, where you’ll both guide a team and roll up your sleeves to ensure a smooth deployment and sustained operational excellence.
This is an on-site role in Lisle, Illinois. Travel to our other data center sites across the US will be required.We are not able to provide visa sponsorship for this position at this time.
### **What You Will Do**
#### **Launch and Operate the Chicago Facility**
* Oversee the setup of data center systems specific to our deployment within an existing colocation facility
* Coordinate equipment installation, including rack and roll delivery, cabling, network installation, and power-up procedures
* Manage vendor relationships, scheduling, and performance to ensure successful deployment
* Track and manage all inventory and incoming hardware
#### **Onsite Operational Leadership**
* Lead a team of data center technicians handling maintenance, break fix, diagnostics, and routine inspections
* Develop and implement procedures for uptime, preventive maintenance, and incident response
* Act as the primary on-site point of contact for operational issues and escalation
#### **Cross-Functional and Team Collaboration**
* Partner with NetOps, Engineering, and Supply Chain teams to support deployments and ongoing operations
* Participate in hiring, training, and mentoring on-site technicians
* Travel to other Voltage Park data centers to learn processes and align operational standards
### **Who You Are**
#### **Required Qualifications**
* 4+ years of experience in data center operations
* 2+ years of leadership experience
* Strong understanding of data center infrastructure (racking, cabling, cooling basics, power distribution, network hardware installations)
* Ability to lead technicians through break/fix, installation, troubleshooting, and maintenance workflows
* Troubleshooting skills and ability to respond quickly to operational issues
* Strong vendor management experience (scheduling, acc

... (truncated, 5789 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `kula`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `Data Center Site Operations Manager - Voltage Park`

Normalized markdown after handler processing:

```markdown
Data Center Site Operations Manager - Voltage Park
[
Return to jobs list
](https://careers.kula.ai/voltagepark)
![Job Details Logo](https://assets.kula.ai/images/career/6kirob1z7khxgvfsuezvaeyzn3ub)
Data Center Site Operations Manager
Job type: Full Time · Department: Operations · Work type: On-Site · USD 110000 -
130000 / year
Lisle, Illinois, United States
Apply for this position
Voltage Park is your enterprise AI factory. We offer scalable compute power, on-demand and reserved bare metal AI infrastructure using NVIDIA GPUs, with world-class service, performance, and value. Founded with the mission of making accessible AI computing for all, our flexible, affordable GPU solutions power everyone from builders to enterprises.
As part of this effort, we’re hiring a **Data Center Site Operations Manager** to lead operations at our new Chicago facility. You’ll oversee the tenant fit-out of our space, stand up our operational environment, and build a high-performance team responsible for day-to-day data center operations. This is a hands-on leadership role, where you’ll both guide a team and roll up your sleeves to ensure a smooth deployment and sustained operational excellence.
This is an on-site role in Lisle, Illinois. Travel to our other data center sites across the US will be required.We are not able to provide visa sponsorship for this position at this time.
### **What You Will Do**
#### **Launch and Operate the Chicago Facility**
* Oversee the setup of data center systems specific to our deployment within an existing colocation facility
* Coordinate equipment installation, including rack and roll delivery, cabling, network installation, and power-up procedures
* Manage vendor relationships, scheduling, and performance to ensure successful deployment
* Track and manage all inventory and incoming hardware
#### **Onsite Operational Leadership**
* Lead a team of data center technicians handling maintenance, break fix, diagnostics, and routine inspections
* Develop and

... (truncated, 4369 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: kula

```json
{
  "url": "https://careers.kula.ai/voltagepark/19780",
  "handler": "kula"
}
```

### Raw Content Capture

Captured 5789 chars of commonmark content

```json
{
  "length": 5789,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='Data Center Site Operations Manager - Voltage Park', 4369 chars of normalized content

```json
{
  "title": "Data Center Site Operations Manager - Voltage Park",
  "normalized_length": 4369
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://careers.kula.ai/voltagepark/19780",
      "sourceUrl": "https://careers.kula.ai/voltagepark/19780",
      "provider": "spidercloud",
      "siteId": "kula",
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
    "value": "Data Center Site Operations Manager - Voltage Park"
  },
  "company": {
    "winner": "site_handler_company",
    "value": "Voltage Park"
  },
  "location": {
    "winner": "site_handler_location_hint",
    "value": "Lisle, Illinois, United States"
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
    "winner": "unknown_compensation",
    "value": 0
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 150
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:33:15.048000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Data Center Site Operations Manager - Voltage Park\nReturn to jobs list\n](https://careers.kula.ai/voltagepark)\n![Job Details Logo](https://assets.kula.ai/images/career/6kirob1z7khxgvfsuezvaeyzn3ub)\nData Center Site Operations Manager\nJob type: Full Time · Department: Operations · Work type: On-Site · USD 110000 -\n130000 / year\nLisle, Illinois, United States\nApply for this position\nVoltage Park is your enterprise AI factory. We offer scalable compute power, on-demand and reserved bare metal AI infrastructure using NVIDIA GPUs, with world-class service, performance, and value. Founded with t
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Data Center Site Operations Manager - Voltage Park`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599195184,
  "heuristicVersion": 5,
  "location": "Lisle, Illinois, United States",
  "locationSearch": "Lisle, Illinois, United States"
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
| title | `site_handler_title` | `Data Center Site Operations Manager - Voltage Park` |
| company | `site_handler_company` | `Voltage Park` |
| location | `site_handler_location_hint` | `Lisle, Illinois, United States` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `senior` |
| compensation | `unknown_compensation` | `(none)` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `150` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:33:15.048000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Data Center Site Operations Manager - Voltage Park` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Data Center Site Operations Manager - Voltage Park`
**Winning Strategy:** `site_handler_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_title** 🏆 | SITE_HANDLER | ✅ | `Data Center Site Operations Ma` | Valid title |
| raw_row_title | EXPLICIT_FIELD | ✅ | `Data Center Site Operations Ma` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `**What You Will Do**` | Valid title |
| hinted_title | HEURISTIC | ✅ | `**Onsite Operational Leadershi` | Valid title |
| first_line_title | FALLBACK | ✅ | `Data Center Site Operations Ma` | Valid title |

#### COMPANY

**Final Value:** `Voltage Park`
**Winning Strategy:** `site_handler_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_company** 🏆 | SITE_HANDLER | ✅ | `Voltage Park` | Valid company name |
| raw_row_company | EXPLICIT_FIELD | ✅ | `Voltage Park` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Kula` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `This` | Found 'Company is a' pattern |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Lisle, Illinois, United States`
**Winning Strategy:** `site_handler_location_hint`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_location_hint** 🏆 | SITE_HANDLER | ✅ | `Lisle, Illinois, United States` | Valid location |
| raw_row_location | EXPLICIT_FIELD | ✅ | `Lisle, Illinois, United States` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `Lisle, Illinois` | Matched pattern LOCATION_FULL |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Lisle, Illinois, United States` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `Chicago, IL` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Lisle, Illinois, United States' present  |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Voltage Park' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `senior`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `senior` | Explicit level field: senior -> senior |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `senior` | Level from title: 'manager' -> senior |
| content_pattern_level | CUSTOM_550 | ✅ | `mid` | Level from experience: 4+ years -> mid |
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

**Final Value:** `150`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `150` | Valid cost: 150 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:33:15.048000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:33:15.048000` | Valid date: 2026-01-16T14:33:15.048000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'kula' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:15.226096` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'kula' returned no first_published |

#### DESCRIPTION

**Final Value:** `Data Center Site Operations Manager - Voltage Park
Return to jobs list
](https://careers.kula.ai/voltagepark)
![Job Details Logo](https://assets.kula.ai/images/career/6kirob1z7khxgvfsuezvaeyzn3ub)
Data Center Site Operations Manager
Job type: Full Time · Department: Operations · Work type: On-Site · USD 110000 -
130000 / year
Lisle, Illinois, United States
Apply for this position
Voltage Park is your enterprise AI factory. We offer scalable compute power, on-demand and reserved bare metal AI infrastructure using NVIDIA GPUs, with world-class service, performance, and value. Founded with the mission of making accessible AI computing for all, our flexible, affordable GPU solutions power everyone from builders to enterprises.
As part of this effort, we’re hiring a **Data Center Site Operations Manager** to lead operations at our new Chicago facility. You’ll oversee the tenant fit-out of our space, stand up our operational environment, and build a high-performance team responsible for day-to-day data center operations. This is a hands-on leadership role, where you’ll both guide a team and roll up your sleeves to ensure a smooth deployment and sustained operational excellence.
This is an on-site role in Lisle, Illinois. Travel to our other data center sites across the US will be required.We are not able to provide visa sponsorship for this position at this time.
### **What You Will Do**
#### **Launch and Operate the Chicago Facility**
* Oversee the setup of data center systems specific to our deployment within an existing colocation facility
* Coordinate equipment installation, including rack and roll delivery, cabling, network installation, and power-up procedures
* Manage vendor relationships, scheduling, and performance to ensure successful deployment
* Track and manage all inventory and incoming hardware
#### **Onsite Operational Leadership**
* Lead a team of data center technicians handling maintenance, break fix, diagnostics, and routine inspections
* Develop and implement procedures for uptime, preventive maintenance, and incident response
* Act as the primary on-site point of contact for operational issues and escalation
#### **Cross-Functional and Team Collaboration**
* Partner with NetOps, Engineering, and Supply Chain teams to support deployments and ongoing operations
* Participate in hiring, training, and mentoring on-site technicians
* Travel to other Voltage Park data centers to learn processes and align operational standards
### **Who You Are**
#### **Required Qualifications**
* 4+ years of experience in data center operations
* 2+ years of leadership experience
* Strong understanding of data center infrastructure (racking, cabling, cooling basics, power distribution, network hardware installations)
* Ability to lead technicians through break/fix, installation, troubleshooting, and maintenance workflows
* Troubleshooting skills and ability to respond quickly to operational issues
* Strong vendor management experience (scheduling, accountability, performance tracking)
* Detail-oriented, organized, and thrives working in fast-moving environments
* Ability to manage a team responsible for supporting a 24/7 data center environment
* Ability to meet physical requirements, including lifting up to 50 lbs
#### **Ideal Experience**
* Experience with data center tenant fit-outs
* Experience in high-density or hyperscale environments
* Relevant certifications such as CompTIA, Cisco, Juniper, or similar
### **Our Culture**
* You enjoy working with a small group of friendly, highly motivated, high execution colleagues
* You’re comfortable with a high degree of autonomy. We expect you to independently prioritize your work and understand how it maps to the overall needs and goals of the company
* You’re knowledgeable in your domain but also enjoy wearing multiple hats and venturing outside of your comfort zone when the need arises
*Voltage Park is an equal opportunity employer and makes employment decisions on the basis of merit. All qualified applicants will receive consideration for employment without regard to race, color, religion, sex, sexual orientation, gender identity, national origin, disability, protected veteran status, or any other characteristic under federal, state, or local law. If you require an accommodation during the job application process, please notify your recruiter.*`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Data Center Site Operations Ma` | Valid description (4367 chars, 625 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Data Center Site Operations Ma` | Valid description (4367 chars, 625 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Data Center Site Operations Ma` | Valid description (4367 chars, 625 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Data Center Site Operations Manager - Voltage Park` |
| Company | `Voltage Park` |
| Location | `Lisle, Illinois, United States` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1768599195048` |
| Description Words | `625` |
| Cost (milli-cents) | `150` |
| URL | `https://careers.kula.ai/voltagepark/19780` |

**Description Preview (first 200 words):**

```
Data Center Site Operations Manager - Voltage Park Return to jobs list ](https://careers.kula.ai/voltagepark) ![Job Details Logo](https://assets.kula.ai/images/career/6kirob1z7khxgvfsuezvaeyzn3ub) Data Center Site Operations Manager Job type: Full Time · Department: Operations · Work type: On-Site · USD 110000 - 130000 / year Lisle, Illinois, United States Apply for this position Voltage Park is your enterprise AI factory. We offer scalable compute power, on-demand and reserved bare metal AI infrastructure using NVIDIA GPUs, with world-class service, performance, and value. Founded with the mission of making accessible AI computing for all, our flexible, affordable GPU solutions power everyone from builders to enterprises. As part of this effort, we’re hiring a **Data Center Site Operations Manager** to lead operations at our new Chicago facility. You’ll oversee the tenant fit-out of our space, stand up our operational environment, and build a high-performance team responsible for day-to-day data center operations. This is a hands-on leadership role, where you’ll both guide a team and roll up your sleeves to ensure a smooth deployment and sustained operational excellence. This is an on-site role in Lisle, Illinois. Travel to our other data center sites across the US will be required.We are not able to provide visa sponsorship for this position at...
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
  "title": "Data Center Site Operations Manager - Voltage Park",
  "company": "Voltage Park",
  "location": "Lisle, Illinois, United States",
  "description": "Data Center Site Operations Manager - Voltage Park\nReturn to jobs list\n](https://careers.kula.ai/voltagepark)\n![Job Details Logo](https://assets.kula.ai/images/career/6kirob1z7khxgvfsuezvaeyzn3ub)\nData Center Site Operations Manager\nJob type: Full Time · Department: Operations · Work type: On-Site · USD 110000 -\n130000 / year\nLisle, Illinois, United States\nApply for this position\nVoltage Park is your enterprise AI factory. We offer scalable compute power, on-demand and reserved bare metal AI inf...",
  "url": "https://careers.kula.ai/voltagepark/19780",
  "posted_at": 1768599195048,
  "level": "senior",
  "remote": false,
  "cost_milli_cents": 150,
  "_full_description_word_count": 625
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 150,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
