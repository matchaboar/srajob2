# Extraction Steps: workday

**Detail URL:** `https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-1/R-D-Engineer-Hardware_R023773`
**Source URL:** `https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-1/R-D-Engineer-Hardware_R023773`
**Handler:** `workday`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
<html><meta name="color-scheme" content="light dark"><meta charset="utf-8"><pre>{"jobPostingInfo":{"id":"607987a91e251001b7ffbf65a3050000","title":"R&amp;D Engineer Hardware","jobDescription":"&lt;p style=\"text-align:left\"&gt;&lt;span&gt;&lt;span&gt;&lt;span&gt;&lt;span class=\"emphasis\"&gt;&lt;b&gt;Please Note:&lt;/b&gt;&lt;/span&gt;&lt;/span&gt;&lt;/span&gt;&lt;/span&gt;&lt;/p&gt;&lt;p style=\"text-align:left\"&gt;&lt;span&gt;&lt;span&gt;&lt;span&gt;&lt;span class=\"emphasis\"&gt;&lt;b&gt;1...
```

---

## Step 2: Handler Detection

**Detected Handler:** `workday`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
<html><meta name="color-scheme" content="light dark"><meta charset="utf-8"><pre>{"jobPostingInfo":{"id":"607987a91e251001b7ffbf65a3050000","title":"R&amp;D Engineer Hardware","jobDescription":"&lt;p style=\"text-align:left\"&gt;&lt;span&gt;&lt;span&gt;&lt;span&gt;&lt;span class=\"emphasis\"&gt;&lt;b&gt;Please Note:&lt;/b&gt;&lt;/span&gt;&lt;/span&gt;&lt;/span&gt;&lt;/span&gt;&lt;/p&gt;&lt;p style=\"text-align:left\"&gt;&lt;span&gt;&lt;span&gt;&lt;span&gt;&lt;span class=\"emphasis\"&gt;&lt;b&gt;1...
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: workday

```json
{
  "url": "https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-1/R-D-Engineer-Hardware_R023773",
  "handler": "workday"
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
      "url": "https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-1/R-D-Engineer-Hardware_R023773",
      "sourceUrl": "https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-1/R-D-Engineer-Hardware_R023773",
      "provider": "spidercloud",
      "siteId": "workday",
      "pattern": null,
      "urlType": "detail"
    }
  ]
}
```

### Workflow Complete

Workflow returned, captured 1 scrapes, 0 ingested jobs

```json
{
  "stored_scrapes": 1,
  "ingested_jobs": 0,
  "description_uploads": 0
}
```

### Extractor Debug Trace

Ran 8 extractors with all strategies

```json
{
  "title": {
    "winner": "site_handler_title",
    "value": "R&D Engineer Hardware"
  },
  "company": {
    "winner": "site_handler_company",
    "value": "Broadcom"
  },
  "location": {
    "winner": "url_location",
    "value": "Usa Ca Irvine Alton Parkway Bldg 1"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": false
  },
  "level": {
    "winner": "explicit_level_field",
    "value": "mid"
  },
  "compensation": {
    "winner": "content_pattern_compensation",
    "value": 191600
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-13 08:00:00"
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "# R&D Engineer Hardware\n\nPlease Note:\n\n1. If you are a first time user, please create your candidate\u00a0login account before you apply for a job. (Click Sign In > Create Account)\n\n2. If you already have a Candidate Account, please Sign-In before you apply.\n\nWe are currently seeking a highly motivated engineer to join our talented Hardware Systems Team to design development platforms for the test and validation of custom mixed signal ASIC\u2019s.\n\nThe role will encompass the full development platform cycle starting from design concept based on technology requirements, package pin out feasibility studies, key component selection, schematic and layout design, fabrication and assembly, verification of the full system hardware, measurement and system evaluation of the custom ASIC\u20
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `R&D Engineer Hardware`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768382777653,
  "heuristicVersion": 5,
  "totalCompensation": 191600,
  "compensationUnknown": false,
  "compensationReason": "extractor:content_pattern_compensation",
  "description": "# R&D Engineer Hardware\n\nPlease Note:\n\n1. If you are a first time user, please create your candidate\u00a0login account before you apply for a job. (Click Sign In > Create Account)\n\n2. If you already have a Candidate Account, please Sign-In before you apply.\n\nWe are currently seeking a highly motivated engineer to join our talented Hardware Systems Team to design development platforms for the test and validation of custom mixed signal ASIC\u2019s.\n\nThe role will encompass the full development platform cycle starting from design concept based on technology requirements, package pin out feasibility studies, key component selection, schematic and layout design, fabrication and assembly, verification of the full system hardware, measurement and 
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `site_handler_title` | `R&D Engineer Hardware` |
| company | `site_handler_company` | `Broadcom` |
| location | `url_location` | `Usa Ca Irvine Alton Parkway Bldg 1` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `mid` |
| compensation | `content_pattern_compensation` | `191600` |
| posted_at | `explicit_posted_at_field` | `2026-01-13 08:00:00` |
| description | `normalized_markdown_description` | `# R&D Engineer Hardware

Please Note:

1. If you a` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `R&D Engineer Hardware`
**Winning Strategy:** `site_handler_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_title** 🏆 | SITE_HANDLER | ✅ | `R&D Engineer Hardware` | Valid title |
| raw_row_title | EXPLICIT_FIELD | ✅ | `R&D Engineer Hardware` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `R&D Engineer Hardware` | Valid title |
| hinted_title | HEURISTIC | ✅ | `R&D Engineer Hardware` | Valid title |
| first_line_title | FALLBACK | ✅ | `R&D Engineer Hardware` | Valid title |

#### COMPANY

**Final Value:** `Broadcom`
**Winning Strategy:** `site_handler_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_company** 🏆 | SITE_HANDLER | ✅ | `Broadcom` | Valid company name |
| raw_row_company | EXPLICIT_FIELD | ✅ | `Broadcom` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Broadcom` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ❌ | `` | No company pattern matched |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Usa Ca Irvine Alton Parkway Bldg 1`
**Winning Strategy:** `url_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'workday' returned no location hint |
| raw_row_location | EXPLICIT_FIELD | ❌ | `` | Placeholder location: Unknown |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| **url_location** 🏆 | URL_DERIVED | ✅ | `Usa Ca Irvine Alton Parkway Bl` | Extracted from URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| hinted_location | HEURISTIC | ❌ | `` | No location in hints |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| location_remote | EXPLICIT_FIELD | ✅ | `` | Location is specific place: Usa Ca Irvine Alton Pa |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | No remote in hints |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Broadcom' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `mid`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `mid` | Explicit level field: mid -> mid |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ❌ | `` | No level indicator in title |
| content_pattern_level | CUSTOM_550 | ❌ | `` | No level pattern in content |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `191600`
**Winning Strategy:** `content_pattern_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | Could not parse compensation: 0 |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **content_pattern_compensation** 🏆 | CONTENT_PATTERN | ✅ | `191600` | Compensation range pattern: $147,400-$235,800 -> $ |
| hinted_compensation | HEURISTIC | ✅ | `191600` | Compensation from hints: $191,600 |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### POSTED_AT

**Final Value:** `2026-01-13 08:00:00`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-13 08:00:00` | Valid date: 2026-01-13T08:00:00 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'workday' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-14 02:26:17.769445` | Using current time as fallback (date unknown) |

#### DESCRIPTION

**Final Value:** `# R&D Engineer Hardware

Please Note:

1. If you are a first time user, please create your candidate login account before you apply for a job. (Click Sign In > Create Account)

2. If you already have a Candidate Account, please Sign-In before you apply.

We are currently seeking a highly motivated engineer to join our talented Hardware Systems Team to design development platforms for the test and validation of custom mixed signal ASIC’s.

The role will encompass the full development platform cycle starting from design concept based on technology requirements, package pin out feasibility studies, key component selection, schematic and layout design, fabrication and assembly, verification of the full system hardware, measurement and system evaluation of the custom ASIC’s, debug of critical technical issues, and failure analysis of defects during high quantity production.

Job Requirements

Translate silicon validation test plans into an evaluation board PCB design specification.

Schematic capture, generate layout guidelines, and oversee layout design.

Experienced designing high speed memory and digital interfaces

Experienced with consumer product design for cost, size, and power constraints.

Experienced with low noise analog design.

Excellent troubleshooting and debug skills.

Familiar with the use of CPLD/FPGA devices in multi voltage supply designs.

Signal Integrity experience, simulation experience a plus.

Mentor DxDesigner or Cadance Allegro use

Candidates should have BSEE +15 years of relevant hardware and system experience.

Additional Job Description:

Compensation and Benefits

The annual base salary range for this position is $147,400 - $235,800

This position is also eligible for a discretionary annual bonus in accordance with relevant plan documents, and equity in accordance with equity plan documents and equity award agreements.

Broadcom offers a competitive and comprehensive benefits package: Medical, dental and vision plans, 401(K) participation including company matching, Employee Stock Purchase Program (ESPP), Employee Assistance Program (EAP), company paid holidays, paid sick leave and vacation time. The company follows all applicable laws for Paid Family Leave and other leaves of absence.

Broadcom is proud to be an equal opportunity employer.  We will consider qualified applicants without regard to race, color, creed, religion, sex, sexual orientation, national origin, citizenship, disability status, medical condition, pregnancy, protected veteran status or any other characteristic protected by federal, state, or local law.  We will also consider qualified applicants with arrest and conviction records consistent with local law.

If you are located outside USA, please be sure to fill out a home address as this will be used for future correspondence.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `# R&D Engineer Hardware

Pleas` | Valid description (2824 chars) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `# R&D Engineer Hardware

Pleas` | Valid description (2842 chars) |
| raw_markdown_description | CUSTOM_800 | ✅ | `# R&D Engineer Hardware

Pleas` | Valid description (2842 chars) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `R&D Engineer Hardware` |
| Company | `Broadcom` |
| Location | `Unknown` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1768316400000` |
| Description Words | `405` |
| Cost (milli-cents) | `1` |
| URL | `https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-1/R-D-Engineer-Hardware_R023773` |

**Description Preview (first 200 words):**

```
# R&D Engineer Hardware Please Note: 1. If you are a first time user, please create your candidate login account before you apply for a job. (Click Sign In > Create Account) 2. If you already have a Candidate Account, please Sign-In before you apply. We are currently seeking a highly motivated engineer to join our talented Hardware Systems Team to design development platforms for the test and validation of custom mixed signal ASIC’s. The role will encompass the full development platform cycle starting from design concept based on technology requirements, package pin out feasibility studies, key component selection, schematic and layout design, fabrication and assembly, verification of the full system hardware, measurement and system evaluation of the custom ASIC’s, debug of critical technical issues, and failure analysis of defects during high quantity production. Job Requirements Translate silicon validation test plans into an evaluation board PCB design specification. Schematic capture, generate layout guidelines, and oversee layout design. Experienced designing high speed memory and digital interfaces Experienced with consumer product design for cost, size, and power constraints. Experienced with low noise analog design. Excellent troubleshooting and debug skills. Familiar with the use of CPLD/FPGA devices in multi voltage supply designs. Signal...
```

---

## Step 6: Convex Mutation Payload

**Ingested Jobs Count:** 0
**Stored Scrapes Count:** 1
**Description Uploads Count:** 0

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": "https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-1/R-D-Engineer-Hardware_R023773",
  "provider": "spidercloud",
  "costMilliCents": 1,
  "items_keys": [
    "normalized",
    "normalizedCount",
    "normalizedSample",
    "page_links",
    "provider",
    "costMilliCents",
    "workflowName",
    "job_urls",
    "raw",
    "request"
  ],
  "normalized_count": 1
}
```
