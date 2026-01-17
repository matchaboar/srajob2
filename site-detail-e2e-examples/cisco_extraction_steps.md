# Extraction Steps: cisco

**Detail URL:** `https://careers.cisco.com/global/en/job/2000598/Systems-Architect-Defence`
**Source URL:** `https://careers.cisco.com/global/en/job/2000598/Systems-Architect-Defence`
**Handler:** `cisco_careers`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Systems Architect - Defence in Dusseldorf, Germany | Sales - Cisco Careers
Job
-
# Systems Architect - Defence
Available in 9 locationsAvailable in 9 locations
## Systems Architect - Defence
Available in 9 locations
Dusseldorf, Germany
Berlin, Berlin, Germany
Cologne, North Rhine-Westphalia, Germany
Eschborn, Hesse, Germany
Frankfurt, Germany
Garching, Germany
Hamburg, Hamburg, Germany
Munich, Bavaria, Germany
Stuttgart, Baden-Wurttemberg, Germany
Category Sales
Job ID 2000598
Hybrid
Save job
[Apply Now](https://careers.cisco.com/global/en/hvhapply?jobSeqNo=CISCISGLOBAL2000598EXTERNALENGLOBAL)
Share
Share via Facebook
Share via twitter
Share via linkedin
Share via Email
![](https://pp-cdn.phenompeople.com/CareerConnectResources/migration/GLOBAL/images/1653217174174_jd-banner-1549291468959s.jpg)
## Job Description
**Meet The Team**
You’ll join a collaborative, cross-functional team covering Federal and Defense customers in the German public sector. Reporting to the Senior SE Manager, you will work alongside Customer Systems Engineers and broader Cisco teams. You will foster innovation, drive solution adoption, and deliver lasting value to customers who are often thought leaders within their sectors.
**Your Impact**
As a Portfolio Systems Architect (Public Sector), you’ll be a key technical advisor and architect for large Federal and Defense accounts in Germany. You’ll drive the adoption of Cisco’s latest portfolio—networking, security, (cloud), automation, and collaboration—by designing innovative solutions tailored to the unique challenges of the public sector. Acting as a trusted advisor, you’ll shape the technical strategy for your accounts, accelerate digital transformation, and ensure the highest value from Cisco’s offerings.
Key Responsibilities
* Partner with Account Teams to develop and execute customer-centric strategies that align technology solutions to business goals
* Proactively identify and create new business opportunities through direct customer engagement, workshops, and industry events
* Analyze customer environments and translate business drivers and technical requirements into tailored solution architectures
* Present and demonstrate Cisco’s latest solutions, articulating unique business value for public sector organizations
* Collaborate closely with internal teams (Sales, Architecture, CX, Engineering, Services, …) and external partners to deliver integrated, future-ready solutions
* Facilitate deal progression by mobilizing the right resources and accelerating the sales cycle
* Maintain deep knowledge of industry trends, emerging technologies, and the competitive landscape to advise customers and inform go-to-market strategies
* Build and maintain strong relationships with key stakeholders, establishing credibility through technical expertise and strategic insight.
**Minimum Qualifications**
* Experience as a Systems Architect or similar technical role in the networking or IT industry
* Strong expertise in Cisco networking 

... (truncated, 8089 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `cisco_careers`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `Systems Architect - Defence`

Normalized markdown after handler processing:

```markdown
## Job Description
**Meet The Team**
You’ll join a collaborative, cross-functional team covering Federal and Defense customers in the German public sector. Reporting to the Senior SE Manager, you will work alongside Customer Systems Engineers and broader Cisco teams. You will foster innovation, drive solution adoption, and deliver lasting value to customers who are often thought leaders within their sectors.
**Your Impact**
As a Portfolio Systems Architect (Public Sector), you’ll be a key technical advisor and architect for large Federal and Defense accounts in Germany. You’ll drive the adoption of Cisco’s latest portfolio—networking, security, (cloud), automation, and collaboration—by designing innovative solutions tailored to the unique challenges of the public sector. Acting as a trusted advisor, you’ll shape the technical strategy for your accounts, accelerate digital transformation, and ensure the highest value from Cisco’s offerings.
Key Responsibilities
* Partner with Account Teams to develop and execute customer-centric strategies that align technology solutions to business goals
* Proactively identify and create new business opportunities through direct customer engagement, workshops, and industry events
* Analyze customer environments and translate business drivers and technical requirements into tailored solution architectures
* Present and demonstrate Cisco’s latest solutions, articulating unique business value for public sector organizations
* Collaborate closely with internal teams (Sales, Architecture, CX, Engineering, Services, …) and external partners to deliver integrated, future-ready solutions
* Facilitate deal progression by mobilizing the right resources and accelerating the sales cycle
* Maintain deep knowledge of industry trends, emerging technologies, and the competitive landscape to advise customers and inform go-to-market strategies
* Build and maintain strong relationships with key stakeholders, establishing credibility through technical 

... (truncated, 4096 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: cisco_careers

```json
{
  "url": "https://careers.cisco.com/global/en/job/2000598/Systems-Architect-Defence",
  "handler": "cisco_careers"
}
```

### Raw Content Capture

Captured 8089 chars of commonmark content

```json
{
  "length": 8089,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='Systems Architect - Defence', 4096 chars of normalized content

```json
{
  "title": "Systems Architect - Defence",
  "normalized_length": 4096
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://careers.cisco.com/global/en/job/2000598/Systems-Architect-Defence",
      "sourceUrl": "https://careers.cisco.com/global/en/job/2000598/Systems-Architect-Defence",
      "provider": "spidercloud",
      "siteId": "cisco",
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
    "value": "Why Cisco?"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Cisco"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Dusseldorf, Germany"
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
    "value": 105
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2025-12-26T08:00:00"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "**Meet The Team**\nYou’ll join a collaborative, cross-functional team covering Federal and Defense customers in the German public sector. Reporting to the Senior SE Manager, you will work alongside Customer Systems Engineers and broader Cisco teams. You will foster innovation, drive solution adoption, and deliver lasting value to customers who are often thought leaders within their sectors.\n**Your Impact**\nAs a Portfolio Systems Architect (Public Sector), you’ll be a key technical advisor and architect for large Federal and Defense accounts in Germany. You’ll drive the adoption of Cisco’s latest portfolio—networking, security, (cloud), automation, and collaboration—by designing innov
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Systems Architect - Defence`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599235968,
  "heuristicVersion": 5,
  "location": "Dusseldorf, Germany",
  "locationSearch": "Dusseldorf, Germany",
  "description": "**Meet The Team**\nYou’ll join a collaborative, cross-functional team covering Federal and Defense customers in the German public sector. Reporting to the Senior SE Manager, you will work alongside Customer Systems Engineers and broader Cisco teams. You will foster innovation, drive solution adoption, and deliver lasting value to customers who are often thought leaders within their sectors.\n**Your Impact**\nAs a Portfolio Systems Architect (Public Sector), you’ll be a key technical advisor and architect for large Federal and Defense accounts in Germany. You’ll drive the adoption of Cisco’s latest portfolio—networking, security, (cloud), automation, and collaboration—by designing innovative solutions tailored to the unique challenges of the public sector. Acting as a trusted advisor, you’ll shape th
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `site_handler_title` | `Why Cisco?` |
| company | `raw_row_company` | `Cisco` |
| location | `raw_row_location` | `Dusseldorf, Germany` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `default_level` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `105` |
| posted_at | `explicit_posted_at_field` | `2025-12-26 08:00:00` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `**Meet The Team**
You’ll join a collaborative, cro` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Why Cisco?`
**Winning Strategy:** `site_handler_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_title** 🏆 | SITE_HANDLER | ✅ | `Why Cisco?` | Valid title |
| raw_row_title | EXPLICIT_FIELD | ✅ | `Systems Architect - Defence` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | Generic title rejected: Job Description |
| hinted_title | HEURISTIC | ❌ | `` | No title in hints |
| first_line_title | FALLBACK | ✅ | `Key Responsibilities` | Valid title |

#### COMPANY

**Final Value:** `Cisco`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'cisco_careers' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Cisco` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Cisco` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ❌ | `` | No company pattern matched |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Dusseldorf, Germany`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'cisco_careers' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Dusseldorf, Germany` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `Sales, Architecture` | Matched pattern LOCATION_FULL |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Dusseldorf, Germany` | Country-only fallback: Valid location |
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
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Dusseldorf, Germany' present but not inf |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | No remote in hints |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Cisco' not in remote company list |
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

**Final Value:** `105`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `105` | Valid cost: 105 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2025-12-26 08:00:00`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-12-26 08:00:00` | Valid date: 2025-12-26T08:00:00 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'cisco_careers' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:55.981152` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'cisco_careers' returned no first_publishe |

#### DESCRIPTION

**Final Value:** `**Meet The Team**
You’ll join a collaborative, cross-functional team covering Federal and Defense customers in the German public sector. Reporting to the Senior SE Manager, you will work alongside Customer Systems Engineers and broader Cisco teams. You will foster innovation, drive solution adoption, and deliver lasting value to customers who are often thought leaders within their sectors.
**Your Impact**
As a Portfolio Systems Architect (Public Sector), you’ll be a key technical advisor and architect for large Federal and Defense accounts in Germany. You’ll drive the adoption of Cisco’s latest portfolio—networking, security, (cloud), automation, and collaboration—by designing innovative solutions tailored to the unique challenges of the public sector. Acting as a trusted advisor, you’ll shape the technical strategy for your accounts, accelerate digital transformation, and ensure the highest value from Cisco’s offerings.
Key Responsibilities
* Partner with Account Teams to develop and execute customer-centric strategies that align technology solutions to business goals
* Proactively identify and create new business opportunities through direct customer engagement, workshops, and industry events
* Analyze customer environments and translate business drivers and technical requirements into tailored solution architectures
* Present and demonstrate Cisco’s latest solutions, articulating unique business value for public sector organizations
* Collaborate closely with internal teams (Sales, Architecture, CX, Engineering, Services, …) and external partners to deliver integrated, future-ready solutions
* Facilitate deal progression by mobilizing the right resources and accelerating the sales cycle
* Maintain deep knowledge of industry trends, emerging technologies, and the competitive landscape to advise customers and inform go-to-market strategies
* Build and maintain strong relationships with key stakeholders, establishing credibility through technical expertise and strategic insight.
**Minimum Qualifications**
* Experience as a Systems Architect or similar technical role in the networking or IT industry
* Strong expertise in Cisco networking (wireless, switching, routing), software-defined networking (SDN), and automation
* Proficiency with SASE architectures and cloud technologies (IaaS, SaaS, PaaS); experience designing secure, scalable hybrid environments
* Demonstrated success leading complex, large-scale projects involving multiple technologies and integration points
* Fluent in German and English: excellent communication and presentation skills, with the ability to influence technical and business audiences
**Preferred Qualifications**
* Bachelor’s or Master’s degree in Computer Science, Engineering, or related field (or equivalent professional experience)
* In-depth knowledge of Cisco’s portfolio, strategic direction, and industry best practices
* Cisco certifications (CCNP, CCDP, CCIE) highly desirable
* Hands-on experience with network automation and programmability (e.g., Python, APIs, infrastructure as code)
* Strong consultative sales skills, with a focus on understanding customer needs, industry trends, and emerging technologies
* Outstanding relationship-building, organizational, and multitasking abilities
# Why Cisco?
At Cisco, we’re revolutionizing how data and infrastructure connect and protect organizations in the AI era – and beyond. We’ve been innovating fearlessly for 40 years to create solutions that power how humans and technology work together across the physical and digital worlds. These solutions provide customers with unparalleled security, visibility, and insights across the entire digital footprint.
Fueled by the depth and breadth of our technology, we experiment and create meaningful solutions. Add to that our worldwide network of doers and experts, and you’ll see that the opportunities to grow and build are limitless. We work as a team, collaborating with empathy to make really big things happen on a global scale. Because our solutions are everywhere, our impact is everywhere.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `**Meet The Team**
You’ll join ` | Valid description (4077 chars, 551 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `## Job Description
**Meet The ` | Valid description (4096 chars, 554 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `## Job Description
**Meet The ` | Valid description (4096 chars, 554 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Systems Architect - Defence` |
| Company | `Cisco` |
| Location | `Dusseldorf, Germany` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1766761200000` |
| Description Words | `551` |
| Cost (milli-cents) | `105` |
| URL | `https://careers.cisco.com/global/en/job/2000598/Systems-Architect-Defence` |

**Description Preview (first 200 words):**

```
**Meet The Team** You’ll join a collaborative, cross-functional team covering Federal and Defense customers in the German public sector. Reporting to the Senior SE Manager, you will work alongside Customer Systems Engineers and broader Cisco teams. You will foster innovation, drive solution adoption, and deliver lasting value to customers who are often thought leaders within their sectors. **Your Impact** As a Portfolio Systems Architect (Public Sector), you’ll be a key technical advisor and architect for large Federal and Defense accounts in Germany. You’ll drive the adoption of Cisco’s latest portfolio—networking, security, (cloud), automation, and collaboration—by designing innovative solutions tailored to the unique challenges of the public sector. Acting as a trusted advisor, you’ll shape the technical strategy for your accounts, accelerate digital transformation, and ensure the highest value from Cisco’s offerings. Key Responsibilities * Partner with Account Teams to develop and execute customer-centric strategies that align technology solutions to business goals * Proactively identify and create new business opportunities through direct customer engagement, workshops, and industry events * Analyze customer environments and translate business drivers and technical requirements into tailored solution architectures * Present and demonstrate Cisco’s latest solutions, articulating unique business value for public sector organizations * Collaborate closely...
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
  "title": "Systems Architect - Defence",
  "company": "Cisco",
  "location": "Dusseldorf, Germany",
  "description": "## Job Description\n**Meet The Team**\nYou’ll join a collaborative, cross-functional team covering Federal and Defense customers in the German public sector. Reporting to the Senior SE Manager, you will work alongside Customer Systems Engineers and broader Cisco teams. You will foster innovation, drive solution adoption, and deliver lasting value to customers who are often thought leaders within their sectors.\n**Your Impact**\nAs a Portfolio Systems Architect (Public Sector), you’ll be a key techni...",
  "url": "https://careers.cisco.com/global/en/job/2000598/Systems-Architect-Defence",
  "posted_at": 1766761200000,
  "level": "mid",
  "remote": false,
  "cost_milli_cents": 105,
  "_full_description_word_count": 554
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 105,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
