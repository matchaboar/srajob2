# Extraction Steps: docusign

**Detail URL:** `https://careers.docusign.com/jobs/27215?lang=en-us`
**Source URL:** `https://careers.docusign.com/jobs/27215?lang=en-us`
**Handler:** `docusign`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Software Engineer in Seattle, Washington | Docusign
[ Back ](<javascript: history.go(-1)>)
# Software Engineer
CAROUSEL\_PARAGRAPH
* Engineering
* Seattle, Washington
* 09/04/2025
Share Job ### Share Job
[](javascript:)![](https://cms.jibecdn.com/prod/jibeapply/assets/SOCIAL-SHARE-TWITTER-X-ICON-en-us-1695672397749.png)
## Job Description
## Company Overview
Docusign brings agreements to life. Over 1.5 million customers and more than a billion people in over 180 countries use Docusign solutions to accelerate the process of doing business and simplify people’s lives. With intelligent agreement management, Docusign unleashes business-critical data that is trapped inside of documents. Until now, these were disconnected from business systems of record, costing businesses time, money, and opportunity. Using Docusign’s Intelligent Agreement Management platform, companies can create, commit, and manage agreements with solutions created by the #1 company in e-signature and contract lifecycle management (CLM).
## What you'll do
The Backend Software Engineer will own all aspects of the software development lifecycle including design, development, testing, deployment and maintenance of product features in Docusign software products. You will primarily use the Microsoft technology stack, however we also embrace open source technologies such as Redis, Cassandra and Elasticsearch, and Azure cloud services.
You enjoy fast-paced entrepreneurial environments where you can solve difficult problems using current technologies and tools. You collaborate well with other team members when brainstorming, designing, and implementing new solutions. You will also help the team succeed by thinking about ways to improve processes, suggesting ways to make the team more effective, and mentoring and modeling engineering best practices.
This position is an individual contributor role reporting to the Senior Manager, Software Engineering.
**Responsibility**
* Design, develop, and maintain high-performance backend systems and APIs using C# and .NET technologies, hosted on Microsoft Azure and on-prem data centers
* Leverage Azure services like Azure App Services, Azure Kubernetes Service (AKS), Azure Blob Storage, and SQL/No-SQL Databases to build scalable, secure, and reliable cloud-native solutions
* Build and maintain microservices-based architectures using C#,nbsp;[ASP.NET](http://asp.net/), and other Azure technologies
* Design and implement RESTful APIs and integrate with frontend teams to ensure seamless integration
* Ensure the backend infrastructure is optimized for scalability and high availability
* Address performance bottlenecks and scalability challenges proactively
* Work as part of a cross-functional team, proactively getting alignment on designs and communicating roadblocks
* Guide and mentor other engineers through design and code reviews
## Job Designation
**Hybrid:** Employee divides their time between in-office and remote work. Access to an office location is r

... (truncated, 10248 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `docusign`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
Software Engineer in Seattle, Washington | Docusign
[ Back ](<javascript: history.go(-1)>)
# Software Engineer
CAROUSEL\_PARAGRAPH
* Engineering
* Seattle, Washington
* 09/04/2025
Share Job ### Share Job
[](javascript:)![](https://cms.jibecdn.com/prod/jibeapply/assets/SOCIAL-SHARE-TWITTER-X-ICON-en-us-1695672397749.png)
## Job Description
## Company Overview
Docusign brings agreements to life. Over 1.5 million customers and more than a billion people in over 180 countries use Docusign solutions to accelerate the process of doing business and simplify people’s lives. With intelligent agreement management, Docusign unleashes business-critical data that is trapped inside of documents. Until now, these were disconnected from business systems of record, costing businesses time, money, and opportunity. Using Docusign’s Intelligent Agreement Management platform, companies can create, commit, and manage agreements with solutions created by the #1 company in e-signature and contract lifecycle management (CLM).
## What you'll do
The Backend Software Engineer will own all aspects of the software development lifecycle including design, development, testing, deployment and maintenance of product features in Docusign software products. You will primarily use the Microsoft technology stack, however we also embrace open source technologies such as Redis, Cassandra and Elasticsearch, and Azure cloud services.
You enjoy fast-paced entrepreneurial environments where you can solve difficult problems using current technologies and tools. You collaborate well with other team members when brainstorming, designing, and implementing new solutions. You will also help the team succeed by thinking about ways to improve processes, suggesting ways to make the team more effective, and mentoring and modeling engineering best practices.
This position is an individual contributor role reporting to the Senior Manager, Software Engineering.
**Responsibility**
* Design, develop, and maintain high-perfo

... (truncated, 10248 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: docusign

```json
{
  "url": "https://careers.docusign.com/jobs/27215?lang=en-us",
  "handler": "docusign"
}
```

### Raw Content Capture

Captured 10248 chars of commonmark content

```json
{
  "length": 10248,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 10248 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 10248
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://careers.docusign.com/jobs/27215?lang=en-us",
      "sourceUrl": "https://careers.docusign.com/jobs/27215?lang=en-us",
      "provider": "spidercloud",
      "siteId": "docusign",
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
    "value": "Software Engineer"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Docusign"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Seattle, WA"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": false
  },
  "level": {
    "winner": "content_pattern_level",
    "value": "mid"
  },
  "compensation": {
    "winner": "hinted_compensation",
    "value": 165775
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 462
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:33:54.849000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Software Engineer in Seattle, Washington | Docusign\n[ Back ](<javascript: history.go(-1)>)\n# Software Engineer\nCAROUSEL\\_PARAGRAPH\n* Engineering\n* Seattle, Washington\n* 09/04/2025\nShare Job ### Share Job\n[](javascript:)![](https://cms.jibecdn.com/prod/jibeapply/assets/SOCIAL-SHARE-TWITTER-X-ICON-en-us-1695672397749.png)\n\n## Company Overview\nDocusign brings agreements to life. Over 1.5 million customers and more than a billion people in over 180 countries use Docusign solutions to accelerate the process of doing business and simplify people’s lives. With intelligent agreement management, Docusign unleashes business-critical data that is trapped inside of docu
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Software Engineer`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599234906,
  "heuristicVersion": 5,
  "locations": [
    "Seattle, WA"
  ],
  "location": "Seattle, WA",
  "locationStates": [
    "WA"
  ],
  "locationSearch": "WA Seattle",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "totalCompensation": 165775,
  "compensationUnknown": false,
  "compensationReason": "extractor:hinted_compensation",
  "description": "Software Engineer in Seattle, Washington | Docusign\n[ Back ](<javascript: history.go(-1)>)\n# Software Engineer\nCAROUSEL\\_PARAGRAPH\n* Engineering\n* Seattle, Washington\n* 09/04/2025\nShare Job ### Share Job\n[](javascript:)![](https://cms.jibecdn.com/prod/jibeapply/assets/SOCIAL-SHARE-TWITTER-X-ICON-en-us-1695672397749.png)\n\n## Company Overview\nDocusign brings agreements to life. Over 1.5 million customers and more than a billion people in over 180 countries use Docusign solutions to accelerate the process of doing business and simplify people’s
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `raw_row_title` | `Software Engineer` |
| company | `raw_row_company` | `Docusign` |
| location | `raw_row_location` | `Seattle, WA` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `content_pattern_level` | `mid` |
| compensation | `hinted_compensation` | `165775` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `462` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:33:54.849000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Software Engineer in Seattle, Washington | Docusig` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Software Engineer`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'docusign' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Software Engineer` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `Software Engineer` | Valid title |
| hinted_title | HEURISTIC | ✅ | `Software Engineer` | Valid title |
| first_line_title | FALLBACK | ✅ | `Software Engineer in Seattle, ` | Valid title |

#### COMPANY

**Final Value:** `Docusign`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'docusign' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Docusign` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Docusign` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `This position` | Found 'Company is a' pattern |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Seattle, WA`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'docusign' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Seattle, WA` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Seattle, WA` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `Seattle, WA` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Seattle, WA' present but not inferring r |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Docusign' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `mid`
**Winning Strategy:** `content_pattern_level`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_level_field | STRUCTURED_DATA | ❌ | `` | Skipping mid-level default: mid |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ❌ | `` | No level indicator in title |
| **content_pattern_level** 🏆 | CUSTOM_550 | ✅ | `mid` | Level from experience: 5+ years -> mid |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `165775`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `165775` | Compensation from hint range: $133,800-$197,750 -> |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `133800` | Single compensation pattern: $133,800 |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `462`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `462` | Valid cost: 462 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:33:54.849000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:33:54.849000` | Valid date: 2026-01-16T14:33:54.849000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'docusign' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:54.933959` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'docusign' returned no first_published |

#### DESCRIPTION

**Final Value:** `Software Engineer in Seattle, Washington | Docusign
[ Back ](<javascript: history.go(-1)>)
# Software Engineer
CAROUSEL\_PARAGRAPH
* Engineering
* Seattle, Washington
* 09/04/2025
Share Job ### Share Job
[](javascript:)![](https://cms.jibecdn.com/prod/jibeapply/assets/SOCIAL-SHARE-TWITTER-X-ICON-en-us-1695672397749.png)

## Company Overview
Docusign brings agreements to life. Over 1.5 million customers and more than a billion people in over 180 countries use Docusign solutions to accelerate the process of doing business and simplify people’s lives. With intelligent agreement management, Docusign unleashes business-critical data that is trapped inside of documents. Until now, these were disconnected from business systems of record, costing businesses time, money, and opportunity. Using Docusign’s Intelligent Agreement Management platform, companies can create, commit, and manage agreements with solutions created by the #1 company in e-signature and contract lifecycle management (CLM).
## What you'll do
The Backend Software Engineer will own all aspects of the software development lifecycle including design, development, testing, deployment and maintenance of product features in Docusign software products. You will primarily use the Microsoft technology stack, however we also embrace open source technologies such as Redis, Cassandra and Elasticsearch, and Azure cloud services.
You enjoy fast-paced entrepreneurial environments where you can solve difficult problems using current technologies and tools. You collaborate well with other team members when brainstorming, designing, and implementing new solutions. You will also help the team succeed by thinking about ways to improve processes, suggesting ways to make the team more effective, and mentoring and modeling engineering best practices.
This position is an individual contributor role reporting to the Senior Manager, Software Engineering.
**Responsibility**
* Design, develop, and maintain high-performance backend systems and APIs using C# and .NET technologies, hosted on Microsoft Azure and on-prem data centers
* Leverage Azure services like Azure App Services, Azure Kubernetes Service (AKS), Azure Blob Storage, and SQL/No-SQL Databases to build scalable, secure, and reliable cloud-native solutions
* Build and maintain microservices-based architectures using C#,nbsp;[ASP.NET](http://asp.net/), and other Azure technologies
* Design and implement RESTful APIs and integrate with frontend teams to ensure seamless integration
* Ensure the backend infrastructure is optimized for scalability and high availability
* Address performance bottlenecks and scalability challenges proactively
* Work as part of a cross-functional team, proactively getting alignment on designs and communicating roadblocks
* Guide and mentor other engineers through design and code reviews
## Job Designation
**Hybrid:** Employee divides their time between in-office and remote work. Access to an office location is required. (Frequency: Minimum 2 days per week; may vary by team but will be weekly in-office expectation)
Positions at Docusign are assigned a job designation of either In Office, Hybrid or Remote and are specific to the role/job. Preferred job designations are not guaranteed when changing positions within Docusign. Docusign reserves the right to change a position's job designation depending on business needs and as permitted by local law.
## What you bring
**Basic**
* B.S. in Computer Science or similar field of study
* 5+ years of professional experience in backend software engineering, using OOP languages and design patterns (e.g., C#, Java, C++)
* 1+ years of hands-on experience with building and operating cloud-native microservices and APIs at scale
* Experience with SQL databases (e.g., Azure SQL Database, SQL Server) and NoSQL solutions (e.g., Cosmos DB)
* Experience with service reliability and incident response best practices
**Preferred**
* Experience with Microsoft technology stack (e.g., C#, .NET, ASP.NET)
* Experience with Microsoft Azure or AWS
* Past exposure to full-stack development and UI technologies (e.g., React, JS) and UI-facing APIs (e.g., GraphQL, GRPC, REST)
* Experience with Enterprise SaaS software products
## Wage Transparency
Pay for this position is based on a number of factors including geographic location and may vary depending on job-related knowledge, skills, and experience.
Based on applicable legislation, the below details pay ranges in the following locations:
Washington, Maryland, New Jersey and New York (including NYC metro area): $133,800.00 - $197,750.00 base salary
This role is also eligible for the following:
* Bonus: Sales personnel are eligible for variable incentive pay dependent on their achievement of pre-established sales goals. Non-Sales roles are eligible for a company bonus plan, which is calculated as a percentage of eligible wages and dependent on company performance.
* Stock: This role is eligible to receive Restricted Stock Units (RSUs).
[Global benefits](https://careers.docusign.com/benefits) provide options for the following:
* Paid Time Off: earned time off, as well as paid company holidays based on region
* Paid Parental Leave: take up to six months off with your child after birth, adoption or foster care placement
* Full Health Benefits Plans: options for 100% employer paid and minimum employee contribution health plans from day one of employment
* Retirement Plans: select retirement and pension programs with potential for employer contributions
* Learning and Development: options for coaching, online courses and education reimbursements
* Compassionate Care Leave: paid time off following the loss of a loved one and other life-changing events
## Life at Docusign
**Working here**
Docusign is committed to building trust and making the world more agreeable for our employees, customers and the communities in which we live and work. You can count on us to listen, be honest, and try our best to do what’s right, every day. At Docusign, everything is equal.
We each have a responsibility to ensure every team member has an equal opportunity to succeed, to be heard, to exchange ideas openly, to build lasting relationships, and to do the work of their life. Best of all, you will be able to feel deep pride in the work you do, because your contribution helps us make the world better than we found it. And for that, you’ll be loved by us, our customers, and the world in which we live.
**Accommodation**
Docusign is committed to providing reasonable accommodations for qualified individuals with disabilities in our job application procedures. If you need such an accommodation, or a religious accommodation, during the application process, please contact us at [accommodations@docusign.com](mailto:accommodations@docusign.com).
If you experience any issues, concerns, or technical difficulties during the application process please get in touch with our Talent organization at [taops@docusign.com](mailto:taops@docusign.com) for assistance.
[Applicant and Candidate Privacy Notice](https://www.docusign.com/privacy/applicant-candidate-notice/)
## States Not Eligible for Employment
This position is not eligible for employment in the following states: Alaska, Hawaii, Maine, Mississippi, North Dakota, South Dakota, Vermont, West Virginia and Wyoming.
## Equal Opportunity Employer
It's important to us that we build a talented team that is as diverse as our customers and where all employees feel a deep sense of belonging and thrive. We encourage great talent who bring a range of perspectives to apply for our open positions. Docusign is an Equal Opportunity Employer and makes hiring decisions based on experience, skill, aptitude and a can-do approach. We will not discriminate based on race, ethnicity, color, age, sex, religion, national origin, ancestry, pregnancy, sexual orientation, gender identity, gender expression, genetic information, physical or mental disability, registered domestic partner status, caregiver status, marital status, veteran or military status, or any other legally protected category.
[EEO Know Your Rights poster](https://urldefense.com/v3/__https://www.eeoc.gov/sites/default/files/2023-06/22-088_EEOC_KnowYourRights6.12ScreenRdr.pdf__;!!BN3BN5aqUA!7l3IpK6Tz3eYjD-YQjhQ5xFm0Uxc2PO6lKkp3MXBDxSUW05YVTB7sRT5_FaM0XM6_lZWB7DMHU42l4lWP9FaclbV8-lolVL2hYvLrQ$)
#LI-Hybrid
### Our global benefits
![](https://cms.jibecdn.com/prod/docusign/assets/LP-SKU-F6-ICON_1-en-us-1662546149975.svg)
#### Paid time off
Take time to unwind with earned days off, plus paid company holidays based on your region.
![](https://cms.jibecdn.com/prod/docusign/assets/LP-SKU-F6-ICON_2-en-us-1662546456685.svg)
#### Paid parental leave
Take up to six months off with your child after birth, adoption or foster care placement.
![](https://cms.jibecdn.com/prod/docusign/assets/LP-SKU-F6-ICON_3-en-us-1662546616055.svg)
#### Full health benefits
Options for 100% employer-paid health plans from day one of employment.
![](https://cms.jibecdn.com/prod/docusign/assets/LP-SKU-F6-ICON_4-en-us-1662545678923.svg)
#### Retirement plans
Select retirement and pension programs with potential for employer contributions.
![](https://cms.jibecdn.com/prod/docusign/assets/LP-SKU-F6-ICON_5-en-us-1662545794244.svg)
#### Learning &amp; development
Grow your career with coaching, online courses and education reimbursements.
![](https://cms.jibecdn.com/prod/docusign/assets/LP-SKU-F6-ICON_6-en-us-1662545952188.svg)
#### Compassionate care leave
Paid time off following the loss of a loved one and other life-changing events.
### Life at Docusign
01
#### Explore our complete benefits
What we offer ![right arrow](https://cms.jibecdn.com/prod/docusign/assets/LP-SKU-F1-ARROW_1-en-us-1663822478197.svg)
](https://careers.docusign.com/benefits)
02
#### Read about diversity, equity &amp; inclusion
Belong here ![right arrow](https://cms.jibecdn.com/prod/docusign/assets/LP-SKU-F1-ARROW_1-en-us-1663822478197.svg)
](https://careers.docusign.com/together)
03
#### What it’s like working here
Our culture ![right arrow](https://cms.jibecdn.com/prod/docusign/assets/LP-SKU-F1-ARROW_1-en-us-1663822478197.svg)
](https://careers.docusign.com/culture)`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Software Engineer in Seattle, ` | Valid description (10224 chars, 1312 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Software Engineer in Seattle, ` | Valid description (10242 chars, 1315 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Software Engineer in Seattle, ` | Valid description (10242 chars, 1315 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Software Engineer` |
| Company | `Docusign` |
| Location | `Seattle, WA` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1768599234849` |
| Description Words | `1312` |
| Cost (milli-cents) | `462` |
| URL | `https://careers.docusign.com/jobs/27215?lang=en-us` |

**Description Preview (first 200 words):**

```
Software Engineer in Seattle, Washington | Docusign [ Back ](<javascript: history.go(-1)>) # Software Engineer CAROUSEL\_PARAGRAPH * Engineering * Seattle, Washington * 09/04/2025 Share Job ### Share Job [](javascript:)![](https://cms.jibecdn.com/prod/jibeapply/assets/SOCIAL-SHARE-TWITTER-X-ICON-en-us-1695672397749.png) ## Company Overview Docusign brings agreements to life. Over 1.5 million customers and more than a billion people in over 180 countries use Docusign solutions to accelerate the process of doing business and simplify people’s lives. With intelligent agreement management, Docusign unleashes business-critical data that is trapped inside of documents. Until now, these were disconnected from business systems of record, costing businesses time, money, and opportunity. Using Docusign’s Intelligent Agreement Management platform, companies can create, commit, and manage agreements with solutions created by the #1 company in e-signature and contract lifecycle management (CLM). ## What you'll do The Backend Software Engineer will own all aspects of the software development lifecycle including design, development, testing, deployment and maintenance of product features in Docusign software products. You will primarily use the Microsoft technology stack, however we also embrace open source technologies such as Redis, Cassandra and Elasticsearch, and Azure cloud services. You enjoy fast-paced entrepreneurial environments where you can solve difficult problems using current technologies and tools. You collaborate well with other team members...
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
  "title": "Software Engineer",
  "company": "Docusign",
  "location": "Seattle, WA",
  "description": "Software Engineer in Seattle, Washington | Docusign\n[ Back ](<javascript: history.go(-1)>)\n# Software Engineer\nCAROUSEL\\_PARAGRAPH\n* Engineering\n* Seattle, Washington\n* 09/04/2025\nShare Job ### Share Job\n[](javascript:)![](https://cms.jibecdn.com/prod/jibeapply/assets/SOCIAL-SHARE-TWITTER-X-ICON-en-us-1695672397749.png)\n## Job Description\n## Company Overview\nDocusign brings agreements to life. Over 1.5 million customers and more than a billion people in over 180 countries use Docusign solutions ...",
  "url": "https://careers.docusign.com/jobs/27215?lang=en-us",
  "posted_at": 1768599234849,
  "level": "mid",
  "remote": false,
  "cost_milli_cents": 462,
  "_full_description_word_count": 1315
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 462,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
