# Extraction Steps: confluent

**Detail URL:** `https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1`
**Source URL:** `https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1`
**Handler:** `confluent`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Confluent Careers
[](https://careers.confluent.io/)
[Life at confluent](https://careers.confluent.io/life-at-confluent)[Belonging](https://careers.confluent.io/belonging)
Our Teams
[early talent](https://careers.confluent.io/early-talent)
[Open Positions](https://careers.confluent.io/jobs)
[Life at confluent](https://careers.confluent.io/life-at-confluent)[Belonging](https://careers.confluent.io/belonging)
Our Teams
[early talent](https://careers.confluent.io/early-talent)
[Open Positions](https://careers.confluent.io/jobs)[&lt; Back to Confluent.io](https://www.confluent.io/)
# Staff Software Engineer - Flink SQL
Engineering
Remote, United States
R03754
[Apply Now](79c5035c-4266-40f0-86e1-84d067ed77b1/apply)
##### Description
We’re not just building better tech. We’re rewriting how data moves and what the world can do with it. With Confluent, data doesn’t sit still. Our platform puts information in motion, streaming in near real-time so companies can react faster, build smarter, and deliver experiences as dynamic as the world around them.
It takes a certain kind of person to join this team. Those who ask hard questions, give honest feedback, and show up for each other. No egos, no solo acts. Just smart, curious humans pushing toward something bigger, together.
One Confluent. One Team. One Data Streaming Platform.
## **About the Role:**
Confluent is seeking an innovative and executing Staff Engineer to help make stream processing feel like using a database with SQL. In this role, you will be instrumental in developing the core building blocks that will allow users a true just-in time, consumption based experience, making it the most efficient and fast cloud native Apache Flink experience on the market. You will be working in a team with other known Flink experts iterating on an already existing implementation based on actual usage data and product requirements. With the team you will also be working on relevant components of open source Apache Flink that will benefit both the Open Source community as well as Confluent.
## **What You Will Do:**
* Work on the next iterations of Flink SQL components such as logical query planner, parser, optimizer and SQL runtime on Confluent Cloud to deliver an efficient and unified experience for batch and streaming workloads.
* Following a holistic development approach you will be responsible for the code quality, test coverage, documentation and maintainability of the components you and your team work on.
* Designing Components: You will not only implement aspects of Flink SQL but also draft designs, discuss them and curate and moderate a decision with involved stakeholders.
* Open Source contribution: As part of the Flink SQL team you will contribute to relevant changes and features to Open Source Apache Flink and help the community to maintain a high quality project by participating in relevant mailing list discussions, reviewing PRs affecting deployment and coordination components.
* Open Source visibility: Y

... (truncated, 10663 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `confluent`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
Confluent Careers
[](https://careers.confluent.io/)
[Life at confluent](https://careers.confluent.io/life-at-confluent)[Belonging](https://careers.confluent.io/belonging)
Our Teams
[early talent](https://careers.confluent.io/early-talent)
[Open Positions](https://careers.confluent.io/jobs)
[Life at confluent](https://careers.confluent.io/life-at-confluent)[Belonging](https://careers.confluent.io/belonging)
Our Teams
[early talent](https://careers.confluent.io/early-talent)
[Open Positions](https://careers.confluent.io/jobs)[&lt; Back to Confluent.io](https://www.confluent.io/)
# Staff Software Engineer - Flink SQL
Engineering
Remote, United States
R03754
[Apply Now](79c5035c-4266-40f0-86e1-84d067ed77b1/apply)
##### Description
We’re not just building better tech. We’re rewriting how data moves and what the world can do with it. With Confluent, data doesn’t sit still. Our platform puts information in motion, streaming in near real-time so companies can react faster, build smarter, and deliver experiences as dynamic as the world around them.
It takes a certain kind of person to join this team. Those who ask hard questions, give honest feedback, and show up for each other. No egos, no solo acts. Just smart, curious humans pushing toward something bigger, together.
One Confluent. One Team. One Data Streaming Platform.
## **About the Role:**
Confluent is seeking an innovative and executing Staff Engineer to help make stream processing feel like using a database with SQL. In this role, you will be instrumental in developing the core building blocks that will allow users a true just-in time, consumption based experience, making it the most efficient and fast cloud native Apache Flink experience on the market. You will be working in a team with other known Flink experts iterating on an already existing implementation based on actual usage data and product requirements. With the team you will also be working on relevant components of open source Apache Flink that will benefi

... (truncated, 10663 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: confluent

```json
{
  "url": "https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1",
  "handler": "confluent"
}
```

### Raw Content Capture

Captured 10663 chars of commonmark content

```json
{
  "length": 10663,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 10663 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 10663
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1",
      "sourceUrl": "https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1",
      "provider": "spidercloud",
      "siteId": "confluent",
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
    "value": "Staff Software Engineer - Flink SQL"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Confluent"
  },
  "location": {
    "winner": "country_only_fallback_location",
    "value": "United States"
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
    "value": 230000
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 32
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:34:00.184000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "We’re not just building better tech. We’re rewriting how data moves and what the world can do with it. With Confluent, data doesn’t sit still. Our platform puts information in motion, streaming in near real-time so companies can react faster, build smarter, and deliver experiences as dynamic as the world around them.\nIt takes a certain kind of person to join this team. Those who ask hard questions, give honest feedback, and show up for each other. No egos, no solo acts. Just smart, curious humans pushing toward something bigger, together.\nOne Confluent. One Team. One Data Streaming Platform.\n## **About the Role:**\nConfluen
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Staff Software Engineer - Flink SQL`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599240251,
  "heuristicVersion": 5,
  "locations": [
    "United States"
  ],
  "location": "United States",
  "locationStates": [],
  "locationSearch": "United States",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "totalCompensation": 230000,
  "compensationUnknown": false,
  "compensationReason": "extractor:content_pattern_compensation",
  "description": "We’re not just building better tech. We’re rewriting how data moves and what the world can do with it. With Confluent, data doesn’t sit still. Our platform puts information in motion, streaming in near real-time so companies can react faster, build smarter, and deliver experiences as dynamic as the world around them.\nIt takes a certain kind of person to join this team. Those who ask hard questions, give honest feedback, and show up for each other. No egos, no solo acts. Just smart, curious humans pushing toward something bigger, together.\nOne Conf
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `raw_row_title` | `Staff Software Engineer - Flink SQL` |
| company | `raw_row_company` | `Confluent` |
| location | `country_only_fallback_location` | `United States` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `staff` |
| compensation | `content_pattern_compensation` | `230000` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `32` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:34:00.184000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `We’re not just building better tech. We’re rewriti` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Staff Software Engineer - Flink SQL`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'confluent' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Staff Software Engineer - Flin` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `Staff Software Engineer - Flin` | Valid title |
| hinted_title | HEURISTIC | ✅ | `Staff Software Engineer - Flin` | Valid title |
| first_line_title | FALLBACK | ✅ | `One Confluent. One Team. One D` | Valid title |

#### COMPANY

**Final Value:** `Confluent`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'confluent' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Confluent` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Confluent` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `this team` | Found 'Join Company' pattern |
| hinted_company | HEURISTIC | ❌ | `` | URL as company name rejected |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `United States`
**Winning Strategy:** `country_only_fallback_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'confluent' returned no location hint |
| raw_row_location | EXPLICIT_FIELD | ❌ | `` | Country-only location too generic: United States |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| **country_only_fallback_location** 🏆 | CUSTOM_550 | ✅ | `United States` | Country-only fallback: Country-only location accep |
| hinted_location | HEURISTIC | ✅ | `United States` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'United States' present but not inferring |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Confluent' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `staff`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `staff` | Explicit level field: staff -> staff |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `staff` | Level from title: 'staff' -> staff |
| content_pattern_level | CUSTOM_550 | ❌ | `` | No level pattern in content |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `230000`
**Winning Strategy:** `content_pattern_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| hinted_compensation | CUSTOM_450 | ❌ | `` | No compensation in hints |
| **content_pattern_compensation** 🏆 | CONTENT_PATTERN | ✅ | `230000` | Single compensation pattern: $230,000 |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `32`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `32` | Valid cost: 32 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:34:00.184000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:34:00.184000` | Valid date: 2026-01-16T14:34:00.184000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'confluent' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:34:00.278279` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'confluent' returned no first_published |

#### DESCRIPTION

**Final Value:** `We’re not just building better tech. We’re rewriting how data moves and what the world can do with it. With Confluent, data doesn’t sit still. Our platform puts information in motion, streaming in near real-time so companies can react faster, build smarter, and deliver experiences as dynamic as the world around them.
It takes a certain kind of person to join this team. Those who ask hard questions, give honest feedback, and show up for each other. No egos, no solo acts. Just smart, curious humans pushing toward something bigger, together.
One Confluent. One Team. One Data Streaming Platform.
## **About the Role:**
Confluent is seeking an innovative and executing Staff Engineer to help make stream processing feel like using a database with SQL. In this role, you will be instrumental in developing the core building blocks that will allow users a true just-in time, consumption based experience, making it the most efficient and fast cloud native Apache Flink experience on the market. You will be working in a team with other known Flink experts iterating on an already existing implementation based on actual usage data and product requirements. With the team you will also be working on relevant components of open source Apache Flink that will benefit both the Open Source community as well as Confluent.
## **What You Will Do:**
* Work on the next iterations of Flink SQL components such as logical query planner, parser, optimizer and SQL runtime on Confluent Cloud to deliver an efficient and unified experience for batch and streaming workloads.
* Following a holistic development approach you will be responsible for the code quality, test coverage, documentation and maintainability of the components you and your team work on.
* Designing Components: You will not only implement aspects of Flink SQL but also draft designs, discuss them and curate and moderate a decision with involved stakeholders.
* Open Source contribution: As part of the Flink SQL team you will contribute to relevant changes and features to Open Source Apache Flink and help the community to maintain a high quality project by participating in relevant mailing list discussions, reviewing PRs affecting deployment and coordination components.
* Open Source visibility: You will also write blog posts and give talks at meetups and conferences to strengthen the position of Confluent Cloud as industry experts for stream processing and Apache Flink in particular.
* Team: As part of the team you will help to follow and maintain processes, by being an active part of the team, leading meetings and taking initiatives where feasible and participating in on-call rotations.
* Mentoring: You will mentor other engineers with less experience in Apache Flink to get started in the community and more junior developers to get into the area of serverless Apache Flink.
## **What You Will Bring:**
* 10+ years of relevant software development experience.
* Technical Expertise: Deep knowledge of database internals, especially around query planning and optimization. Extensive experience in software development, including hands-on experience in designing and development of complex distributed systems.
* Problem Solving: Strong problem-solving skills, capable of translating complex requirements into effective solutions.
* Communication: Excellent communication skills, both written and verbal, with the ability to collaborate across teams.
* Industry Engagement: Active involvement in stream processing communities, conferences, and a strong network within the industry.
##
## **Ready to build what's next? Let’s get in motion.**
###
# **Come As You Are**
Belonging isn’t a perk here. It’s the baseline. We work across time zones and backgrounds, knowing the best ideas come from different perspectives. And we make space for everyone to lead, grow, and challenge what’s possible.
We’re proud to be an equal opportunity workplace. Employment decisions are based on job-related criteria, without regard to race, color, religion, sex, sexual orientation, gender identity, national origin, disability, veteran status, or any other classification protected by law.
[Apply](79c5035c-4266-40f0-86e1-84d067ed77b1/apply)[
Return to All Jobs
](https://careers.confluent.io/jobs)
##### Share This Job
[](<https://twitter.com/intent/tweet?text=Staff Software Engineer - Flink SQL in Remote, United States&amp;url=https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1>)[](<http://www.linkedin.com/shareArticle?mini=true&amp;url=https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1&amp;title=Staff Software Engineer - Flink SQL in Remote, United States>)[](<http://www.facebook.com/sharer.php?u=https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1&amp;t=Staff Software Engineer - Flink SQL in Remote, United States>)
##### Global Benefits to Help You Do Your Best Work
Remote-First Work
Robust Insurance Benefits
Flexible Time Away
The Best Teammates
Experience Ambassadors
Open and Honest Culture
Well-Being and Growth
##### Leadership Principles Define How we Act
Our Leadership Principles outline a shared set of expectations for how we think and behave at Confluent. They’re an extension of our company values which we all live each day. You can learn more [here](https://careers.confluent.io/life-at-confluent).
##### Confluent is Remote-First
We care about how you work, not where. Confluent is built for flexibility, and we encourage you to apply even if you're outside the listed location. While we're remote first, we know nothing beats an in-person welcome. New Confluent employees may spend part of their first week at a Confluent office to kick things off.
##### Compensation
$230.8K – $271.2K • Offers Equity
At Confluent, we are committed to providing competitive pay and benefits that are in line with industry standards. We analyze and carefully consider several factors when determining compensation, including work history, education, professional experience, and location. The actual pay may vary depending on your skills, qualifications, experience, and work location. In addition, Confluent offers a wide range of employee benefits. To learn more about our benefits click [here](https://confluentbenefits.com/).
##### Product
* [Confluent Platform](https://www.confluent.io/product/confluent-platform/)
* [ksqlDB](https://www.confluent.io/product/confluent-platform/)
* [Contentful Hub](https://www.confluent.io/hub/)
* [Subscription](https://www.confluent.io/subscription/)
* [Professional Services](https://www.confluent.io/services/)
* [Training](https://www.confluent.io/training/)
* [Customers](https://www.confluent.io/customers/)
##### Cloud
* [Confluent Cloud](https://www.confluent.io/confluent-cloud/)
* [Cloud](https://www.confluent.io/confluent-cloud/support/)
* [Sign Up](https://www.confluent.io/confluent-cloud/tryfree-v1/)
* [Log in](https://www.confluent.io/confluent-cloud/tryfree-v1/)
* [Cloud FAQ](https://www.confluent.io/confluent-cloud/tryfree-v1/)
##### Solutions
* [Financial Services](https://www.confluent.io/industry-solutions/financial-services/)
* [Insurance](https://www.confluent.io/industry-solutions/insurance/)
* [Retail and eCommerce](https://www.confluent.io/industry-solutions/retail/)
* [Automotive](https://www.confluent.io/industry-solutions/automotive/)
* [Government](https://www.confluent.io/industry-solutions/government/)
* [Gaming](https://www.confluent.io/industry-solutions/gaming/)
* [Technology](https://www.confluent.io/industry-solutions/telco/)
* [Manufacturing](https://www.confluent.io/industry-solutions/manufacturing/)
* [Fraud Detection](https://www.confluent.io/use-case/fraud-detection/)
* [Customer 360](https://www.confluent.io/use-case/customer-360/)
* [Messaging Modernization](https://www.confluent.io/use-case/messaging-modernization/)
* [Streaming ETL](https://www.confluent.io/use-case/data-integration/)
* [Event-driven Microservices](https://www.confluent.io/use-case/event-driven-microservices-communication/)
* [Mainframe Offload](https://www.confluent.io/use-case/mainframe-offload/)
* [SIEM Optimization](https://www.confluent.io/use-case/siem/)
* [Bridge to Cloud](https://www.confluent.io/use-case/hybrid-and-multicloud/)
* [Internet of Things](https://www.confluent.io/use-case/internet-of-things-iot/)
##### Developers
* [What is Kafka?](https://www.confluent.io/what-is-apache-kafka/)
* [Resources](https://www.confluent.io/resources/)
* [Events](https://events.confluent.io/)
* [Online Talks](https://www.confluent.io/online-talks/)
* [Meetups](https://www.confluent.io/community/)
* [Kafka Summit](https://www.kafka-summit.org/)
* [Kafka Tutorials](https://kafka-tutorials.confluent.io/)
* [Confluent Developer](https://developer.confluent.io/)
* [Docs](https://docs.confluent.io/home/overview.html)
* [Blogs](https://www.confluent.io/blog/)
##### About
* [Company](https://www.confluent.io/about/)
* [Careers](https://www.confluent.io/careers)
* [Partners](https://www.confluent.io/partners/)
* [News](https://www.confluent.io/in-the-news/)
* [Contact](https://www.confluent.io/contact/)
* [Trust and Security](https://www.confluent.io/trust-and-security/)
[](https://twitter.com/ConfluentInc)[](https://www.linkedin.com/company/confluent)[](https://www.instagram.com/confluent_inc/)[](https://www.facebook.com/confluentinc/)
[Terms &amp; Conditions](https://www.confluent.io/terms-of-use/)|[Privacy Policy](https://www.confluent.io/legal/confluent-privacy-statement/)|[Do Not Sell My Information](https://www.confluent.io/legal/confluent-privacy-statement/)|[Modern Slavery Policy](https://www.confluent.io/modern-slavery-policy/)|[Cookie Preferences](https://www.confluent.io/)
Copyright © Confluent, Inc. 2014-2025. ®, Apache Kafka®, Kafka®, Apache Flink®, Flink®, Apache Iceberg®, Iceberg® and associated open source project names are trademarks of the Apache Software Foundation`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `We’re not just building better` | Valid description (9926 chars, 1057 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Confluent Careers
[](https://c` | Valid description (10663 chars, 1097 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Confluent Careers
[](https://c` | Valid description (10663 chars, 1097 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Staff Software Engineer - Flink SQL` |
| Company | `Confluent` |
| Location | `United States` |
| Is Remote | `False` |
| Level | `staff` |
| Posted At | `1768599240184` |
| Description Words | `1057` |
| Cost (milli-cents) | `32` |
| URL | `https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1` |

**Description Preview (first 200 words):**

```
We’re not just building better tech. We’re rewriting how data moves and what the world can do with it. With Confluent, data doesn’t sit still. Our platform puts information in motion, streaming in near real-time so companies can react faster, build smarter, and deliver experiences as dynamic as the world around them. It takes a certain kind of person to join this team. Those who ask hard questions, give honest feedback, and show up for each other. No egos, no solo acts. Just smart, curious humans pushing toward something bigger, together. One Confluent. One Team. One Data Streaming Platform. ## **About the Role:** Confluent is seeking an innovative and executing Staff Engineer to help make stream processing feel like using a database with SQL. In this role, you will be instrumental in developing the core building blocks that will allow users a true just-in time, consumption based experience, making it the most efficient and fast cloud native Apache Flink experience on the market. You will be working in a team with other known Flink experts iterating on an already existing implementation based on actual usage data and product requirements. With the team you will also be working on relevant components...
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
  "title": "Staff Software Engineer - Flink SQL",
  "company": "Confluent",
  "location": "United States",
  "description": "Confluent Careers\n[](https://careers.confluent.io/)\n[Life at confluent](https://careers.confluent.io/life-at-confluent)[Belonging](https://careers.confluent.io/belonging)\nOur Teams\n[early talent](https://careers.confluent.io/early-talent)\n[Open Positions](https://careers.confluent.io/jobs)\n[Life at confluent](https://careers.confluent.io/life-at-confluent)[Belonging](https://careers.confluent.io/belonging)\nOur Teams\n[early talent](https://careers.confluent.io/early-talent)\n[Open Positions](https...",
  "url": "https://careers.confluent.io/jobs/job/79c5035c-4266-40f0-86e1-84d067ed77b1",
  "posted_at": 1768599240184,
  "level": "staff",
  "remote": false,
  "cost_milli_cents": 32,
  "_full_description_word_count": 1097
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 32,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
