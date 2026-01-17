# Extraction Steps: avature

**Detail URL:** `https://bloomberg.avature.net/careers/JobDetail/Senior-Data-Management-Professional-Data-Engineer-BNEF-Data/16762`
**Source URL:** `https://bloomberg.avature.net/careers/JobDetail/Senior-Data-Management-Professional-Data-Engineer-BNEF-Data/16762`
**Handler:** `avature`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Senior Data Management Professional - Data Engineer - BNEF Data
- 16762
- Bloomberg
Your choice regarding cookies on this website: This website uses cookies, beacons, invisible tags, and similar technologies (collectively "cookies") in order to function and to understand how users engage with us. More information on the types of cookies we use can be found by clicking on "Cookie Preferences". For more information relating to how we use your personal data, please see our [privacy policy](https://www.bloomberg.com/notices/careers-privacy/).
[Accept All](#)[Reject All](#)[**Cookie Preferences](#)[Cookie Preferences](#)
###### Essential
These cookies are essential for our website to function properly.
These cookies enable the basic functionalities of the website to function and ensure the security features of the website. We have to keep these cookies enabled as we cannot provide you with the website without these cookies. Essential cookies include: Session cookie, Language cookie, Token field for CSRF, and Cookie consent preferences.
Performance###### Performance
These cookies allow us to understand and analyse the performance of the website to deliver a better experience to our users. Among other features, these cookies help us count the number of visits and/or track user activity to measure and improve performance.
Performance cookies include [Google Analytics](https://tools.google.com/dlpage/gaoptout),
[Microsoft Clarity](https://privacy.microsoft.com/en-US/privacystatement) and
[ Optimizely](https://www.optimizely.com/trust-center/privacy/).
Functional###### Functional
These cookies enhance or add functionalities to the website (e.g. add fonts or live chats, embed videos). If rejected, some or all of the website’s functionalities may not function properly or as intended. Functional cookies include [iCIMS](https://www.icims.com/legal/privacy-notice-website/).
Advertising###### Advertising
These cookies are used by the following advertising companies: [Appcast](https://www.appcast.io/privacy-policy/), [Microsoft Advertising](https://about.ads.microsoft.com/en-us/resources/policies/microsoft-advertising-privacy-policy), [Reddit](https://www.reddit.com/policies/privacy-policy?rdt=64219), [LinkedIn Insights](https://www.linkedin.com/legal/cookie-policy) and [Google Ads](https://policies.google.com/technologies/ads?hl=en-US).
[Save and Close](#)
Senior Data Management Professional - Data Engineer - BNEF Data
Location
Skillman
Business Area
Data
Ref #
10048587
##
Description &amp; Requirements
Bloomberg runs on data. Our products are fueled by powerful information. We combine data and context to paint the whole picture for our clients, around the clock – from around the world. In Data, we are responsible for delivering this data, news and analytics through innovative technology - quickly and accurately. We apply problem-solving skills to identify innovative workflow efficiencies, and we implement technology solutions to enhance our systems, products an

... (truncated, 9206 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `avature`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
Senior Data Management Professional - Data Engineer - BNEF Data
- 16762
- Bloomberg
Senior Data Management Professional - Data Engineer - BNEF Data
Location
Skillman
Business Area
Data
Ref #
10048587
##
Description &amp; Requirements
Bloomberg runs on data. Our products are fueled by powerful information. We combine data and context to paint the whole picture for our clients, around the clock – from around the world. In Data, we are responsible for delivering this data, news and analytics through innovative technology - quickly and accurately. We apply problem-solving skills to identify innovative workflow efficiencies, and we implement technology solutions to enhance our systems, products and processes.
The BNEF Data Team maintains databases for renewable and conventional power plants, storage projects and manufacturing plants globally. The team is currently working on a new future-proof data model and workflow that can facilitate and accelerate coverage expansion for integrated use in downstream analysis across our customer groups (including governments, portfolio managers, corporations, equity analysts etc.).
The Role:
As a Data Engineer on the BNEF Data Team, you’re required to understand the data requirements, specify the modeling needs of datasets and use existing tech stack solutions for efficient data ingestion workflows and data pipelining. You will implement technical solutions using programming, machine learning, AI, and human-in-the-loop approaches to make sure our data is fit-for-purpose for our clients. You will work closely with our Engineering partners, our Data Product Managers as well as Product teams, so you need to be able to coordinate with multi-disciplinary and regional teams and have experience in project management and stakeholder engagement. You will need to be comfortable working with large, varied, sophisticated and often unstructured data sets and you will need to demonstrate strong experience in data engineering.
We trust you to:
● Buil

... (truncated, 5481 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: avature

```json
{
  "url": "https://bloomberg.avature.net/careers/JobDetail/Senior-Data-Management-Professional-Data-Engineer-BNEF-Data/16762",
  "handler": "avature"
}
```

### Raw Content Capture

Captured 9206 chars of commonmark content

```json
{
  "length": 9206,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 5481 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 5481
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://bloomberg.avature.net/careers/JobDetail/Senior-Data-Management-Professional-Data-Engineer-BNEF-Data/16762",
      "sourceUrl": "https://bloomberg.avature.net/careers/JobDetail/Senior-Data-Management-Professional-Data-Engineer-BNEF-Data/16762",
      "provider": "spidercloud",
      "siteId": "avature",
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
    "value": "Senior Data Management Professional - Data Engineer - BNEF Data"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Bloomberg"
  },
  "location": {
    "winner": "site_handler_location_hint",
    "value": "Skillman"
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
    "value": 150000
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 102
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:33:51.655000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Bloomberg runs on data. Our products are fueled by powerful information. We combine data and context to paint the whole picture for our clients, around the clock – from around the world. In Data, we are responsible for delivering this data, news and analytics through innovative technology - quickly and accurately. We apply problem-solving skills to identify innovative workflow efficiencies, and we implement technology solutions to enhance our systems, products and processes.\nThe BNEF Data Team maintains databases for renewable and conventional power plants, storage projects and manufacturing plants globally. The t
```

### Heuristic Title Override

Title changed from 'Senior Data Management Professional - Data Engineer - BNEF Data - 16762 - Bloomberg' to 'Senior Data Management Professional - Data Engineer - BNEF Data'

```json
{
  "original_title": "Senior Data Management Professional - Data Engineer - BNEF Data - 16762 - Bloomberg",
  "patched_title": "Senior Data Management Professional - Data Engineer - BNEF Data",
  "patch": {
    "heuristicAttempts": 1,
    "heuristicLastTried": 1768599231687,
    "heuristicVersion": 5,
    "title": "Senior Data Management Professional - Data Engineer - BNEF Data",
    "jobTitle": "Senior Data Management Professional - Data Engineer - BNEF Data",
    "location": "Skillman",
    "locationSearch": "Skillman",
    "totalCompensation": 150000,
    "compensationUnknown": false,
    "compensationReason": "extractor:hinted_compensation",
    "description": "Bloomberg runs on data. Our products are fueled by powerful information. We combine data and context to paint the whole picture for our clients, around the clock – from around the world. In Data, we are responsible for delivering this data, news and analytics through innovative technology - quickly and accurately. We apply problem-solving skills to identify innovative workflow efficiencies, and we implement technology solutions to enhance our systems, products and processes.\nThe BNEF Data Team maintains databases for renewable and conventional power plants, storage projects and manufacturing plants globally. The team is currently working on a new future-proof data model and workflow that can facilitate and accelerate coverage expansion for integrated use in downstream analysis across our customer groups (includin
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**⚠️ TITLE CHANGED:**
- Original: `Senior Data Management Professional - Data Engineer - BNEF Data - 16762 - Bloomberg`
- After Heuristics: `Senior Data Management Professional - Data Engineer - BNEF Data`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599231687,
  "heuristicVersion": 5,
  "title": "Senior Data Management Professional - Data Engineer - BNEF Data",
  "jobTitle": "Senior Data Management Professional - Data Engineer - BNEF Data",
  "location": "Skillman",
  "locationSearch": "Skillman",
  "totalCompensation": 150000,
  "compensationUnknown": false,
  "compensationReason": "extractor:hinted_compensation",
  "description": "Bloomberg runs on data. Our products are fueled by powerful information. We combine data and context to paint the whole picture for our clients, around the clock – from around the world. In Data, we are responsible for delivering this data, news and analytics through innovative technology - quickly and accurately. We apply problem-solving skills to identify innovative workflow efficiencies, and we implement technology solutions to enhance our systems, products and processes.\nThe BNEF Data Team maintains databases for renewable and conventional po
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `raw_row_title` | `Senior Data Management Professional - Data Enginee` |
| company | `raw_row_company` | `Bloomberg` |
| location | `site_handler_location_hint` | `Skillman` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `senior` |
| compensation | `hinted_compensation` | `150000` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `102` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:33:51.655000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Bloomberg runs on data. Our products are fueled by` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Senior Data Management Professional - Data Engineer - BNEF Data`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'avature' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Senior Data Management Profess` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | Generic title rejected: Description &amp; Requirem |
| hinted_title | HEURISTIC | ✅ | `Senior Data Management Profess` | Valid title |
| first_line_title | FALLBACK | ✅ | `We trust you to:` | Valid title |

#### COMPANY

**Final Value:** `Bloomberg`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'avature' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Bloomberg` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Bloomberg` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ❌ | `` | No company pattern matched |
| hinted_company | HEURISTIC | ✅ | `Bloomberg` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Skillman`
**Winning Strategy:** `site_handler_location_hint`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_location_hint** 🏆 | SITE_HANDLER | ✅ | `Skillman` | Valid location |
| raw_row_location | EXPLICIT_FIELD | ✅ | `Skillman` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Skillman` | Country-only fallback: Valid location |
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
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'Skillman' present but not inferring remo |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | No remote in hints |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Bloomberg' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `senior`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `senior` | Explicit level field: senior -> senior |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ✅ | `senior` | Level from title: 'senior' -> senior |
| content_pattern_level | CUSTOM_550 | ✅ | `mid` | Level from experience: 4+ years -> mid |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `150000`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `150000` | Compensation from hint range: $110,000-$190,000 -> |
| content_pattern_compensation | CONTENT_PATTERN | ❌ | `` | No compensation pattern in content |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `102`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `102` | Valid cost: 102 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:33:51.655000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:33:51.655000` | Valid date: 2026-01-16T14:33:51.655000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'avature' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:51.699406` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'avature' returned no first_published |

#### DESCRIPTION

**Final Value:** `Bloomberg runs on data. Our products are fueled by powerful information. We combine data and context to paint the whole picture for our clients, around the clock – from around the world. In Data, we are responsible for delivering this data, news and analytics through innovative technology - quickly and accurately. We apply problem-solving skills to identify innovative workflow efficiencies, and we implement technology solutions to enhance our systems, products and processes.
The BNEF Data Team maintains databases for renewable and conventional power plants, storage projects and manufacturing plants globally. The team is currently working on a new future-proof data model and workflow that can facilitate and accelerate coverage expansion for integrated use in downstream analysis across our customer groups (including governments, portfolio managers, corporations, equity analysts etc.).
The Role:
As a Data Engineer on the BNEF Data Team, you’re required to understand the data requirements, specify the modeling needs of datasets and use existing tech stack solutions for efficient data ingestion workflows and data pipelining. You will implement technical solutions using programming, machine learning, AI, and human-in-the-loop approaches to make sure our data is fit-for-purpose for our clients. You will work closely with our Engineering partners, our Data Product Managers as well as Product teams, so you need to be able to coordinate with multi-disciplinary and regional teams and have experience in project management and stakeholder engagement. You will need to be comfortable working with large, varied, sophisticated and often unstructured data sets and you will need to demonstrate strong experience in data engineering.
We trust you to:
● Build database schema and configure ETL software to onboard new data sets.
● Analyze internal processes to find opportunities for improvement, as well as devise and implement innovative solutions.
● Build quality data workflows to verify and validate third party data.
● Contribute to the technical implementation of a new Physical Assets Data Model.
● Maintain workflow configurations for critical functions such as acquisition, worklist management, and quality control.
● Contribute to the creation of best practices and guidelines for data governance.
● Partner with Engineering and Product to propose, develop and implement market leading solutions for our clients.
● Understand customer needs and markets to ensure our data sets are fit-for-purpose and seamlessly integrate with other data products when developing data product strategies.
● Stay updated on market, industry and dataset developments related to your area of support.
● Make well-informed decisions in a fast-paced, ever-changing environment.
● Report on results of on-going operations and projects, as required.
You’ll need to have:
● A BA/BS degree or higher in Computer Science, Mathematics, or relevant data technology field, or equivalent professional work experience.
● 4+ years of programming and scripting in a production environment (Python, JavaScript, etc.).
● 4+ years of experience working with databases (either SQL or NoSQL).
● Understanding and experience of large-scale, distributed systems as well as ETL logics.
● The ability to think creatively and provide out of the box solutions with an eagerness to learn and collaborate.
● Familiarity with data processing paradigms and associated tools and technologies.
● Strong passion for data, exceptional problem-solving skills, and high attention to detail.
● Excellent written and verbal communication skills, especially when explaining technical processes and solutions to business partners and management.
● Ability to work independently as well as in a distributed team environment.
We’d love to see:
● Track record of collaborating with Engineering to promote code to production.
● Experience and engagement with the energy transition movement.
● Knowledge of AI and Machine Learning frameworks.
● Experience in conducting technical training and mentoring others.
● Proficiency and previous experience working with Bloomberg Data tech stack.
Does this sound like you?
Apply if you think we're a good match. We'll get in touch to let you know what the next steps are!
Salary Range = 110000-190000 USD Annually+ Benefits + Bonus
The referenced salary range is based on the Company's good faith belief at the time of posting. Actual compensation may vary based on factors such as geographic location, work experience, market conditions, education/training and skill level.
We offer one of the most comprehensive and generous benefits plans available and offer a range of total rewards that may include merit increases, incentive compensation (exempt roles only), paid holidays, paid time off, medical, dental, vision, short and long term disability benefits, 401(k) +match, life insurance, and various wellness programs, among others. The Company does not provide benefits directly to contingent workers/contractors and interns.
Discover what makes Bloomberg unique - watch our [podcast series](https://www.youtube.com/playlist?list=PLnZuxOufsXnskmdKsB1GgrEKtmNqCdsbr) for an inside look at our culture, values, and the people behind our success.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Bloomberg runs on data. Our pr` | Valid description (5245 chars, 771 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Senior Data Management Profess` | Valid description (5470 chars, 806 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Senior Data Management Profess` | Valid description (5470 chars, 806 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Senior Data Management Professional - Data Engineer - BNEF Data` |
| Company | `Bloomberg` |
| Location | `Skillman` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1768599231655` |
| Description Words | `771` |
| Cost (milli-cents) | `102` |
| URL | `https://bloomberg.avature.net/careers/JobDetail/Senior-Data-Management-Professional-Data-Engineer-BNEF-Data/16762` |

**Description Preview (first 200 words):**

```
Bloomberg runs on data. Our products are fueled by powerful information. We combine data and context to paint the whole picture for our clients, around the clock – from around the world. In Data, we are responsible for delivering this data, news and analytics through innovative technology - quickly and accurately. We apply problem-solving skills to identify innovative workflow efficiencies, and we implement technology solutions to enhance our systems, products and processes. The BNEF Data Team maintains databases for renewable and conventional power plants, storage projects and manufacturing plants globally. The team is currently working on a new future-proof data model and workflow that can facilitate and accelerate coverage expansion for integrated use in downstream analysis across our customer groups (including governments, portfolio managers, corporations, equity analysts etc.). The Role: As a Data Engineer on the BNEF Data Team, you’re required to understand the data requirements, specify the modeling needs of datasets and use existing tech stack solutions for efficient data ingestion workflows and data pipelining. You will implement technical solutions using programming, machine learning, AI, and human-in-the-loop approaches to make sure our data is fit-for-purpose for our clients. You will work closely with our Engineering partners, our Data Product...
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
  "title": "Senior Data Management Professional - Data Engineer - BNEF Data - 16762 - Bloomberg",
  "company": "Bloomberg",
  "location": "Skillman",
  "description": "Senior Data Management Professional - Data Engineer - BNEF Data\n- 16762\n- Bloomberg\nSenior Data Management Professional - Data Engineer - BNEF Data\nLocation\nSkillman\nBusiness Area\nData\nRef #\n##\nDescription &amp; Requirements\nBloomberg runs on data. Our products are fueled by powerful information. We combine data and context to paint the whole picture for our clients, around the clock – from around the world. In Data, we are responsible for delivering this data, news and analytics through innovat...",
  "url": "https://bloomberg.avature.net/careers/JobDetail/Senior-Data-Management-Professional-Data-Engineer-BNEF-Data/16762",
  "posted_at": 1768599231655,
  "level": "senior",
  "remote": false,
  "cost_milli_cents": 102,
  "_full_description_word_count": 806
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 102,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
