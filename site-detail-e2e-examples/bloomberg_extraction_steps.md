# Extraction Steps: bloomberg

**Detail URL:** `https://bloomberg.avature.net/careers/JobDetail/Senior-Software-Engineer-Editorial-Apps-Platforms/16497`
**Source URL:** `https://bloomberg.avature.net/careers/JobDetail/Senior-Software-Engineer-Editorial-Apps-Platforms/16497`
**Handler:** `avature`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Senior Software Engineer - Editorial Apps &amp; Platforms
- 16497
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
Senior Software Engineer - Editorial Apps &amp; Platforms
Location
London
Business Area
Engineering and CTO
Ref #
10048322
##
Description &amp; Requirements
Bloomberg News, Research, and Media make up one of the world’s largest and most influential news organizations - more than 2,700 journalists and analysts across 120+ countries - delivering market-moving reporting at global scale. Our award-winning journalism, recognized with multiple Pulitzer Prizes, Loeb Awards, and Emmys, drives decision-making for business leaders, investors, and policymakers every day.
We are responsible for the full product suite behind Blo

... (truncated, 9792 total chars)
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
Senior Software Engineer - Editorial Apps &amp; Platforms
- 16497
- Bloomberg
Senior Software Engineer - Editorial Apps &amp; Platforms
Location
London
Business Area
Engineering and CTO
Ref #
10048322
##
Description &amp; Requirements
Bloomberg News, Research, and Media make up one of the world’s largest and most influential news organizations - more than 2,700 journalists and analysts across 120+ countries - delivering market-moving reporting at global scale. Our award-winning journalism, recognized with multiple Pulitzer Prizes, Loeb Awards, and Emmys, drives decision-making for business leaders, investors, and policymakers every day.
We are responsible for the full product suite behind Bloomberg’s editorial and research workflow - including content discovery platforms, analyst coding environments, research &amp; content management systems.
Our teams builds tools that enable content creators to:
* Discover insights buried in massive datasets
* Write, edit, visualize, and publish high-impact stories
* Collaborate to build content in real time
* Personalize workflows across editorial domains
* Accelerate research and reduce time-to-market
* Leverage AI to uncover narratives and optimize content delivery
**Our mission is simple: **
***Build software to generate real-time content, visual insights, and analysis that give Bloomberg clients an edge in monitoring markets around the world. ***
This mission unifies the entire Editorial Apps &amp; Platforms portfolio - from discovery to data analysis to publishing - ensuring Bloomberg’s content creators can produce trusted, market-moving insights with unprecedented speed and intelligence.
**Where We’re Going
**
In 2026 and beyond, we’re bringing these ecosystems together under a unified vision:
* AI-native editorial workflows (story optimization, metadata inference, human-in-the-loop content drafting)
* Personalized content tools based on user roles, beats, and expertise
* Faster, more robust publishing pipelines, helping us

... (truncated, 4686 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: avature

```json
{
  "url": "https://bloomberg.avature.net/careers/JobDetail/Senior-Software-Engineer-Editorial-Apps-Platforms/16497",
  "handler": "avature"
}
```

### Raw Content Capture

Captured 9792 chars of commonmark content

```json
{
  "length": 9792,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 4686 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 4686
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://bloomberg.avature.net/careers/JobDetail/Senior-Software-Engineer-Editorial-Apps-Platforms/16497",
      "sourceUrl": "https://bloomberg.avature.net/careers/JobDetail/Senior-Software-Engineer-Editorial-Apps-Platforms/16497",
      "provider": "spidercloud",
      "siteId": "bloomberg",
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
    "value": "Senior Software Engineer - Editorial Apps & Platforms"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Bloomberg"
  },
  "location": {
    "winner": "site_handler_location_hint",
    "value": "London"
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
    "value": 142
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:33:08.511000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Bloomberg News, Research, and Media make up one of the world’s largest and most influential news organizations - more than 2,700 journalists and analysts across 120+ countries - delivering market-moving reporting at global scale. Our award-winning journalism, recognized with multiple Pulitzer Prizes, Loeb Awards, and Emmys, drives decision-making for business leaders, investors, and policymakers every day.\nWe are responsible for the full product suite behind Bloomberg’s editorial and research workflow - including content discovery platforms, analyst coding environments, research &amp; content management systems.\nOur teams builds
```

### Heuristic Title Override

Title changed from 'Senior Software Engineer - Editorial Apps & Platforms - 16497 - Bloomberg' to 'Senior Software Engineer - Editorial Apps & Platforms'

```json
{
  "original_title": "Senior Software Engineer - Editorial Apps & Platforms - 16497 - Bloomberg",
  "patched_title": "Senior Software Engineer - Editorial Apps & Platforms",
  "patch": {
    "heuristicAttempts": 1,
    "heuristicLastTried": 1768599188544,
    "heuristicVersion": 5,
    "title": "Senior Software Engineer - Editorial Apps & Platforms",
    "jobTitle": "Senior Software Engineer - Editorial Apps & Platforms",
    "locations": [
      "London, United Kingdom"
    ],
    "location": "London, United Kingdom",
    "locationStates": [
      "United Kingdom"
    ],
    "locationSearch": "United Kingdom London",
    "countries": [
      "United Kingdom"
    ],
    "country": "United Kingdom",
    "description": "Bloomberg News, Research, and Media make up one of the world’s largest and most influential news organizations - more than 2,700 journalists and analysts across 120+ countries - delivering market-moving reporting at global scale. Our award-winning journalism, recognized with multiple Pulitzer Prizes, Loeb Awards, and Emmys, drives decision-making for business leaders, investors, and policymakers every day.\nWe are responsible for the full product suite behind Bloomberg’s editorial and research workflow - including content discovery platforms, analyst coding environments, research &amp; content management systems.\nOur teams builds tools that enable content creators to:\n* Discover insights buried in massive datasets\n* Write, edit, visualize, and publish high-i
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**⚠️ TITLE CHANGED:**
- Original: `Senior Software Engineer - Editorial Apps & Platforms - 16497 - Bloomberg`
- After Heuristics: `Senior Software Engineer - Editorial Apps & Platforms`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599188544,
  "heuristicVersion": 5,
  "title": "Senior Software Engineer - Editorial Apps & Platforms",
  "jobTitle": "Senior Software Engineer - Editorial Apps & Platforms",
  "locations": [
    "London, United Kingdom"
  ],
  "location": "London, United Kingdom",
  "locationStates": [
    "United Kingdom"
  ],
  "locationSearch": "United Kingdom London",
  "countries": [
    "United Kingdom"
  ],
  "country": "United Kingdom",
  "description": "Bloomberg News, Research, and Media make up one of the world’s largest and most influential news organizations - more than 2,700 journalists and analysts across 120+ countries - delivering market-moving reporting at global scale. Our award-winning journalism, recognized with multiple Pulitzer Prizes, Loeb Awards, and Emmys, drives decision-making for business leaders, investors, and policymakers every day.\nWe are responsible for the full product suite behind Bloomberg’s editorial and res
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `raw_row_title` | `Senior Software Engineer - Editorial Apps & Platfo` |
| company | `raw_row_company` | `Bloomberg` |
| location | `site_handler_location_hint` | `London` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `senior` |
| compensation | `unknown_compensation` | `(none)` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `142` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:33:08.511000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `Bloomberg News, Research, and Media make up one of` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Senior Software Engineer - Editorial Apps & Platforms`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'avature' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Senior Software Engineer - Edi` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | Generic title rejected: Description &amp; Requirem |
| hinted_title | HEURISTIC | ✅ | `Senior Software Engineer - Edi` | Valid title |
| first_line_title | FALLBACK | ✅ | `Our teams builds tools that en` | Valid title |

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

**Final Value:** `London`
**Winning Strategy:** `site_handler_location_hint`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_location_hint** 🏆 | SITE_HANDLER | ✅ | `London` | Valid location |
| raw_row_location | EXPLICIT_FIELD | ✅ | `London` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `London` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `New York, NY` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'London' present but not inferring remote |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
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

**Final Value:** `142`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `142` | Valid cost: 142 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:33:08.511000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:33:08.511000` | Valid date: 2026-01-16T14:33:08.511000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'avature' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:08.555867` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'avature' returned no first_published |

#### DESCRIPTION

**Final Value:** `Bloomberg News, Research, and Media make up one of the world’s largest and most influential news organizations - more than 2,700 journalists and analysts across 120+ countries - delivering market-moving reporting at global scale. Our award-winning journalism, recognized with multiple Pulitzer Prizes, Loeb Awards, and Emmys, drives decision-making for business leaders, investors, and policymakers every day.
We are responsible for the full product suite behind Bloomberg’s editorial and research workflow - including content discovery platforms, analyst coding environments, research &amp; content management systems.
Our teams builds tools that enable content creators to:
* Discover insights buried in massive datasets
* Write, edit, visualize, and publish high-impact stories
* Collaborate to build content in real time
* Personalize workflows across editorial domains
* Accelerate research and reduce time-to-market
* Leverage AI to uncover narratives and optimize content delivery
**Our mission is simple: **
***Build software to generate real-time content, visual insights, and analysis that give Bloomberg clients an edge in monitoring markets around the world. ***
This mission unifies the entire Editorial Apps &amp; Platforms portfolio - from discovery to data analysis to publishing - ensuring Bloomberg’s content creators can produce trusted, market-moving insights with unprecedented speed and intelligence.
**Where We’re Going
**
In 2026 and beyond, we’re bringing these ecosystems together under a unified vision:
* AI-native editorial workflows (story optimization, metadata inference, human-in-the-loop content drafting)
* Personalized content tools based on user roles, beats, and expertise
* Faster, more robust publishing pipelines, helping us automate how editors promote stories
* Cross-platform editorial collaboration
* Modernized web-first authoring and data analysis tools
* Consistent metadata governance &amp; quality
**What You’ll Do:
**
*As a Senior Software Engineer in Editorial Apps &amp; Platforms, you will: *
* Build scalable, full-stack applications using technologies such as Python, Typescript, React/NextJS, Kafka, Redis and Bloomberg’s internal platforms.
* Own the full software development lifecycle: architecture, design, coding, testing, deployment, and support.
* Work directly with editors, reporters, analysts, economists, researchers, and product managers to understand their workflows and craft intuitive, high-impact solutions.
* Develop re-usable federated web applications powering editorial and research content creation.
* Contribute to the modernization of our CMS platforms, coding environments, and content discovery tools.
* Experiment with AI, NLP, and automation to improve quality, reduce friction, and accelerate content creation.
* Uphold high standards for code quality, testing, CI, resilience, and maintainability.
**You'll Need to Have:
**
* Strong experience in an object-oriented programming language (Python, C++, Java, etc.).
* Experience building robust, production-grade full-stack applications.
* Ability to collaborate with non-technical and technical users alike.
* A passion for enabling others - building tools that empower content creators.
**We'd love to see:
**
* Experience with React/NextJS, Typescript, modern web UI patterns, and real-time collaboration frameworks.
* Background in data pipelines, large-scale data processing, Kafka, Trino, Iceberg, or analytics tooling.
* Experience in CMS development, newsroom engineering, or news vendor applications.
* Interest in NLP, generative AI, personalization systems, or content quality models.
* Experience mentoring teammates and shaping engineering culture.
**Why You’ll Love Working With Us
**
* Direct impact on how the world’s most influential newsroom and research organization operates.
* Close partnerships with journalists and analysts - real users with mission-critical needs.
* A collaborative, supportive engineering culture across New York, London, and Princeton.
* Opportunities to work across multiple product areas, from CMS to data platforms to editorial AI.
* Regular team events, hackathons, cross-Atlantic collaboration, and a thriving community of content-focused engineers.
Discover what makes Bloomberg unique - watch our [podcast series](https://www.youtube.com/playlist?list=PLnZuxOufsXnskmdKsB1GgrEKtmNqCdsbr) for an inside look at our culture, values, and the people behind our success.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Bloomberg News, Research, and ` | Valid description (4449 chars, 603 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Senior Software Engineer - Edi` | Valid description (4675 chars, 636 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Senior Software Engineer - Edi` | Valid description (4675 chars, 636 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Senior Software Engineer - Editorial Apps & Platforms` |
| Company | `Bloomberg` |
| Location | `London, United Kingdom` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1768599188511` |
| Description Words | `603` |
| Cost (milli-cents) | `142` |
| URL | `https://bloomberg.avature.net/careers/JobDetail/Senior-Software-Engineer-Editorial-Apps-Platforms/16497` |

**Description Preview (first 200 words):**

```
Bloomberg News, Research, and Media make up one of the world’s largest and most influential news organizations - more than 2,700 journalists and analysts across 120+ countries - delivering market-moving reporting at global scale. Our award-winning journalism, recognized with multiple Pulitzer Prizes, Loeb Awards, and Emmys, drives decision-making for business leaders, investors, and policymakers every day. We are responsible for the full product suite behind Bloomberg’s editorial and research workflow - including content discovery platforms, analyst coding environments, research &amp; content management systems. Our teams builds tools that enable content creators to: * Discover insights buried in massive datasets * Write, edit, visualize, and publish high-impact stories * Collaborate to build content in real time * Personalize workflows across editorial domains * Accelerate research and reduce time-to-market * Leverage AI to uncover narratives and optimize content delivery **Our mission is simple: ** ***Build software to generate real-time content, visual insights, and analysis that give Bloomberg clients an edge in monitoring markets around the world. *** This mission unifies the entire Editorial Apps &amp; Platforms portfolio - from discovery to data analysis to publishing - ensuring Bloomberg’s content creators can produce trusted, market-moving insights with unprecedented speed and intelligence. **Where We’re...
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
  "title": "Senior Software Engineer - Editorial Apps & Platforms - 16497 - Bloomberg",
  "company": "Bloomberg",
  "location": "London",
  "description": "Senior Software Engineer - Editorial Apps &amp; Platforms\n- 16497\n- Bloomberg\nSenior Software Engineer - Editorial Apps &amp; Platforms\nLocation\nLondon\nBusiness Area\nEngineering and CTO\nRef #\n##\nDescription &amp; Requirements\nBloomberg News, Research, and Media make up one of the world’s largest and most influential news organizations - more than 2,700 journalists and analysts across 120+ countries - delivering market-moving reporting at global scale. Our award-winning journalism, recognized wit...",
  "url": "https://bloomberg.avature.net/careers/JobDetail/Senior-Software-Engineer-Editorial-Apps-Platforms/16497",
  "posted_at": 1768599188511,
  "level": "senior",
  "remote": false,
  "cost_milli_cents": 142,
  "_full_description_word_count": 636
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 142,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
