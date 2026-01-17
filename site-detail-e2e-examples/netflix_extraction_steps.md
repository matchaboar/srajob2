# Extraction Steps: netflix

**Detail URL:** `https://explore.jobs.netflix.net/careers/job/790312429476`
**Source URL:** `https://explore.jobs.netflix.net/careers/job/790312429476`
**Handler:** `netflix`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Systems Development Engineer L5 | California - Remote,United States of America | Netflix
`{"themeOptions": {"customTheme": {"customFonts": [{"src": "url(\\"https://static.vscdn.net/images/careers/demo/netflix/1721052598::NetflixSans\_W\_Lt.woff\\") format(\\"woff\\")", "fontFamily": "NetflixSans"}], "varTheme": {"navbar-text-color": "#ffffff", "primary-color": "#E50914", "primary-color-10": "#ffd6d1", "primary-color-20": "#ffd6d1", "primary-color-30": "#ffaca6", "primary-color-40": "#ff7f7a", "primary-color-50": "#f04a4a", "primary-color-60": "#E50914", "primary-color-70": "#b3101b", "primary-color-80": "#8c0613", "primary-color-90": "#66000e", "primary-color-100": "#40000b", "accent-color": "#221F1F", "accent-color-10": "#737373", "accent-color-20": "#5c5c5c", "accent-color-30": "#454545", "accent-color-40": "#2e2e2e", "accent-color-50": "#171717", "accent-color-60": "#221F1F", "accent-color-70": "#221F1F", "accent-color-80": "#221F1F", "accent-color-90": "#221F1F", "accent-color-100": "#221F1F", "border-radius-l": "18px", "border-radius-m": "0px", "button-primary-background-color": "#E50914", "button-primary-active-text-color": "#e6e6e6", "button-primary-text-color": "#ffffff", "button-primary-hover-background-color": "#E50914", "button-secondary-hover-text-color": "#221F1F", "button-secondary-hover-background-color": "#ffffff", "card-border-radius": "0", "button-secondary-border-color": "#c51818", "button-secondary-active-background-color": "#221F1F", "text-tertiary-color": "#d0021b", "background-color": "#f5f5f1", "card-background-color": "#ffffff", "border-radius-s": "0px", "navbar-background": "#221F1F", "navbar-text-hover-background": "#221F1F", "navbar-text-hover-color": "E50914", "border-radius-round": "0%", "border-radius-xs": "0px", "input-background-color": "#ffffff", "navbar-backdrop-filter": "undefined", "navbar-border-bottom": "0px", "button-default-background-color": "#ce3b2b", "input-placeholder-text-color": "#221F1F", "input-border-color": "black", "input-border-color-active": "#E50914", "input-border-radius": "3px", "legacy-jobcard-selected-title-background-color": "#E50914", "legacy-jobcard-selected-title-text-color": "white", "legacy-jobcard-title-background-color": "#221f1f", "card-border-color": "#f5f5f1", "border-color": "#221f1f", "upload-resume-modal-background": "#494949", "position-card-position-title-color-unselected": "#ffffff", "shadow-card-box-shadow": "2px 2px 2px 1px rgba(0, 0, 0, 0.2)", "position-card-position-title-color-selected": "#221F1F", "legacy-jobcard-title-text-color": "var(--white-color)", "jobcard-good-match-icon": "#E50914", "jobcard-strong-match-icon": "#E50914", "nav-button-border-color": "#221F1F", "nav-button-background-color": "#221F1F", "position-description-card-background": "var(--white-color)", "position-description-card-box-shadow": "0px 4px 16px 0px grey", "pcs-personalization-bar-background": "var(--accent-color)", "font-family": "NetflixSans", "font-stack": "NetflixSans", "search-text-bo

... (truncated, 27696 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `netflix`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `Systems Development Engineer L5`

Normalized markdown after handler processing:

```markdown
# Systems Development Engineer L5
California - Remote, United States of America
#### Job Posting Date
11-03-2025
#### Job Requisition ID
JR36538
#### Teams
Engineering
#### Work Type
Remote
Netflix is one of the world's leading entertainment services, with over 300 million paid memberships in over 190 countries enjoying TV series, films and games across a wide variety of genres and languages. Members can play, pause and resume watching as much as they want, anytime, anywhere, and can change their plans at any time.
The Production Systems Engineering team is responsible for designing and delivering scalable technical solutions in support of Netflix’s content creation businesses. We excel at delivering thoughtfully designed solutions and take pride in our ability to partner with our cross-functional engineering teams to provide the best possible outcome for our stakeholders. Though technical in nature, these solutions empower our creative businesses and bring value to millions of subscribers worldwide.
Our team is looking for an experienced Senior Systems Development Engineer to help us design, develop, and deploy these solutions in support of our stunning colleagues in content. You will work with internal product and engineering teams, technical creatives, production teams, and external vendors to deliver these solutions and infrastructure. You will bring a broad set of technical skills and a career that has influenced your mind into one that constantly asks “how can I make this scalable?”
**As a member of the team, you will...**
* Design, implement and support the services and solutions that improve the experience and productivity for a broad range of creative content domains (e.g. post-production, animation, visual effects, genAI, virtual production).
* Work at a scale where your deployments will have a wide-reaching impact, across multiple studios, supporting distributed teams in multiple regions.
* Design, automate, and develop solutions to enhance the efficiency

... (truncated, 4825 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: netflix

```json
{
  "url": "https://explore.jobs.netflix.net/careers/job/790312429476",
  "handler": "netflix"
}
```

### Raw Content Capture

Captured 27696 chars of commonmark content

```json
{
  "length": 27696,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='Systems Development Engineer L5', 4825 chars of normalized content

```json
{
  "title": "Systems Development Engineer L5",
  "normalized_length": 4825
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://explore.jobs.netflix.net/careers/job/790312429476",
      "sourceUrl": "https://explore.jobs.netflix.net/careers/job/790312429476",
      "provider": "spidercloud",
      "siteId": "netflix",
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
    "value": "Systems Development Engineer L5"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Netflix"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Remote"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": true
  },
  "level": {
    "winner": "content_pattern_level",
    "value": "senior"
  },
  "compensation": {
    "winner": "hinted_compensation",
    "value": 700000
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 940
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:34:20.486000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "# Systems Development Engineer L5\nCalifornia - Remote, United States of America\n#### Job Posting Date\n11-03-2025\n#### Job Requisition ID\nJR36538\n#### Teams\nEngineering\n#### Work Type\nRemote\nNetflix is one of the world's leading entertainment services, with over 300 million paid memberships in over 190 countries enjoying TV series, films and games across a wide variety of genres and languages. Members can play, pause and resume watching as much as they want, anytime, anywhere, and can change their plans at any time.\nThe Production Systems Engineering team is responsible for designing and delivering scalable technical solutions in support of Netf
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Systems Development Engineer L5`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599260527,
  "heuristicVersion": 5,
  "locations": [
    "Remote"
  ],
  "location": "Remote",
  "locationStates": [],
  "locationSearch": "Remote",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "totalCompensation": 700000,
  "compensationUnknown": false,
  "compensationReason": "extractor:hinted_compensation",
  "level": "senior"
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
| title | `site_handler_title` | `Systems Development Engineer L5` |
| company | `raw_row_company` | `Netflix` |
| location | `raw_row_location` | `Remote` |
| remote | `explicit_remote_flag` | `True` |
| level | `content_pattern_level` | `senior` |
| compensation | `hinted_compensation` | `700000` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `940` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:34:20.486000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `# Systems Development Engineer L5
California - Rem` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Systems Development Engineer L5`
**Winning Strategy:** `site_handler_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **site_handler_title** 🏆 | SITE_HANDLER | ✅ | `Systems Development Engineer L` | Valid title |
| raw_row_title | EXPLICIT_FIELD | ✅ | `Systems Development Engineer L` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `Systems Development Engineer L` | Valid title |
| hinted_title | HEURISTIC | ✅ | `Systems Development Engineer L` | Valid title |
| first_line_title | FALLBACK | ✅ | `Systems Development Engineer L` | Valid title |

#### COMPANY

**Final Value:** `Netflix`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'netflix' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Netflix` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Netflix` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `Inclusion` | Found company in markdown link |
| hinted_company | HEURISTIC | ✅ | `you` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Remote`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'netflix' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Remote` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `California - Remote, United St` | Matched pattern LOCATION_FULL |
| country_only_fallback_location | CUSTOM_550 | ✅ | `Remote` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ❌ | `` | Remote-only location 'Remote' skipped |
| remote_fallback_location | FALLBACK | ✅ | `Remote` | Job marked as remote, using 'Remote' as location |

#### REMOTE

**Final Value:** `True`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `True` | Explicit boolean remote=True |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ✅ | `True` | Location contains 'remote': Remote |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ✅ | `True` | Content contains remote pattern at position 47 |
| hinted_remote | HEURISTIC | ✅ | `True` | Remote from hints: True |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Netflix' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `senior`
**Winning Strategy:** `content_pattern_level`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_level_field | STRUCTURED_DATA | ❌ | `` | Skipping mid-level default: mid |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ❌ | `` | No level indicator in title |
| **content_pattern_level** 🏆 | CUSTOM_550 | ✅ | `senior` | Level from experience: 6+ years -> senior |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `700000`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `700000` | Compensation from hints: $700,000 |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `400000` | Compensation range pattern: $100,000-$700,000 -> $ |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `940`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `940` | Valid cost: 940 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:34:20.486000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:34:20.486000` | Valid date: 2026-01-16T14:34:20.486000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'netflix' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:34:20.542104` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'netflix' returned no first_published |

#### DESCRIPTION

**Final Value:** `# Systems Development Engineer L5
California - Remote, United States of America
#### Job Posting Date
11-03-2025
#### Job Requisition ID
JR36538
#### Teams
Engineering
#### Work Type
Remote
Netflix is one of the world's leading entertainment services, with over 300 million paid memberships in over 190 countries enjoying TV series, films and games across a wide variety of genres and languages. Members can play, pause and resume watching as much as they want, anytime, anywhere, and can change their plans at any time.
The Production Systems Engineering team is responsible for designing and delivering scalable technical solutions in support of Netflix’s content creation businesses. We excel at delivering thoughtfully designed solutions and take pride in our ability to partner with our cross-functional engineering teams to provide the best possible outcome for our stakeholders. Though technical in nature, these solutions empower our creative businesses and bring value to millions of subscribers worldwide.
Our team is looking for an experienced Senior Systems Development Engineer to help us design, develop, and deploy these solutions in support of our stunning colleagues in content. You will work with internal product and engineering teams, technical creatives, production teams, and external vendors to deliver these solutions and infrastructure. You will bring a broad set of technical skills and a career that has influenced your mind into one that constantly asks “how can I make this scalable?”
**As a member of the team, you will...**
* Design, implement and support the services and solutions that improve the experience and productivity for a broad range of creative content domains (e.g. post-production, animation, visual effects, genAI, virtual production).
* Work at a scale where your deployments will have a wide-reaching impact, across multiple studios, supporting distributed teams in multiple regions.
* Design, automate, and develop solutions to enhance the efficiency and resiliency of our systems and services by taking into account hybrid environments and planning for them.
* Maintain and extend internal tooling and automation, including configuration management and service based infrastructure management.
* Participate in system and infrastructure updates and migrations, including on-call rotation, as well as troubleshooting tasks to maintain system resiliency and performance.
**About you**
* You have a minimum of 6 years of experience in developing, automating and deploying complex systems to deliver technology solutions to end-users
* You are able to distill and understand large, complex systems into their most critical components and understand the value each brings to the overall solution, spanning from hardware-level to software architectures.
* You have a passion for automation, efficiency, and scalability, consistently driving operational excellence.
* You excel at identifying high-leverage, high-value opportunities and focus your attention on them independently.
* You enjoy and can effectively work collaboratively with business and product teams to develop product plans and roadmaps and then are able to lead the hands-on implementation of those plans.
* You use data-driven decision making, applying thorough analytics and real-time insights for strategic and operational decisions.
* Broad experience deploying configuration management across multiple platforms (Linux, Windows, macOS) with disparate methodologies.
**Nice to have**
* Experience in VFX, animation, or a similar creative services industry
Our compensation structure consists solely of an annual salary; we do not have bonuses. You choose each year how much of your compensation you want in salary versus stock options. To determine your personal top of market compensation, we rely on market indicators and consider your specific job family, background, skills, and experience to determine your compensation in the market range. The range for this role is $100K - $700K.
[Inclusion](https://about.netflix.com/en/inclusion) is a Netflix value and we strive to host a meaningful interview experience for all candidates. If you want an accommodation/adjustment for a disability or any other reason during the hiring process, please send a request to your recruiting partner.
We are an equal-opportunity employer and celebrate diversity, recognizing that diversity builds stronger teams. We approach diversity and inclusion seriously and thoughtfully. We do not discriminate on the basis of race, religion, color, ancestry, national origin, caste, sex, sexual orientation, gender, gender identity or expression, age, disability, medical condition, pregnancy, genetic makeup, marital status, or military service.
Job is open for no less than 7 days and will be removed when the position is filled.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `# Systems Development Engineer` | Valid description (4825 chars, 706 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `# Systems Development Engineer` | Valid description (4825 chars, 706 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `# Systems Development Engineer` | Valid description (4825 chars, 706 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Systems Development Engineer L5` |
| Company | `Netflix` |
| Location | `Remote` |
| Is Remote | `True` |
| Level | `senior` |
| Posted At | `1768599260486` |
| Description Words | `706` |
| Cost (milli-cents) | `940` |
| URL | `https://explore.jobs.netflix.net/careers/job/790312429476` |

**Description Preview (first 200 words):**

```
# Systems Development Engineer L5 California - Remote, United States of America #### Job Posting Date 11-03-2025 #### Job Requisition ID JR36538 #### Teams Engineering #### Work Type Remote Netflix is one of the world's leading entertainment services, with over 300 million paid memberships in over 190 countries enjoying TV series, films and games across a wide variety of genres and languages. Members can play, pause and resume watching as much as they want, anytime, anywhere, and can change their plans at any time. The Production Systems Engineering team is responsible for designing and delivering scalable technical solutions in support of Netflix’s content creation businesses. We excel at delivering thoughtfully designed solutions and take pride in our ability to partner with our cross-functional engineering teams to provide the best possible outcome for our stakeholders. Though technical in nature, these solutions empower our creative businesses and bring value to millions of subscribers worldwide. Our team is looking for an experienced Senior Systems Development Engineer to help us design, develop, and deploy these solutions in support of our stunning colleagues in content. You will work with internal product and engineering teams, technical creatives, production teams, and external vendors to deliver these solutions...
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
  "title": "Systems Development Engineer L5",
  "company": "Netflix",
  "location": "Remote",
  "description": "# Systems Development Engineer L5\nCalifornia - Remote, United States of America\n#### Job Posting Date\n11-03-2025\n#### Job Requisition ID\nJR36538\n#### Teams\nEngineering\n#### Work Type\nRemote\nNetflix is one of the world's leading entertainment services, with over 300 million paid memberships in over 190 countries enjoying TV series, films and games across a wide variety of genres and languages. Members can play, pause and resume watching as much as they want, anytime, anywhere, and can change thei...",
  "url": "https://explore.jobs.netflix.net/careers/job/790312429476",
  "posted_at": 1768599260486,
  "level": "mid",
  "remote": true,
  "cost_milli_cents": 940,
  "_full_description_word_count": 706
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 940,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
