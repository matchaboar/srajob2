# Extraction Steps: ashby

**Detail URL:** `https://jobs.ashbyhq.com/notion/0fe96f47-a9ea-4623-ac73-9053c3541297`
**Source URL:** `https://jobs.ashbyhq.com/notion/0fe96f47-a9ea-4623-ac73-9053c3541297`
**Handler:** `ashby`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
Account Executive, Mid-Market @ Notion
```

---

## Step 2: Handler Detection

**Detected Handler:** `ashby`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
Account Executive, Mid-Market @ Notion
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: ashby

```json
{
  "url": "https://jobs.ashbyhq.com/notion/0fe96f47-a9ea-4623-ac73-9053c3541297",
  "handler": "ashby"
}
```

### Raw Content Capture

Captured 38 chars of commonmark content

```json
{
  "length": 38,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 38 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 38
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://jobs.ashbyhq.com/notion/0fe96f47-a9ea-4623-ac73-9053c3541297",
      "sourceUrl": "https://jobs.ashbyhq.com/notion/0fe96f47-a9ea-4623-ac73-9053c3541297",
      "provider": "spidercloud",
      "siteId": "ashby",
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
    "value": "Account Executive, Mid-Market"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Notion"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "New York, United States"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": false
  },
  "level": {
    "winner": "title_level",
    "value": "mid"
  },
  "compensation": {
    "winner": "hinted_compensation",
    "value": 205000
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 20
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:34:21.543000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "# Account Executive, Mid-Market\n\nABOUT US: Notion helps you build beautiful tools for your life’s work. In today's world of endless apps and tabs, Notion provides one place for teams to get everything done, seamlessly connecting docs, notes, projects, calendar, and email—with AI built in to find answers and automate work. Millions of users, from individuals to large organizations like Toyota, Figma, and OpenAI, love Notion for its flexibility and choose it because it helps them save time and money. In-person collaboration is essential to Notion's culture. We require all team members to work from our offices on Mondays and Thursdays, our designated Anchor Da
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Account Executive, Mid-Market`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599261583,
  "heuristicVersion": 5,
  "locations": [
    "New York, NY"
  ],
  "location": "New York, NY",
  "locationStates": [
    "NY"
  ],
  "locationSearch": "New York NY",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "totalCompensation": 205000,
  "compensationUnknown": false,
  "compensationReason": "extractor:hinted_compensation"
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
| title | `raw_row_title` | `Account Executive, Mid-Market` |
| company | `raw_row_company` | `Notion` |
| location | `raw_row_location` | `New York, United States` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `title_level` | `mid` |
| compensation | `hinted_compensation` | `205000` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `20` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:34:21.543000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `# Account Executive, Mid-Market

ABOUT US: Notion ` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Account Executive, Mid-Market`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'ashby' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Account Executive, Mid-Market` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `Account Executive, Mid-Market` | Valid title |
| hinted_title | HEURISTIC | ✅ | `Account Executive, Mid-Market` | Valid title |
| first_line_title | FALLBACK | ✅ | `Account Executive, Mid-Market` | Valid title |

#### COMPANY

**Final Value:** `Notion`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'ashby' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Notion` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Notion` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `the GTM Sales Team` | Found 'Join Company' pattern |
| hinted_company | HEURISTIC | ❌ | `` | Company name too long: 1748 chars |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `New York, United States`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'ashby' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `New York, United States` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `New York` | Matched pattern BASED_IN |
| country_only_fallback_location | CUSTOM_550 | ✅ | `New York, United States` | Country-only fallback: Valid location |
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
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'New York, United States' present but not |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Notion' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `mid`
**Winning Strategy:** `title_level`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_level_field | STRUCTURED_DATA | ❌ | `` | Skipping mid-level default: mid |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| **title_level** 🏆 | CONTENT_PATTERN | ✅ | `mid` | Level from title: 'mid' -> mid |
| content_pattern_level | CUSTOM_550 | ✅ | `mid` | Level from content: 'Mid' -> mid |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `205000`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `205000` | Compensation from hint range: $180,000-$230,000 -> |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `205000` | Compensation range pattern: $180,000-$230,000 -> $ |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `20`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `20` | Valid cost: 20 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:34:21.543000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:34:21.543000` | Valid date: 2026-01-16T14:34:21.543000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'ashby' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:34:21.611021` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'ashby' returned no first_published |

#### DESCRIPTION

**Final Value:** `# Account Executive, Mid-Market

ABOUT US: Notion helps you build beautiful tools for your life’s work. In today's world of endless apps and tabs, Notion provides one place for teams to get everything done, seamlessly connecting docs, notes, projects, calendar, and email—with AI built in to find answers and automate work. Millions of users, from individuals to large organizations like Toyota, Figma, and OpenAI, love Notion for its flexibility and choose it because it helps them save time and money. In-person collaboration is essential to Notion's culture. We require all team members to work from our offices on Mondays and Thursdays, our designated Anchor Days. Certain teams or positions may require additional in-office workdays. ABOUT THE ROLE: We are looking for a motivated Account Executive with an entrepreneurial and building spirit to join the GTM Sales Team. You will be instrumental in helping build pipeline, generate revenue, and expand our most strategic customers. As an Account Executive, you will play an important role defining/iterating on our sales motions and providing customer feedback to help share our roadmap. WHAT YOU'LL ACHIEVE: - Prospect and develop new potential accounts by educating contacts about how they can increase their team's collaboration, transparency, and productivity with Notion - Actively work to maintain strong relationships with Notion's existing customer base while identifying opportunity for expanding usage - Be creative and iterate on the contract renewal process to retain and grow customers while mitigating churn or contraction - Hold face-to-face and Zoom meetings with prospective customers to understand their business challenges and goals - Drive executive level relationships - Run product demos to close business at or above quota level - Help build playbooks and define our sales motion - Liaise with our incredible user base to provide world class customer experience - Work cross-functionally and collaboratively with internal teams (sales, inside sales, customer success, solution engineer, deal-desk, ops, legal) SKILLS YOU'LL NEED TO BRING: - 3-5 years of full cycle sales at a fast growing software company - A track record of high achievement in current and previous roles hitting or exceeding quotas - A phenomenally strong communicator - Engaging and compelling presentation skills - A positive and openness minded attitude - A strong desire to be successful without sacrificing your values - A builder mentality who thrives in collaborative environments - An ability to operate within the gray and find creative or out-of-the-box solutions when faced with ambiguity NICE TO HAVES: - You've been an early sales hire at a fast growing start up before - You've got strong technical chops - Direct sales plus experience of selling through Partners - Experience of quarterly sales planning - New logo acquisition sales experience Our customers come from all walks of life and so do we. We hire great people from a wide variety of backgrounds, not just because it's the right thing to do, but because it makes our company stronger. If you share our values and our enthusiasm for small businesses, you will find a home at Notion. Notion is proud to be an equal opportunity employer. We do not discriminate in hiring or any employment decision based on race, color, religion, national origin, age, sex (including pregnancy, childbirth, or related medical conditions), marital status, ancestry, physical or mental disability, genetic information, veteran status, gender identity or expression, sexual orientation, or other applicable legally protected characteristic. Notion considers qualified applicants with criminal histories, consistent with applicable federal, state and local law. Notion is also committed to providing reasonable accommodations for qualified individuals with disabilities and disabled veterans in our job application procedures. If you need assistance or an accommodation due to a disability, please let your recruiter know. Notion is committed to providing highly competitive cash compensation, equity, and benefits. The compensation offered for this role will be based on multiple factors such as location, the role’s scope and complexity, and the candidate’s experience and expertise, and may vary from the range provided below. For roles based in San Francisco or New York City, the estimated range for total on target earnings (including base salary and on target incentive pay) for this role is $180,000 - $230,000 per year. By clicking “Submit Application”, I understand and agree that Notion and its affiliates and subsidiaries will collect and process my information in accordance with Notion’s Global Recruiting Privacy Policy https://notion.notion.site/Notion-Global-Recruiting-Privacy-Policy-fc3eb4e829354a26a2bb6fd5e289b550?pvs=74 and NYLL 144 https://notion.notion.site/Ashby-AI-Bias-Audit-2b0efdeead05803bbbfae159ec86c528. #LI-Onsite`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `# Account Executive, Mid-Marke` | Valid description (4944 chars, 730 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `# Account Executive, Mid-Marke` | Valid description (4944 chars, 730 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `# Account Executive, Mid-Marke` | Valid description (4944 chars, 730 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Account Executive, Mid-Market` |
| Company | `Notion` |
| Location | `New York, NY` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1768599261543` |
| Description Words | `730` |
| Cost (milli-cents) | `20` |
| URL | `https://jobs.ashbyhq.com/notion/0fe96f47-a9ea-4623-ac73-9053c3541297` |

**Description Preview (first 200 words):**

```
# Account Executive, Mid-Market ABOUT US: Notion helps you build beautiful tools for your life’s work. In today's world of endless apps and tabs, Notion provides one place for teams to get everything done, seamlessly connecting docs, notes, projects, calendar, and email—with AI built in to find answers and automate work. Millions of users, from individuals to large organizations like Toyota, Figma, and OpenAI, love Notion for its flexibility and choose it because it helps them save time and money. In-person collaboration is essential to Notion's culture. We require all team members to work from our offices on Mondays and Thursdays, our designated Anchor Days. Certain teams or positions may require additional in-office workdays. ABOUT THE ROLE: We are looking for a motivated Account Executive with an entrepreneurial and building spirit to join the GTM Sales Team. You will be instrumental in helping build pipeline, generate revenue, and expand our most strategic customers. As an Account Executive, you will play an important role defining/iterating on our sales motions and providing customer feedback to help share our roadmap. WHAT YOU'LL ACHIEVE: - Prospect and develop new potential accounts by educating contacts about how they can increase their team's collaboration, transparency, and...
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
  "title": "Account Executive, Mid-Market",
  "company": "Notion",
  "location": "New York, United States",
  "description": "# Account Executive, Mid-Market\n\nABOUT US: Notion helps you build beautiful tools for your life’s work. In today's world of endless apps and tabs, Notion provides one place for teams to get everything done, seamlessly connecting docs, notes, projects, calendar, and email—with AI built in to find answers and automate work. Millions of users, from individuals to large organizations like Toyota, Figma, and OpenAI, love Notion for its flexibility and choose it because it helps them save time and mon...",
  "url": "https://jobs.ashbyhq.com/notion/0fe96f47-a9ea-4623-ac73-9053c3541297",
  "posted_at": 1768599261543,
  "level": "mid",
  "remote": false,
  "cost_milli_cents": 20,
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
  "costMilliCents": 20,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
