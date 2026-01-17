# Extraction Steps: ashbyhq

**Detail URL:** `https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd`
**Source URL:** `https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd`
**Handler:** `ashby`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
<html lang="en">
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <meta content="#483fad" name="theme-color">
    <meta name="csp-nonce" id="csp-nonce" content="bhjY1tM-9Gfic1nR25V856mCUpnbQSwA4H6OhW0r7bA">
    <link sizes="192x192" rel="icon" type="image/png" href="https://cdn.ashbyprd.com/cdn_assets/f60311c15b1c9db3843dfe8f1790884afa6cf89f/favicon.png">
    <link href="https://cdn.ashbyprd.com/cdn_assets/f60311c15b1c9db3843dfe8f1790884afa6cf89f/favicon.png" rel="apple-touch-icon">
    <title>Account Manager, Growth @ Ramp</title><meta content="Account Manager, Growth @ Ramp" name="title">
    
  <meta content="ABOUT RAMP

At Ramp, we’re rethinking how modern finance teams function in the age of AI. We believe AI isn’t just the next big wave. It’s the new foundation for how business gets done. We’re investing in that future — and in the people bold enough to build it.

Ramp is a financial operations platform designed to save companies time and money. Our all-in-one solution combines payments, corporate cards, vendor management, procurement, travel booking, and automated bookkeeping with built-in intelligence to maximize the impact of every dollar and hour spent. More than 50,000 businesses, from family-owned farms to e-commerce giants to space startups, have saved $10B and 27.5M hours with Ramp. Founded in 2019, Ramp powers the fastest-growing corporate card and bill payment platform in America, and enables over $100 billion in purchases each year.

Ramp’s investors include Lightspeed Venture Partners, Thrive Capital, Sands Capital, General Catalyst, Founders Fund, Khosla Ventures, Sequoia Capital, Greylock, Redpoint, and ICONIQ, as well as over 100 angel investors who were founders or executives of leading companies. The Ramp team comprises talented leaders from leading financial services and fintech companies—Stripe, Affirm, Goldman Sachs, American Express, Mastercard, Visa, Capital One—as well as technology companies such as Meta, Uber, Netflix, Twitter, Dropbox, and Instacart.

Ramp has been named to Fast Company’s Most Innovative Companies https://www.fastcompany.com/91038883/ramp-most-innovative-companies-2024 list and LinkedIn’s Top U.S. Startups https://www.linkedin.com/pulse/linkedin-top-startups-2024-50-us-companies-rise-linkedin-news-hxote/?trackingId=uBI29YlAOxikbTI7cdvG4g%3D%3D for more than 3 years, as well as the Forbes Cloud 100 https://www.forbes.com/sites/richardnieva/2024/08/06/ramp-cloud-100/, CNBC Disruptor 50 https://www.cnbc.com/2024/05/14/ramp-cnbc-disruptor-50.html, and TIME Magazine’s 100 Most Influential Companies https://time.com/collection/time100-companies-2023/6285147/ramp/.


ABOUT THE ROLE

As a member of the Growth Account Management team, you will deliver value to our customers and revenue for our business by owning expansion and retention of a scaled portfolio of current Ramp customers. You will help identify growth opportunities within y

... (truncated, 95763 total chars)
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

**Extracted Title:** `Account Manager, Growth`

Normalized markdown after handler processing:

```markdown
<html lang="en">
    
    
    
    
    
    
    
    
  
  
  
  
  
  
  
  
    
    
    
  
  

        
    
    
    
    


  
  
    <noscript>
      You need to enable JavaScript to run this app.
    </noscript>
    <div id="root">
      
      <div class="center">
        <div class="fade-in">
          <div class="spinner"></div>
        </div>
      </div>    </div>
      
</html>
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: ashby

```json
{
  "url": "https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd",
  "handler": "ashby"
}
```

### Raw Content Capture

Captured 95763 chars of raw_html content

```json
{
  "length": 95763,
  "content_type": "raw_html"
}
```

### Handler Normalization

normalize_markdown() returned title='Account Manager, Growth', 398 chars of normalized content

```json
{
  "title": "Account Manager, Growth",
  "normalized_length": 398
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd",
      "sourceUrl": "https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd",
      "provider": "spidercloud",
      "siteId": "ashbyhq",
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
    "value": "Account Manager, Growth"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Ramp"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "New York City, NY, USA"
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
    "winner": "unknown_compensation",
    "value": 0
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 15
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-14T08:00:00"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "About Ramp\n\nAt Ramp, we’re rethinking how modern finance teams function in the age of AI. We believe AI isn’t just the next big wave. It’s the new foundation for how business gets done. We’re investing in that future — and in the people bold enough to build it.\n\nRamp is a financial operations platform designed to save companies time and money. Our all-in-one solution combines payments, corporate cards, vendor management, procurement, travel booking, and automated bookkeeping with built-in intelligence to maximize the impact of every dollar and hour spent. More than 50,000 businesses, from family-owned farms to e-commerce giants to space startups, have saved $10B and 27.5M ho
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Account Manager, Growth`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599209357,
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
  "level": "mid",
  "description": "About Ramp\n\nAt Ramp, we’re rethinking how modern finance teams function in the age of AI. We believe AI isn’t just the next big wave. It’s the new foundation for how business gets done. We’re investing in that future — and in the people bold enough to build it.\n\nRamp is a financial operations platform designed to save companies time and money. Our all-in-one solution combines payments, corporate cards, vendor management, procurement, travel booking, and automated bookkeeping with built-in intelligence to maximize the impact of every dollar and hour spent. More than 50,000 businesses, from family-owned farms to e-commerce giants to space startups,
```

---

## Step 4.6: Extractor Strategy Trace

**⚠️ IMPORTANT:** This shows which extraction strategy won for each field.
Use this to debug extraction bugs like location or remote detection issues.

Each field has multiple strategies tried in priority order. The first valid result wins.

### Summary (Winners)

| Field | Winner Strategy | Value |
|-------|----------------|-------|
| title | `raw_row_title` | `Account Manager, Growth` |
| company | `raw_row_company` | `Ramp` |
| location | `raw_row_location` | `New York City, NY, USA` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `title_level` | `mid` |
| compensation | `unknown_compensation` | `(none)` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `15` |
| posted_at | `explicit_posted_at_field` | `2026-01-14 08:00:00` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `About Ramp

At Ramp, we’re rethinking how modern f` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Account Manager, Growth`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'ashby' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Account Manager, Growth` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ❌ | `` | No title in hints |
| first_line_title | FALLBACK | ✅ | `About Ramp` | Valid title |

#### COMPANY

**Final Value:** `Ramp`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'ashby' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Ramp` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Ramp` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `Ramp` | Found 'Company is a' pattern |
| hinted_company | HEURISTIC | ✅ | `Ramp` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `New York City, NY, USA`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'ashby' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `New York City, NY, USA` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `Lightspeed Venture Partners, T` | Matched pattern LOCATION_FULL |
| country_only_fallback_location | CUSTOM_550 | ✅ | `New York City, NY, USA` | Country-only fallback: Valid location |
| hinted_location | HEURISTIC | ✅ | `San Francisco, CA` | From hints.locations (1 locations) |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| schema_org_remote | CUSTOM_110 | ❌ | `` | No jobLocationType found in Schema.org data |
| greenhouse_metadata_remote | CUSTOM_120 | ❌ | `` | No Workplace Type found in Greenhouse metadata |
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'New York City, NY, USA' present but not  |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Ramp' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `mid`
**Winning Strategy:** `title_level`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_level_field | STRUCTURED_DATA | ❌ | `` | Skipping explicit senior level for account manager |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| **title_level** 🏆 | CONTENT_PATTERN | ✅ | `mid` | Account manager title maps to mid level: Account M |
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

**Final Value:** `15`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `15` | Valid cost: 15 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-14 08:00:00`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-14 08:00:00` | Valid date: 2026-01-14T08:00:00 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'ashby' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:29.371818` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'ashby' returned no first_published |

#### DESCRIPTION

**Final Value:** `About Ramp

At Ramp, we’re rethinking how modern finance teams function in the age of AI. We believe AI isn’t just the next big wave. It’s the new foundation for how business gets done. We’re investing in that future — and in the people bold enough to build it.

Ramp is a financial operations platform designed to save companies time and money. Our all-in-one solution combines payments, corporate cards, vendor management, procurement, travel booking, and automated bookkeeping with built-in intelligence to maximize the impact of every dollar and hour spent. More than 50,000 businesses, from family-owned farms to e-commerce giants to space startups, have saved $10B and 27.5M hours with Ramp. Founded in 2019, Ramp powers the fastest-growing corporate card and bill payment platform in America, and enables over $100 billion in purchases each year.

Ramp’s investors include Lightspeed Venture Partners, Thrive Capital, Sands Capital, General Catalyst, Founders Fund, Khosla Ventures, Sequoia Capital, Greylock, Redpoint, and ICONIQ, as well as over 100 angel investors who were founders or executives of leading companies. The Ramp team comprises talented leaders from leading financial services and fintech companies—Stripe, Affirm, Goldman Sachs, American Express, Mastercard, Visa, Capital One—as well as technology companies such as Meta, Uber, Netflix, Twitter, Dropbox, and Instacart.

Ramp has been named to Fast Company’s Most Innovative Companies list and LinkedIn’s Top U.S. Startups for more than 3 years, as well as the Forbes Cloud 100, CNBC Disruptor 50, and TIME Magazine’s 100 Most Influential Companies.

About the Role

As a member of the Growth Account Management team, you will deliver value to our customers and revenue for our business by owning expansion and retention of a scaled portfolio of current Ramp customers. You will help identify growth opportunities within your portfolio and operate a set of scaled touch-points to engage with these customers. As an early member of the team, you will have the opportunity to help build and refine Ramp’s scaled account management motion.

What You’ll Do

Identify expansion opportunities within a scaled portfolio of Ramp customers and own the entire sales process through close

Develop and execute upsell/cross-sell strategies and maintain a robust expansion pipeline

Develop a deep understanding of Ramp customer personas and act as a trusted advisor and consultant to help them achieve their goals with Ramp

Leverage sales methodologies to uncover customer needs and pain points

Gather and provide customer feedback to directly inform Ramp’s product roadmap & customer communication strategy

Collaborate with cross-functional partners to continuously improve operational efficiencies while also advocating for your customers

What You Need

Minimum 2 years of customer-facing or direct sales experience

Proven track record of achievement in a high velocity business environment

Prior CRM experience (preferably Salesforce)

Experience with outbound prospecting and conducting product demonstrations, with a high level of comfort with sales tools & processes

Consultative sales approach and comfortable leveraging analytical & quantitative skills, with a deep interest in understanding business challenges

Ability to discuss Ramp's value proposition with C-level executives, finance teams, and decision makers

Consistent track record of hitting or exceeding sales targets in a fast-paced and metrics-driven environment

Dedication to tracking and improving performance and efficiency on a daily basis

Excellent verbal and written communication skills, with the ability to deliver compelling sales pitches and presentations

Organizational, project management, and time management skills

Sense of entrepreneurship: a self-starter with a high sense of urgency and ability to work within undefined processes

Nice-to-Haves

Bachelor’s degree from an accredited university

Experience with financial services sales

Experience at a high-growth startup

Benefits (for U.S.-based full-time employees)

100% medical, dental & vision insurance coverage for you

Partially covered for your dependents

One Medical annual membership

401k (including employer match on contributions made while employed by Ramp)

Flexible PTO

Fertility HRA (up to $10,000 per year)

Parental Leave

Pet insurance

Centralized home-office equipment ordering for all employees

Health and Wellness stipend

In-office perks: lunch, snacks, drinks, and more

Budget for intra-office travel

Relocation support to NYC or SF (as needed)

Referral Instructions

If you are being referred for the role, please contact that person to apply on your behalf.

Other notices

Pursuant to the San Francisco Fair Chance Ordinance, we will consider for employment qualified applicants with arrest and conviction records.

Ramp Applicant Privacy Notice`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `About Ramp

At Ramp, we’re ret` | Valid description (4898 chars, 707 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `About Ramp

At Ramp, we’re ret` | Valid description (4898 chars, 707 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `About Ramp

At Ramp, we’re ret` | Valid description (4898 chars, 707 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Account Manager, Growth` |
| Company | `Ramp` |
| Location | `New York, NY` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1768402800000` |
| Description Words | `707` |
| Cost (milli-cents) | `15` |
| URL | `https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd` |

**Description Preview (first 200 words):**

```
About Ramp At Ramp, we’re rethinking how modern finance teams function in the age of AI. We believe AI isn’t just the next big wave. It’s the new foundation for how business gets done. We’re investing in that future — and in the people bold enough to build it. Ramp is a financial operations platform designed to save companies time and money. Our all-in-one solution combines payments, corporate cards, vendor management, procurement, travel booking, and automated bookkeeping with built-in intelligence to maximize the impact of every dollar and hour spent. More than 50,000 businesses, from family-owned farms to e-commerce giants to space startups, have saved $10B and 27.5M hours with Ramp. Founded in 2019, Ramp powers the fastest-growing corporate card and bill payment platform in America, and enables over $100 billion in purchases each year. Ramp’s investors include Lightspeed Venture Partners, Thrive Capital, Sands Capital, General Catalyst, Founders Fund, Khosla Ventures, Sequoia Capital, Greylock, Redpoint, and ICONIQ, as well as over 100 angel investors who were founders or executives of leading companies. The Ramp team comprises talented leaders from leading financial services and fintech companies—Stripe, Affirm, Goldman Sachs, American Express, Mastercard, Visa, Capital One—as well as technology companies such as...
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
  "title": "Account Manager, Growth",
  "company": "Ramp",
  "location": "New York City, NY, USA",
  "description": "About Ramp\n\nAt Ramp, we’re rethinking how modern finance teams function in the age of AI. We believe AI isn’t just the next big wave. It’s the new foundation for how business gets done. We’re investing in that future — and in the people bold enough to build it.\n\nRamp is a financial operations platform designed to save companies time and money. Our all-in-one solution combines payments, corporate cards, vendor management, procurement, travel booking, and automated bookkeeping with built-in intell...",
  "url": "https://jobs.ashbyhq.com/ramp/6ac50530-ce1c-4375-9cc1-c56f8b5925fd",
  "posted_at": 1768402800000,
  "level": "senior",
  "remote": false,
  "cost_milli_cents": 15,
  "_full_description_word_count": 707
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 15,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
