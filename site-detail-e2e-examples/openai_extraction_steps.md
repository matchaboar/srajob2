# Extraction Steps: openai

**Detail URL:** `https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco`
**Source URL:** `https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco`
**Handler:** `openai_careers`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
AI Support Engineer - San Francisco | OpenAI
[Skip to main content](#main)
Log in
[
](https://openai.com/)
Switch to
* [ChatGPT(opens in a new window)](https://chatgpt.com/?openaicom-did=bd2c834a-9be9-4843-a1bb-61a91928031b&amp;openaicom_referred=true)
* [Sora(opens in a new window)](https://sora.com/)
* [API Platform(opens in a new window)](https://platform.openai.com/)
AI Support Engineer - San Francisco | OpenAI
Careers
## AI Support Engineer - San Francisco
User Operations - San Francisco
[Apply now(opens in a new window)](https://jobs.ashbyhq.com/openai/99f823a1-66ec-44bd-ba30-d3645aa49d74/application)
**About the Team**
OpenAI’s User Operations team shepherds our customers’ adoption of AI and ensures that our customers' product experience is nothing short of exceptional.** **We are building the very first post-AGI support team.** **We resolve complex issues, provide technical guidance, and support customers in maximizing value and adoption from deploying our products. We work closely with Sales, Technical Success, Product, Engineering and others, to deliver the best possible experience to our customers at scale. OpenAI's customers represent a range of diverse backgrounds and maturity, from early-stage startups to established global enterprises.
**About the Role**
We are looking for dedicated, experienced, and passionate individuals to help solve some of the most difficult problems faced by our customers and build our post-AGI support team with us. In this role, you will interact directly with customers through support tickets and Slack messages, troubleshooting complex issues and resolving novel, often undefined technical problems while setting a positive precedent for the rest of the team. You’ll partner closely with cross-functional teams to drive initiatives that reduce bugs, improve features, and build the systems that elevate our customer experience. Your work will bring us toward industry-leading response times and service levels, while strengthening our internal customer feedback operations in an increasingly intricate space. You will help scale our support organization by improving operational processes and leveraging our own technology to build the next version of the support team in the new AI world. You will be crucial to the success of the most innovative, disruptive, and high-scale AI solutions being built with OpenAI.
If you thrive in environments that value impact, collaboration, and fast-paced problem-solving, you might be the perfect fit for our team
We use a hybrid work model of 3 days in the office per week and offer relocation assistance to new employees.
**You’ll be responsible for:**
* Work directly with customers, solving their most complex problems and providing ownership and education on the use of our platforms.
* Be among the foremost experts on everything related to OpenAI products, even where our AI does not have the answer.
* Be among the last line of defense before our core Product and Engineering teams, and p

... (truncated, 8573 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `openai_careers`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
AI Support Engineer - San Francisco | OpenAI
[Skip to main content](#main)
Log in
[
](https://openai.com/)
Switch to
* [ChatGPT(opens in a new window)](https://chatgpt.com/?openaicom-did=bd2c834a-9be9-4843-a1bb-61a91928031b&amp;openaicom_referred=true)
* [Sora(opens in a new window)](https://sora.com/)
* [API Platform(opens in a new window)](https://platform.openai.com/)
AI Support Engineer - San Francisco | OpenAI
Careers
## AI Support Engineer - San Francisco
User Operations - San Francisco
[Apply now(opens in a new window)](https://jobs.ashbyhq.com/openai/99f823a1-66ec-44bd-ba30-d3645aa49d74/application)
**About the Team**
OpenAI’s User Operations team shepherds our customers’ adoption of AI and ensures that our customers' product experience is nothing short of exceptional.** **We are building the very first post-AGI support team.** **We resolve complex issues, provide technical guidance, and support customers in maximizing value and adoption from deploying our products. We work closely with Sales, Technical Success, Product, Engineering and others, to deliver the best possible experience to our customers at scale. OpenAI's customers represent a range of diverse backgrounds and maturity, from early-stage startups to established global enterprises.
**About the Role**
We are looking for dedicated, experienced, and passionate individuals to help solve some of the most difficult problems faced by our customers and build our post-AGI support team with us. In this role, you will interact directly with customers through support tickets and Slack messages, troubleshooting complex issues and resolving novel, often undefined technical problems while setting a positive precedent for the rest of the team. You’ll partner closely with cross-functional teams to drive initiatives that reduce bugs, improve features, and build the systems that elevate our customer experience. Your work will bring us toward industry-leading response times and service levels, while strengthening our

... (truncated, 8573 total chars)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: openai_careers

```json
{
  "url": "https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco",
  "handler": "openai_careers"
}
```

### Raw Content Capture

Captured 8573 chars of commonmark content

```json
{
  "length": 8573,
  "content_type": "commonmark"
}
```

### Handler Normalization

normalize_markdown() returned title='None', 8573 chars of normalized content

```json
{
  "title": null,
  "normalized_length": 8573
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco",
      "sourceUrl": "https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco",
      "provider": "spidercloud",
      "siteId": "openai",
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
    "value": "AI Support Engineer"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Openai"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "San Francisco, CA"
  },
  "remote": {
    "winner": "explicit_remote_flag",
    "value": false
  },
  "level": {
    "winner": "content_pattern_level",
    "value": "senior"
  },
  "compensation": {
    "winner": "hinted_compensation",
    "value": 260000
  },
  "cost_milli_cents": {
    "winner": "raw_row_cost_milli_cents",
    "value": 449
  },
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2026-01-16T14:33:33.477000"
  },
  "first_published_on": {
    "winner": null,
    "value": null
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "AI Support Engineer - San Francisco | OpenAI\n[Skip to main content](#main)\nLog in\n](https://openai.com/)\nSwitch to\n* [ChatGPT(opens in a new window)](https://chatgpt.com/?openaicom-did=bd2c834a-9be9-4843-a1bb-61a91928031b&amp;openaicom_referred=true)\n* [Sora(opens in a new window)](https://sora.com/)\n* [API Platform(opens in a new window)](https://platform.openai.com/)\nAI Support Engineer - San Francisco | OpenAI\nCareers\n## AI Support Engineer - San Francisco\nUser Operations - San Francisco\n[Apply now(opens in a new window)](https://jobs.ashbyhq.com/openai/99f823a1-66ec-44bd-ba30-d3645aa49d74/application)\n**About the Team**\nOpenAI’s User Operation
```

### Heuristic Title Override

Title changed from 'AI Support Engineer - San Francisco' to 'AI Support Engineer'

```json
{
  "original_title": "AI Support Engineer - San Francisco",
  "patched_title": "AI Support Engineer",
  "patch": {
    "heuristicAttempts": 1,
    "heuristicLastTried": 1768599213504,
    "heuristicVersion": 5,
    "title": "AI Support Engineer",
    "jobTitle": "AI Support Engineer",
    "locations": [
      "San Francisco, CA"
    ],
    "location": "San Francisco, CA",
    "locationStates": [
      "CA"
    ],
    "locationSearch": "San Francisco CA",
    "countries": [
      "United States"
    ],
    "country": "United States",
    "totalCompensation": 260000,
    "compensationUnknown": false,
    "compensationReason": "extractor:hinted_compensation",
    "level": "senior"
  }
}
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**⚠️ TITLE CHANGED:**
- Original: `AI Support Engineer - San Francisco`
- After Heuristics: `AI Support Engineer`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768599213504,
  "heuristicVersion": 5,
  "title": "AI Support Engineer",
  "jobTitle": "AI Support Engineer",
  "locations": [
    "San Francisco, CA"
  ],
  "location": "San Francisco, CA",
  "locationStates": [
    "CA"
  ],
  "locationSearch": "San Francisco CA",
  "countries": [
    "United States"
  ],
  "country": "United States",
  "totalCompensation": 260000,
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
| title | `raw_row_title` | `AI Support Engineer` |
| company | `raw_row_company` | `Openai` |
| location | `raw_row_location` | `San Francisco, CA` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `content_pattern_level` | `senior` |
| compensation | `hinted_compensation` | `260000` |
| cost_milli_cents | `raw_row_cost_milli_cents` | `449` |
| posted_at | `explicit_posted_at_field` | `2026-01-16 14:33:33.477000` |
| first_published_on | `none` | `(none)` |
| description | `normalized_markdown_description` | `AI Support Engineer - San Francisco | OpenAI
[Skip` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `AI Support Engineer`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'openai_careers' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `AI Support Engineer` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ✅ | `AI Support Engineer` | Valid title |
| hinted_title | HEURISTIC | ✅ | `AI Support Engineer - San Fran` | Valid title |
| first_line_title | FALLBACK | ✅ | `AI Support Engineer - San Fran` | Valid title |

#### COMPANY

**Final Value:** `Openai`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'openai_careers' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Openai` | Valid company name |
| url_company | URL_DERIVED | ✅ | `Openai` | Valid company name |
| content_pattern_company | CONTENT_PATTERN | ✅ | `us in shaping the future of te` | Found 'Join Company' pattern |
| hinted_company | HEURISTIC | ✅ | `the Team` | Valid company name |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `San Francisco, CA`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'openai_careers' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `San Francisco, CA` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ❌ | `` | No location pattern matched |
| country_only_fallback_location | CUSTOM_550 | ✅ | `San Francisco, CA` | Country-only fallback: Valid location |
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
| location_remote | EXPLICIT_FIELD | ❌ | `` | Location 'San Francisco, CA' present but not infer |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | Skipping hints remote=False (unreliable inference  |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Openai' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `senior`
**Winning Strategy:** `content_pattern_level`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_level_field | STRUCTURED_DATA | ❌ | `` | Skipping mid-level default: mid |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ❌ | `` | No level indicator in title |
| **content_pattern_level** 🏆 | CUSTOM_550 | ✅ | `senior` | Level from experience: 8+ years -> senior |
| hinted_level | HEURISTIC | ❌ | `` | No level in hints |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `260000`
**Winning Strategy:** `hinted_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | No compensation field in raw row |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| **hinted_compensation** 🏆 | CUSTOM_450 | ✅ | `260000` | Compensation from hints: $260,000 |
| content_pattern_compensation | CONTENT_PATTERN | ✅ | `230000` | Compensation range pattern: $200,000-$260,000 -> $ |
| unknown_compensation | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### COST_MILLI_CENTS

**Final Value:** `449`
**Winning Strategy:** `raw_row_cost_milli_cents`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **raw_row_cost_milli_cents** 🏆 | STRUCTURED_DATA | ✅ | `449` | Valid cost: 449 |
| raw_row_cost_cents | CUSTOM_110 | ❌ | `` | No cost cents field |
| costs_block | CUSTOM_120 | ❌ | `` | No costs block |
| raw_row_credits_used | CUSTOM_130 | ❌ | `` | No creditsUsed field |

#### POSTED_AT

**Final Value:** `2026-01-16 14:33:33.477000`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2026-01-16 14:33:33.477000` | Valid date: 2026-01-16T14:33:33.477000 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'openai_careers' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-16 14:33:33.516342` | Using current time as fallback (date unknown) |

#### FIRST_PUBLISHED_ON

**Final Value:** `None`
**Winning Strategy:** `None`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_first_published_field | STRUCTURED_DATA | ❌ | `` | No first_published field in raw row |
| structured_data_first_published | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_first_published | SITE_HANDLER | ❌ | `` | Handler 'openai_careers' returned no first_publish |

#### DESCRIPTION

**Final Value:** `AI Support Engineer - San Francisco | OpenAI
[Skip to main content](#main)
Log in
](https://openai.com/)
Switch to
* [ChatGPT(opens in a new window)](https://chatgpt.com/?openaicom-did=bd2c834a-9be9-4843-a1bb-61a91928031b&amp;openaicom_referred=true)
* [Sora(opens in a new window)](https://sora.com/)
* [API Platform(opens in a new window)](https://platform.openai.com/)
AI Support Engineer - San Francisco | OpenAI
Careers
## AI Support Engineer - San Francisco
User Operations - San Francisco
[Apply now(opens in a new window)](https://jobs.ashbyhq.com/openai/99f823a1-66ec-44bd-ba30-d3645aa49d74/application)
**About the Team**
OpenAI’s User Operations team shepherds our customers’ adoption of AI and ensures that our customers' product experience is nothing short of exceptional.** **We are building the very first post-AGI support team.** **We resolve complex issues, provide technical guidance, and support customers in maximizing value and adoption from deploying our products. We work closely with Sales, Technical Success, Product, Engineering and others, to deliver the best possible experience to our customers at scale. OpenAI's customers represent a range of diverse backgrounds and maturity, from early-stage startups to established global enterprises.
**About the Role**
We are looking for dedicated, experienced, and passionate individuals to help solve some of the most difficult problems faced by our customers and build our post-AGI support team with us. In this role, you will interact directly with customers through support tickets and Slack messages, troubleshooting complex issues and resolving novel, often undefined technical problems while setting a positive precedent for the rest of the team. You’ll partner closely with cross-functional teams to drive initiatives that reduce bugs, improve features, and build the systems that elevate our customer experience. Your work will bring us toward industry-leading response times and service levels, while strengthening our internal customer feedback operations in an increasingly intricate space. You will help scale our support organization by improving operational processes and leveraging our own technology to build the next version of the support team in the new AI world. You will be crucial to the success of the most innovative, disruptive, and high-scale AI solutions being built with OpenAI.
If you thrive in environments that value impact, collaboration, and fast-paced problem-solving, you might be the perfect fit for our team
We use a hybrid work model of 3 days in the office per week and offer relocation assistance to new employees.
**You’ll be responsible for:**
* Work directly with customers, solving their most complex problems and providing ownership and education on the use of our platforms.
* Be among the foremost experts on everything related to OpenAI products, even where our AI does not have the answer.
* Be among the last line of defense before our core Product and Engineering teams, and partner with engineering and customer teams to resolve issues.
* Use scripting and emerging AI capabilities to improve internal tooling and automate recurring processes.
* Take learnings from resolving customer issues and create our approach to scaling these solutions, partnering with product and Go-To-Market teams.
* Orchestrate agentic improvements to our operations that will level-up our entire team.
* Foster a supportive and productive work culture within the User Operations team.
* Provide support coverage in on call shifts and during holidays and weekends based on business needs.
* **Note: each person in this role will be expected to work a standard 5x8 work week, with 1 of the days covering weekend shifts each week (e.g., work weeks covering Tuesday - Saturday or Sunday - Thursday)**
**You might thrive in this role if you have:**
* Have 8+ years of experience in user operations, technical support, or support engineering roles, ideally within tech startups or fast-paced environments.
* Are comfortable using emerging technologies (ie. Codex, ChatGPT, OpenAI API, etc.) to script or engineer code (e.g., Python or similar) for automating repetitive tasks and integrating tools.
* Have expert-level SaaS troubleshooting skills with the ability to rapidly understand new technologies and complex concepts.
* Naturally question established norms, skillfully identify root causes, and proactively drive innovation and process improvements.
* Are among the very best critical thinkers, problem solvers, and communicators of complex technical issues in the industry.
* Thrive in ambiguity, adapt rapidly to change, continuously learn, and proactively seek opportunities for growth.
* Have proven experience building strong relationships with customers and cross-functionally to drive resolution to complex issues.
* Have a humble attitude, an eagerness to help others, and a desire to pick up whatever knowledge you're missing to make both your team and our customers succeed.
* Have high horsepower, are adept at frequent context switching and working on multiple projects at once with expansive ownership and prioritization.
* *Preferred:* Bachelor’s degree in Computer Science, Computer Engineering, another relevant technical field, or equivalent practical experience.
**About OpenAI**
OpenAI is an AI research and deployment company dedicated to ensuring that general-purpose artificial intelligence benefits all of humanity. We push the boundaries of the capabilities of AI systems and seek to safely deploy them to the world through our products. AI is an extremely powerful tool that must be created with safety and human needs at its core, and to achieve our mission, we must encompass and value the many different perspectives, voices, and experiences that form the full spectrum of humanity.
We are an equal opportunity employer, and we do not discriminate on the basis of race, religion, color, national origin, sex, sexual orientation, age, veteran status, disability, genetic information, or other applicable legally protected characteristic.
For additional information, please see [OpenAI’s Affirmative Action and Equal Employment Opportunity Policy Statement](https://cdn.openai.com/policies/eeo-policy-statement.pdf).
Background checks for applicants will be administered in accordance with applicable law, and qualified applicants with arrest or conviction records will be considered for employment consistent with those laws, including the San Francisco Fair Chance Ordinance, the Los Angeles County Fair Chance Ordinance for Employers, and the California Fair Chance Act, for US-based candidates. For unincorporated Los Angeles County workers: we reasonably believe that criminal history may have a direct, adverse and negative relationship with the following job duties, potentially resulting in the withdrawal of a conditional offer of employment: protect computer hardware entrusted to you from theft, loss or damage; return all computer hardware in your possession (including the data contained therein) upon termination of employment or end of assignment; and maintain the confidentiality of proprietary, confidential, and non-public information. In addition, job duties require access to secure and protected information technology systems and related data security obligations.
To notify OpenAI that you believe this job posting is non-compliant, please submit a report through [this form](https://form.asana.com/?d=57018692298241&amp;k=5MqR40fZd7jlxVUh5J-UeA). No response will be provided to inquiries unrelated to job posting compliance.
We are committed to providing reasonable accommodations to applicants with disabilities, and requests can be made via this [link](https://form.asana.com/?k=bQ7w9h3iexRlicUdWRiwvg&amp;d=57018692298241).
[OpenAI Global Applicant Privacy Policy](https://cdn.openai.com/policies/global-employee-and-contractor-privacy-policy.pdf)
At OpenAI, we believe artificial intelligence has the potential to help people solve immense global challenges, and we want the upside of AI to be widely shared. Join us in shaping the future of technology.
**Compensation**
$200K – $260K + Offers Equity
[Apply now(opens in a new window)](https://jobs.ashbyhq.com/openai/99f823a1-66ec-44bd-ba30-d3645aa49d74/application)
We use cookies`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `AI Support Engineer - San Fran` | Valid description (8290 chars, 1151 words) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `AI Support Engineer - San Fran` | Valid description (8290 chars, 1151 words) |
| raw_markdown_description | CUSTOM_800 | ✅ | `AI Support Engineer - San Fran` | Valid description (8290 chars, 1151 words) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `AI Support Engineer` |
| Company | `Openai` |
| Location | `San Francisco, CA` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1768599213477` |
| Description Words | `1151` |
| Cost (milli-cents) | `449` |
| URL | `https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco` |

**Description Preview (first 200 words):**

```
AI Support Engineer - San Francisco | OpenAI [Skip to main content](#main) Log in ](https://openai.com/) Switch to * [ChatGPT(opens in a new window)](https://chatgpt.com/?openaicom-did=bd2c834a-9be9-4843-a1bb-61a91928031b&amp;openaicom_referred=true) * [Sora(opens in a new window)](https://sora.com/) * [API Platform(opens in a new window)](https://platform.openai.com/) AI Support Engineer - San Francisco | OpenAI Careers ## AI Support Engineer - San Francisco User Operations - San Francisco [Apply now(opens in a new window)](https://jobs.ashbyhq.com/openai/99f823a1-66ec-44bd-ba30-d3645aa49d74/application) **About the Team** OpenAI’s User Operations team shepherds our customers’ adoption of AI and ensures that our customers' product experience is nothing short of exceptional.** **We are building the very first post-AGI support team.** **We resolve complex issues, provide technical guidance, and support customers in maximizing value and adoption from deploying our products. We work closely with Sales, Technical Success, Product, Engineering and others, to deliver the best possible experience to our customers at scale. OpenAI's customers represent a range of diverse backgrounds and maturity, from early-stage startups to established global enterprises. **About the Role** We are looking for dedicated, experienced, and passionate individuals to help solve some of the most difficult problems faced by our customers and build our post-AGI support team with us. In this role, you will interact directly with customers through support tickets...
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
  "title": "AI Support Engineer - San Francisco",
  "company": "Openai",
  "location": "San Francisco, CA",
  "description": "AI Support Engineer - San Francisco | OpenAI\n[Skip to main content](#main)\nLog in\n](https://openai.com/)\nSwitch to\n* [ChatGPT(opens in a new window)](https://chatgpt.com/?openaicom-did=bd2c834a-9be9-4843-a1bb-61a91928031b&amp;openaicom_referred=true)\n* [Sora(opens in a new window)](https://sora.com/)\n* [API Platform(opens in a new window)](https://platform.openai.com/)\nAI Support Engineer - San Francisco | OpenAI\nCareers\n## AI Support Engineer - San Francisco\nUser Operations - San Francisco\n[App...",
  "url": "https://openai.com/careers/ai-support-engineer-san-francisco-san-francisco",
  "posted_at": 1768599213477,
  "level": "mid",
  "remote": false,
  "cost_milli_cents": 449,
  "_full_description_word_count": 1151
}
```

### Sample Stored Scrape

Scrape record stored for debugging/audit:

```json
{
  "url": null,
  "sourceUrl": null,
  "provider": null,
  "costMilliCents": 449,
  "items_keys": [
    "normalized"
  ],
  "normalized_count": 1
}
```
