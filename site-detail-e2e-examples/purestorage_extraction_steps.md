# Extraction Steps: purestorage

**Detail URL:** `https://boards-api.greenhouse.io/v1/boards/purestorage/jobs/7457367`
**Source URL:** `https://api.greenhouse.io/v1/boards/purestorage/jobs`
**Handler:** `greenhouse`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
(No raw markdown captured)
```

---

## Step 2: Handler Detection

**Detected Handler:** `greenhouse`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's HTML/markdown format
- Extract title, location, and other fields
- Clean up JSON blocks or other noise

---

## Step 3: Handler normalize_markdown() Output

**Extracted Title:** `(None)`

Normalized markdown after handler processing:

```markdown
(No normalized markdown captured - handler may not implement normalize_markdown)
```

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: greenhouse

```json
{
  "url": "https://boards-api.greenhouse.io/v1/boards/purestorage/jobs/7457367",
  "handler": "greenhouse"
}
```

### Raw Content Capture

Captured 0 chars of unknown content

```json
{
  "length": 0,
  "content_type": "unknown"
}
```

### Workflow Execution

Calling process_spidercloud_job_batch()

```json
{
  "urls": [
    {
      "url": "https://boards-api.greenhouse.io/v1/boards/purestorage/jobs/7457367",
      "sourceUrl": "https://api.greenhouse.io/v1/boards/purestorage/jobs",
      "provider": "spidercloud",
      "siteId": "purestorage",
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
    "winner": "raw_row_title",
    "value": "Account Executive, Central Government, Japan"
  },
  "company": {
    "winner": "raw_row_company",
    "value": "Pure Storage"
  },
  "location": {
    "winner": "raw_row_location",
    "value": "Tokyo, Japan"
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
  "posted_at": {
    "winner": "explicit_posted_at_field",
    "value": "2025-12-25 19:13:02"
  },
  "description": {
    "winner": "normalized_markdown_description",
    "value": "Account Executive, Central Government, Japan\n\nWe\u2019re in an unbelievably exciting area of tech and are fundamentally reshaping the data storage industry. Here, you lead with innovative thinking, grow along with us, and join the smartest team in the industry.\nThis type of work\u2014work that changes the world\u2014is what the tech industry was founded on. So, if you're ready to seize the endless opportunities and leave your mark, come join us.\nTHE ROLE\nPure Storage Japan &nbsp;is seeking a dynamic and driven&nbsp; Sales Hunter&nbsp; to join our Central Government team in Japan In this role, you\u2019ll be at the forefront of developing new business, working with our channel resellers and key customer accounts. Collaborate with Sales, Pre-sales, Partner sales, and Field Marketing teams to drive 
```

---

## Step 4.5: Heuristic Processing

**IMPORTANT:** Heuristics are applied AFTER scraper extraction, before Convex ingestion.
This can override the title if the original title doesn't contain required keywords.

### Job 1 Heuristics

**Title unchanged:** `Account Executive, Central Government, Japan`

**Full patch applied:**
```json
{
  "heuristicAttempts": 1,
  "heuristicLastTried": 1768382324853,
  "heuristicVersion": 5,
  "locations": [
    "Tokyo, Japan"
  ],
  "location": "Tokyo, Japan",
  "locationStates": [
    "Japan"
  ],
  "locationSearch": "Japan Tokyo",
  "countries": [
    "Japan"
  ],
  "country": "Japan"
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
| title | `raw_row_title` | `Account Executive, Central Government, Japan` |
| company | `raw_row_company` | `Pure Storage` |
| location | `raw_row_location` | `Tokyo, Japan` |
| remote | `explicit_remote_flag` | `(none)` |
| level | `explicit_level_field` | `senior` |
| compensation | `unknown_compensation` | `(none)` |
| posted_at | `explicit_posted_at_field` | `2025-12-25 19:13:02` |
| description | `normalized_markdown_description` | `Account Executive, Central Government, Japan

We’r` |

### Detailed Strategy Breakdown

#### TITLE

**Final Value:** `Account Executive, Central Government, Japan`
**Winning Strategy:** `raw_row_title`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_title | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_title | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' did not extract title |
| **raw_row_title** 🏆 | EXPLICIT_FIELD | ✅ | `Account Executive, Central Gov` | Valid title |
| markdown_heading_title | CONTENT_PATTERN | ❌ | `` | No markdown heading found |
| hinted_title | HEURISTIC | ✅ | `Account Executive, Central Gov` | Valid title |
| first_line_title | FALLBACK | ✅ | `Account Executive, Central Gov` | Valid title |

#### COMPANY

**Final Value:** `Pure Storage`
**Winning Strategy:** `raw_row_company`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_company | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_company | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no company |
| **raw_row_company** 🏆 | EXPLICIT_FIELD | ✅ | `Pure Storage` | Valid company name |
| url_company | URL_DERIVED | ❌ | `` | Could not derive company from URL |
| content_pattern_company | CONTENT_PATTERN | ❌ | `` | No company pattern matched |
| hinted_company | HEURISTIC | ❌ | `` | No company in hints |
| fallback_company | FALLBACK | ✅ | `Unknown` | Fallback to 'Unknown' company name |

#### LOCATION

**Final Value:** `Tokyo, Japan`
**Winning Strategy:** `raw_row_location`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_location | STRUCTURED_DATA | ❌ | `` | No structured data available |
| site_handler_location_hint | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no location hint |
| **raw_row_location** 🏆 | EXPLICIT_FIELD | ✅ | `Tokyo, Japan` | Valid location |
| explicit_label_location | CUSTOM_350 | ❌ | `` | No explicit location label found |
| url_location | URL_DERIVED | ❌ | `` | No location pattern found in URL path |
| content_pattern_location | CONTENT_PATTERN | ✅ | `Account Executive, Central Gov` | Matched pattern LOCATION_FULL |
| hinted_location | HEURISTIC | ❌ | `` | No location in hints |
| remote_fallback_location | FALLBACK | ❌ | `` | Job not marked as remote |

#### REMOTE

**Final Value:** `False`
**Winning Strategy:** `explicit_remote_flag`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_remote_flag** 🏆 | STRUCTURED_DATA | ✅ | `` | Explicit boolean remote=False |
| location_remote | EXPLICIT_FIELD | ✅ | `` | Location is specific place: Tokyo, Japan |
| title_remote | CONTENT_PATTERN | ❌ | `` | No remote keyword in title |
| content_remote_pattern | CUSTOM_550 | ❌ | `` | No clear remote pattern in content |
| hinted_remote | HEURISTIC | ❌ | `` | No remote in hints |
| remote_company | CUSTOM_650 | ❌ | `` | Company 'Pure Storage' not in remote company list |
| default_remote | FALLBACK | ✅ | `` | Default to non-remote (no clear remote signals fou |

#### LEVEL

**Final Value:** `senior`
**Winning Strategy:** `explicit_level_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_level_field** 🏆 | STRUCTURED_DATA | ✅ | `senior` | Explicit level field: senior -> senior |
| structured_data_level | CUSTOM_150 | ❌ | `` | No structured data available |
| title_level | CONTENT_PATTERN | ❌ | `` | No level indicator in title |
| content_pattern_level | CUSTOM_550 | ✅ | `senior` | Level from content: 'lead' -> senior |
| hinted_level | HEURISTIC | ✅ | `senior` | Level from hints: lead -> senior |
| default_level | FALLBACK | ✅ | `mid` | Default to mid level (no clear level signals found |

#### COMPENSATION

**Final Value:** `0`
**Winning Strategy:** `unknown_compensation`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| explicit_compensation_field | STRUCTURED_DATA | ❌ | `` | Could not parse compensation: 0 |
| structured_data_compensation | CUSTOM_150 | ❌ | `` | No structured data available |
| content_pattern_compensation | CONTENT_PATTERN | ❌ | `` | No compensation pattern in content |
| hinted_compensation | HEURISTIC | ❌ | `` | No compensation in hints |
| **unknown_compensation** 🏆 | FALLBACK | ✅ | `` | No compensation found, using 0 (unknown) |

#### POSTED_AT

**Final Value:** `2025-12-25 19:13:02`
**Winning Strategy:** `explicit_posted_at_field`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| **explicit_posted_at_field** 🏆 | STRUCTURED_DATA | ✅ | `2025-12-25 19:13:02` | Valid date: 2025-12-25T19:13:02 |
| structured_data_posted_at | CUSTOM_150 | ❌ | `` | No structured data available |
| site_handler_posted_at | SITE_HANDLER | ❌ | `` | Handler 'greenhouse' returned no posted_at |
| content_pattern_posted_at | CONTENT_PATTERN | ❌ | `` | No date pattern in content |
| hinted_posted_at | HEURISTIC | ❌ | `` | No posted_at in hints |
| now_fallback_posted_at | FALLBACK | ✅ | `2026-01-14 02:18:45.087183` | Using current time as fallback (date unknown) |

#### DESCRIPTION

**Final Value:** `Account Executive, Central Government, Japan

We’re in an unbelievably exciting area of tech and are fundamentally reshaping the data storage industry. Here, you lead with innovative thinking, grow along with us, and join the smartest team in the industry.
This type of work—work that changes the world—is what the tech industry was founded on. So, if you're ready to seize the endless opportunities and leave your mark, come join us.
THE ROLE
Pure Storage Japan &nbsp;is seeking a dynamic and driven&nbsp; Sales Hunter&nbsp; to join our Central Government team in Japan In this role, you’ll be at the forefront of developing new business, working with our channel resellers and key customer accounts. Collaborate with Sales, Pre-sales, Partner sales, and Field Marketing teams to drive success in the Enterprise private sector.
WHAT YOU'LL DO
- Promote our innovative and simple all-flash enterprise storage technology and data solutions, clearly articulating the value Pure provides to public-sector customers in Japan, including central government ministries and independent administrative institutions.
- Build and invest in strong customer relationships, continuously leading efforts to achieve industry-leading levels of customer satisfaction.
- Develop and execute effective account plans by fully leveraging internal resources, leading cross-functional project teams to drive new customer acquisition.
- Manage a healthy and robust sales pipeline to consistently achieve quarterly and annual targets.
本ポジションでは、以下の業務をお任せします
- 当社の革新的かつシンプルなオールフラッシュ・エンタープライズストレージ技術およびデータソリューションを推進し、日本国内の公共団体（中央省庁・独立行政法人）のお客様に対して、Pureが提供する価値を的確に伝えていただきます
- 顧客との関係構築と投資を通じて、業界最高水準の顧客満足度の実現を継続的にリードしていただきます
- 社内のリソースを最大限に活用するアカウントプランを策定・実行し、プロジェクトチームをリードして新規顧客の獲得を推進していただきます
- 健全かつ充実した営業パイプラインの管理により、四半期・年間の目標達成を確実に導いていただきます
WHAT YOU BRING
- Demonstrates sincerity and passion in engaging with public-sector customers in Japan, with a strong commitment to deeply understanding their business challenges.
- Possesses strong insight, creativity, and a challenger mindset, leveraging expertise in technology solutions to propose effective problem-solving approaches through Pure’s product portfolio.
- Has a proven track record and strong drive in new customer acquisition, with the ability to communicate effectively from executive management to data center operations, and to articulate the value of our solutions with enthusiasm.
- Exhibits excellent interpersonal skills as well as strong written and verbal communication abilities, enabling the building and deepening of trusted customer relationships.
- Capable of developing and executing strategic plans aimed at maximizing revenue and improving operational efficiency within the assigned territory.
- Experienced in leading complex sales processes involving multiple stakeholders, and able to collaborate effectively through strong teamwork with both internal and external partners.
- Has consistently exceeded targets and delivered business results strong enough to earn recommendations and recognition from customers and partners.
- Demonstrates a strong commitment to post-contract customer success.
- Brings 5+ years of B2B sales experience, primarily focused on new customer acquisition or business development.
- Possesses sales experience and industry knowledge within the Japanese public sector, along with the ability to read and understand English at an intermediate level.
このポジションに求められるご経験・スキル
- 日本国内の公共顧客と真摯に向き合い、ビジネス課題を深く理解しようとする誠実さと情熱
- 洞察力と創造性、そしてチャンレンジャー精神を備え、技術ソリューションに関する知見を活かしながら、Pureの製品ポートフォリオを通じて顧客の課題解決を提案する能力
- 新規開拓における豊富な実績と積極性を有し、経営層からデータセンター現場まで自在にコミュニケーションをとりながら、当社ソリューションの価値を熱意を持って訴求できる方
- 優れた対人スキルおよび文章／口頭でのコミュニケーション能力を有し、信頼関係を構築・深化させることができる方
- 担当エリアの収益最大化と業務効率向上を目的とした戦略的な計画立案・実行能力
- 複数のステークホルダーが関わる複雑な営業プロセスを主導したご経験と、社内外とのチームワークを重視し協働できる能力
- 継続的に目標を上回る実績を達成し、他社からも推薦されるようなビジネス成果を上げてきたご経験
- 契約後のカスタマーサクセスにも情熱を注げる方
- 新規顧客開拓またはビジネスデベロップメントを中心とした5年以上の法人営業経験
- 日本の公共における営業経験および業界知識を有していること　•　英語を中程度のレベルで読み、理解する能力が求められます。
Join Pure Storage and be part of a team that's redefining data storage. Apply now and unleash your potential!
INCLUDE FOR POSTING LOCATION IDENTIFICATION
#LI-REMOTE, #LI-ONSITE
WHAT YOU CAN EXPECT FROM US:
- Pure Innovation : We celebrate those who think critically, like a challenge and aspire to be trailblazers.
- Pure Growth : We give you the space and support to grow along with us and to contribute to something meaningful. We have been Named Fortune's Best Large Workplaces in the Bay Area™, Fortune's Best Workplaces for Millennials™ and certified as a Great Place to Work®!
- Pure Team : We build each other up and set aside ego for the greater good.
And because we understand the value of bringing your full and best self to work, we offer a variety of perks to manage a healthy balance, including flexible time off, wellness resources and company-sponsored team events. Check out purebenefits.com for more information.
ACCOMMODATIONS AND ACCESSIBILITY:
Candidates with disabilities may request accommodations for all aspects of our hiring process. For more on this, contact us at TA-Ops@purestorage.com if you’re invited to an interview.
OUR COMMITMENT TO A STRONG AND INCLUSIVE TEAM:
We’re forging a future where everyone finds their rightful place and where every voice matters. Where uniqueness isn’t just accepted but embraced. That’s why we are committed to fostering the growth and development of every person, cultivating a sense of community through our Employee Resource Groups and advocating for inclusive leadership.
&nbsp;
Pure is proud to be an equal opportunity and affirmative action employer. We do not discriminate based upon race, religion, color, national origin, sex (including pregnancy, childbirth, or related medical conditions), sexual orientation, gender, gender identity, gender expression, transgender status, sexual stereotypes, age, status as a protected veteran, status as an individual with a disability, or any other characteristic legally protected by the laws of the jurisdiction in which you are being considered for hire.
JOIN US AND BRING YOUR BEST.
BRING YOUR BOLD.
BRING YOUR FLASH.`
**Winning Strategy:** `normalized_markdown_description`

| Strategy | Priority | Valid | Value | Reason |
|----------|----------|-------|-------|--------|
| structured_data_description | STRUCTURED_DATA | ❌ | `` | No structured data available |
| **normalized_markdown_description** 🏆 | SITE_HANDLER | ✅ | `Account Executive, Central Gov` | Valid description (6122 chars) |
| raw_row_description | EXPLICIT_FIELD | ✅ | `Account Executive, Central Gov` | Valid description (6122 chars) |
| raw_markdown_description | CUSTOM_800 | ✅ | `Account Executive, Central Gov` | Valid description (6122 chars) |
| empty_description_fallback | FALLBACK | ✅ | `` | No description found, using empty string |

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Account Executive, Central Government, Japan` |
| Company | `Pure Storage` |
| Location | `Tokyo, Japan` |
| Is Remote | `False` |
| Level | `senior` |
| Posted At | `1766715182000` |
| Description Words | `806` |
| Cost (milli-cents) | `3` |
| URL | `https://boards-api.greenhouse.io/v1/boards/purestorage/jobs/7457367` |

**Description Preview (first 200 words):**

```
Account Executive, Central Government, Japan We’re in an unbelievably exciting area of tech and are fundamentally reshaping the data storage industry. Here, you lead with innovative thinking, grow along with us, and join the smartest team in the industry. This type of work—work that changes the world—is what the tech industry was founded on. So, if you're ready to seize the endless opportunities and leave your mark, come join us. THE ROLE Pure Storage Japan &nbsp;is seeking a dynamic and driven&nbsp; Sales Hunter&nbsp; to join our Central Government team in Japan In this role, you’ll be at the forefront of developing new business, working with our channel resellers and key customer accounts. Collaborate with Sales, Pre-sales, Partner sales, and Field Marketing teams to drive success in the Enterprise private sector. WHAT YOU'LL DO - Promote our innovative and simple all-flash enterprise storage technology and data solutions, clearly articulating the value Pure provides to public-sector customers in Japan, including central government ministries and independent administrative institutions. - Build and invest in strong customer relationships, continuously leading efforts to achieve industry-leading levels of customer satisfaction. - Develop and execute effective account plans by fully leveraging internal resources, leading cross-functional project teams to...
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
  "sourceUrl": "https://api.greenhouse.io/v1/boards/purestorage/jobs",
  "provider": "spidercloud",
  "costMilliCents": 3,
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
