# Listing Extraction Steps: snapchat_20260114T134509

## URL Pipeline
- **Input URL:** `https://careers.snap.com/jobs`
- **Scrape URL:** `https://careers.snap.com/jobs`

**Listing URL:** `https://careers.snap.com/jobs`
**Source URL:** `https://careers.snap.com/jobs`
**Handler:** `SnapchatCareersHandler`
**Content Type:** `commonmark`

## Detail URL Pipeline Counts
- **Raw Extracted:** 321
- **Handler Filtered:** 290
- **API Transformed:** 290

---

## Step 1: SpiderCloud Response

Raw commonmark from SpiderCloud scrape:

```markdown
Jobs - Snap Inc
[![logo](https://images.ctfassets.net/jwenq9l5fmib/5Am8vNzwNAXm2asqiP9SKP/1a7a6d315963c88cc318b96258c06135/Snap.svg?q=40)](https://careers.snap.com/)[](https://careers.snap.com/)[View Openings](https://careers.snap.com/jobs)
#### Jobs at Snap Inc.
All locations
All locationsAbingdonAustinBeijingBellevueBerlinBoulderChandlerChicagoDelhiDubaiEindhovenHamburgLondonLos AngelesMumbaiNew YorkPalo AltoParisPlymouthRemoteRiyadhSan DiegoSan FranciscoSanta MonicaSeattleShenzhenSingaporeStockholmSydneyTaipei CityTel AvivTorontoVancouverViennaWashingtonZurich
All teams
All teamsAugmented RealityBitmojiCommunicationsContentEngineeringFinance &amp; AccountingGrowthInformation TechnologyLegalMarketingOperationsOtherPeopleProductResearchSalesSecuritySnap Product R&amp;DSpectaclesStrategy
All roles
All rolesCommunicationsCorporateCreativeData &amp; AnalyticsEngineeringFinance &amp; AccountingGlobal Brand ExperienceGlobal Workplace ExperienceInformation TechnologyLegalMarketingOperations And Platform IntegrityPartnershipsPeopleProductProgram/Project ManagementResearchSalesSales OperationsSecurity - PSA Risk Management Services Inc.Talent AcquisitionTechnical Program Manager
All types
All typesInternRegularTemporary
##### Engineering
|** Role**|** Team**|** Type**|** Location**|
|[Epitaxy Engineer](https://careers.snap.com/job?id=R0043314)|Spectacles|Regular|Abingdon|
|[Multi-skilled Maintenance Technician](https://careers.snap.com/job?id=R0038181)|Spectacles|Regular|Abingdon|
|[Manager, Production Operations](https://careers.snap.com/job?id=R0042624)|Spectacles|Regular|Abingdon|
|[Materials Planner](https://careers.snap.com/job?id=R0044004)|Spectacles|Regular|Abingdon|
|[NPI Engineer](https://careers.snap.com/job?id=R0042985)|Spectacles|Regular|Abingdon|
|[Optical Engineer, Inkjet Printing](https://careers.snap.com/job?id=R0043586)|Spectacles|Regular|Abingdon|
|[Production Operator](https://careers.snap.com/job?id=R0043742)|Spectacles|Regular|Abingdon|
|[Production Supervisor](https://careers.snap.com/job?id=R0042547)|Spectacles|Regular|Abingdon|
|[Production Technician / Engineer](https://careers.snap.com/job?id=R0042004)|Spectacles|Regular|Abingdon|
|[Software Engineer, Android Security, Level 4](https://careers.snap.com/job?id=R0043636)|Engineering|Regular|Bellevue; Seattle|
|[Software Engineer, Spectacles Full Stack, Level 4](https://careers.snap.com/job?id=R0043259)|Spectacles|Regular|Bellevue; Los Angeles|
|[Software Engineer, Full Stack, Spectacles Cloud, Level 4](https://careers.snap.com/job?id=R0043263)|Spectacles|Regular|Bellevue; Los Angeles|
|[Software Engineer, Full Stack, Spectacles Cloud, Level 5](https://careers.snap.com/job?id=R0043267)|Spectacles|Regular|Bellevue; Los Angeles|
|[Software Engineer, Spectacles Mobile, Level 4](https://careers.snap.com/job?id=R0042895)|Spectacles|Regular|Bellevue; Los Angeles|
|[Staff Software Engineer, Level 6](https://careers.snap.com/job?id=R0041979)|Engineering|Regular|Bellevue; Santa Monica; Seattle|
|[Staff Software Engineer, User and Friends, Level 6](https://careers.snap.com/job?id=R0043755)|Engineering|Regular|Bellevue; Los Angeles; Seattle|
|[Equipment Technician](https://careers.snap.com/job?id=R0043216)|Spectacles|Regular|Chandler|
|[Fab Operator](https://careers.snap.com/job?id=H225FO)|Spectacles|Regular|Chandler|
|[Process Technician](https://careers.snap.com/job?id=H225SPT3)|Spectacles|Regular|Chandler|
|[Product Quality Engineer, Level 5](https://careers.snap.com/job?id=R0042914)|Spectacles|Regular|Chandler|
|[Production Supervisor](https://careers.snap.com/job?id=R0043472)|Spectacles|Regular|Chandler|
|[ASIC Package Engineer](https://careers.snap.com/job?id=R0043514)|Spectacles|Regular|Eindhoven; Paris|
|[Senior Embedded Processor Architect](https://careers.snap.com/job?id=R0043517)|Spectacles|Regular|Eindhoven|
|[Software Engineer, Lead (Spectacles AR)](https://careers.snap.com/job?id=R0041277)|Spectacles|Regular|London|
|[Computer Vision Engineer](https://careers.snap.com/job?id=R0042824)|Spectacles|Regular|London|
|[Full Stack Engineer – 3D Web Tools (PlayCanvas)](https://careers.snap.com/job?id=R0042707)|Augmented Reality|Regular|London|
|[Lead Machine Learning Engineer, Gen AI](https://careers.snap.com/job?id=R0043783)|Other|Regular|London|
|[Machine Learning Engineer, Gen AI](https://careers.snap.com/job?id=R0043784)|Other|Regular|London|
|[Senior Machine Learning Engineer, Gen AI](https://careers.snap.com/job?id=R0043787)|Other|Regular|London|
|[Machine Learning Engineering Manager, Gen AI](https://careers.snap.com/job?id=R0043764)|Other|Regular|London|
|[Software Engineering Manager (Spectacles AR)](https://careers.snap.com/job?id=R0042971)|Spectacles|Regular|London|
|[Network Engineer](https://careers.snap.com/job?id=R0042851)|Information Technology|Regular|London|
|[Software Engineer - C++](https://careers.snap.com/job?id=R0042880)|Spectacles|Regular|London|
|[Software Engineer, Android](https://careers.snap.com/job?id=R0043782)|Other

... (truncated, 37706 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `SnapchatCareersHandler`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's listing page format
- Extract job URLs from JSON API responses or HTML
- Identify pagination links
- Filter out non-job URLs

---

## Step 3: URL Extraction Method

**Method Used:** `response.links`

URL extraction methods (in priority order):
1. **JSON API**: Parse structured JSON response with job array
2. **HTML Links**: Extract href attributes from anchor tags
3. **Regex Fallback**: Search for URL patterns in raw text

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: SnapchatCareersHandler

```json
{
  "url": "https://careers.snap.com/jobs",
  "source_url": "https://careers.snap.com/jobs",
  "handler": "SnapchatCareersHandler"
}
```

### Raw Content Capture

Captured 37706 chars of commonmark content

```json
{
  "length": 37706,
  "content_type": "commonmark"
}
```

### Calling handler.get_links_from_raw_html()

Running SnapchatCareersHandler.get_links_from_raw_html()

```json
{
  "url": "https://careers.snap.com/jobs",
  "source_url": "https://careers.snap.com/jobs",
  "content_length": 37706
}
```

### Handler URL Filtering

Handler filtered 321 URLs: 290 transformed, 0 rejected

```json
{
  "input_count": 321,
  "output_count": 290,
  "transformed_count": 290,
  "rejected_count": 0,
  "sample_transformations": [
    {
      "original": "https://www.snap.com/ad-policies?utm_source=careers_snap_com&utm_medium=referral&utm_campaign=universal_navigation&utm_content=footer_item_link&lang=en-US",
      "transformed": "https://careers.snap.com/job?id=R0043117",
      "reason": "filter_job_urls_for_site"
    },
    {
      "original": "https://careers.snap.com/job?id=R0043117",
      "transformed": "https://careers.snap.com/job?id=R0043868",
      "reason": "filter_job_urls_for_site"
    },
    {
      "original": "https://careers.snap.com/job?id=R0043868",
      "transformed": "https://careers.snap.com/job?id=Q126DSA5",
      "reason": "filter_job_urls_for_site"
    },
    {
      "original": "https://careers.snap.com/job?id=Q126DSA5",
      "transformed": "https://careers.snap.com/job?id=R0041924",
      "reason": "filter_job_urls_for_site"
    },
    {
      "original": "https://www.snap.com/political-ads?utm_source=careers_snap_com&utm_medium=referral&utm_campaign=universal_navigation&utm_content=footer_item_link&lang=en-US",
      "transformed": "https://careers.snap.com/job?id=Q425SCP8",
      "reason": "filter_job_urls_for_site"
    },
    {
      "original": "https://careers.snap.com/job?id=R0041924",
      "transformed": "https://careers.snap.com/job?id=R0043678",
      "reason": "filter_job_urls_for_site"
    },
    {
      "original": "https://careers.snap.com/job?id=Q425SCP8",
      "transformed": "https://careers.snap.com/job?id=H225SPT3",
      "reason": "filter_job_urls_for_site"
    },
    {
      "original": "https://careers.snap.com/job?id=R0043678",
      "transformed": "https://careers.snap.com/job?id=R0043563",
      "reason": "filter_job_urls_for_site"
    },
    {
      "original": "https://careers.snap.com/job?id=H225SPT3",
      "transformed": "https://careers.snap.com/job?id=H126SWEM9",
      "reason": "filter_job_urls_for_site"
 
```

### Extraction Complete

Extracted 321 URLs, filtered to 290 detail + 0 pagination, normalized to 290 final

```json
{
  "extracted_count": 321,
  "detail_count": 290,
  "normalized_count": 290,
  "pagination_count": 0,
  "sample_normalized_urls": [
    "https://careers.snap.com/job?id=R0043117",
    "https://careers.snap.com/job?id=R0043868",
    "https://careers.snap.com/job?id=Q126DSA5",
    "https://careers.snap.com/job?id=R0041924",
    "https://careers.snap.com/job?id=Q425SCP8"
  ],
  "sample_pagination_urls": []
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 321
**URLs After Filtering:** 290
**URLs After Normalization:** 290
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://careers.snap.com/job?id=R0043117`
2. `https://careers.snap.com/job?id=R0043868`
3. `https://careers.snap.com/job?id=Q126DSA5`
4. `https://careers.snap.com/job?id=R0041924`
5. `https://careers.snap.com/job?id=Q425SCP8`
6. `https://careers.snap.com/job?id=R0043678`
7. `https://careers.snap.com/job?id=H225SPT3`
8. `https://careers.snap.com/job?id=R0043563`
9. `https://careers.snap.com/job?id=H126SWEM9`
10. `https://careers.snap.com/job?id=R0043524`
11. `https://careers.snap.com/job?id=R0043685`
12. `https://careers.snap.com/job?id=R0043545`
13. `https://careers.snap.com/job?id=R0042929`
14. `https://careers.snap.com/job?id=R0043301`
15. `https://careers.snap.com/job?id=R0043306`
16. `https://careers.snap.com/job?id=R0039654`
17. `https://careers.snap.com/job?id=R0041277`
18. `https://careers.snap.com/job?id=R0043874`
19. `https://careers.snap.com/job?id=R0041101`
20. `https://careers.snap.com/job?id=R0043826`
... and 270 more

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 290

