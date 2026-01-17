# Listing Extraction Steps: hubspot_careers_20260114T134509

## URL Pipeline
- **Input URL:** `https://www.hubspot.com/careers/jobs?hubs_signup-cta=careers-nav-cta&page=1#office=toronto,san-francisco,remote,cambridge;`
- **Scrape URL:** `https://www.hubspot.com/careers/jobs?hubs_signup-cta=careers-nav-cta&page=1#office=toronto,san-francisco,remote,cambridge;`

**Listing URL:** `https://www.hubspot.com/careers/jobs?hubs_signup-cta=careers-nav-cta&page=1#office=toronto,san-francisco,remote,cambridge;`
**Source URL:** `https://www.hubspot.com/careers/jobs?hubs_signup-cta=careers-nav-cta&page=1#office=toronto,san-francisco,remote,cambridge;`
**Handler:** `HubspotCareersHandler`
**Content Type:** `commonmark`

## Detail URL Pipeline Counts
- **Raw Extracted:** 22
- **Handler Filtered:** 12
- **API Transformed:** 12

---

## Step 1: SpiderCloud Response

Raw commonmark from SpiderCloud scrape:

```markdown
HubSpot Careers | All Openings
Logo - Full (Color)
Emerging Talent North America Roles
[
Explore Roles
](https://app.ripplematch.com/v2/public/company/hubspot?tl=cd866a5f&amp;hubs_signup-cta=careers-nav-cta)
# All Open Positions
However you identify or whatever your path here, please apply if you see a position that makes your heart skip a beat. Come join us and help us build a global company where we're all proud to belong.
Careers Menu
*
Filter by Location
Choose Location(s)**
* Amsterdam, Netherlands
* Bengaluru, India
* Berlin, Germany
* Bogotá, Colombia
* Cambridge, MA, USA
* Dublin, Ireland
* Ghent, Belgium
* Madrid, Spain
* Montreal, Canada
* Ontario, Canada
* Paris, France
* Remote
* San Francisco, CA, USA
* Singapore
* Sydney, Australia
* Tokyo, Japan
* Toronto, Canada
* United Kingdom
*
Filter by Department
Choose Department(s)**
* Business Technology
* Customer Success
* General &amp; Administrative
* Marketing
* People Operations
* Product, UX &amp; Engineering
* Operations
* Sales
*
Filter by Language
Open Tooltip
Unless specified, all roles require English language proficiency.
Choose Language(s)**
* Arabic
* Dutch
* English
* French
* German
* Hebrew
* Italian
* Japanese
* Polish
* Portuguese
* Russian
* Spanish
* Swedish
*
Filter by Role Type
Choose Role(s)**
* Global Emerging Talent
* Individual Contributor
* People Manager
* Leadership (Director+)
Toronto, CanadaSan Francisco, CA, USARemoteCambridge, MA, USAClear all filters
## Browse Open Positions
Showing 1–12 of 132
[Show all](#show-all)
*
### Account Executive, Corporate - Benelux
Sales
Remote - Netherlands
[Apply](https://www.hubspot.com/careers/jobs/5986323?hubs_signup-cta=careers-apply)
*
### Account Executive, Corporate - UKI
Sales
Remote - Ireland
[Apply](https://www.hubspot.com/careers/jobs/5986932?hubs_signup-cta=careers-apply)
*
### Account Executive, Corporate - UKI
Sales
Remote - United Kingdom
[Apply](https://www.hubspot.com/careers/jobs/5986934?hubs_signup-cta=careers-apply)
*
### Account Executive (Mexican territory, Colombia based)
Sales
Remote - Colombia
[Apply](https://www.hubspot.com/careers/jobs/7137634?hubs_signup-cta=careers-apply)
*
### Account Executive - Mid-Market
Sales
Remote - USA
[Apply](https://www.hubspot.com/careers/jobs/5990166?hubs_signup-cta=careers-apply)
*
### Account Executive - Mid Market, DACH (Remote)
Sales
Remote - Germany
[Apply](https://www.hubspot.com/careers/jobs/5986426?hubs_signup-cta=careers-apply)
*
### Account Executive Mid Market - France
Sales
Remote - France
[Apply](https://www.hubspot.com/careers/jobs/5986119?hubs_signup-cta=careers-apply)
*
### Account Executive, Mid Market - France
Sales
Remote - Ireland
[Apply](https://www.hubspot.com/careers/jobs/5986204?hubs_signup-cta=careers-apply)
*
### Account Executive, Mid Market - Hebrew speaker
Sales
Remote - Ireland
[Apply](https://www.hubspot.com/careers/jobs/5986360?hubs_signup-cta=careers-apply)
*
### Account Executive, Mid Market - Hebrew speaker
Sales
Remote - United Kingdom
[Apply](https://www.hubspot.com/careers/jobs/5986358?hubs_signup-cta=careers-apply)
*
### Account Executive, Mid Market - Middle East
Sales
Remote - Ireland
[Apply](https://www.hubspot.com/careers/jobs/5986400?hubs_signup-cta=careers-apply)
*
### Account Executive, Mid Market - Middle East
Sales
Remote - United Kingdom
[Apply](https://www.hubspot.com/careers/jobs/6024525?hubs_signup-cta=careers-apply)
## What's the recruiting process like at HubSpot?
So you’re considering submitting your résumé. What happens next? Learn more about your experience as a candidate at HubSpot.
[
Learn about the recruiting process
](https://www.hubspot.com/careers/candidate-experience?hubs_signup-cta=careers-nav-cta)
```

---

## Step 2: Handler Detection

**Detected Handler:** `HubspotCareersHandler`

The handler is selected based on URL pattern matching. Each handler knows how to:
- Parse the specific job board's listing page format
- Extract job URLs from JSON API responses or HTML
- Identify pagination links
- Filter out non-job URLs

---

## Step 3: URL Extraction Method

**Method Used:** `handler.get_links_from_raw_html`

URL extraction methods (in priority order):
1. **JSON API**: Parse structured JSON response with job array
2. **HTML Links**: Extract href attributes from anchor tags
3. **Regex Fallback**: Search for URL patterns in raw text

---

## Step 4: Detailed Extraction Log

### Handler Detection

Detected handler: HubspotCareersHandler

```json
{
  "url": "https://www.hubspot.com/careers/jobs?hubs_signup-cta=careers-nav-cta&page=1#office=toronto,san-francisco,remote,cambridge;",
  "source_url": "https://www.hubspot.com/careers/jobs?hubs_signup-cta=careers-nav-cta&page=1#office=toronto,san-francisco,remote,cambridge;",
  "handler": "HubspotCareersHandler"
}
```

### Raw Content Capture

Captured 3709 chars of commonmark content

```json
{
  "length": 3709,
  "content_type": "commonmark"
}
```

### Calling handler.get_links_from_raw_html()

Running HubspotCareersHandler.get_links_from_raw_html()

```json
{
  "url": "https://www.hubspot.com/careers/jobs?hubs_signup-cta=careers-nav-cta&page=1#office=toronto,san-francisco,remote,cambridge;",
  "source_url": "https://www.hubspot.com/careers/jobs?hubs_signup-cta=careers-nav-cta&page=1#office=toronto,san-francisco,remote,cambridge;",
  "content_length": 3709
}
```

### Extraction Complete

Extracted 22 URLs, filtered to 12 detail + 0 pagination, normalized to 12 final

```json
{
  "extracted_count": 22,
  "detail_count": 12,
  "normalized_count": 12,
  "pagination_count": 0,
  "sample_normalized_urls": [
    "https://www.hubspot.com/careers/jobs/5986323",
    "https://www.hubspot.com/careers/jobs/5986932",
    "https://www.hubspot.com/careers/jobs/5986934",
    "https://www.hubspot.com/careers/jobs/7137634",
    "https://www.hubspot.com/careers/jobs/5990166"
  ],
  "sample_pagination_urls": []
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 22
**URLs After Filtering:** 12
**URLs After Normalization:** 12
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://www.hubspot.com/careers/jobs/5986323`
2. `https://www.hubspot.com/careers/jobs/5986932`
3. `https://www.hubspot.com/careers/jobs/5986934`
4. `https://www.hubspot.com/careers/jobs/7137634`
5. `https://www.hubspot.com/careers/jobs/5990166`
6. `https://www.hubspot.com/careers/jobs/5986426`
7. `https://www.hubspot.com/careers/jobs/5986119`
8. `https://www.hubspot.com/careers/jobs/5986204`
9. `https://www.hubspot.com/careers/jobs/5986360`
10. `https://www.hubspot.com/careers/jobs/5986358`
11. `https://www.hubspot.com/careers/jobs/5986400`
12. `https://www.hubspot.com/careers/jobs/6024525`

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 12

