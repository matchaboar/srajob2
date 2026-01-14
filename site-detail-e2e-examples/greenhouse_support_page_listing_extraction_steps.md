# Listing Extraction Steps: greenhouse_support_page

**Listing URL:** `https://support.greenhouse.io/hc/en-us`
**Source URL:** `https://support.greenhouse.io/hc/en-us`
**Handler:** `GreenhouseHandler`
**Content Type:** `commonmark`

---

## Step 1: SpiderCloud Response

Raw commonmark from SpiderCloud scrape:

```markdown
# Greenhouse Support

Welcome to Greenhouse Support. Here are some helpful links:

- [Permissions](https://support.greenhouse.io/hc/en-us/search?query=Permissions)
- [Reports](https://support.greenhouse.io/hc/en-us/search?query=Reports)
- [Job board configuration](https://support.greenhouse.io/hc/en-us/search?query=Job+board+configuration)
- [What's new](https://www.greenhouse.com/greenhouse-latest-features)
- [Video tutorials](https://learn.greenhouse.io/)
- [Contact support](https://support.greenhouse.io/hc/en-us/requests/new)

## Check out some job boards:

- [Lyft Jobs](https://boards.greenhouse.io/lyft/jobs/1234567)
- [Stripe Jobs](https://job-boards.greenhouse.io/stripe/jobs/7654321)
- [Airbnb Jobs](https://careers.airbnb.com/positions?gh_jid=9876543)

## More resources:

- [Sign In](https://my.greenhouse.io/users/sign_in)
- [Learn Greenhouse](https://learn.greenhouse.io/recruiter-course?reg=1)
- [Training Calendar](https://learn.greenhouse.io/calendar)
- [CSS Styles](https://job-seekers.cdn.greenhouse.io/assets/sprout-C7gwjKnk.css)
- [JavaScript](https://job-seekers.cdn.greenhouse.io/assets/theme-DoMkWPNL.js)
```

---

## Step 2: Handler Detection

**Detected Handler:** `GreenhouseHandler`

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

Detected handler: GreenhouseHandler

```json
{
  "url": "https://support.greenhouse.io/hc/en-us",
  "source_url": "https://support.greenhouse.io/hc/en-us",
  "handler": "GreenhouseHandler"
}
```

### Raw Content Capture

Captured 1133 chars of commonmark content

```json
{
  "length": 1133,
  "content_type": "commonmark"
}
```

### Calling handler.get_links_from_raw_html()

Running GreenhouseHandler.get_links_from_raw_html()

```json
{
  "url": "https://support.greenhouse.io/hc/en-us",
  "source_url": "https://support.greenhouse.io/hc/en-us",
  "content_length": 1133
}
```

### Extraction Complete

Extracted 17 URLs, filtered to 4 detail + 0 pagination, normalized to 4 final

```json
{
  "extracted_count": 17,
  "detail_count": 4,
  "normalized_count": 4,
  "pagination_count": 0,
  "sample_normalized_urls": [
    "https://boards.greenhouse.io/lyft/jobs/1234567",
    "https://job-boards.greenhouse.io/stripe/jobs/7654321",
    "https://careers.airbnb.com/positions?gh_jid=9876543",
    "https://boards-api.greenhouse.io/v1/boards/stripe/jobs/7654321"
  ],
  "sample_pagination_urls": []
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 17
**URLs After Filtering:** 4
**URLs After Normalization:** 4
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://boards.greenhouse.io/lyft/jobs/1234567`
2. `https://job-boards.greenhouse.io/stripe/jobs/7654321`
3. `https://careers.airbnb.com/positions?gh_jid=9876543`
4. `https://boards-api.greenhouse.io/v1/boards/stripe/jobs/7654321`

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 4

