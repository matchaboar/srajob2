# Listing Extraction Steps: greenhouse_support_page_20260114T201434

## URL Pipeline
- **Input URL:** `https://support.greenhouse.io/hc/en-us`
- **Scrape URL:** `https://support.greenhouse.io/hc/en-us`

**Listing URL:** `https://support.greenhouse.io/hc/en-us`
**Source URL:** `https://support.greenhouse.io/hc/en-us`
**Handler:** `GreenhouseHandler`
**Content Type:** `commonmark`

## Detail URL Pipeline Counts
- **Raw Extracted:** 0
- **Handler Filtered:** 0
- **API Transformed:** 0

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

**Method Used:** `auto-detected`

URL extraction methods (in priority order):
1. **JSON API**: Parse structured JSON response with job array
2. **HTML Links**: Extract href attributes from anchor tags
3. **Regex Fallback**: Search for URL patterns in raw text

---

## Step 4: Detailed Extraction Log

### Production Workflow

scrape_listing_batch enqueued 0 URLs

```json
{
  "enqueue_calls": 0,
  "enqueued_count": 0
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 0
**URLs After Filtering:** 0
**URLs After Normalization:** 0
**Pagination URLs:** 0

*No job URLs extracted*

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 0

