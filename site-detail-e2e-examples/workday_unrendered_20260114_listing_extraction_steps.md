# Listing Extraction Steps: workday_unrendered_20260114

**Listing URL:** `https://broadcom.wd1.myworkdayjobs.com/External_Career?q=engineer&locations=877d747df71910021366662e2df00000`
**Source URL:** `https://broadcom.wd1.myworkdayjobs.com/External_Career?q=engineer&locations=877d747df71910021366662e2df00000`
**Handler:** `WorkdayHandler`
**Content Type:** `raw_html`

---

## Step 1: SpiderCloud Response

Raw raw_html from SpiderCloud scrape:

```html
<html lang="en-US" dir="ltr" data-react-helmet="dir,lang"><head><title>Careers</title><meta content="chrome=1;IE=EDGE" http-equiv="X-UA-Compatible"><meta content="text/html; charset=UTF-8" http-equiv="content-type"><meta content="width=device-width, initial-scale=1.0, maximum-scale=2.0" name="viewport"><link href="https://broadcom.wd1.myworkdayjobs.com/External_Career" rel="canonical"><meta property="og:title" name="title"><meta property="og:description" name="description" content="Welcome! Thank you for your interest in Broadcom!"><meta content="https://broadcom.wd1.myworkdayjobs.com/External_Career/assets/logo" property="og:image" name="image"><meta content="website" property="og:type"><meta property="og:url" content="https://broadcom.wd1.myworkdayjobs.com/External_Career"><script type="text/javascript">window.workday = window.workday || {tenant: "broadcom",siteId: "External_Career",locale: "",requestLocale: "en-US"};</script></head><body><div id="root"><div class="WDAY spinner">Loading jobs...</div></div><nav><a href="/External_Career?limit=20&amp;offset=0">Page 1</a><a href="/External_Career?limit=20&amp;offset=20">Page 2</a><a href="/External_Career?limit=20&amp;offset=40">Page 3</a><a href="/External_Career?limit=20&amp;offset=60">Page 4</a><a href="/External_Career?limit=20&amp;offset=80">Page 5</a><a href="/External_Career?limit=20&amp;offset=100">Page 6</a><a href="/External_Career?limit=20&amp;offset=120">Page 7</a><a href="/External_Career?limit=20&amp;offset=140">Page 8</a><a href="/External_Career?limit=20&amp;offset=160">Page 9</a><a href="/External_Career?limit=20&amp;offset=180">Page 10</a></nav></body></html>
```

---

## Step 2: Handler Detection

**Detected Handler:** `WorkdayHandler`

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

Detected handler: WorkdayHandler

```json
{
  "url": "https://broadcom.wd1.myworkdayjobs.com/External_Career?q=engineer&locations=877d747df71910021366662e2df00000",
  "source_url": "https://broadcom.wd1.myworkdayjobs.com/External_Career?q=engineer&locations=877d747df71910021366662e2df00000",
  "handler": "WorkdayHandler"
}
```

### Raw Content Capture

Captured 1653 chars of raw_html content

```json
{
  "length": 1653,
  "content_type": "raw_html"
}
```

### Calling handler.get_links_from_raw_html()

Running WorkdayHandler.get_links_from_raw_html()

```json
{
  "url": "https://broadcom.wd1.myworkdayjobs.com/External_Career?q=engineer&locations=877d747df71910021366662e2df00000",
  "source_url": "https://broadcom.wd1.myworkdayjobs.com/External_Career?q=engineer&locations=877d747df71910021366662e2df00000",
  "content_length": 1653
}
```

### Extraction Complete

Extracted 10 URLs, filtered to 0 detail + 10 pagination

```json
{
  "extracted_count": 10,
  "detail_count": 0,
  "pagination_count": 10,
  "sample_detail_urls": [],
  "sample_pagination_urls": [
    "https://broadcom.wd1.myworkdayjobs.com/External_Career?limit=20&offset=0",
    "https://broadcom.wd1.myworkdayjobs.com/External_Career?limit=20&offset=20",
    "https://broadcom.wd1.myworkdayjobs.com/External_Career?limit=20&offset=40"
  ]
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 10
**URLs After Filtering:** 0
**Pagination URLs:** 10

*No job URLs extracted*

---

## Step 6: Pagination Detection

**Pagination URLs Found:** 10

1. `https://broadcom.wd1.myworkdayjobs.com/External_Career?limit=20&offset=0`
2. `https://broadcom.wd1.myworkdayjobs.com/External_Career?limit=20&offset=20`
3. `https://broadcom.wd1.myworkdayjobs.com/External_Career?limit=20&offset=40`
4. `https://broadcom.wd1.myworkdayjobs.com/External_Career?limit=20&offset=60`
5. `https://broadcom.wd1.myworkdayjobs.com/External_Career?limit=20&offset=80`

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 0

