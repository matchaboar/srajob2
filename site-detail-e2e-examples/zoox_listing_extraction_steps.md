# Listing Extraction Steps: zoox

## URL Pipeline
- **Input URL:** `https://jobs.lever.co/zoox`
- **Scrape URL:** `https://jobs.lever.co/zoox`

**Listing URL:** `https://jobs.lever.co/zoox`
**Source URL:** `https://jobs.lever.co/zoox`
**Handler:** `LeverHandler`
**Content Type:** `commonmark`

## Detail URL Pipeline Counts
- **Raw Extracted:** 256
- **Handler Filtered:** 256
- **API Transformed:** 256

---

## Step 1: SpiderCloud Response

Raw commonmark from SpiderCloud scrape:

```markdown
Zoox
Location type
* [All](?)
* [On-site](?workplaceType=onsite)
* [Hybrid](?workplaceType=hybrid)
* [Remote](?workplaceType=remote)
Location
* [All](?)
* [Atlanta, GA](<?location=Atlanta, GA>)
* [Austin, TX](<?location=Austin, TX>)
* [Boston, MA](<?location=Boston, MA>)
* [Foster City, CA](<?location=Foster City, CA>)
* [Fremont, CA](<?location=Fremont, CA>)
* [Hayward, CA](<?location=Hayward, CA>)
* [Las Vegas, NV](<?location=Las Vegas, NV>)
* [Miami, FL](<?location=Miami, FL>)
* [Remote (United States)](<?location=Remote (United States)>)
* [San Carlos, CA](<?location=San Carlos, CA>)
* [San Diego, CA](<?location=San Diego, CA>)
* [San Francisco, CA](<?location=San Francisco, CA>)
* [Seattle, WA](<?location=Seattle, WA>)
Team
* [All](?)
* [Administrative Office](<?department=Administrative Office>)
* [Administrative Office](<?department=Administrative Office&amp;team=Administrative Office>)
* [Advanced Hardware Engineering](<?department=Advanced Hardware Engineering>)
* [AHE Programs and Operations](<?department=Advanced Hardware Engineering&amp;team=AHE Programs and Operations>)
* [Electrical Engineering](<?department=Advanced Hardware Engineering&amp;team=Electrical Engineering>)
* [Mechanical Engineering](<?department=Advanced Hardware Engineering&amp;team=Mechanical Engineering>)
* [Sensor Engineering](<?department=Advanced Hardware Engineering&amp;team=Sensor Engineering>)
* [Communications and Marketing](<?department=Communications and Marketing>)
* [Communications](<?department=Communications and Marketing&amp;team=Communications>)
* [Marketing](<?department=Communications and Marketing&amp;team=Marketing>)
* [Data Science](<?department=Data Science>)
* [Data Science](<?department=Data Science&amp;team=Data Science>)
* [Facilities](?department=Facilities)
* [Facilities](?department=Facilities&amp;team=Facilities)
* [Finance](?department=Finance)
* [Finance](?department=Finance&amp;team=Finance)
* [Fleet Operations](<?department=Fleet Operations>)
* [Fleet Operations](<?department=Fleet Operations&amp;team=Fleet Operations>)
* [Information Technology](<?department=Information Technology>)
* [Business Applications](<?department=Information Technology&amp;team=Business Applications>)
* [Endpoint Engineering](<?department=Information Technology&amp;team=Endpoint Engineering>)
* [Enterprise Applications](<?department=Information Technology&amp;team=Enterprise Applications>)
* [IT Program Management](<?department=Information Technology&amp;team=IT Program Management>)
* [Legal](?department=Legal)
* [Legal](?department=Legal&amp;team=Legal)
* [Manufacturing Operations](<?department=Manufacturing Operations>)
* [Advanced Hardware Manufacturing Operations](<?department=Manufacturing Operations&amp;team=Advanced Hardware Manufacturing Operations>)
* [General Assembly](<?department=Manufacturing Operations&amp;team=General Assembly>)
* [Logistics](<?department=Manufacturing Operations&amp;team=Logistics>)
* [Manufacturing Engineering](<?department=Manufacturing Operations&amp;team=Manufacturing Engineering>)
* [Manufacturing Operations Program Management](<?department=Manufacturing Operations&amp;team=Manufacturing Operations Program Management>)
* [Manufacturing Test &amp; Diagnostics](<?department=Manufacturing Operations&amp;team=Manufacturing Test & Diagnostics>)
* [Prototyping](<?department=Manufacturing Operations&amp;team=Prototyping>)
* [People Experience](<?department=People Experience>)
* [People Experience](<?department=People Experience&amp;team=People Experience>)
* [Talent](<?department=People Experience&amp;team=Talent>)
* [Policy and Regulatory Affairs](<?department=Policy and Regulatory Affairs>)
* [Public Policy](<?department=Policy and Regulatory Affairs&amp;team=Public Policy>)
* [Product](?department=Product)
* [Experience Design](<?department=Product&amp;team=Experience Design>)
* [Product Management](<?department=Product&amp;team=Product Management>)
* [Program Management Office](<?department=Program Management Office>)
* [Program Management Office](<?department=Program Management Office&amp;team=Program Management Office>)
* [Quality &amp; Reliability](<?department=Quality & Reliability>)
* [Quality](<?department=Quality & Reliability&amp;team=Quality>)
* [Reliability Engineering](<?department=Quality & Reliability&amp;team=Reliability Engineering>)
* [Safety Policy and Strategy](<?department=Safety Policy and Strategy>)
* [Safety Policy and Strategy](<?department=Safety Policy and Strategy&amp;team=Safety Policy and Strategy>)
* [Software](?department=Software)
* [Autonomy Integration](<?department=Software&amp;team=Autonomy Integration>)
* [C++ Software Architecture](<?department=Software&amp;team=C++ Software Architecture>)
* [Calibration, Localization &amp; Mapping](<?department=Software&amp;team=Calibration, Localization & Mapping>)
* [Collision Avoidance System](<?department=Software&amp;team=Collision Avoidance System>)
* [Embedded Software &amp; Systems Integration](<?departme

... (truncated, 68770 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `LeverHandler`

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

scrape_listing_batch enqueued 256 URLs

```json
{
  "enqueue_calls": 1,
  "enqueued_count": 256
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 256
**URLs After Filtering:** 256
**URLs After Normalization:** 256
**Apply URLs:** 256
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://jobs.lever.co/zoox/2b4ebe4a-029b-49f1-9661-e424129f4dc5`
2. `https://jobs.lever.co/zoox/566fb296-4b03-42b3-9ee7-71d684b36ccc`
3. `https://jobs.lever.co/zoox/c2adffdf-b466-4e90-b643-eb26d10b00f5`
4. `https://jobs.lever.co/zoox/d512e5a5-be81-47a1-afe1-fa2588c59981`
5. `https://jobs.lever.co/zoox/789865f1-f929-4905-86e1-c82ed23c5db8`
6. `https://jobs.lever.co/zoox/1b5c9170-340e-4a75-8705-5c8d90894fd7`
7. `https://jobs.lever.co/zoox/e3477276-acab-4885-91b6-4009ca57613d`
8. `https://jobs.lever.co/zoox/25b24c3c-1d90-460e-bdc4-b028356c4f95`
9. `https://jobs.lever.co/zoox/73df9e0e-abb3-48b3-a3c0-196ed21a41cf`
10. `https://jobs.lever.co/zoox/db062c87-1f97-4f02-8ce5-7cc76ca37b52`
11. `https://jobs.lever.co/zoox/a5a81f53-22cc-494f-b577-f52f58c959a0`
12. `https://jobs.lever.co/zoox/84c01def-33ef-4147-ba2f-e46053a28237`
13. `https://jobs.lever.co/zoox/c73446f6-3e66-4e3d-a694-f768e86038e9`
14. `https://jobs.lever.co/zoox/21303efc-7c2b-40db-a65d-7ba579448d29`
15. `https://jobs.lever.co/zoox/8cc30801-d3c0-4860-aaab-d4426576bdd3`
16. `https://jobs.lever.co/zoox/c802f67d-a514-4427-9b69-bcd768dbcee4`
17. `https://jobs.lever.co/zoox/aacc2d9b-b80f-4003-b0bc-6ead045049a9`
18. `https://jobs.lever.co/zoox/3434172d-8018-4c11-8b51-a6220c5db215`
19. `https://jobs.lever.co/zoox/8307a786-3be1-403c-b02b-ba7a9312d318`
20. `https://jobs.lever.co/zoox/87c67e1d-c2a8-4ccb-b1a1-fc555f10d42d`
... and 236 more

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 256

### Enqueue Payload Sample

```json
{
  "urls": [
    "https://jobs.lever.co/zoox/2b4ebe4a-029b-49f1-9661-e424129f4dc5",
    "https://jobs.lever.co/zoox/566fb296-4b03-42b3-9ee7-71d684b36ccc",
    "https://jobs.lever.co/zoox/c2adffdf-b466-4e90-b643-eb26d10b00f5",
    "https://jobs.lever.co/zoox/d512e5a5-be81-47a1-afe1-fa2588c59981",
    "https://jobs.lever.co/zoox/789865f1-f929-4905-86e1-c82ed23c5db8",
    "https://jobs.lever.co/zoox/1b5c9170-340e-4a75-8705-5c8d90894fd7",
    "https://jobs.lever.co/zoox/e3477276-acab-4885-91b6-4009ca57613d",
    "https://jobs.lever.co/zoox/25b24c3c-1d90-460e-bdc4-b028356c4f95",
    "https://jobs.lever.co/zoox/73df9e0e-abb3-48b3-a3c0-196ed21a41cf",
    "https://jobs.lever.co/zoox/db062c87-1f97-4f02-8ce5-7cc76ca37b52",
    "https://jobs.lever.co/zoox/a5a81f53-22cc-494f-b577-f52f58c959a0",
    "https://jobs.lever.co/zoox/84c01def-33ef-4147-ba2f-e46053a28237",
    "https://jobs.lever.co/zoox/c73446f6-3e66-4e3d-a694-f768e86038e9",
    "https://jobs.lever.co/zoox/21303efc-7c2b-40db-a65d-7ba579448d29",
    "https://jobs.lever.co/zoox/8cc30801-d3c0-4860-aaab-d4426576bdd3",
    "https://jobs.lever.co/zoox/c802f67d-a514-4427-9b69-bcd768dbcee4",
    "https://jobs.lever.co/zoox/aacc2d9b-b80f-4003-b0bc-6ead045049a9",
    "https://jobs.lever.co/zoox/3434172d-8018-4c11-8b51-a6220c5db215",
    "https://jobs.lever.co/zoox/8307a786-3be1-403c-b02b-ba7a9312d318",
    "https://jobs.lever.co/zoox/87c67e1d-c2a8-4ccb-b1a1-fc555f10d42d",
    "https://jobs.lever.co/zoox/c4146f70-b0bb-4a87-b580-1373eecc19a0",
    "https://jobs.lever.co/zoox/1eac90c9-16d7-43e8-943a-f989ff053165",
    "https://jobs.lever.co/zoox/00ae30ba-a04c-495d-99fc-c2ac918f14e4",
    "https://jobs.lever.co/zoox/a5921e3a-6b36-429d-ac69-916eafbbbc1a",
    "https://jobs.lever.co/zoox/2ef5b0c9-04af-4ece-9f3e-16a07be96151",
    "https://jobs.lever.co/zoox/45638e7a-4784-45b7-8c36-337b96ba9b7c",
    "https://jobs.lever.co/zoox/c9b548c6-4941-4510-bbeb-81df2d0c630c",
    "https://jobs.lever.co/zoox/427d0912-a7e5-4f33-9f86-e90dc3abe3cf"
```
