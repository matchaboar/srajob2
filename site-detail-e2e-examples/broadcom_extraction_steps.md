# Extraction Steps: broadcom

**Detail URL:** `https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-2/R-D-Engineer-IC-Design-4_R024388`
**Source URL:** `https://broadcom.wd1.myworkdayjobs.com/External_Career?q=engineer&locations=877d747df71910021366662e2df00000&locations=877d747df719100213665b4fa1470000&locations=0dd627624e2e013c1b0b00dadcd9d20c&locations=036f545a07811067fea0fff0959fbf8a&locations=52f1f3a9fe8001922f2330ab9a0cc8a8&locations=bc19fd96cebf1069285cfef83d445107&locations=4d92fd6213b61072a7417474224cdf6f&locations=0dd627624e2e01ef94cee9d9dcd9be0c&locations=4c1d526324ca1001993e6efe01540000&locations=3d9f1a0214ac0196b0342b08d2463f01&locations=4b92da390b9b10b7bbda11b85613cb77&locations=29320def7b02106689d3b1774d23aec8&locations=036f545a07811067fe145154fa3cbe5a&locations=12a17a8024ab0188b084e8699205dedc&locations=036f545a07811067fe06ebd34c95be15&locations=036f545a07811067fdde3f0aaa04bddc&locations=036f545a07811067f02d8e8d652ca59a&locations=092b5fae35ea103936b2cf96c8937ee4&locations=3d9f1a0214ac01d3224c7b03d2462700&locations=036f545a07811067fdaef047ea16bd84&locations=036f545a07811067fd922a750736bd46&locations=036f545a07811067fd81d424f6f7bd34&locations=44b3a7caf8e6480795125e010f577053&locations=036f545a07811067f088b65977bba653&locations=036f545a07811067fe3102fad18abe98&locations=036f545a07811067fe54543fbf78bf0b&locations=877d747df719100213668b996bb60000&locations=877d747df7191002136629d4bc1f0000&locations=3fe9c5fb131001012dd089fc94a00000&locations=036f545a07811067fb9e5a23b23bba74&locations=34a3eda408dd1000f53fee68ed950000&locations=0dd627624e2e0140aadb9fd9dcd9780c&locations=036f545a07811067fb8008d4ab6db9fa&locations=036f545a07811067ed3afb5b6f6ca09f&locations=877d747df71910021365fa3b7dd40000&locations=036f545a07811067fb6fcb79192fb9db&locations=288fd69044a1013faf91cae80d12033d&locations=df820e04c9924c84b5214f4d68b50fa9&locations=092b5fae35ea1039363a0cdcb5837e8b&locations=092b5fae35ea103937177f80adeb7f1f&locations=b8b934041f1710a36c7d5f74cb2b8324&locations=4b92da390b9b10b7bb847e8f1b75cac8&locations=3d9f1a0214ac01439ca04708d2465801&locations=036f545a07811067ec96c1512ad3a036&locations=036f545a07811067fb4d9a7c1d47b9b8&locations=2a204116f85f0193baeeb7e2796b85c8&locations=f1900192220f010e8b06cc0dfeb6f74e&locations=2a204116f85f013c9e832787796b52c8&locations=877d747df719100213635c7f40d30000&locations=877d747df71910021363291521ae0000&locations=877d747df719100213626ffcfae00000&locations=877d747df719100213623cd467ed0000&locations=877d747df71910021361d95ffda70000&locations=092b5fae35ea103935df5e4c2e637d4f`
**Handler:** `workday`

---

## Step 1: SpiderCloud Response

Raw markdown content from SpiderCloud scrape:

```markdown
(No raw markdown captured)
```

---

## Step 2: Handler Detection

**Detected Handler:** `workday`

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

Detected handler: workday

```json
{
  "url": "https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-2/R-D-Engineer-IC-Design-4_R024388",
  "handler": "workday"
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
      "url": "https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-2/R-D-Engineer-IC-Design-4_R024388",
      "sourceUrl": "https://broadcom.wd1.myworkdayjobs.com/External_Career?q=engineer&locations=877d747df71910021366662e2df00000&locations=877d747df719100213665b4fa1470000&locations=0dd627624e2e013c1b0b00dadcd9d20c&locations=036f545a07811067fea0fff0959fbf8a&locations=52f1f3a9fe8001922f2330ab9a0cc8a8&locations=bc19fd96cebf1069285cfef83d445107&locations=4d92fd6213b61072a7417474224cdf6f&locations=0dd627624e2e01ef94cee9d9dcd9be0c&locations=4c1d526324ca1001993e6efe01540000&locations=3d9f1a0214ac0196b0342b08d2463f01&locations=4b92da390b9b10b7bbda11b85613cb77&locations=29320def7b02106689d3b1774d23aec8&locations=036f545a07811067fe145154fa3cbe5a&locations=12a17a8024ab0188b084e8699205dedc&locations=036f545a07811067fe06ebd34c95be15&locations=036f545a07811067fdde3f0aaa04bddc&locations=036f545a07811067f02d8e8d652ca59a&locations=092b5fae35ea103936b2cf96c8937ee4&locations=3d9f1a0214ac01d3224c7b03d2462700&locations=036f545a07811067fdaef047ea16bd84&locations=036f545a07811067fd922a750736bd46&locations=036f545a07811067fd81d424f6f7bd34&locations=44b3a7caf8e6480795125e010f577053&locations=036f545a07811067f088b65977bba653&locations=036f545a07811067fe3102fad18abe98&locations=036f545a07811067fe54543fbf78bf0b&locations=877d747df719100213668b996bb60000&locations=877d747df7191002136629d4bc1f0000&locations=3fe9c5fb131001012dd
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

---

## Step 5: Extracted Job Details

### Job 1

| Field | Value |
|-------|-------|
| Title | `Firmware Engineer` |
| Company | `Broadcom` |
| Location | `USA-CA Irvine Alton Parkway Bldg 2` |
| Is Remote | `False` |
| Level | `mid` |
| Posted At | `1768230000000` |
| Description Words | `368` |
| Cost (milli-cents) | `2` |
| URL | `https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/job/USA-CA-Irvine-Alton-Parkway-Bldg-2/R-D-Engineer-IC-Design-4_R024388` |

**Description Preview (first 200 words):**

```
Please Note: 1. If you are a first time user, please create your candidate login account before you apply for a job. (Click Sign In &gt; Create Account) 2. If you already have a Candidate Account, please Sign-In before you apply. Job Description: Job Description: An experienced firmware engineer who can architect, develop and debug firmware running on Arm processors Chip and board bring up and embedded firmware development for ARM-based microcontrollers inside various optical modules, such as NPO/LPO Interface to hardware control loops: implement automatic gain control/equalizer/CDR loop DSP integration: interface with high-speed DSPs to configure serdes and monitor link health Job Requirement: B.S degree in EE or Computer Engineering and 8&#43; years of related experience M.S degree and 6&#43; years of related experience/Ph.D in EE or Computer and 3&#43; years of related experience Expert proficiency in Embedded C/C&#43;&#43; with a strong understanding of memory management, pointers and register-level programming Good knowledge of ARM subsystem Good knowledge of control loops. Good knowledge of optical components (lasers, Mach-zehnder modulators, photo-detectors, TIAs) and optical standards (IEEE 802.3 OIF) Proficiency with peripherals: ADC, DAC, GPIO, MDIO, I2C, SPI, UART Additional Job Description: Compensation and Benefits The annual base salary range for this...
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
  "sourceUrl": "https://broadcom.wd1.myworkdayjobs.com/External_Career?q=engineer&locations=877d747df71910021366662e2df00000&locations=877d747df719100213665b4fa1470000&locations=0dd627624e2e013c1b0b00dadcd9d20c&locations=036f545a07811067fea0fff0959fbf8a&locations=52f1f3a9fe8001922f2330ab9a0cc8a8&locations=bc19fd96cebf1069285cfef83d445107&locations=4d92fd6213b61072a7417474224cdf6f&locations=0dd627624e2e01ef94cee9d9dcd9be0c&locations=4c1d526324ca1001993e6efe01540000&locations=3d9f1a0214ac0196b0342b08d2463f01&locations=4b92da390b9b10b7bbda11b85613cb77&locations=29320def7b02106689d3b1774d23aec8&locations=036f545a07811067fe145154fa3cbe5a&locations=12a17a8024ab0188b084e8699205dedc&locations=036f545a07811067fe06ebd34c95be15&locations=036f545a07811067fdde3f0aaa04bddc&locations=036f545a07811067f02d8e8d652ca59a&locations=092b5fae35ea103936b2cf96c8937ee4&locations=3d9f1a0214ac01d3224c7b03d2462700&locations=036f545a07811067fdaef047ea16bd84&locations=036f545a07811067fd922a750736bd46&locations=036f545a07811067fd81d424f6f7bd34&locations=44b3a7caf8e6480795125e010f577053&locations=036f545a07811067f088b65977bba653&locations=036f545a07811067fe3102fad18abe98&locations=036f545a07811067fe54543fbf78bf0b&locations=877d747df719100213668b996bb60000&locations=877d747df7191002136629d4bc1f0000&locations=3fe9c5fb131001012dd089fc94a00000&locations=036f545a07811067fb9e5a23b23bba74&locations=34a3eda408dd1000f53fee68ed950000&locations=0dd627624e2e0140aadb9fd9dcd9780c&locations=036f545a07811067fb8008d4ab6db9fa&locations=036f545a07811067ed3afb5b6f6ca09f&locations=877d747df71910021365fa3b7dd40000&locations=036f545a07811067fb6fcb79192fb9db&locations=288fd69044a1013faf91cae80d12033d&locations=df820e04c9924c84b5214f4d68b50fa9&locations=092b5fae35ea1039363a0cdcb5837e8b&locations=092b5fae35ea103937177f80adeb7f1f&locations=b8b934041f1710a36c7d5f74cb2b8324&locations=4b92da390b9b10b7bb847e8f1b75cac8&locations=3d9f1a0214ac01439ca04708d2465801&locations=036f545a07811067ec96c1512ad3a036&locations=036f545a07811067fb4d9a7c1d47b9b8&locations=2a204116f85f0193baeeb7e2796b85c8&locations=f1900192220f010e8b06cc0dfeb6f74e&locations=2a204116f85f013c9e832787796b52c8&locations=877d747df719100213635c7f40d30000&locations=877d747df71910021363291521ae0000&locations=877d747df719100213626ffcfae00000&locations=877d747df719100213623cd467ed0000&locations=877d747df71910021361d95ffda70000&locations=092b5fae35ea103935df5e4c2e637d4f",
  "provider": "spidercloud",
  "costMilliCents": 2,
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
