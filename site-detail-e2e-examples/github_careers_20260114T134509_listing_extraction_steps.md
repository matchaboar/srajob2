# Listing Extraction Steps: github_careers_20260114T134509

## URL Pipeline
- **Input URL:** `https://www.github.careers/api/jobs?keywords=engineer&sortBy=relevance&limit=100`
- **Scrape URL:** `https://www.github.careers/api/jobs?keywords=engineer&sortBy=relevance&limit=100`

**Listing URL:** `https://www.github.careers/api/jobs?keywords=engineer&sortBy=relevance&limit=100`
**Source URL:** `https://www.github.careers/api/jobs?keywords=engineer&sortBy=relevance&limit=100`
**Handler:** `GithubCareersHandler`
**Content Type:** `raw_html`

## Detail URL Pipeline Counts
- **Raw Extracted:** 50
- **Handler Filtered:** 50
- **API Transformed:** 50

---

## Step 1: SpiderCloud Response

Raw raw_html from SpiderCloud scrape:

```html
{"jobs":[{"data":{"slug":"4867","language":"en-us","languages":["en-us"],"req_id":"4867","title":"Senior Software Engineer, Copilot Agents","description":"About GitHub GitHub is the world’s leading platform for agentic software development — powered by Copilot to build, scale, and deliver secure software. Over 180 million developers, including more than 90% of the Fortune 100 companies, use GitHub to collaborate, and more than 77,000 organisations have adopted GitHub Copilot. Locations In this role you can work from Remote, United States Overview GitHub is the home for software development, where we collaborate to build the world's leading AI-powered developer platform. In the Copilot Agents organization at GitHub, we are passionate about ensuring the security and quality of the world’s software - from open source to the enterprise, written by humans and by AI tools. We believe that the best way to secure and improve the quality of software is to detect actionable issues early in the development process, and actively facilitate their remediation as part of the developer workflow. Our team develops detection and remediation engines that power several GitHub products used by hundreds of thousands of developers and projects every day: Copilot Code Review agent is GitHub LLM-powered code review engine which provides feedback for immense volumes of code changes daily helping developers fix bugs and improve the quality of their code, leading to faster time to merge. Copilot Autofix is GitHub's LLM-powered remediation engine that produces high-quality fix suggestions for security or quality findings, empowering developers to fix them as soon as they are found or burn down the debt already existing in their codebase. It is used as the remediation engine within the GitHub Code Security, GitHub Advanced Security, and Copilot code review products. CodeQL is GitHub's semantic code analysis engine that uses world-class static analysis research and technology to deeply analyze code, enabling the early detection of security vulnerabilities and correctness errors in software. CodeQL supports a wide range of programming languages, including C/C++, C#, Go, Java, JavaScript/TypeScript, Kotlin, Python, Ruby, and Swift. It is used as the primary detection engine within the GitHub Code Security and GitHub Advanced Security products. We work as a distributed group within a distributed company. The majority of our team members live across Europe, the US, and Canada, and while we do have some offices, all our meetings are location-agnostic and happen online. We operate with a high degree of autonomy and trust, and we have a significant level of influence on the product and technical direction of security and code review products at GitHub. We value learning, introspection and reflection, and we’re always looking for ways to improve as a team and as individuals, so candor and a culture that values safety to speak up are highly important to us. Responsibilities We are looking for a Senior Software Engineer to join one of the distributed software engineering teams responsible for building and expanding code analysis engines and agents at GitHub. You will work closely with various engineering teams, product managers, designers, and technical writers that build different aspects of the products, to influence product direction and deliver features to users, with clear focus on quality, reliability, and user experience. You will engage with internal users and external users (both from enterprise customers and the open-source community) to help them succeed with the product. You’ll influence and provide feedback on the organizational culture and processes, always looking for opportunities to improve in a continuous pursuit of excellence. Qualifications Required Qualifications: For this role, we’re looking for an experienced software engineer with: 6+ years experience in Software Engineering, Computer Science, or related technical discipline with proven experience maintaining and delivering production software coding in languages including, but not limited to, C, C++, C#, Java, JavaScript/TypeScript, Go, Ruby, Rust, or Python. OR Associate’s Degree in Computer Science, Electrical Engineering, Electronics Engineering, Math, Physics, Computer Engineering, or related field AND 5+ years experience in Software Engineering, Computer Science, or related technical discipline with proven experience maintaining and delivering production software coding in languages including, but not limited to, C, C++, C#, Java, JavaScript/TypeScript, Go, Ruby, Rust, or Python. OR Bachelor's Degree in Computer Science, Electrical Engineering, Electronics Engineering, Math, Physics, Computer Engineering, or related field AND 4+ years experience in Software Engineering, Computer Science, or related technical discipline with proven experience maintaining and delivering production software coding in languages including, but not limited to, C, C++, C#, Java, JavaScript/T

... (truncated, 736635 total chars)
```

---

## Step 2: Handler Detection

**Detected Handler:** `GithubCareersHandler`

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

scrape_listing_batch enqueued 50 URLs

```json
{
  "enqueue_calls": 1,
  "enqueued_count": 50
}
```

---

## Step 5: Extracted URLs

**Total URLs Found:** 50
**URLs After Filtering:** 50
**URLs After Normalization:** 50
**Apply URLs:** 50
**Pagination URLs:** 0

### Final Normalized URLs (first 20)

1. `https://www.github.careers/careers-home/jobs/4867?lang=en-us`
2. `https://www.github.careers/careers-home/jobs/4871?lang=en-us`
3. `https://www.github.careers/careers-home/jobs/4870?lang=en-us`
4. `https://www.github.careers/careers-home/jobs/4901?lang=en-us`
5. `https://www.github.careers/careers-home/jobs/4900?lang=en-us`
6. `https://www.github.careers/careers-home/jobs/4626?lang=en-us`
7. `https://www.github.careers/careers-home/jobs/4788?lang=en-us`
8. `https://www.github.careers/careers-home/jobs/4764?lang=en-us`
9. `https://www.github.careers/careers-home/jobs/4869?lang=en-us`
10. `https://www.github.careers/careers-home/jobs/4852?lang=en-us`
11. `https://www.github.careers/careers-home/jobs/4861?lang=en-us`
12. `https://www.github.careers/careers-home/jobs/4863?lang=en-us`
13. `https://www.github.careers/careers-home/jobs/4796?lang=en-us`
14. `https://www.github.careers/careers-home/jobs/4850?lang=en-us`
15. `https://www.github.careers/careers-home/jobs/4678?lang=en-us`
16. `https://www.github.careers/careers-home/jobs/4732?lang=en-us`
17. `https://www.github.careers/careers-home/jobs/4793?lang=en-us`
18. `https://www.github.careers/careers-home/jobs/4904?lang=en-us`
19. `https://www.github.careers/careers-home/jobs/4875?lang=en-us`
20. `https://www.github.careers/careers-home/jobs/4902?lang=en-us`
... and 30 more

---

## Step 6: Pagination Detection

*No pagination URLs detected*

---

## Step 7: Queue Enqueue Summary

**URLs to Enqueue:** 50

### Enqueue Payload Sample

```json
{
  "urls": [
    "https://www.github.careers/careers-home/jobs/4867?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4871?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4870?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4901?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4900?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4626?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4788?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4764?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4869?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4852?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4861?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4863?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4796?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4850?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4678?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4732?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4793?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4904?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4875?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4902?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4874?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4843?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4756?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4749?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4940?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4851?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4934?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4829?lang=en-us",
    "https://www.github.careers/careers-home/jobs/4665?lang=en-us",
    "https://w
```
