import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { extractJobs } from "../../convex/router";

const fixturesDir = resolve(process.cwd(), "src/test/fixtures");

const loadSpidercloudFixture = () => {
  const fixturePath = resolve(
    process.cwd(),
    "../tests/fixtures/spidercloud_store_scrape_input.json"
  );
  let raw = readFileSync(fixturePath, "utf-8");
  const rawIndex = raw.indexOf('"raw":');
  if (rawIndex !== -1) {
    const markers = ['\n      "request"', '\n      "seedUrls"', '\n      "requestedFormat"'];
    let end = -1;
    for (const marker of markers) {
      const idx = raw.indexOf(marker, rawIndex);
      if (idx !== -1 && (end === -1 || idx < end)) end = idx;
    }
    if (end !== -1) {
      raw = raw.slice(0, rawIndex) + raw.slice(end + 1);
    }
  }
  let cleaned = raw.replace(/\r/g, " ").replace(/\n/g, " ");
  const invalidEscape = /\\(?!["\\/bfnrtu])/g;
  for (let i = 0; i < 5; i += 1) {
    const next = cleaned.replace(invalidEscape, "");
    if (next === cleaned) break;
    cleaned = next;
  }
  return JSON.parse(cleaned);
};

describe("extractJobs sanitization", () => {
  it("strips raw HTML/JSON blobs from title and description", () => {
    const greenhouseBlob = `
      <html><meta name="color-scheme" content="light dark"><meta charset="utf-8">
      <pre>{
        "title":"Senior Software Engineer, Web3",
        "company_name":"Robinhood",
        "location":{"name":"Menlo Park, CA; New York, NY"}
      }</pre></html>`;

    const jobs = extractJobs([
      {
        title: greenhouseBlob,
        content: "<div><h2>Join</h2><p>Build the future of finance.</p></div>",
        url: "https://boards.greenhouse.io/robinhood/jobs/7371859",
        location: { name: "Menlo Park, CA; New York, NY" },
      },
    ]);

    expect(jobs).toHaveLength(1);
    const job = jobs[0];
    expect(job.title).toBe("Senior Software Engineer, Web3");
    expect(job.company).toBe("Robinhood");
    expect(job.location).toContain("Menlo Park");
    expect(job.description).toBe("Join Build the future of finance.");
  });

  it("falls back to cleaned raw string when JSON parse fails", () => {
    const htmlTitle = "<h1>Staff Security Engineer</h1><p>Blockchain</p>";
    const jobs = extractJobs([
      {
        title: htmlTitle,
        company: "Example Co",
        location: "Remote, USA",
        url: "https://example.com/job/1",
        description: "<p>Secure our protocols.</p>",
      },
    ]);

    expect(jobs[0].title).toBe("Staff Security Engineer Blockchain");
    expect(jobs[0].description).toBe("Secure our protocols.");
    expect(jobs[0].remote).toBe(true);
  });

  it("parses greenhouse JSON with salary range, location, and full description", () => {
    const payloadPath = resolve(fixturesDir, "datadog_greenhouse.json");
    const payload = JSON.parse(readFileSync(payloadPath, "utf-8"));
    const jobs = extractJobs([payload]);
    expect(jobs).toHaveLength(1);
    const job = jobs[0] as any;

    expect(job.title).toBe("Premier Support Engineer 2");
    expect(job.company).toBe("Datadog");
    expect(job.totalCompensation).toBe(118000); // uses max of range
    expect(job.compensationUnknown).toBe(false);
    expect(job.compensationReason).toMatch(/metadata/i);

    expect(job.location).toContain("San Francisco");
    expect(job.state).toBe("California");
    expect(job.city).toBe("San Francisco");

    expect(job.description).toContain("Technical Solutions team enables Datadog");
    expect(job.description).toContain("customers’ entire technology stacks");
  });

  it("prefers description section titles over summary sentences in listing blobs", () => {
    const payloadPath = resolve(fixturesDir, "purestorage_greenhouse_listing_blob.json");
    const payload = JSON.parse(readFileSync(payloadPath, "utf-8"));
    const jobs = extractJobs([payload]);

    expect(jobs).toHaveLength(1);
    expect(jobs[0].title).toBe("Principal Product Manager - K8s, Observability, Manageability");
    expect(jobs[0].description).toContain("unbelievably exciting area of tech");
  });

  it("prefers description section titles over summary sentences for remote listings", () => {
    const payloadPath = resolve(fixturesDir, "samsara_greenhouse_listing_blob.json");
    const payload = JSON.parse(readFileSync(payloadPath, "utf-8"));
    const jobs = extractJobs([payload]);

    expect(jobs).toHaveLength(1);
    expect(jobs[0].title).toBe("Enterprise Customer Success Manager");
    expect(jobs[0].description).toContain("Connected Operations");
  });

  it("removes embedded Netflix theme JSON from descriptions", () => {
    const payloadPath = resolve(fixturesDir, "netflix_job.json");
    const payload = JSON.parse(readFileSync(payloadPath, "utf-8"));
    const jobs = extractJobs([payload]);
    expect(jobs).toHaveLength(1);
    const job = jobs[0];
    expect(job.description).toContain("Netflix is one of the world's leading entertainment services");
    expect(job.description).not.toContain("themeOptions");
    expect(job.description).not.toContain("customTheme");
    expect(job.description).not.toContain("NetflixSans");
  });

  it("drops seed URLs from normalized scrape payloads", () => {
    const jobs = extractJobs({
      normalized: [
        {
          title: "Jobs - Snap Inc",
          company: "snapchat",
          location: "Remote",
          url: "https://careers.snap.com/jobs",
        },
        {
          title: "Staff Software Engineer",
          company: "snapchat",
          location: "Remote",
          url: "https://careers.snap.com/jobs/12345",
        },
      ],
      seedUrls: ["https://careers.snap.com/jobs"],
    });

    expect(jobs).toHaveLength(1);
    expect(jobs[0].url).toBe("https://careers.snap.com/jobs/12345");
  });

  it("drops listing URLs when sourceUrl is a listing page", () => {
    const jobs = extractJobs(
      [
        {
          title: "",
          company: "snapchat",
          location: "United States",
          url: "https://careers.snap.com/jobs",
          description: "https://careers.snap.com/jobs",
        },
        {
          title: "Staff Software Engineer",
          company: "snapchat",
          location: "Remote",
          url: "https://careers.snap.com/job?id=R0043314",
          description: "Build the next generation of Snap products.",
        },
      ],
      { sourceUrl: "https://careers.snap.com/jobs" }
    );

    expect(jobs).toHaveLength(1);
    expect(jobs[0].url).toBe("https://careers.snap.com/job?id=R0043314");
  });

  it("keeps spidercloud job URLs even when they are seed URLs", () => {
    const payload = loadSpidercloudFixture();
    const items = payload.scrape?.items ?? payload.items ?? payload;
    const seedUrls = items.seedUrls ?? [];
    const jobs = extractJobs(items);

    expect(Array.isArray(seedUrls)).toBe(true);
    expect(seedUrls.length).toBeGreaterThan(0);
    expect(jobs.length).toBeGreaterThan(0);
    expect(jobs.some((job) => job.url === seedUrls[0])).toBe(true);
  });
});
