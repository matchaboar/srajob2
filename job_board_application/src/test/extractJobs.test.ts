import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { extractJobs } from "../../convex/router";

const fixturesDir = resolve(process.cwd(), "src/test/fixtures");

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
});
