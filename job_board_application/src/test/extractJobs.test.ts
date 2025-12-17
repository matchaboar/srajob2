import { describe, expect, it } from "vitest";
import { extractJobs } from "../../convex/router";

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
    const payload = require("./fixtures/datadog_greenhouse.json");
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
});
