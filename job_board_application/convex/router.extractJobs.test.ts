import { describe, expect, it } from "vitest";
import { extractJobs } from "./router";

const baseRow = {
  company: "ExampleCo",
  description: "Example description",
  location: "Remote",
  remote: true,
  level: "mid",
  totalCompensation: 0,
  postedAt: Date.now(),
};

describe("extractJobs", () => {
  it("strips Ashby /application URLs to the job detail page", () => {
    const jobs = extractJobs(
      [
        {
          ...baseRow,
          title: "Software Engineer",
          url: "https://jobs.ashbyhq.com/ramp/a4ecdd59-e379-4841-9bd3-c3f1f86da008/application",
        },
      ],
      { sourceUrl: "https://jobs.ashbyhq.com/ramp" }
    );

    expect(jobs).toHaveLength(1);
    expect(jobs[0].url).toBe("https://jobs.ashbyhq.com/ramp/a4ecdd59-e379-4841-9bd3-c3f1f86da008");
  });

  it("drops noisy Ashby titles for ramp job listings", () => {
    const title = `1F074311 D20A 428A 9Ca1 86E5Afbe9Baf
ramp
United States
Mid
Posted Dec 22 • 0d ago

Direct Apply
Apply with AI
https://jobs.ashbyhq.com/ramp/1f074311-d20a-428a-9ca1-86e5afbe9baf
Description
1 words
https://jobs.ashbyhq.com/ramp/1f074311-d20a-428a-9ca1-86e5afbe9baf`;

    const jobs = extractJobs(
      [
        {
          ...baseRow,
          title,
          url: "https://jobs.ashbyhq.com/ramp/1f074311-d20a-428a-9ca1-86e5afbe9baf",
        },
      ],
      { sourceUrl: "https://jobs.ashbyhq.com/ramp" }
    );

    expect(jobs).toEqual([]);
  });

  it("drops noisy Ashby titles for lambda job listings", () => {
    const title = `4B807933 F10A 45Fd B92D 6820F66Bae27
Lambda
United States
Mid
Posted Dec 22 • 0d ago

Direct Apply
Apply with AI
https://jobs.ashbyhq.com/lambda/4b807933-f10a-45fd-b92d-6820f66bae27
Description
1 words
https://jobs.ashbyhq.com/lambda/4b807933-f10a-45fd-b92d-6820f66bae27`;

    const jobs = extractJobs(
      [
        {
          ...baseRow,
          title,
          url: "https://jobs.ashbyhq.com/lambda/4b807933-f10a-45fd-b92d-6820f66bae27",
        },
      ],
      { sourceUrl: "https://jobs.ashbyhq.com/lambda" }
    );

    expect(jobs).toEqual([]);
  });

  it("extracts the title from longform Ashby listing text", () => {
    const title = `- Partner with the Customer Success and Solutions Engineering teams to onboard new customers and refine our customer journey to set new customers up for longer term success with Notion.
Ashbyhq
New York, New York
Senior
$280,000
Posted Dec 28 • 0d ago

Direct Apply
Apply with AI
https://jobs.ashbyhq.com/notion/5703a1d4-e1a2-4286-af10-a48c65fd4114
Description
1037 words

Manager, Commercial Sales @ Notion
ABOUT US:

Notion helps you build beautiful tools for your life's work.`;

    const jobs = extractJobs(
      [
        {
          ...baseRow,
          title,
          url: "https://jobs.ashbyhq.com/notion/5703a1d4-e1a2-4286-af10-a48c65fd4114",
        },
      ],
      { sourceUrl: "https://jobs.ashbyhq.com/notion" }
    );

    expect(jobs).toHaveLength(1);
    expect(jobs[0].title).toBe("Manager, Commercial Sales");
  });

  it("extracts the title from flattened Ashby listing text", () => {
    const rawTitle = `You'll independently run research projects from start to finish, translating stakeholder needs into concrete research plans and delivering insights that shape our product strategy. Working alongside passionate experts across Product, Design, Data, and Engineering, you'll help build the tools that millions of people rely on to get their work done.
Ashbyhq
San Francisco, California
Junior
$83,000
Posted Dec 28 • 0d ago

Direct Apply
Apply with AI
https://jobs.ashbyhq.com/notion/2e7b83cd-9210-4492-ad30-7b898b80807b
Description
982 words

UX Research Intern (Summer 2026) @ Notion`;
    const title = rawTitle.replace(/\s+/g, " ").trim();

    const jobs = extractJobs(
      [
        {
          ...baseRow,
          title,
          url: "https://jobs.ashbyhq.com/notion/2e7b83cd-9210-4492-ad30-7b898b80807b",
        },
      ],
      { sourceUrl: "https://jobs.ashbyhq.com/notion" }
    );

    expect(jobs).toHaveLength(1);
    expect(jobs[0].title).toContain("UX Research Intern (Summer 2026)");
  });

  it("drops Ashby listings with placeholder Application titles", () => {
    const jobs = extractJobs(
      [
        {
          ...baseRow,
          title: "Application",
          url: "https://jobs.ashbyhq.com/notion/2e7b83cd-9210-4492-ad30-7b898b80807b/application",
        },
      ],
      { sourceUrl: "https://jobs.ashbyhq.com/notion" }
    );

    expect(jobs).toEqual([]);
  });

  it("uses the Ashby slug when the company name is the provider", () => {
    const jobs = extractJobs(
      [
        {
          ...baseRow,
          title: "Account Executive",
          company: "Ashbyhq",
          url: "https://jobs.ashbyhq.com/notion/2e7b83cd-9210-4492-ad30-7b898b80807b",
        },
      ],
      { sourceUrl: "https://jobs.ashbyhq.com/notion" }
    );

    expect(jobs).toHaveLength(1);
    expect(jobs[0].company).toBe("Notion");
  });

  it("normalizes Avature URLs and derives a slug title when the title is noise", () => {
    const noisyTitle = "\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\";
    const url =
      "https://bloomberg.avature.net/careers/JobDetail/Enterprise-Services-Fixed-Income-Electronic-Trading-FIT-Client-Services-Specialist-Sydney/16436////////////////////\\\\\\\\\\\\";

    const jobs = extractJobs(
      [
        {
          ...baseRow,
          title: noisyTitle,
          url,
          company: "bloomberg",
          location: "Sydney, Australia",
        },
      ],
      { sourceUrl: "https://bloomberg.avature.net/careers/SearchJobs/engineer" }
    );

    expect(jobs).toHaveLength(1);
    expect(jobs[0].url).toBe(
      "https://bloomberg.avature.net/careers/JobDetail/Enterprise-Services-Fixed-Income-Electronic-Trading-FIT-Client-Services-Specialist-Sydney/16436"
    );
    expect(jobs[0].title).toBe(
      "Enterprise Services Fixed Income Electronic Trading FIT Client Services Specialist Sydney"
    );
  });

  it("drops unrelated external links when the source is an Avature board", () => {
    const jobs = extractJobs(
      [
        {
          ...baseRow,
          title: "Ads",
          company: "Google",
          description: "Ads",
          location: "United States",
          url: "https://policies.google.com/technologies/ads?hl=en-US",
        },
      ],
      { sourceUrl: "https://bloomberg.avature.net/careers/SearchJobs/engineer" }
    );

    expect(jobs).toEqual([]);
  });

  it("drops Avature SaveJob links", () => {
    const title =
      "Automated Source Picker (hidden)Select an option100 Women in Finance (100WF)10,000 Interns FoundationAccessibility Consortium of Enterprises";
    const jobs = extractJobs(
      [
        {
          ...baseRow,
          title,
          company: "bloomberg",
          location: "San Jose, California",
          url: "https://bloomberg.avature.net/careers/SaveJob?jobId=16453",
        },
      ],
      { sourceUrl: "https://bloomberg.avature.net/careers/SearchJobs/engineer" }
    );

    expect(jobs).toEqual([]);
  });
});
