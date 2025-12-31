import { describe, expect, it } from "vitest";
import { resolveShareJobTitle } from "./router";

const OFFSEC_MARKDOWN = `
Job Application for Senior Offensive Security Engineer at Robinhood
# Senior Offensive Security Engineer
Menlo Park, CA
`;

describe("resolveShareJobTitle", () => {
  it("strips the job application prefix when description is missing", () => {
    const title = resolveShareJobTitle({
      title: "Job Application for Staff Software Engineer, Reliability at Robinhood",
    });

    expect(title).toBe("Staff Software Engineer, Reliability");
  });

  it("prefers the markdown header when the title is a placeholder", () => {
    const title = resolveShareJobTitle({
      title: "Application",
      description: OFFSEC_MARKDOWN,
    });

    expect(title).toBe("Senior Offensive Security Engineer");
  });
});
