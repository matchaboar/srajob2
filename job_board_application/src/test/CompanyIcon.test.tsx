// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { CompanyIcon } from "../components/CompanyIcon";

describe("CompanyIcon custom logos", () => {
  const expectSvgSrc = (img: HTMLElement, slug: RegExp) => {
    const src = img.getAttribute("src") ?? "";
    const isDataSvg = src.startsWith("data:image/svg+xml");
    expect(isDataSvg || slug.test(src)).toBe(true);
  };

  it("uses the CoreWeave SVG from assets", () => {
    render(<CompanyIcon company="CoreWeave" />);
    const img = screen.getByRole("img", { name: /coreweave logo/i });
    expectSvgSrc(img, /coreweave/i);
  });

  it("uses the Coupang SVG from assets", () => {
    render(<CompanyIcon company="Coupang" />);
    const img = screen.getByRole("img", { name: /coupang logo/i });
    expectSvgSrc(img, /coupang/i);
  });

  it("uses the Rubrik SVG from assets", () => {
    render(<CompanyIcon company="Rubrik" />);
    const img = screen.getByRole("img", { name: /rubrik logo/i });
    expectSvgSrc(img, /rubrik/i);
  });

  it("uses the The Trade Desk SVG from assets", () => {
    render(<CompanyIcon company="The Trade Desk" />);
    const img = screen.getByRole("img", { name: /the trade desk logo/i });
    expectSvgSrc(img, /thetradedesk/i);
  });

  it("prefers brandfetch when the company name is a hosted platform", async () => {
    render(<CompanyIcon company="Avature" url="https://bloomberg.avature.net/careers/jobs/1234" />);
    const img = await screen.findByRole("img", { name: /avature logo/i });
    expect(img.getAttribute("src") ?? "").toContain("cdn.brandfetch.io/bloomberg.com");
  });

  it("ignores JobDetail path segments for hosted job detail URLs", async () => {
    render(<CompanyIcon company="Bloomberg" url="https://bloomberg.avature.net/careers/JobDetail/Some-Role/12345" />);
    const img = await screen.findByRole("img", { name: /bloomberg logo/i });
    expect(img.getAttribute("src") ?? "").toContain("cdn.brandfetch.io/bloomberg.com");
  });

  it("falls back to the company domain when hosted urls lack a slug", async () => {
    render(<CompanyIcon company="Bloomberg" url="https://searchjobs.com/careers/JobDetail/12345" />);
    const img = await screen.findByRole("img", { name: /bloomberg logo/i });
    expect(img.getAttribute("src") ?? "").toContain("cdn.brandfetch.io/bloomberg.com");
  });

  it("falls back to the company domain when the host is an opaque UUID", async () => {
    render(
      <CompanyIcon
        company="Voltage Park"
        url="https://c93c2f7d-f00d-409f-b288-8956f84976dd.com/jobs/role/123"
      />,
    );
    const img = await screen.findByRole("img", { name: /voltage park logo/i });
    expect(img.getAttribute("src") ?? "").toContain("cdn.brandfetch.io/voltagepark.com");
  });

  it("prefers the domain segment when hosted job paths include a full domain", async () => {
    render(
      <CompanyIcon
        company="Voltage Park"
        url="https://jobs.ashbyhq.com/voltagepark.com/5b6e2a55-3f19-437f-ba4c-284d5b7b7724"
      />,
    );
    const img = await screen.findByRole("img", { name: /voltage park logo/i });
    const src = img.getAttribute("src") ?? "";
    expect(src).toContain("cdn.brandfetch.io/voltagepark.com");
    expect(src).not.toContain("voltagepark.com.com");
  });

  it("uses the brandfetch domain override for Serval", async () => {
    render(<CompanyIcon company="Serval" url="https://serval.ai/careers" />);
    const img = await screen.findByRole("img", { name: /serval logo/i });
    expect(img.getAttribute("src") ?? "").toContain("cdn.brandfetch.io/serval.com");
  });
});
