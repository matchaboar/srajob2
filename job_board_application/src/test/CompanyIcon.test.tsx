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
});
