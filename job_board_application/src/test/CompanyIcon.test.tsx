// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { CompanyIcon } from "../components/CompanyIcon";

describe("CompanyIcon custom logos", () => {
  it("uses the CoreWeave SVG from assets", () => {
    render(<CompanyIcon company="CoreWeave" />);
    const img = screen.getByRole("img", { name: /coreweave logo/i });
    expect(img).toHaveAttribute("src", expect.stringMatching(/coreweave/i));
  });

  it("uses the Coupang SVG from assets", () => {
    render(<CompanyIcon company="Coupang" />);
    const img = screen.getByRole("img", { name: /coupang logo/i });
    expect(img).toHaveAttribute("src", expect.stringMatching(/coupang/i));
  });

  it("uses the Rubrik SVG from assets", () => {
    render(<CompanyIcon company="Rubrik" />);
    const img = screen.getByRole("img", { name: /rubrik logo/i });
    expect(img).toHaveAttribute("src", expect.stringMatching(/rubrik/i));
  });

  it("uses the The Trade Desk SVG from assets", () => {
    render(<CompanyIcon company="The Trade Desk" />);
    const img = screen.getByRole("img", { name: /the trade desk logo/i });
    expect(img).toHaveAttribute("src", expect.stringMatching(/thetradedesk/i));
  });
});
