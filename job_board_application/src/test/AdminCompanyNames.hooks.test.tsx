// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as convexReact from "convex/react";
import { CompanyNamesSection } from "../AdminPage";

vi.mock("convex/react", () => {
  let domainAliasesFixture: any = undefined;
  let searchCompaniesFixture: any = [];
  let useQueryCallCount = 0;

  const useQuery = vi.fn((_queryFn: any, _args?: any) => {
    const callIndex = useQueryCallCount++;
    return callIndex % 2 === 0 ? domainAliasesFixture : searchCompaniesFixture;
  });

  const useMutation = vi.fn(() => vi.fn(async () => ({})));

  return {
    useQuery,
    useMutation,
    __setDomainAliases: (value: any) => {
      domainAliasesFixture = value;
    },
    __setSearchCompanies: (value: any) => {
      searchCompaniesFixture = value;
    },
    __resetUseQueryCounter: () => {
      useQueryCallCount = 0;
    },
  };
});

vi.mock("sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

afterEach(() => {
  cleanup();
  (convexReact as any).useQuery?.mockClear?.();
  (convexReact as any).__resetUseQueryCounter?.();
});

describe("Admin CompanyNamesSection hook order", () => {
  it("rerenders safely across listDomainAliases transitions", () => {
    (convexReact as any).__setDomainAliases(undefined);
    (convexReact as any).__setSearchCompanies([]);
    const { rerender } = render(<CompanyNamesSection />);
    expect(screen.getByText(/Loading company names/i)).toBeInTheDocument();

    (convexReact as any).__setDomainAliases([]);
    expect(() => rerender(<CompanyNamesSection />)).not.toThrow();
    expect(screen.getByText(/No scrape domains found yet/i)).toBeInTheDocument();

    (convexReact as any).__setDomainAliases([
      { domain: "example.com", derivedName: "Example Co", alias: undefined, siteName: undefined, siteUrl: undefined },
    ]);
    expect(() => rerender(<CompanyNamesSection />)).not.toThrow();
    expect(screen.getByText(/No domains with aliases yet/i)).toBeInTheDocument();
  });
});
