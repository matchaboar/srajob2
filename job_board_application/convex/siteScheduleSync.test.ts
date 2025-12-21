import { describe, expect, it } from "vitest";
import {
  normalizeSiteScheduleEntries,
  parseSiteScheduleYamlText,
  syncSiteSchedulesFromEntries,
  type NormalizedSiteSchedule,
} from "./siteScheduleSync";

describe("parseSiteScheduleYamlText", () => {
  it("returns entries from yaml", () => {
    const text = `site_schedules:\n  - url: https://example.com/jobs\n    enabled: true\n`;
    const entries = parseSiteScheduleYamlText(text);
    expect(entries).toHaveLength(1);
    expect(entries[0]?.url).toBe("https://example.com/jobs");
  });

  it("returns empty array on invalid yaml", () => {
    const entries = parseSiteScheduleYamlText("site_schedules: [");
    expect(entries).toEqual([]);
  });
});

describe("normalizeSiteScheduleEntries", () => {
  it("dedupes by canonical key and defaults enabled", () => {
    const entries = normalizeSiteScheduleEntries([
      { url: "https://EXAMPLE.com/jobs", enabled: true },
      { url: "example.com/jobs", enabled: false },
    ]);
    expect(entries).toHaveLength(1);
    expect(entries[0]?.enabled).toBe(true);
  });

  it("drops invalid schedule blocks", () => {
    const entries = normalizeSiteScheduleEntries([
      {
        url: "https://example.com/jobs",
        schedule: { name: "Daily", days: ["nope"], startTime: "9am", intervalMinutes: 0 },
      },
    ]);
    expect(entries).toHaveLength(1);
    expect(entries[0]?.schedule).toBeUndefined();
  });

  it("normalizes schedule defaults", () => {
    const entries = normalizeSiteScheduleEntries([
      {
        url: "https://example.com/jobs",
        schedule: {
          name: "Daily",
          days: ["MON", "tue", "tue"],
          startTime: "08:00",
          intervalMinutes: 60,
        },
      },
    ]);
    expect(entries[0]?.schedule?.days).toEqual(["mon", "tue"]);
    expect(entries[0]?.schedule?.timezone).toBe("America/Denver");
  });
});

describe("syncSiteSchedulesFromEntries", () => {
  it("creates schedules and sites when missing", async () => {
    const schedules: any[] = [];
    const sites: any[] = [];
    const inserts: any[] = [];
    const ctx: any = {
      db: {
        query: (table: string) => ({
          collect: async () => (table === "scrape_schedules" ? schedules : sites),
        }),
        insert: async (table: string, payload: any) => {
          const id = `${table}-${inserts.length + 1}`;
          const row = { _id: id, ...payload };
          inserts.push({ table, row });
          if (table === "scrape_schedules") schedules.push(row);
          if (table === "sites") sites.push(row);
          return id;
        },
      },
    };

    const entries: NormalizedSiteSchedule[] = [
      {
        url: "https://example.com/jobs",
        enabled: true,
        schedule: {
          name: "Daily",
          days: ["mon"],
          startTime: "08:00",
          intervalMinutes: 1440,
          timezone: "America/Denver",
        },
      },
    ];

    const result = await syncSiteSchedulesFromEntries(ctx, entries, 123);
    expect(result.createdSchedules).toBe(1);
    expect(result.addedSites).toBe(1);
    expect(schedules[0]?.name).toBe("Daily");
    expect(sites[0]?.scheduleId).toBe(schedules[0]?._id);
  });

  it("reuses existing schedules by name", async () => {
    const schedules: any[] = [{ _id: "sched-1", name: "Daily" }];
    const sites: any[] = [];
    const ctx: any = {
      db: {
        query: (table: string) => ({
          collect: async () => (table === "scrape_schedules" ? schedules : sites),
        }),
        insert: async (table: string, payload: any) => {
          const id = `${table}-new`;
          const row = { _id: id, ...payload };
          if (table === "sites") sites.push(row);
          return id;
        },
      },
    };

    const entries: NormalizedSiteSchedule[] = [
      {
        url: "https://example.com/jobs",
        enabled: true,
        schedule: {
          name: "Daily",
          days: ["mon"],
          startTime: "08:00",
          intervalMinutes: 1440,
          timezone: "America/Denver",
        },
      },
    ];

    const result = await syncSiteSchedulesFromEntries(ctx, entries, 123);
    expect(result.createdSchedules).toBe(0);
    expect(sites[0]?.scheduleId).toBe("sched-1");
  });

  it("skips existing sites by canonical key", async () => {
    const schedules: any[] = [];
    const sites: any[] = [{ _id: "site-1", url: "https://example.com/jobs", type: "general" }];
    const inserts: any[] = [];
    const ctx: any = {
      db: {
        query: (table: string) => ({
          collect: async () => (table === "scrape_schedules" ? schedules : sites),
        }),
        insert: async (table: string, payload: any) => {
          const id = `${table}-${inserts.length + 1}`;
          inserts.push({ table, payload, id });
          return id;
        },
      },
    };

    const entries: NormalizedSiteSchedule[] = [
      { url: "example.com/jobs", enabled: true },
    ];

    const result = await syncSiteSchedulesFromEntries(ctx, entries, 123);
    expect(result.skippedExisting).toBe(1);
    expect(inserts).toHaveLength(0);
  });

  it("adds sites without schedules when none provided", async () => {
    const schedules: any[] = [];
    const sites: any[] = [];
    const ctx: any = {
      db: {
        query: (table: string) => ({
          collect: async () => (table === "scrape_schedules" ? schedules : sites),
        }),
        insert: async (_table: string, payload: any) => {
          sites.push(payload);
          return "site-1";
        },
      },
    };

    const entries: NormalizedSiteSchedule[] = [
      { url: "https://example.com/jobs", enabled: true },
    ];

    const result = await syncSiteSchedulesFromEntries(ctx, entries, 123);
    expect(result.addedSites).toBe(1);
    expect(sites[0]?.scheduleId).toBeUndefined();
  });
});
