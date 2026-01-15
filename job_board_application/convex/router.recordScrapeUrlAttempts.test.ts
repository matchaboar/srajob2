import { describe, expect, it, vi } from "vitest";
import { recordScrapeUrlAttempts } from "./router";
import { getHandler } from "./__tests__/getHandler";

describe("recordScrapeUrlAttempts", () => {
  it("creates a new record when URL does not exist", async () => {
    const insertedRecords: any[] = [];
    const ctx: any = {
      db: {
        query: vi.fn().mockReturnValue({
          withIndex: vi.fn().mockReturnValue({
            first: vi.fn().mockResolvedValue(null),
          }),
        }),
        insert: vi.fn().mockImplementation((_table, record) => {
          insertedRecords.push(record);
          return Promise.resolve("inserted-id");
        }),
        patch: vi.fn(),
      },
    };

    const handler = getHandler(recordScrapeUrlAttempts);
    const result = await handler(ctx, {
      entries: [
        {
          url: "https://example.com/job/123",
          sourceUrl: "https://example.com/jobs",
          provider: "spidercloud",
          attempts: 1,
        },
      ],
    });

    expect(result).toEqual({ created: 1, updated: 0 });
    expect(ctx.db.insert).toHaveBeenCalledTimes(1);
    expect(ctx.db.patch).not.toHaveBeenCalled();
    expect(insertedRecords[0]).toMatchObject({
      url: "https://example.com/job/123",
      sourceUrl: "https://example.com/jobs",
      provider: "spidercloud",
      attemptCount: 1,
      lastQueueAttempt: 1,
    });
    expect(insertedRecords[0].lastAttemptAt).toBeDefined();
  });

  it("updates existing record when URL already exists", async () => {
    const existingRecord = {
      _id: "existing-id",
      url: "https://example.com/job/123",
      sourceUrl: "https://example.com/jobs",
      provider: "spidercloud",
      attemptCount: 2,
      lastAttemptAt: Date.now() - 60000,
      lastQueueAttempt: 1,
    };

    const patchedRecords: any[] = [];
    const ctx: any = {
      db: {
        query: vi.fn().mockReturnValue({
          withIndex: vi.fn().mockReturnValue({
            first: vi.fn().mockResolvedValue(existingRecord),
          }),
        }),
        insert: vi.fn(),
        patch: vi.fn().mockImplementation((id, patch) => {
          patchedRecords.push({ id, patch });
          return Promise.resolve();
        }),
      },
    };

    const handler = getHandler(recordScrapeUrlAttempts);
    const result = await handler(ctx, {
      entries: [
        {
          url: "https://example.com/job/123",
          sourceUrl: "https://example.com/jobs",
          provider: "spidercloud",
          attempts: 3,
        },
      ],
    });

    expect(result).toEqual({ created: 0, updated: 1 });
    expect(ctx.db.insert).not.toHaveBeenCalled();
    expect(ctx.db.patch).toHaveBeenCalledTimes(1);
    expect(patchedRecords[0].id).toBe("existing-id");
    expect(patchedRecords[0].patch.attemptCount).toBe(3); // 2 + 1
    expect(patchedRecords[0].patch.lastQueueAttempt).toBe(3);
    expect(patchedRecords[0].patch.provider).toBe("spidercloud");
  });

  it("processes multiple entries correctly", async () => {
    let queryCount = 0;
    const ctx: any = {
      db: {
        query: vi.fn().mockReturnValue({
          withIndex: vi.fn().mockReturnValue({
            first: vi.fn().mockImplementation(() => {
              queryCount++;
              // First URL exists, second doesn't
              return Promise.resolve(
                queryCount === 1
                  ? {
                      _id: "existing-id",
                      attemptCount: 1,
                      lastQueueAttempt: 0,
                      provider: null,
                    }
                  : null
              );
            }),
          }),
        }),
        insert: vi.fn().mockResolvedValue("new-id"),
        patch: vi.fn().mockResolvedValue(undefined),
      },
    };

    const handler = getHandler(recordScrapeUrlAttempts);
    const result = await handler(ctx, {
      entries: [
        {
          url: "https://example.com/job/111",
          sourceUrl: "https://example.com/jobs",
        },
        {
          url: "https://example.com/job/222",
          sourceUrl: "https://example.com/jobs",
        },
      ],
    });

    expect(result).toEqual({ created: 1, updated: 1 });
    expect(ctx.db.patch).toHaveBeenCalledTimes(1);
    expect(ctx.db.insert).toHaveBeenCalledTimes(1);
  });

  it("skips entries with empty url or sourceUrl", async () => {
    const ctx: any = {
      db: {
        query: vi.fn(),
        insert: vi.fn(),
        patch: vi.fn(),
      },
    };

    const handler = getHandler(recordScrapeUrlAttempts);
    const result = await handler(ctx, {
      entries: [
        { url: "", sourceUrl: "https://example.com/jobs" },
        { url: "https://example.com/job/123", sourceUrl: "" },
        { url: "   ", sourceUrl: "https://example.com/jobs" },
      ],
    });

    expect(result).toEqual({ created: 0, updated: 0 });
    expect(ctx.db.query).not.toHaveBeenCalled();
    expect(ctx.db.insert).not.toHaveBeenCalled();
    expect(ctx.db.patch).not.toHaveBeenCalled();
  });

  it("trims whitespace from url and sourceUrl", async () => {
    const insertedRecords: any[] = [];
    const ctx: any = {
      db: {
        query: vi.fn().mockReturnValue({
          withIndex: vi.fn().mockReturnValue({
            first: vi.fn().mockResolvedValue(null),
          }),
        }),
        insert: vi.fn().mockImplementation((_table, record) => {
          insertedRecords.push(record);
          return Promise.resolve("inserted-id");
        }),
        patch: vi.fn(),
      },
    };

    const handler = getHandler(recordScrapeUrlAttempts);
    await handler(ctx, {
      entries: [
        {
          url: "  https://example.com/job/123  ",
          sourceUrl: "  https://example.com/jobs  ",
        },
      ],
    });

    expect(insertedRecords[0].url).toBe("https://example.com/job/123");
    expect(insertedRecords[0].sourceUrl).toBe("https://example.com/jobs");
  });

  it("handles optional provider and attempts fields", async () => {
    const insertedRecords: any[] = [];
    const ctx: any = {
      db: {
        query: vi.fn().mockReturnValue({
          withIndex: vi.fn().mockReturnValue({
            first: vi.fn().mockResolvedValue(null),
          }),
        }),
        insert: vi.fn().mockImplementation((_table, record) => {
          insertedRecords.push(record);
          return Promise.resolve("inserted-id");
        }),
        patch: vi.fn(),
      },
    };

    const handler = getHandler(recordScrapeUrlAttempts);
    await handler(ctx, {
      entries: [
        {
          url: "https://example.com/job/123",
          sourceUrl: "https://example.com/jobs",
          // No provider or attempts
        },
      ],
    });

    expect(insertedRecords[0].provider).toBeUndefined();
    expect(insertedRecords[0].lastQueueAttempt).toBeUndefined();
  });
});
