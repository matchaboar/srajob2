import { describe, expect, it, vi } from "vitest";
import { completeScrapeUrls } from "./router";
import { getHandler } from "./__tests__/getHandler";

describe("completeScrapeUrls", () => {
  it("no-ops now that DBOS handles queue completion", async () => {
    const ctx: any = {
      db: {
        query: vi.fn(),
        patch: vi.fn(),
        insert: vi.fn(),
        delete: vi.fn(),
      },
    };

    const handler = getHandler(completeScrapeUrls);
    const result = await handler(ctx, {
      urls: ["https://example.com/job/1"],
      status: "failed",
      error: "timeout",
    });

    expect(result).toEqual({ processed: 0, skipped: true });
    expect(ctx.db.query).not.toHaveBeenCalled();
    expect(ctx.db.patch).not.toHaveBeenCalled();
    expect(ctx.db.insert).not.toHaveBeenCalled();
    expect(ctx.db.delete).not.toHaveBeenCalled();
  });

  it("returns a no-op response for 404 failures", async () => {
    const ctx: any = {
      db: {
        query: vi.fn(),
        patch: vi.fn(),
        insert: vi.fn(),
        delete: vi.fn(),
      },
    };

    const handler = getHandler(completeScrapeUrls);
    const result = await handler(ctx, {
      urls: ["https://example.com/job/404"],
      status: "failed",
      error: "404 not found",
    });

    expect(result).toEqual({ processed: 0, skipped: true });
    expect(ctx.db.query).not.toHaveBeenCalled();
    expect(ctx.db.patch).not.toHaveBeenCalled();
    expect(ctx.db.insert).not.toHaveBeenCalled();
    expect(ctx.db.delete).not.toHaveBeenCalled();
  });
});
