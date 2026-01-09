import { describe, expect, it, vi } from "vitest";
import { getHandler } from "./__tests__/getHandler";

vi.mock("@convex-dev/auth/server", () => ({
  getAuthUserId: () => "user-1",
}));

import { resetQueuedUrlRetries } from "./jobs";

describe("resetQueuedUrlRetries", () => {
  it("backfills bucket and scheduledAt when resetting failed rows", async () => {
    const now = Date.now();
    const row: any = {
      _id: "queue-1",
      url: "https://example.com/jobs/1",
      sourceUrl: "https://example.com/jobs",
      status: "failed",
      attempts: 2,
      createdAt: now - 10_000,
      updatedAt: now - 5_000,
    };
    const patches: Array<{ id: string; updates: Record<string, any> }> = [];
    const ctx: any = {
      db: {
        get: async (id: string) => (id === row._id ? row : null),
        patch: async (id: string, updates: Record<string, any>) => {
          patches.push({ id, updates });
        },
      },
    };
    const handler = getHandler(resetQueuedUrlRetries);

    const nowSpy = vi.spyOn(Date, "now").mockReturnValue(now);
    await handler(ctx, { id: row._id });
    nowSpy.mockRestore();

    expect(patches).toHaveLength(1);
    const update = patches[0].updates;
    expect(update.status).toBe("pending");
    expect(update.scheduledAt).toBe(now);
    expect(typeof update.bucket).toBe("number");
  });
});
