import { describe, expect, it, vi } from "vitest";
import { getHandler } from "./__tests__/getHandler";

vi.mock("@convex-dev/auth/server", () => ({
  getAuthUserId: () => "user-1",
}));

import { resetQueuedUrlRetries } from "./jobs";

describe("resetQueuedUrlRetries", () => {
  it("no-ops now that DBOS handles retries", async () => {
    const patches: Array<{ id: string; updates: Record<string, any> }> = [];
    const ctx: any = {
      db: {
        get: async () => null,
        patch: async (id: string, updates: Record<string, any>) => {
          patches.push({ id, updates });
        },
      },
    };
    const handler = getHandler(resetQueuedUrlRetries);

    const result = await handler(ctx, { id: "queue-1" });

    expect(result).toEqual({ success: true, skipped: true });
    expect(patches).toHaveLength(0);
  });
});
