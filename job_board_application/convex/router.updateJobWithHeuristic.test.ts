import { describe, expect, it, vi } from "vitest";
import { updateJobWithHeuristicHandler } from "./router";
import type { Id } from "./_generated/dataModel";

describe("updateJobWithHeuristic", () => {
  it("allows heuristicVersion in args and patches the job", async () => {
    const patches: any[] = [];
    const inserts: any[] = [];
    const ctx: any = {
      db: {
        patch: vi.fn((id: string, payload: any) => patches.push({ id, payload })),
        insert: vi.fn((table: string, payload: any) => inserts.push({ table, payload })),
        query: vi.fn(() => ({
          withIndex: () => ({
            first: () => null,
          }),
        })),
      },
    };

    const res = await updateJobWithHeuristicHandler(ctx, {
      id: "job-1" as Id<"jobs">,
      location: "NYC",
      heuristicAttempts: 2,
      heuristicLastTried: 123,
      heuristicVersion: 4,
      metadata: "Location\nNew York",
    });

    expect(res.updated).toBe(true);
    expect(patches[0]?.payload.location).toBe("NYC");
    expect(inserts[0]?.table).toBe("job_details");
    expect(inserts[0]?.payload.heuristicVersion).toBe(4);
    expect(inserts[0]?.payload.metadata).toBe("Location\nNew York");
  });
});
