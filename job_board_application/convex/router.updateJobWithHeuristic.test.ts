import { describe, expect, it, vi } from "vitest";
import { updateJobWithHeuristicHandler } from "./router";

describe("updateJobWithHeuristic", () => {
  it("allows heuristicVersion in args and patches the job", async () => {
    const patches: any[] = [];
    const ctx: any = {
      db: {
        patch: vi.fn((id: string, payload: any) => patches.push({ id, payload })),
      },
    };

    const res = await updateJobWithHeuristicHandler(ctx, {
      id: "job-1",
      location: "NYC",
      heuristicAttempts: 2,
      heuristicLastTried: 123,
      heuristicVersion: 4,
    });

    expect(res.updated).toBe(true);
    expect(patches[0]?.payload.heuristicVersion).toBe(4);
    expect(patches[0]?.payload.location).toBe("NYC");
  });
});
