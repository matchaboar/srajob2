import { describe, expect, it } from "vitest";
import { listWorkflowRuns } from "./temporal";
import { getHandler } from "./__tests__/getHandler";

type Run = { _id: string; startedAt: number };

class FakeRunQuery {
  constructor(private readonly rows: Run[]) {}

  withIndex(name: string) {
    if (name !== "by_started") {
      throw new Error(`unexpected index ${name}`);
    }
    return this;
  }

  order(direction: string) {
    if (direction !== "desc") {
      throw new Error(`unexpected order ${direction}`);
    }
    const sorted = [...this.rows].sort((a, b) => (b.startedAt ?? 0) - (a.startedAt ?? 0));
    return new FakeRunQuery(sorted);
  }

  take(n: number) {
    return this.rows.slice(0, n);
  }

  collect() {
    throw new Error("collect should not be used for listWorkflowRuns");
  }
}

describe("listWorkflowRuns", () => {
  it("uses the startedAt index and returns newest runs first", async () => {
    const runs: Run[] = [
      { _id: "run-1", startedAt: 100 },
      { _id: "run-2", startedAt: 300 },
      { _id: "run-3", startedAt: 200 },
    ];
    const ctx: any = {
      db: {
        query: (table: string) => {
          if (table !== "workflow_runs") throw new Error(`unexpected table ${table}`);
          return new FakeRunQuery(runs);
        },
      },
    };

    const handler = getHandler(listWorkflowRuns);
    const result = await handler(ctx, { limit: 2 });

    expect(result).toHaveLength(2);
    expect(result[0]._id).toBe("run-2");
    expect(result[1]._id).toBe("run-3");
  });
});
