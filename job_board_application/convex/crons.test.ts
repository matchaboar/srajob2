import { describe, expect, it } from "vitest";
import { clearExpiredSiteLocks } from "./crons";
import { getHandler } from "./__tests__/getHandler";

type IndexCall = {
  indexName: string | null;
  lockExpiresAtMin: number | null;
  lockExpiresAtMax: number | null;
};

class FakeQuery {
  constructor(
    private readonly rows: any[],
    private readonly tracker: { indexCalls: IndexCall[] }
  ) {}

  withIndex(name: string, cb: (q: any) => any) {
    const record: IndexCall = {
      indexName: name,
      lockExpiresAtMin: null,
      lockExpiresAtMax: null,
    };
    const builder = {
      gt: (_field: string, val: number) => {
        record.lockExpiresAtMin = val;
        return builder;
      },
      lte: (_field: string, val: number) => {
        record.lockExpiresAtMax = val;
        return builder;
      },
    };
    cb(builder);
    this.tracker.indexCalls.push(record);
    return this;
  }

  collect() {
    return this.rows;
  }
}

class FakeDb {
  constructor(
    private readonly rows: any[],
    private readonly tracker: { indexCalls: IndexCall[] }
  ) {}

  query(table: string) {
    if (table !== "sites") throw new Error(`Unexpected table ${table}`);
    return new FakeQuery(this.rows, this.tracker);
  }

  patch() {
    // noop
  }
}

describe("clearExpiredSiteLocks", () => {
  it("uses the lockExpiresAt index to avoid full site scans", async () => {
    const tracker = { indexCalls: [] as IndexCall[] };
    const ctx: any = {
      db: new FakeDb([], tracker),
    };
    const handler = getHandler(clearExpiredSiteLocks);

    await handler(ctx, {} as any);

    expect(tracker.indexCalls).toHaveLength(1);
    expect(tracker.indexCalls[0]?.indexName).toBe("by_lock_expires_at");
    expect(tracker.indexCalls[0]?.lockExpiresAtMin).toBe(0);
    expect(typeof tracker.indexCalls[0]?.lockExpiresAtMax).toBe("number");
  });
});
