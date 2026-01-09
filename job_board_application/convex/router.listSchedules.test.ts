import { describe, expect, it } from "vitest";
import { listSchedules } from "./router";
import { getHandler } from "./__tests__/getHandler";

class FakeQuery {
  constructor(private readonly rows: any[]) {}
  collect() {
    return this.rows;
  }
}

class FakeDb {
  constructor(private readonly schedules: any[]) {}

  query(table: string) {
    if (table === "scrape_schedules") {
      return new FakeQuery(this.schedules);
    }
    if (table === "sites") {
      throw new Error("listSchedules should not scan sites");
    }
    throw new Error(`Unexpected table ${table}`);
  }
}

describe("listSchedules", () => {
  it("uses aggregate counts and avoids scanning sites", async () => {
    const schedules = [
      {
        _id: "sched-1",
        name: "Beta",
        days: ["mon"],
        startTime: "08:00",
        intervalMinutes: 60,
        timezone: "America/Denver",
        createdAt: 1,
        updatedAt: 2,
      },
      {
        _id: "sched-2",
        name: "Alpha",
        days: ["tue"],
        startTime: "09:00",
        intervalMinutes: 120,
        timezone: "America/Denver",
        createdAt: 3,
        updatedAt: 4,
      },
    ];

    const runQueryCalls: any[] = [];
    const ctx: any = {
      db: new FakeDb(schedules),
      runQuery: async (_fn: any, args: any) => {
        runQueryCalls.push(args);
        return (args.queries ?? []).map((_q: any, idx: number) => ({ count: idx + 1 }));
      },
    };
    const handler = getHandler(listSchedules);

    const res = await handler(ctx, {} as any);

    expect(runQueryCalls).toHaveLength(1);
    expect(res.map((row: any) => row.name)).toEqual(["Alpha", "Beta"]);
    expect(res.find((row: any) => row._id === "sched-1")?.siteCount).toBe(1);
    expect(res.find((row: any) => row._id === "sched-2")?.siteCount).toBe(2);
  });
});
