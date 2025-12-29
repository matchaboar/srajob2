import { describe, expect, it } from "vitest";
import { __test } from "./sites";

const { collectWithLimit, countJobs } = __test;

describe("collectWithLimit", () => {
  it("paginates until the max items", async () => {
    const pages = [
      { page: [1, 2], isDone: false, continueCursor: 1 },
      { page: [3, 4], isDone: false, continueCursor: 2 },
      { page: [5], isDone: true, continueCursor: null },
    ];
    const cursorable = {
      paginate: async ({ cursor }: { cursor: number | null }) => pages[cursor ?? 0],
    };

    const res = await collectWithLimit(cursorable, 4, 2);
    expect(res).toEqual([1, 2, 3, 4]);
  });

  it("uses collect when available", async () => {
    const cursorable = {
      collect: async () => [1, 2, 3],
      paginate: async () => {
        throw new Error("should not paginate");
      },
    };

    const res = await collectWithLimit(cursorable, 10, 2);
    expect(res).toEqual([1, 2, 3]);
  });

  it("stops when paginate repeats the same cursor", async () => {
    const calls: Array<string | null> = [];
    const cursorable = {
      paginate: async ({ cursor }: { cursor: string | null }) => {
        calls.push(cursor ?? null);
        if (cursor == null) {
          return { page: [1], isDone: false, continueCursor: "same" };
        }
        return { page: [2], isDone: false, continueCursor: "same" };
      },
    };

    const res = await collectWithLimit(cursorable, 10, 2);
    expect(res).toEqual([1, 2]);
    expect(calls).toHaveLength(2);
  });
});

describe("countJobs", () => {
  it("counts array shapes", () => {
    expect(countJobs([1, 2, 3])).toBe(3);
  });

  it("counts nested result shapes", () => {
    expect(countJobs({ items: [1, 2] })).toBe(2);
    expect(countJobs({ results: [1] })).toBe(1);
    expect(countJobs({ results: { items: [1, 2, 3] } })).toBe(3);
    expect(countJobs({ normalized: [1] })).toBe(1);
  });
});
