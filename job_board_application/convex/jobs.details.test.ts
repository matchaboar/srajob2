import { describe, expect, it } from "vitest";
import { getJobById, getJobDetails } from "./jobs";
import { getHandler } from "./__tests__/getHandler";

type JobRow = { _id: string; url: string; location?: string; remote?: boolean; totalCompensation?: number };
type DetailRow = { _id: string; jobId: string; description?: string };

class FakeDb {
  private job: JobRow | null;
  private detail: DetailRow | null;

  constructor(job: JobRow | null, detail: DetailRow | null) {
    this.job = job ? { ...job } : null;
    this.detail = detail ? { ...detail } : null;
  }

  get(id: string) {
    if (this.job?._id === id) return this.job;
    return null;
  }

  patch(id: string, updates: Record<string, any>) {
    if (this.job?._id === id) {
      Object.assign(this.job, updates);
      return;
    }
    throw new Error(`Unknown record ${id}`);
  }

  query(table: string) {
    if (table !== "job_details") {
      throw new Error(`Unexpected table ${table}`);
    }
    const detail = this.detail;
    return {
      withIndex(_name: string, cb: (q: any) => any) {
        const jobId = cb({ eq: (_field: string, value: string) => value });
        const match = detail && detail.jobId === jobId ? detail : null;
        return {
          first: async () => match,
        };
      },
    };
  }
}

describe("getJobDetails", () => {
  it("does not expose job_details identifiers", async () => {
    const ctx: any = {
      db: new FakeDb(
        { _id: "job-1", url: "https://example.com/job/1", location: "Remote" },
        { _id: "detail-1", jobId: "job-1", description: "Details" }
      ),
    };

    const handler = getHandler(getJobDetails);
    const result = await handler(ctx, { jobId: "job-1" });

    expect(result).toMatchObject({ description: "Details" });
    expect(result).not.toHaveProperty("_id");
    expect(result).not.toHaveProperty("jobId");
  });
});

describe("getJobById", () => {
  it("keeps the job id when merging details", async () => {
    const ctx: any = {
      db: new FakeDb(
        { _id: "job-1", url: "https://example.com/job/1", location: "Remote" },
        { _id: "detail-1", jobId: "job-1", description: "Details" }
      ),
    };

    const handler = getHandler(getJobById);
    const result = await handler(ctx, { id: "job-1" });

    expect(result?._id).toBe("job-1");
    expect(result?.description).toBe("Details");
  });
});
