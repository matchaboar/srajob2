import { describe, expect, it } from "vitest";
import { getJobById, getJobDetails } from "./jobs";
import { getHandler } from "./__tests__/getHandler";

type JobRow = { _id: string; url: string; location?: string; remote?: boolean; totalCompensation?: number };
type DetailRow = { _id: string; jobId: string; description?: string; metadata?: string };
type ApplicationRow = { _id?: string; jobId: string; status: "applied" | "rejected" };

class FakeDb {
  private job: JobRow | null;
  private detail: DetailRow | null;
  private applications: ApplicationRow[];

  constructor(job: JobRow | null, detail: DetailRow | null, applications: ApplicationRow[] = []) {
    this.job = job ? { ...job } : null;
    this.detail = detail ? { ...detail } : null;
    this.applications = applications.map((app) => ({ ...app }));
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
    if (table === "job_details") {
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
    if (table === "applications") {
      const applications = this.applications;
      return {
        withIndex(name: string, cb: (q: any) => any) {
          if (name !== "by_job") {
            throw new Error(`Unexpected applications index ${name}`);
          }
          const jobId = cb({ eq: (_field: string, value: string) => value });
          return {
            filter(filterCb: (q: any) => any) {
              const status = filterCb({
                field: (fieldName: string) => fieldName,
                eq: (_field: string, value: string) => value,
              });
              const matches = applications.filter((app) => app.jobId === jobId && app.status === status);
              return {
                collect: async () => matches,
              };
            },
            collect: async () => applications.filter((app) => app.jobId === jobId),
          };
        },
      };
    }
    throw new Error(`Unexpected table ${table}`);
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
        { _id: "detail-1", jobId: "job-1", description: "Details", metadata: "Location\nRemote" }
      ),
    };

    const handler = getHandler(getJobById);
    const result = await handler(ctx, { id: "job-1" });

    expect(result?._id).toBe("job-1");
    expect(result?.description).toBe("Details");
    expect(result?.metadata).toBe("Location\nRemote");
  });
});

describe("getJobDetails application counts", () => {
  it("sums applied applications for grouped job ids", async () => {
    const ctx: any = {
      db: new FakeDb(null, null, [
        { jobId: "job-1", status: "applied" },
        { jobId: "job-1", status: "rejected" },
        { jobId: "job-2", status: "applied" },
      ]),
    };

    const handler = getHandler(getJobDetails);
    const result = await handler(ctx, { jobId: "job-1", groupedJobIds: ["job-1", "job-2"] });

    expect(result).toMatchObject({ applicationCount: 2 });
  });
});
