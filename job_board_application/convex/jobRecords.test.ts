import { describe, expect, it } from "vitest";
import schema from "./schema";
import { buildJobInsert, makeFakeJobSeeds } from "./jobRecords";
import type { JobInsert } from "./jobRecords";

const requiredJobFields = (): string[] => {
  const exported = JSON.parse((schema as any).export());
  const jobsTable = exported.tables.find((table: any) => table.tableName === "jobs");
  if (!jobsTable) throw new Error("jobs table is missing from the schema export");

  const fields = jobsTable.documentType.value ?? jobsTable.documentType.fields ?? {};
  return Object.entries(fields)
    .filter(([, config]: any) => config.optional !== true)
    .map(([name]) => name);
};

const expectRecordHasRequiredFields = (record: JobInsert) => {
  for (const field of requiredJobFields()) {
    expect(record[field as keyof JobInsert]).not.toBeUndefined();
  }
};

describe("job record generators stay aligned with schema", () => {
  const fixedNow = 1_700_000_000_000;

  it("fake job seeds include every required field from the schema", () => {
    const seeds = makeFakeJobSeeds(fixedNow);
    const record = buildJobInsert(seeds[0], fixedNow);
    expectRecordHasRequiredFields(record);
  });

  it("builder derives location fields and timestamps while keeping schema alignment", () => {
    const record = buildJobInsert(
      {
        title: "Sample Role",
        company: "Example Co",
        description: "Example description",
        location: "Seattle, WA",
        remote: false,
        level: "mid",
        totalCompensation: 123000,
        url: "https://example.com/jobs/sample",
        scrapedWith: "test",
      },
      fixedNow
    );

    expect(record.location).toBe("Seattle, Washington");
    expect(record.city).toBe("Seattle");
    expect(record.state).toBe("Washington");
    expect(record.postedAt).toBe(fixedNow);
    expect(record.scrapedAt).toBe(fixedNow);
    expectRecordHasRequiredFields(record);
  });
});
