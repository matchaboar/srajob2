import { mutation } from "./_generated/server";
import { v } from "convex/values";
import { buildJobInsert, makeFakeJobSeeds } from "./jobRecords";

export const insertFakeJobs = mutation({
  args: {},
  handler: async (ctx) => {
    const now = Date.now();
    const fakeJobs = makeFakeJobSeeds(now);
    const insertedJobs = [];
    for (const job of fakeJobs) {
      insertedJobs.push(await ctx.db.insert("jobs", buildJobInsert(job, now)));
    }

    return {
      success: true,
      message: `Inserted ${insertedJobs.length} fake jobs`,
      jobIds: insertedJobs,
    };
  },
});
