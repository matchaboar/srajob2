import { mutation } from "./_generated/server";
import { buildJobInsert, makeFakeJobSeeds } from "./jobRecords";

export const insertFakeJobs = mutation({
  args: {},
  handler: async (ctx) => {
    const now = Date.now();
    const fakeJobs = makeFakeJobSeeds(now);
    const insertedJobs = [];
    for (const job of fakeJobs) {
      const jobId = await ctx.db.insert("jobs", buildJobInsert(job, now));
      if (job.details && Object.keys(job.details).length > 0) {
        await ctx.db.insert("job_details", { jobId, ...job.details });
      }
      insertedJobs.push(jobId);
    }

    return {
      success: true,
      message: `Inserted ${insertedJobs.length} fake jobs`,
      jobIds: insertedJobs,
    };
  },
});
