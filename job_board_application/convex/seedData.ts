import { mutation } from "./_generated/server";
import { buildJobInsert, makeFakeJobSeeds } from "./jobRecords";
import { normalizeJobUrlKey } from "./jobUrlUtils";

const JOB_URL_BUCKETS = 256;
const hashStringToBucket = (value: string, bucketCount: number) => {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) % bucketCount;
};

const recordJobUrlKey = async (ctx: any, rawUrl: string, jobId: string) => {
  const key = normalizeJobUrlKey(rawUrl);
  if (!key) return;
  const bucket = hashStringToBucket(key, JOB_URL_BUCKETS);
  const existing = await ctx.db
    .query("job_url_keys")
    .withIndex("by_bucket_url", (q: any) => q.eq("bucket", bucket).eq("url", key))
    .first();
  if (existing) return;
  await ctx.db.insert("job_url_keys", {
    bucket,
    url: key,
    jobId,
    createdAt: Date.now(),
  });
};

export const insertFakeJobs = mutation({
  args: {},
  handler: async (ctx) => {
    const now = Date.now();
    const fakeJobs = makeFakeJobSeeds(now);
    const insertedJobs = [];
    for (const job of fakeJobs) {
      const jobId = await ctx.db.insert("jobs", buildJobInsert(job, now));
      await recordJobUrlKey(ctx, job.url, jobId);
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
