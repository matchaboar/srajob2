import { cronJobs } from "convex/server";
import { internal } from "./_generated/api";
import { internalMutation } from "./_generated/server";

const internalAny = internal as any;

// Clear expired locks so stuck rows recover even if workers die without reporting failure.
export const clearExpiredSiteLocks = internalMutation({
  args: {},
  handler: async (ctx) => {
    const now = Date.now();
    const sites = await ctx.db.query("sites").collect();
    let cleared = 0;
    for (const s of sites as any[]) {
      if (s.lockExpiresAt && s.lockExpiresAt <= now && s.lockedBy) {
        await ctx.db.patch(s._id, { lockedBy: "", lockExpiresAt: 0 });
        cleared++;
      }
    }
    return { cleared };
  },
});

const crons = cronJobs();

// Every 2 minutes, clean expired locks
crons.interval(
  "clearExpiredSiteLocks",
  { minutes: 2 },
  internal.crons.clearExpiredSiteLocks,
);

// Hourly, purge stale queued URLs older than 7 days
crons.interval(
  "clearStaleScrapeQueue",
  { hours: 1 },
  internal.router.clearStaleScrapeQueue,
);

// Keep the company summary cron registered (component-based cron is idempotent).
crons.interval(
  "ensureCompanySummaryCron",
  { hours: 12 },
  internalAny.companySummaryCron.registerCompanySummaryCron,
);

export default crons;
