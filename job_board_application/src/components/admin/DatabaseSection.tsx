import { useQuery, useMutation } from "convex/react";
import { api } from "../../../convex/_generated/api";
import { useState } from "react";
import clsx from "clsx";
import { toast } from "sonner";

export function DatabaseSection() {
  const insertFakeJobs = useMutation(api.seedData.insertFakeJobs);
  const normalizeDevTestJobs = useMutation(api.jobs.normalizeDevTestJobs);
  const reparseAllJobs = useMutation(api.jobs.reparseAllJobs);
  const deleteJob = useMutation(api.jobs.deleteJob);
  const resetTodayAndRunAll = useMutation(api.router.resetTodayAndRunAllScheduled);
  const recentJobs = useQuery(api.jobs.getRecentJobs);
  const [resettingToday, setResettingToday] = useState(false);
  const resetBatchSize = 25;
  const resetMaxPasses = 500;

  const handleInsertFakeJobs = async () => {
    try {
      const result = await insertFakeJobs({});
      toast.success(result.message);
    } catch {
      toast.error("Failed to insert fake jobs");
    }
  };

  const handleResetTodayAndRunAll = async () => {
    const confirmed = window.confirm(
      "This will delete all scrapes and jobs from today, clear every queued scrape URL, remove today's skipped jobs, and trigger every enabled scheduled site to run now. Continue?"
    );
    if (!confirmed) return;

    try {
      setResettingToday(true);
      const totals = {
        jobsDeleted: 0,
        scrapesDeleted: 0,
        queueDeleted: 0,
        skippedDeleted: 0,
        sitesTriggered: 0,
      };
      let hasMore = true;
      let passes = 0;
      let windowStart: number | undefined;
      let windowEnd: number | undefined;

      while (hasMore) {
        const res = await resetTodayAndRunAll({ batchSize: resetBatchSize, windowStart, windowEnd });
        if (windowStart === undefined) {
          windowStart = res.windowStart;
          windowEnd = res.windowEnd;
        }
        totals.jobsDeleted += res.jobsDeleted ?? 0;
        totals.scrapesDeleted += res.scrapesDeleted ?? 0;
        totals.queueDeleted += res.queueDeleted ?? 0;
        totals.skippedDeleted += res.skippedDeleted ?? 0;
        totals.sitesTriggered += res.sitesTriggered ?? 0;
        hasMore = Boolean(res.hasMore);
        passes += 1;
        if (passes >= resetMaxPasses && hasMore) {
          throw new Error("Reset exceeded the maximum number of passes. Try again or reduce the batch size.");
        }
      }
      toast.success(
        `Deleted ${totals.jobsDeleted} jobs, removed ${totals.scrapesDeleted} scrapes, cleared ${totals.queueDeleted} queued URLs, removed ${totals.skippedDeleted} skipped, triggered ${totals.sitesTriggered} sites`
      );
    } catch (err: any) {
      toast.error(err?.message ?? "Failed to reset and run all scheduled sites");
    } finally {
      setResettingToday(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm">
        <h2 className="text-lg font-semibold text-white mb-4">Actions</h2>
        <div className="flex flex-wrap gap-3">
          <button
            onClick={() => { void handleInsertFakeJobs(); }}
            className="px-3 py-1.5 bg-indigo-600 text-white text-sm font-medium rounded hover:bg-indigo-500 transition-colors"
          >
            Insert 10 Fake Jobs
          </button>
          <button
            onClick={() => {
              void (async () => {
                try {
                  const res = await normalizeDevTestJobs({});
                  toast.success(`Normalized ${res.updated} jobs`);
                } catch {
                  toast.error("Failed to normalize");
                }
              })();
            }}
            className="px-3 py-1.5 bg-emerald-600 text-white text-sm font-medium rounded hover:bg-emerald-500 transition-colors"
          >
            Normalize Dev/Test Jobs
          </button>
          <button
            onClick={() => {
              void (async () => {
                try {
                  const res = await reparseAllJobs({});
                  toast.success(`Re-parsed ${res.updated} of ${res.scanned} jobs`);
                } catch (err: any) {
                  toast.error(err?.message ?? "Failed to re-parse");
                }
              })();
            }}
            className="px-3 py-1.5 bg-amber-600 text-white text-sm font-medium rounded hover:bg-amber-500 transition-colors"
          >
            Re-parse All Jobs
          </button>
          <button
            onClick={() => { void handleResetTodayAndRunAll(); }}
            disabled={resettingToday}
            className={clsx(
              "px-3 py-1.5 text-white text-sm font-medium rounded transition-colors",
              resettingToday
                ? "bg-red-900/60 cursor-not-allowed"
                : "bg-red-700 hover:bg-red-600"
            )}
            title="Deletes today's scrapes/jobs/skipped records, clears the scrape queue, and manually triggers all enabled scheduled sites"
          >
            {resettingToday ? "Running..." : "Purge today + Run all scheduled"}
          </button>
        </div>
        <p className="text-[11px] text-slate-500 mt-2">
          Uses the current server day (midnight to midnight) for the delete window and triggers every enabled site with a schedule.
        </p>
      </div>

      <div className="bg-slate-900 p-4 rounded border border-slate-800 shadow-sm">
        <h2 className="text-lg font-semibold text-white mb-4">Current Jobs ({recentJobs?.length || 0})</h2>
        <div className="space-y-2">
          {recentJobs ? (
            recentJobs.map((job) => (
              <div key={job._id} className="flex items-center justify-between p-3 bg-slate-950/30 border border-slate-800 rounded hover:border-slate-700 transition-colors group">
                <div className="min-w-0">
                  <h3 className="text-sm font-medium text-slate-200 truncate">{job.title}</h3>
                  <p className="text-xs text-slate-500 truncate">{job.company} • {job.location}</p>
                </div>
                <div className="flex items-center gap-3 pl-4">
                  <span className="text-[10px] text-slate-600 font-mono whitespace-nowrap">
                    {new Date(job.postedAt).toLocaleDateString()}
                  </span>
                  <button
                    onClick={() => {
                      void (async () => {
                        try {
                          await deleteJob({ jobId: job._id as any });
                          toast.success("Deleted");
                        } catch {
                          toast.error("Failed");
                        }
                      })();
                    }}
                    className="opacity-0 group-hover:opacity-100 px-2 py-1 text-[10px] bg-red-900/20 text-red-400 border border-red-900/30 rounded hover:bg-red-900/40 transition-all"
                  >
                    Delete
                  </button>
                </div>
              </div>
            ))
          ) : (
            <p className="text-sm text-slate-500">Loading...</p>
          )}
        </div>
      </div>
    </div>
  );
}
