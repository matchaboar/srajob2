import { internalMutation } from "./_generated/server";
import { internal, components } from "./_generated/api";
import { Crons } from "@convex-dev/crons";

const crons = new Crons((components as any).crons);
const COMPANY_SUMMARY_CRON_NAME = "company-summary-refresh";
const COMPANY_SUMMARY_INTERVAL_MS = 1000 * 60 * 60 * 24;

type RegisterCompanySummaryCronResult =
  | { status: "exists" }
  | { status: "updated"; id: string }
  | { status: "created"; id: string };

export const registerCompanySummaryCron = internalMutation({
  args: {},
  handler: async (ctx): Promise<RegisterCompanySummaryCronResult> => {
    const desiredSchedule = { kind: "interval", ms: COMPANY_SUMMARY_INTERVAL_MS } as const;
    const existing = await crons.get(ctx, { name: COMPANY_SUMMARY_CRON_NAME });
    if (existing) {
      const currentSchedule = existing.schedule;
      const matchesInterval =
        currentSchedule.kind === "interval" && currentSchedule.ms === desiredSchedule.ms;
      if (matchesInterval) {
        return { status: "exists" };
      }
      await crons.delete(ctx, { name: COMPANY_SUMMARY_CRON_NAME });
    }

    const id: string = await crons.register(
      ctx,
      desiredSchedule,
      internal.jobs.refreshCompanySummaries,
      {},
      COMPANY_SUMMARY_CRON_NAME,
    );

    return { id, status: existing ? "updated" : "created" };
  },
});
