import { internalMutation } from "./_generated/server";
import { internal, components } from "./_generated/api";
import { Crons } from "@convex-dev/crons";

const crons = new Crons((components as any).crons);

type RegisterCompanySummaryCronResult =
  | { status: "exists" }
  | { status: "created"; id: string };

export const registerCompanySummaryCron = internalMutation({
  args: {},
  handler: async (ctx): Promise<RegisterCompanySummaryCronResult> => {
    const existing = await crons.get(ctx, { name: "company-summary-refresh" });
    if (existing) {
      return { status: "exists" };
    }

    const id: string = await crons.register(
      ctx,
      { kind: "interval", ms: 1000 * 60 * 60 * 24 * 2 },
      internal.jobs.refreshCompanySummaries,
      {},
      "company-summary-refresh",
    );

    return { id, status: "created" };
  },
});
