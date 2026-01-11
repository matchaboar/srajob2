"use node";

import { internalAction } from "./_generated/server";
import { v } from "convex/values";

const DBOS_API_BASE = (process.env.DBOS_API_BASE ?? "http://localhost:8080").replace(/\/+$/, "");

export const enqueueListing = internalAction({
  args: {
    listingUrl: v.string(),
    provider: v.string(),
    siteId: v.string(),
    urlTypes: v.array(v.string()),
  },
  returns: v.null(),
  handler: async (_ctx, args) => {
    const response = await fetch(`${DBOS_API_BASE}/api/workflows/enqueue-listing`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
    });

    if (!response.ok) {
      const body = await response.text();
      throw new Error(`DBOS enqueue failed (${response.status}): ${body}`);
    }

    return null;
  },
});
