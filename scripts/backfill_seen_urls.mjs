#!/usr/bin/env node
/**
 * Backfill seen_job_urls for all sites.
 *
 * This script runs the backfillSeenJobUrlsForAllSites mutation repeatedly
 * until all sites have been processed.
 *
 * Usage:
 *   node scripts/backfill_seen_urls.mjs
 *   node scripts/backfill_seen_urls.mjs --dry-run
 */

import { ConvexHttpClient } from "convex/browser";

const CONVEX_URL = process.env.CONVEX_URL || "https://affable-kiwi-46.convex.cloud";
const DRY_RUN = process.argv.includes("--dry-run");

const client = new ConvexHttpClient(CONVEX_URL);

async function backfillAllSites() {
  console.log(`Backfilling seen_job_urls for all sites...`);
  console.log(`Convex URL: ${CONVEX_URL}`);
  console.log(`Dry run: ${DRY_RUN}`);
  console.log("");

  let siteIndex = 0;
  let jobCursor = null;
  let totalCreated = 0;
  let totalExisted = 0;
  let totalProcessed = 0;
  let sitesProcessed = 0;

  while (true) {
    const args = {
      siteIndex,
      dryRun: DRY_RUN,
      batchSize: 200,
    };
    if (jobCursor) args.jobCursor = jobCursor;

    let result;
    try {
      result = await client.mutation("admin:backfillSeenJobUrlsForAllSites", args);
    } catch (err) {
      console.error(`Error calling mutation:`, err.message);
      console.error(`Retrying in 2 seconds...`);
      await new Promise(r => setTimeout(r, 2000));
      continue;
    }

    if (result.status === "complete") {
      console.log(`\n========================================`);
      console.log(`Backfill complete!`);
      console.log(`Total sites: ${result.totalSites}`);
      console.log(`Sites processed: ${sitesProcessed}`);
      console.log(`Total jobs processed: ${totalProcessed}`);
      console.log(`Total seen_job_urls created: ${totalCreated}`);
      console.log(`Total seen_job_urls existed: ${totalExisted}`);
      console.log(`========================================`);
      break;
    }

    if (result.status === "skipped") {
      console.log(`[${siteIndex}] Skipped: ${result.siteName || "unnamed"} (${result.reason})`);
    } else if (result.status === "processing") {
      totalCreated += result.created || 0;
      totalExisted += result.existed || 0;
      totalProcessed += result.processed || 0;

      const marker = result.siteComplete ? "DONE" : "...";
      console.log(
        `[${siteIndex}] ${result.siteName || "unnamed"}: ` +
        `processed=${result.processed}, created=${result.created}, existed=${result.existed} ${marker}`
      );

      if (result.siteComplete) {
        sitesProcessed++;
      }
    }

    if (!result.hasMore) {
      console.log(`\n========================================`);
      console.log(`Backfill complete!`);
      console.log(`Sites processed: ${sitesProcessed}`);
      console.log(`Total jobs processed: ${totalProcessed}`);
      console.log(`Total seen_job_urls created: ${totalCreated}`);
      console.log(`Total seen_job_urls existed: ${totalExisted}`);
      console.log(`========================================`);
      break;
    }

    siteIndex = result.nextSiteIndex;
    jobCursor = result.nextJobCursor;

    // Small delay to avoid rate limiting
    await new Promise(r => setTimeout(r, 100));
  }
}

backfillAllSites().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});
