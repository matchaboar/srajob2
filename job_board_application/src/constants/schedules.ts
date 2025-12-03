// Shared schedule metadata for display in the admin UI.
export type WorkflowScheduleMeta = {
  name: string;
  scheduleId: string;
  intervalSeconds: number;
  description: string;
  taskQueue?: string;
};

export const SITE_LEASE_WORKFLOW: WorkflowScheduleMeta = {
  name: "SiteLease",
  scheduleId: "site-lease-firecrawl",
  intervalSeconds: 15,
  description: "Leases sites and starts Firecrawl jobs",
  taskQueue: "scraper-task-queue",
};

export const PROCESS_WEBHOOK_WORKFLOW: WorkflowScheduleMeta = {
  name: "ProcessWebhookScrape",
  scheduleId: "process-firecrawl-webhooks",
  intervalSeconds: 20,
  description: "Processes Firecrawl webhook rows and stores scrapes",
  taskQueue: "scraper-task-queue",
};

export const FETCHFOX_SPIDERCLOUD_WORKFLOW: WorkflowScheduleMeta = {
  name: "FetchfoxSpidercloud",
  scheduleId: "fetchfox-spidercloud",
  intervalSeconds: 45,
  description: "Crawls listings with FetchFox and queues SpiderCloud detail scrapes",
  taskQueue: "scraper-task-queue",
};

export const formatInterval = (seconds: number) =>
  seconds < 60 ? `${seconds}s` : `${Math.round(seconds / 60)}m`;
