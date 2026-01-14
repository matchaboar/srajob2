/**
 * Queue-related type definitions.
 * Re-exports from statusStyles for convenience.
 */
export type { QueueStatus } from "../lib/statusStyles";

export interface QueuedUrlRowItem {
  _id: string;
  url: string;
  sourceUrl: string;
  provider?: string;
  urlType?: "listing" | "detail" | null;
  bucket?: number | null;
  siteId?: string | null;
  status: import("../lib/statusStyles").QueueStatus;
  createdAt: number;
  scheduledAt?: number | null;
  attempts?: number | null;
  completedAt?: number | null;
  lastError?: string | null;
}
