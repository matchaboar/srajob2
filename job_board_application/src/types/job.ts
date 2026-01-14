/**
 * Job-related type definitions.
 * Extracted from JobBoard.tsx.
 */
import type { Id } from "../../convex/_generated/dataModel";

export type JobId = Id<"jobs">;
export type Level = "junior" | "mid" | "senior" | "staff";

export interface DetailItem {
  label: string;
  value: string | string[];
  badge?: string;
  type?: "link";
}

export interface IgnoredJobRow {
  _id: string;
  url: string;
  sourceUrl?: string;
  company?: string;
  reason?: string;
  provider?: string;
  workflowName?: string;
  createdAt: number;
  details?: Record<string, unknown>;
  title?: string;
  description?: string;
}
