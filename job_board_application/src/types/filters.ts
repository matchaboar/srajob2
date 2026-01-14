/**
 * Filter-related type definitions.
 * Extracted from JobBoard.tsx.
 */
import type { Id } from "../../convex/_generated/dataModel";
import type { Level } from "./job";

export const TARGET_STATES = [
  "Washington",
  "New York",
  "California",
  "Arizona",
] as const;
export type TargetState = (typeof TARGET_STATES)[number];

export type SavedFilterId = Id<"saved_filters">;

export interface Filters {
  search: string;
  includeRemote: boolean;
  state: TargetState | null;
  country: string;
  level: Level | null;
  minCompensation: number | null;
  maxCompensation: number | null;
  hideUnknownCompensation: boolean;
  engineer: boolean;
  companies: string[];
}

export interface SavedFilter {
  _id: SavedFilterId;
  name: string;
  search?: string;
  useSearch?: boolean;
  remote?: boolean;
  includeRemote?: boolean;
  state?: TargetState | null;
  country?: string | null;
  level?: Level | null;
  minCompensation?: number;
  maxCompensation?: number;
  hideUnknownCompensation?: boolean;
  engineer?: boolean;
  isSelected: boolean;
  companies?: string[];
}

/**
 * Create empty filters with default values.
 */
export const buildEmptyFilters = (): Filters => ({
  search: "",
  includeRemote: true,
  state: null,
  country: "",
  level: null,
  minCompensation: null,
  maxCompensation: null,
  hideUnknownCompensation: false,
  engineer: false,
  companies: [],
});
