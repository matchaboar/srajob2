/**
 * Centralized date formatting utilities.
 * Extracted from JobRow, JobBoard, JobDetailsPage.
 */

/**
 * Format timestamp as short date: "Dec 21"
 */
export const formatShortDate = (timestamp: number): string => {
  return new Date(timestamp).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
};

/**
 * Format timestamp as days ago: "7d ago"
 */
export const formatDaysAgo = (timestamp: number): string => {
  const days = Math.max(
    0,
    Math.floor((Date.now() - timestamp) / (1000 * 60 * 60 * 24))
  );
  return `${days}d ago`;
};

/**
 * Format timestamp as relative time with absolute: "7d ago • Dec 21, 2025, 3:45 PM"
 */
export const formatRelativeTime = (timestamp?: number | null): string | null => {
  if (typeof timestamp !== "number") return null;
  const delta = Math.max(0, Date.now() - timestamp);
  const minutes = Math.round(delta / (1000 * 60));
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  let relative: string;
  if (days > 0) {
    relative = `${days}d ago`;
  } else if (hours > 0) {
    relative = `${hours}h ago`;
  } else if (minutes > 0) {
    relative = `${minutes}m ago`;
  } else {
    relative = "just now";
  }
  const absolute = new Date(timestamp).toLocaleString();
  return `${relative} • ${absolute}`;
};

/**
 * Format timestamp as full date/time: "12/21/2025, 3:45:00 PM"
 */
export const formatDateTime = (value?: number): string => {
  if (typeof value !== "number") return "Unknown";
  return new Date(value).toLocaleString();
};

/**
 * Format duration between two timestamps: "2h 15m"
 */
export const formatDuration = (
  start?: number | null,
  end?: number | null
): string => {
  if (typeof start !== "number" || typeof end !== "number") return "—";
  const diff = Math.max(0, end - start);
  const totalSeconds = Math.floor(diff / 1000);
  if (totalSeconds <= 0) return "0s";
  const minutes = Math.floor(totalSeconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  if (days > 0) return `${days}d ${hours % 24}h`;
  if (hours > 0) return `${hours}h ${minutes % 60}m`;
  if (minutes > 0) return `${minutes}m ${totalSeconds % 60}s`;
  return `${totalSeconds}s`;
};

/**
 * Capitalize level label: "senior" -> "Senior"
 */
export const formatLevelLabel = (level?: string | null): string => {
  if (!level || typeof level !== "string") return "N/A";
  return level.charAt(0).toUpperCase() + level.slice(1);
};
