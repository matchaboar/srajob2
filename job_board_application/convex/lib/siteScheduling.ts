export type ScheduleLike = {
  days: ("mon" | "tue" | "wed" | "thu" | "fri" | "sat" | "sun")[];
  startTime?: string | null;
  intervalMinutes?: number | null;
  timezone?: string | null;
};

type ScheduleDay = "sun" | "mon" | "tue" | "wed" | "thu" | "fri" | "sat";

const weekdayFromShort: Record<string, ScheduleDay> = {
  Sun: "sun",
  Mon: "mon",
  Tue: "tue",
  Wed: "wed",
  Thu: "thu",
  Fri: "fri",
  Sat: "sat",
};

const MINUTES_PER_DAY = 24 * 60;
const DAY_MS = 24 * 60 * 60 * 1000;

export const DEFAULT_TIMEZONE = "America/Denver";

export const parseTimeToMinutes = (value?: string) => {
  const match = (value ?? "").match(/^(\d{2}):(\d{2})$/);
  if (!match) return 0;
  const hours = parseInt(match[1] ?? "0", 10);
  const minutes = parseInt(match[2] ?? "0", 10);
  return Math.max(0, Math.min(23, hours)) * 60 + Math.max(0, Math.min(59, minutes));
};

export const zonedParts = (nowMs: number, timeZone: string) => {
  let formatter: Intl.DateTimeFormat;
  try {
    formatter = new Intl.DateTimeFormat("en-US", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
      weekday: "short",
    });
  } catch {
    formatter = new Intl.DateTimeFormat("en-US", {
      timeZone: DEFAULT_TIMEZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
      weekday: "short",
    });
  }

  const parts = formatter.formatToParts(nowMs);
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? "00";
  const year = parseInt(get("year"), 10);
  const month = parseInt(get("month"), 10);
  const day = parseInt(get("day"), 10);
  const hour = parseInt(get("hour"), 10);
  const minute = parseInt(get("minute"), 10);
  const second = parseInt(get("second"), 10);
  const weekday = weekdayFromShort[get("weekday")] ?? "sun";

  // Calculate offset for this instant in the target timezone.
  const asUtc = Date.UTC(year, month - 1, day, hour, minute, second);
  const offsetMs = nowMs - asUtc;

  return {
    year,
    month,
    day,
    hour,
    minute,
    second,
    weekday,
    offsetMs,
  };
};

export const latestEligibleTime = (schedule: ScheduleLike | null | undefined, nowMs: number) => {
  if (!schedule) return null;
  const timeZone = schedule.timezone || DEFAULT_TIMEZONE;
  const parts = zonedParts(nowMs, timeZone);
  const dayKey = parts.weekday;
  if (!schedule.days.includes(dayKey)) return null;

  const minutesNow = parts.hour * 60 + parts.minute;
  const startMinutes = parseTimeToMinutes(schedule.startTime ?? "00:00");
  if (minutesNow < startMinutes) return null;

  const interval = Math.max(1, Math.floor(schedule.intervalMinutes ?? MINUTES_PER_DAY));
  const steps = Math.floor((minutesNow - startMinutes) / interval);
  const minutesAtSlot = startMinutes + steps * interval;

  const dayStartUtc = Date.UTC(parts.year, parts.month - 1, parts.day, 0, 0, 0);
  return dayStartUtc + parts.offsetMs + minutesAtSlot * 60 * 1000;
};

export const nextEligibleTime = (schedule: ScheduleLike | null | undefined, afterMs: number) => {
  if (!schedule) return null;
  const timeZone = schedule.timezone || DEFAULT_TIMEZONE;
  const startMinutes = parseTimeToMinutes(schedule.startTime ?? "00:00");
  const interval = Math.max(1, Math.floor(schedule.intervalMinutes ?? MINUTES_PER_DAY));
  const baseMs = Math.max(0, afterMs ?? 0) + 1;

  for (let dayOffset = 0; dayOffset <= 7; dayOffset += 1) {
    const probeMs = baseMs + dayOffset * DAY_MS;
    const parts = zonedParts(probeMs, timeZone);
    const dayKey = parts.weekday;
    if (!schedule.days.includes(dayKey)) continue;

    let nextMinutes = startMinutes;
    if (dayOffset === 0) {
      const minutesNow = parts.hour * 60 + parts.minute + parts.second / 60;
      if (minutesNow >= startMinutes) {
        const steps = Math.floor((minutesNow - startMinutes) / interval);
        nextMinutes = startMinutes + (steps + 1) * interval;
      }
    }

    if (nextMinutes >= MINUTES_PER_DAY) continue;

    const dayStartUtc = Date.UTC(parts.year, parts.month - 1, parts.day, 0, 0, 0);
    return dayStartUtc + parts.offsetMs + nextMinutes * 60 * 1000;
  }

  return null;
};

export const deriveNextEligibleAt = (params: {
  hasSchedule: boolean;
  schedule?: ScheduleLike | null;
  lastRunAt?: number;
  completed?: boolean;
  nowMs?: number;
}) => {
  const lastRunAt = typeof params.lastRunAt === "number" ? params.lastRunAt : 0;
  if (!params.hasSchedule) {
    if (params.completed) return undefined;
    return lastRunAt;
  }
  if (!params.schedule) return undefined;
  if (typeof params.nowMs === "number") {
    const latest = latestEligibleTime(params.schedule, params.nowMs);
    if (latest !== null && lastRunAt < latest) {
      return latest;
    }
    const next = nextEligibleTime(params.schedule, Math.max(lastRunAt, params.nowMs));
    return typeof next === "number" ? next : undefined;
  }
  const next = nextEligibleTime(params.schedule, lastRunAt);
  return typeof next === "number" ? next : undefined;
};

export const scheduleFromRow = (row: any): ScheduleLike | null => {
  if (!row) return null;
  const days = Array.isArray(row.days) ? row.days : [];
  return {
    days,
    startTime: row.startTime ?? undefined,
    intervalMinutes: row.intervalMinutes ?? undefined,
    timezone: row.timezone ?? undefined,
  };
};
