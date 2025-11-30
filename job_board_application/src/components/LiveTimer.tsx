import { useState, useEffect } from "react";

interface LiveTimerProps {
  startTime: number | string;
  className?: string;
  colorize?: boolean;
  warnAfterMs?: number;
  dangerAfterMs?: number;
  showAgo?: boolean;
  suffix?: string | null;
  suffixClassName?: string;
}

export function LiveTimer({
  startTime,
  className,
  colorize = false,
  warnAfterMs = 2 * 60 * 1000,
  dangerAfterMs = 10 * 60 * 1000,
  showAgo = false,
  suffix,
  suffixClassName,
}: LiveTimerProps) {
  const [currentTime, setCurrentTime] = useState(Date.now());

  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentTime(Date.now());
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const parsedStart = typeof startTime === "string" ? Date.parse(startTime) : startTime;
  const validStart = Number.isFinite(parsedStart) ? parsedStart : 0;
  const elapsedMs = Math.max(0, currentTime - validStart);

  const formatElapsedTime = (timestamp: number) => {
    const elapsed = Math.floor(Math.max(0, currentTime - timestamp) / 1000);
    const days = Math.floor(elapsed / 86400);
    const hours = Math.floor((elapsed % 86400) / 3600);
    const minutes = Math.floor((elapsed % 3600) / 60);
    const seconds = elapsed % 60;

    const timeString = `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}:${seconds.toString().padStart(2, "0")}`;

    if (days > 0) {
      return `${days} days, ${timeString}`;
    }
    return timeString;
  };

  const resolvedWarnAfterMs = warnAfterMs ?? 2 * 60 * 1000;
  const resolvedDangerAfterMs = Math.max(resolvedWarnAfterMs, dangerAfterMs ?? 10 * 60 * 1000);

  const timeColorClass = colorize
    ? elapsedMs >= resolvedDangerAfterMs
      ? "text-rose-300"
      : elapsedMs >= resolvedWarnAfterMs
        ? "text-amber-300"
        : "text-emerald-300"
    : undefined;

  const baseClass = "font-mono font-semibold inline-flex items-center align-middle leading-none gap-1";
  const containerClassName = [baseClass, className].filter(Boolean).join(" ");
  const suffixLabel = suffix ?? (showAgo ? "ago" : undefined);
  const suffixClasses = suffixClassName ?? "text-slate-400";

  if (!validStart) {
    return <span className={containerClassName}>—</span>;
  }

  return (
    <span className={containerClassName}>
      <span className={timeColorClass}>{formatElapsedTime(validStart)}</span>
      {suffixLabel && <span className={suffixClasses}>{suffixLabel}</span>}
    </span>
  );
}
