import { useState, useEffect } from "react";

interface LiveTimerProps {
  startTime: number | string;
  className?: string;
  colorize?: boolean;
  warnAfterMs?: number;
  dangerAfterMs?: number;
  showAgo?: boolean;
  showSeconds?: boolean;
  suffix?: string | null;
  suffixClassName?: string;
  dataTestId?: string;
}

export function LiveTimer({
  startTime,
  className,
  colorize = false,
  warnAfterMs = 2 * 60 * 1000,
  dangerAfterMs = 10 * 60 * 1000,
  showAgo = false,
  showSeconds = true,
  suffix,
  suffixClassName,
  dataTestId,
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

    const timeString = showSeconds
      ? `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}:${seconds.toString().padStart(2, "0")}`
      : `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}`;

    if (days > 0) {
      return `${days} days, ${timeString}`;
    }
    return timeString;
  };

  const resolvedWarnAfterMs = warnAfterMs ?? 2 * 60 * 1000;
  const resolvedDangerAfterMs = Math.max(resolvedWarnAfterMs, dangerAfterMs ?? 10 * 60 * 1000);

  const resolvedColorHex =
    elapsedMs >= resolvedDangerAfterMs
      ? "#fda4af" // rose-300
      : elapsedMs >= resolvedWarnAfterMs
        ? "#fcd34d" // amber-300
        : "#6ee7b7"; // emerald-300
  const colorHex = colorize ? resolvedColorHex : undefined;

  const timeColorClass = colorize
    ? elapsedMs >= resolvedDangerAfterMs
      ? "text-rose-300"
      : elapsedMs >= resolvedWarnAfterMs
        ? "text-amber-300"
        : "text-emerald-300"
    : undefined;

  // Inline/variable color to avoid inheritance overriding the intended tone when wrapped in muted containers.
  const timeColorStyle = colorize
    ? { color: colorHex, WebkitTextFillColor: colorHex, ["--live-timer-color" as any]: colorHex }
    : undefined;

  const baseClass = "font-mono font-semibold inline-flex items-center align-middle leading-none gap-1";
  const containerClassName = ["live-timer", baseClass, className].filter(Boolean).join(" ");
  const containerStyle = colorize ? timeColorStyle : undefined;
  const suffixLabel = suffix ?? (showAgo ? "ago" : undefined);
  const suffixClasses = colorize ? suffixClassName ?? "" : suffixClassName ?? "text-slate-400";
  const suffixStyle = colorize ? timeColorStyle : undefined;

  const dataColorAttr = colorHex ? "true" : undefined;

  if (!validStart) {
    return (
      <span
        className={containerClassName}
        style={containerStyle}
        data-testid={dataTestId}
        data-color={dataColorAttr}
      >
        —
      </span>
    );
  }

  return (
    <span
      className={containerClassName}
      style={containerStyle}
      data-testid={dataTestId}
      data-color={dataColorAttr}
    >
      <span className={timeColorClass} style={timeColorStyle}>
        {formatElapsedTime(validStart)}
      </span>
      {suffixLabel && (
        <span className={suffixClasses} style={suffixStyle}>
          {suffixLabel}
        </span>
      )}
    </span>
  );
}
