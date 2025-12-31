import { useEffect, useState } from "react";

interface CountdownTimerProps {
  targetTime: number;
  className?: string;
  showSeconds?: boolean;
}

const formatRemaining = (ms: number, showSeconds: boolean) => {
  const totalSeconds = Math.max(0, Math.floor(ms / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (!showSeconds) {
    const totalMinutes = Math.ceil(ms / 60000);
    return `${totalMinutes}m`;
  }

  if (hours > 0) {
    return `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}:${seconds
      .toString()
      .padStart(2, "0")}`;
  }
  return `${minutes.toString().padStart(2, "0")}:${seconds.toString().padStart(2, "0")}`;
};

export function CountdownTimer({ targetTime, className, showSeconds = true }: CountdownTimerProps) {
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    const interval = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(interval);
  }, []);

  const remainingMs = Math.max(0, targetTime - now);
  const isExpired = targetTime <= now;

  return (
    <span className={className}>
      {isExpired ? "Expired" : formatRemaining(remainingMs, showSeconds)}
    </span>
  );
}
