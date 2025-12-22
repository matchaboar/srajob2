import { motion } from "framer-motion";
import { LiveTimer } from "./LiveTimer";


interface StatusTrackerProps {
  status: string | null;
  updatedAt: number | null;
  compact?: boolean;
}

export function StatusTracker({ status, updatedAt, compact = false }: StatusTrackerProps) {
  const steps = ["Applied", "Queued", "Processing", "Done"];

  // Normalize status to match steps
  const currentStatus = (status || "Applied").toLowerCase();

  let activeIndex = 0;
  if (currentStatus === "pending") activeIndex = 1;
  else if (currentStatus === "processing") activeIndex = 2;
  else if (currentStatus === "completed") activeIndex = 3;
  else if (currentStatus === "failed") activeIndex = 3; // Failed is also "done" but red

  const isFailed = currentStatus === "failed";

  return (
    <div className={`flex flex-col w-full ${compact ? "" : "max-w-[280px] items-end"}`}>
      {/* Arrow-based tracker */}
      <div className={`flex items-center w-full ${compact ? "" : "mb-2"}`}>
        {steps.map((step, index) => {
          const isCompleted = index < activeIndex;
          const isCurrent = index === activeIndex;

          // Determine colors based on state
          let bgGradient = "from-slate-800 to-slate-700";
          let borderColor = "border-slate-700";
          let textColor = "text-slate-500";
          let glowClass = "";

          if (isCompleted) {
            if (isFailed && index === steps.length - 1) {
              bgGradient = "from-red-600 to-red-700";
              borderColor = "border-red-500";
              textColor = "text-red-100";
            } else if (index === steps.length - 1) {
              bgGradient = "from-emerald-500 to-emerald-600";
              borderColor = "border-emerald-400";
              textColor = "text-emerald-100";
            } else {
              bgGradient = "from-blue-500 to-blue-600";
              borderColor = "border-blue-400";
              textColor = "text-blue-100";
            }
          } else if (isCurrent) {
            if (isFailed) {
              bgGradient = "from-red-600 to-red-700";
              borderColor = "border-red-500";
              textColor = "text-red-100";
              glowClass = "status-tracker-glow-red";
            } else if (index === steps.length - 1) {
              bgGradient = "from-emerald-500 to-emerald-600";
              borderColor = "border-emerald-400";
              textColor = "text-emerald-100";
              glowClass = "status-tracker-glow-emerald";
            } else {
              bgGradient = "from-blue-500 to-blue-600";
              borderColor = "border-blue-400";
              textColor = "text-blue-100";
              glowClass = "status-tracker-glow-blue";
            }
          }

          return (
            <div key={step} className="relative flex-1">
              {/* Rectangular Shape */}
              <motion.div
                initial={false}
                animate={{ scale: 1, opacity: 1 }}
                className={`
                                    relative ${compact ? "h-4" : "h-6"} bg-gradient-to-br ${bgGradient}
                                    border ${borderColor}
                                    ${isCurrent ? glowClass + " status-tracker-pulse" : ""}
                                    ${index === 0 ? "rounded-l-md" : ""}
                                    ${index === steps.length - 1 ? "rounded-r-md" : ""}
                                    ${index > 0 && index < steps.length - 1 ? "" : ""}
                                    transition-all duration-300
                                    flex items-center justify-center
                                `}
                style={{
                  zIndex: steps.length - index,
                }}
              >
                {/* Step Label */}
                <span className={`${compact ? "text-[8px] px-1" : "text-[9px] px-2"} font-bold uppercase tracking-wide ${textColor} relative z-10 truncate`}>
                  {step}
                </span>
              </motion.div>
            </div>
          );
        })}
      </div>

      {/* Status Text - Hidden in compact mode */}
      {!compact && (
        <div className="flex items-center gap-2 text-[10px]">
          <span
            className={`font-semibold uppercase tracking-wider ${isFailed
              ? "text-red-400"
              : activeIndex === 3
                ? "text-emerald-400"
                : "text-blue-400"
              }`}
          >
            {isFailed ? "Failed" : steps[activeIndex]}
          </span>
          {updatedAt && (
            <span className="text-slate-500 flex items-center gap-1">
              •
              <LiveTimer
                startTime={updatedAt}
                colorize
                warnAfterMs={5 * 60 * 1000}
                dangerAfterMs={30 * 60 * 1000}
                showAgo
                suffixClassName="text-slate-500"
              />
            </span>
          )}
        </div>
      )}
    </div>
  );
}
