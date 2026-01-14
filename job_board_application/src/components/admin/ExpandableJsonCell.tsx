import { useState, useEffect, useRef, type MouseEvent } from "react";
import { toast } from "sonner";

interface ExpandableJsonCellProps {
  value: any;
}

export function ExpandableJsonCell({ value }: ExpandableJsonCellProps) {
  const [hovered, setHovered] = useState(false);
  const [popoverStyle, setPopoverStyle] = useState<{
    top: number;
    left: number;
    maxWidth: number;
    maxHeight: number;
  }>(() => ({
    top: 0,
    left: 0,
    maxWidth: 520,
    maxHeight: 520,
  }));
  const [copied, setCopied] = useState(false);
  const copyResetRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (copyResetRef.current) {
        clearTimeout(copyResetRef.current);
      }
    };
  }, []);

  const formatJson = (val: any) => {
    if (val === undefined) return "—";
    if (typeof val === "string") return val;
    try {
      return JSON.stringify(val, null, 2);
    } catch {
      return String(val);
    }
  };

  const handleMove = (event: MouseEvent<HTMLDivElement>) => {
    const vw = window.innerWidth || 1200;
    const vh = window.innerHeight || 800;
    const maxWidth = Math.min(520, vw - 24);
    const maxHeight = Math.min(520, vh - 24);
    const preferredLeft = event.clientX - maxWidth * 0.2;
    const clampedLeft = Math.min(Math.max(12, preferredLeft), vw - maxWidth - 12);
    const preferredTop = event.clientY + 12;
    const clampedTop = Math.min(preferredTop, vh - maxHeight - 12);
    setPopoverStyle({ top: clampedTop, left: clampedLeft, maxWidth, maxHeight });
  };

  const formatted = formatJson(value);

  const handleCopy = async () => {
    if (!formatted || formatted === "—") return;
    if (copyResetRef.current) {
      clearTimeout(copyResetRef.current);
    }
    try {
      if (typeof navigator === "undefined" || !navigator.clipboard) {
        toast.error("Clipboard not available in this browser");
        return;
      }
      await navigator.clipboard.writeText(formatted);
      setCopied(true);
      copyResetRef.current = setTimeout(() => setCopied(false), 1200);
    } catch (err) {
      console.error("Failed to copy JSON", err);
      toast.error("Failed to copy");
    }
  };

  return (
    <div
      className="relative flex items-center gap-2 group"
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onMouseMove={handleMove}
    >
      <pre
        className="bg-slate-950/60 border border-slate-800 rounded px-1 py-0.5 max-h-5 min-h-[14px] leading-none overflow-hidden truncate font-mono text-[9px] cursor-pointer transition-colors hover:border-slate-600 focus:outline-none focus:ring-1 focus:ring-emerald-500"
        onClick={() => {
          void handleCopy();
        }}
        role="button"
        tabIndex={0}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            void handleCopy();
          }
        }}
        title={copied ? "Copied" : "Click to copy"}
      >
        {formatted}
      </pre>
      <button
        type="button"
        onClick={(event) => {
          event.stopPropagation();
          void handleCopy();
        }}
        className="inline-flex h-5 w-5 items-center justify-center rounded border border-slate-800 bg-slate-950 text-slate-300 hover:text-white hover:border-slate-600 hover:bg-slate-800 transition-colors focus:outline-none focus:ring-1 focus:ring-emerald-500"
        title={copied ? "Copied" : "Copy JSON"}
        aria-label="Copy JSON to clipboard"
      >
        <svg
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="h-3.5 w-3.5"
        >
          <rect x="9" y="9" width="11" height="11" rx="2" ry="2" />
          <path d="M5 15V5a2 2 0 0 1 2-2h10" />
        </svg>
      </button>
      {copied && <span className="text-[9px] text-emerald-300 font-semibold">Copied</span>}
      {hovered && (
        <div
          className="fixed z-50 pointer-events-none"
          style={{
            top: popoverStyle.top,
            left: popoverStyle.left,
            width: popoverStyle.maxWidth,
            maxWidth: popoverStyle.maxWidth,
            maxHeight: popoverStyle.maxHeight,
          }}
        >
          <div className="bg-slate-950 border border-slate-600 rounded shadow-2xl p-3 max-h-[32rem] overflow-auto">
            <pre className="whitespace-pre-wrap break-words font-mono text-[11px]">{formatted}</pre>
          </div>
        </div>
      )}
    </div>
  );
}
