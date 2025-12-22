interface JobRowLevelPillProps {
  label: string;
}

export function JobRowLevelPill({ label }: JobRowLevelPillProps) {
  return (
    <span className="px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wide rounded-md border border-slate-800 bg-slate-900/70 text-slate-400 shrink-0">
      {label}
    </span>
  );
}
