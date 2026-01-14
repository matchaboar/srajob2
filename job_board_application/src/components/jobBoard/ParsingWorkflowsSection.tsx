interface ParsingStep {
  label: string;
  checked: boolean;
  note: string;
  status?: string;
  subtext?: string;
}

interface ParsingWorkflowsSectionProps {
  parsingSteps: ParsingStep[];
  parseNotes: string;
}

export function ParsingWorkflowsSection({ parsingSteps, parseNotes }: ParsingWorkflowsSectionProps) {
  return (
    <div className="rounded-lg border border-slate-800/70 bg-slate-900/40 p-2 space-y-2">
      <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500">
        Parsing Workflows
      </div>
      <div className="flex flex-col gap-1.5">
        {parsingSteps.map((step) => (
          <label key={step.label} className="flex items-start gap-2 text-sm text-slate-100">
            <input
              type="checkbox"
              checked={step.checked}
              readOnly
              className="mt-0.5 h-4 w-4 rounded border-slate-700 bg-slate-900 text-emerald-400 focus:ring-emerald-500"
            />
            <span className="flex-1 flex flex-col leading-tight gap-0.5">
              <span className="flex items-center gap-2">
                <span className="font-semibold">{step.label}</span>
                <span
                  className={`px-2 py-0.5 rounded-full text-[10px] font-semibold uppercase tracking-wide border ${
                    step.checked
                      ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-200"
                      : "border-amber-500/40 bg-amber-500/10 text-amber-100"
                  }`}
                >
                  {step.status || (step.checked ? "Completed" : "Pending")}
                </span>
              </span>
              <span className="text-[11px] text-slate-400">{step.note}</span>
              {step.subtext && <span className="text-[11px] text-slate-500">{step.subtext}</span>}
            </span>
          </label>
        ))}
      </div>
      <div className="text-[10px] uppercase tracking-wider font-semibold text-slate-500 pt-1">
        Parse Notes
      </div>
      <div className="rounded border border-slate-800 bg-slate-950/70 text-sm text-slate-200 px-3 py-2 whitespace-pre-wrap">
        {parseNotes}
      </div>
    </div>
  );
}

export type { ParsingStep };
