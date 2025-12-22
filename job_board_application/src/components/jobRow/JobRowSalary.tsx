import type { CompensationMeta } from "../../lib/compensation";

interface JobRowSalaryProps {
  meta: CompensationMeta;
  className?: string;
}

const getCompensationClass = (meta: CompensationMeta) => {
  if (meta.isEstimated) return "text-slate-300";
  if (meta.isUnknown) return "text-slate-500";
  return "text-emerald-400";
};

export function JobRowSalary({ meta, className }: JobRowSalaryProps) {
  const sizeClass = className ? `${className} ` : "";
  return (
    <span className={`${sizeClass}font-bold ${getCompensationClass(meta)} truncate block`} title={meta.reason}>
      {meta.display}
    </span>
  );
}
