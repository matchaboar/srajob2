import type { MouseEventHandler } from "react";

interface JobRowCompanyPillProps {
  company: string;
  href?: string;
  onClick?: MouseEventHandler<HTMLAnchorElement>;
  title?: string;
}

const BASE_CLASS =
  "px-2 py-0.5 text-[10px] font-medium rounded-md border border-slate-700 bg-slate-800/50 text-slate-300 truncate max-w-[12rem]";

export function JobRowCompanyPill({ company, href, onClick, title }: JobRowCompanyPillProps) {
  if (href) {
    return (
      <a
        href={href}
        target="_blank"
        rel="noreferrer"
        onClick={onClick}
        className={`${BASE_CLASS} hover:text-white hover:border-slate-500`}
        title={title}
      >
        {company}
      </a>
    );
  }

  return (
    <span className={BASE_CLASS} title={title}>
      {company}
    </span>
  );
}
