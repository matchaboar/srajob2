import { motion } from "framer-motion";
import { LiveTimer } from "./LiveTimer";
import { CompanyIcon } from "./CompanyIcon";

export type QueueStatus = "pending" | "processing" | "completed" | "failed" | "invalid";

export interface QueuedUrlRowItem {
  _id: string;
  url: string;
  sourceUrl: string;
  provider?: string;
  status: QueueStatus;
  createdAt: number;
  scheduledAt?: number | null;
  attempts?: number | null;
}

interface QueuedUrlRowProps {
  item: QueuedUrlRowItem;
  index: number;
  isSelected: boolean;
  onSelect: () => void;
  keyboardBlur?: boolean;
}

const STATUS_STYLES: Record<QueueStatus, string> = {
  pending: "bg-amber-500/10 text-amber-300 border-amber-500/20",
  processing: "bg-blue-500/10 text-blue-300 border-blue-500/20",
  completed: "bg-emerald-500/10 text-emerald-300 border-emerald-500/20",
  failed: "bg-red-500/10 text-red-300 border-red-500/20",
  invalid: "bg-slate-700/50 text-slate-300 border-slate-600/50",
};

const COMMON_SUBDOMAIN_PREFIXES = new Set([
  "www",
  "jobs",
  "careers",
  "boards",
  "board",
  "apply",
  "app",
  "join",
  "team",
  "teams",
  "work",
]);
const RESERVED_PATH_SEGMENTS = new Set([
  "boards",
  "jobs",
  "careers",
  "jobdetail",
  "job-details",
  "jobdetails",
  "apply",
  "application",
  "applications",
  "openings",
  "positions",
  "roles",
  "role",
  "departments",
  "teams",
  "en",
  "en-us",
  "en-gb",
  "en-au",
  "v1",
  "v2",
  "api",
]);
const HOSTED_JOB_DOMAINS = [
  "avature.net",
  "avature.com",
  "searchjobs.com",
  "greenhouse.io",
  "ashbyhq.com",
  "lever.co",
  "workable.com",
  "smartrecruiters.com",
  "myworkdayjobs.com",
  "icims.com",
  "jobvite.com",
  "bamboohr.com",
];

const baseDomainFromHost = (host: string) => {
  const parts = host.split(".").filter(Boolean);
  if (parts.length <= 1) return host;
  const last = parts[parts.length - 1];
  const secondLast = parts[parts.length - 2];
  const shouldUseThree = last.length === 2 || secondLast.length === 2;
  if (shouldUseThree && parts.length >= 3) {
    return parts.slice(-3).join(".");
  }
  return parts.slice(-2).join(".");
};

const extractCompanyFromPath = (pathname: string) => {
  const parts = pathname.split("/").filter(Boolean);
  for (const part of parts) {
    const cleaned = part.toLowerCase();
    if (RESERVED_PATH_SEGMENTS.has(cleaned)) continue;
    if (!/^[a-z0-9-]+$/.test(cleaned)) continue;
    return cleaned;
  }
  return null;
};

const extractCompanyLabel = (urlValue?: string) => {
  if (!urlValue) return null;
  try {
    const parsed = new URL(urlValue);
    const host = parsed.hostname.toLowerCase().replace(/^www\./, "");
    const baseDomain = baseDomainFromHost(host);
    const hostedDomain = HOSTED_JOB_DOMAINS.find((domain) => host === domain || host.endsWith(`.${domain}`));
    if (hostedDomain) {
      const hostParts = host.split(".").filter(Boolean);
      const baseParts = hostedDomain.split(".").filter(Boolean);
      if (hostParts.length > baseParts.length) {
        const subdomains = hostParts.slice(0, hostParts.length - baseParts.length);
        for (let i = subdomains.length - 1; i >= 0; i -= 1) {
          const candidate = subdomains[i];
          if (!candidate || COMMON_SUBDOMAIN_PREFIXES.has(candidate)) continue;
          return candidate;
        }
      }
      const pathCandidate = extractCompanyFromPath(parsed.pathname);
      if (pathCandidate) return pathCandidate;
      return baseDomain.split(".")[0] ?? baseDomain;
    }

    const hostParts = host.split(".").filter(Boolean);
    const baseParts = baseDomain.split(".").filter(Boolean);
    if (hostParts.length > baseParts.length) {
      const subdomains = hostParts.slice(0, hostParts.length - baseParts.length);
      for (let i = subdomains.length - 1; i >= 0; i -= 1) {
        const candidate = subdomains[i];
        if (!candidate || COMMON_SUBDOMAIN_PREFIXES.has(candidate)) continue;
        return candidate;
      }
    }
    return baseDomain.split(".")[0] ?? baseDomain;
  } catch {
    return null;
  }
};

const toDisplayName = (value: string) =>
  value
    .replace(/[-_]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());

export function QueuedUrlRow({ item, index, isSelected, onSelect, keyboardBlur }: QueuedUrlRowProps) {
  const statusStyle = STATUS_STYLES[item.status] ?? STATUS_STYLES.pending;
  const label =
    extractCompanyLabel(item.sourceUrl) ?? extractCompanyLabel(item.url) ?? "Company";
  const displayLabel = toDisplayName(label);
  const logoUrl = item.sourceUrl || item.url;

  return (
    <motion.div
      layout
      initial={false}
      animate={{
        opacity: 1,
        x: 0,
        backgroundColor: isSelected ? "rgba(30, 41, 59, 1)" : "rgba(15, 23, 42, 0)",
        transition: keyboardBlur ? { duration: 0.12 } : { duration: 0.2 },
      }}
      exit={{
        x: 0,
        opacity: 0,
        transition: { duration: 0.16 },
      }}
      onClick={onSelect}
      data-job-id={item._id}
      className={
        `relative group flex items-center gap-3 px-3 sm:px-4 py-2 border-b border-slate-800 cursor-pointer transition-colors ` +
        `${isSelected ? "bg-slate-800" : "hover:bg-slate-900"} ` +
        `${keyboardBlur ? "blur-[1px] opacity-70" : ""}`
      }
    >
      <div className={`w-1 h-8 rounded-full transition-colors ${isSelected ? "bg-amber-400" : "bg-transparent"}`} />

      <div className="flex-1 min-w-0 grid grid-cols-[auto_auto_minmax(0,1fr)_auto] sm:grid-cols-[auto_auto_minmax(0,4.5fr)_minmax(0,2fr)_minmax(0,2fr)_minmax(0,1fr)_minmax(0,1fr)_minmax(0,2fr)_minmax(0,2fr)] gap-3 items-center">
        <div className="text-right text-xs text-slate-500 font-mono">{index + 1}</div>
        <CompanyIcon company={displayLabel} size={26} url={logoUrl} />
        <div className="min-w-0">
          <a
            href={item.url}
            target="_blank"
            rel="noreferrer"
            className="text-sm font-semibold text-slate-200 hover:text-white truncate block"
            title={item.url}
            onClick={(event) => event.stopPropagation()}
          >
            {item.url}
          </a>
          <div className="text-[10px] text-slate-500 truncate" title={item.sourceUrl}>
            {item.sourceUrl}
          </div>
        </div>

        <div className="hidden sm:block text-xs text-slate-400 truncate" title={item.provider ?? ""}>
          {item.provider ?? "—"}
        </div>

        <div className="hidden sm:block text-xs text-slate-400 truncate">
          {typeof item.scheduledAt === "number"
            ? new Date(item.scheduledAt).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })
            : "—"}
        </div>

        <div className="hidden sm:block text-right text-xs text-slate-400">
          {item.attempts ?? 0}
        </div>

        <div className="flex justify-end">
          <span className={`px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide rounded border ${statusStyle}`}>
            {item.status}
          </span>
        </div>

        <div className="hidden sm:flex justify-end">
          <LiveTimer
            startTime={item.createdAt}
            colorize={isSelected}
            warnAfterMs={6 * 60 * 60 * 1000}
            dangerAfterMs={24 * 60 * 60 * 1000}
            showAgo
            showSeconds={isSelected}
            className="text-[10px] font-mono text-slate-400 truncate"
          />
        </div>
      </div>
    </motion.div>
  );
}
