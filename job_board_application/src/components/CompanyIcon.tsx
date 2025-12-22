import { useEffect, useMemo, useState } from "react";
import type { SimpleIcon } from "simple-icons";

const slugCharMap: Record<string, string> = {
    "+": "plus",
    ".": "dot",
    "&": "and",
    "đ": "d",
    "ħ": "h",
    "ı": "i",
    "ĸ": "k",
    "ŀ": "l",
    "ł": "l",
    "ß": "ss",
    "ŧ": "t",
    "ø": "o",
};

type IconModule = typeof import("simple-icons");

let iconsModulePromise: Promise<IconModule> | null = null;
const iconCache = new Map<string, SimpleIcon | null>();

type CustomLogoDefinition = {
    src: string;
};

const logoUrls = import.meta.glob("../assets/company-logos/*.{svg,png}", { eager: true, as: "url" }) as Record<string, string>;
const customLogos: Record<string, CustomLogoDefinition> = Object.fromEntries(
    Object.entries(logoUrls).map(([path, src]) => {
        const filename = path.split("/").pop() ?? "";
        const slug = filename.replace(/\.(svg|png)$/i, "").toLowerCase();
        return [slug, { src }];
    }),
);

const getIconsModule = () => {
    if (!iconsModulePromise) {
        iconsModulePromise = import("simple-icons") as Promise<IconModule>;
    }
    return iconsModulePromise;
};

const normalizeHex = (hex: string) => {
    let cleaned = hex.replace("#", "").toUpperCase();
    if (cleaned.length === 3) {
        cleaned = cleaned.split("").map((c) => c + c).join("");
    }
    if (cleaned.length !== 6) {
        return null;
    }
    return `#${cleaned}`;
};

const ensureReadableColor = (hex: string) => {
    const normalized = normalizeHex(hex) ?? "#E2E8F0";
    const r = parseInt(normalized.slice(1, 3), 16);
    const g = parseInt(normalized.slice(3, 5), 16);
    const b = parseInt(normalized.slice(5, 7), 16);
    if (Number.isNaN(r) || Number.isNaN(g) || Number.isNaN(b)) {
        return "#E2E8F0";
    }
    const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
    if (luminance < 0.38) {
        const mix = (value: number) => Math.round(value + (255 - value) * 0.45);
        const toHex = (value: number) => value.toString(16).padStart(2, "0");
        return `#${toHex(mix(r))}${toHex(mix(g))}${toHex(mix(b))}`;
    }
    return normalized;
};

const toSlug = (company: string) => {
    const lowered = company.toLowerCase();
    const replaced = lowered.replace(/[+.&đħıĸŀłßŧø]/g, (char) => slugCharMap[char] ?? "");
    const normalized = replaced.normalize("NFD").replace(/[^a-z0-9]/g, "");
    return normalized || null;
};

const slugToExportName = (slug: string) => `si${slug[0].toUpperCase()}${slug.slice(1)}`;

const loadIcon = async (slug: string): Promise<SimpleIcon | null> => {
    if (iconCache.has(slug)) {
        return iconCache.get(slug) ?? null;
    }
    const module = await getIconsModule();
    const exportName = slugToExportName(slug);
    const icon = (module as Record<string, SimpleIcon | undefined>)[exportName] ?? null;
    iconCache.set(slug, icon);
    return icon;
};

const buildFallbackInitial = (company: string) => {
    const trimmed = company.trim();
    const first = trimmed.match(/[A-Za-z0-9]/)?.[0];
    return first ? first.toUpperCase() : "?";
};

interface CompanyIconProps {
    company: string;
    size?: number;
    url?: string;
}

const BRAND_FETCH_CLIENT = "1idXaGHc5cKcElppzC7";
const BRANDFETCH_LOGO_OVERRIDES: Record<string, string> = {
    mithril: "https://cdn.brandfetch.io/idZPhPbkaC/w/432/h/432/theme/dark/logo.png?c=1bxid64Mup7aczewSAYMX&t=1759798646882",
    together: "https://cdn.brandfetch.io/idgEzjThpb/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1764613007905",
    togetherai: "https://cdn.brandfetch.io/idgEzjThpb/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1764613007905",
    togetherdotai: "https://cdn.brandfetch.io/idgEzjThpb/w/400/h/400/theme/dark/icon.jpeg?c=1bxid64Mup7aczewSAYMX&t=1764613007905",
};
const BRANDFETCH_DOMAIN_OVERRIDES: Record<string, string> = {
    oscar: "hioscar.com",
    serval: "serval.com",
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
const HOSTED_COMPANY_SLUGS = new Set([
    "avature",
    "greenhouse",
    "ashby",
    "lever",
    "workable",
    "smartrecruiters",
    "workday",
    "icims",
    "jobvite",
    "bamboohr",
]);

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

const extractCompanySlug = (pathname: string) => {
    const parts = pathname.split("/").filter(Boolean);
    for (const part of parts) {
        const cleaned = part.toLowerCase();
        if (cleaned === "jobdetail" || cleaned === "job-details" || cleaned === "jobdetails") {
            break;
        }
        if (RESERVED_PATH_SEGMENTS.has(cleaned)) continue;
        if (/^\d+$/.test(cleaned)) continue;
        if (!/^[a-z0-9-]+$/.test(cleaned)) continue;
        return cleaned;
    }
    return null;
};

const resolveHostedJobsDomain = (host: string) =>
    HOSTED_JOB_DOMAINS.find((domain) => host === domain || host.endsWith(`.${domain}`)) ?? null;

const extractCompanySlugFromHost = (host: string, hostedDomain: string) => {
    const hostParts = host.split(".").filter(Boolean);
    const domainParts = hostedDomain.split(".").filter(Boolean);
    if (hostParts.length <= domainParts.length) return null;
    const subdomains = hostParts.slice(0, hostParts.length - domainParts.length);
    for (let i = subdomains.length - 1; i >= 0; i -= 1) {
        const candidate = subdomains[i]?.toLowerCase() ?? "";
        if (!candidate) continue;
        if (COMMON_SUBDOMAIN_PREFIXES.has(candidate)) continue;
        if (!/^[a-z0-9-]+$/.test(candidate)) continue;
        return candidate;
    }
    return null;
};

const domainMatchesSlug = (domain: string, slug: string) => {
    const normalizedDomain = domain.toLowerCase();
    const normalizedSlug = slug.toLowerCase();
    return normalizedDomain === normalizedSlug || normalizedDomain.startsWith(`${normalizedSlug}.`);
};

const deriveBrandfetchDomain = (company: string, url?: string) => {
    const trimmedCompany = (company || "").trim();
    const companySlug = toSlug(trimmedCompany);
    const domainOverride = companySlug ? BRANDFETCH_DOMAIN_OVERRIDES[companySlug] ?? null : null;
    if (domainOverride) {
        return domainOverride;
    }
    const fallbackCompanyDomain = () => {
        if (trimmedCompany.includes(".")) {
            return trimmedCompany.toLowerCase();
        }
        return companySlug ? `${companySlug}.com` : null;
    };
    if (url) {
        try {
            const parsed = new URL(url.includes("://") ? url : `https://${url}`);
            const host = parsed.hostname.toLowerCase();
            const hostedDomain = resolveHostedJobsDomain(host);
            if (hostedDomain) {
                const slug = extractCompanySlug(parsed.pathname);
                if (slug) {
                    return `${slug}.com`;
                }
                const hostSlug = extractCompanySlugFromHost(host, hostedDomain);
                if (hostSlug) {
                    return `${hostSlug}.com`;
                }
                const companyFallback = fallbackCompanyDomain();
                if (companyFallback) {
                    return companyFallback;
                }
            }
            return baseDomainFromHost(host);
        } catch {
            // fall through to company fallback
        }
    }
    return fallbackCompanyDomain();
};

export function CompanyIcon({ company, size = 34, url }: CompanyIconProps) {
    const slug = useMemo(() => toSlug(company), [company]);
    const customLogo = useMemo(() => (slug ? customLogos[slug] ?? null : null), [slug]);
    const brandfetchOverride = useMemo(() => (slug ? BRANDFETCH_LOGO_OVERRIDES[slug] ?? null : null), [slug]);
    const brandfetchDomain = useMemo(() => deriveBrandfetchDomain(company, url), [company, url]);
    const brandfetchUrl = brandfetchDomain ? `https://cdn.brandfetch.io/${brandfetchDomain}?c=${BRAND_FETCH_CLIENT}` : null;
    const effectiveBrandfetchUrl = brandfetchOverride ?? brandfetchUrl;
    const preferBrandfetch = useMemo(() => {
        if (!slug || !brandfetchDomain) return false;
        if (!HOSTED_COMPANY_SLUGS.has(slug)) return false;
        return !domainMatchesSlug(brandfetchDomain, slug);
    }, [brandfetchDomain, slug]);
    const [iconState, setIconState] = useState<{ icon: SimpleIcon | null; loaded: boolean }>({
        icon: null,
        loaded: false,
    });
    const [brandfetchFailed, setBrandfetchFailed] = useState(false);

    useEffect(() => {
        let cancelled = false;
        if (!slug || customLogo || preferBrandfetch) {
            setIconState({ icon: null, loaded: true });
            return () => { cancelled = true; };
        }
        setIconState({ icon: null, loaded: false });
        loadIcon(slug).then((result) => {
            if (!cancelled) {
                setIconState({ icon: result, loaded: true });
            }
        }).catch(() => {
            if (!cancelled) {
                setIconState({ icon: null, loaded: true });
            }
        });
        return () => {
            cancelled = true;
        };
    }, [slug, customLogo]);

    const dimension = `${size}px`;
    const color = ensureReadableColor(iconState.icon?.hex ?? "#E2E8F0");
    const showBrandfetch = !customLogo
        && (brandfetchOverride || preferBrandfetch || (iconState.loaded && !iconState.icon))
        && !!effectiveBrandfetchUrl
        && !brandfetchFailed;

    useEffect(() => {
        setBrandfetchFailed(false);
    }, [effectiveBrandfetchUrl, company]);

    return (
        <div
            className="flex-shrink-0 overflow-hidden rounded-full border border-slate-700/70 bg-slate-900/70 flex items-center justify-center shadow-sm shadow-slate-900/40"
            style={{ width: dimension, height: dimension }}
            aria-label={company ? `${company} logo` : "Company logo"}
        >
            {customLogo ? (
                <img
                    src={customLogo.src}
                    alt={`${company} logo`}
                    className="w-6 h-6 object-contain"
                    draggable={false}
                />
            ) : iconState.icon ? (
                <svg
                    viewBox="0 0 24 24"
                    role="img"
                    aria-hidden="true"
                    className="w-6 h-6"
                    style={{ color }}
                    focusable="false"
                >
                    <path d={iconState.icon.path} fill="currentColor" />
                </svg>
            ) : showBrandfetch ? (
                <img
                    src={effectiveBrandfetchUrl ?? undefined}
                    alt={`${company} logo`}
                    className="w-6 h-6 object-contain"
                    draggable={false}
                    onError={() => setBrandfetchFailed(true)}
                />
            ) : (
                <span className="text-sm font-semibold text-slate-200">{buildFallbackInitial(company)}</span>
            )}
        </div>
    );
}
