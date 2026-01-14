import { useEffect, useMemo, useState } from "react";
import type { SimpleIcon } from "simple-icons";
import { ensureReadableColor } from "../lib/companyIcon/colorUtils";
import { toSlug, buildFallbackInitial } from "../lib/companyIcon/slugUtils";
import { loadIcon } from "../lib/companyIcon/iconLoader";
import {
  BRAND_FETCH_CLIENT,
  BRANDFETCH_LOGO_OVERRIDES,
  HOSTED_COMPANY_SLUGS,
  deriveBrandfetchDomain,
  domainMatchesSlug,
} from "../lib/companyIcon/brandfetch";

type CustomLogoDefinition = {
  src: string;
};

const logoUrls = import.meta.glob("../assets/company-logos/*.{svg,png}", {
  eager: true,
  query: "?url",
  import: "default",
}) as Record<string, string>;

const customLogos: Record<string, CustomLogoDefinition> = Object.fromEntries(
  Object.entries(logoUrls).map(([path, src]) => {
    const filename = path.split("/").pop() ?? "";
    const slug = filename.replace(/\.(svg|png)$/i, "").toLowerCase();
    return [slug, { src }];
  })
);

interface CompanyIconProps {
  company: string;
  size?: number;
  url?: string;
}

export function CompanyIcon({ company, size = 34, url }: CompanyIconProps) {
  const slug = useMemo(() => toSlug(company), [company]);
  const customLogo = useMemo(
    () => (slug ? customLogos[slug] ?? null : null),
    [slug]
  );
  const brandfetchOverride = useMemo(
    () => (slug ? BRANDFETCH_LOGO_OVERRIDES[slug] ?? null : null),
    [slug]
  );
  const brandfetchDomain = useMemo(
    () => deriveBrandfetchDomain(company, url),
    [company, url]
  );
  const brandfetchUrl = brandfetchDomain
    ? `https://cdn.brandfetch.io/${brandfetchDomain}?c=${BRAND_FETCH_CLIENT}`
    : null;
  const effectiveBrandfetchUrl = brandfetchOverride ?? brandfetchUrl;
  const preferBrandfetch = useMemo(() => {
    if (!slug || !brandfetchDomain) return false;
    if (!HOSTED_COMPANY_SLUGS.has(slug)) return false;
    return !domainMatchesSlug(brandfetchDomain, slug);
  }, [brandfetchDomain, slug]);
  const [iconState, setIconState] = useState<{
    icon: SimpleIcon | null;
    loaded: boolean;
  }>({
    icon: null,
    loaded: false,
  });
  const [brandfetchFailed, setBrandfetchFailed] = useState(false);

  useEffect(() => {
    let cancelled = false;
    if (!slug || customLogo || preferBrandfetch) {
      setIconState({ icon: null, loaded: true });
      return () => {
        cancelled = true;
      };
    }
    setIconState({ icon: null, loaded: false });
    loadIcon(slug)
      .then((result) => {
        if (!cancelled) {
          setIconState({ icon: result, loaded: true });
        }
      })
      .catch(() => {
        if (!cancelled) {
          setIconState({ icon: null, loaded: true });
        }
      });
    return () => {
      cancelled = true;
    };
  }, [slug, customLogo, preferBrandfetch]);

  const dimension = `${size}px`;
  const color = ensureReadableColor(iconState.icon?.hex ?? "#E2E8F0");
  const showBrandfetch =
    !customLogo &&
    (brandfetchOverride ||
      preferBrandfetch ||
      (iconState.loaded && !iconState.icon)) &&
    !!effectiveBrandfetchUrl &&
    !brandfetchFailed;

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
        <span className="text-sm font-semibold text-slate-200">
          {buildFallbackInitial(company)}
        </span>
      )}
    </div>
  );
}
