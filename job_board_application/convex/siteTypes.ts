export const SITE_TYPES = [
  "general",
  "greenhouse",
  "avature",
  "workday",
  "netflix",
  "uber",
  "cisco",
  "adobe",
  "docusign",
  "notion",
  "paloalto",
] as const;

export type SiteType = (typeof SITE_TYPES)[number];

export const SPIDER_CLOUD_DEFAULT_SITE_TYPES = new Set<SiteType>([
  "greenhouse",
  "avature",
  "workday",
  "netflix",
  "uber",
  "cisco",
  "adobe",
  "docusign",
  "notion",
  "paloalto",
]);
