/**
 * Icon loading utilities for simple-icons.
 */
import type { SimpleIcon } from "simple-icons";
import { slugToExportName } from "./slugUtils";

type IconModule = typeof import("simple-icons");

let iconsModulePromise: Promise<IconModule> | null = null;
const iconCache = new Map<string, SimpleIcon | null>();

const getIconsModule = (): Promise<IconModule> => {
  if (!iconsModulePromise) {
    iconsModulePromise = import("simple-icons") as Promise<IconModule>;
  }
  return iconsModulePromise;
};

/**
 * Load an icon by slug from simple-icons library.
 */
export const loadIcon = async (slug: string): Promise<SimpleIcon | null> => {
  if (iconCache.has(slug)) {
    return iconCache.get(slug) ?? null;
  }
  const module = await getIconsModule();
  const exportName = slugToExportName(slug);
  const icon =
    (module as Record<string, SimpleIcon | undefined>)[exportName] ?? null;
  iconCache.set(slug, icon);
  return icon;
};
