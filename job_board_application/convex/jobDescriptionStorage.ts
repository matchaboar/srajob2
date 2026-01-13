import type { Id } from "./_generated/dataModel";

const MAX_CONVEX_STRING_BYTES = 1_000_000;
const DESCRIPTION_TRUNCATION_SUFFIX = "...";
const DESCRIPTION_PREVIEW_MAX_BYTES = 4_000;
const DESCRIPTION_PREVIEW_MAX_WORDS = 100;
const DESCRIPTION_PREVIEW_ERROR_CODE = "description_preview_word_limit_exceeded";

export const clampStringToBytes = (value: string, maxBytes = MAX_CONVEX_STRING_BYTES) => {
  const encoder = new TextEncoder();
  if (encoder.encode(value).length <= maxBytes) return value;

  const rawValue = value.endsWith(DESCRIPTION_TRUNCATION_SUFFIX)
    ? value.slice(0, -DESCRIPTION_TRUNCATION_SUFFIX.length)
    : value;
  const suffixBytes = encoder.encode(DESCRIPTION_TRUNCATION_SUFFIX).length;
  const targetBytes = Math.max(0, maxBytes - suffixBytes);
  let low = 0;
  let high = rawValue.length;
  while (low < high) {
    const mid = Math.floor((low + high + 1) / 2);
    const chunk = rawValue.slice(0, mid);
    const size = encoder.encode(chunk).length;
    if (size <= targetBytes) {
      low = mid;
    } else {
      high = mid - 1;
    }
  }
  return `${rawValue.slice(0, low)}${DESCRIPTION_TRUNCATION_SUFFIX}`;
};

const stripTruncationSuffix = (value: string) =>
  value.endsWith(DESCRIPTION_TRUNCATION_SUFFIX)
    ? value.slice(0, -DESCRIPTION_TRUNCATION_SUFFIX.length)
    : value;

const countWords = (value: string) => {
  const trimmed = stripTruncationSuffix(value).trim();
  if (!trimmed) return 0;
  return trimmed.split(/\s+/).length;
};

const truncateToWordLimit = (value: string, maxWords: number) => {
  const trimmed = value.trim();
  if (!trimmed) return "";
  const words = trimmed.split(/\s+/);
  if (words.length <= maxWords) return trimmed;
  return `${words.slice(0, maxWords).join(" ")}${DESCRIPTION_TRUNCATION_SUFFIX}`;
};

const assertPreviewWithinWordLimit = (value: string) => {
  const wordCount = countWords(value);
  if (wordCount > DESCRIPTION_PREVIEW_MAX_WORDS) {
    throw new Error(`${DESCRIPTION_PREVIEW_ERROR_CODE}:${wordCount}`);
  }
};

export const buildDescriptionPreview = (value: string) => {
  const wordLimited = truncateToWordLimit(value, DESCRIPTION_PREVIEW_MAX_WORDS);
  const clamped = clampStringToBytes(wordLimited, DESCRIPTION_PREVIEW_MAX_BYTES);
  assertPreviewWithinWordLimit(clamped);
  return clamped;
};

export const storeDescriptionInStorage = async (
  ctx: {
    storage: {
      delete: (id: Id<"_storage">) => Promise<void>;
      store?: (blob: Blob) => Promise<Id<"_storage">>;
    };
  },
  description: string,
  existingStorageId?: Id<"_storage"> | null
) => {
  const trimmed = description.trim();
  if (!trimmed) {
    if (existingStorageId) {
      await ctx.storage.delete(existingStorageId);
    }
    return { description: undefined, descriptionStorageId: undefined };
  }

  if (typeof ctx.storage.store === "function") {
    const storageId = await ctx.storage.store(
      new Blob([description], { type: "text/plain; charset=utf-8" })
    );
    if (existingStorageId) {
      await ctx.storage.delete(existingStorageId);
    }
    return {
      description: buildDescriptionPreview(description),
      descriptionStorageId: storageId,
    };
  }

  if (existingStorageId) {
    await ctx.storage.delete(existingStorageId);
  }
  return {
    description: buildDescriptionPreview(description),
    descriptionStorageId: undefined,
  };
};

export const loadDescriptionFromStorage = async (
  ctx: { storage: { get?: (id: Id<"_storage">) => Promise<Blob | null> } },
  storageId?: Id<"_storage"> | null
) => {
  if (!storageId || typeof ctx.storage.get !== "function") return null;
  const blob = await ctx.storage.get(storageId);
  if (!blob) return null;
  try {
    return await blob.text();
  } catch {
    return null;
  }
};

export const deleteDescriptionFromStorage = async (
  ctx: { storage: { delete: (id: Id<"_storage">) => Promise<void> } },
  storageId?: Id<"_storage"> | null
) => {
  if (!storageId) return;
  await ctx.storage.delete(storageId);
};
