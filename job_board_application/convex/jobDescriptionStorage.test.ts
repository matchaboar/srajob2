import { describe, expect, it, vi } from "vitest";
import {
  buildDescriptionPreview,
  deleteDescriptionFromStorage,
  loadDescriptionFromStorage,
  storeDescriptionInStorage,
} from "./jobDescriptionStorage";
import type { Id } from "./_generated/dataModel";

describe("jobDescriptionStorage", () => {
  it("stores the description in storage when available", async () => {
    const storageId = "storage-id" as Id<"_storage">;
    const store = vi.fn<(blob: Blob) => Promise<Id<"_storage">>>().mockResolvedValue(storageId);
    const del = vi.fn(async () => undefined);
    const ctx = { storage: { store, delete: del } };
    const description = "Hello there";

    const result = await storeDescriptionInStorage(ctx, description, "old-id" as any);

    expect(store).toHaveBeenCalledTimes(1);
    expect(del).toHaveBeenCalledWith("old-id");
    expect(result).toEqual({
      description: buildDescriptionPreview(description),
      descriptionStorageId: storageId,
    });
  });

  it("returns preview-only payload when storage.store is unavailable", async () => {
    const del = vi.fn(async () => undefined);
    const ctx = { storage: { delete: del } };
    const description = "Preview only";

    const result = await storeDescriptionInStorage(ctx, description, "old-id" as any);

    expect(del).toHaveBeenCalledWith("old-id");
    expect(result).toEqual({
      description: buildDescriptionPreview(description),
      descriptionStorageId: undefined,
    });
  });

  it("clears storage when description is empty", async () => {
    const store = vi.fn<(blob: Blob) => Promise<Id<"_storage">>>().mockResolvedValue("storage-id" as Id<"_storage">);
    const del = vi.fn(async () => undefined);
    const ctx = { storage: { store, delete: del } };

    const result = await storeDescriptionInStorage(ctx, "   ", "old-id" as any);

    expect(store).not.toHaveBeenCalled();
    expect(del).toHaveBeenCalledWith("old-id");
    expect(result).toEqual({
      description: undefined,
      descriptionStorageId: undefined,
    });
  });

  it("loads description text from storage when available", async () => {
    const get = vi.fn(async () => ({ text: async () => "Stored text" } as Blob));
    const ctx = { storage: { get } };

    const result = await loadDescriptionFromStorage(ctx, "storage-id" as any);

    expect(get).toHaveBeenCalledWith("storage-id");
    expect(result).toBe("Stored text");
  });

  it("returns null when storage.get is unavailable", async () => {
    const result = await loadDescriptionFromStorage({ storage: {} }, "storage-id" as any);

    expect(result).toBeNull();
  });

  it("skips delete when no storage id is provided", async () => {
    const del = vi.fn(async () => undefined);
    const ctx = { storage: { delete: del } };

    await deleteDescriptionFromStorage(ctx, null);

    expect(del).not.toHaveBeenCalled();
  });
});
