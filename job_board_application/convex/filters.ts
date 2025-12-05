import { mutation, query } from "./_generated/server";
import { v } from "convex/values";
import { getAuthUserId } from "@convex-dev/auth/server";

const levelValidator = v.union(
  v.literal("junior"),
  v.literal("mid"),
  v.literal("senior"),
  v.literal("staff")
);

async function requireUserId(ctx: any) {
  const userId = await getAuthUserId(ctx);
  if (!userId) {
    throw new Error("Not authenticated");
  }
  return userId;
}

async function clearSelected(ctx: any, userId: string) {
  const selected = await ctx.db
    .query("saved_filters")
    .withIndex("by_user_selected", (q: any) => q.eq("userId", userId).eq("isSelected", true))
    .collect();

  for (const row of selected) {
    await ctx.db.patch(row._id, { isSelected: false });
  }
}

export const getSavedFilters = query({
  args: {},
  handler: async (ctx) => {
    const userId = await requireUserId(ctx);
    const filters = await ctx.db
      .query("saved_filters")
      .withIndex("by_user", (q) => q.eq("userId", userId))
      .collect();

    return filters.sort((a: any, b: any) => b.createdAt - a.createdAt);
  },
});

export const ensureDefaultFilter = mutation({
  args: {},
  handler: async (ctx) => {
    const userId = await requireUserId(ctx);
    const existing = await ctx.db
      .query("saved_filters")
      .withIndex("by_user", (q) => q.eq("userId", userId))
      .collect();

    if (existing.length > 0) {
      const alreadySelected = existing.find((f: any) => f.isSelected);
      if (alreadySelected) {
        return alreadySelected._id;
      }
      // If user has filters but none are selected, select the newest one.
      const newest = [...existing].sort((a, b) => b.createdAt - a.createdAt)[0];
      await ctx.db.patch(newest._id, { isSelected: true });
      return newest._id;
    }

    const id = await ctx.db.insert("saved_filters", {
      userId,
      name: "Software Engineer $150k+",
      search: "software engineer",
      useSearch: false,
      minCompensation: 150000,
      includeRemote: true,
      state: undefined,
      country: "United States",
      hideUnknownCompensation: false,
      isSelected: true,
      createdAt: Date.now(),
    });
    return id;
  },
});

export const saveFilter = mutation({
  args: {
    name: v.string(),
    search: v.optional(v.string()),
    remote: v.optional(v.boolean()),
    useSearch: v.optional(v.boolean()),
    includeRemote: v.optional(v.boolean()),
    state: v.optional(v.string()),
    country: v.optional(v.string()),
    level: v.optional(levelValidator),
    minCompensation: v.optional(v.number()),
    maxCompensation: v.optional(v.number()),
    hideUnknownCompensation: v.optional(v.boolean()),
    companies: v.optional(v.array(v.string())),
  },
  handler: async (ctx, args) => {
    const userId = await requireUserId(ctx);
    await clearSelected(ctx, userId);

    const normalizedCompanies = (args.companies ?? []).map((c) => c.trim()).filter(Boolean);
    const uniqueCompanies = Array.from(new Set(normalizedCompanies));

    const id = await ctx.db.insert("saved_filters", {
      ...args,
      useSearch: args.useSearch ?? false,
      includeRemote: args.includeRemote ?? true,
      country: args.country ?? "United States",
      hideUnknownCompensation: args.hideUnknownCompensation ?? false,
      companies: uniqueCompanies.length > 0 ? uniqueCompanies : undefined,
      userId,
      isSelected: true,
      createdAt: Date.now(),
    });
    return id;
  },
});

export const selectSavedFilter = mutation({
  args: {
    filterId: v.optional(v.id("saved_filters")),
  },
  handler: async (ctx, args) => {
    const userId = await requireUserId(ctx);
    await clearSelected(ctx, userId);

    if (!args.filterId) {
      return null;
    }

    const filter = await ctx.db.get(args.filterId);
    if (!filter || filter.userId !== userId) {
      throw new Error("Filter not found");
    }

    await ctx.db.patch(args.filterId, { isSelected: true });
    return args.filterId;
  },
});

export const deleteSavedFilter = mutation({
  args: {
    filterId: v.id("saved_filters"),
  },
  handler: async (ctx, args) => {
    const userId = await requireUserId(ctx);
    const filter = await ctx.db.get(args.filterId);

    if (!filter || filter.userId !== userId) {
      throw new Error("Filter not found");
    }

    await ctx.db.delete(args.filterId);
  },
});
