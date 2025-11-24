import { convexAuth, getAuthUserId } from "@convex-dev/auth/server";
import { Password } from "@convex-dev/auth/providers/Password";
import { Anonymous } from "@convex-dev/auth/providers/Anonymous";
import { query } from "./_generated/server";

export const { auth, signIn, signOut, store, isAuthenticated } = convexAuth({
  providers: [
    Password({
      profile: (params) => {
        const email = String(params.email ?? "").trim().toLowerCase();
        if (!email) {
          throw new Error("Email is required");
        }
        const password = String(params.password ?? "");
        if (password.length < 8) {
          throw new Error("Password must be at least 8 characters long");
        }

        return { email } as any;
      },
    }),
    Anonymous,
  ],
});

const adminEmails = (process.env.ADMIN_EMAILS ?? "")
  .split(",")
  .map((e) => e.trim().toLowerCase())
  .filter(Boolean);

export const loggedInUser = query({
  handler: async (ctx) => {
    const userId = await getAuthUserId(ctx);
    if (!userId) {
      return null;
    }
    const user = await ctx.db.get(userId);
    if (!user) {
      return null;
    }
    return user;
  },
});

export const isAdmin = query({
  handler: async (ctx) => {
    if (adminEmails.length === 0) return false;

    const userId = await getAuthUserId(ctx);
    if (!userId) return false;

    const user = await ctx.db.get(userId);
    if (!user) return false;

    const email = (user as any).email ?? (user as any).name ?? "";
    return typeof email === "string" && adminEmails.includes(email.toLowerCase());
  },
});
