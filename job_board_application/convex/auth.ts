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

// In local/dev environments we skip admin gating to simplify testing the admin UI.
// Convex hosted deployments always set NODE_ENV="production", even for dev deployments,
// so rely on other signals that we control (local SITE_URL or an explicit deployment flag).
const isDevEnv =
  process.env.NODE_ENV !== "production" ||
  Boolean(
    process.env.SITE_URL &&
      (process.env.SITE_URL.includes("127.0.0.1") || process.env.SITE_URL.includes("localhost")),
  ) ||
  (process.env.CONVEX_DEPLOYMENT?.startsWith("dev:") ?? false);

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
    const userId = await getAuthUserId(ctx);
    // In local/dev we only require that the user is signed in (password or anonymous).
    // Admin email gating remains for hosted environments.
    if (isDevEnv) return Boolean(userId);
    if (adminEmails.length === 0) return false;

    if (!userId) return false;

    const user = await ctx.db.get(userId);
    if (!user) return false;

    const email = (user as any).email ?? (user as any).name ?? "";
    return typeof email === "string" && adminEmails.includes(email.toLowerCase());
  },
});
