"use client";
import { useAuthActions } from "@convex-dev/auth/react";
import { useState } from "react";
import { toast } from "sonner";

export function SignInForm({ mode = "compact" }: { mode?: "compact" | "spacious" }) {
  const { signIn } = useAuthActions();
  const [flow, setFlow] = useState<"signIn" | "signUp">("signIn");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  return (
    <div className="w-full">
      <form
        className={`flex flex-col ${mode === "spacious" ? "gap-6" : "gap-3"}`}
        onSubmit={(e) => {
          e.preventDefault();
          setError(null);
          setSubmitting(true);
          const formData = new FormData(e.target as HTMLFormElement);
          formData.set("flow", flow);

          const email = String(formData.get("email") ?? "").trim();
          const password = String(formData.get("password") ?? "");

          if (!email || !password) {
            setError("Email and password are required.");
            setSubmitting(false);
            return;
          }
          if (password.length < 8) {
            setError("Password must be at least 8 characters long.");
            setSubmitting(false);
            return;
          }

          void signIn("password", formData)
            .then(() => {
              setSubmitting(false);
              if (flow === "signUp") {
                toast.success("Account created. You're signed in!");
              }
            })
            .catch((err) => {
              const message = err?.message ?? "Unable to complete request.";
              setError(message);
              let toastTitle = message;
              if (message.includes("Invalid password")) {
                toastTitle = "Invalid password. Please try again.";
              }
              toast.error(toastTitle);
              setSubmitting(false);
            });
        }}
      >
        {mode === "compact" ? (
          <div className="flex gap-2">
            <input
              className="auth-input-field flex-1 min-w-0"
              type="email"
              name="email"
              placeholder="Email"
              required
            />
            <input
              className="auth-input-field flex-1 min-w-0"
              type="password"
              name="password"
              placeholder="Password"
              required
            />
          </div>
        ) : (
          <div className="flex flex-col gap-4">
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-300 ml-1">Email</label>
              <input
                className="auth-input-field w-full px-4 py-3 text-base"
                type="email"
                name="email"
                placeholder="name@example.com"
                required
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-300 ml-1">Password</label>
              <input
                className="auth-input-field w-full px-4 py-3 text-base"
                type="password"
                name="password"
                placeholder="••••••••"
                required
              />
            </div>
          </div>
        )}

        <button
          className={`auth-button ${mode === "spacious" ? "py-3 text-base font-semibold bg-blue-600 hover:bg-blue-500 text-white border-transparent" : "py-2"}`}
          type="submit"
          disabled={submitting}
        >
          {flow === "signIn" ? "Sign in" : "Sign up"}
        </button>

        {error && <p className="text-xs text-red-400 text-center">{error}</p>}

        <div className={`flex items-center justify-between text-slate-400 ${mode === "spacious" ? "text-sm px-2" : "text-xs px-1"}`}>
          <button
            type="button"
            className="hover:text-slate-200 hover:underline cursor-pointer transition-colors"
            onClick={() => setFlow(flow === "signIn" ? "signUp" : "signIn")}
          >
            {flow === "signIn" ? "Create account" : "Sign in instead"}
          </button>
          {mode === "compact" && (
            <button
              type="button"
              className="hover:text-slate-200 hover:underline cursor-pointer transition-colors"
              onClick={() => void signIn("anonymous")}
            >
              Guest access
            </button>
          )}
        </div>
      </form>
    </div>
  );
}
