import { Authenticated, Unauthenticated, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";
import { SignInForm } from "./SignInForm";
import { SignOutButton } from "./SignOutButton";
import { Toaster, toast } from "sonner";
import { JobBoard } from "./JobBoard";
import { PublicJobPreview } from "./PublicJobPreview";
import { AdminPage } from "./AdminPage";
import { StatusTrackerTest } from "./test/StatusTrackerTest";
import { useState, useEffect } from "react";

export default function App() {
  const isAdmin = useQuery(api.auth.isAdmin);

  // Check for test page route
  const [showTestPage, setShowTestPage] = useState(() => {
    return window.location.hash === "#test-status-tracker";
  });

  // Use URL hash to persist showAdmin intent across refreshes
  const [showAdmin, setShowAdmin] = useState(() => {
    return window.location.hash.startsWith("#admin");
  });

  // Update URL hash when showAdmin changes
  useEffect(() => {
    const currentHash = window.location.hash;
    const shouldBeAdmin = showAdmin;
    const isAdmin = currentHash.startsWith("#admin");

    // Only update if there's a mismatch
    if (shouldBeAdmin && !isAdmin) {
      window.location.hash = "#admin-scraper";
    } else if (!shouldBeAdmin && isAdmin) {
      window.location.hash = "";
    }
  }, [showAdmin]);

  // Listen for hash changes (back/forward navigation)
  useEffect(() => {
    const handleHashChange = () => {
      const hash = window.location.hash;
      setShowTestPage(hash === "#test-status-tracker");
      setShowAdmin(hash.startsWith("#admin"));
    };
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  // If admin hash is present but user is not an admin, bounce them back gracefully
  useEffect(() => {
    if (showAdmin && isAdmin === false) {
      setShowAdmin(false);
      window.location.hash = "";
      toast.error("Admin access requires an admin account.");
    }
  }, [showAdmin, isAdmin]);

  const handleAdminToggle = () => {
    if (isAdmin === undefined) return; // still loading auth state
    if (!isAdmin) {
      toast.error("Admin access requires an admin account.");
      return;
    }
    setShowAdmin((prev) => !prev);
  };

  const showAdminPage = showAdmin && isAdmin === true;
  const adminLoading = showAdmin && isAdmin === undefined;
  const adminBlocked = showAdmin && isAdmin === false;

  return (
    <div className="min-h-screen flex flex-col bg-slate-950 text-slate-200">
      <header className="sticky top-0 z-10 bg-slate-950 border-b border-slate-800 h-16 flex justify-between items-center px-6">
        <div className="flex items-center gap-4">
          <h2 className="text-xl font-bold text-white tracking-tight">JobBoard</h2>
        </div>
        <div className="flex items-center gap-4">
          <button
            onClick={handleAdminToggle}
            className="text-sm text-slate-400 hover:text-white transition-colors"
            disabled={isAdmin === undefined}
          >
            {showAdmin ? "Back to Jobs" : "Admin"}
          </button>
          <Authenticated>
            <SignOutButton />
          </Authenticated>
        </div>
      </header>
      <main className="flex-1 flex flex-col overflow-hidden">
        {showTestPage ? (
          <StatusTrackerTest />
        ) : adminLoading ? (
          <AdminLoading />
        ) : adminBlocked ? (
          <AdminDenied />
        ) : showAdminPage ? (
          <AdminPage />
        ) : (
          <Content />
        )}
      </main>
      <Toaster theme="dark" />
    </div>
  );
}

function Content() {
  const loggedInUser = useQuery(api.auth.loggedInUser);

  if (loggedInUser === undefined) {
    return (
      <div className="flex justify-center items-center min-h-[400px]">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
      </div>
    );
  }

  return (
    <div className="flex flex-col">
      <Authenticated>
        <JobBoard />
      </Authenticated>
      <Unauthenticated>
        <PublicJobPreview />
      </Unauthenticated>
    </div>
  );
}

function AdminLoading() {
  return (
    <div className="flex flex-1 items-center justify-center text-slate-400">
      Checking admin access...
    </div>
  );
}

function AdminDenied() {
  return (
    <div className="flex flex-1 items-center justify-center">
      <div className="max-w-md w-full bg-slate-900 border border-slate-800 rounded p-6 text-center">
        <h3 className="text-lg font-semibold text-white mb-2">Admins only</h3>
        <p className="text-sm text-slate-400 mb-4">
          You must sign in with an admin account to view the admin panel.
        </p>
        <SignInForm />
      </div>
    </div>
  );
}
