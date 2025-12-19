import { useQuery, useMutation } from "convex/react";
import { api } from "../convex/_generated/api";
import { SignInForm } from "./SignInForm";
import { useState, useEffect } from "react";
import { useAuthActions } from "@convex-dev/auth/react";
import { buildCompensationMeta } from "./lib/compensation";

export function FakePublicJobPreview() {
  const { signIn } = useAuthActions();
  const [isLoginExpanded, setIsLoginExpanded] = useState(false);
  const recentJobs = useQuery(api.jobs.getRecentJobs);
  const jobsExist = useQuery(api.jobs.checkIfJobsExist);
  const insertFakeJobs = useMutation(api.seedData.insertFakeJobs);

  // Auto-insert fake jobs if none exist
  useEffect(() => {
    if (jobsExist === false) {
      insertFakeJobs({}).catch(console.error);
    }
  }, [jobsExist, insertFakeJobs]);

  // Show only first 5 jobs for preview
  const previewJobs = recentJobs?.slice(0, 5) || [];

  return (
    <div className="w-full pb-12">
      {/* Hero Section */}
      <div className="max-w-6xl mx-auto p-6">
        <div className="text-center mb-12 mt-8">
          <h1 className="text-4xl font-bold text-white mb-4 tracking-tight">
            Find Your Next Opportunity
          </h1>
          <p className="text-lg text-slate-400 max-w-2xl mx-auto">
            Discover thousands of job opportunities from top companies.
          </p>
        </div>
      </div>

      {/* Full Width Accordion Login */}
      <div className="w-full bg-slate-800 mb-12">
        <div className="w-full flex items-center justify-end px-6 py-4 gap-6 max-w-6xl mx-auto">
          {/* Login Toggle */}
          <button
            onClick={() => setIsLoginExpanded(!isLoginExpanded)}
            className="flex items-center gap-2 text-slate-300 hover:text-white transition-colors group"
          >
            {/* Animated Arrow pointing to Login */}
            {!isLoginExpanded && (
              <svg
                className="w-4 h-4 text-blue-400 animate-bounce-x-wide mr-4"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
              </svg>
            )}
            <span className="text-sm font-semibold animate-text-glow">Login</span>
            <svg
              className={`w-4 h-4 transition-transform duration-200 ${isLoginExpanded ? "rotate-90" : ""}`}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </button>

          {/* Guest Login Button */}
          <button
            onClick={() => void signIn("anonymous")}
            className="flex items-center gap-2 px-3 py-1.5 bg-slate-700 hover:bg-slate-600 text-white rounded-md transition-colors shadow-sm font-medium text-sm border border-slate-600"
          >
            <span>Guest Login</span>
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 5l7 7m0 0l-7 7m7-7H3" />
            </svg>
          </button>
        </div>

        <div
          className={`overflow-hidden transition-all duration-300 ease-in-out bg-slate-800 ${isLoginExpanded ? "max-h-[500px] opacity-100" : "max-h-0 opacity-0"
            }`}
        >
          <div className="max-w-6xl mx-auto px-6 pb-12 pt-4">
            <div className="max-w-md mx-auto">
              <SignInForm mode="spacious" />
            </div>
          </div>
        </div>
      </div>

      {/* Job Preview Section */}
      <div className="max-w-6xl mx-auto p-6">
        <div className="mb-12">
          <h2 className="text-xl font-semibold text-white mb-6 flex items-center gap-2">
            <span className="w-1 h-6 bg-blue-500 rounded-full"></span>
            Latest Job Opportunities
          </h2>

          {previewJobs.length > 0 ? (
            <div className="space-y-4">
              {previewJobs.map((job, index) => {
                const meta = buildCompensationMeta(job);
                return (
                  <div
                    key={job._id}
                    className={`bg-slate-900/50 p-4 rounded-lg border border-slate-800 transition-all hover:border-slate-700 hover:bg-slate-900 ${index >= 3 ? "opacity-60" : ""
                      }`}
                  >
                    <div className="flex justify-between items-start gap-4">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-3 mb-1.5 flex-wrap">
                          <h3 className="text-lg font-semibold text-slate-100 truncate">{job.title}</h3>
                          <div className="flex gap-2">
                            <span className="px-2 py-0.5 bg-blue-500/10 border border-blue-500/20 text-blue-400 text-xs font-medium rounded-md">
                              {job.level}
                            </span>
                            {job.remote && (
                              <span className="px-2 py-0.5 bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-xs font-medium rounded-md">
                                Remote
                              </span>
                            )}
                          </div>
                        </div>
                        <p className="text-sm font-medium text-slate-300 mb-1">{job.company}</p>
                        <p className="text-xs text-slate-500 mb-2">{job.location}</p>
                        <p className="text-sm text-slate-500 mb-3 line-clamp-2 leading-relaxed italic">
                          Sign in to read the full description.
                        </p>
                        <div className="flex items-center gap-4 text-xs text-slate-500 border-t border-slate-800/50 pt-3">
                          <span
                            className={`font-mono ${meta.isUnknown ? "text-amber-300 border-amber-500/20 bg-amber-500/5" : "text-emerald-400 border-emerald-500/10 bg-emerald-500/5"} px-1.5 py-0.5 rounded border`}
                            title={meta.reason}
                          >
                            {meta.display}
                          </span>
                          <span>Posted {new Date(job.postedAt).toLocaleDateString()}</span>
                        </div>
                      </div>

                      <div className="flex flex-col gap-2 shrink-0">
                        <button
                          disabled
                          className="px-4 py-2 bg-slate-800 text-slate-500 rounded-md text-xs font-medium cursor-not-allowed border border-slate-700"
                        >
                          Sign in to Apply
                        </button>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="bg-slate-900/50 p-12 rounded-lg border border-slate-800 text-center">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mx-auto mb-4"></div>
              <p className="text-slate-500">Loading job opportunities...</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
