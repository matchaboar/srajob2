import { useQuery, useMutation } from "convex/react";
import { api } from "../convex/_generated/api";
import { SignInForm } from "./SignInForm";
import { useEffect } from "react";

export function PublicJobPreview() {
  const recentJobs = useQuery(api.jobs.getRecentJobs);
  const jobsExist = useQuery(api.jobs.checkIfJobsExist);
  const insertFakeJobs = useMutation(api.seedData.insertFakeJobs);

  // Auto-insert fake jobs if none exist
  useEffect(() => {
    if (jobsExist === false) {
      insertFakeJobs({}).catch(console.error);
    }
  }, [jobsExist, insertFakeJobs]);

  const formatSalary = (amount: number) => {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(amount);
  };

  // Show only first 5 jobs for preview
  const previewJobs = recentJobs?.slice(0, 5) || [];

  return (
    <div className="max-w-6xl mx-auto p-6">
      {/* Hero Section with Login */}
      <div className="flex flex-col items-center justify-center mb-12 mt-8 gap-8">
        <div className="text-center max-w-2xl">
          <h1 className="text-4xl font-bold text-white mb-4 tracking-tight">
            Find Your Next Opportunity
          </h1>
          <p className="text-lg text-slate-400">
            Discover thousands of job opportunities from top companies.
          </p>
        </div>

        {/* Compact Login Form */}
        <div className="w-full max-w-xs bg-slate-950 border border-slate-800 rounded-lg p-5 shadow-xl">
          <SignInForm />
        </div>
      </div>

      {/* Job Preview Section */}
      <div className="mb-12">
        <h2 className="text-xl font-semibold text-white mb-6 flex items-center gap-2">
          <span className="w-1 h-6 bg-blue-500 rounded-full"></span>
          Latest Job Opportunities
        </h2>

        {previewJobs.length > 0 ? (
          <div className="space-y-4">
            {previewJobs.map((job, index) => (
              <div
                key={job._id}
                className={`bg-slate-900/50 p-4 rounded-lg border border-slate-800 transition-all hover:border-slate-700 hover:bg-slate-900 ${index >= 3 ? 'opacity-60' : ''
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
                    <p className="text-sm text-slate-400 mb-3 line-clamp-2 leading-relaxed">{job.description}</p>
                    <div className="flex items-center gap-4 text-xs text-slate-500 border-t border-slate-800/50 pt-3">
                      <span className="font-mono text-emerald-400 bg-emerald-500/5 px-1.5 py-0.5 rounded border border-emerald-500/10">
                        {formatSalary(job.totalCompensation)}
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
            ))}
          </div>
        ) : (
          <div className="bg-slate-900/50 p-12 rounded-lg border border-slate-800 text-center">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mx-auto mb-4"></div>
            <p className="text-slate-500">Loading job opportunities...</p>
          </div>
        )}
      </div>
    </div>
  );
}
