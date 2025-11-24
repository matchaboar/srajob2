import { motion } from "framer-motion";
import { StatusTracker } from "./StatusTracker";

interface AppliedJobRowProps {
    job: any;
    isSelected: boolean;
    onSelect: () => void;
}

export function AppliedJobRow({ job, isSelected, onSelect }: AppliedJobRowProps) {
    const formatDate = (date: number) => {
        return new Date(date).toLocaleDateString(undefined, {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    };

    return (
        <motion.div
            layout
            initial={{ opacity: 0 }}
            animate={{
                opacity: 1,
                backgroundColor: isSelected ? "rgba(30, 41, 59, 1)" : "rgba(15, 23, 42, 0)",
            }}
            onClick={onSelect}
            className={`
        group flex items-center gap-4 px-4 py-2 border-b border-slate-800 cursor-pointer transition-colors
        ${isSelected ? "bg-slate-800" : "hover:bg-slate-900"}
      `}
        >
            {/* Selection Indicator */}
            <div className={`w-1 h-8 rounded-full transition-colors ${isSelected ? "bg-blue-500" : "bg-transparent"}`} />

            <div className="flex-1 min-w-0 grid grid-cols-12 gap-4 items-center">
                {/* Title & Company */}
                <div className="col-span-4">
                    <h3 className={`text-sm font-semibold truncate ${isSelected ? "text-white" : "text-slate-200"}`}>
                        {job.title}
                    </h3>
                    <p className="text-xs text-slate-500 truncate">{job.company}</p>
                </div>

                {/* Location */}
                <div className="col-span-2 flex items-center gap-2">
                    <span className="text-xs text-slate-400 truncate max-w-[100px]">{job.location}</span>
                    {job.remote && (
                        <span className="px-1.5 py-0.5 bg-emerald-500/10 text-emerald-400 text-[10px] font-medium rounded border border-emerald-500/20">
                            Remote
                        </span>
                    )}
                </div>

                {/* Applied Date */}
                <div className="col-span-2 text-right">
                    <span className="text-xs text-slate-500 block">Applied</span>
                    <span className="text-xs text-slate-300">{formatDate(job.appliedAt)}</span>
                </div>

                {/* Status Tracker */}
                <div className="col-span-4 flex justify-end">
                    <StatusTracker
                        status={job.workerStatus || (job.userStatus === 'applied' ? 'Applied' : null)}
                        updatedAt={job.workerUpdatedAt || job.appliedAt}
                    />
                </div>
            </div>
        </motion.div>
    );
}
