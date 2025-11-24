import { StatusTracker } from "../components/StatusTracker";

/**
 * Visual test page for StatusTracker component
 * Navigate to http://localhost:5173/status-tracker-test to view all states
 */
export function StatusTrackerTest() {
    const now = Date.now();
    const oneHourAgo = now - 3600000;
    const fiveMinutesAgo = now - 300000;

    const testCases = [
        {
            title: "Applied (Step 1 - Current)",
            status: "Applied",
            updatedAt: fiveMinutesAgo,
        },
        {
            title: "Queued (Step 2 - Current)",
            status: "pending",
            updatedAt: fiveMinutesAgo,
        },
        {
            title: "Processing (Step 3 - Current)",
            status: "processing",
            updatedAt: fiveMinutesAgo,
        },
        {
            title: "Completed (Step 4 - Success)",
            status: "completed",
            updatedAt: oneHourAgo,
        },
        {
            title: "Failed (Step 4 - Error)",
            status: "failed",
            updatedAt: oneHourAgo,
        },
    ];

    return (
        <div className="min-h-screen bg-slate-950 text-slate-200 p-8">
            <div className="max-w-4xl mx-auto">
                <h1 className="text-3xl font-bold mb-2 text-white">StatusTracker Visual Tests</h1>
                <p className="text-slate-400 mb-8">
                    All possible states of the status tracker component for visual regression testing
                </p>

                <div className="space-y-8">
                    {testCases.map((testCase, index) => (
                        <div
                            key={index}
                            className="bg-slate-900/50 border border-slate-800 rounded-lg p-6"
                            data-testid={`status-tracker-${testCase.status}`}
                        >
                            <h2 className="text-lg font-semibold mb-4 text-slate-300">
                                {testCase.title}
                            </h2>
                            <div className="flex justify-center items-center py-4">
                                <StatusTracker
                                    status={testCase.status}
                                    updatedAt={testCase.updatedAt}
                                />
                            </div>
                            <div className="mt-4 text-xs text-slate-500 font-mono">
                                Status: <span className="text-blue-400">{testCase.status}</span>
                            </div>
                        </div>
                    ))}
                </div>

                <div className="mt-12 p-6 bg-slate-900/30 border border-slate-800 rounded-lg">
                    <h3 className="text-sm font-semibold text-slate-400 uppercase mb-3">
                        Color Legend
                    </h3>
                    <div className="grid grid-cols-2 gap-4 text-sm">
                        <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded bg-gradient-to-br from-slate-800 to-slate-700 border border-slate-700"></div>
                            <span className="text-slate-400">Pending (Gray)</span>
                        </div>
                        <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded bg-gradient-to-br from-blue-500 to-blue-600 border border-blue-400"></div>
                            <span className="text-slate-400">In Progress (Blue)</span>
                        </div>
                        <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded bg-gradient-to-br from-emerald-500 to-emerald-600 border border-emerald-400"></div>
                            <span className="text-slate-400">Completed (Green)</span>
                        </div>
                        <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded bg-gradient-to-br from-red-600 to-red-700 border border-red-500"></div>
                            <span className="text-slate-400">Failed (Red)</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
