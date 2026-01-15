#!/usr/bin/env bash
#MISE description="Run DBOS runner + frontend (both on Convex dev with dev site_schedules)"
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Trap to cleanup background processes on exit
cleanup() {
    echo "Shutting down..."
    jobs -p | xargs -r kill 2>/dev/null || true
    wait
}
trap cleanup EXIT INT TERM

# Explicitly set dev Convex URL
export CONVEX_HTTP_URL=https://elegant-magpie-239.convex.site

echo "Using dev Convex deployment: $CONVEX_HTTP_URL"
echo ""

# Start DBOS runner with API in background
echo "Starting DBOS runner with API (Convex dev)..."
cd "$repo_root"
uv run -m job_scrape_application.dbos_runtime.runner \
    --with-api \
    --listing-concurrency 2 \
    --detail-concurrency 4 &

DBOS_PID=$!

# Give DBOS a moment to start
sleep 2

# Start frontend (which runs both Vite + Convex dev)
echo "Starting frontend (pnpm dev)..."
cd "$repo_root/job_board_application"
pnpm dev &

FRONTEND_PID=$!

echo ""
echo "==================================================="
echo "Services running:"
echo "  DBOS API:        http://localhost:8080"
echo "  Convex Dev:      (check terminal output)"
echo "  Frontend:        http://localhost:5173"
echo "  Environment:     DEV (elegant-magpie-239)"
echo "==================================================="
echo ""
echo "To sync dev site schedules to Convex, run:"
echo "  uv run python agent_scripts/config/update_and_sync_site_schedules.py"
echo ""
echo "Press Ctrl+C to stop all services"

# Wait for either process to exit
wait -n

# If we get here, one process exited, so cleanup will trigger
exit $?
