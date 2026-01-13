#!/usr/bin/env bash
#MISE description="Run DBOS runner + frontend (both on Convex dev)"
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Trap to cleanup background processes on exit
cleanup() {
    echo "Shutting down..."
    jobs -p | xargs -r kill 2>/dev/null || true
    wait
}
trap cleanup EXIT INT TERM

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
echo "==================================================="
echo ""
echo "Press Ctrl+C to stop all services"

# Wait for either process to exit
wait -n

# If we get here, one process exited, so cleanup will trigger
exit $?
