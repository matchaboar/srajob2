#!/usr/bin/env bash
# Unified DBOS worker wrapper script
# Called by mise tasks with parameters:
#   --prod          Use production environment
#   --force         Force fresh scrape
#   --reset-db      Delete SQLite database before running

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
db_file="$repo_root/job_scrape_application/dbos_runtime/dbos.sqlite"

# Parse arguments
USE_PROD=false
FORCE_SCRAPE=false
RESET_DB=false
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --prod)
            USE_PROD=true
            shift
            ;;
        --force)
            FORCE_SCRAPE=true
            shift
            ;;
        --reset-db)
            RESET_DB=true
            shift
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

# Print queue status function
print_queue_status() {
    local label="${1:-}"
    if [[ -f "$db_file" ]]; then
        echo "=== DBOS Queue Status${label:+ ($label)} ==="
        uv run python -c "
import sqlite3
conn = sqlite3.connect('$db_file')
cursor = conn.cursor()
try:
    cursor.execute('SELECT status, COUNT(*) FROM queue_items GROUP BY status ORDER BY status;')
    rows = cursor.fetchall()
    if rows:
        print(f\"{'Status':<15} {'Count':>8}\")
        print('-' * 25)
        for status, count in rows:
            print(f'{status:<15} {count:>8}')
        print('-' * 25)
        cursor.execute('SELECT COUNT(*) FROM queue_items;')
        total = cursor.fetchone()[0]
        print(f\"{'Total':<15} {total:>8}\")
    else:
        print('Queue is empty')
except sqlite3.OperationalError as e:
    print(f'Database error: {e}')
conn.close()
" 2>/dev/null || echo "Could not read queue status"
        echo ""
    else
        echo "No DBOS database found"
        echo ""
    fi
}

# Track if we're cleaning up
CLEANUP_DONE=false

# Cleanup function for graceful shutdown
cleanup() {
    if [[ "$CLEANUP_DONE" == "true" ]]; then
        return
    fi
    CLEANUP_DONE=true

    echo ""
    echo "[shutdown] Cleaning up..."

    # Print final queue status
    print_queue_status "after exit"

    echo "[shutdown] Done."
}

# Set up signal handlers for graceful shutdown
trap cleanup EXIT
trap 'echo ""; echo "[signal] Received SIGINT, shutting down..."; exit 130' INT
trap 'echo ""; echo "[signal] Received SIGTERM, shutting down..."; exit 143' TERM

# Print initial queue status
if [[ "$RESET_DB" == "true" ]]; then
    print_queue_status "before reset"
else
    print_queue_status "before start"
fi

# Reset database if requested
if [[ "$RESET_DB" == "true" ]]; then
    rm -f "$db_file"
    if [[ -f "$db_file" ]]; then
        echo "ERROR: Failed to remove SQLite database: $db_file" >&2
        exit 1
    fi
    echo "Removed DBOS SQLite database: $db_file"
    echo ""
fi

# Build PowerShell arguments
PS_ARGS=()
if [[ "$USE_PROD" == "true" ]]; then
    PS_ARGS+=("-UseProd")
fi
if [[ "$FORCE_SCRAPE" == "true" ]]; then
    PS_ARGS+=("-ForceScrapeAll")
fi
PS_ARGS+=("${EXTRA_ARGS[@]}")

# Run the PowerShell worker script
# Use exec to replace this shell so signals go directly to pwsh
exec pwsh "$repo_root/start_worker.ps1" "${PS_ARGS[@]}"
