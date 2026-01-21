#!/usr/bin/env bash
# Unified DBOS worker wrapper script
# Called by mise tasks with parameters:
#   --prod          Use production environment
#   --force         Force fresh scrape
#   --reset-db      Delete SQLite database before running

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
db_file="$repo_root/job_scrape_application/dbos_runtime/dbos.sqlite"
dbos_system_file="$repo_root/job_scrape_application/dbos_runtime/dbos_system.sqlite"

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

# Print queue status function with unicode table
print_queue_status() {
    local label="${1:-}"
    if [[ -f "$db_file" ]]; then
        uv run python -c "
import sqlite3
conn = sqlite3.connect('$db_file')
cursor = conn.cursor()

label = '${label}'
label_str = f' ({label})' if label else ''

# Box drawing chars
TL, TR, BL, BR = '╔', '╗', '╚', '╝'
H, V = '═', '║'
LT, RT, TT, BT, X = '╠', '╣', '╦', '╩', '╬'
HL, VL = '─', '│'
LTL, RTL = '╟', '╢'

W = 70  # total width

def hline(left, mid, right, char=H):
    return f'{left}{char * (W-2)}{right}'

def row(text):
    return f'{V}  {text:<{W-4}}{V}'

def section_header(title):
    return f'{V}  {title:<{W-4}}{V}'

try:
    # Summary counts
    print(hline(TL, H, TR))
    print(row(f'DBOS Queue Status{label_str}'))
    print(hline(LT, H, RT))

    cursor.execute('SELECT status, COUNT(*) FROM queue_items GROUP BY status ORDER BY status;')
    status_rows = cursor.fetchall()

    if status_rows:
        for status, count in status_rows:
            print(row(f'{status:<30} {count:>30}'))
        print(hline(LTL, HL, RTL, HL))
        cursor.execute('SELECT COUNT(*) FROM queue_items;')
        total = cursor.fetchone()[0]
        print(row(f'{\"Total\":<30} {total:>30}'))
    else:
        print(row('Queue is empty'))

    # Failed items detail
    cursor.execute('''
        SELECT site_id, url, error
        FROM queue_items
        WHERE status = 'failed'
        ORDER BY site_id, updated_at DESC
    ''')
    failed = cursor.fetchall()

    if failed:
        print(hline(LT, H, RT))
        print(row('FAILED ITEMS'))
        print(hline(LTL, HL, RTL, HL))
        for site_id, url, error in failed:
            site = site_id or 'unknown'
            # Truncate URL for display
            url_short = url[:50] + '...' if len(url) > 50 else url
            err_short = (error or 'No error message')[:50]
            print(row(f'{site:<20} {url_short}'))
            print(row(f'  └─ {err_short}'))

    # Pending items detail
    cursor.execute('''
        SELECT site_id, COUNT(*) as cnt
        FROM queue_items
        WHERE status = 'pending'
        GROUP BY site_id
        ORDER BY cnt DESC
    ''')
    pending = cursor.fetchall()

    if pending:
        print(hline(LT, H, RT))
        print(row('PENDING BY SITE'))
        print(hline(LTL, HL, RTL, HL))
        for site_id, count in pending:
            site = site_id or 'unknown'
            print(row(f'{site:<50} {count:>10}'))

    # Processing items
    cursor.execute('''
        SELECT site_id, url
        FROM queue_items
        WHERE status = 'processing'
        ORDER BY site_id
    ''')
    processing = cursor.fetchall()

    if processing:
        print(hline(LT, H, RT))
        print(row('CURRENTLY PROCESSING'))
        print(hline(LTL, HL, RTL, HL))
        for site_id, url in processing:
            site = site_id or 'unknown'
            url_short = url[:45] + '...' if len(url) > 45 else url
            print(row(f'{site:<20} {url_short}'))

    print(hline(BL, H, BR))

    # Print full URLs below the table
    if failed or pending or processing:
        print()

    if failed:
        print('Failed URLs:')
        for site_id, url, error in failed:
            site = site_id or 'unknown'
            print(f'  [{site}] {url}')
        print()

    if pending:
        # Get actual pending URLs
        cursor.execute('''
            SELECT site_id, url
            FROM queue_items
            WHERE status = 'pending'
            ORDER BY site_id, created_at
        ''')
        pending_urls = cursor.fetchall()
        if pending_urls:
            print('Pending URLs:')
            for site_id, url in pending_urls:
                site = site_id or 'unknown'
                print(f'  [{site}] {url}')
            print()

    if processing:
        print('Processing URLs:')
        for site_id, url in processing:
            site = site_id or 'unknown'
            print(f'  [{site}] {url}')
        print()

except sqlite3.OperationalError as e:
    print(hline(TL, H, TR))
    print(row(f'Database error: {e}'))
    print(hline(BL, H, BR))

conn.close()
" 2>/dev/null || {
            echo "╔════════════════════════════════════════════════════════════════════╗"
            echo "║  Could not read queue status                                       ║"
            echo "╚════════════════════════════════════════════════════════════════════╝"
        }
        echo ""
    else
        echo "╔════════════════════════════════════════════════════════════════════╗"
        echo "║  No DBOS database found                                            ║"
        echo "╚════════════════════════════════════════════════════════════════════╝"
        echo ""
    fi
}

# Track child process PID
CHILD_PID=""

# Gracefully stop the child process and its workers.
stop_child_process() {
    local initial_signal="$1"
    local grace_seconds="${2:-10}"

    if [[ -z "$CHILD_PID" ]] || ! kill -0 "$CHILD_PID" 2>/dev/null; then
        return
    fi

    kill "-${initial_signal}" "$CHILD_PID" 2>/dev/null || true

    local start_time=$SECONDS
    while kill -0 "$CHILD_PID" 2>/dev/null; do
        if (( SECONDS - start_time >= grace_seconds )); then
            break
        fi
        sleep 0.2
    done

    if kill -0 "$CHILD_PID" 2>/dev/null; then
        kill -TERM "$CHILD_PID" 2>/dev/null || true
        start_time=$SECONDS
        while kill -0 "$CHILD_PID" 2>/dev/null; do
            if (( SECONDS - start_time >= 5 )); then
                break
            fi
            sleep 0.2
        done
    fi

    if kill -0 "$CHILD_PID" 2>/dev/null; then
        # Kill any direct children before forcing the parent down.
        pkill -TERM -P "$CHILD_PID" 2>/dev/null || true
        sleep 0.2
        pkill -KILL -P "$CHILD_PID" 2>/dev/null || true
        kill -KILL "$CHILD_PID" 2>/dev/null || true
    fi
}

# Cleanup function for graceful shutdown
cleanup() {
    echo ""
    echo "[shutdown] Cleaning up..."

    # Print final queue status
    print_queue_status "after exit"

    echo "[shutdown] Done."
}

# Signal handler for SIGINT (CTRL+C)
handle_sigint() {
    echo ""
    echo "[signal] Received SIGINT, shutting down..."

    stop_child_process "INT" 12

    cleanup
    exit 130
}

# Signal handler for SIGTERM
handle_sigterm() {
    echo ""
    echo "[signal] Received SIGTERM, shutting down..."

    stop_child_process "TERM" 10

    cleanup
    exit 143
}

# Set up signal handlers
trap handle_sigint INT
trap handle_sigterm TERM

# Kill zombie processes holding deleted SQLite files (prevents "readonly database" errors)
kill_zombie_sqlite_processes() {
    local dbos_runtime_dir="$1"
    if ! command -v lsof &>/dev/null; then
        return
    fi

    # Find PIDs holding deleted SQLite files in the dbos_runtime directory
    local zombie_pids=()
    while IFS= read -r line; do
        if [[ "$line" =~ \.sqlite.*\(deleted\) ]] || [[ "$line" =~ dbos.*\.sqlite ]]; then
            local pid
            pid=$(echo "$line" | awk '{print $2}')
            if [[ -n "$pid" && "$pid" != "$$" ]]; then
                # Check if we've already added this PID
                local found=false
                for existing in "${zombie_pids[@]}"; do
                    if [[ "$existing" == "$pid" ]]; then
                        found=true
                        break
                    fi
                done
                if [[ "$found" == "false" ]]; then
                    zombie_pids+=("$pid")
                fi
            fi
        fi
    done < <(lsof +D "$dbos_runtime_dir" 2>/dev/null || true)

    for pid in "${zombie_pids[@]}"; do
        echo "[preflight] Killing process $pid holding deleted SQLite files"
        kill -9 "$pid" 2>/dev/null || true
    done

    if [[ ${#zombie_pids[@]} -gt 0 ]]; then
        sleep 0.5
    fi
}

# Print initial queue status
if [[ "$RESET_DB" == "true" ]]; then
    print_queue_status "before reset"
else
    print_queue_status "before start"
fi

# Kill any zombie processes holding deleted SQLite files
dbos_runtime_dir="$repo_root/job_scrape_application/dbos_runtime"
kill_zombie_sqlite_processes "$dbos_runtime_dir"

# Reset database if requested (includes WAL, SHM, and journal files to prevent corruption issues)
if [[ "$RESET_DB" == "true" ]]; then
    removed_files=()
    # Remove both the queue database and DBOS system database
    for f in "$db_file" "${db_file}-wal" "${db_file}-shm" "${db_file}-journal" \
             "$dbos_system_file" "${dbos_system_file}-wal" "${dbos_system_file}-shm" "${dbos_system_file}-journal"; do
        if [[ -f "$f" ]]; then
            rm -f "$f"
            removed_files+=("$(basename "$f")")
        fi
    done
    if [[ -f "$db_file" ]]; then
        echo "ERROR: Failed to remove SQLite database: $db_file" >&2
        exit 1
    fi
    if [[ ${#removed_files[@]} -gt 0 ]]; then
        echo "Removed SQLite files: ${removed_files[*]}"
    else
        echo "No SQLite files to remove"
    fi
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
if [[ "$RESET_DB" == "true" ]]; then
    # Pass -ClearSqlite to trigger zombie process detection in PowerShell
    PS_ARGS+=("-ClearSqlite")
fi
PS_ARGS+=("${EXTRA_ARGS[@]}")

# Run as child process with signal handling
pwsh "$repo_root/start_worker.ps1" "${PS_ARGS[@]}" &
CHILD_PID=$!

# Wait for child process and capture its exit status
wait "$CHILD_PID"
EXIT_CODE=$?

# Show final status on normal exit
cleanup
exit $EXIT_CODE
