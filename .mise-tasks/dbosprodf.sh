#!/usr/bin/env bash
#MISE description="Run DBOS runner on Convex prod (force fresh schedules)"
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Delete DBOS SQLite database before running
rm -f "$repo_root/job_scrape_application/dbos.sqlite"

pwsh "$repo_root/start_worker.ps1" -UseProd -ForceScrapeAll "$@"
