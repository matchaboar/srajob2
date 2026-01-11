#!/usr/bin/env bash
#MISE description="Run DBOS runner on Convex dev (force fresh schedules)"
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

pwsh "$repo_root/start_worker.ps1" -ForceScrapeAll "$@"
