#!/usr/bin/env bash
#MISE description="Run DBOS runner with Convex dev backend"
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

pwsh "$repo_root/start_worker.ps1" "$@"
