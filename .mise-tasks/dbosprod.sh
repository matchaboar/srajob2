#!/usr/bin/env bash
#MISE description="Run DBOS runner on Convex prod"
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

pwsh "$repo_root/start_worker.ps1" -UseProd "$@"
