#!/usr/bin/env bash
#MISE description="Run DBOS runner on Convex dev (force fresh schedules)"
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CONVEX_HTTP_URL="https://elegant-magpie-239.convex.site"
exec "$script_dir/lib/dbos_wrapper.sh" --force --reset-db --tui "$@"
