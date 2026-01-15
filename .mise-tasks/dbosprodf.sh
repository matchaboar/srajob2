#!/usr/bin/env bash
#MISE description="Run DBOS runner on Convex prod (force fresh schedules)"
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$script_dir/lib/dbos_wrapper.sh" --prod --force --reset-db "$@"
