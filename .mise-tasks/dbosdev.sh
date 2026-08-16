#!/usr/bin/env bash
#MISE description="Run DBOS runner with Convex dev backend"
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$script_dir/lib/dbos_wrapper.sh" "$@"
