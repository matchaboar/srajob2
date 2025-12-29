#!/usr/bin/env bash
#MISE description="Wipe site data by company/domain and rerun (Convex dev/prod)"
set -euo pipefail

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  cat <<'USAGE'
Usage:
  ./.mise-tasks/wipe_site.sh [--env dev|prod] [--company "Name"] [--domain example.com] [--dry-run]

Examples:
  ./.mise-tasks/wipe_site.sh --company "GitHub" --env prod
  ./.mise-tasks/wipe_site.sh --domain bloomberg.avature.net
USAGE
  exit 0
fi

uv run agent_scripts/wipe_comapny_convex.py "$@"
