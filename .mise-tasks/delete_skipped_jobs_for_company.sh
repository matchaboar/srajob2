#!/usr/bin/env bash
#MISE description="Delete skipped/ignored jobs for a company (name or alias) in Convex prod"
set -euo pipefail

read -r -p "Company name or alias: " company
if [[ -z "${company}" ]]; then
  echo "Company name is required." >&2
  exit 1
fi

uv run agent_scripts/delete_skipped_jobs_for_company.py --company "${company}" --env prod
