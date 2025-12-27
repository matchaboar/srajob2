#!/usr/bin/env bash
#MISE description="Delete jobs/scrapes/ignored/queue from last N hours in Convex prod"
set -euo pipefail

read -r -p "How many hours to delete? " hours
if [[ -z "${hours}" ]]; then
  echo "Hours is required." >&2
  exit 1
fi
if [[ ! "${hours}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "Hours must be a number (e.g. 24 or 0.5)." >&2
  exit 1
fi

uv run agent_scripts/delete_jobs_from_time.py --hours "${hours}" --env prod
