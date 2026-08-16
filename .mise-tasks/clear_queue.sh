#!/usr/bin/env bash
#MISE description="Clear pending + processing scrape_url_queue rows (Convex dev/prod)"
set -euo pipefail

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  cat <<'USAGE'
Usage:
  ./.mise-tasks/clear_queue.sh [--env dev|prod] [--limit N] [--dry-run] [--max-batches N]

Examples:
  ./.mise-tasks/clear_queue.sh
  ./.mise-tasks/clear_queue.sh --env prod --limit 5000
  ./.mise-tasks/clear_queue.sh --dry-run --max-batches 1
USAGE
  exit 0
fi

env="dev"
limit="1000"
dry_run="false"
max_batches="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      env="${2:-}"
      shift 2
      ;;
    --limit)
      limit="${2:-}"
      shift 2
      ;;
    --dry-run)
      dry_run="true"
      shift
      ;;
    --max-batches)
      max_batches="${2:-}"
      shift 2
      ;;
    -h|--help)
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ "$env" != "dev" && "$env" != "prod" ]]; then
  echo "--env must be dev or prod (got: $env)" >&2
  exit 1
fi
if [[ ! "$limit" =~ ^[0-9]+$ ]]; then
  echo "--limit must be an integer (got: $limit)" >&2
  exit 1
fi
if [[ ! "$max_batches" =~ ^[0-9]+$ ]]; then
  echo "--max-batches must be an integer (got: $max_batches)" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "$env" == "prod" ]]; then
  env_file="$repo_root/job_board_application/.env.production"
  if [[ ! -f "$env_file" ]]; then
    env_file="$repo_root/.env.production"
  fi
  if [[ ! -f "$env_file" ]]; then
    echo "Missing $env_file (required for --env prod)" >&2
    exit 1
  fi
  set -a
  # shellcheck source=/dev/null
  source "$env_file"
  set +a
elif [[ -f "$repo_root/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$repo_root/.env"
  set +a
fi

pushd "$repo_root/job_board_application" >/dev/null

payload=$(printf '{"statuses":["pending","processing"],"limit":%s,"dryRun":%s}' "$limit" "$dry_run")
batch=1
while :; do
  echo "Clearing batch $batch (limit=$limit, dryRun=$dry_run, env=$env)..."
  if [[ "$env" == "prod" ]]; then
    output=$(npx convex run --prod admin:wipeScrapeQueueByStatus "$payload")
  else
    output=$(npx convex run admin:wipeScrapeQueueByStatus "$payload")
  fi
  echo "$output"

  if echo "$output" | rg -q '"hasMore":true'; then
    if [[ "$max_batches" -gt 0 && "$batch" -ge "$max_batches" ]]; then
      echo "Stopped after $batch batches due to --max-batches=$max_batches." >&2
      popd >/dev/null
      exit 1
    fi
    batch=$((batch + 1))
    continue
  fi
  break
done

popd >/dev/null
