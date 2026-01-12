#!/usr/bin/env bash
set -euo pipefail

batch_size=${1:-1000}

npx convex run --prod migrations:runPurgeScrapes "{\"batchSize\":${batch_size}}"
npx convex run --prod migrations:runPurgeScrapeActivity "{\"batchSize\":${batch_size}}"
