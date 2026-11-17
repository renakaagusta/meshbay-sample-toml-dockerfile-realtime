#!/usr/bin/env bash
# Runs the smoke test against a local process: starts src/index.ts on a free port, waits for /health, runs the smoke,
# and stops the process again. Same code path as LIVE, no cluster involved.
#
# usage: bash scripts/verify-local.sh [port=8099]
#        REDIS_URL=redis://localhost:6379/0 bash scripts/verify-local.sh 8099
set -euo pipefail

scriptFolder="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repositoryRoot="$(cd "$scriptFolder/.." && pwd)"
port="${1:-8099}"

cd "$repositoryRoot"

logFile="$(mktemp)"
PORT="$port" bun src/index.ts >"$logFile" 2>&1 &
applicationPID=$!
trap 'kill "$applicationPID" 2>/dev/null || true; rm -f "$logFile"' EXIT

# curl does the waiting: --retry-connrefused retries until the process listens, no shell loop needed.
if ! curl --fail --silent --show-error --retry 40 --retry-delay 1 --retry-connrefused --max-time 2 \
  "http://127.0.0.1:$port/health" > /dev/null; then
  echo "the app did not answer /health on port $port, its output:"
  cat "$logFile"
  exit 1
fi
node "$scriptFolder/smoke.mjs" "http://127.0.0.1:$port"
