#!/usr/bin/env bash
set -euo pipefail

baseUrl="${1:?usage: scripts/smoke.sh https://<service>.renakaagusta.dev}"
scriptFolder="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

node "$scriptFolder/smoke.mjs" "$baseUrl"
