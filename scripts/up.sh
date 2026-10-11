#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --profile tools build
docker compose up -d --wait --wait-timeout "${LAB_START_TIMEOUT:-900}"
curl --fail-with-body --max-time 180 -sS -X POST http://localhost:8080/api/bootstrap
printf '\nAKHQ: http://localhost:8085\n'
