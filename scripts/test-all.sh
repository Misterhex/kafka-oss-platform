#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${1:-}" != "--yes-delete-data" ]]; then
  echo 'This suite resets lab volumes before each scenario. Use --yes-delete-data.'
  exit 2
fi
for mode in planned forced; do
  ./scripts/reset.sh --yes-delete-data
  APP_KAFKA_BOOTSTRAP=k0x:9092 ./scripts/up.sh
  ./scripts/test.sh "$mode"
  cp artifacts/compose.log "artifacts/${mode}-compose.log"
done
