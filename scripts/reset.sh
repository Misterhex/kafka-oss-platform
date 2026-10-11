#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${1:-}" != "--yes-delete-data" ]]; then
  echo 'This deletes both Kafka clusters, producer sequence state, and DR checkpoints.'
  echo 'Run scripts/reset.sh --yes-delete-data to reset the lab.'
  exit 2
fi
docker compose down -v --remove-orphans
