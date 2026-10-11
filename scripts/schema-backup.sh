#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p backups
docker compose run --rm -T --no-deps schema-backup "$@"
