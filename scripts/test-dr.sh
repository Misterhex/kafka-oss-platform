#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Application-team actions belong in the test harness, never in controller promotion.
for target in k0y k0x; do
  curl --fail-with-body --max-time 300 -sS -X POST "http://localhost:8080/api/promote?target=$target"
  printf 'APP_KAFKA_BOOTSTRAP=%s:9092\n' "$target" > .env
  docker compose up -d --no-deps --force-recreate --wait synthetic-producer synthetic-consumer
  curl -fsS -X POST http://localhost:8081/resume
  curl -fsS -X POST http://localhost:8082/resume
  ./scripts/lab.sh assert-dr "$target"
done
