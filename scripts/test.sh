#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode="${1:-planned}"
[[ "$mode" == planned || "$mode" == forced ]] || { echo 'Use planned or forced'; exit 2; }
mkdir -p artifacts
trap 'docker compose ps -a > artifacts/compose-status.txt; docker compose logs --no-color > artifacts/compose.log' EXIT
./scripts/lab.sh snapshot > "artifacts/$mode-before.json"
# MM2 preserves bytes, but does not replicate Schema Registry. Prepare the empty DR
# registry before testing Kafka failover, preserving schema IDs through commit-log replay.
if [[ "$(curl -fsS 'http://localhost:8087/subjects?deleted=true')" != '[]' ]]; then
  echo 'The DR test requires an empty SG registry; use a fresh lab.' >&2
  exit 1
fi
./scripts/schema-backup.sh backup --bootstrap=k0x:19092 --topic=_schemas --file=/backups/dr-test.json --overwrite
docker compose stop schema-registry-k0y
docker compose exec -T k0y /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server k0y:19092 --delete --if-exists --topic _schemas
for attempt in {1..30}; do
  topics=$(docker compose exec -T k0y /opt/kafka/bin/kafka-topics.sh --bootstrap-server k0y:19092 --list)
  if [[ $'\n'"$topics"$'\n' != *$'\n_schemas\n'* ]]; then break; fi
  if [[ "$attempt" == 30 ]]; then echo 'SG schema topic deletion timed out' >&2; exit 1; fi
  sleep 1
done
./scripts/schema-backup.sh restore --bootstrap=k0y:19092 --file=/backups/dr-test.json --registry-stopped
docker compose up -d --no-deps --wait --wait-timeout 180 schema-registry-k0y
force=false
if [[ "$mode" == forced ]]; then
  force=true
  docker compose stop k0x
fi
curl --fail-with-body --max-time 240 -sS -X POST \
  "http://localhost:8080/api/failover?force=$force" > "artifacts/$mode-ready.json"
./scripts/lab.sh paused
# This is the operator action under test, not controller-driven container orchestration.
APP_KAFKA_BOOTSTRAP=k0y:9092 docker compose up -d --no-deps --force-recreate \
  --wait --wait-timeout 180 synthetic-producer synthetic-consumer
curl -fsS -X POST http://localhost:8081/resume
curl -fsS -X POST http://localhost:8082/resume
if [[ "$mode" == forced ]]; then
  docker compose up -d --no-deps --wait --wait-timeout 180 k0x
fi
./scripts/lab.sh verify < "artifacts/$mode-before.json" > "artifacts/$mode-result.json"
docker compose restart dr-controller
./scripts/lab.sh wait-controller
./scripts/lab.sh same-checkpoint < "artifacts/$mode-ready.json"
docker compose restart synthetic-producer synthetic-consumer
./scripts/lab.sh wait-clients
curl -fsS -X POST http://localhost:8081/resume
curl -fsS -X POST http://localhost:8082/resume
curl --fail-with-body --max-time 60 -sS -X POST \
  "http://localhost:8080/api/failover?force=$force" > /dev/null
./scripts/lab.sh verify < "artifacts/$mode-before.json" > "artifacts/$mode-result.json"
printf '\n%s recovery PASS. Both clients now use k0y:9092.\n' "$mode"
