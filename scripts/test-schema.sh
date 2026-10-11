#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export RESTORE_SCHEMA_TOPIC=_schemas_restore
# Isolated test registries/topics only; no failover or application topic deletion.
docker compose --profile schema-test stop schema-registry-fixture schema-registry-restore
for pair in 'k0x _schemas_fixture' 'k0y _schemas_restore'; do
  read -r broker topic <<< "$pair"
  docker compose exec -T "$broker" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$broker:19092" --delete --if-exists --topic "$topic"
done
# Wait for the test topic deletions, including asynchronous broker metadata removal.
for pair in 'k0x _schemas_fixture' 'k0y _schemas_restore'; do
  read -r broker topic <<< "$pair"
  for attempt in {1..30}; do
    topics=$(docker compose exec -T "$broker" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$broker:19092" --list)
    if [[ $'\n'"$topics"$'\n' != *$'\n'"$topic"$'\n'* ]]; then break; fi
    if [[ "$attempt" == 30 ]]; then echo "Topic deletion timed out: $topic" >&2; exit 1; fi
    sleep 1
  done
done
docker compose --profile schema-test up -d --no-deps --wait --wait-timeout 180 schema-registry-fixture
mvn -B -ntp -pl schema-backup -am test -Dschema.integration=seed
# Verification needs only the destination; release the fixture's JVM memory first.
docker compose --profile schema-test stop schema-registry-fixture
docker compose --profile schema-test up -d --no-deps --wait --wait-timeout 180 schema-registry-restore
mvn -B -ntp -pl schema-backup -am test -Dschema.integration=verify
printf '\nSchema Registry backup/restore integration PASS. Restore registry: http://localhost:8088\n'
