#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f artifacts/schema-test/backup.json && -f artifacts/schema-test/event.bin ]] || {
  echo 'First generate the isolated fixtures with scripts/test-schema.sh.' >&2; exit 1;
}
port="${SCHEMA_IMPORT_PORT:-8090}"
test_name="kafka-schema-import-test-$(date +%s)"
test_topic="_schemas_import_test_$(date +%s)"
# Own only this dedicated registry and topic. Leave site registries and demo intact.
docker compose --profile schema-test run -d --no-deps --name "$test_name" \
  -p "127.0.0.1:$port:8081" \
  -e "SCHEMA_REGISTRY_HOST_NAME=$test_name" \
  -e "SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID=$test_name" \
  -e "SCHEMA_REGISTRY_KAFKASTORE_TOPIC=$test_topic" \
  -e 'SCHEMA_REGISTRY_HEAP_OPTS=-Xms64m -Xmx192m' schema-registry-restore
cleanup() {
  docker rm -f "$test_name" >/dev/null
  docker compose exec -T k0y /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server k0y:19092 --delete --if-exists --topic "$test_topic"
}
trap cleanup EXIT
for attempt in {1..90}; do
  if curl -fsS "http://localhost:$port/subjects" >/dev/null 2>&1; then break; fi
  if [[ "$attempt" == 90 ]]; then docker logs "$test_name"; exit 1; fi
  sleep 1
done
mvn -B -ntp -pl schema-backup -am test -Dschema.import.url="http://localhost:$port" -Dschema.import.topic="$test_topic"
printf '\nSchema Registry IMPORT API restore integration PASS.\n'
