# Manual HK → SG connector cleanup and SG → HK creation

These commands are for you to execute. Java helpers run in the lab's tools container; no Python or host JDK is needed.

This starts reverse replication at **the SG log end recorded now**. Existing SG records are excluded, including any SG-only records that HK lacks. For a lossless planned reversal, capture the boundary after forward replication drains and before the first SG-origin write. A later snapshot is not that earlier boundary.

Use a running lab with SG active and both brokers available. Follow the README's failover API and manual client restart steps first if the clients still use HK. This procedure changes replication direction; it does not switch application clients back to HK.

## Stop and record

Run from this repository directory, in the same Bash terminal:

```bash
set -euo pipefail
mkdir -p artifacts/manual
./scripts/lab.sh expect-phase SG_ACTIVE
# Confirm both statuses show bootstrap=k0y:9092 before continuing.
curl -fsS http://localhost:8081/status
curl -fsS http://localhost:8082/status

curl -fsS -X POST http://localhost:8081/pause
curl -fsS -X POST http://localhost:8082/pause

curl -fsS -X PUT http://localhost:8084/connectors/hk-to-sg/stop
./scripts/lab.sh wait-connector http://connect-k0y:8083 hk-to-sg STOPPED

curl -fsS http://localhost:8084/connectors/hk-to-sg/config > artifacts/manual/forward-config.json
curl -fsS http://localhost:8084/connectors/hk-to-sg/offsets > artifacts/manual/forward-offsets.json
curl -fsS http://localhost:8080/api/status > artifacts/manual/dr-state.json

docker compose exec -T k0y /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server k0y:19092 --topic appTopic --time -1 \
  > artifacts/manual/sg-end-offsets.txt
cat artifacts/manual/sg-end-offsets.txt

# Optional: separately record application consumer commits.
docker compose exec -T k0y /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server k0y:19092 --describe --group dr-consumer \
  > artifacts/manual/sg-consumer-commits.txt

# Keep automatic DR orchestration out of the manual experiment.
docker compose stop dr-controller
```

The end-offset file contains one line per partition, for example `appTopic:0:120`. These are **next offsets**; consumer-group commits and old HK-source connector offsets are different values.

## Delete the connector and its metadata

Reset offsets **before** deleting the connector. Offset management requires an existing, stopped connector.

```bash
curl -fsS -X DELETE http://localhost:8084/connectors/hk-to-sg/offsets
curl -fsS http://localhost:8084/connectors/hk-to-sg/offsets
# Expect {"offsets":[]} before proceeding.
curl -fsS -X DELETE http://localhost:8084/connectors/hk-to-sg

docker compose exec -T k0y /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server k0y:19092 --delete --if-exists \
  --topic '^mm2-offset-syncs[.]hk[.]internal$'
docker compose exec -T k0y /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server k0y:19092 --delete --if-exists \
  --topic '^hk[.]checkpoints[.]internal$'

curl -fsS http://localhost:8084/connectors
docker compose exec -T k0y /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server k0y:19092 --list
```

The supplied lab has no MirrorCheckpointConnector, so `hk.checkpoints.internal` normally does not exist. If you added one, stop/reset/delete that connector first so it cannot recreate the topic. The offset-sync topic lives on SG because the forward connector sets its location to `target`.

Connect offset deletion uses tombstones; old log bytes disappear through compaction. This fully resets that connector's logical offsets. Do not delete shared `_connect-k0y-*` topics to remove one connector. Application data, consumer-group commits and the controller's independent DR state remain intact.

If you specifically want to erase the entire SG worker's stored configuration/offset/status logs, first confirm its connectors list is empty, then:

```bash
docker compose stop connect-k0y
for topic in _connect-k0y-configs _connect-k0y-offsets _connect-k0y-status; do
  docker compose exec -T k0y /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server k0y:19092 --delete --if-exists --topic "^$topic$"
done
docker compose exec -T k0y /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server k0y:19092 --list
# Once all three topics are absent:
docker compose start connect-k0y
```

## Create reverse replication at the recorded boundary

The checked-in [sg-to-hk.json](infra/connect/sg-to-hk.json) uses source `k0y:19092`, target `k0x:19092`, source alias `sg`, identity topic names, RF 1 and **initial_state STOPPED**. Create it on HK's Connect worker.

```bash
./scripts/lab.sh reverse-offsets sg < artifacts/manual/sg-end-offsets.txt \
  > artifacts/manual/reverse-offsets.json
cat artifacts/manual/reverse-offsets.json

curl -fsS -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data-binary @infra/connect/sg-to-hk.json
./scripts/lab.sh wait-connector http://connect-k0x:8083 sg-to-hk STOPPED

# Clear any offsets left by an older experiment with this name.
curl -fsS -X DELETE http://localhost:8083/connectors/sg-to-hk/offsets
curl -fsS -X PATCH http://localhost:8083/connectors/sg-to-hk/offsets \
  -H 'Content-Type: application/json' \
  --data-binary @artifacts/manual/reverse-offsets.json
curl -fsS http://localhost:8083/connectors/sg-to-hk/offsets
```

MirrorMaker stores the **last processed** offset, so the Java helper converts next offset `120` to `119`. A zero boundary becomes a null offset; the reverse config explicitly uses `source.consumer.auto.offset.reset=earliest`.

## Start

```bash
curl -fsS -X PUT http://localhost:8083/connectors/sg-to-hk/resume
./scripts/lab.sh wait-connector http://connect-k0x:8083 sg-to-hk RUNNING
curl -fsS -X POST http://localhost:8081/resume
curl -fsS -X POST http://localhost:8082/resume

curl -fsS http://localhost:8083/connectors/sg-to-hk/status
curl -fsS http://localhost:8083/connectors/sg-to-hk/offsets
docker compose exec -T k0x /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server k0x:19092 --topic appTopic --time -1
```

SG remains the application primary. New SG records now replicate to HK. AKHQ: <http://localhost:8085/> → `k0x-hk` → Connect → `connect-k0x` → `sg-to-hk`.

Keep the controller stopped during the manual experiment; the automated suite expects the original forward connector. Do not run both identity-policy directions together, as records can circulate indefinitely.

References: [Connect REST API](https://kafka.apache.org/41/kafka-connect/user-guide/#rest-api), [Kafka 4.1.1 MirrorSourceTask](https://github.com/apache/kafka/blob/4.1.1/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java).
