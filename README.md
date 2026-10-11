# Kafka DR lab

Two independent Kafka **4.1.1** single-node KRaft clusters: HK (`k0x`) and SG (`k0y`). Each has a distributed Connect worker. Java **21**, Spring Boot **4.0.3**, Maven modules and Apache Kafka SDK **4.1.1**; no Spring Kafka.

Clients connect directly to **k0x:9092** or **k0y:9092**. The controller prepares the failover; you manually restart clients with the other direct address. There is no CoreDNS, DNS switching, TCP gateway, Docker socket adapter or Python.

## Start

Requires Docker Compose 2.20.2+ and curl. Allow roughly 6 GB RAM and several minutes for the first build. No host Java or Maven installation is needed.

```sh
./scripts/up.sh
curl -sS http://localhost:8080/api/status
curl -sS http://localhost:8080/api/apps
```

Open **[AKHQ](http://localhost:8085/)**. Both cluster connections use fixed internal addresses; changing the application primary never disconnects AKHQ or either Connect worker. An actual broker outage naturally affects access to that broker.

| Service | Host address |
| --- | --- |
| AKHQ | http://localhost:8085 |
| HK Connect | http://localhost:8083 |
| SG Connect | http://localhost:8084 |
| DR controller | http://localhost:8080 |
| Producer administration | http://localhost:8081 |
| Consumer administration | http://localhost:8082 |
| HK Kafka bootstrap | localhost:19092 |
| SG Kafka bootstrap | localhost:29092 |

## Failover API and manual client restart

```sh
curl --fail-with-body --max-time 240 -X POST http://localhost:8080/api/failover
curl -sS http://localhost:8080/api/status
```

The API pauses traffic, drains HK to SG replication, stops `hk-to-sg`, restores translated SG consumer offsets, and durably marks SG as primary. It leaves both clients paused, still configured for HK. It does not change their bootstrap addresses, resume them, or restart containers. Both brokers and Connect workers remain running.

Then manually switch and restart the clients:

```sh
printf 'APP_KAFKA_BOOTSTRAP=k0y:9092\n' > .env
docker compose up -d --no-deps --force-recreate --wait \
  synthetic-producer synthetic-consumer
curl -X POST http://localhost:8081/resume
curl -X POST http://localhost:8082/resume
curl -sS http://localhost:8080/api/apps
```

Clients start paused after every process/container restart. `SG_ACTIVE` describes the controller's primary designation; verify actual application connectivity through `/api/apps`, which reports the configured bootstrap and connected Kafka cluster ID.

For an actual HK outage, use `/api/failover?force=true`. It uses the last durable checkpoint and may replay records or lose records that never replicated. A completed failover request is idempotent and never restarts the clients.

## Manual client controls

```sh
curl -X POST http://localhost:8081/pause
curl -X POST http://localhost:8082/pause
curl -X POST http://localhost:8081/resume
curl -X POST http://localhost:8082/resume
```

For independent experiments, put either `k0x:9092` or `k0y:9092` in `APP_KAFKA_BOOTSTRAP` in `.env`, then recreate and resume the containers as above. Both clients read the address at process startup. Direct manual controls do not update controller DR state or translate offsets. Switching back to HK alone is not a reconciled failback.

[MANUAL-REVERSAL.md](MANUAL-REVERSAL.md) contains the commands to stop/delete `hk-to-sg`, record SG offsets, clean its metadata, and create/start `sg-to-hk` at that boundary.

## Connect and Kafka commands

```sh
curl -sS http://localhost:8083/connectors
curl -sS http://localhost:8084/connectors
curl -sS http://localhost:8084/connector-plugins
curl -sS http://localhost:8084/connectors/hk-to-sg/status
curl -sS http://localhost:8084/connectors/hk-to-sg/config
curl -sS http://localhost:8084/connectors/hk-to-sg/offsets

# Shell access uses Docker exec; there is no SSH server.
docker compose exec k0x bash
# Inside the container:
/opt/kafka/bin/kafka-topics.sh --bootstrap-server k0x:19092 --list
exit

# Direct CLI commands; substitute k0x for k0y to inspect HK.
docker compose exec k0y /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server k0y:19092 --describe --topic appTopic
docker compose exec k0y /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server k0y:19092 --describe --group dr-consumer
docker compose exec k0y /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server k0y:19092 --topic appTopic --from-beginning \
  --max-messages 5 --timeout-ms 10000
```

## appTopic: one partition

Each event has a UUID key and an Avro payload using one of five registered schemas.
Decoded, an example event is:

```json
{"id":"example-uuid","seqNo":123,"createdAt":"2026-10-10T14:00:00Z","eventType":"ShipmentDispatched","shipmentId":"synthetic-123"}
```

The sole producer reserves each sequence number durably before sending. Numbers start at 1 and continue across client recreation and switching clusters. Failed or ambiguous sends can leave gaps; replay can duplicate records. The topic is appTopic with one partition.

The consumer uses group `dr-consumer`, processes records and commits synchronously. MirrorMaker's `MirrorSourceConnector` on SG's worker preserves topic names, keys, values and partitions through `IdentityReplicationPolicy`.

Confluent Schema Registry **8.1.1** runs independently on each site (HK REST port
8086, SG 8087). The producer registers five Avro subjects and cycles through their
event types. The consumer decodes the Confluent schema-ID frame and also accepts
retained legacy JSON events. Raw console-consumer output is binary for new events.
See [SCHEMA-REGISTRY.md](SCHEMA-REGISTRY.md) for schema subjects, Java CLI commands,
backup scope and full restore instructions. The manual `sg-to-hk` connector includes
schema topics and keeps registry modes local via a Java Connect transform. HK uses
its replicated `_schemas` in READONLY mode; SG is READWRITE. The controller's
original app-topic-only workflow still requires explicit schema backup/restore.

The controller captures committed event IDs every three seconds. Planned recovery pauses both clients, freezes a checkpoint, waits for the last source event in each partition to reach SG, stops the mirror, translates event IDs into SG offsets and restores the consumer group. It then persists **SG_ACTIVE** and leaves the clients paused for a manual restart against SG. Failed transitions remain paused and can be retried with the original force mode. Completed requests are idempotent.

The controller pauses this lab's two clients before switching, but does not fence arbitrary external producers: both brokers remain writable through their direct listeners. Keep the old clients stopped/paused before starting clients on the other cluster. Administration, Connect and AKHQ always use fixed cluster-specific addresses.

## Layout and persistence

- `common`: Kafka SDK helpers and atomic durable JSON writes.
- `apps`: producer/consumer REST service selected by `ROLE`.
- `controller`: checkpoints and one-way recovery preparation.
- `tools`: Java CLI for Connect waits, reverse-offset JSON and integration checks.
- `schema-backup`: Spring Boot ApplicationRunner CLI to back up, inspect and restore
  Schema Registry's compacted commit log, preserving schema IDs and logical state.
  Optional `restore --via-import --registry-url=URL` imports supported default-context
  schema state into a running, empty registry through REST; see SCHEMA-REGISTRY.md.
- `infra`: Connect and AKHQ configuration.
- `scripts`: short shell startup, reset and test commands.

Docker networks separate application Kafka listeners, replication, and HTTP administration. Both Kafka data volumes, controller checkpoints and producer sequence reservations persist across `docker compose down`. KRaft cluster IDs are fixed and distinct. Kafka/Connect internal topics use replication factor and minimum ISR 1.

| Controller API | Purpose |
| --- | --- |
| POST /api/bootstrap | Create topics/forward connector, validate direct client address, resume initial HK traffic |
| POST /api/checkpoint | Persist the current HK consumer checkpoint |
| POST /api/failover[?force=true] | Drain/checkpoint, restore SG offsets, mark SG primary; leave clients paused |
| GET /api/status | Durable phase, checkpoint and restored offsets |
| GET /api/apps | Connected cluster IDs, addresses, counters and sequence |
| GET /api/clusters | Independent Kafka cluster IDs |
| GET /api/records?cluster=k0x | Event IDs and offsets for lab assertions; also accepts k0y |

## Tests and reset

```sh
# Fresh HK-primary lab required for each scenario:
./scripts/test.sh planned
# Full suite, deleting this lab's data before each scenario:
./scripts/test-all.sh --yes-delete-data
# Java integration/admin checks run in the tools container:
./scripts/lab.sh akhq
# Isolated schema backup/restore integration test; does not perform failover:
./scripts/test-schema.sh
# Optional host build with Java 21 and Maven:
mvn verify
```

The integration harness invokes the failover API, asserts that clients remain paused at their old address, then explicitly performs the operator’s manual client restart. It checks independent clusters, replication, payload sequences across recreation, translated and advancing consumer offsets, checkpoint and sequence persistence across restarts, and both AKHQ cluster/worker connections. Reports go to `artifacts/`. See [VALIDATION.md](VALIDATION.md).

```sh
docker compose down                         # preserve data
./scripts/reset.sh --yes-delete-data         # erase lab volumes
printf 'APP_KAFKA_BOOTSTRAP=k0x:9092\n' > .env
./scripts/up.sh
```

Single-node Kafka is deliberately non-HA. Replication is asynchronous and recovery is at least once. Event-ID translation scans retained records and is intended only for this small lab. REST and Kafka use plaintext, with host ports bound to loopback. Host Kafka listeners advertise localhost. Automatic failback and production security are outside scope.
