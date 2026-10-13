# Kafka DR lab

Two independent Kafka 4.1.1 single-node KRaft clusters: HK (`k0x`) and SG (`k0y`).
Java 21, Spring Boot 4.0.3 and the Apache Kafka SDK; no Spring Kafka.

## Start

Requires Docker Compose 2.20.2+, curl and roughly 6 GB RAM:

```sh
./scripts/up.sh
curl -fsS http://localhost:8080/api/status
curl -fsS http://localhost:8080/api/apps
```

| Service | Host address |
| --- | --- |
| AKHQ | http://localhost:8085 |
| HK / SG Connect | http://localhost:8083 / http://localhost:8084 |
| HK / SG Schema Registry | http://localhost:8086 / http://localhost:8087 |
| DR controller | http://localhost:8080 |
| Producer / consumer administration | http://localhost:8081 / http://localhost:8082 |
| HK / SG Kafka bootstrap | localhost:19092 / localhost:29092 |

All Kafka client listeners use SASL/PLAIN and StandardAuthorizer. These are fixed,
public lab credentials: `producer` / `producer-lab`, `consumer` / `consumer-lab`,
`mirror` / `mirror-lab`, and `platform` / `platform-lab`. Platform and mirror are
lab superusers; applications are governed by topic and consumer-group ACLs.
The plaintext controller listener is separate from client listeners. Host ports
are loopback-only. This is a lab configuration, not production credential management.

## Three-stage promotion

`Promotion.promote(current, target, stages)` calls exactly `drain`, `sync`, and
`flip`. The REST entry point is `POST /api/promote?target=k0y` (SG) or `target=k0x`
(HK). There is no retry engine, resumable workflow, automatic client restart, or
application pause/resume call in promotion. Temporal can wrap these stages later.

1. **Drain:** fence application principals on both sites. Set the source registry
   READONLY, allow accepted writes to settle, and capture source topic boundaries,
   raw committed offsets for every source consumer group and schema state.
   ACL fencing is not a graceful application flush;
   uncommitted processing may replay.
2. **Sync:** wait for final source event IDs and schema state on the destination.
   Stop and delete both forward MirrorSource and MirrorCheckpoint connectors on the
   destination worker. Save destination log-end offsets for `appTopic` and `_schemas`
   before enabling writes. Verify schema IDs in the running registry. Consumer-group
   offsets are neither translated nor applied to the destination.
3. **Flip:** set the destination registry READWRITE and grant application ACLs there.
   Keep the old site fenced. Set up reverse replication on the old site's Connect
   worker from the saved destination offsets. If the old site is unavailable, report
   `reverseStatus=PENDING`; promotion has completed. `POST /api/reverse` can perform
   that pending setup later. Keep the external application fence in place when the old
   site returns; reverse setup installs its DENY ACLs and READONLY registry mode before
   starting replication. There is no background retry.

```sh
curl --fail-with-body --max-time 300 -X POST \
  'http://localhost:8080/api/promote?target=k0y'
curl -fsS http://localhost:8080/api/status
```

The same sequence applies to an unreachable source, but a source ACL update and
fresh drain cannot be performed. After independently fencing the old site, explicitly
assert that fact and accept possible unreplicated data loss:

```sh
curl --fail-with-body --max-time 300 -X POST \
  'http://localhost:8080/api/promote?target=k0y&force=true&sourceFenced=true'
```

The controller requires its last source schema snapshot to match the destination.
Schema mismatches block promotion. MM2 checkpoints remain available for application
recovery, but the controller does not read or apply their consumer offsets.
`sourceFenced=true` is an operator assertion, not proof that an unreachable broker was
fenced. A network partition alone does not satisfy it. Status records skipped steps.
Reachable sources always undergo the full drain, even with `force=true`.

MirrorSource and MirrorCheckpoint run on the **destination** site. Connect config,
offset and status topics use that site's broker; `offset-syncs.topic.location=target`
is set on both connectors, and checkpoints also live on the destination. Only one
replication direction is active. `_schemas` replication preserves IDs and filters MODE
records so READONLY/READWRITE remains local. Do not manually remap schema IDs: existing
messages embed their writer's original ID.

Clients are managed by their application team. After promotion, manually restart them
with the new direct address; the controller does not do this:

```sh
printf 'APP_KAFKA_BOOTSTRAP=k0y:9092\n' > .env
docker compose up -d --no-deps --force-recreate --wait synthetic-producer synthetic-consumer
curl -fsS -X POST http://localhost:8081/resume
curl -fsS -X POST http://localhost:8082/resume
curl -fsS http://localhost:8080/api/apps
```

To promote HK again, call `/api/promote?target=k0x`, then restart clients against
`k0x:9092`. The exact same stages run in the opposite direction.

## Verification and persistence

```sh
mvn verify                 # Java 21 + Maven; live integration tests opt in
./scripts/test-dr.sh        # promotes SG and HK; restarts lab clients explicitly
```

The controller uses final event IDs only to verify replication has drained; it does
not translate consumer offsets. This scan is intended for the one-partition synthetic
lab. The source snapshot is saved atomically in `/state/state.json` with `sourceCluster`,
`sourceCapturedAt`, `sourceEnds` (topic LEOs), and `sourceGroupOffsets` (group →
`topic:partition` → raw committed next offset). `targetEnds` separately stores the
destination's reverse-replication boundaries. `/api/checkpoint` can capture the active
site's current state without a promotion; only the drain capture is fenced/stable.

Destination consumer commits remain unchanged. Applications must choose their own
restart position, using existing commits or an explicit recovery procedure. Existing
commits can replay records already processed on the former primary; a new group uses
its configured reset policy.

SG's reverse boundary is a **next offset**; MirrorSource stored offsets are last-processed offsets, so setup
installs `next - 1` (null for an empty partition) while the connector is STOPPED,
then resumes it. It checks that the saved boundaries remain within retained source
offsets and verifies the installed offsets first. Expired boundaries require a
standby reseed; they are not silently reset to earliest. Old recovery topics are
retained; shared Connect state is not deleted.

Kafka data, controller status and producer sequence numbers persist across
`docker compose down`. Inspect `/api/status` on any stage failure. This intentionally
simple controller does not resume interrupted promotions automatically. The previous
pause-based `/api/failover` and its old test harness are superseded by `/api/promote`
and `scripts/test-dr.sh`.

```sh
docker compose exec k0x /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server k0x:19092 --command-config /opt/lab/client.properties --list
```

See [SCHEMA-REGISTRY.md](SCHEMA-REGISTRY.md) for the raw-topic backup/restore CLI.
Backup itself never changes registry modes. [DR-VALIDATION.md](DR-VALIDATION.md)
describes the current controller checks. The older manual reversal and validation
notes describe the previous controller and are historical.
