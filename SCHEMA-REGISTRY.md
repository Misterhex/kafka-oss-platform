# Schema Registry and Java backup/restore

This lab pins `confluentinc/cp-schema-registry:8.1.1`. Each site has an independent
registry on its own Kafka cluster. Kafka remains Apache Kafka 4.1.1. All application
and backup code runs on Java 21 and uses the Kafka SDK directly; no Spring Kafka,
Confluent serializer library, or Python is used.

| Registry | Host REST URL | Docker REST URL | Persistent Kafka store |
| --- | --- | --- | --- |
| HK | http://localhost:8086 | http://schema-registry-k0x:8081 | k0x / `_schemas_restored` |
| SG | http://localhost:8087 | http://schema-registry-k0y:8081 | k0y / `_schemas` |
| Restore exercise | http://localhost:8088 | http://schema-registry-restore:8081 | k0y / configurable fresh topic |
| Integration fixture | http://localhost:8089 | http://schema-registry-fixture:8081 | k0x / `_schemas_fixture` |

The last two services use the `schema-test` profile and are normally stopped. Schema
Registry's durable state lives in Kafka; the existing Kafka volumes persist it.
AKHQ exposes each site's registry through its fixed cluster connection.

## Producer and consumer

Whenever the producer opens its Kafka client, it idempotently registers five Avro
schemas using Schema Registry's REST API. Subjects use topic-plus-record names:

- `appTopic-lab.events.OrderCreated` with `orderId`
- `appTopic-lab.events.PaymentReceived` with `paymentId`
- `appTopic-lab.events.ShipmentDispatched` with `shipmentId`
- `appTopic-lab.events.InventoryUpdated` with `sku`
- `appTopic-lab.events.CustomerUpdated` with `customerId`

All five contain `id`, durable `seqNo`, `createdAt`, and `eventType`. Events cycle
through these types by sequence number. Values use Confluent's wire format:
magic byte 0, four-byte big-endian schema ID, then Avro binary data. Kafka keys
remain the stable event UUID. The consumer fetches the writer schema by ID,
decodes and checks the event ID, then commits after processing. It also reads the
old plain JSON events retained from earlier lab runs.

The default registry follows `BOOTSTRAP`: k0x uses the HK registry and k0y uses SG.
`APP_SCHEMA_REGISTRY_URL` in `.env` optionally overrides this selection. A client
recreation is required after changing either address. `/status` reports the registry
URL and the producer's subject-to-ID map.

```bash
curl -fsS http://localhost:8081/status
curl -fsS http://localhost:8087/subjects
curl -fsS http://localhost:8087/subjects/appTopic-lab.events.OrderCreated/versions/1
```

MM2's ByteArrayConverters preserve the Avro frame and schema ID, but **MM2 does not
copy registry state**. Before moving consumers to another site's registry, restore
schemas with their original IDs. Independently registering the same five schemas
in a registry with other preexisting schemas can assign different IDs and decode
mirrored records incorrectly. The DR controller currently orchestrates Kafka only;
schema backup/restore remains an explicit administration step. The fresh-lab DR test
harness includes that step before failing over.

## Java CLI

`schema-backup` is a Spring Boot non-web application with an `ApplicationRunner`.
It exits when the command completes, and exits nonzero on failure. Build it and
run through Compose:

```bash
cd "/Users/yonghengtan/Documents/ChatGPT/Kafka platform"
docker compose --profile tools build schema-backup
./scripts/schema-backup.sh --help
```

Or run the same fat JAR locally:

```bash
mvn verify
java -jar schema-backup/target/schema-backup-1.0.0.jar --help
java -jar schema-backup/target/schema-backup-1.0.0.jar backup \
  --bootstrap=localhost:29092 --registry-url=http://localhost:8087 \
  --topic=_schemas --file=backups/sg.json
```

Host bootstrap ports are HK 19092 and SG 29092. Inside Docker they are respectively
`k0x:19092` and `k0y:19092`. `--kafka-properties=/path/client.properties` supplies
additional Kafka authentication/TLS properties for the host CLI; mount that file
when running it in Docker.

## Back up SG to disk

Supply `--registry-url` for an automatic READONLY window while backing up a running
registry. The URL must identify the registry using the supplied Kafka cluster/topic.
The CLI reads the previous global mode and all stored subject/context mode
overrides, sets writable scopes to READONLY, captures the Kafka topic, then restores
the previous modes in reverse order, including when capture fails. A previously
READWRITE source returns to READWRITE; an intentionally READONLY source stays so.
The backup contains READONLY modes, so a recovered destination starts READONLY.
Existing-schema producers and consumers can continue during the backup.

```bash
./scripts/schema-backup.sh backup \
  --bootstrap=k0y:19092 --registry-url=http://schema-registry-k0y:8081 \
  --topic=_schemas --file=/backups/sg.json --overwrite
```

The host file is `backups/sg.json`. Existing files are refused unless `--overwrite`
is supplied. The CLI scans a fixed boundary without committing a consumer group,
rejects a changing end offset, checks the single-partition compacted store, and
writes atomically with fsync and a SHA-256 checksum. On POSIX the backup is mode
0600. The JSON contains Base64 keys/values, null tombstones, record order, timestamps,
headers and source offsets. Source offsets are informational and are not recreated
in Kafka. Back up outside the Docker host as well for real disaster recovery.

Avoid concurrent administrative/config/mode changes or direct backing-topic
writers, and let in-flight schema changes finish before capture. The guard is not
an atomic transaction or an administrative fence; an end-offset change still
aborts capture. Without `--registry-url`, backup remains a generic raw-topic read
and the operator must quiesce writers (for example by stopping the registry).
If the CLI is killed or the registry becomes unreachable, mode cleanup cannot be
guaranteed: check source modes manually. Cleanup failures cause a nonzero exit even
when a backup file was written. The initial override-discovery scan can also fail
if registry writes are ongoing; retry after quiescing them. No REST authentication
options are provided in this lab.

Print the final logical state with decoded JSON keys/values:

```bash
./scripts/schema-backup.sh inspect --file=/backups/sg.json
```

## Restore into a brand-new registry

Stop **all destination registry instances** before restoring. The CLI's
`--registry-stopped` flag acknowledges that operator precondition; it cannot
independently fence registry instances. The destination commit-log topic must be
new/empty, with beginning and end offsets both zero. An existing empty REST subject
list is insufficient: a used store can contain configuration and deletion state.

This example uses an isolated destination, leaving HK and SG registries intact:

```bash
docker compose --profile schema-test stop schema-registry-restore
./scripts/schema-backup.sh restore \
  --bootstrap=k0y:19092 --topic=_schemas_sg_restored \
  --file=/backups/sg.json --registry-stopped
RESTORE_SCHEMA_TOPIC=_schemas_sg_restored docker compose --profile schema-test up \
  -d --no-deps --force-recreate --wait --wait-timeout 180 schema-registry-restore
curl -fsS http://localhost:8088/subjects
curl -fsS http://localhost:8088/subjects/appTopic-lab.events.OrderCreated/versions/1
```

Use another fresh topic name for a second restore exercise. Keep
`RESTORE_SCHEMA_TOPIC` set when recreating this service; add it to `.env` if the
exercise destination should persist across Compose commands. To restore HK's
actual registry, stop `schema-registry-k0x` and restore to `k0x:19092/_schemas_restored` only
when that topic is truly new/empty, then start its registry. The CLI never purges
an existing destination.

Restore validates the complete checksum and record sequence before writing,
creates a compacted one-partition topic if needed, writes with idempotence and
acks=all, detects interleaved destination writers and verifies final key/value
state. An interrupted restore is not resumed over a partial target: keep the
registry stopped and retry into a new empty topic. Reusing an already restored
store is deliberately rejected.

## Restore through the IMPORT REST API

Both restore methods accept the optional boolean flag `--allow-checksum-mismatch`.
By default a mismatched SHA-256 checksum aborts before destination writes. With the
flag, the CLI warns that file integrity is unverified and proceeds with the file's
current contents. Format, Base64, record ordering/boundaries, import-plan validation,
destination checks and post-restore verification remain enabled. This does not
repair corruption or bypass schema-ID collisions; omit the flag for normal backups.

Use `--via-import --registry-url=URL` to restore the **same backup file** through
Schema Registry's IMPORT API rather than producing to Kafka directly.
The destination registry must be **running**, configured with `mode.mutability=true`,
and freshly initialized with no schemas or subjects, including soft-deleted ones.
Quiesce all other registry writers throughout the operation. This path refuses
merges into populated registries; IMPORT cannot overwrite a conflicting ID.

Start the isolated registry with a **fresh unused** backing topic:

```bash
RESTORE_SCHEMA_TOPIC=_schemas_import_restored docker compose --profile schema-test up \
  -d --no-deps --force-recreate --wait --wait-timeout 180 schema-registry-restore
./scripts/schema-backup.sh restore --via-import \
  --registry-url=http://schema-registry-restore:8081 \
  --file=/backups/sg.json
curl -fsS 'http://localhost:8088/subjects?deleted=true'
```

For a local Java invocation, use `--registry-url=http://localhost:8088` instead.
Do not pass `--bootstrap`, `--topic`, `--registry-stopped`, `--replication-factor`
or `--kafka-properties` with `--via-import`: the running registry controls its own
Kafka connection and backing topic. The lab CLI uses unauthenticated HTTP;
Kafka authentication properties do not configure REST authentication.

The CLI verifies the checksum and builds the complete import plan before changing
anything, rejects unsupported state and missing/cyclic references, checks that the
destination is empty, enters global IMPORT mode, and registers referenced schemas
before dependents with their original **IDs and versions**. It restores retained
schema definitions, references, metadata/rules, supported global/subject configs
and modes, and soft deletions. It verifies schema fields, subjects and settings
through REST before printing `IMPORT RESTORE OK`.

NOOP records and hard-deleted history are not replayed. Kafka offsets, record
timestamps/headers, schema registration timestamps/GUIDs, and allocation history
are not preserved by this API path. A highest-ever ID belonging only to deleted
history is not reserved. Custom contexts and unknown stored key types/fields are
rejected before mutation; use Kafka replay for those and for full backing-store
state. Settings absent from the backup inherit destination deployment defaults.

On success, backed-up modes are restored; without a stored global mode the original
destination mode is restored. On failure after entering IMPORT, the CLI attempts
to leave the destination in IMPORT mode, reports a nonzero exit status, and does
not roll back partial changes. Keep writers quiesced and retry with a fresh
registry/backing topic. REST operations are not an atomic restore or writer fence.

Reference: [Confluent 8.1 IMPORT migration API](https://docs.confluent.io/platform/8.1/schema-registry/installation/migrate.html).

## What "full state" means

This is a **logical backup of the Schema Registry `_schemas` commit log**, matching
Confluent's documented backup approach:
https://docs.confluent.io/platform/8.1/schema-registry/installation/deployment.html#backup-and-restore

It preserves all retained records rather than only latest schemas: schema IDs,
subject versions, all stored schema types, references, contexts, metadata/rule sets,
global/context/subject compatibility and modes, soft-deletion flags and retained
hard-delete tombstones. Binary replay preserves references without re-registration
or ID remapping. READONLY modes in the backup remain READONLY after restore.

This is not a physical Kafka-log backup. It does not recover history already removed
by compaction or hard deletion, preserve original Kafka offsets/topic IDs/consumer
groups, or back up deployment configuration, authentication credentials, KMS keys,
licenses, separate DEK/exporter-state topics or runtime exporter cursors. Any
enterprise extension using additional backing topics needs those topics backed up
separately. Protect the file: metadata or extension records may contain sensitive
information. Restore to the same compatible registry version (8.1.1 here).

## Tests

```bash
# Unit tests and packaging; live test skipped unless requested.
mvn verify
# Live tests use ONLY the isolated fixture/restore registries and their topics.
./scripts/test-schema.sh
# IMPORT API test using its saved default-context fixtures and a temporary registry:
./scripts/test-schema-import.sh
# Read-only smoke test for the current SG-primary lab, after HK registry restoration:
mvn -pl schema-backup -am test -Dschema.runtime=true
```

The live test seeds five producer schemas plus shared IDs, reference dependencies,
multiple versions with a soft-deleted version, a deleted subject, a hard-deleted
subject, JSON Schema metadata, Protobuf, a custom context, compatibility settings,
and modes. It backs up the Kafka store and restores to a fresh store; after starting
the new registry it compares REST-visible state, decodes a pre-backup Avro event
and verifies allocation of a new schema ID. It also checks that a nonempty target
is refused. This test does not perform Kafka failover or delete `appTopic`.

The test deletes only `_schemas_fixture` and `_schemas_restore` to make itself
repeatable. It temporarily overrides RESTORE_SCHEMA_TOPIC to `_schemas_restore`.
It also reconfigures the exercise service to that topic; to return the service to
an earlier restored store, recreate it with that store's RESTORE_SCHEMA_TOPIC.
Stop test registries when done to release memory:

```bash
docker compose --profile schema-test stop schema-registry-fixture schema-registry-restore
```
