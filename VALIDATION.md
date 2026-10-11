# Validation — 2026-10-10

Current configuration: **appTopic, one partition**, direct cluster addresses, manual client restart.

- Java 21 Maven/Docker build and seven unit tests: PASS.
- HK-primary startup, appTopic with exactly one partition and RF 1 on both clusters, replication and AKHQ connectivity: PASS. Smoke report: artifacts/appTopic-smoke.json.
- Failover is intentionally not invoked after this change; the user will trigger it.

The earlier three-partition configuration passed planned and forced recovery, offset translation, sequence persistence and controller restart tests. Those historical results do not constitute an executed failover test of the new one-partition configuration.

`README.md` contains the failover API and manual restart commands. `MANUAL-REVERSAL.md` contains the connector cleanup and reverse-replication commands; those remain unexecuted.

## Schema Registry extension — 2026-10-11

- Pinned Confluent Schema Registry 8.1.1; verified its runtime is Java 21.
- Java 21 Maven reactor verification: 14 unit tests passed (2 Avro codec, 5 DR
  checkpoint, 2 lab CLI, 5 schema-log backup tests). Opt-in registry integration is
  skipped by the normal build and run explicitly below.
- `scripts/test-schema.sh` passed seed/restore and post-start REST verification.
  Tested five Avro event schemas, shared IDs, Avro references, multiple versions,
  soft-deleted version/subject, hard deletion, JSON metadata and a disabled CEL
  rule, Protobuf, a custom context, subject/global settings and modes. A restored
  registry decoded a previously serialized event, allocated a new ID beyond
  existing IDs, and rejected a second restore into a nonempty destination.
- Docker CLI backup/inspect/restore passed against the real SG registry. Backup
  is `backups/sg.json` (0600), with seven retained commit-log records and schema
  IDs 1–5. Isolated destination uses `_schemas_sg_restored`, REST port 8088.
- Initialized the newly added empty HK registry from this SG backup. Both site
  registries now contain the same five schemas/IDs. Read-only
  `LiveAvroReplicationTest` passed: all five event types sampled from SG arrived
  byte-for-byte on HK and decoded using HK's restored schema IDs.
- Producer and consumer were recreated on k0y and resumed. Both decoded/produced
  Avro records with zero errors after recreation; sequence reservations continued.
  SG→HK MirrorSourceConnector remained RUNNING with advancing offsets.
- AKHQ's SG schema endpoint returned all five schemas; both registry services are
  integrated with their fixed site connections.
- During image building the small Docker VM stopped HK's broker (exit 137). The
  broker was restarted without data deletion; the restore tests then passed.
  Maven build heap is now capped at 256 MiB and the fixture registry is stopped
  before starting the restore registry to reduce test memory pressure.
- No failover was performed. The old DR controller remains stopped because the
  current SG-primary state came from manual operations. Schema copying is an
  explicit step; the fresh-lab DR harness was updated but its destructive
  planned/forced scenarios were not rerun against this live manual recovery.

## IMPORT API restore extension — 2026-10-11

- Added `restore --via-import --registry-url=URL` using the existing checksummed
  backup format, with strict separation from Kafka replay options.
- Eight API unit tests passed, including reference ordering, subject-deletion
  markers, ID collision rejection, unsupported-context/state rejection,
  populated-target refusal before writes, and failure handling in IMPORT mode.
- The live `scripts/test-schema-import.sh` test passed against a dedicated
  Confluent Schema Registry 8.1.1 instance: 15 default-context schema versions,
  shared IDs, references, Avro/JSON/Protobuf, metadata/rules, soft deletions,
  compatibility settings and modes. A previously serialized Avro record decoded
  successfully using restored IDs; repeating restore into the populated target
  was refused without changing its READONLY mode.
- The temporary registry and its own backing topic were removed after testing.
  HK/SG site registries and their schema/application topics were not modified.
- API restore does not preserve raw log history, timestamps/GUIDs or custom
  contexts; unsupported backing-store state is rejected instead of silently lost.
- Added `--allow-checksum-mismatch` for both restore methods. Tests cover strict
  rejection without writes, explicit override through REST, retained structural
  validation despite override, and rejection of flag values/backup-command usage.
- The operator-requested IMPORT restore into HK completed successfully with all
  five SG schema versions before the subsequent checksum-flag change. HK is now
  populated; the CLI will refuse another IMPORT restore into that registry.
- Backup now accepts `--registry-url` to temporarily freeze global and all stored
  scoped mode overrides to READONLY, then restore prior values even on capture
  failure. Five unit tests cover successful restoration, failure cleanup,
  preserving an intentionally READONLY source, IMPORT override restoration, and
  mismatched override discovery before mode writes.
- The isolated live test confirmed a READWRITE source and subject override were
  READONLY inside the capture callback, all MODE keys in the saved backup were
  READONLY, then writable source scopes returned to READWRITE and an existing
  READONLY override remained unchanged. The dedicated test registry/topic were
  removed afterward; site registries were not involved in this live fixture test.
