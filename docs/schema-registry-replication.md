# Schema Registry Replication via MirrorMaker 2

## Overview

This document covers how the secondary Schema Registry (`schema-registry-secondary`) stays in sync with the primary by reading a replicated `_schemas` topic, and why global schema IDs are preserved across clusters.

## Architecture

```
kafka-primary (9092)          kafka-secondary (9092)
       |                              |
  _schemas topic ---[MM2 replication]--> _schemas topic
       |                              |
schema-registry (8081)     schema-registry-secondary (8082)
   READWRITE                      READONLY
```

- MirrorMaker 2 replicates all topics (`.*`) from primary to secondary using `IdentityReplicationPolicy` (no topic name prefixing).
- The secondary Schema Registry reads the replicated `_schemas` topic and reconstructs its state from it.
- The secondary is set to READONLY mode to prevent accidental writes.

## Proof: Global Schema IDs Are Preserved

### How IDs are stored

Each message in the `_schemas` topic has:
- **Key** (`SchemaKey`): `{keytype, subject, version, magic}`
- **Value** (`SchemaValue`): `{subject, version, id, md5, schema, schemaType, references, metadata, ruleSet, deleted}`

The global schema ID is an **explicit field** (`id`) in the message value. It is not derived from Kafka offsets, internal counters, or any external state.

### How Schema Registry bootstraps

On startup, `KafkaStoreReaderThread` reads the `_schemas` topic **from the beginning** (`auto.offset.reset=earliest`). For every schema message it calls:

1. `IncrementalIdGenerator.schemaRegistered(key, value)` -- updates the max ID tracker via `Math.max(value.getId(), currentMax)`
2. `LookupCache.schemaRegistered(key, value)` -- populates the in-memory ID-to-schema mapping

`IncrementalIdGenerator.init()` is a **no-op**. All state is reconstructed purely from topic content. This means any Schema Registry instance reading the same `_schemas` messages builds the **identical** ID-to-schema mapping.

### Source code references

All under `core/src/main/java/io/confluent/kafka/schemaregistry/` in the [schema-registry repo](https://github.com/confluentinc/schema-registry):

| File | Role |
|------|------|
| `storage/KafkaSchemaRegistry.java` | Main registry: `register()`, `init()`, leader election |
| `storage/KafkaStore.java` | Reads/writes the `_schemas` topic |
| `storage/KafkaStoreReaderThread.java` | Consumer loop that replays from topic beginning on startup |
| `storage/KafkaStoreMessageHandler.java` | Processes messages, calls `idGenerator.schemaRegistered()` |
| `storage/SchemaValue.java` | Value format with explicit `id` field |
| `id/IncrementalIdGenerator.java` | Tracks `maxId` per context from topic data; `init()` is a no-op |

### Why MM2 byte-level replication is sufficient

MM2 copies messages byte-for-byte. Since Schema Registry reconstructs all state (including IDs) from the message content alone, a byte-level replica of `_schemas` produces an identical registry. Kafka offsets on the secondary cluster will differ from the primary -- this is irrelevant because SR uses message content, not offsets, to build its state.

## Confluent's Officially Documented Approaches

Confluent does **not** document MM2-based `_schemas` replication. Their recommended approaches are:

| Approach | Description | License |
|----------|-------------|---------|
| **Schema Linking** (CP 7.0+) | Purpose-built schema replication between registries. Secondary in IMPORT mode. | Commercial |
| **Confluent Replicator** | Replicates `_schemas` with schema awareness. Used with IMPORT mode on destination. | Commercial |
| **Shared primary** | All SR instances across DCs point to the same primary Kafka cluster's `_schemas` topic. Secondary instances have `leader.eligibility=false`. | Open source |

The MM2 approach used here is mechanically sound (confirmed by source code analysis) but is not officially supported by Confluent.

## Risks and Mitigations

### Critical: No writes on secondary

If schemas are registered on the secondary while MM2 is also replicating `_schemas` from the primary, schema ID collisions are almost certain. Both registries independently increment from their local `maxId`, and the same ID could be assigned to different schemas.

**Mitigation**: The secondary is set to READONLY mode after startup:

```bash
curl -X PUT -H "Content-Type: application/json" \
  --data '{"mode": "READONLY"}' \
  http://localhost:8082/mode
```

This must be re-applied after every restart (there is no environment variable for initial mode in Confluent SR 7.x/8.x).

### Medium: Replication lag

MM2 replicates `_schemas` and data topics independently. There is no ordering guarantee between them. A consumer on the secondary cluster may receive a data message referencing schema ID X before the corresponding `_schemas` record has been replicated. This causes a temporary deserialization failure until the schema arrives.

**Mitigation**: Accept brief deserialization failures during the lag window. For local dev/test, this is negligible.

### Low: Noop message interleaving

The secondary SR writes noop messages to its local `_schemas` topic for offset tracking and leader election. These are interleaved with replicated messages from MM2. Noops are processed as offset-only updates and are benign in practice.

### Low: Log compaction edge cases

The `_schemas` topic uses log compaction. If compaction runs on the secondary between MM2 replication batches, some records could theoretically be affected. In practice this is unlikely for a low-volume topic like `_schemas`.

## Verification

After bringing the stack up:

```bash
# Register a schema on primary
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}"}' \
  http://localhost:8081/subjects/test-value/versions

# Check it appears on secondary with the same ID
curl http://localhost:8081/subjects/test-value/versions/latest  # primary
curl http://localhost:8082/subjects/test-value/versions/latest  # secondary

# Confirm the "id" field matches in both responses

# Confirm secondary is READONLY
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"string\"}"}' \
  http://localhost:8082/subjects/should-fail/versions
# Expected: 42205 error "Subject should-fail in context is in read-only mode"
```

## Ports

| Service | Host Port |
|---------|-----------|
| schema-registry (primary) | 8081 |
| schema-registry-secondary | 8082 |
