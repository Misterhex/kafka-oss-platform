# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Docker Compose-based dual-cluster Kafka environment for local development/testing. Runs active-passive replication via MirrorMaker 2, with Schema Registry on both clusters.

## Architecture

- **kafka-primary** — Apache Kafka 4.0.0 (KRaft, combined broker+controller). Internal `9092`, host `localhost:29092`.
- **kafka-secondary** — Apache Kafka 4.0.0 (KRaft, combined broker+controller). Internal `9092`, host `localhost:29093`.
- **schema-registry** — Confluent Schema Registry 8.1.2, connected to primary cluster. Host `localhost:8081`.
- **schema-registry-secondary** — Confluent Schema Registry 8.1.2, connected to secondary cluster. Has `MODE_MUTABILITY=true` for import mode. Host `localhost:8082`. Depends on mirrormaker2 being healthy (needs `_schemas` topic replicated).
- **mirrormaker2** — One-way replication primary→secondary. Uses `IdentityReplicationPolicy` (identical topic names, no prefixing). Syncs consumer offsets, heartbeats, and checkpoints.
- **init-topics** — One-off container that creates the `sensor-readings` topic (3 partitions, RF=1) on kafka-primary at startup.
- **kafka-producer** — Spring Boot 3.4.4 / Java 21 app. Generates random Avro-serialized sensor readings to `sensor-readings` every 3 seconds.
- **kafka-consumer** — Spring Boot 3.4.4 / Java 21 app. Consumes from `sensor-readings` using Avro deserialization via Schema Registry.
- **kafka-ui** — Kafbat UI dashboard showing both clusters with their respective schema registries. Host `localhost:8080`.

All services share a `kafka-net` bridge network. Both brokers use KRaft (no ZooKeeper) with replication factor 1 for single-node operation.

### Service startup order

`kafka-primary` / `kafka-secondary` (parallel) → `schema-registry` / `mirrormaker2` → `schema-registry-secondary` → `kafka-ui`

## Commands

```bash
# Start all services
docker compose up -d

# Start all and rebuild
docker compose up -d --build

# Stop all services
docker compose down

# Stop and remove volumes (full reset)
docker compose down -v

# View logs
docker compose logs -f                    # all services
docker compose logs -f kafka-primary      # single service

# Check health
docker compose ps

# Kafka CLI tools (run inside container)
docker compose exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka-primary /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
docker compose exec kafka-secondary /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

# Schema Registry API
curl http://localhost:8081/subjects          # primary
curl http://localhost:8082/subjects          # secondary
curl http://localhost:8081/subjects/mysubject/versions
```

## Applications

Both `producer/` and `consumer/` are Gradle projects using Spring Boot 3.4.4, Java 21, kafka-clients 3.9.0, and Confluent Avro serializer 7.9.0.

- **Avro schema**: `SensorReading` — fields: `sensorId`, `location`, `temperature`, `humidity`, `timestamp`
- **Producer**: idempotent mode (`acks=all`, `enable.idempotence=true`), keys on `sensorId`
- **Consumer**: group `sensor-consumer-group`, offset reset `earliest`
- **Environment variables**: `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:29092`), `SCHEMA_REGISTRY_URL` (default `http://localhost:8081`)

```bash
# Build producer/consumer locally
cd producer && ./gradlew build
cd consumer && ./gradlew build

# Rebuild Docker images for producer/consumer only
docker compose up -d --build kafka-producer kafka-consumer
```

## Key Configuration

- MirrorMaker 2 config: `config/mm2.properties`
- Replication is primary→secondary only, replicating all topics (`.*`) including internal topics
- Consumer offset sync interval: 60s
- Heartbeat interval: 5s
- `IdentityReplicationPolicy` keeps topic names identical across clusters
- Schema replication deep-dive: `docs/schema-registry-replication.md`

## Ports

| Service                    | Host Port |
|----------------------------|-----------|
| kafka-primary              | 29092     |
| kafka-secondary            | 29093     |
| schema-registry (primary)  | 8081      |
| schema-registry (secondary)| 8082      |
| kafka-ui                   | 8080      |
