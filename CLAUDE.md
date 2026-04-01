# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Docker Compose-based dual-cluster Kafka environment for local development/testing. Runs active-passive replication via MirrorMaker 2, with Schema Registry on both clusters. Uses mTLS for all inter-service communication and CoreDNS for DNS-based failover simulation.

## Architecture

- **kafka-primary / kafka-secondary** — Apache Kafka 4.0.0 (KRaft, combined broker+controller). Both advertise `SSL://kafka.internal:9092` so Kafka clients route through CoreDNS.
- **schema-registry / schema-registry-secondary** — Confluent Schema Registry 7.9.0 backends behind nginx proxies (mTLS termination + write protection). Primary is READWRITE, secondary is READONLY.
- **coredns** — Serves `.internal` zone, resolving `kafka.internal` and `schema-registry.internal` to primary or secondary IPs. Enables DNS-based failover without app restarts.
- **mirrormaker2** — One-way replication primary→secondary using `IdentityReplicationPolicy` (identical topic names, no prefixing).
- **kafka-producer / kafka-consumer** — Spring Boot 3.4.4 / Java 21 apps. Connect via `kafka.internal:9092` and `schema-registry.internal:8081` (resolved by CoreDNS).
- **kafka-ui** — Kafbat UI dashboard at `localhost:8080`, monitors both clusters directly.

All services share a `kafka-net` bridge network (subnet `172.20.0.0/24`) with static IPs for key containers. Both brokers use KRaft (no ZooKeeper) with replication factor 1 for single-node operation.

### DNS and Networking

Producer and consumer use `dns: [172.20.0.2]` (CoreDNS) to resolve virtual hostnames. Infrastructure containers (SR backends, MirrorMaker, init-topics, kafka-ui) use `extra_hosts` to resolve `kafka.internal` since they need direct broker access, not DNS-based failover.

Static IPs: CoreDNS=`172.20.0.2`, kafka-primary=`172.20.0.10`, kafka-secondary=`172.20.0.11`, schema-registry=`172.20.0.20`, schema-registry-secondary=`172.20.0.21`.

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

# Regenerate mTLS certificates (required after changing SANs)
./scripts/generate-certs.sh

# DNS failover
./scripts/dns-failover.sh secondary    # flip to secondary cluster
./scripts/dns-failover.sh primary      # flip back to primary
./scripts/dns-failover.sh status       # show current DNS target

# View logs
docker compose logs -f                    # all services
docker compose logs -f kafka-primary      # single service

# Kafka CLI tools (requires SSL config — run inside container)
docker compose exec kafka-primary /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka-secondary /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

# Schema Registry API
curl http://localhost:8081/subjects          # primary
curl http://localhost:8082/subjects          # secondary

# Build producer/consumer locally
cd producer && ./gradlew build
cd consumer && ./gradlew build

# Rebuild Docker images for producer/consumer only
docker compose up -d --build kafka-producer kafka-consumer
```

## Applications

Both `producer/` and `consumer/` are Gradle projects using Spring Boot 3.4.4, Java 21, kafka-clients 3.9.0, and Confluent Avro serializer 7.9.0.

- **Avro schema**: `SensorReading` — fields: `sensorId`, `location`, `temperature`, `humidity`, `timestamp`
- **Producer**: idempotent mode (`acks=all`, `enable.idempotence=true`), keys on `sensorId`
- **Consumer**: group `sensor-consumer-group`, offset reset `earliest`
- **Environment variables**: `KAFKA_BOOTSTRAP_SERVERS` (default `kafka.internal:9092`), `SCHEMA_REGISTRY_URL` (default `https://schema-registry.internal:8081`)
- **JVM DNS cache**: Set to 5s via `JAVA_TOOL_OPTIONS="-Dnetworkaddress.cache.ttl=5"` for fast failover

## Key Configuration

- MirrorMaker 2 config: `config/mm2.properties`
- CoreDNS zone files: `config/dns/db.internal`, `db.internal.primary`, `db.internal.failover`
- Nginx proxy configs: `config/nginx-schema-registry.conf`, `config/nginx-schema-registry-secondary.conf`
- Certificate generation: `scripts/generate-certs.sh` — generates CA, broker/client keystores, truststore, and nginx PEM files into `certs/`
- Broker certs include SAN `DNS:kafka.internal`; SR certs include SAN `DNS:schema-registry.internal`
- Replication is primary→secondary only, replicating all topics (`.*`) including internal topics
- `IdentityReplicationPolicy` keeps topic names identical across clusters
- Failover deep-dive: `docs/failover-and-restore.md`
- Schema replication deep-dive: `docs/schema-registry-replication.md`

## Ports

| Service                    | Host Port |
|----------------------------|-----------|
| schema-registry (primary)  | 8081      |
| schema-registry (secondary)| 8082      |
| kafka-ui                   | 8080      |

Kafka brokers are not exposed to the host — apps connect within the Docker network via `kafka.internal:9092`.
