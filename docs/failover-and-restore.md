# Failover and Restore Guide

## Overview

This document covers the failover and restore procedures for the dual-cluster Kafka setup. The architecture is **active-passive** with one-way replication (primary to secondary) via MirrorMaker 2.

Failover is implemented as a **DNS flip** using CoreDNS. Applications connect to virtual hostnames (`kafka.internal`, `schema-registry.internal`) which CoreDNS resolves to either the primary or secondary cluster. Switching clusters requires no application restarts -- only a zone file swap.

### Key design constraints

- **MirrorMaker 2** replicates one-way: `primary -> secondary` only.
- **IdentityReplicationPolicy** keeps topic names identical across clusters (no prefix). This simplifies failover but makes bidirectional replication unsafe (infinite loop risk).
- **Secondary Schema Registry** is read-only. The nginx proxy blocks POST on `/subjects/*/versions`, preventing new schema registration regardless of the Schema Registry's internal mode.
- **Data produced to secondary during failover does not replicate back to primary.** This is the fundamental tradeoff of active-passive with IdentityReplicationPolicy.

### Architecture during normal operation

```
                          CoreDNS
                     kafka.internal -> 172.20.0.10 (kafka-primary)
              schema-registry.internal -> 172.20.0.20 (schema-registry)

Producer --> kafka.internal:9092 --> kafka-primary --> MirrorMaker 2 --> kafka-secondary
                                         |                                    |
                              schema-registry.internal            schema-registry-secondary
                                    (READWRITE)                      (READ-ONLY via nginx)
                                         |
Consumer <-- kafka.internal:9092 <-- kafka-primary
```

### Architecture during failover

```
                          CoreDNS
                     kafka.internal -> 172.20.0.11 (kafka-secondary)
              schema-registry.internal -> 172.20.0.21 (schema-registry-secondary)

         kafka-primary                                          kafka-secondary
           (down or                                                  |
          unavailable)                                   schema-registry-secondary
                                                            (READ-ONLY via nginx)
                                                                     |
                                               Producer --> kafka.internal:9092
                                               Consumer <-- kafka.internal:9092
```

## Failover: Primary to Secondary

### When to failover

- Primary Kafka broker is down or unreachable.
- Primary Schema Registry is unavailable.
- Network partition isolating the primary cluster.

### Prerequisites

Before failover, verify:

1. Secondary cluster is healthy:
   ```bash
   docker compose ps kafka-secondary schema-registry-secondary
   ```
2. MirrorMaker 2 has been replicating (check that topics exist on secondary):
   ```bash
   docker compose exec kafka-secondary /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server localhost:9092 \
     --command-config /tmp/ssl.properties \
     --list
   ```

### Procedure

Run the DNS failover script:

```bash
./scripts/dns-failover.sh secondary
```

This swaps the CoreDNS zone file so that:
- `kafka.internal` resolves to `172.20.0.11` (kafka-secondary)
- `schema-registry.internal` resolves to `172.20.0.21` (schema-registry-secondary)

CoreDNS auto-reloads within 2 seconds. No container restarts are needed.

Check the current DNS target at any time:

```bash
./scripts/dns-failover.sh status
```

### What happens during failover

- CoreDNS zone file is swapped. The serial number increments, triggering CoreDNS to reload within 2 seconds.
- The JVM DNS cache TTL is set to 5 seconds (`-Dnetworkaddress.cache.ttl=5`), so applications re-resolve hostnames quickly.
- Kafka clients detect broken connections to the old broker and reconnect. The bootstrap address `kafka.internal:9092` now resolves to `kafka-secondary`, and the advertised listener (`SSL://kafka.internal:9092`) matches, so metadata responses route clients correctly.
- Consumer offsets on secondary were synced by MM2's checkpoint connector (sync interval: 60s). The consumer resumes from approximately where it left off.
- **Schema registration is blocked.** If the producer attempts to register a new or modified schema, it will fail with HTTP 403. Only schemas that already exist on the secondary (replicated from primary) can be used.

### Limitations during failover

| Limitation | Impact |
|-----------|--------|
| No new schema versions | Producer fails if schema changed since last replication |
| No data flows back to primary | Messages produced to secondary are not replicated to primary |
| Consumer offset gap | Up to 60s of offset drift (MM2 sync interval) |
| MM2 still running primary->secondary | Harmless if primary is down; resumes if primary recovers |

---

## Restore: Secondary Back to Primary

### Why you must restore

The secondary site is designed as a **temporary** failover target:

1. **Schema Registry is read-only** -- no schema evolution is possible, blocking any producer that needs to register a new schema version.
2. **Data is stranded** -- messages produced to secondary during failover exist only on secondary. They do not replicate back to primary.
3. **IdentityReplicationPolicy prevents reverse replication** -- enabling `secondary->primary` MM2 would create an infinite replication loop because topic names are identical on both clusters and MM2 cannot distinguish local vs replicated messages.

### Prerequisites

Before restoring, verify:

1. **Primary cluster is healthy:**
   ```bash
   docker compose ps kafka-primary schema-registry schema-registry-backend
   ```
   All three should show `healthy` status.

2. **Primary Schema Registry is writable:**
   ```bash
   docker compose exec schema-registry-backend \
     curl -s http://localhost:8081/mode
   ```
   Should return `{"mode":"READWRITE"}`.

3. **MirrorMaker 2 replication lag is zero** (primary->secondary is caught up):
   ```bash
   docker compose logs --tail 5 mirrormaker2
   ```
   No errors, heartbeats are flowing.

### Procedure

Flip DNS back to primary:

```bash
./scripts/dns-failover.sh primary
```

This swaps the CoreDNS zone file so that:
- `kafka.internal` resolves to `172.20.0.10` (kafka-primary)
- `schema-registry.internal` resolves to `172.20.0.20` (schema-registry)

Applications reconnect to the primary cluster automatically. No container restarts are needed.

### Verify

```bash
# Check current DNS target
./scripts/dns-failover.sh status

# Check producer is sending to primary
docker compose logs --tail 10 kafka-producer

# Check consumer is reading from primary
docker compose logs --tail 10 kafka-consumer

# Verify schema registry is writable
docker compose exec schema-registry-backend \
  curl -s http://localhost:8081/subjects
```

### Restore sequence summary

```
1. Verify primary cluster is healthy
2. Run ./scripts/dns-failover.sh primary
3. Verify producer/consumer reconnect to primary
```

> With DNS failover, restore is symmetric to failover -- a single command flips the DNS zone back.

---

## Data Implications

### What happens to data produced during failover

Messages produced to `kafka-secondary` during the failover window:

- **Are consumed by the consumer** running on secondary during failover -- this is the primary recovery path.
- **Remain on `kafka-secondary`** indefinitely (subject to retention policy, default 168 hours).
- **Are NOT replicated to `kafka-primary`** -- there is no reverse replication.
- **Are NOT consumed again** when the consumer switches back to primary -- the consumer starts from its last committed offset on primary.

### Potential data gap

```
Timeline:
  t0: Normal operation (primary). Consumer at offset 1000.
  t1: Primary fails. Last MM2 sync had consumer at offset 995 on secondary.
  t2: Failover. Consumer starts at ~995 on secondary, reprocesses 5 messages.
  t3: During failover, producer writes 100 messages to secondary (offsets 0-99).
  t4: Consumer on secondary processes all 100 messages.
  t5: Restore. Consumer reconnects to primary at offset 1000.
      The 100 messages from t3 exist only on secondary.
```

If those 100 messages have downstream side effects (database writes, API calls, etc.), they are captured by the consumer processing at t4. But the messages themselves are not on primary.

### If you need data to flow back to primary

The current architecture intentionally does not support this. The alternatives are:

| Approach | Tradeoff |
|----------|----------|
| **Switch to DefaultReplicationPolicy** | Topic names get prefixed (`primary.sensor-readings`). Bidirectional MM2 becomes safe. Apps need topic name translation logic. |
| **Manual export/import** | Use `kafka-console-consumer` to dump messages from secondary, replay to primary. Operationally heavy, not automated. |
| **Accept the gap** | Most active-passive setups accept this. The consumer processed the data during failover -- primary just won't have the raw messages. |

---

## Why Not Bidirectional Replication?

### The problem: IdentityReplicationPolicy cannot detect cycles

MirrorMaker 2 needs a way to distinguish between "this message was produced locally" and "this message was replicated from another cluster". Without that distinction, a message replicates endlessly between two clusters.

**DefaultReplicationPolicy** solves this with **topic name prefixing**. When MM2 replicates `sensor-readings` from primary to secondary, it creates `primary.sensor-readings` on secondary. The reverse direction creates `secondary.sensor-readings` on primary. MM2 knows never to replicate a topic that already carries a remote cluster prefix -- `primary.sensor-readings` on secondary is not replicated back because MM2 recognizes the `primary.` prefix as originating from the source.

Additionally, MM2 stamps each replicated record with a `source.cluster` header. Even if topic naming were ambiguous, this header provides a second layer of cycle detection.

**IdentityReplicationPolicy** strips both safeguards:

- Topic names are identical on both clusters (`sensor-readings` on both).
- There is no prefix to signal origin.
- The `source.cluster` header is still written, but IdentityReplicationPolicy does not use it for filtering.

This means MM2 has no way to tell whether a message on `sensor-readings` (secondary) was produced locally or replicated from primary.

### Step-by-step: how the infinite loop forms

```
                     kafka-primary                    kafka-secondary
                    ┌──────────────┐                 ┌──────────────┐
                    │              │   forward MM2   │              │
 1. Producer -----> │ sensor-readings ─────────────> │ sensor-readings │
                    │  offset 0: msg-A               │  offset 0: msg-A │
                    │              │                  │              │
                    │              │  reverse MM2     │              │
 3. Duplicated! <── │ sensor-readings <───────────── │ sensor-readings │
                    │  offset 0: msg-A               │  offset 0: msg-A │
                    │  offset 1: msg-A (duplicate)   │              │
                    │              │                  │              │
                    │              │   forward MM2    │              │
 5. And again... -> │              ─────────────────> │ sensor-readings │
                    │              │                  │  offset 0: msg-A │
                    │              │                  │  offset 1: msg-A │
                    └──────────────┘                  └──────────────┘

  This continues indefinitely. Each cycle adds another copy of every message.
```

Step by step:

1. Producer writes `msg-A` to `sensor-readings` on primary (offset 0).
2. Forward MM2 replicates `msg-A` to `sensor-readings` on secondary (offset 0).
3. Reverse MM2 reads `sensor-readings` on secondary. It sees `msg-A` but cannot tell it was replicated from primary (same topic name, no prefix check). It replicates `msg-A` back to primary (offset 1).
4. Forward MM2 reads `sensor-readings` on primary. It sees the new `msg-A` at offset 1. It cannot tell this is a duplicate. It replicates it to secondary (offset 1).
5. The cycle repeats. Each iteration doubles the message count.

The result is **exponential message growth** -- storage fills, consumer lag explodes, and both clusters eventually become unusable.

### Why DefaultReplicationPolicy breaks the cycle

With DefaultReplicationPolicy and bidirectional MM2:

```
                     kafka-primary                    kafka-secondary
                    ┌──────────────────────┐         ┌──────────────────────┐
                    │                      │         │                      │
 1. Producer -----> │ sensor-readings      │         │                      │
                    │                      │  fwd    │                      │
 2. Replicated ---> │                      ───────>  │ primary.sensor-readings │
                    │                      │         │                      │
 3. NOT replicated  │                      │  rev    │                      │
    (has "primary." │                      <───────  │ primary.sensor-readings │
     prefix, skip!) │                      │         │  ^^ MM2 sees "primary." │
                    │                      │         │     prefix, skips it   │
                    └──────────────────────┘         └──────────────────────┘
```

- Forward MM2 replicates `sensor-readings` to `primary.sensor-readings` on secondary.
- Reverse MM2 inspects `primary.sensor-readings` on secondary. The topic name starts with a remote cluster alias (`primary.`). MM2 skips it -- this topic is already a replica.
- No cycle forms. Each message exists exactly once on each cluster (under different topic names).

### Why this setup uses IdentityReplicationPolicy anyway

| Consideration | IdentityReplicationPolicy | DefaultReplicationPolicy |
|--------------|---------------------------|--------------------------|
| Topic names | Identical (`sensor-readings`) | Prefixed (`primary.sensor-readings`) |
| App changes for failover | None -- same topic name | Must update topic names or use alias mapping |
| Bidirectional safe | No | Yes |
| Failback data recovery | Not possible via MM2 | Possible -- reverse replication works |
| Operational complexity | Lower | Higher |
| Consumer offset sync | Straightforward | Requires topic name translation |

For this setup, the tradeoffs favor IdentityReplicationPolicy:

- **Failover is simple** -- DNS flip redirects applications to the secondary cluster with zero topic name changes.
- **Bidirectional replication is not needed** -- the secondary is designed as a temporary standby, not a permanent active site.
- **Data loss during failover is accepted** -- messages produced to secondary during failover are consumed there but not replicated back to primary.

### When to consider switching to DefaultReplicationPolicy

- You need data produced during failover to replicate back to primary.
- Your RTO for failback must be near-zero (bidirectional replication is always running).
- You are willing to accept topic name prefixes in your applications.
- You have multiple consumer applications that need to transparently consume from both clusters.

For this local dev/testing setup, active-passive with IdentityReplicationPolicy is the simpler and more appropriate choice.

---

## How the DNS Flip Works

### Components

- **CoreDNS** (`coredns/coredns:1.12.0`) serves the `.internal` zone from `/etc/coredns/db.internal`
- Zone file auto-reload every 2 seconds
- Non-`.internal` queries forwarded to Docker's embedded DNS (`127.0.0.11`)
- Producer and consumer use `dns: [172.20.0.2]` to resolve via CoreDNS
- Infrastructure containers use `extra_hosts` to resolve `kafka.internal` statically

### Static IP assignments

| Container | IP |
|-----------|-----|
| CoreDNS | 172.20.0.2 |
| kafka-primary | 172.20.0.10 |
| kafka-secondary | 172.20.0.11 |
| schema-registry (nginx) | 172.20.0.20 |
| schema-registry-secondary (nginx) | 172.20.0.21 |

### Zone files

| File | Purpose |
|------|---------|
| `config/dns/db.internal` | Active zone (CoreDNS reads this) |
| `config/dns/db.internal.primary` | Primary zone (restore source) |
| `config/dns/db.internal.failover` | Secondary zone (failover source) |

### Why advertised listeners matter

Both Kafka brokers advertise `SSL://kafka.internal:9092`. After initial bootstrap, Kafka clients use the advertised listener hostname for all subsequent connections. If the broker advertised its real hostname (e.g., `kafka-primary`), clients would bypass DNS and connect directly, defeating the DNS flip mechanism.

---

## Quick Reference

| Action | Command |
|--------|---------|
| Failover to secondary | `./scripts/dns-failover.sh secondary` |
| Restore to primary | `./scripts/dns-failover.sh primary` |
| Check current DNS target | `./scripts/dns-failover.sh status` |
| Check producer target | `docker compose logs --tail 5 kafka-producer \| grep "schema.registry.url"` |
| Check consumer target | `docker compose logs --tail 5 kafka-consumer \| grep "bootstrap.servers"` |
| Check MM2 health | `docker compose logs --tail 5 mirrormaker2` |
| Check SR mode (primary) | `curl http://localhost:8081/mode` |
| Check SR mode (secondary) | `curl http://localhost:8082/mode` |
| Check CoreDNS logs | `docker compose logs coredns` |
