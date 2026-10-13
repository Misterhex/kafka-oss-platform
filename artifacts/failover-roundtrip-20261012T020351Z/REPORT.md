# HK → SG → HK failover report

**Result: PASS.** HK was promoted to SG, both application clients were restarted against SG, traffic was observed for two minutes, and HK was promoted back. Both clients now use HK with zero errors.

Run: **2026-10-12T02:03:51Z → 2026-10-12T02:07:57.170278+00:00** (UTC).

## Promotion evidence

| Promotion | Completed phase | Source appTopic end | Destination appTopic end | Installed consumer next offset | Reverse replication |
| --- | --- | --- | --- | --- | --- |
| HK → SG | SG_ACTIVE | 82672 | 82672 | 82672 | RUNNING |
| SG → HK | HK_ACTIVE | 84009 | 84009 | 84009 | RUNNING |

Source/destination end offsets above are the frozen boundaries from each completed promotion, not live simultaneous lag measurements. Consumer restart positions are translated by event identity.

[01-sg-promotion.json](01-sg-promotion.json) · [07-hk-promotion.json](07-hk-promotion.json)

## Two-minute SG traffic window

Sample timestamps: **2026-10-12T02:04:46.689316+00:00 → 2026-10-12T02:06:46.689129+00:00**, spanning **120.000 seconds**. The timer ran for **123.555 seconds**, including final evidence capture.

Over the sampled interval, the producer completed **1,166** additional sends and the consumer processed **1,166** additional events. Both error counters stayed at **0** across all **9** observations.

Producer and consumer bootstrap: `k0y:9092`; connected cluster ID: `NDU2N0FCQ0RFRjEyMzQ1Ng`.

| Elapsed | UTC | Produced | Consumed | Producer sequence | Producer / consumer errors | HK log end | SG log end |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0s | 02:04:46 | 55 | 55 | 82807 | 0 / 0 | 82742 | 82760 |
| 15s | 02:05:01 | 200 | 200 | 82952 | 0 / 0 | 82886 | 82902 |
| 30s | 02:05:16 | 345 | 345 | 83097 | 0 / 0 | 83031 | 83048 |
| 45s | 02:05:31 | 491 | 491 | 83243 | 0 / 0 | 83177 | 83194 |
| 60s | 02:05:46 | 637 | 637 | 83389 | 0 / 0 | 83323 | 83340 |
| 75s | 02:06:01 | 782 | 782 | 83534 | 0 / 0 | 83468 | 83485 |
| 90s | 02:06:16 | 928 | 928 | 83680 | 0 / 0 | 83614 | 83631 |
| 105s | 02:06:31 | 1074 | 1074 | 83826 | 0 / 0 | 83761 | 83777 |
| 120s | 02:06:46 | 1221 | 1221 | 83973 | 0 / 0 | 83907 | 83923 |

HK and SG log ends were queried sequentially while traffic continued. Their small differences include sampling time and must not be read as a precise simultaneous replication lag. The completed drain and recent-record replication assertions provide the stronger completion evidence.

Snapshots: [04-sg-sample-00.json](04-sg-sample-00.json), [04-sg-sample-01.json](04-sg-sample-01.json), [04-sg-sample-02.json](04-sg-sample-02.json), [04-sg-sample-03.json](04-sg-sample-03.json), [04-sg-sample-04.json](04-sg-sample-04.json), [04-sg-sample-05.json](04-sg-sample-05.json), [04-sg-sample-06.json](04-sg-sample-06.json), [04-sg-sample-07.json](04-sg-sample-07.json), [04-sg-sample-08.json](04-sg-sample-08.json)

## Replication and fencing

- During SG-primary traffic, `sg-to-hk` and `sg-to-hk-checkpoints` ran on HK Connect. Both connectors and every task were RUNNING in all sampled snapshots.
- SG Connect had no remaining HK-to-SG connectors after promotion. After failback, HK Connect had no remaining SG-to-HK connectors, and the opposite pair ran on SG.
- Reverse replication starts at the saved destination next offsets (installed as MirrorSource last-processed offset `next - 1`). Promotion verifies the installed values before resuming the connector.
- The Java assertion tool confirmed recent primary event IDs appeared on the standby and performed actual producer-send and consumer-poll probes using application credentials. Both standby probes were rejected with authorization errors, in both directions.

[03-sg-traffic-replication-and-acl-assertions.log](03-sg-traffic-replication-and-acl-assertions.log) · [05-sg-end-traffic-replication-and-acl-assertions.log](05-sg-end-traffic-replication-and-acl-assertions.log) · [09-hk-traffic-replication-and-acl-assertions.log](09-hk-traffic-replication-and-acl-assertions.log)

### Latest decoded offset-sync records

**06-sg-to-hk-offset-syncs.log**

```text
OffsetSync{topicPartition=appTopic-0, upstreamOffset=83940, downstreamOffset=83940}
OffsetSync{topicPartition=appTopic-0, upstreamOffset=83950, downstreamOffset=83950}
OffsetSync{topicPartition=appTopic-0, upstreamOffset=83960, downstreamOffset=83960}
OffsetSync{topicPartition=appTopic-0, upstreamOffset=83970, downstreamOffset=83970}
OffsetSync{topicPartition=appTopic-0, upstreamOffset=83979, downstreamOffset=83979}
```

[06-sg-to-hk-offset-syncs.log](06-sg-to-hk-offset-syncs.log)

**11-hk-to-sg-offset-syncs.log**

```text
OffsetSync{topicPartition=appTopic-0, upstreamOffset=84064, downstreamOffset=84064}
OffsetSync{topicPartition=appTopic-0, upstreamOffset=84073, downstreamOffset=84073}
OffsetSync{topicPartition=appTopic-0, upstreamOffset=84083, downstreamOffset=84083}
OffsetSync{topicPartition=appTopic-0, upstreamOffset=84093, downstreamOffset=84093}
OffsetSync{topicPartition=appTopic-0, upstreamOffset=84102, downstreamOffset=84102}
```

[11-hk-to-sg-offset-syncs.log](11-hk-to-sg-offset-syncs.log)

## Schema Registry

All five schema definitions, IDs and versions matched between HK and SG in every snapshot. The active registry was READWRITE and the standby registry was READONLY.

| Subject | HK ID | SG ID | Version |
| --- | --- | --- | --- |
| appTopic-lab.events.CustomerUpdated | 5 | 5 | 1 |
| appTopic-lab.events.InventoryUpdated | 4 | 4 | 1 |
| appTopic-lab.events.OrderCreated | 1 | 1 | 1 |
| appTopic-lab.events.PaymentReceived | 2 | 2 | 1 |
| appTopic-lab.events.ShipmentDispatched | 3 | 3 | 1 |

The earlier ID-4/5 swap is not present; original mappings were restored before this run. Raw `_schemas` offsets can differ because registry MODE records stay local and are filtered from replication.

## Post-run record audit

Audited at **2026-10-12T02:21:49.072507323Z**. Independently scanned both retained topic prefixes, using the initial last-acknowledged event as the excluded baseline and the final last-acknowledged event as the included endpoint. The test interval contains **1,431 records per site**, HK offsets **82632–84062**.

- Missing event IDs on either site: **0**.
- Duplicate stored event IDs on either site: **0**. Frequencies were checked across the complete saved prefixes, not a map that silently removes duplicates.
- Duplicate sequence numbers in the audited events: **0**.
- Raw payload SHA-256 mismatches, timestamp mismatches, or header mismatches: **0**.

| Phase | Producer acknowledged sends | Retained records |
| --- | --- | --- |
| HK before SG promotion | 40 | 40 |
| SG primary | 1337 | 1337 |
| HK after failback | 54 | 54 |

There are **36 reserved sequence-number gaps**, in two blocks of 18 around fencing (82735–82752 and 84090–84107). The producer reserves a sequence before attempting a send; failed authorization attempts therefore leave gaps. Acknowledged-send counts reconcile with retained records in every phase, so these gaps are not evidence of lost acknowledged records.

**Consumer processing limitation:** No per-event consumer processing/commit ledger was retained. The SG consumer processed 1,337 events, matching its 1,337 unique topic records, and its final committed offset reached the end boundary. That supports no repeated processing during this run, but does not directly prove which IDs the consumer processed how many times. Likewise, producer acknowledgement evidence consists of counts and boundary IDs, not a complete per-event acknowledgement manifest. This is a bounded record comparison and count reconciliation, not a general exactly-once guarantee.

[audit-summary.json](audit-summary.json) · [audit-hk-events.json](audit-hk-events.json) · [audit-sg-events.json](audit-sg-events.json) · [audit.log](audit.log) · [audit-source.java](audit-source.java)

## Final HK traffic

| Client | Bootstrap | Connected cluster ID | Count since restart | Errors | Enabled |
| --- | --- | --- | --- | --- | --- |
| producer | k0x:9092 | MkU3OEVBNTcwNTJENDM2Qk | 54 | 0 | True |
| consumer | k0x:9092 | MkU3OEVBNTcwNTJENDM2Qk | 54 | 0 | True |

Final phase: **HK_ACTIVE**. Reverse replication: **RUNNING**. Controller error: `none`.

[10-final-hk.json](10-final-hk.json)

## Logs and expected transition errors

The controller fences the old application principals; it does not pause or restart application processes. The test harness explicitly restarted the clients against the promoted site. Old-site clients can therefore log authorization or READONLY errors during the handoff before restart. Those are expected fencing effects, not errors during the SG steady traffic window. Process counters reset on restart; the report compares counters within each process lifetime.

Pre-retarget client evidence: [01-sg-clients-before-retarget.json](01-sg-clients-before-retarget.json) · [07-hk-clients-before-retarget.json](07-hk-clients-before-retarget.json)

| Handoff | Old client bootstrap | Producer errors before restart | Consumer errors before restart |
| --- | --- | --- | --- |
| HK → SG | k0x:9092 | 16 | 16 |
| SG → HK | k0y:9092 | 16 | 16 |

The recorded last errors in both handoffs are `TopicAuthorizationException: Not authorized to access topics: [appTopic]`. New clients had zero errors after retargeting.

### SG replication log excerpt

```text
connect-k0x-1  | 2026-10-12T02:04:41.965619630Z [2026-10-12 02:04:41,965] INFO [sg-to-hk|task-0|offsets] WorkerSourceTask{id=sg-to-hk-0} Committing offsets for 10 acknowledged messages (org.apache.kafka.connect.runtime.WorkerSourceTask:241)
connect-k0x-1  | 2026-10-12T02:04:42.972855047Z [2026-10-12 02:04:42,971] INFO [sg-to-hk|task-0|offsets] WorkerSourceTask{id=sg-to-hk-0} Committing offsets for 9 acknowledged messages (org.apache.kafka.connect.runtime.WorkerSourceTask:241)
connect-k0x-1  | 2026-10-12T02:04:43.978504320Z [2026-10-12 02:04:43,978] INFO [sg-to-hk|task-0|offsets] WorkerSourceTask{id=sg-to-hk-0} Committing offsets for 9 acknowledged messages (org.apache.kafka.connect.runtime.WorkerSourceTask:241)
connect-k0x-1  | 2026-10-12T02:06:56.858910574Z [2026-10-12 02:06:56,858] INFO [sg-to-hk|task-0|offsets] WorkerSourceTask{id=sg-to-hk-0} Committing offsets for 10 acknowledged messages (org.apache.kafka.connect.runtime.WorkerSourceTask:241)
connect-k0x-1  | 2026-10-12T02:06:57.866908203Z [2026-10-12 02:06:57,866] INFO [sg-to-hk|task-0|offsets] WorkerSourceTask{id=sg-to-hk-0} Committing offsets for 10 acknowledged messages (org.apache.kafka.connect.runtime.WorkerSourceTask:241)
connect-k0x-1  | 2026-10-12T02:06:58.872727087Z [2026-10-12 02:06:58,872] INFO [sg-to-hk|task-0|offsets] WorkerSourceTask{id=sg-to-hk-0} Committing offsets for 10 acknowledged messages (org.apache.kafka.connect.runtime.WorkerSourceTask:241)
```

Structured SG observations: [sg-traffic-observations.jsonl](sg-traffic-observations.jsonl) · Orchestration output: [orchestration.log](orchestration.log)

- [dr-controller.log](dr-controller.log)
- [synthetic-producer.log](synthetic-producer.log)
- [synthetic-consumer.log](synthetic-consumer.log)
- [connect-k0x.log](connect-k0x.log)
- [connect-k0y.log](connect-k0y.log)
- [k0x.log](k0x.log)
- [k0y.log](k0y.log)
- [schema-registry-k0x.log](schema-registry-k0x.log)
- [schema-registry-k0y.log](schema-registry-k0y.log)

Broker, registry, Connect and controller logs contain the available timestamped Docker Compose output since run start. The producer/consumer log files contain the stdout of the final HK containers; earlier client containers were replaced during retargeting. SG application traffic is evidenced by the nine captured API snapshots, the structured observation log, and assertion outputs; the applications do not emit per-event stdout logs. `run.json` records command arguments, UTC times, exit codes, check results and evidence filenames. `manifest.sha256` identifies every report/evidence file.

[run.json](run.json)

