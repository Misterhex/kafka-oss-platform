# Example controller state

`state.json` is an actual HK lab snapshot captured on 2026-10-13, committed unchanged for inspection. It is a historical example, not the current live cluster state or a failover input.

- `sourceCluster` / `sourceCapturedAt`: source and capture time.
- `sourceEnds`: log-end offsets (next offset) for every partition of the configured DR topics, currently `appTopic` and `_schemas`. This is **not an inventory of all Kafka topics**.
- `sourceGroupOffsets`: raw committed next offsets for every group returned by Kafka's group listing. These offsets are recorded without translation or application to another cluster.
- `targetEnds`: destination boundaries saved by an earlier promotion for reverse replication; their capture time differs from the source snapshot.
- `checkpoint.barriers` and `schemas`: replication checks; schema keys and values are Base64 encoded.
- `phase`, `reverseStatus`, and other flags: diagnostic values carried by the controller snapshot.

The controller writes this structure to `$STATE_DIR/state.json` (default `/state/state.json`) using `recordSourceState()` and `save()`. Source capture runs during drain after fencing and stable source log ends, or on an explicit `POST /api/checkpoint`. Topic and group reads are sequential, not an atomic Kafka-wide snapshot; manual capture while traffic is active can reflect different instants.
