# DR controller validation

`mvn verify` checks stage order in both directions, blocks flip after a sync failure,
rejects invalid targets, verifies reverse next-offset conversion (including empty
partitions), and confirms ACL fences affect application identities only. Existing
checkpoint tests cover identity-based offset translation and drain barriers.

`./scripts/test-dr.sh` executes actual SG promotion followed by HK promotion. The test
harness explicitly restarts/resumes application clients after each promotion; the
controller never invokes those endpoints. It asserts the primary and client bootstrap,
advancing event counts, zero client errors, recent event replication back to the standby,
standby DENY ACLs, actual rejected application reads/writes, removed forward
connectors and running reverse replication.

Unplanned promotion uses the same stages. Stop the old site in the lab (an external
fence), wait for surviving checkpoints, then invoke promotion with `force=true` and
`sourceFenced=true`. Missing external-fence assertion must block promotion. An
unreachable source skips its fresh boundary capture/drain and uses the destination's
MM2 checkpoints. Old-site reverse setup remains pending. No workflow retries are
implemented; failures preserve diagnostic status for an external orchestrator.

Schema backup remains a raw-topic tool and does not change registry modes. The
controller separately manages registry modes as part of promotion. The earlier ID-swap
experiment must be restored with original IDs before serving mirrored events.

## Executed against the managed lab

- HK → SG and SG → HK promotions completed with both clients processing and zero
  client errors after the application-team restart.
- Actual producer sends and consumer polls using application credentials on the
  standby were rejected with authorization errors in both directions.
- The destination worker had no remaining forward connectors; reverse source and
  checkpoint connectors ran on the new standby.
- With HK broker, Connect and registry stopped, SG promotion completed using SG's
  checkpoint topic. Both forward connectors were deleted on SG, skipped source
  steps were reported, and reverse setup was marked PENDING.
- SG clients processed with zero errors while HK remained stopped. After HK returned,
  `/api/reverse` completed and application ACL probes confirmed HK was fenced.
- Invalid/current-target promotion requests returned conflict without modifying the
  primary, stage, saved boundaries or reverse status.

The controller remains deliberately sequential and does not retry failed stages.

## Recorded round-trip evidence (2026-10-12)

The HK → SG → HK run includes a two-minute SG traffic window and a post-run
record audit:

- [Observations and report](artifacts/failover-roundtrip-20261012T020351Z/REPORT.md)
- [Record audit summary](artifacts/failover-roundtrip-20261012T020351Z/audit-summary.json)
- [Complete evidence and logs ZIP](artifacts/failover-roundtrip-20261012T020351Z.zip)

The audit matched 1,431 records on each site, with no missing or duplicate stored
event IDs and matching payloads. Producer acknowledged-send counts reconciled in
every phase. Consumer per-event processing was not logged, so the report does not
claim a formal exactly-once processing guarantee. The evidence manifest includes
SHA-256 checksums; generated artifacts from other runs remain ignored.
