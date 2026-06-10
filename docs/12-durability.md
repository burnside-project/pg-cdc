# Durability and crash semantics

## What pg-cdc guarantees

**At-least-once delivery.** Every event committed in the source database
will appear in the sink at least once, even across pg-cdc process
crashes. Events are not deduplicated within a flush window — see
"Duplicates" below for the recovery footprint.

## How it works

The durability boundary is the `confirmed_flush_lsn` of the Postgres
replication slot. Postgres retains WAL at and beyond this position; it
re-sends everything past it on reconnect.

The streamer advances `confirmed_flush_lsn` as follows:

1. WAL events flow into pg-cdc's in-memory buffer.
2. A flush writes per-table delta Parquet to the sink, then writes the
   manifest to the sink.
3. **After the manifest write succeeds**, the streamer reports the
   highest LSN of that batch back to the source via
   `Source.ConfirmFlushedLSN(lsn)`.
4. The next periodic standby status update (every 10s by default) sends
   that LSN to Postgres, which then advances `confirmed_flush_lsn`.

Crucially, the standby update reports the **flushed** position — never
the position Postgres has merely sent. Underreporting is safe (Postgres
keeps the WAL); overreporting causes Postgres to drop WAL pg-cdc still
needs after a crash.

## Crash scenarios

### Mid-flush crash

Process is killed (`kill -9`, OOM) after some delta files have been
uploaded but before `WriteManifest` completes:

- `confirmed_flush_lsn` was last advanced after the **previous** flush
  succeeded — it points at the LSN of that previous batch's last event
- On restart, Postgres re-sends every event since that LSN
- pg-cdc buffers the replayed events, computes `nextEpoch =
  max(LatestDeltaEpoch) + 1` (the manifest still reflects the last
  successful flush), and writes a new delta file
- Any partially-uploaded delta files from the crashed flush get
  overwritten by the replay-driven flush. S3 PutObject is atomic; the
  end state is consistent.

**No data loss.** Events committed in Postgres reach the sink eventually.

### Manifest write fails

Same outcome as a mid-flush crash:

- `ConfirmFlushedLSN` is not called (it's gated on manifest write success)
- `confirmed_flush_lsn` stays at the previous flush's position
- Next reconnect replays everything since that point

### Process restart with no crash (graceful shutdown)

On `SIGTERM`/`SIGINT`, the streamer drains the buffer with a 30s
timeout, calls `WriteManifest`, then `ConfirmFlushedLSN`. The standby
update sent during shutdown reports the freshly-flushed LSN, so the
next start sees no replay.

### Slot dropped

`pgcdc_slot_active` going to `0` is the alert signal. See
[`runbooks/disaster-recovery.md`](runbooks/disaster-recovery.md) §1.

## Duplicates

Replay can produce duplicate writes within the **flush window of the
crash**. Consumers should treat the sink as at-least-once and key off
the primary key + a recency field (e.g. `__op_ts` or
`LatestDeltaEpoch`) when exactly-once semantics matter.

For Iceberg sinks, snapshot-level idempotency is preserved: each flush
is a single Iceberg commit, and a re-flush appends a new snapshot
rather than mutating the previous one. Time-travel reads to tags
created before the crash see the pre-crash data unchanged.

## Latency vs. durability trade-off

The flush interval (`flush.interval_sec`, default 10s) sets both the
flush cadence and the **maximum replayable window** on crash. Tightening
flush_interval reduces replay-on-crash but increases sink-write
overhead and may produce small Parquet files. Operators tune to match
their freshness SLO and replay tolerance.

The standby-status interval (10s, hardcoded) sets the maximum delay
between a successful flush and Postgres learning about it. After a
flush, up to 10s of WAL Postgres could otherwise release stays around.
This is intentional — Postgres can't release WAL until pg-cdc says it's
flushed, and we'd rather hold extra WAL briefly than risk overreporting.

## What's not durable

- **Run artifacts** (`events.ndjson`, `state.json`) are advisory. They
  live on local disk for the daemon's lifetime; a restart begins fresh
  artifacts in a new run-id directory. Audit retention is via the
  `audit=true` slog stream, not run artifacts.
- **Compaction state** (in `internal/state/state.go`) is durable
  per-table but optional — compaction is idempotent over delta files.
- **Refs in `refs.json`** are written after the manifest. A crash
  between manifest write and refs write means the new flush is in the
  manifest but not yet in any ref. The next flush re-derives all refs
  from the current manifest, so consumers see a one-flush gap before
  catching up.

### CAS conflict semantics (PRs #132, #133, #137)

The manifest is persisted under `If-Match` CAS on sinks that support it (S3 today). Two designed-in writer pairings can collide:

1. **`pg-cdc compact` racing the streamer** — steady-state, ~1 conflict per hour. The streamer's CAS recovery in `persistManifest` re-reads the latest manifest, merges the concurrent writer's `BaseEpoch`/`RowCount` (compact's fields) plus `Schemas`/`LatestDeltaEpoch` (streamer's fields) using max-by-monotonic, and overwrites unconditionally. `pgcdc_manifest_conflicts_resolved_total` ticks once.
2. **`pg-cdc reconcile` racing the streamer** — operator-driven; rare. PR #133 wired Reconcile to seed its ETag from `ReadManifestWithETag` so the first write goes through `If-Match` (not `If-None-Match: *`) and extended the CAS-recovery merge to honor concurrent `Schemas` evolution.

When conflicts fire faster than ~1/hour — typically two `pg-cdc start` processes pointed at the same prefix — the **CAS conflict circuit breaker** (`flush.cas_breaker_threshold`, default 10; `flush.cas_breaker_window_sec`, default 60) trips the streamer fatal so the dual-writer surfaces as a hard exit rather than an endless overwrite-each-other livelock. Once tripped the breaker is sticky; an operator restart is the recovery path because a dual-writer needs diagnosis before resumption. See [`runbooks/manifest-conflicts.md`](runbooks/manifest-conflicts.md).

Compaction (PR #132) writes the manifest **before** deleting its old delta files — a per-table CAS commit gates the delete. A crash anywhere before that gate leaves the old base + deltas authoritative and the next compact replays from the same `BaseEpoch`. Pre-fix the loop persisted the manifest once at the end of all tables, so a per-table delete that completed before the trailing write silently lost data on crash.

## Verified scenarios

The unit chaos suite (`internal/chaos_test.go`) exercises the durability boundary against an in-memory fake sink with injected failures. Each test asserts the at-least-once contract holds without a real Postgres or sink.

| Scenario | Outcome | Test |
|---|---|---|
| Delta upload fails mid-flush | Flush errors; `ConfirmFlushedLSN` not called | `TestChaos_DeltaUploadFailureSkipsLSNConfirm` |
| Manifest write fails (deltas already on disk) | Flush errors; orphan deltas left for next flush to overwrite; `ConfirmFlushedLSN` not called | `TestChaos_ManifestWriteFailureSkipsLSNConfirm` |
| Retry after manifest-write failure | Second flush succeeds, reports correct LSN | `TestChaos_RetryAfterPartialFailureIsIdempotent` |
| `refs.json` write fails | Flush succeeds; durability path uncompromised; logged warning | `TestChaos_RefstoreWriteFailureIsNonFatal` |
| Context cancelled during manifest write | Flush errors; `ConfirmFlushedLSN` not called | `TestChaos_ContextCancellationMidFlushSkipsLSNConfirm` |

Out of scope for the unit suite (belongs in deployment smoke tests):

- Real Postgres replay after `pkill -9` — needs a live cluster; verify `confirmed_flush_lsn` matches the previous successful flush. See [`runbooks/disaster-recovery.md`](runbooks/disaster-recovery.md) §1.
- Iceberg-side failure injection — would require a fake Iceberg catalog. The streamer treats Iceberg append failures as non-fatal warnings, so at-least-once is unaffected by Iceberg faults.
- Slot loss recovery — covered by `pgcdc_slot_active` and the DR runbook.

## Related Prometheus metrics

| Metric | What it tells you |
|---|---|
| `pgcdc_replication_lag_seconds` | Approx seconds between Postgres tip and pg-cdc's confirmed flush position |
| `pgcdc_slot_active` | 1 when slot exists and is being consumed; 0 when slot loss happened |
| `pgcdc_sink_errors_total` | Sink upload failures — flushes that didn't reach the sink |

A spike in `pgcdc_sink_errors_total` without a corresponding spike in
`pgcdc_replication_lag_seconds` means flushes are failing but the slot
still believes them durable — investigate immediately.
