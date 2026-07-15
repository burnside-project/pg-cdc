# Metrics reference

Single source of truth for the Prometheus metrics pg-cdc exposes on
`/metrics`. Each row names the metric exactly as it appears on the wire,
plus the operational meaning, the typical alert shape, and the runbook
that owns the response. Definitions live in
[`internal/metrics/metrics.go`](../../internal/metrics/metrics.go).

If you're paging through alerts, start with the on-call playbook
([`docs/on-call-playbook.md`](../on-call-playbook.md)) — this doc is
the lookup, not the response.

## Replication health

| Metric | Type | Unit | What it indicates | Alert shape | Runbook |
|---|---|---|---|---|---|
| `pgcdc_slot_active` | Gauge | 0 / 1 | Replication slot exists AND a consumer (the streamer) is attached. `0` after a streamer crash that the supervisor hasn't restarted yet. | `== 0` for > 60s | [disaster-recovery.md §1](../runbooks/disaster-recovery.md) |
| `pgcdc_replication_lag_seconds` | Gauge | seconds | Approximate distance between the Postgres WAL tip and the streamer's confirmed flush position, expressed in seconds. Steady > 0 is normal under load; growth is the signal. | `> 60` and rising for 5 min | [replication-slot-full.md](../runbooks/replication-slot-full.md), [s3-5xx-spike.md](../runbooks/s3-5xx-spike.md) |
| `pgcdc_source_oldest_xact_age_seconds` | Gauge | seconds | Age of the oldest open client-backend transaction on the source. A long-held `BEGIN` pins `restart_lsn` even when pg-cdc is fully caught up — so this is what distinguishes "the streamer is behind" from "an application transaction is holding WAL hostage." `0` when none open. Visibility caveat: without `pg_read_all_stats`, the streamer only sees its own backends; grant the role for full-fleet visibility. | warn `> 300`, page `> 1800` | [source-oldest-xact.md](../runbooks/source-oldest-xact.md) |
| `pgcdc_slot_retained_wal_bytes` | Gauge | bytes | WAL the replication slot is pinning (lag in bytes). On managed Postgres this cannot be reclaimed at the filesystem, so growth threatens source storage. Compared to the configured guardrail ceiling. | trend; alert via the breach gauge below | [replication-slot-full.md](../runbooks/replication-slot-full.md) |
| `pgcdc_slot_guardrail_breach` | Gauge | 0 / 1 | `1` when retained WAL is at or above `guardrail.max_retained_wal_bytes`. The primary slot-safety alert on RDS/Aurora — a pinned slot can fill storage and take the DB down. Disabled (always `0`) when the guardrail ceiling is `0`. | `== 1` for > 1 min | [replication-slot-full.md](../runbooks/replication-slot-full.md) |
| `pgcdc_heartbeat_total` | Counter | messages | Heartbeat WAL messages emitted (`replication.heartbeat_interval_sec`) to keep an idle slot advancing on Aurora Serverless v2 / quiet sources. A flat counter while the heartbeat is enabled means emits are failing — check logs. | `rate == 0` while enabled | [replication-slot-full.md](../runbooks/replication-slot-full.md) |

## Throughput

| Metric | Type | Unit | What it indicates | Alert shape | Runbook |
|---|---|---|---|---|---|
| `pgcdc_events_per_sec` | Gauge | events/s | Rolling CDC event throughput, set per flush window. Useful for "is the streamer making progress at all?". *(Naming exception: keeps the `_sec` short form because dashboards and alerts already reference it; new metrics should use `_per_second`.)* | `== 0` for > 5 min under expected load | [on-call-playbook.md](../on-call-playbook.md) (daemon-lagging triage) |
| `pgcdc_buffer_depth` | Gauge | events | Current in-memory event buffer size. Climbs when the source produces faster than the sink consumes; sustained high values precede OOM. | `> 10000` for 1 min | [daemon-oom.md](../runbooks/daemon-oom.md) |

## Sink health

| Metric | Type | Unit | What it indicates | Alert shape | Runbook |
|---|---|---|---|---|---|
| `pgcdc_sink_latency_seconds` | Histogram | seconds | Per-operation latency for sink uploads (S3, GCS, filesystem). p99 spikes correlate with provider-side regressions. | p99 > 5s for 5 min | [s3-5xx-spike.md](../runbooks/s3-5xx-spike.md) |
| `pgcdc_sink_errors_total` | Counter | errors | Total sink upload failures. Each failure causes the flush to be retried; the durability contract guarantees no `confirmed_flush_lsn` advance until a flush succeeds. | `rate() > 0` for 5 min | [s3-5xx-spike.md](../runbooks/s3-5xx-spike.md) |

## Compaction

| Metric | Type | Unit | What it indicates | Alert shape | Runbook |
|---|---|---|---|---|---|
| `pgcdc_compact_duration_seconds` | Histogram | seconds | Wall-clock duration of a compaction run. Useful for capacity planning; sudden growth suggests delta backlog. | p99 > 1h | [compact-backlog.md](../runbooks/compact-backlog.md) |
| `pgcdc_compact_staleness_seconds` | Gauge | seconds | Time since the last successful compaction. Climbs when the compact timer fails to fire (oneshot service errors, systemd timer disabled) — the warning signal *before* deltas accumulate to a query-perf problem. | `> 86400` (1 day) | [compact-backlog.md](../runbooks/compact-backlog.md) |

## ACL drift

| Metric | Type | Unit | What it indicates | Alert shape | Runbook |
|---|---|---|---|---|---|
| `pgcdc_acl_drift_events_total` | Counter | sync ops | Total ACL sync operations performed. **The name is historical**: counts every sync run, not just drift detections. Steady rate is normal under the scheduled sync cadence; absence is the signal. | `rate() == 0` for > 2× sync interval | [acl-sync-drift.md](../runbooks/acl-sync-drift.md) |
| `pgcdc_acl_sync_staleness_seconds` | Gauge | seconds | Time since the last successful ACL sync. The cleaner signal for "is ACL drift being healed?" — climbs whether the sync timer fired or not. | `> 2× sync interval` | [acl-sync-drift.md](../runbooks/acl-sync-drift.md) |

## Glue catalog (Iceberg sink)

| Metric | Type | Unit | What it indicates | Alert shape | Runbook |
|---|---|---|---|---|---|
| `pgcdc_catalog_register_retries_total` | Counter | retries | Every transient retry against the Glue API, regardless of final outcome. Early signal of AWS API flakiness before it becomes a register-failure incident. | `rate() > 1/min` for 10 min | [glue-api-throttling.md](../runbooks/glue-api-throttling.md) |
| `pgcdc_catalog_register_failures_total` | Counter | failures | Catalog registrations that exhausted the retry budget. A nonzero rate means the manifest has advanced but Athena/Iceberg consumers still see the pre-evolution schema; operator runs `pg-cdc catalog register` to repair. | `rate() > 0` for 5 min | [glue-api-throttling.md](../runbooks/glue-api-throttling.md) |
| `pgcdc_iceberg_snapshot_count` | Gauge (per-table) | snapshots | Current Iceberg snapshots retained per active table after the background expiration ticker runs. Bounded by `catalog.snapshot_retain_count`. If the gauge keeps growing while you have expiration configured, expiration is failing. | `max > 2 × snapshot_retain_count` | (none yet) |
| `pgcdc_iceberg_snapshots_expired_total` | Counter (per-table) | snapshots | Snapshots removed by the streamer's background expiration ticker. Rising rate = expiration keeping up; flatlined while `iceberg_snapshot_count` rises = expiration failing. | `rate() == 0` while `iceberg_snapshot_count` rising | (none yet) |

## Schema reconciliation

| Metric | Type | Unit | What it indicates | Alert shape | Runbook |
|---|---|---|---|---|---|
| `pgcdc_reconcile_staleness_seconds` | Gauge | seconds | Seconds since the last successful schema-reconcile cycle. Should stay near zero under normal load. Sustained elevation means flush throughput is starving the periodic reconcile poll on the shared goroutine (Stage D pre-work H1). | `max > 3 × flush.schema_reconcile_interval_sec` for 3 min | [reconcile-staleness.md](../runbooks/reconcile-staleness.md) |

## Manifest CAS + backpressure

| Metric | Type | Unit | What it indicates | Alert shape | Runbook |
|---|---|---|---|---|---|
| `pgcdc_manifest_conflicts_resolved_total` | Counter | conflicts | Manifest CAS conflicts the streamer recovered from via merge-and-overwrite. Steady-state is ~1/hour (one per `pg-cdc compact` run). Sustained higher rate signals a second writer racing the prefix. The circuit breaker in PR #137 trips the streamer fatal after `flush.cas_breaker_threshold` conflicts in `flush.cas_breaker_window_sec`. | `> 5 in 1h window` | [manifest-conflicts.md](../runbooks/manifest-conflicts.md) |
| `pgcdc_event_channel_full_total` | Counter | events | Times the source→buffer channel was full when an event needed to be enqueued — i.e. the downstream flush loop is slower than the WAL is arriving. Sustained nonzero rate means the sink is backpressuring the replication session. | Companion to the seconds counter below — the rate gives "how often"; the seconds gives "how long". | [event-channel-blocked.md](../runbooks/event-channel-blocked.md) |
| `pgcdc_event_channel_blocked_seconds_total` | Counter | seconds | Cumulative time the source adapter spent waiting for room in the buffer channel. Rate approaching 1 s/s means permanent block and `wal_sender_timeout` is at risk. | `≥ 60 s cumulative` in any 5-min window | [event-channel-blocked.md](../runbooks/event-channel-blocked.md) |

## Flush internals

| Metric | Type | Unit | What it indicates | Alert shape | Runbook |
|---|---|---|---|---|---|
| `pgcdc_flush_parallel_workers_used` | Gauge | workers | Peak concurrent per-table workers observed during the last flush. Compare to `flush.parallel_workers` — equal value sustained means the cap is binding and raising it would help. Phase 8 #3a. | Informational (see dashboard) | [`docs/13-tuning-parallelism.md`](../13-tuning-parallelism.md) |
| `pgcdc_table_latest_epoch` | Gauge (per-table, label `table`) | epoch | Latest delta epoch flushed for the named table. Monotonic per table; a table whose epoch stops advancing while peers move is the signal of a per-table stall. | Informational | — |
| `pgcdc_events_flushed_total` | Counter (per-table, label `table`) | events | Total CDC events flushed per table since process start. Rate gives per-table throughput; a flat counter on a table that should be active is the signal. | Informational | — |
| `pgcdc_refs_raw_tags_pruned_total` | Counter | tags | `raw@<ts>` tags removed from `refs.json` by retention pruning (`refstore.retain_raw`). Steady with flush cadence once the retention window is full; `0` while `retain_raw: -1` (unbounded). | Informational | — |

## Audit (MCP / REST query surface)

These come from `internal/audit/` and cover the consumer-facing query surfaces (`pg-cdc serve`), not the streamer. They are the source of truth behind `GET /v1/audit/summary` and `GET /v1/stream/audit` (see [`spec/console-api.md`](../spec/console-api.md)).

| Metric | Type | Unit | What it indicates | Alert shape | Runbook |
|---|---|---|---|---|---|
| `pgcdc_audit_events_total` | Counter (labels `surface`, `endpoint`, `result`) | events | Every audited MCP tool call / REST handler invocation, bucketed by surface (`mcp`/`rest`), endpoint, and result (`ok`/`denied`/`error`). The `result="denied"` rate is the governance-blocking canary. | `rate(result="denied")` burst | [on-call-playbook.md](../on-call-playbook.md) |
| `pgcdc_audit_duration_ms` | Histogram (labels `surface`, `endpoint`) | ms | Audit-recorded request latency by surface and endpoint. p99 spikes flag slow consumer queries (large scans, cold S3). | p99 > 1s for 5 min | [on-call-playbook.md](../on-call-playbook.md) |
| `pgcdc_audit_dropped_total` | Counter | events | Audit events dropped because a `/v1/stream/audit` subscriber's channel was full (slow consumer / backpressure). Sustained nonzero means a dashboard is falling behind and missing events. | `rate() > 0` sustained | [on-call-playbook.md](../on-call-playbook.md) |

## Logging conventions

pg-cdc uses `log/slog` with structured (JSON) output. Levels are picked to make log queries operationally useful:

- `ERROR` — the streamer or a one-shot command failed at something it can't recover from on its own. Always actionable; every ERROR should map to a runbook signal.
- `WARN` — recovered automatically (retry succeeded) OR degraded mode (e.g., ACL adapter unavailable, falling back to permissive). Worth dashboarding rate but doesn't page.
- `INFO` — lifecycle events: streaming started, epoch flushed, snapshot complete, schema reconciled. The runbook log-query examples key off these.
- `DEBUG` — only useful when reproducing a specific bug; off in production by default.

Standard log queries operators use:

```bash
# Live structured logs from the systemd-managed daemon
journalctl -u pg-cdc --since "10 min ago" -f

# Errors only, parsed as JSON
journalctl -u pg-cdc --since "30 min ago" | jq 'select(.level == "ERROR")'

# Specific event categories
journalctl -u pg-cdc --since "10 min ago" | jq 'select(.msg == "epoch flushed")'
journalctl -u pg-cdc --since "10 min ago" | jq 'select(.msg | contains("catalog"))'
journalctl -u pg-cdc --since "10 min ago" | jq 'select(.error // empty | contains("Throttl"))'
```

## Known gaps

Still not exposed as Prometheus surface (tracked as Stage B follow-ups):

- **Per-route HTTP metrics** — the audit metrics above (`pgcdc_audit_events_total` / `pgcdc_audit_duration_ms`) already cover request count, result, and latency keyed by `(surface, endpoint)`. What remains a gap is transport-level HTTP detail (status-code histogram, in-flight request gauge, bytes served) for the contract endpoints — useful for distinguishing a slow handler from a slow client.
- **Flush counter** — `pgcdc_flushes_total` would give a per-flush count to pair with `pgcdc_sink_errors_total`'s rate. The existing `pgcdc_events_per_sec` answers "events flowing?" but not "flushes happening?".
- **Per-table schema reconciliation counter** — `pgcdc_schema_reconciles_total` (labelled by table) would surface how often the polling reconciler bumps SchemaVersion per table. (`pgcdc_reconcile_staleness_seconds` shipped as the global gauge in PR #138 — the per-table counter remains a gap.)

Shipped since the prior version of this doc and now covered in the tables above: `pgcdc_reconcile_staleness_seconds`, `pgcdc_iceberg_snapshot_count`, `pgcdc_iceberg_snapshots_expired_total`, `pgcdc_event_channel_full_total`, `pgcdc_event_channel_blocked_seconds_total`, `pgcdc_manifest_conflicts_resolved_total`, `pgcdc_flush_parallel_workers_used`, `pgcdc_source_oldest_xact_age_seconds`. Alarms wired in PR #138.
