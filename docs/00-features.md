# pg-cdc — Features

Comprehensive index of what pg-cdc ships today. Outcome-first: each entry leads with what the feature *does* for an operator or consumer, followed by a checklist of the concrete sub-capabilities that back it up.

## What pg-cdc is

A single-binary PostgreSQL change-data-capture daemon that streams WAL into typed Parquet files on cloud storage. Pure Go, no CGO, no Kafka, no JVM. Produces an immutable, governed data zone that downstream consumers (Athena, Spark, Iceberg-aware engines, AI agents via the MCP server) read from without ever touching the source database.

## How to read this page

- **[x]** — shipped and on `main`. Linked to deeper docs where useful.
- **[ ]** — planned or in flight (rare on this page; see `docs/internal/plans/` for roadmap).
- **(commercial)** — present in this repo (the full edition) but **withheld from the public OSS mirror** at [`github.com/burnside-project/pg-cdc`](https://github.com/burnside-project/pg-cdc). There is **no in-binary edition gate**: the single Apache-2.0 binary built from this repo contains every feature, and the OSS/commercial boundary is enforced by *which packages are mirrored to the public repo* — not by build tags or a license check (the code has neither). The mirror holds back the governance / ACL / audit / break-glass plane as the commercial moat. The authoritative, current package-by-package split lives in [`internal/plans/oss-adoption.md`](internal/plans/oss-adoption.md); `commercial-edition.md` on the mirror is the customer-facing summary.

---

## 1. CDC Engine

### Postgres logical replication
Reads WAL from a publication + replication slot, decodes `pgoutput` v2 messages, emits per-row change events. The canonical Postgres replica pattern; no triggers, no application changes.
- [x] Publication + slot lifecycle managed by `pg-cdc init` / `teardown`
- [x] `pgoutput` v2 decoder for INSERT / UPDATE / DELETE
- [x] Replica-identity validation at init (fail-loud on `REPLICA IDENTITY NOTHING`)
- [x] Source LSN tracked in manifest as the consistent starting point

### Managed sources — AWS RDS & Aurora
First-class support for managed Postgres alongside self-managed, behind the same pgoutput decode path. The core engine is unchanged; managed behavior is capability-driven. Full guide: [`14-managed-postgres.md`](14-managed-postgres.md).
- [x] Runtime provider detection (`source.postgres.provider: auto|self-managed|rds|aurora`), logged at startup
- [x] `pg-cdc preflight` — readiness check (wal_level, replication privilege, CREATE-on-database, slot headroom, replica identity) with provider-specific remediation
- [x] Built-in RDS/Aurora IAM-token auth (`iam_auth: true`) — mints + rotates tokens per connect, no static password
- [x] Slot-safety guardrail (`guardrail.max_retained_wal_bytes`) — alarms before a lagging slot pins enough WAL to threaten the source DB
- [x] Idle-source heartbeat (`replication.heartbeat_interval_sec`) for Aurora Serverless v2
- [x] Ephemeral-RDS/Aurora Terraform validation harness with zero-dormant-cost backstops — standalone [`burnside-project-pg-cdc-testbed`](https://github.com/dataalgebra-engineering/burnside-project-pg-cdc-testbed)

### Initial consistent snapshot
Captures every active table's contents at a single LSN before streaming begins, so consumers see a clean base + replayable deltas.
- [x] `pg-cdc init` exports a snapshot via `CREATE_REPLICATION_SLOT`
- [x] Per-table COPY runs against `SET TRANSACTION SNAPSHOT` for consistency
- [x] Cross-table parallel snapshot via `init.parallel_workers` (default 4) — Phase 8 #4a
- [x] Iceberg `CreateTable` registered inline alongside parquet base files

### Streaming WAL → delta Parquet
Long-running daemon that consumes the replication slot, buffers events, and flushes typed Parquet per table on interval or row/byte count.
- [x] `pg-cdc start` daemon mode (one binary, no external scheduler)
- [x] Configurable flush triggers: `flush.interval_sec`, `flush.max_rows`, `flush.max_bytes`
- [x] Per-table parallel flush via `flush.parallel_workers` (default 4) — Phase 8 #3a
- [x] Per-table iceberg mutex serializes AppendRows / EvolveSchema / ExpireSnapshots
- [x] Detailed phase docs in [`04-streaming.md`](04-streaming.md)

### Compaction
Merges delta epochs into a new base snapshot, applies I/U/D semantics, expires soft-deletes on a configurable TTL.
- [x] `pg-cdc compact` subcommand (also wired to a systemd timer — see Operability)
- [x] Tombstone TTL (default 30 days, configurable)
- [x] Iceberg snapshot expiration (default retain 100 per table)
- [x] Manifest CAS-safe coexistence with the streaming daemon
- [x] Compaction details in [`05-compaction.md`](05-compaction.md)

### Schema evolution
Detects new columns in source tables and propagates the new schema to the manifest, Glue, and Iceberg without restart.
- [x] Per-flush detection of `ALTER TABLE ADD COLUMN` events
- [x] Background reconciler for drops that don't fire a flush trigger (60 s default)
- [x] `onSchemaEvolved` hook updates the catalog after the manifest write succeeds
- [x] `pg-cdc reconcile --table foo.bar --force` one-shot operator command

### Manifest format
Versioned JSON file at the root of the sink that describes every active table, its schema version, epoch markers, and source LSN. The single source of truth across all sinks.
- [x] Conditional writes (If-Match ETag CAS) on S3 (the only sink implementing `ConditionalManifestSink`; filesystem falls back to plain writes, GCS is an unimplemented stub)
- [x] Schema-merge CAS recovery handles concurrent writers (streamer + compact)
- [x] CAS-conflict circuit breaker fails fast on suspected dual-writer scenarios
- [x] Branch / tag / promote refs (`refs.json`) for git-shaped consumer workflows

---

## 2. Storage Sinks

### Filesystem sink
Writes parquet to a local directory. Useful for dev, on-prem, and air-gapped environments.
- [x] `storage.type: filesystem`
- [x] No external dependencies; just a writable path

### S3 sink
Writes to AWS S3 (or any S3-compatible store via custom endpoint — MinIO, R2, etc.).
- [x] `storage.type: s3` with `bucket` / `prefix` / `region` / `endpoint`
- [x] Multipart upload via aws-sdk-go-v2 transfer manager
- [x] Native AWS SDK retry middleware (configurable via `aws.max_retries`)
- [x] Sink-side errors increment `pgcdc_sink_errors_total` (alertable)

### GCS sink — _not yet implemented_
A GCS sink adapter is scaffolded but **not implemented**. The adapter under `internal/adapters/sink/gcs/` returns `gcs sink: not yet implemented` for every operation. Do not configure `storage.type: gcs` for a real deployment.
- [ ] `storage.type: gcs` — stub only (no ETA)
- [ ] Multipart + retry behavior (deferred until the adapter is built)

---

## 3. Catalog Integrations

### AWS Glue
Registers active tables in Glue so Athena, Spark, and other AWS-aware engines can query the parquet files directly.
- [x] `catalog.type: glue` with `database` + `region`
- [x] Registration runs at the end of `pg-cdc init` and after every successful schema evolution
- [x] Idempotent `pg-cdc catalog register` for re-syncing after manifest changes
- [x] Failures tracked via `pgcdc_catalog_register_failures_total` + `pgcdc_catalog_register_retries_total`

### Apache Iceberg
First-class Iceberg table writer — every flush produces an Iceberg snapshot; schema evolution and snapshot expiration handled in-tree.
- [x] `catalog.type: iceberg` with `warehouse` + `database`
- [x] Background snapshot expiration goroutine (configurable retain count, default 100)
- [x] Per-table mutex serializes AppendRows / EvolveSchema / ExpireSnapshots
- [x] `pgcdc_iceberg_snapshot_count` and `pgcdc_iceberg_snapshots_expired_total` metrics

---

## 4. Reliability & Hardening

### Credential re-resolution on reconnect
The pgxpool's `BeforeConnect` hook re-resolves `${VAR}` references in the connection URL on every physical connection, so short-lived secrets (RDS IAM 15-min token, Vault leases) don't require a daemon restart on expiry.
- [x] `pgxpool.Config.BeforeConnect` integration (Phase 8 #1)
- [x] Covers both pool connections and the dedicated replication socket
- [x] Operational caveats documented in [`08-authentication.md`](08-authentication.md)

### Byte-budgeted batches
A `flush.max_bytes` ceiling (default 128 MiB) sits alongside `max_rows` and the interval timer — protects against OOM under wide-row workloads (TOAST, large JSONB) where one or two rows can blow past the row counter.
- [x] Configurable via `flush.max_bytes` (Phase 8 #2)
- [x] Best-effort per-event byte accounting in the streaming hot path
- [x] Default sized below typical container limits while leaving steady-state on `max_rows` / interval

### Graceful shutdown / SIGTERM drain
On SIGTERM the streamer drains the in-flight event buffer to the sink before exit; no parquet partial files, no orphan epochs.
- [x] `signal.NotifyContext` integration in `pg-cdc start`
- [x] Drain uses a fresh `context.WithTimeout` so the sink can finish even after the streaming ctx fires
- [x] Configurable `flush.drain_timeout_sec` (default 30 s)
- [x] Validated by `deploy/test-harness/sigterm-loop.sh` (100-cycle harness)

### AWS SDK retry middleware
Native aws-sdk-go-v2 retry middleware wired on every AWS client (S3, Glue, Lake Formation, DynamoDB). Transient 5xx and throttling errors are retried with exponential backoff.
- [x] Configurable via `aws.max_retries` (default 5)
- [x] Shared `internal/awsutil` helper applies the policy to every client

### PG transient-error retry with backoff
Replication-side connection drops (TCP reset, network timeout, libpq transient) trigger a bounded exponential backoff before reconnecting to the slot.
- [x] 1 s → 30 s cap, 10 attempts default
- [x] Transient vs fatal classification in `internal/postgres/errors.go`
- [x] Exhausted retries return non-zero exit so systemd `Restart=on-failure` kicks in

### TOAST partial-row preservation
Postgres' logical replication uses `TupleDataType 'u'` to signal that a TOAST column was unchanged by an UPDATE. pg-cdc preserves the existing value instead of writing NULL — a silent data-corruption fix.
- [x] Decoder emits `pkg/parquet.UnchangedTOASTSentinel` for `'u'` columns
- [x] `MergeDeltas` in compact honors the sentinel by preserving the base value

### Manifest CAS recovery + circuit breaker
Concurrent writers (streamer + scheduled compact) coordinate via conditional writes on the manifest. On conflict, both retry with a Schemas-aware merge instead of blindly overwriting.
- [x] If-Match ETag CAS on S3 (filesystem falls back to plain writes; GCS is an unimplemented stub)
- [x] `Schemas` field merged by max version per table on conflict
- [x] `pgcdc_manifest_conflicts_resolved_total` counter tracks healthy conflict rate
- [x] Circuit breaker trips when N conflicts within W seconds — points at dual-writer misconfig (default 10/60)

### Backpressure observability
When the source-adapter bridge channel is full, the streamer surfaces the wait time so operators can spot a slow sink before it OOMs the daemon.
- [x] `pgcdc_event_channel_full_total` and `pgcdc_event_channel_blocked_seconds_total`
- [x] Channel cap warning log at 10 000 buffered events

---

## 5. Observability

### Prometheus metrics
The daemon exposes 29 metrics on `:9090/metrics` covering replication health, throughput, sink errors, buffer pressure, compaction, catalog registration, Iceberg snapshots, schema reconciliation, CAS conflicts, backpressure, and the MCP/REST audit surface.
- [x] Replication: `pgcdc_replication_lag_seconds`, `pgcdc_slot_active`
- [x] Slot safety (managed): `pgcdc_slot_retained_wal_bytes`, `pgcdc_slot_guardrail_breach`, `pgcdc_heartbeat_total`
- [x] Throughput: `pgcdc_events_per_sec`, `pgcdc_buffer_depth`, `pgcdc_flush_parallel_workers_used`
- [x] Sink: `pgcdc_sink_latency_seconds` (histogram), `pgcdc_sink_errors_total`
- [x] Source-side: `pgcdc_source_oldest_xact_age_seconds` (Phase 8 #5)
- [x] Compaction: `pgcdc_compact_duration_seconds`, `pgcdc_compact_staleness_seconds`
- [x] Catalog: `pgcdc_catalog_register_failures_total`, `pgcdc_catalog_register_retries_total`
- [x] Iceberg: `pgcdc_iceberg_snapshot_count`, `pgcdc_iceberg_snapshots_expired_total`
- [x] CAS coordination: `pgcdc_manifest_conflicts_resolved_total`
- [x] Backpressure: `pgcdc_event_channel_full_total`, `pgcdc_event_channel_blocked_seconds_total`
- [x] Schema reconcile: `pgcdc_reconcile_staleness_seconds`
- [x] ACL: `pgcdc_acl_drift_events_total`, `pgcdc_acl_sync_staleness_seconds`
- [x] Per-table progress: `pgcdc_table_latest_epoch`, `pgcdc_events_flushed_total`; refs retention: `pgcdc_refs_raw_tags_pruned_total`
- [x] MCP/REST audit surface: `pgcdc_audit_events_total`, `pgcdc_audit_duration_ms`, `pgcdc_audit_dropped_total`
- [x] Full table with alert shapes in [`observability/metrics.md`](observability/metrics.md)

### CloudWatch dashboard + alarms
Terraform-managed dashboard (11 panels) and alarm set per deployment. Parameterized by `deployment_name`; zero per-deployment customization required.
- [x] `deploy/terraform/governance/observability.tf` + `templates/dashboard.json.tpl`
- [x] 9 alarms covering replication lag, sink errors, compact staleness, ACL drift, buffer depth, reconcile staleness, source xact age, manifest conflicts, event-channel block

### Agent-consumable run artifacts
Per-run directory under `<state-dir>/runs/<run_id>/` containing structured JSON events, a state snapshot, run metadata, and a shutdown summary. Designed for LLM agents to crawl without bespoke parsers.
- [x] `run.json` — config + start/stop metadata
- [x] `events.ndjson` — structured event stream with stable schema version
- [x] `state.json` — minute-cadence snapshot of LSN, lag, per-table epoch, buffer depth, last error
- [x] `summary.md` — written on shutdown (event counts, schema evolutions, compacts, ACL syncs)
- [x] Schema doc in [`observability/event-schema.md`](observability/event-schema.md)

### RefreshLag timeout (Phase 9 #7)
Bounded `context.WithTimeout` on the periodic metric-refresh queries so a saturated pgxpool can't wedge the state-writer goroutine and freeze the gauges.
- [x] Default 10 s timeout, overridable via `runtimeState.refreshTimeout`
- [x] Stuck queries logged + abandoned; next tick retries
- [x] Validated live on `daedmonds4` — gauge now produces distinct values under load (pre-fix: frozen to 14 decimal places)

---

## 6. Governance & ACL (commercial)

### Layer-2 tag governance — (commercial)
Policy-as-code in the `governance:` block of `pg-cdc.yml`. Required-tag enforcement, pattern-matched overrides, column-level tags, emergency overrides with expiry.
- [x] Strict mode refuses writes for resources missing required tags
- [x] JSON Schema validation at config-load time (fail-loud on misconfig)
- [x] Tag values validated against Layer-1 LF-Tag taxonomy via `lakeformation:GetLFTag` before DynamoDB write

### DynamoDB ACL registry — (commercial)
Versioned classification store: one item per resource (`db.schema.table`), monotonic version, direct + resolved tags, audit fields (`last_intent_at`, `last_intent_by`, `last_intent_reason`).
- [x] `pg-cdc acl register | get | set | list | diff | sync` subcommand tree
- [x] Sync exit codes designed for CI: 0 = clean, 3 = drift healed, 2 = AWS error
- [x] `last_applied_epoch` marker written back to DDB after each sync

### AWS Lake Formation reconciliation — (commercial)
`pg-cdc acl diff` computes the add/remove plan vs. live LF tags; `pg-cdc acl sync` applies it through `AddLFTagsToResource` / `RemoveLFTagsFromResource`.
- [x] Idempotent re-runs (LF Add is value-idempotent)
- [x] Every plan item emits an audit event before apply

### Break-glass workflow — (commercial)
`.github/workflows/break-glass.yml` for time-boxed emergency grants. Input validation, GitHub Environment approval gate, automatic TTL revocation.
- [x] Aggressive CloudWatch audit trail (grant + revoke phases)
- [x] Per-deployment + break-glass log streams in `/pgcdc/audit` log group
- [x] OIDC-trusted IAM role with `lakeformation:Grant/RevokePermissions`

### Operator audit aggregation — (commercial)
Single CloudWatch log group across deployments with 365-day retention; per-deployment + break-glass streams; IAM write grants for the daemon role.
- [x] `deploy/terraform/governance/audit_logs.tf` (`/pgcdc/audit` log group)

---

## 7. Consumption Layer

> **OSS / commercial split:** the **MCP server** is the OSS consumption path. The **REST API server**, **manifest refs** (`refstore/` — branch/tag/promote), and the **rate limiter** (`ratelimit/`) are currently held back from the public mirror per [`internal/plans/oss-adoption.md`](internal/plans/oss-adoption.md) and are therefore **(commercial)**. As with everything on this page, they are fully present in this repo's binary — see the legend at the top for what "(commercial)" means.

### MCP server
First-class Model Context Protocol server so AI agents (Claude Desktop, Cursor, etc.) can query governed Parquet directly via DuckDB. Eight tools (`list_tables`, `describe_table`, `query`, `get_changes`, `get_manifest`, `get_freshness`, `get_snapshot`, `get_diff`) — see [`11-ai-agent-consumption.md`](11-ai-agent-consumption.md).
- [x] `pg-cdc serve --mcp` exposes the MCP endpoint
- [x] **Query-time Lake Formation enforcement** — every read resolves the caller's LF grants via `LFGrantChecker` (5-min cache); columns the role can't see never leave storage. Live enforcement, not manifest-based.
- [x] Manifest + refs reload on a configurable interval (default 30 s)
- [x] **Live ACL refresh** — `pg-cdc acl set` bumps `policy_version` and the new surface appears without a server restart (the refresh loop re-reads policy versions)
- [x] Freshness signal (`is_stale`) honors the configured SLO

### REST API server
HTTP surface for non-MCP consumers — same query semantics, same governance, different transport. Two endpoint families: the **data plane** ([`spec/data-plane-api.md`](spec/data-plane-api.md) — `/v1/tables*`, `/v1/query`, `/v1/manifest`, `/v1/diff`) and the **console/operational** surface ([`spec/console-api.md`](spec/console-api.md) — `/v1/status`, `/v1/catalog`, `/v1/audit/summary`, and SSE `/v1/stream/{status,events,audit}`). `/metrics` + `/healthz` always on.
- [x] Bearer-token auth (constant-time comparison) — required for non-loopback bind
- [x] Loopback bind by default; `--listen-addr` flag for explicit network exposure
- [x] Server timeouts + body cap + bounded handler middleware (1 MiB request body)
- [x] Rate-limiting (per principal) configurable
- [x] **Server-Sent Events** streams (`/v1/stream/{status,events,audit}`) for live console/dashboard updates without polling
- [x] **Glue catalog drift detection** — `/v1/catalog` joins the live Glue catalog against the manifest and flags per-table version drift (cached 30 s, serves stale-but-flagged on Glue errors)
- [x] **`--pprof <loopback-addr>`** exposes `/debug/pprof/` for profiling (loopback only; off by default)

### Manifest refs — branch / tag / promote
Git-shaped workflow for consumers: `staging` advances on every flush, `main` advances based on `promotion.mode` (auto or manual), immutable `raw@<ts>` tags every flush for time-travel.
- [x] `refs.json` written alongside the manifest after every flush
- [x] `pg-cdc promote` for manual mode operators
- [x] Tag-window time travel honored by Iceberg snapshot-retention math

---

## 8. Operability

### systemd integration
Production deployment runs as a systemd unit. Companion timers for scheduled work (compaction, soak test).
- [x] `deploy/systemd/pg-cdc.service` — main daemon (User=pgcdc, security-hardened)
- [x] `deploy/systemd/pg-cdc-compact.{service,timer}` — hourly compaction
- [x] `deploy/systemd/pg-cdc-e2e-soak.{service,timer}` — hourly soak test (Phase 9 #5)
- [x] Drop-in overrides documented for memory caps + GOMEMLIMIT (`systemctl edit pg-cdc`)

### Runbooks
On-call documentation for the recurring failure classes; symptom → alert → diagnosis → remediation → prevention shape.
- [x] daemon-oom
- [x] replication-slot-full
- [x] s3-5xx-spike
- [x] compact-backlog
- [x] acl-sync-drift — (commercial)
- [x] glue-api-throttling
- [x] postgres-version-upgrade
- [x] dependency-bump-broke-build
- [x] disaster-recovery
- [x] upgrade (zero-loss binary swap procedure)
- [x] reconcile-staleness
- [x] source-oldest-xact
- [x] manifest-conflicts
- [x] event-channel-blocked
- [x] All 14 under [`runbooks/`](runbooks/)

### SLOs
Replication lag P99 < 5 s, audit-object lag P99 < 10 min, zero silent data loss. Error budgets + incident classification.
- [x] [`slos.md`](slos.md)

### On-call playbook
Quick reference, triage flow, escalation tree, runbook index, key file paths.
- [x] [`on-call-playbook.md`](on-call-playbook.md)

---

## 9. Testing & Validation

### Integration suite (Docker)
Real Postgres + LocalStack S3 + fake Glue end-to-end pipeline test in CI.
- [x] `internal/integration/` with `//go:build integration` tag
- [x] `docker-compose.integration.yml` for local + CI runs
- [x] CI integration job in `.github/workflows/ci-cd.yml` (Postgres 16 service container)

### `e2e-soak.sh` — 5-minute end-to-end soak
Operator-runnable test that exercises the full PG → CDC → S3 → catalog pipeline against a live deployment. Eight phases, ~5 minute budget, hourly cron-friendly.
- [x] Pre-flight → INSERT / UPDATE / DELETE → drain → compact → schema-evo → validate → cleanup
- [x] Two-tier WARN/PASS for drain (handles standby-cadence tail without false-FAIL)
- [x] In-line `pg_stat_activity` diagnostic on drain stall
- [x] Schema-evolution arm — ALTER ADD COLUMN + Glue verify + DROP COLUMN cleanup
- [x] Compaction-cycle arm — runs `pg-cdc compact` mid-test, verifies staleness reset
- [x] Hourly cron via `pg-cdc-e2e-soak.timer`; alerts on `^FAIL:` in journal
- [x] Runbook at [`../deploy/test-harness/e2e-soak.md`](../deploy/test-harness/e2e-soak.md)

### Failure-injection harness
Scripts that simulate the recurring failure modes locally so runbooks can be rehearsed without breaking prod.
- [x] `inject-oom.sh`, `inject-slot-full.sh`, `inject-s3-failure.sh`
- [x] `sigterm-loop.sh` (100-cycle SIGTERM drain validation)
- [x] `load-gen.sh` (env-based auth, deterministic INSERT load)
- [x] All scripts support `--self-test` to validate prerequisites

---

## 10. Security & Authentication

### Authentication profiles
Four supported PG auth methods; all wire through the connection URL so secret management stays at the URL boundary.
- [x] Password (with `${VAR}` env-var substitution + re-resolution on reconnect)
- [x] mTLS (client cert + key)
- [x] AWS RDS IAM (short-lived token, refreshed via the BeforeConnect hook)
- [x] GCP Cloud SQL IAM (via the Cloud SQL Auth Proxy sidecar)
- [x] Operational caveats (env scoping, silent-empty footgun, CLI vs daemon env) documented in [`08-authentication.md`](08-authentication.md)

### Required Postgres privileges
Documented minimal grant set: `SELECT` on covered schemas, `REPLICATION` on the user, `SELECT` on `information_schema` privilege views for governance, optional `pg_read_all_stats` for full-fleet oldest-txn visibility.

### Compliance overlays — (commercial)
Pre-built deployment topologies that pass common audit profiles.
- [x] HIPAA deployment overlay (`security/hipaa-deployment.md`)
- [x] Least-privilege deployment posture (`security/least-privilege-deployment.md`)
- [x] B5b producer-side security audit on file ([`internal/audits/security-audit-b5b.md`](internal/audits/security-audit-b5b.md))

---

## Where to go next

- New to pg-cdc? Start at [`01-getting-started.md`](01-getting-started.md).
- Operating an existing deployment? [`07-operations.md`](07-operations.md) + [`runbooks/`](runbooks/).
- Looking for the OSS / commercial split? The authoritative, current boundary (package-by-package) is [`internal/plans/oss-adoption.md`](internal/plans/oss-adoption.md) in this repo; the customer-facing summary is `commercial-edition.md` on the public mirror. The split is repository-level — there is no in-binary edition gate.
- Planning work? [`internal/plans/production-readiness.md`](internal/plans/production-readiness.md) for the live roadmap.
