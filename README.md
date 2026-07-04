

<h1 align="center">
  pg-cdc — A frustratingly simple way to create governed operational data from PostgreSQL for AI applications.
</h1>

<p align="center">
  <a href="https://aws.amazon.com/marketplace/pp/prodview-vpbhhtmdwzezy">
    <img src="assets/image.png" width="100%" alt="Available on AWS Marketplace">
  </a>
</p>



<p align="center">
  <strong>Build AI agents from PostgreSQL operational data — no production credentials.</strong>
</p>

<p align="center">
  <img src="assets/pg-cdc-logo.png" alt="AWS Marketplace"/>
</p>

<p align="center">
Build ai agent from Postgres Operational Data — no prod credentials.
</p>

<p align="center">
  <a href="https://github.com/burnside-project/pg-cdc/actions"><img src="https://img.shields.io/github/actions/workflow/status/burnside-project/pg-cdc/ci-cd.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/burnside-project/pg-cdc/releases"><img src="https://img.shields.io/github/v/release/burnside-project/pg-cdc?include_prereleases" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"></a>
  <a href="https://github.com/burnside-project/pg-cdc"><img src="https://img.shields.io/github/go-mod/go-version/burnside-project/pg-cdc" alt="Go"></a>
  <a href="https://github.com/burnside-project/pg-cdc/stargazers"><img src="https://img.shields.io/github/stars/burnside-project/pg-cdc?style=social" alt="Stars"></a>
</p>



**PostgreSQL change data capture → governed Apache Iceberg / Parquet on AWS S3 — built for AI agents.**

📅 <a href="https://github.com/burnside-project/pg-cdc/blob/main/assets/pg-cdc-flyer.png"> Book a Meeting</a>
<br>

<a href="https://aws.amazon.com/marketplace/pp/prodview-vpbhhtmdwzezy"> AWS MarketplaceListing</a>

### What pg-cdc IS:

- **One-way valve	WAL** - Iceberg/Parquet on S3. No return path to PG. Immutable, append-only, time-travelable.
- **Governance boundary** -	Every read gated by AWS Lake Formation tags + column-level ACL. No DB credentials ever reach consumers.
- **AI consumption layer** -	MCP server (8 tools) + REST API — purpose-built for agents, not for app queries.Schema evolves live	Detects ADD/DROP COLUMN, propagates to Glue + Iceberg without restart.
- **Crash-safe** -	confirmed_flush_lsn advances only after Parquet + Iceberg + manifest are durable. At-least-once.
- **Single binary** -	Go, no CGO, no PG extension, no sidecars. Works with any PG ≥10 (RDS, Aurora, self-managed).
- **No database credentials** — consumers authenticate via AWS IAM + Lake Formation, never a connection string.

### What pg-cdc is NOT:

- **Storage tiering** -	It doesn't keep hot data in PG. Everything leaves PG. pg-cdc snapshots data out of PG entirely.
- **Query engine** -	pg-cdc delegates the actual query to DuckDB
- **PG extension** -	It's an external Go process. No shared_preload_libraries, no C hooks, no PG restart.
- **Application data plane** -	Your app doesn't read through pg-cdc. Your app keeps hitting PG. pg-cdc feeds a separate governed zone for analytics / AI / compliance.
- **Bidirectional** -	No writes back to PG. No writes back to the Iceberg. Append-only.
- **General-purpose replication** -	It's not a standby, not a read replica, not a replicate-to-another-PG tool. It's WAL → columnar lake, full stop.


### Security Architecture

See [`docs/10-security-architecture.md`](docs/10-security-architecture.md) for the full five-property model and [`docs/11-ai-agent-consumption.md`](docs/11-ai-agent-consumption.md) for the two consumption paths.

---

## What makes it unique

A feature-by-feature catalog. The exhaustive, link-rich version lives in [`docs/00-features.md`](docs/00-features.md).

### 1. CDC engine — Postgres → typed columnar, no triggers

| Capability | What it does |
| --- | --- |
| **Logical replication** | Reads a publication + replication slot, decodes `pgoutput` v2 (I/U/D). No triggers, no app changes. |
| **Consistent initial snapshot** | `init` exports a snapshot at one LSN via `CREATE_REPLICATION_SLOT`; per-table `COPY` under `SET TRANSACTION SNAPSHOT`. Cross-table parallelism (`init.parallel_workers`). |
| **Streaming WAL → delta Parquet** | `start` daemon buffers events and flushes typed Parquet per table on interval / row / **byte** budget. Per-table parallel flush. |
| **Schema evolution** | Detects `ADD/DROP COLUMN`, propagates to manifest + Glue + Iceberg live — no restart. `reconcile --table` one-shot for hot tables. |
| **Compaction** | `compact` merges delta epochs into a new base, applies I/U/D, soft-deletes on a tombstone TTL. CAS-safe alongside the live streamer. |
| **TOAST partial-row preservation** | Honors `TupleDataType 'u'` — unchanged TOAST columns keep their value instead of silently becoming NULL. |
| **Versioned manifest** | A single JSON source of truth (active tables, schema versions, epoch markers, source LSN), CAS-committed on S3. |

### 2. Storage & formats — Iceberg-first on S3

| Capability | What it does |
| --- | --- |
| **Apache Iceberg writer** | `catalog.type: iceberg` — every flush is an Iceberg snapshot; in-tree schema evolution + background snapshot expiration (default retain 100/table). |
| **Hive/Parquet on Glue** | `catalog.type: glue` — register active tables so Athena / Spark / DuckDB read the Parquet directly. |
| **S3 sink** | Multipart upload (aws-sdk-go-v2 transfer manager), SDK retry middleware, S3-compatible endpoints (MinIO, R2). Configurable `bucket`/`prefix`/`region`/`endpoint`. |
| **Filesystem sink** | Zero-dependency local output for dev / on-prem / air-gapped. _(GCS sink is a scaffolded stub — not implemented.)_ |
| **Time-travel refs** | `refs.json`: `staging` advances every flush, `main` advances by `promotion.mode` (auto/manual), immutable `raw@<ts>` tags per flush. Git-shaped consumer workflow via `promote`. |

### 3. Governance for AI — the part that isn't replication

The differentiator. pg-cdc doesn't just land data — it makes it **default-deny governed** so it's safe to point an autonomous agent at.

| Capability | What it does |
| --- | --- |
| **AWS Glue catalog** | Tables registered at `init` and after every schema evolution; idempotent `catalog register` re-sync; drift metrics. |
| **Lake Formation tag enforcement** | Tag-policy grants gate reads. Untagged = invisible. **Column-level** tags supported. The bucket policy + IAMAllowedPrincipals removal make it load-bearing, not advisory. |
| **DynamoDB ACL intent registry** | Versioned classification per resource (`db.schema.table[.column]`) with tag inheritance (db→table→column), monotonic versions, and full audit provenance (`who / when / why`). |
| **`acl` reconciliation** | `acl register \| get \| set \| list \| diff \| sync` — diffs intent vs live LF and applies `AddLFTagsToResource` / `RemoveLFTagsFromResource`. CI-friendly exit codes (0 clean · 3 drift-healed · 2 AWS error). |
| **Query-time enforcement** | The serve layer resolves each caller's LF grants (`ListPermissions`, 5-min cache) — columns a role can't see **never leave storage**. Live, not manifest-based. |
| **Break-glass + audit** | Time-boxed emergency grants with TTL auto-revoke (GitHub Environment gate, OIDC role) and a 365-day CloudWatch audit log group. |

### 4. AI & agent consumption — MCP, REST, time travel

| Capability | What it does |
| --- | --- |
| **MCP server** (`serve --mcp`) | Model Context Protocol stdio server for Claude Desktop / Cursor / any MCP client. **8 tools:** `list_tables`, `describe_table`, `query`, `get_changes`, `get_manifest`, `get_freshness`, `get_snapshot`, `get_diff`. Governed reads via DuckDB over the Parquet. |
| **REST API** (`serve --http`) | Same query semantics + governance over HTTP. **Data plane:** `/v1/tables*`, `/v1/query`, `/v1/manifest`, `/v1/diff`. **Console/ops:** `/v1/status`, `/v1/catalog`, `/v1/audit/summary`, SSE `/v1/stream/{status,events,audit}`. Bearer auth, body caps, per-principal rate limiting. |
| **Freshness & snapshot signals** | `get_freshness` (`is_stale`, lag, schema/policy versions) and `get_snapshot` (Iceberg snapshot_id / Parquet base path) so agents decide whether to re-pull and read object storage with their own identity — pg-cdc doesn't proxy bytes. |
| **Contract diff** | `get_diff` / `/v1/diff` classifies ref-to-ref change as `safe` / `schema_change` / `policy_change` / `breaking` with a `promote_safe` gate. |
| **Ad-hoc query CLI** | `pg-cdc query` — project, filter (`col:op:value`), limit, role-scope, `--changes-since <epoch>` for the CDC changelog. |
| **Live policy refresh** | `acl set` bumps `policy_version`; the new access surface appears with no server restart. |

### 5. Managed Postgres — RDS & Aurora, first-class

| Capability | What it does |
| --- | --- |
| **Provider detection** | `source.postgres.provider: auto\|self-managed\|rds\|aurora`, logged at startup; same pgoutput path for all three. |
| **`preflight`** | Readiness check (wal_level, replication privilege, CREATE-on-db, slot headroom, replica identity) with provider-specific remediation. |
| **IAM-token auth** | `iam_auth: true` mints + rotates short-lived RDS/Aurora tokens per connect — no static password — via a `BeforeConnect` re-resolution hook. |
| **Slot-safety guardrail** | `guardrail.max_retained_wal_bytes` alarms before a lagging slot pins enough WAL to threaten the source. |
| **Idle heartbeat** | `replication.heartbeat_interval_sec` keeps an idle slot advancing on Aurora Serverless v2. |
| **Ephemeral test bed** | Zero-dormant-cost RDS/Aurora validation harness — standalone [`burnside-project-pg-cdc-testbed`](https://github.com/dataalgebra-engineering/burnside-project-pg-cdc-testbed). |

### 6. Reliability & durability — at-least-once, crash-safe

| Capability | What it does |
| --- | --- |
| **At-least-once delivery** | The slot's `confirmed_flush_lsn` advances **only after** Parquet + Iceberg + manifest are durable. A crash replays from the last durable flush — no silent data loss. ([`docs/12-durability.md`](docs/12-durability.md)) |
| **Manifest CAS + circuit breaker** | If-Match ETag CAS on S3; schema-aware merge on conflict; a breaker trips on suspected dual-writers (default 10 conflicts / 60 s). |
| **Graceful SIGTERM drain** | Drains the in-flight buffer to the sink (fresh timeout) before exit — no partial Parquet, no orphan epochs. |
| **Byte-budgeted batches** | `flush.max_bytes` (default 128 MiB) guards against OOM on wide-row / large-JSONB workloads. |
| **Backpressure visibility** | Channel-full counters + block-seconds surface a slow sink before it OOMs the daemon. |
| **Transient-error retry** | Bounded exponential backoff on PG reconnect (1 s→30 s, 10 attempts) and native AWS SDK retry on every client. |

### 7. Observability — 29 metrics, streams, agent-readable artifacts

| Capability | What it does |
| --- | --- |
| **Prometheus metrics** | **29** `pgcdc_*` metrics on `:9090/metrics` — replication lag, slot safety, throughput, sink latency/errors, compaction, catalog, Iceberg snapshots, CAS conflicts, backpressure, ACL drift, audit. ([`docs/metrics.md`](docs/metrics.md)) |
| **CloudWatch dashboard + alarms** | Terraform-managed 11-panel dashboard + 9 alarms, parameterized by `deployment_name`. |
| **SSE event streams** | `/v1/stream/{status,events,audit}` for live consoles/dashboards without polling. |
| **Agent-consumable run artifacts** | Per-run `run.json` · `events.ndjson` (stable schema) · `state.json` (minute-cadence LSN/lag/epoch) · `summary.md` — designed for an LLM to crawl. |

### 8. Security & operability

| Capability | What it does |
| --- | --- |
| **4 auth methods** | Password (`${VAR}` substitution, re-resolved on reconnect), mTLS, AWS RDS IAM, GCP Cloud SQL IAM. ([`docs/08-authentication.md`](docs/08-authentication.md)) |
| **Least-privilege posture** | Documented minimal grants; default-deny LF + S3 bucket policy; HIPAA overlay; on-file producer-side security audit. |
| **systemd integration** | Hardened `pg-cdc.service` + compaction/soak timers; documented memory-cap drop-ins. |
| **14 runbooks + SLOs** | Symptom→diagnosis→remediation runbooks, SLO definitions, on-call playbook. ([`docs/runbooks/`](docs/runbooks/)) |
| **Validation harness** | Dockerized integration suite, a 5-minute `e2e-soak.sh`, and failure-injection scripts (OOM, slot-full, S3 5xx, SIGTERM-loop). |

> **OSS / commercial:** the single Apache-2.0 binary built from this repo contains every feature above — there is **no in-binary edition gate**. The public OSS mirror at [`github.com/burnside-project/pg-cdc`](https://github.com/burnside-project/pg-cdc) holds back the governance / ACL / audit / REST / refs plane at the *repository* level. Authoritative split: [`docs/internal/plans/oss-adoption.md`](docs/internal/plans/oss-adoption.md).

---

## Commands

```bash
# Core CDC  (every command accepts --config <path>, default pg-cdc.yml)
pg-cdc preflight                  # Verify the source is ready for logical replication (managed-aware)
pg-cdc init                       # Snapshot tables → base Iceberg/Parquet + manifest + catalog + ACL registration
pg-cdc start                      # Stream WAL → delta epochs (the long-running daemon)
pg-cdc compact                    # Merge deltas → new base (applies I/U/D, soft-deletes on tombstone TTL)
pg-cdc status                     # Health: slot, lag, LSN, epochs, tables, oldest source txn
pg-cdc discover [--acl] [--dry-run]      # List tables (+ role→table→column access map with --acl)
pg-cdc reconcile [--table <q>] --force   # One-shot schema reconcile after an ALTER on a hot table
pg-cdc teardown                   # Drop slot + publication + storage + state

# Servers  (full endpoint list: pg-cdc serve --help)
pg-cdc serve --mcp                # MCP stdio server — 8 tools (Claude Desktop & other MCP clients)
pg-cdc serve --http               # REST API on 127.0.0.1:8080 (bearer auth required for non-loopback)
#   data plane:  /v1/tables* · /v1/query · /v1/manifest · /v1/diff           (docs/spec/data-plane-api.md)
#   console:     /v1/status · /v1/catalog · /v1/audit/summary · /v1/stream/* (docs/spec/console-api.md)
#   always on:   /healthz · /metrics   ·   --pprof <loopback> for /debug/pprof

# Query  (flags: -t -c --role -f -n -w --no-deltas --changes-since)
pg-cdc query                                                              # list active tables
pg-cdc query -t public.orders -c id,name -w amount:gt:100 -n 10 -f json   # project + filter + limit
pg-cdc promote --from <ref> --to main [--force --dry-run --json --reason "..."]   # manual ref promotion

# Catalog (Glue / Iceberg)
pg-cdc catalog register           # Register manifest tables in the catalog without re-snapshotting

# ACL (DynamoDB-backed Layer-2 tag intent → Lake Formation)
pg-cdc acl register <resource>    # Idempotently register at v=0 with default tags
pg-cdc acl get <resource>         # Resolved + direct tags
pg-cdc acl set <resource> --tag k=v [--tag k2=v2 ...] --reason "..."      # Bump version, set intent (audited)
pg-cdc acl list [--unclassified]  # List classifications
pg-cdc acl diff                   # Drift between ACL intent and Lake Formation
pg-cdc acl sync                   # Apply intent → Lake Formation (exits 3 when drift healed)

pg-cdc version                    # Print version + build info
```

Full operations guide: [`docs/07-operations.md`](docs/07-operations.md). Smoke-test runbook: [`docs/09-smoke-test.md`](docs/09-smoke-test.md).

---

## Quickstart

```bash
# 1. Install (Linux amd64, from the private repo via gh)
gh release download --repo dataalgebra-engineering/burnside-project-pg-cdc \
  --pattern 'pg-cdc_*_linux_amd64.tar.gz' --dir /tmp
tar xzf /tmp/pg-cdc_*_linux_amd64.tar.gz -C /tmp
sudo install -m 0755 /tmp/pg-cdc-linux-amd64 /usr/local/bin/pg-cdc
# OSS mirror: curl -fsSL https://github.com/burnside-project/pg-cdc/releases/latest/download/pg-cdc_linux_amd64.tar.gz | tar xz

# 2. Verify the source, snapshot, stream
pg-cdc preflight
pg-cdc init
pg-cdc start

# 3. Serve to AI agents
pg-cdc serve --mcp
```

### Configuration

Minimal (filesystem sink, no governance) — full reference in [`docs/02-configuration.md`](docs/02-configuration.md):

```yaml
# pg-cdc.yml
source:
  postgres:
    url: "postgresql://cdc_user:${PGCDC_PASSWORD}@host:5432/db"
    schemas: ["public"]
    provider: auto                 # auto | self-managed | rds | aurora

storage:
  type: filesystem                 # filesystem | s3   (gcs is a stub)
  path: /var/lib/pg-cdc/output/

replication:
  publication: pg_cdc_pub
  slot: pg_cdc_slot

flush:
  interval_sec: 10
  max_rows: 1000
```

Governed S3 + Iceberg + Lake Formation (RDS/Aurora with IAM auth):

```yaml
source:
  postgres:
    url: "postgresql://cdc_user@my-db.abc.us-west-2.rds.amazonaws.com:5432/db?sslmode=require"
    schemas: ["public"]
    provider: aurora
    iam_auth: true                 # short-lived RDS/Aurora IAM tokens, no static password

storage:
  type: s3
  bucket: my-governed-warehouse
  prefix: cdc/
  region: us-west-2

catalog:
  type: iceberg                    # glue (Hive/Parquet) | iceberg
  warehouse: s3://my-governed-warehouse/cdc/
  database: my_db
  region: us-west-2
  acl_table: my-acl-table          # DynamoDB ACL intent table

governance:
  catalog: glue
  mode: strict                     # refuse to publish untagged tables
  require_tags: ["sensitivity"]

replication:
  publication: pg_cdc_pub
  slot: pg_cdc_slot
  heartbeat_interval_sec: 30       # keep an idle slot advancing (Aurora Serverless v2)

guardrail:
  max_retained_wal_bytes: 10737418240   # alarm if the slot pins > 10 GiB of WAL
```

The Terraform for the Glue DB, DynamoDB ACL table, Lake Formation default-deny, audit pipeline, and per-role IAM scopes is in [`deploy/terraform/governance/`](deploy/terraform/governance/).

---

## Consumption paths

| Consumer | How | Reads |
| --- | --- | --- |
| **AI agents** | `pg-cdc serve --mcp` (8 MCP tools) | Governed Parquet via DuckDB; column-filtered by LF grants |
| **HTTP clients / dashboards** | `pg-cdc serve --http` (REST + SSE) | Same query/governance over HTTP |
| **Developers / analytics** | [pg-warehouse](https://github.com/burnside-project/pg-warehouse) | Reads object storage directly with its own AWS identity |
| **Athena / Spark / Iceberg engines** | AWS Glue + Lake Formation | The catalog tables, gated by LF tags |

pg-cdc **produces** the governed zone; the contract with consumers is defined in [burnside-go](https://github.com/burnside-project/burnside-go).

---

## Tech stack

| Layer | Technology |
| --- | --- |
| Language | Go 1.25 (pure Go, no CGO) |
| CLI | Cobra |
| PostgreSQL | pgx/v5 + pglogrepl (self-managed, AWS RDS, Aurora) |
| Columnar | parquet-go · iceberg-go (Apache Iceberg) |
| Query | DuckDB (read path) |
| State | SQLite (modernc.org/sqlite) |
| Storage | S3 · filesystem (GCS stub) |
| Catalog / governance | AWS Glue · Lake Formation · DynamoDB |
| Platforms | Linux amd64/arm64 · macOS amd64/arm64 · Windows amd64 |

## Related repos

| Repo | Role |
| --- | --- |
| **pg-cdc** (this repo) | CDC server — WAL streaming, Iceberg/Parquet, compaction, governance, MCP/REST |
| [pg-warehouse](https://github.com/burnside-project/pg-warehouse) | Analytics client — refresh, model, version, export |
| [burnside-go](https://github.com/burnside-project/burnside-go) | Shared types — manifest spec, storage interface |
| [pg-cdc-testbed](https://github.com/dataalgebra-engineering/burnside-project-pg-cdc-testbed) | Ephemeral RDS/Aurora validation beds |

## Versioning

Release candidates auto-increment on push to `main` (`v0.1.0-rc1`, `-rc2`, …); stable releases are promoted from an RC via workflow dispatch. See [`CHANGELOG.md`](CHANGELOG.md) and [`docs/about/stability.md`](docs/about/stability.md) for what's GA vs beta.

## Documentation

Start at [`docs/README.md`](docs/README.md) — getting started, configuration, security architecture, governance, AI-agent consumption, durability, runbooks, and the full [feature index](docs/00-features.md).

## License

[Apache License 2.0](LICENSE)

## Community

- [GitHub Issues](https://github.com/burnside-project/pg-cdc/issues) — bugs and feature requests
- [GitHub Discussions](https://github.com/burnside-project/pg-cdc/discussions) — questions and ideas
- [Contributing](CONTRIBUTING.md) — development setup and guidelines
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security Policy](SECURITY.md)

## License

[Apache License 2.0](LICENSE) — Copyright 2025-2026 [Burnside Project](https://burnsideproject.ai)
