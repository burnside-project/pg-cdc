# pg-cdc Documentation

PostgreSQL CDC server. Streams WAL changes into typed, compacted Parquet files in cloud storage. Follows the native Postgres replica pattern.

## Architecture

pg-cdc uses hexagonal architecture (ports & adapters):

```
                  ┌─────────────────────────┐
                  │      Domain Core         │
                  │  CDC events, epochs,     │
                  │  tags, compaction logic   │
                  └────────┬────────────────┘
            ┌──────────────┼──────────────┐
       ─────▼─────    ─────▼─────    ─────▼─────
      │  Source   │  │   Sink    │  │  State    │
      │  (port)   │  │  (port)   │  │  (port)   │
       ───────────    ───────────    ───────────
            │              │              │
       ┌────┘         ┌────┼────┐    ┌────┘
       ▼              ▼    ▼    ▼    ▼
  ┌─────────┐   ┌────┐ ┌───┐ ┌──┐ ┌──────┐
  │Postgres │   │FS  │ │GCS│ │S3│ │SQLite│
  │ adapter │   └────┘ └───┘ └──┘ └──────┘
  └─────────┘
```

**Ports** (interfaces in `internal/ports/`):
- **Source** — database discovery, snapshot, WAL streaming, ACL introspection
- **Sink** — file storage (write/read Parquet, manifest management)
- **StateStore** — replication progress, epoch lifecycle, compaction state

**Adapters** (implementations in `internal/adapters/`):
- Source: PostgreSQL (pgx/v5 + pglogrepl)
- Sink: filesystem, GCS, S3
- State: SQLite

## Guides

| # | Guide | What you'll do |
|---|-------|---------------|
| 1 | [Getting Started](01-getting-started.md) | Prerequisites, install, first init |
| 2 | [Configuration](02-configuration.md) | pg-cdc.yml reference — source, storage, replication, tables, catalog |
| 3 | [Init & Snapshot](03-init.md) | Create replication slot, snapshot tables, (optional) register in Glue |
| 4 | [Streaming](04-streaming.md) | Start WAL streaming, flush intervals, epochs |
| 5 | [Compaction](05-compaction.md) | Merge deltas into base, tombstones, retention |
| 6 | [Operations](08-operations.md) | Status, discover, teardown, recovery, `catalog register` |

## Deployment topologies

| Topology | Source auth | Sink | Notes |
|----------|-----------|------|-------|
| **On-prem → GCS** | mTLS (client certs) | `gcs` + SA key file | VPN/Interconnect for PG, SA key for GCS |
| **GCE VM → GCS** | mTLS or Cloud SQL Proxy | `gcs` + VM service account | No key files — VM metadata provides GCS creds |
| **Cloud SQL → GCS** | Cloud SQL Auth Proxy (IAM) | `gcs` + VM service account | Fully managed — one SA, zero certificates |
| **EC2 → S3** | RDS IAM token (`${RDS_AUTH_TOKEN}`) | `s3` + instance role | IAM everywhere — no static credentials |

## Commercial edition

For regulated deployments — Layer-2 tag governance, DynamoDB-backed ACL registry, AWS Lake Formation reconciliation, Terraform provisioning, HIPAA-ready topology — see [`commercial-edition.md`](commercial-edition.md).

## Related repos

| Repo | Role |
|------|------|
| **pg-cdc** (this repo) | CDC server — WAL streaming, Parquet, compaction |
| [burnside-go](https://github.com/burnside-project/burnside-go) | Shared types — manifest spec, storage interface |
