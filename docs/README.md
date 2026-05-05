# pg-cdc Documentation

PostgreSQL CDC server. Streams WAL changes into typed, compacted Parquet files on disk or in cloud storage, and exposes the result to AI clients (Claude Desktop, Cursor, …) over a local MCP endpoint.

> **The wedge:** talk to your real Postgres data from Claude in 5 minutes — no prod credentials, no cloud account. See the [project README](../README.md) for the pitch and demo, or jump straight to [Getting Started](01-getting-started.md).

## Architecture

pg-cdc uses hexagonal architecture (ports & adapters). The daemon writes; the MCP server reads.

```
                  ┌─────────────────────────┐
                  │      Domain Core        │
                  │  CDC events, epochs,    │
                  │  tags, compaction       │
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
                       ▲
                       │ read manifest + Parquet (via DuckDB)
                       │
                  ┌────┴──────┐
                  │ MCP server│ ◀── stdio JSON-RPC ── Claude / Cursor / …
                  │ (read-only)│
                  └───────────┘
```

**Ports** (interfaces in `internal/ports/`):
- **Source** — database discovery, snapshot, WAL streaming, ACL introspection
- **Sink** — file storage (write/read Parquet, manifest management)
- **StateStore** — replication progress, epoch lifecycle, compaction state

**Adapters** (implementations in `internal/adapters/`):
- Source: PostgreSQL (`pgx/v5` + `pglogrepl`)
- Sink: filesystem, GCS, S3
- State: SQLite

**MCP server** (`internal/mcpserver/`) — JSON-RPC 2.0 over stdio, four tools (`list_tables`, `describe_table`, `query`, `recent_changes`); `query` and `recent_changes` shell out to the DuckDB CLI for SQL on Parquet.

## Guides

| # | Guide | What it covers |
|---|-------|---------------|
| 1 | [Getting Started](01-getting-started.md) | 5-minute MCP-first quickstart — Postgres → Parquet → Claude |
| 2 | [Configuration](02-configuration.md) | `pg-cdc.yml` reference — source, storage, replication, tables, catalog |
| 3 | [Init & Snapshot](03-init.md) | Create replication slot, snapshot tables, optional Glue registration |
| 4 | [Streaming](04-streaming.md) | Start WAL streaming, flush intervals, epoch files |
| 5 | [Compaction](05-compaction.md) | Merge deltas into a new base, tombstones, retention |
| 6 | [MCP Server](06-mcp.md) | Tool reference, client wiring (Claude / Cursor), troubleshooting, security model |
| 8 | [Operations](08-operations.md) | Status, discover, teardown, recovery, systemd, `catalog register` |

## Commands

| Command | Purpose | Section |
|---|---|---|
| `pg-cdc init` | Snapshot tables, create replication slot, write manifest | [03-init](03-init.md) |
| `pg-cdc start` | Stream WAL → delta Parquet | [04-streaming](04-streaming.md) |
| `pg-cdc compact` | Merge deltas into a new base snapshot | [05-compaction](05-compaction.md) |
| `pg-cdc mcp` | Serve a local MCP endpoint over the Parquet output | [06-mcp](06-mcp.md) |
| `pg-cdc status` | Show slot LSN, lag, table counts, last compaction | [08-operations](08-operations.md) |
| `pg-cdc discover` | List tables with tags / policies, optionally with ACLs | [08-operations](08-operations.md) |
| `pg-cdc teardown` | Drop publication and replication slot | [08-operations](08-operations.md) |
| `pg-cdc catalog register` | Register manifest tables in Glue without re-snapshotting | [08-operations](08-operations.md) |
| `pg-cdc version` | Print binary version | — |

## Deployment topologies

| Topology | Source auth | Sink | Notes |
|----------|-----------|------|-------|
| **Laptop → local FS → Claude** | local trust / dev creds | `filesystem` | The open-core wedge — 5-minute setup |
| **On-prem → GCS** | mTLS (client certs) | `gcs` + SA key file | VPN/Interconnect for PG, SA key for GCS |
| **GCE VM → GCS** | mTLS or Cloud SQL Proxy | `gcs` + VM service account | No key files — VM metadata provides GCS creds |
| **Cloud SQL → GCS** | Cloud SQL Auth Proxy (IAM) | `gcs` + VM service account | Fully managed — one SA, zero certificates |
| **EC2 → S3** | RDS IAM token (`${RDS_AUTH_TOKEN}`) | `s3` + instance role | IAM everywhere — no static credentials |

## Commercial edition

For regulated deployments — Layer-2 tag governance, DynamoDB-backed ACL registry, AWS Lake Formation reconciliation, Terraform provisioning, HIPAA-ready topology, multi-user authenticated MCP — see [`commercial-edition.md`](commercial-edition.md).

## Related repos

| Repo | Role |
|------|------|
| **pg-cdc** (this repo) | CDC server — WAL streaming, Parquet writing, MCP read path |
| [burnside-go](https://github.com/burnside-project/burnside-go) | Shared types — manifest spec, storage interface, epoch helpers |
