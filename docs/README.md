# pg-cdc Documentation

**PostgreSQL CDC → governed Apache Iceberg on AWS S3 — purpose-built for AI agent data planes.**

---

- [pg-cdc Documentation](#pg-cdc-documentation)
- [Architecture](#architecture)
- [Guides](#guides)
- [Commands](#commands)
- [Deployment Topologies](#deployment-topologies)
- [Governance & Security](#governance--security)
- [Related Repos](#related-repos)

---

## Architecture

pg-cdc uses hexagonal architecture (ports & adapters). The daemon streams WAL into governed Iceberg tables on S3; the server layer exposes governed reads via MCP (Model Context Protocol) and REST.

```
                  ┌─────────────────────────────────────┐
                  │           Domain Core               │
                  │  CDC events, epochs, compaction,    │
                  │  Iceberg snapshots, refs, ACLs      │
                  └──────┬──────────────────┬───────────┘
            ┌────────────┤                  ├────────────┐
       ─────▼─────  ─────▼─────        ─────▼─────  ─────▼─────
      │  Source   ││   Sink    │      │  Catalog  ││  State    │
      │  (port)   ││  (port)   │      │  (port)   ││  (port)   │
       ───────────  ───────────        ───────────  ───────────
            │            │                  │            │
            │       ┌────┼────┐            │            │
            ▼       ▼    ▼    ▼            ▼            ▼
       ┌─────────┐ ┌───┐ ┌──┐ ┌────┐ ┌────────┐ ┌──────────┐
       │Postgres │ │FS │ │S3│ │GCS │ │Glue    │ │ SQLite   │
       │ adapter │ └───┘ └──┘ └────┘ │Catalog │ │ / S3     │
       └─────────┘                    └────────┘ └──────────┘
                                           │
                                    ┌──────┴──────┐
                                    │Lake Formation│
                                    │(tag govern.) │
                                    └──────┬──────┘
                                           │
              ┌────────────────────────────┼────────────────────────────┐
              │                            │                            │
         ─────▼───────               ─────▼───────               ─────▼───────
        │  MCP Server │             │  REST API   │             │  pg-warehouse│
        │  (stdio)    │             │  (HTTP)     │             │  (analytics) │
         ─────────────               ─────────────               ─────────────
              │                            │                            │
              └──────────────┬─────────────┴─────────────┬──────────────┘
                             │                           │
                        ┌────▼────┐               ┌─────▼─────┐
                        │  DuckDB │               │  Athena / │
                        │ (local) │               │  Spark /  │
                        └─────────┘               │  Trino    │
                                                  └───────────┘
```

**Ports** (interfaces in `internal/ports/`):
- **Source** — database discovery, snapshot, WAL streaming, ACL introspection
- **Sink** — file storage (Iceberg/Parquet write, manifest management)
- **Catalog** — metadata catalog (Glue, Iceberg REST)
- **StateStore** — replication progress, epoch lifecycle, compaction state

**Adapters** (implementations in `internal/adapters/`):
- Source: PostgreSQL (`pgx/v5` + `pglogrepl`)
- Sink: filesystem, S3, GCS (stub)
- Catalog: AWS Glue, Iceberg REST
- State: SQLite, S3

**Servers** (`internal/server/`):
- **MCP** — Model Context Protocol over stdio, 8 tools, governed reads via DuckDB
- **REST** — HTTP API, same query semantics, bearer auth, SSE event streams

Governance is enforced at query time: Lake Formation tag resolution gates which columns leave storage. See [10-security-architecture.md](10-security-architecture.md) for the full five-property model.

---

## Guides

| # | Guide | Covers |
|---|-------|--------|
| 00 | [Feature Index](00-features.md) | Exhaustive catalog of every capability |
| 01 | [Getting Started](01-getting-started.md) | Full production deployment — Postgres → Iceberg → S3 → governed MCP/REST |
| 02 | [Configuration](02-configuration.md) | `pg-cdc.yml` reference — source, storage, replication, catalog, governance |
| 03 | [Init & Snapshot](03-init.md) | Create replication slot, snapshot tables, Glue registration |
| 04 | [Streaming](04-streaming.md) | Start WAL streaming, flush intervals, epoch files |
| 05 | [Compaction](05-compaction.md) | Merge deltas into new base, tombstones, retention |
| 06 | [MCP Server](06-mcp.md) | Tool reference (8 tools), client wiring, troubleshooting |
| 08 | [Operations](08-operations.md) | Status, discover, teardown, recovery, systemd, catalog register |
| 10 | [Security Architecture](10-security-architecture.md) | Five-property security model, network, secrets, break-glass |
| 11 | [AI Agent Consumption](11-ai-agent-consumption.md) | MCP tools, REST endpoints, freshness signals, contract diffs |
| 12 | [Durability](12-durability.md) | At-least-once delivery proof, CAS manifest, crash recovery |

### Supporting References

| Document | Covers |
|----------|--------|
| [Metrics](metrics.md) | All 29 Prometheus metrics with descriptions |
| [HIPAA Deployment](hipaa-deployment.md) | HIPAA overlay checklist for regulated environments |
| [Least-Privilege Deployment](least-privilege-deployment.md) | Minimal IAM grants, default-deny posture |
| [Commercial Edition](commercial-edition.md) | Layer-2 tag governance, DynamoDB ACL, LF reconciliation |

---

## Commands

### Core CDC

| Command | Purpose | Guide |
|---------|---------|-------|
| `pg-cdc preflight` | Verify source readiness (WAL level, privileges, slot headroom) | [01](01-getting-started.md) |
| `pg-cdc init` | Snapshot tables → base Iceberg/Parquet + manifest + catalog + ACL | [03](03-init.md) |
| `pg-cdc start` | Stream WAL → delta epochs (long-running daemon) | [04](04-streaming.md) |
| `pg-cdc compact` | Merge deltas → new base (applies I/U/D, tombstone cleanup) | [05](05-compaction.md) |
| `pg-cdc status` | Health: slot, lag, LSN, epochs, tables, oldest txn | [08](08-operations.md) |
| `pg-cdc discover` | List tables with tags/policies, optional ACLs | [08](08-operations.md) |
| `pg-cdc reconcile --force` | One-shot schema reconcile after ALTER on hot table | [04](04-streaming.md) |
| `pg-cdc teardown` | Drop slot + publication + storage + state | [08](08-operations.md) |

### Servers

| Command | Purpose | Guide |
|---------|---------|-------|
| `pg-cdc serve --mcp` | MCP stdio server — 8 tools for Claude Desktop / Cursor | [06](06-mcp.md) |
| `pg-cdc serve --http` | REST API on `:8080` — query, manifest, status, SSE events | [11](11-ai-agent-consumption.md) |

### Query & Promotion

| Command | Purpose |
|---------|---------|
| `pg-cdc query` | List active tables or query with projection/filter/limit |
| `pg-cdc promote --from <ref> --to main` | Manual snapshot promotion |
| `pg-cdc catalog register` | Register tables in Glue without re-snapshotting |

### Governance (ACL)

| Command | Purpose |
|---------|---------|
| `pg-cdc acl register <resource>` | Register at v=0 with default tags |
| `pg-cdc acl get <resource>` | Resolved + direct tags |
| `pg-cdc acl set --tag k=v --reason "..."` | Bump version, set intent (audited) |
| `pg-cdc acl list [--unclassified]` | List classifications |
| `pg-cdc acl diff` | Drift between ACL intent and Lake Formation |
| `pg-cdc acl sync` | Apply intent → Lake Formation |

Full reference: `pg-cdc --help` and each subcommand's `--help`.

---

## Deployment Topologies

| Topology | Source Auth | Sink | Governance | Use Case |
|----------|-----------|------|-----------|----------|
| **Laptop → FS** | Local trust | `filesystem` | None | Dev evaluation, 5-min MCP demo |
| **RDS/Aurora → S3 (governed)** | RDS IAM token | `s3` + Iceberg | Lake Formation + Glue | **Production (recommended)** |
| **RDS/Aurora → S3 (ungoverned)** | Password / IAM | `s3` + Parquet/Glue | Glue catalog only | Dev/QA, no compliance requirements |
| **Self-managed PG → S3** | Password / mTLS | `s3` + Iceberg | Lake Formation + Glue | On-prem migration path |
| **Self-managed PG → FS** | Local trust | `filesystem` | None | Air-gapped / on-prem evaluation |

Production-grade deployments use **ECS Fargate** or **EKS** with VPC endpoints, KMS encryption, and Lake Formation tag governance. See [01-getting-started.md](01-getting-started.md) for a full AWS reference architecture.

---

## Governance & Security

pg-cdc provides a **default-deny governed data plane** for AI agents and analytics:

1. **DynamoDB ACL registry** — versioned tag intent per resource with full audit provenance
2. **Lake Formation tag enforcement** — column-level tags gate reads; untagged data is invisible
3. **ACL reconciliation** — `acl diff` + `acl sync` heals drift between intent and live LF
4. **No prod credentials** — consumers authenticate via AWS IAM, never a connection string
5. **No return path** — WAL is one-way; Iceberg is immutable

See [10-security-architecture.md](10-security-architecture.md) for the full model, [11-ai-agent-consumption.md](11-ai-agent-consumption.md) for consumption paths, and [commercial-edition.md](commercial-edition.md) for the governance feature set.

---

## Related Repos

| Repo | Role |
|------|------|
| **pg-cdc** (this repo) | CDC server — WAL → governed Iceberg, MCP/REST read paths |
| [pg-warehouse](https://github.com/burnside-project/pg-warehouse) | Analytics client — refresh, model, version, export |
| [burnside-go](https://github.com/burnside-project/burnside-go) | Shared types — manifest spec, storage interface, epoch helpers |

---

## Quick Links

- [Feature Index](00-features.md) — every capability in one page
- [Getting Started](01-getting-started.md) — deploy from scratch to production
- [Security Architecture](10-security-architecture.md) — auth, encryption, audit, break-glass
- [Metrics](metrics.md) — 29 Prometheus metrics reference
- [HIPAA Readiness](hipaa-deployment.md) — regulated environment overlay
- [Least-Privilege Deployment](least-privilege-deployment.md) — minimal IAM grants
