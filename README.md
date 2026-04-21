<!-- Logo placeholder -->
<p align="center">
  <strong>pg-cdc</strong>
</p>

<p align="center">
  PostgreSQL change data capture &rarr; Parquet. Stream WAL changes into typed, compacted Parquet files on S3, GCS, or filesystem. Single Go binary. No CGO. No Kafka.
</p>

<p align="center">
  <a href="https://github.com/burnside-project/pg-cdc/actions"><img src="https://img.shields.io/github/actions/workflow/status/burnside-project/pg-cdc/ci-cd.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/burnside-project/pg-cdc/releases"><img src="https://img.shields.io/github/v/release/burnside-project/pg-cdc?include_prereleases" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"></a>
  <a href="https://github.com/burnside-project/pg-cdc"><img src="https://img.shields.io/github/go-mod/go-version/burnside-project/pg-cdc" alt="Go"></a>
  <a href="https://github.com/burnside-project/pg-cdc/stargazers"><img src="https://img.shields.io/github/stars/burnside-project/pg-cdc?style=social" alt="Stars"></a>
</p>

## Why pg-cdc?

Change data capture from PostgreSQL is usually delivered by Debezium + Kafka + a warehouse loader — three moving parts with their own JVM, schema registry, and ops cost. pg-cdc replaces that stack with a single Go binary that reads WAL via native PostgreSQL logical replication, writes typed Parquet directly to S3/GCS/filesystem, and (optionally) registers tables in AWS Glue. No Kafka. No Java. No warehouse to pre-provision.

## What does it solve?

A PostgreSQL CDC server that streams WAL changes into typed, compacted Parquet files in cloud storage. Follows the native PostgreSQL replica pattern: one publication, one replication slot, one streaming consumer. Suitable for teams who want CDC output as Parquet for downstream warehouses, data lakes, or analytical engines — without running Kafka, Debezium, or a warehouse loader.

## Quick comparison

|                         | pg-cdc                       | Debezium + Kafka            | AWS DMS                    | Fivetran / Airbyte          |
|-------------------------|------------------------------|-----------------------------|----------------------------|-----------------------------|
| Output format           | Parquet (typed, columnar)    | Avro/JSON via Kafka         | Multiple, via engine       | Warehouse-native            |
| Infrastructure          | Single Go binary             | Kafka + Connect + JVM       | Managed AWS service        | SaaS                        |
| Logical replication     | Native pglogrepl             | Debezium connector          | Proprietary                | Via connectors              |
| Sinks                   | Filesystem, S3, GCS          | Kafka topics                | S3, Redshift, others       | Warehouses                  |
| Catalog                 | AWS Glue (optional)          | None                        | Glue                       | Varies                      |
| Cloud required          | No                           | No (but heavy)              | Yes                        | Yes                         |
| Language / runtime      | Go, no CGO                   | Java (JVM)                  | Managed                    | Managed                     |

## How does it work?

```
PostgreSQL ──WAL──→ pg-cdc ──Parquet──→ Cloud Storage
                      │                  (S3 / GCS / filesystem)
                      │
                      └──→ AWS Glue (optional: table catalog)
```

pg-cdc uses PostgreSQL's native logical replication:

| Stage     | Output                                                                                      |
|-----------|---------------------------------------------------------------------------------------------|
| `init`    | Snapshot every table → base Parquet + manifest + (optional) Glue catalog entries            |
| `start`   | Stream WAL → append-only delta Parquet epochs per table                                     |
| `compact` | Merge deltas → new base snapshot (applies inserts/updates/deletes; soft-deletes on 30d TTL) |

State survives restarts: LSN position, epoch watermarks, and table metadata live in a local SQLite state file.

## Documentation

| Doc | Description |
|-----|-------------|
| [Getting Started](docs/01-getting-started.md) | Install, configure, run a first pipeline |
| [Configuration](docs/02-configuration.md) | Full YAML reference |
| [Init](docs/03-init.md) | Snapshot phase details |
| [Streaming](docs/04-streaming.md) | WAL streaming mechanics |
| [Compaction](docs/05-compaction.md) | Base + delta model, TTL semantics |
| [Operations](docs/08-operations.md) | Production run-book, health checks, troubleshooting |
| [Commercial Edition](docs/commercial-edition.md) | Closed-source governance / ACL extensions |

## Install

**Download binary** — see [Releases](https://github.com/burnside-project/pg-cdc/releases) for Linux (amd64/arm64), macOS (arm64), and Windows (amd64):

```bash
# Linux (amd64)
curl -fsSL https://github.com/burnside-project/pg-cdc/releases/latest/download/pg-cdc_linux_amd64.tar.gz | tar xz
sudo install -m 0755 pg-cdc-linux-amd64 /usr/local/bin/pg-cdc

# macOS (Apple Silicon)
curl -fsSL https://github.com/burnside-project/pg-cdc/releases/latest/download/pg-cdc_darwin_arm64.tar.gz | tar xz
sudo install -m 0755 pg-cdc-darwin-arm64 /usr/local/bin/pg-cdc
```

**Build from source**:

```bash
git clone https://github.com/burnside-project/pg-cdc.git
cd pg-cdc
make build
```

## Quickstart

**1. Configure** — create `pg-cdc.yml`:

```yaml
source:
  postgres:
    url: "postgresql://cdc_user@host:5432/db"
    schemas: ["public"]

storage:
  type: filesystem              # filesystem | s3 | gcs
  path: /var/lib/pg-cdc/output/

replication:
  publication: pg_cdc_pub
  slot: pg_cdc_slot

flush:
  interval_sec: 10
  max_rows: 1000
```

**2. Discover tables** — confirm pg-cdc can see what you expect:

```console
$ pg-cdc discover
```

**3. Initialize** — snapshot base tables + create the replication slot:

```console
$ pg-cdc init --config pg-cdc.yml
```

**4. Start streaming** — tail WAL into Parquet deltas:

```console
$ pg-cdc start --config pg-cdc.yml
```

**5. Check health** — LSN lag, epochs, per-table status:

```console
$ pg-cdc status
```

See the [Operations guide](docs/08-operations.md) for production deployment patterns.

## Features

**Source**
- [x] PostgreSQL logical replication (pgx/v5, pglogrepl)
- [x] Per-schema discovery
- [x] Declarative table include/exclude rules
- [x] Tag-based table policy (e.g., `pii`, `ephemeral` tags → include/exclude)

**Output**
- [x] Typed Parquet (pure Go, no CGO)
- [x] Base snapshots + append-only delta epochs
- [x] Compaction into new base (applies I/U/D; soft-deletes on 30d TTL)
- [x] Manifest file per table (schema + epoch ordering)

**Sinks**
- [x] Filesystem
- [x] S3
- [x] GCS

**Catalog**
- [x] AWS Glue (optional; register manifest tables without re-snapshotting)

**Operations**
- [x] Single static binary, no CGO
- [x] Linux amd64/arm64, macOS arm64, Windows amd64
- [x] SQLite state tracking (LSN, epoch watermarks, table metadata)
- [x] Role → table → column ACL discovery from PostgreSQL GRANTs
- [x] Automated RC releases on every push to main; stable releases via workflow dispatch

## Commands

| Command | What it does |
|---------|--------------|
| `init` | Snapshot tables → base Parquet + manifest + (optional) Glue catalog |
| `start` | Stream WAL → delta Parquet epochs |
| `compact` | Merge deltas → new base snapshot (applies I/U/D; soft-deletes on TTL) |
| `status` | Health: lag, LSN, epochs, tables |
| `discover` | List tables from Postgres |
| `discover --acl` | Show role → table → column access map from PostgreSQL GRANTs |
| `teardown` | Drop publication + replication slot |
| `catalog register` | Register manifest tables in Glue without re-snapshotting |
| `version` | Print version |

Full reference in [`docs/08-operations.md`](docs/08-operations.md).

## Configuration

Production example with S3 + Glue and tag-based policy:

```yaml
source:
  postgres:
    url: "postgresql://cdc_user:${PGCDC_PASSWORD}@host:5432/db"
    schemas: ["public"]

storage:
  type: s3
  bucket: my-warehouse
  prefix: cdc/
  region: us-west-2

catalog:
  type: glue
  database: my_db
  region: us-west-2

tables:
  exclude: ["public.tbl_sessions"]
  tags:
    pii: ["public.tbl_cc", "billing.*"]
    ephemeral: ["*.tbl_session*"]
  policy:
    pii: exclude
    ephemeral: exclude
    untagged: include
```

Full reference: [`docs/02-configuration.md`](docs/02-configuration.md).

## Architecture

pg-cdc uses hexagonal architecture with clean port/adapter separation. CLI commands (Cobra) call services that depend only on port interfaces. Adapters for PostgreSQL (source), Parquet writer, filesystem/S3/GCS sinks, SQLite state, and Glue catalog implement those interfaces. New sinks or catalog backends plug in without changing business logic.

## Open Core

The open-source edition covers the full CDC pipeline: logical replication, base/delta Parquet output, compaction, three sinks, Glue catalog, and SQLite-backed state. Production governance, compliance, and access-control features are commercial:

- Layer-2 tag governance (policy-as-code)
- DynamoDB-backed ACL registry with versioned audit trail
- AWS Lake Formation reconciliation (`acl diff`, `acl sync`)
- Emergency-override workflows with expiry
- Terraform stack for IAM / OIDC / governance provisioning
- Extended CLI: `pg-cdc acl register|get|set|diff|sync|list`
- HIPAA-ready deployment topology

### Governance in action

The commercial edition extends open-core CDC output with a tag-based governance layer:

**1. pg-cdc registers Parquet tables in Glue Data Catalog** — the open-source `catalog register` command populates the catalog for downstream query engines:

![Glue Data Catalog with CDC-registered Parquet tables](assets/glue-catalog-tables.png)

**2. Governance intent is stored in a DynamoDB ACL registry** — every policy change is a versioned record, tagged (e.g. `sensitivity: internal`) and stamped with actor, reason, and timestamp:

![DynamoDB ACL registry item — versioned policy record with tags](assets/dynamodb-acl-registry.png)

**3. Tags are reconciled as LF-Tags on Glue tables** — `pg-cdc acl sync` drives Lake Formation's tag-based access control from the registry:

![Lake Formation LF-Tags on a CDC table](assets/lake-formation-lf-tags.png)

Details: [`docs/commercial-edition.md`](docs/commercial-edition.md).

## Related repos

| Repo | Role |
|------|------|
| **pg-cdc** (this repo) | CDC server — WAL streaming, Parquet writing, compaction |
| [burnside-go](https://github.com/burnside-project/burnside-go) | Shared types — manifest spec, storage interface |
| [pg-warehouse](https://github.com/burnside-project/pg-warehouse) | Local-first analytical engine that can consume CDC output |

## Tech stack

| Layer | Technology |
|-------|------------|
| Language | Go 1.25 (pure Go, no CGO) |
| CLI | Cobra |
| PostgreSQL | pgx/v5, pglogrepl |
| Parquet | parquet-go (pure Go) |
| State | SQLite (modernc.org/sqlite) |
| Storage | Filesystem, S3, GCS |
| Platforms | Linux amd64/arm64, macOS arm64, Windows amd64 |

## Versioning

Release candidates auto-increment on every push to `main`: `v0.1.0-rc1`, `v0.1.0-rc2`, ...

Stable releases are promoted from RCs via the **Release** workflow dispatch.

## Community

- [GitHub Issues](https://github.com/burnside-project/pg-cdc/issues) — bugs and feature requests
- [GitHub Discussions](https://github.com/burnside-project/pg-cdc/discussions) — questions and ideas
- [Contributing](CONTRIBUTING.md) — development setup and guidelines
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security Policy](SECURITY.md)

## License

[Apache License 2.0](LICENSE) — Copyright 2025-2026 [Burnside Project](https://burnsideproject.ai)
