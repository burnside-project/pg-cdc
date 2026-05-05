<!-- Logo placeholder -->
<p align="center">
  <strong>pg-cdc</strong>
</p>

<p align="center">
Talk to your Postgres from Claude in 5 minutes — no prod credentials, no cloud account.
</p>

<p align="center">
  <a href="https://github.com/burnside-project/pg-cdc/actions"><img src="https://img.shields.io/github/actions/workflow/status/burnside-project/pg-cdc/ci-cd.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/burnside-project/pg-cdc/releases"><img src="https://img.shields.io/github/v/release/burnside-project/pg-cdc?include_prereleases" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"></a>
  <a href="https://github.com/burnside-project/pg-cdc"><img src="https://img.shields.io/github/go-mod/go-version/burnside-project/pg-cdc" alt="Go"></a>
  <a href="https://github.com/burnside-project/pg-cdc/stargazers"><img src="https://img.shields.io/github/stars/burnside-project/pg-cdc?style=social" alt="Stars"></a>
</p>

<!-- TODO: replace with assets/demo.gif once the 30-second demo is recorded. -->
<p align="center">
  <em>[demo GIF: psql INSERT → Claude answer that includes the new row]</em>
</p>

## What is pg-cdc?

pg-cdc streams your Postgres WAL into typed Parquet files on your laptop, then exposes them through a local **MCP server**. Claude (or Cursor, or any MCP-compatible client) can answer real questions about your real, current data — without touching prod, and without ever seeing your `DATABASE_URL`.

It's the fastest way to let an AI chat with your Postgres safely.

## 5-minute quickstart

**1. Configure** — `pg-cdc.yml`:

```yaml
source:
  postgres:
    url: postgresql://localhost:5432/mydb
    schemas: [public]
storage:
  type: filesystem
  path: ./data
```

**2. Run the daemon and the MCP server**:

```bash
pg-cdc init && pg-cdc start &   # snapshot + stream WAL → ./data
pg-cdc mcp &                    # serve the local MCP endpoint (stdio)
```

> The `query` and `recent_changes` MCP tools shell out to DuckDB. Install it once with `brew install duckdb` (macOS) or see [duckdb.org/docs/installation/](https://duckdb.org/docs/installation/).

**3. Point Claude Desktop at it** — add to `claude_desktop_config.json` (use the **absolute** path to your config — Claude Desktop launches the subprocess from its own working directory, not yours):

```json
{
  "mcpServers": {
    "pg-cdc": { "command": "pg-cdc", "args": ["mcp", "--config", "/absolute/path/to/pg-cdc.yml"] }
  }
}
```

Restart Claude. Ask *"what's the latest row in `orders`?"* — it answers from your real data.

Full walkthrough: [`docs/01-getting-started.md`](docs/01-getting-started.md).

## Why this, not just give Claude a `DATABASE_URL`?

| | pg-cdc | Hand Claude `DATABASE_URL` |
|---|---|---|
| **Local & private** | Data never leaves your machine | Credentials shared with the model |
| **Real-time** | WAL CDC — rows appear seconds after write | One-shot, stale by next question |
| **Safe & fast** | Parquet snapshot, prod untouched, columnar reads | Live queries against prod, lock contention |
| **Cost** | Free, single binary, no cloud | Risk of a bad query |

## Architecture

![artech.png](assets/artech.png)

## Docs
| Doc | Description |
|-----|-------------|
| [Getting Started](docs/01-getting-started.md) | 5-minute MCP-first quickstart |
| [Configuration](docs/02-configuration.md) | Full YAML reference |
| [Init](docs/03-init.md) | Snapshot phase details |
| [Streaming](docs/04-streaming.md) | WAL streaming mechanics |
| [Compaction](docs/05-compaction.md) | Base + delta model, TTL semantics |
| [MCP Server](docs/06-mcp.md) | Tool reference, client wiring, troubleshooting, security model |
| [Operations](docs/08-operations.md) | Production run-book, health checks, troubleshooting |
| [Commercial Edition](docs/commercial-edition.md) | Governance, ACL, Lake Formation reconciliation |

## Install

**Download binary** — see [Releases](https://github.com/burnside-project/pg-cdc/releases) for the full list of platforms. Latest stable is **v0.2.0**.

```bash
# Linux (amd64)
curl -fsSL https://github.com/burnside-project/pg-cdc/releases/download/v0.2.0/pg-cdc_v0.2.0_linux_amd64.tar.gz | tar xz
sudo install -m 0755 pg-cdc-linux-amd64 /usr/local/bin/pg-cdc

# Linux (arm64)
curl -fsSL https://github.com/burnside-project/pg-cdc/releases/download/v0.2.0/pg-cdc_v0.2.0_linux_arm64.tar.gz | tar xz
sudo install -m 0755 pg-cdc-linux-arm64 /usr/local/bin/pg-cdc

# macOS (Apple Silicon)
curl -fsSL https://github.com/burnside-project/pg-cdc/releases/download/v0.2.0/pg-cdc_v0.2.0_darwin_arm64.tar.gz | tar xz
sudo install -m 0755 pg-cdc-darwin-arm64 /usr/local/bin/pg-cdc
```

**Windows (amd64)** — PowerShell:

```powershell
Invoke-WebRequest https://github.com/burnside-project/pg-cdc/releases/download/v0.2.0/pg-cdc_v0.2.0_windows_amd64.zip -OutFile pg-cdc.zip
Expand-Archive pg-cdc.zip -DestinationPath .
# Move pg-cdc-windows-amd64.exe somewhere on your PATH, e.g.:
New-Item -ItemType Directory -Force "$env:USERPROFILE\bin" | Out-Null
Move-Item pg-cdc-windows-amd64.exe "$env:USERPROFILE\bin\pg-cdc.exe"
# Add %USERPROFILE%\bin to PATH if it isn't already.
```

**Build from source**:

```bash
git clone https://github.com/burnside-project/pg-cdc.git
cd pg-cdc
make build
```

## Production deployment

The 5-minute quickstart above runs everything locally. For production deployments — running pg-cdc as a systemd service, writing to S3/GCS, registering Glue tables, configuring TLS to RDS — see the [Operations guide](docs/08-operations.md).

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

**AI / MCP**
- [x] Local MCP server (`pg-cdc mcp`) — read-only, single-user, stdio
- [x] Tools: `list_tables`, `describe_table`, `query`, `recent_changes`
- [ ] Multi-user / authenticated MCP — [commercial](docs/commercial-edition.md)

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
| `mcp` | Serve a local MCP endpoint over the Parquet output (read-only, single-user) |
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

## Open Core

The open-source edition is a complete, working product:

- Full CDC pipeline — logical replication, base/delta Parquet output, compaction
- Three sink adapters — filesystem, S3, GCS
- Glue catalog registration (optional)
- SQLite-backed state
- **Local MCP server** (`pg-cdc mcp`) — single-user, read-only, stdio
- Postgres-native ACL discovery (`discover --acl`)
- Tag-based table inclusion / exclusion

Production governance, compliance, and multi-user features are commercial:

- Layer-2 tag governance (policy-as-code) with required-tag enforcement
- DynamoDB-backed ACL registry with versioned audit trail
- AWS Lake Formation reconciliation (`acl diff`, `acl sync`)
- Authenticated multi-user MCP server with row/column-level access control
- Emergency-override workflows with expiry
- Terraform stack for IAM / OIDC / governance provisioning
- Extended CLI: `pg-cdc acl register|get|set|diff|sync|list`
- HIPAA-ready deployment topology

See [`docs/commercial-edition.md`](docs/commercial-edition.md) for the end-to-end governance flow with screenshots: Parquet output → Glue Catalog → ACL workflow → DynamoDB registry → Lake Formation LF-Tags.

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

Release candidates auto-increment on every push to `main` against the version in `VERSION`: e.g. `v0.2.0-rc1`, `v0.2.0-rc2`, … When ready, a stable release is promoted from the latest RC via the **Release** workflow dispatch (creates the bare `vX.Y.Z` tag, marks the GitHub release as `--latest`, and renames the binaries).

Latest stable: **v0.2.0** — see [Releases](https://github.com/burnside-project/pg-cdc/releases).

## Community

- [GitHub Issues](https://github.com/burnside-project/pg-cdc/issues) — bugs and feature requests
- [GitHub Discussions](https://github.com/burnside-project/pg-cdc/discussions) — questions and ideas
- [Contributing](CONTRIBUTING.md) — development setup and guidelines
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security Policy](SECURITY.md)

## License

[Apache License 2.0](LICENSE) — Copyright 2025-2026 [Burnside Project](https://burnsideproject.ai)
