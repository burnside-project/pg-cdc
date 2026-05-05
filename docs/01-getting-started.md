# Getting Started

> **Goal:** in 5 minutes, ask Claude a question about your real Postgres data — without giving Claude your `DATABASE_URL`.

## What you'll have at the end

- A `pg-cdc` daemon streaming your Postgres WAL into Parquet files on disk
- A local MCP server (stdio) that Claude Desktop, Cursor, or any MCP client can connect to
- Claude answering questions like *"how many orders did we get today?"* using the **real, current** rows from your database

No cloud account. No prod credentials shared with the model. Read-only.

## How the pieces fit together

```
  ┌────────────┐   WAL   ┌──────────┐   Parquet   ┌────────┐
  │ PostgreSQL │ ──────▶ │  pg-cdc  │ ──────────▶ │ ./data │
  │  (local or │ logical │  daemon  │  base +     │  on    │
  │     VM)    │  repl.  │          │  deltas     │  disk  │
  └────────────┘         └──────────┘             └────┬───┘
                                                       │ read
                                                       ▼
                            ┌─────────────┐    ┌───────────────┐
                            │   Claude    │ ◀──│ pg-cdc mcp    │
                            │   Desktop   │MCP │ (stdio, local)│
                            │ (the LLM)   │    │ via DuckDB    │
                            └─────────────┘    └───────────────┘
```

Claude (the LLM) runs in your client. The `pg-cdc mcp` server is a deterministic
tool that reads Parquet via DuckDB. No LLM API calls happen on the server side —
your data and your queries stay local.

## Prerequisites

- **PostgreSQL 10+** (local or on a VM you can reach), with logical replication enabled (`wal_level = logical`)
- A Postgres user with `REPLICATION` and `SELECT` on the tables you want to expose
- **DuckDB CLI** — required by the `query` and `recent_changes` MCP tools:
  ```bash
  brew install duckdb         # macOS
  # or see https://duckdb.org/docs/installation/ for Linux / Windows
  ```
- **Claude Desktop** (or any MCP-compatible client — Cursor, Continue, Zed, etc.)

Quick check:

```sql
SHOW wal_level;            -- must be 'logical'
```

If not, edit `postgresql.conf` and restart:

```ini
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Create the CDC user:

```sql
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'secret';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
```

## Install

```bash
# macOS (Apple Silicon)
curl -fsSL https://github.com/burnside-project/pg-cdc/releases/latest/download/pg-cdc_darwin_arm64.tar.gz | tar xz
sudo install -m 0755 pg-cdc-darwin-arm64 /usr/local/bin/pg-cdc

# Linux (amd64)
curl -fsSL https://github.com/burnside-project/pg-cdc/releases/latest/download/pg-cdc_linux_amd64.tar.gz | tar xz
sudo install -m 0755 pg-cdc-linux-amd64 /usr/local/bin/pg-cdc

pg-cdc version
```

Or build from source: `git clone … && make build`.

---

## The 5-minute walkthrough

### 1. Configure (one YAML)

```yaml
# pg-cdc.yml
source:
  postgres:
    url: postgresql://cdc_user:secret@localhost:5432/mydb
    schemas: [public]

storage:
  type: filesystem
  path: ./data
```

That's the whole config. Three fields. (Full reference: [`02-configuration.md`](02-configuration.md).)

### 2. Snapshot + stream

```bash
pg-cdc init   --config pg-cdc.yml   # one-time: snapshot tables → Parquet, create replication slot
pg-cdc start  --config pg-cdc.yml & # tail WAL, append delta Parquet files
```

You should see `./data/public/<table>/` filling up.

### 3. Start the local MCP server

```bash
pg-cdc mcp --config pg-cdc.yml
```

This serves an MCP endpoint over stdio. It binds nothing — Claude Desktop launches it as a subprocess. The four tools it exposes:

| Tool | What it does | Backed by |
|---|---|---|
| `list_tables` | Tables available from the manifest, with row counts and tags | manifest read |
| `describe_table` | Column list, Postgres types, nullability for one table | manifest read |
| `query` | Run a read-only `SELECT` against the Parquet (base + deltas) | DuckDB |
| `recent_changes` | The latest rows from the streaming delta files for a table | DuckDB |

Tables are exposed under their Postgres-qualified names, so the LLM can write natural SQL like `SELECT * FROM public.orders WHERE customer_id = 42`. The `query` tool rejects anything that isn't a `SELECT` (or `WITH … SELECT`) — there's no path to mutate your data through this tool.

### 4. Wire it into Claude Desktop

Edit `claude_desktop_config.json` (macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "pg-cdc": {
      "command": "pg-cdc",
      "args": ["mcp", "--config", "/absolute/path/to/pg-cdc.yml"]
    }
  }
}
```

Restart Claude Desktop. You should see `pg-cdc` listed in the MCP indicator.

### 5. Try it

In Claude:

> *"What tables do I have, and what's the latest row in `orders`?"*

Then in your psql session:

```sql
INSERT INTO orders (customer_id, total) VALUES (42, 99.50);
```

Ask Claude again:

> *"Did anything new come in?"*

Claude will see the row you just inserted. That's CDC working: WAL → Parquet delta → MCP `recent_changes` → answer.

---

## What just happened

1. `pg-cdc init` connected to Postgres, took a transactionally consistent snapshot of each table, and wrote it as base Parquet.
2. `pg-cdc start` is now tailing the WAL via a logical replication slot. Every INSERT/UPDATE/DELETE becomes a row in a delta Parquet file under `./data/`.
3. `pg-cdc mcp` is reading those Parquet files (not your live DB) when Claude asks a question. Your prod database sees no traffic from Claude.
4. Claude only ever talks to the local MCP process. It never sees your `DATABASE_URL`, your password, or anything network-attached.

## What this free tier is — and isn't

The open-core MCP plug-in is intentionally scoped for a single developer on their own laptop:

| | Open core | [Commercial](commercial-edition.md) |
|---|---|---|
| Single user, localhost stdio | ✅ | ✅ |
| Multi-user / shared MCP endpoint | ❌ | ✅ |
| Authentication / SSO | ❌ | ✅ |
| Row/column-level access control | ❌ | ✅ via Lake Formation LF-Tags |
| Audit log of every query | ❌ | ✅ |
| HIPAA / SOC2 deployment topology | ❌ | ✅ |
| Tag-based governance (`PII`, `PHI`, …) | ❌ | ✅ |

If you need any of those, the commercial edition is a drop-in upgrade — same daemon, same Parquet output, governance plane added on top.

## Next steps

- [Configuration reference](02-configuration.md) — every `pg-cdc.yml` field
- [Init & Snapshot](03-init.md) — what happens during the snapshot phase
- [Streaming](04-streaming.md) — WAL replication mechanics
- [Compaction](05-compaction.md) — merging deltas into base snapshots
- [Operations](08-operations.md) — running pg-cdc as a systemd service, S3/GCS sinks, recovery
- [Commercial edition](commercial-edition.md) — when you outgrow single-user
