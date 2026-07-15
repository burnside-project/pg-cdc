# MCP Server

`pg-cdc mcp` exposes the Parquet output of `pg-cdc init` and `pg-cdc start` as a [Model Context Protocol](https://modelcontextprotocol.io) endpoint over stdio. Claude Desktop, Cursor, Continue, Zed, and any other MCP-compatible client can connect to it and answer questions grounded in your real Postgres data — without a live connection to your database.

## Where it fits

```
PostgreSQL → pg-cdc start → ./data (Parquet) → pg-cdc mcp ↔ Claude Desktop
```

The MCP server is the read path for the same data the daemon writes. The daemon is the only thing that talks to Postgres; the MCP server only reads files on disk via DuckDB.

## Scope (open core)

| Property | Value | Why |
|---|---|---|
| Transport | stdio (the client launches it as a subprocess) | No network listener → no auth needed → simplest possible setup |
| Auth | none | Single-user local tool; the OS user *is* the boundary |
| Direction | read-only | All four tools read manifest or Parquet; mutations are rejected at validation |
| Tenancy | single-user | One process serves one client |
| Storage support | filesystem | `query` / `recent_changes` shell out to DuckDB which currently reads local files only |

For multi-user, authenticated MCP, S3/GCS-backed query, or governed access (row/column-level), see [`commercial-edition.md`](commercial-edition.md).

## Prerequisites

- A pg-cdc deployment that has been initialized (`pg-cdc init`) so a `manifest.json` exists in the configured storage path
- **DuckDB CLI** on `PATH`, required by the `query` and `recent_changes` tools:
  ```bash
  brew install duckdb            # macOS
  # or apt install duckdb       # Debian/Ubuntu
  # or see https://duckdb.org/docs/installation/
  ```
  `list_tables` and `describe_table` work without DuckDB — they only read the manifest.

## Running it

```bash
pg-cdc mcp --config pg-cdc.yml
```

The process talks JSON-RPC 2.0 over stdin/stdout. You don't normally invoke it directly — you wire it into your MCP client and the client launches it for you. To smoke-test the protocol, you can pipe a request in:

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | pg-cdc mcp --config pg-cdc.yml
```

You should see a JSON response carrying the protocol version and server info.

## Wiring into clients

### Claude Desktop

Edit `claude_desktop_config.json`:

| OS | Path |
|---|---|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| Linux | `~/.config/Claude/claude_desktop_config.json` |

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

Restart Claude Desktop. You should see `pg-cdc` listed in the MCP indicator and four tools available.

### Cursor

Cursor follows the same MCP config shape. In Settings → Features → Model Context Protocol, add a server entry equivalent to the JSON above.

### Continue / Zed / other MCP clients

Any client that speaks MCP over stdio can connect. Provide:
- **Command:** `pg-cdc`
- **Args:** `["mcp", "--config", "/absolute/path/to/pg-cdc.yml"]`

## Tools

The server exposes four tools. The LLM picks which to call based on the user's question.

### `list_tables`

Lists all active tables from the manifest with row counts, tags, and epoch state.

**Arguments:** none.

**Backed by:** manifest read (no DuckDB needed).

**Example output (text content):**
```
3 active tables (sink: filesystem)

- public.customers · rows=1240 · base_epoch=0 · latest_delta=12 · tags=[pii]
- public.orders    · rows=58302 · base_epoch=15 · latest_delta=42
- public.products  · rows=189 · base_epoch=0 · latest_delta=3
```

### `describe_table`

Returns the schema (columns, Postgres types, nullability, primary key) for one table.

**Arguments:**
| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Fully-qualified table name (e.g. `public.orders`) |

**Backed by:** manifest read (no DuckDB needed).

**Example output:**
```
public.orders · status=active · rows=58302 · schema_version=1

primary key: id

columns:
  id                             bigint               NOT NULL
  customer_id                    bigint               NOT NULL
  total                          numeric(10,2)        NULL
  created_at                     timestamptz          NOT NULL
```

### `query`

Runs a read-only `SELECT` against the Parquet output (base + deltas combined). Tables are exposed under their Postgres-qualified names, so SQL looks the same as it would against the source database.

**Arguments:**
| Field | Type | Required | Description |
|---|---|---|---|
| `sql` | string | yes | A `SELECT` (or `WITH … SELECT`) statement |
| `limit` | integer | no | Max rows to return — default `100`, max `1000` |

**Backed by:** DuckDB CLI subprocess. Statement timeout: 15 seconds.

**Validation:** the SQL must start with `SELECT` or `WITH`. Statements containing `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `ATTACH`, `COPY`, `PRAGMA`, or `LOAD` are rejected before reaching DuckDB.

**Example invocation (the LLM constructs this):**
```json
{
  "name": "query",
  "arguments": {
    "sql": "SELECT customer_id, COUNT(*) AS n_orders, SUM(total) AS revenue FROM public.orders WHERE created_at >= NOW() - INTERVAL 7 DAY GROUP BY 1 ORDER BY revenue DESC",
    "limit": 10
  }
}
```

### `recent_changes`

Returns rows from the *streaming delta files only* — i.e. changes that have arrived via WAL since the last compaction. Useful for "what just happened?" questions when the user has been editing data.

**Arguments:**
| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Fully-qualified table name |
| `limit` | integer | no | Max rows — default `20`, max `200` |

**Backed by:** DuckDB CLI subprocess.

If no deltas have been written yet (e.g. you just ran `init` and haven't streamed anything), the tool returns a friendly *"no streaming changes recorded yet"* message rather than an error.

## What the LLM does with these tools

A typical conversational round trip:

1. User asks Claude: *"What's the average order total for our top-5 customers in the last week?"*
2. Claude calls `list_tables` → sees `public.orders`, `public.customers`
3. Claude calls `describe_table` for both → learns column names and types
4. Claude generates SQL using that schema, calls `query` with it
5. Claude receives JSON rows, synthesizes the human-readable answer

**The server makes zero LLM calls.** Claude (the client) handles all reasoning. The server just executes deterministic tools. This is what makes the open-core MCP free, local, and private — your data and queries never traverse an LLM API from the server side.

## Troubleshooting

### "duckdb CLI not found on PATH"

The `query` and `recent_changes` tools shell out to DuckDB. Install it:

```bash
brew install duckdb         # macOS
apt install duckdb          # Debian/Ubuntu
# or https://duckdb.org/docs/installation/
```

`list_tables` and `describe_table` continue to work without it.

### "read manifest: … (run `pg-cdc init` first?)"

The MCP server reads the manifest the daemon writes. Run `pg-cdc init --config pg-cdc.yml` before connecting a client.

### "table not found in manifest: …"

The table either wasn't in the configured schemas at init time, or was excluded by `tables.policy`. Check `pg-cdc discover --config pg-cdc.yml` to see what's actually included.

### "query may not contain: <keyword>"

The SQL validator rejected a write keyword. Re-phrase as a `SELECT` or `WITH … SELECT`. There is no path to mutate Postgres or the Parquet files through this tool — by design.

### "query tools currently support only filesystem storage in open core"

The open-core `query` path uses local DuckDB and only handles `storage.type: filesystem`. For S3 or GCS-backed deployments, use the commercial edition (which routes through `httpfs` with proper IAM) or run a separate local pg-cdc against your Postgres for the MCP demo.

### Claude Desktop says "pg-cdc disconnected" or doesn't list the tools

1. Confirm the binary path resolves: `which pg-cdc` (or use an absolute path in `claude_desktop_config.json`)
2. Confirm the config path is **absolute** (Claude doesn't run from your shell's CWD)
3. Run the server directly to see if it errors at startup: `pg-cdc mcp --config /absolute/path/to/pg-cdc.yml`
4. Restart Claude Desktop after editing the config

### Stale schema after `ALTER TABLE`

Views are rebuilt on every `query` call (`CREATE OR REPLACE VIEW`), so column additions show up immediately. New tables added to `ALTER PUBLICATION` won't appear until you re-run `pg-cdc init` or extend the publication and they appear in the manifest. The MCP server does not have its own state.

## Security model

- **Read-only at the protocol level.** The validator rejects every mutation keyword. Even if it didn't, DuckDB only sees the Parquet files — there's no live Postgres connection to mutate.
- **No network listener.** The server binds nothing. The only IPC is stdio with the parent process (the MCP client).
- **No credentials surface.** The `pg-cdc.yml` connection string is only used by the daemon (`init`, `start`). The MCP server never opens a Postgres connection.
- **No LLM API key on the server.** The server does not call Anthropic, Google, OpenAI, or any other LLM provider. Whatever the client does is the client's concern.
- **Subprocess isolation.** A pathological query that crashes DuckDB doesn't take down `pg-cdc mcp`; the next call spawns a fresh DuckDB process.

## Limits and upgrade path

| You'll hit this when | Open core | Commercial |
|---|---|---|
| You want a teammate to query the same data | stdio is single-user | Authenticated multi-user MCP server |
| Compliance asks for query audit logs | Not logged | DynamoDB-backed audit trail |
| Some columns must hide from some users | No row/column-level policy | Lake Formation LF-Tags + governed query engine |
| You need S3/GCS query | Filesystem only | DuckDB `httpfs` + IAM-aware reader |
| You want sub-second freshness | CDC delta lag floors at ~5 s | Push-mode delivery |

See [`commercial-edition.md`](commercial-edition.md) for details.

## Related

- [Getting Started](01-getting-started.md) — the 5-minute MCP-first quickstart
- [Configuration](02-configuration.md) — `pg-cdc.yml` reference (the same file `pg-cdc mcp` reads)
- [Streaming](04-streaming.md) — what the daemon is doing while the MCP server reads
