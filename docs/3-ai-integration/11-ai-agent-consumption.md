# AI Agent Consumption

> **Audience:** developers integrating AI agents with pg-cdc data, platform teams designing agent architectures.
>
> For the security model underlying both consumption paths, see [Security Architecture](10-security-architecture.md).

## Two Paths, One Governed Data Layer

pg-cdc produces governed Parquet data in S3. Two consumption paths serve different audiences, both enforced by the same Lake Formation tags:

```
                      +----------------------------------+
                      |  pg-cdc (producer)                |
                      |  WAL --> Parquet --> S3            |
                      |  ACL intent --> DDB --> LF tags   |
                      +----------------+-----------------+
                                       |
                      +----------------v-----------------+
                      |  S3 + Glue Catalog + LF Tags     |
                      |  (single governed data layer)     |
                      +--------+-----------------+-------+
                               |                 |
                +--------------v---+     +-------v-----------+
                |  Path A: MCP/API |     |  Path B: pg-warehouse |
                |  (remote agents) |     |  (local developers)   |
                +---------+--------+     +----------+-----------+
                          |                         |
                +---------v--------+     +----------v-----------+
                | Claude, GPT,     |     | DuckDB on laptop     |
                | custom agents    |     | (offline capable)    |
                +------------------+     +----------------------+
```

## Path A — MCP Server (AI agents, remote)

The MCP (Model Context Protocol) server exposes pg-cdc data as tools that any AI agent can call. The agent never touches production — it queries governed Parquet via a DuckDB engine behind IAM + Lake Formation.

```
Agent (Claude, etc.)
  | MCP tool call: query(table="public.orders", where="status:eq:pending")
  v
MCP Server
  | 1. Authenticates caller via IAM role
  | 2. Checks LF grants for the caller's role
  | 3. Reads manifest.json from S3
  | 4. Queries permitted Parquet files via DuckDB
  | 5. Returns results (column-filtered by LF tags)
  v
Agent receives governed data
```

### Available Tools

The MCP server registers **eight** tools (exact names as they appear over the wire — see `internal/mcpserver/tools.go`):

| Tool | Description | Example |
|---|---|---|
| `list_tables` | List tables the caller's role can see | `list_tables(role="analyst")` |
| `describe_table` | Column names, types, nullability, tags, row count, epoch for a table | `describe_table(table="public.orders")` |
| `query` | Governed query over Parquet — column projection, `where` filters, `limit`, role | `query(table="public.orders", where="status:eq:pending", limit=100)` |
| `get_changes` | CDC changelog for a table since a given epoch (`__op` / `__epoch` columns) | `get_changes(table="public.orders", since_epoch=42)` |
| `get_manifest` | Full manifest metadata | `get_manifest()` |
| `get_freshness` | Currency signal for a table — `lag_seconds`, `is_stale`, version stamps | `get_freshness(table="public.orders")` |
| `get_snapshot` | Data-location pointer for direct reads (Iceberg `snapshot_id`/`metadata_location` or `parquet_hive` `base_path`) | `get_snapshot(table="public.orders")` |
| `get_diff` | Contract diff between two refs (default `from=main`, `to=staging`) with a `promote_safe` verdict | `get_diff(from="main", to="staging")` |

#### Version stamps (consumer contract, Phase 1)

`list_tables` and `describe_table` responses include two monotonic counters per table:

| Field | Bumped when | Source |
|---|---|---|
| `schema_version` | A flush detects a new column (ALTER TABLE ADD) | `manifest.Schemas[name].Version` |
| `policy_version` | `pg-cdc acl set` runs against the table or any of its columns | max of `Classification.Version` across the table + its columns |

Consumers (notably pg-warehouse `--refresh`) cache the last-seen versions per table and only re-pull schemas/data when a counter advances. Version numbers default to `1` when no source is wired (e.g. air-gap mode without an ACL store), so the field is always present and comparable. See [`docs/internal/plans/data-plane-roadmap.md`](internal/plans/data-plane-roadmap.md) for the broader contract.

#### Freshness endpoint (consumer contract, Phase 1)

`get_freshness` (MCP) and `GET /v1/tables/{name}/freshness` (REST) return:

| Field | Meaning |
|---|---|
| `lag_seconds` | Wall-clock seconds since the manifest was last written by the streamer — a flush-cadence proxy for end-to-end freshness |
| `stale_threshold_sec` | Configured threshold (default 30s, matches `docs/slos.md`) |
| `is_stale` | `lag_seconds > stale_threshold_sec` |
| `base_epoch`, `latest_delta_epoch` | Epoch numbers from the manifest |
| `schema_version`, `policy_version` | Same monotonic counters as `list_tables` |
| `last_committed_lsn` | Reserved (empty in this slice; populated when the streamer-side freshness file lands) |

Configure the threshold under `freshness.stale_threshold_sec` in `pg-cdc.yml`. Role filtering matches `describe_table` — a table not visible to the caller's role returns not-found rather than freshness data.

#### Tags and refs (Phase 3 Slice 1)

Every flush window writes an immutable tag named `raw@<RFC3339Nano>` plus the live `main` and `staging` branch pointers into a sink-resident `refs.json`. Consumers pass `?ref=` (REST) or `ref` (MCP tool argument) to `get_freshness` and `get_snapshot` to pin to a specific point in time.

| `ref` | Behavior |
|---|---|
| `""` or `main` (default) | Live manifest — what the streamer most recently wrote |
| `staging` | What the streamer most recently wrote — always advances on every flush |
| `raw@2026-05-06T12:34:56.789Z` | Immutable historical tag. `base_epoch`, `latest_delta_epoch`, `schema_version`, and `lag_seconds` reflect the tag's stored state — pinning a model to a tag means it reads the same data forever |

Use cases:

- **Production model pinning**: deploy with `--ref=tag:raw@<ts>`. New flushes don't change what the model sees; rollback is changing the deployment's ref.
- **pg-warehouse `--refresh`**: poll `staging` for candidates; promote when validated (Slice 3 adds the `pg-cdc promote` command).
- **Time-travel reads**: `get_snapshot --ref=raw@<old-tag>` returns the historical pointer; consumers read directly from object storage with their own SSO identity.

Unknown refs return 404 / tool error rather than silently falling back to live — defaulting to live would defeat the pinning contract.

##### Promotion mode (Phase 3 Slice 2)

`promotion.mode` controls when `main` advances:

```yaml
promotion:
  mode: auto    # default — main advances every flush, same as staging
  # mode: manual — main bootstraps on first flush, then frozen until promote
```

- **`auto`** (default, backwards-compatible) — `main` and `staging` move together on every flush. Existing deployments unchanged.
- **`manual`** — `main` is established on the first flush so `?ref=main` always resolves, then frozen. `staging` continues to advance every flush. The `pg-cdc promote` command (Slice 3) is required to advance `main` after bootstrap.

Operators wanting git-shaped data versioning — production models stable on `?ref=main` while candidate flushes accumulate on `?ref=staging` — set `mode: manual` and use `pg-cdc promote` (Slice 3) to advance main when ready.

##### Promote command + contract diff (Phase 3 Slice 3)

Inspect the diff between two refs:

```
GET /v1/diff?from=main&to=staging          # REST
get_diff(from='main', to='staging')        # MCP
```

Verdict semantics:

| `verdict` | Meaning | `promote_safe` |
|---|---|---|
| `no_change` | Identical contents | true |
| `safe` | Only forward-progress fields advanced (epoch numbers) | true |
| `schema_change` | A table's `schema_version` advanced or a new table appeared | false |
| `policy_change` | A table's `policy_version` advanced (ACL / grant change) | false |
| `breaking` | At least one table present in `from` is missing in `to` | false |

Run promote:

```
pg-cdc promote --from=staging --to=main --reason 'validated by ML team'
```

- `--dry-run` previews the diff without mutating `refs.json`
- `--force` overrides the verdict gate; required for `schema_change` / `breaking`
- `--reason` is mandatory for non-dry-run promotions; recorded in the audit stream
- `--json` prints the diff as JSON instead of a human summary

Promote is metadata-only — the underlying snapshots already exist on disk; promotion just moves the `to` ref. Audit emits an `audit=true` slog line with `surface=cli`, `endpoint=promote`, principal, verdict, and from/to.

The diff covers `schema_version`, `policy_version`, `base_epoch`, `latest_delta_epoch`, and the table set. A `policy_version` advance (an ACL/grant change from `pg-cdc acl set`) surfaces as the `policy_change` verdict. The full request/response contract is specified in [`spec/data-plane-api.md`](spec/data-plane-api.md).

#### Reacting to schema evolution (Phase 2)

`pg-cdc serve` reloads the manifest from storage every `freshness.refresh_interval_sec` (default 30s), so MCP and REST clients see schema and ACL changes the streamer wrote without restarting the server. Clients react in two steps:

1. Poll `list_tables` (or the per-table endpoint) and compare `schema_version` / `policy_version` against the values you cached on the previous pass.
2. When a counter advances, re-read `describe_table` for that table — added columns appear, dropped columns disappear, and tag-driven access changes propagate.

DROP COLUMN is detected when an ADD COLUMN happens on the same table in the same flush window (the unknown column triggers a schema reconcile that catches drops in the same pass). A DROP without a concurrent ADD is reconciled on the next ADD or on `pg-cdc init` — continuous reconciliation is Phase 4 hardening.

#### Audit log (Phase 4)

Every MCP and REST query emits a structured `audit=true` slog line so compliance can reconstruct *intent* — Lake Formation's API audit captures access decisions but not the application-level ask (which agent, which filter, which role assertion). Fields:

| Field | Meaning |
|---|---|
| `surface` | `mcp` or `rest` |
| `endpoint` | Tool name: `query`, `list_tables`, `get_freshness`, etc. |
| `principal` | Best-effort identity. REST: `X-Principal` header → `RemoteAddr` → `anonymous`. MCP stdio: `$AWS_ROLE_ARN` → `user:$USER` → `stdio` |
| `role` | Self-asserted role profile passed in the request |
| `table`, `columns`, `filters` | What was asked for |
| `row_count`, `truncated` | What was returned |
| `duration_ms` | End-to-end handler time |
| `error` | Set when the handler returned an error (table not accessible, parse error, etc.) |

Operators ship `audit=true` lines to a separate compliance sink (Splunk, S3, CloudWatch) for retention. Identity is best-effort: pg-cdc has no built-in authentication, so for hard authn, front the server with an authenticating proxy and pass the verified principal via `X-Principal`.

#### Rate limit (Phase 4 v2)

A per-principal token bucket gates MCP and REST surfaces. Disabled by default; opt in once authentication is fronted by a proxy:

```yaml
rate_limit:
  rate_per_second: 10     # tokens added per second; 0 disables limiting
  burst: 30               # bucket capacity; 0 disables limiting
  max_principals: 10000   # optional cap; protects server memory from principal cycling
```

REST returns `429 RATE_LIMITED`; MCP returns a tool error. Healthz is exempt. The rate-limit key is the same best-effort principal the audit log uses, so blocked requests still produce an audit entry with `error="rate limited"` for compliance.

`max_principals` caps the in-memory bucket map. A flood of distinct principals (random `X-Principal` headers) hits the cap and is rejected — there is no LRU eviction. With legitimate steady-state traffic, 10K is well above any deployment we've seen; tune higher only if your principal set is genuinely larger.

#### Snapshot endpoint (consumer contract, Phase 1)

`get_snapshot` (MCP) and `GET /v1/tables/{name}/snapshot` (REST) return a pointer peer consumers (notably pg-warehouse) use to read directly from object storage. **pg-cdc never proxies bytes through this endpoint** — consumers authenticate to S3/GCS with their own AWS SSO identity, and Lake Formation gates at the catalog layer.

Two response shapes, signaled by `format`:

| `format` | When | Key fields |
|---|---|---|
| `iceberg` | `catalog.type=iceberg` and the table is registered in Glue | `iceberg_table`, `snapshot_id`, `metadata_location`, `base_path` (Iceberg location) |
| `parquet_hive` | Default — also the fallback when Iceberg lookup fails | `base_path`, `base_epoch`, `latest_delta_epoch` |

`schema_version` is included in both shapes for cross-checking against the version stamp the consumer last cached. Role filtering matches the freshness endpoint.

### Governance Enforcement

The MCP server assumes an IAM role per agent class. Lake Formation checks that role's `SELECT` grants (driven by sensitivity/domain tags) before the query touches data. A `confidential`-tagged column never leaves S3 unless the role is granted access.

| Agent class | IAM role | LF grant | What it sees |
|---|---|---|---|
| Analyst agent | `mcp_analyst_role` | `sensitivity in [public, internal]` | Non-sensitive tables and columns |
| Monitor agent | `mcp_monitor_role` | `sensitivity in [public, internal]`, `domain in [ops]` | Operational data only |
| Compliance agent | `mcp_compliance_role` | `sensitivity in [public, internal, PII]` | Includes PII for audit purposes |

### Status

The MCP server ships in the v0.1.0 release line. Launch with `pg-cdc serve --mcp` (stdio transport — for Claude Desktop and other MCP clients). The full tool set is live: `list_tables`, `describe_table`, `query`, `get_changes`, `get_manifest`, `get_freshness`, `get_snapshot`, `get_diff`. See [`07-operations.md`](07-operations.md) for the operational surface and [`MCP + Query Layer Plan`](internal/plans/mcp-query-layer.md) for the design history.

## Path B — pg-warehouse (developers, local)

pg-warehouse pulls governed Parquet snapshots to a local DuckDB database. Developers query locally — fast, offline-capable, and governed by the same tags that gate the MCP server.

```
Developer laptop
  | pg-warehouse refresh --role analyst_role
  v
pg-warehouse
  | 1. Reads manifest.json from S3
  | 2. Resolves user's Postgres roles --> permitted tables/columns
  | 3. Pulls only permitted Parquet files
  | 4. Loads into local DuckDB
  v
Developer
  | SELECT * FROM orders (local, offline, fast)
```

### Governance Enforcement

**Today (Postgres-ACL profiles):** pg-warehouse reads role profiles from the manifest and filters tables/columns at pull time. The DBA controls access via `GRANT`/`REVOKE` in PostgreSQL.

**After Phase 5 (Lake Formation):** pg-warehouse calls LF before opening each Parquet file. Same tags that gate the MCP server gate the local pull. A developer with `analyst_role` sees the same columns whether they query via MCP or via local DuckDB.

For full details, see [Data Governance](06-governance.md).

## Same Tags, Two Enforcement Points

| | MCP Server (Path A) | pg-warehouse (Path B) |
|---|---|---|
| **Data source** | S3 Parquet (same files) | S3 Parquet (same files) |
| **Discovery** | manifest.json | manifest.json |
| **Tag authority** | DynamoDB ACL table | DynamoDB ACL table |
| **Tag propagation** | `pg-cdc acl sync` --> LF | `pg-cdc acl sync` --> LF |
| **Enforcement** | LF grants checked server-side before query | LF grants checked at pull time (Phase 5) |
| **Query engine** | DuckDB (embedded in server) | DuckDB (embedded locally) |
| **Auth** | IAM role (per agent class) | IAM role (per developer/team) |
| **Network** | HTTPS (MCP protocol) | Direct S3 access |
| **Offline capable** | No | Yes (after initial pull) |

The governance model is **tag-once, enforce-everywhere**. When security runs `pg-cdc acl set billing.payments --tag sensitivity=PII`, that tag gates both the MCP server and every pg-warehouse pull.

## The Shared Query Engine

Both paths need the same underlying capability: a governed DuckDB query over S3 Parquet. This is implemented as a shared package:

```
                     +--- MCP Server (AI agents)
                     |
S3 Parquet -- Query Engine --+
                     |
                     +--- pg-warehouse (developers)
                     |
                     +--- REST API (general consumers)
                     |
                     +--- pg-cdc query CLI (debugging)
```

The query engine:
1. Reads the manifest to discover available tables and schemas
2. Checks Lake Formation grants for the caller's identity
3. Executes SQL via DuckDB with the httpfs extension (reads S3 directly)
4. Returns only permitted columns

## Use Case Spectrum

```
Low value <----------------------------------------------> High value

Simple         Analytical        Autonomous        Autonomous
lookups        queries over      monitoring &      data
by ID          historical data   alerting          governance

Read replica   pg-cdc            pg-cdc            pg-cdc
wins here      wins here         wins here         wins here
```

Most AI agent use cases are analytical and historical, not transactional lookups. Agents ask questions like:

- "What changed in the orders table this week?"
- "Show me anomalies in the payment data"
- "Summarize customer activity trends"
- "Are there data quality issues in recent inserts?"

These are all Parquet-on-S3 queries, not point lookups.

## Additional Query Interfaces

Beyond the MCP server and pg-warehouse, the governed data layer supports:

| Interface | How it works | Best for |
|---|---|---|
| **Athena** | SQL via Glue catalog + LF grants | Ad-hoc queries, dashboards |
| **DuckDB (direct)** | Iceberg reads from S3 via Glue | Notebooks, data science |
| **Spark / Trino** | Iceberg-aware engines with LF integration | Large-scale analytics |
| **REST API** | HTTP endpoints over the query engine. Launch with `pg-cdc serve --http`. Loopback bind by default; bearer auth required for non-loopback bind. Full wire contract: [`spec/data-plane-api.md`](spec/data-plane-api.md); `rest:` config in [`02-configuration.md`](02-configuration.md#rest). | Non-AI integrations |
| **`pg-cdc query` CLI** | DuckDB query from the command line — mirrors the REST `/v1/query` contract for the same role / filter / select semantics. | Debugging, quick checks |

All interfaces read the same governed Parquet files. All respect the same Lake Formation tags.

## Getting Started

### For AI Agent Integration

1. Deploy pg-cdc against your PostgreSQL database ([Getting Started](01-getting-started.md))
2. Configure governance tags ([Data Governance](06-governance.md))
3. Launch the MCP server with `pg-cdc serve --mcp` (see [`07-operations.md`](07-operations.md))
4. Connect your agent via the MCP protocol

### For Developer Local Access

1. Deploy pg-cdc (same as above)
2. Install pg-warehouse on developer machines
3. Configure role profiles in PostgreSQL ([Data Governance](06-governance.md))
4. Run `pg-warehouse refresh` to pull governed data locally

## Related

- [Security Architecture](10-security-architecture.md) — why CDC creates a physical air gap
- [Data Governance](06-governance.md) — two governance models (Postgres-ACL + Lake Formation)
- [MCP + Query Layer Plan](internal/plans/mcp-query-layer.md) — implementation roadmap
- [Operations](07-operations.md) — `acl` subcommand for Layer-2 tag management
