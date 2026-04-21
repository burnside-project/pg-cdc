# Init & Snapshot

`pg-cdc init` creates a replication slot and snapshots all discoverable tables to base Parquet files. This follows the native PostgreSQL replica pattern — snapshot + LSN = zero-gap starting point.

## What Happens

```
pg-cdc init
  1. Connect to PostgreSQL
  2. Discover tables in configured schemas
  3. Apply tags + policies → filter included/excluded
  4. CREATE PUBLICATION for included tables only
  5. CREATE_REPLICATION_SLOT → capture LSN + export snapshot
  6. For each included table:
     SET TRANSACTION SNAPSHOT → COPY → typed Parquet
  7. Read PostgreSQL ACLs → build role profiles (if configured)
  8. Write manifest.json with full catalog
  9. If catalog.type = "glue":
     Register each active table in AWS Glue (idempotent)
```

## Usage

```bash
pg-cdc init --config pg-cdc.yml
```

## Output Structure

```
/var/lib/pg-cdc/output/
├── manifest.json
├── public.orders/
│   └── base/
│       └── base_000000.parquet
├── public.customers/
│   └── base/
│       └── base_000000.parquet
└── ...
```

## Manifest Content

After init, `manifest.json` contains:

- **Source**: PostgreSQL version, init LSN, timestamp
- **Active tables**: status, primary keys, schema (column types), row count, path
- **Excluded tables**: status=excluded, tags, reason (e.g., "policy:pii")
- **Schemas**: column-level type information (pg_type + parquet_type)
- **Roles**: per-role table + column access (if profiles configured)

## Filters at Init Time

Init is where table-inclusion decisions are baked in:

1. **Tags resolve** — each table is matched against tag definitions (glob patterns)
2. **Policies apply** — tagged tables checked against policy (include/exclude)
3. **Publication scoped** — only included tables go into the Postgres publication
4. **Excluded tables recorded** — manifest shows what was excluded and why
5. **ACL profiles written** — role-based access from PostgreSQL GRANTs (when `profiles.source: pg_acl`)

A table excluded at init time will never appear in the Parquet output or be streamed by `pg-cdc start`.

## Re-running Init

Init can only be run once per slot. To re-initialize:

```bash
pg-cdc teardown --config pg-cdc.yml    # drop slot + publication
pg-cdc init --config pg-cdc.yml         # create new slot + snapshot
```

## Catalog registration

When `catalog.type: glue` is set in `pg-cdc.yml`, init registers active tables in the configured Glue database after snapshotting. Registration is idempotent — re-running init on an existing deployment updates existing table definitions rather than failing.

> **Enabling Glue on a deployment that was already initialized?** Don't re-run `init` — it will re-snapshot every table from Postgres (duplicating GB of writes and requiring a slot teardown). Use **`pg-cdc catalog register`** instead: it reads the existing manifest and calls the same upsert path without touching Postgres. See the `catalog` subcommand in [`08-operations.md`](08-operations.md).

For DynamoDB-backed ACL registration and Lake Formation reconciliation, see the [commercial edition](commercial-edition.md).
