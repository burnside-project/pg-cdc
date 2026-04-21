# Operations

Day-to-day commands for monitoring, inspecting, and managing pg-cdc.

## Status

Show replication health:

```bash
pg-cdc status --config pg-cdc.yml
```

```
pg-cdc status

  Slot:          pg_cdc_slot (streaming)
  Confirmed LSN: 0/16B8920
  Restart LSN:   0/16B3740
  Init LSN:      0/16B3740
  Source:         pg_cdc_slot

  Tables:         14 active, 3 excluded
  Latest epoch:   42
  Storage:        /var/lib/pg-cdc/output/
  Last compacted: 2026-04-12 10:00:00
  Role profiles:  3
```

## Discover

List all tables with tags and policies:

```bash
pg-cdc discover --config pg-cdc.yml
```

Preview changes without applying:

```bash
pg-cdc discover --dry-run --config pg-cdc.yml
```

Show PostgreSQL ACL-based role profiles:

```bash
pg-cdc discover --acl --config pg-cdc.yml
```

## Teardown

Drop the publication and replication slot:

```bash
pg-cdc teardown --config pg-cdc.yml
```

This is required before re-running `pg-cdc init` (slots cannot be re-created while active).

**When to teardown:**
- Changing the table scope (new tags/policies)
- Changing the replication slot or publication name
- Decommissioning pg-cdc

## Recovery

### Stream stopped unexpectedly

```bash
# Check status
pg-cdc status --config pg-cdc.yml

# Restart — streaming resumes from the last confirmed LSN
pg-cdc start --config pg-cdc.yml
```

### Manifest corrupted

If `manifest.json` is lost or corrupted, the safest recovery is:

```bash
pg-cdc teardown --config pg-cdc.yml
# Delete output directory contents
rm -rf /var/lib/pg-cdc/output/*
pg-cdc init --config pg-cdc.yml
```

Downstream consumers will detect the gap (their local epoch < new base_epoch) and do a full re-pull automatically.

### PostgreSQL WAL accumulation

If WAL files are growing, check that pg-cdc is running and confirming LSNs:

```bash
# On the PostgreSQL server
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name = 'pg_cdc_slot';
```

If pg-cdc is stopped and WAL is growing, either start pg-cdc or drop the slot:

```bash
pg-cdc teardown --config pg-cdc.yml
```

## Running as a systemd Service

```ini
# /etc/systemd/system/pg-cdc.service
[Unit]
Description=pg-cdc CDC Server
After=postgresql.service

[Service]
Type=simple
User=pgcdc
ExecStart=/usr/local/bin/pg-cdc start --config /etc/pg-cdc/pg-cdc.yml
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable pg-cdc
sudo systemctl start pg-cdc
sudo journalctl -u pg-cdc -f
```

## Cron-Based Compaction

```bash
# Compact hourly
0 * * * * /usr/local/bin/pg-cdc compact --config /etc/pg-cdc/pg-cdc.yml >> /var/log/pg-cdc-compact.log 2>&1
```

## `catalog` subcommand

The `catalog` subcommand manages catalog (Glue) registration for an existing deployment. Use it when you've enabled `catalog.type` on a deployment that was previously snapshotted without it.

| Command | What it does |
|---|---|
| `pg-cdc catalog register` | Reads the existing manifest from storage and registers every active table in the configured Glue database. No Postgres work, no re-snapshot. Safe to re-run — registration is upsert semantics (CreateTable → UpdateTable fallback). |

**When to use this vs. `init`:**

- **Fresh deployment** → `pg-cdc init` (handles slot creation, snapshot, and catalog registration together).
- **Already-initialized deployment, now wiring Glue** → `pg-cdc catalog register` (metadata-only; ~seconds vs. re-snapshotting GB from Postgres).
- **Changed storage bucket/prefix and need Glue `Location` fields refreshed** → `pg-cdc catalog register` after updating `pg-cdc.yml`.
- **New table added via `ALTER PUBLICATION`** → `pg-cdc catalog register` picks it up from the manifest after `init` re-discovers it.

```bash
pg-cdc catalog register --config pg-cdc.yml
#  Glue catalog: <database> (<N> tables)
```

## `acl` subcommand

The `acl` subcommand group (Layer-2 tag intent in DynamoDB, Lake Formation reconciliation) is part of the [commercial edition](commercial-edition.md). It is not present in this binary.
