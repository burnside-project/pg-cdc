# Getting Started

## Prerequisites

**PostgreSQL 10+** with logical replication enabled:

```sql
-- Check WAL level (must be 'logical')
SHOW wal_level;

-- If not logical, update postgresql.conf and restart:
-- wal_level = logical
-- max_replication_slots = 4
-- max_wal_senders = 4
-- max_slot_wal_keep_size = 10GB
```

**PostgreSQL user** with replication privilege:

```sql
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'secure_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
-- Grant on additional schemas as needed
```

## Install

```bash
# Linux (amd64)
curl -fsSL https://github.com/burnside-project/pg-cdc/releases/latest/download/pg-cdc_linux_amd64.tar.gz | tar xz -C /tmp
sudo install -m 0755 /tmp/pg-cdc-linux-amd64 /usr/local/bin/pg-cdc
pg-cdc version
```

Or build from source:

```bash
git clone https://github.com/burnside-project/pg-cdc
cd pg-cdc
make build
sudo install -m 0755 pg-cdc /usr/local/bin/pg-cdc
```

## Quick Start (Filesystem)

### 1. Create config

```bash
sudo mkdir -p /etc/pg-cdc
```

```yaml
# /etc/pg-cdc/pg-cdc.yml
source:
  postgres:
    url: "postgresql://cdc_user:${PGCDC_PASSWORD}@localhost:5432/mydb"
    schemas: ["public"]

storage:
  type: filesystem
  path: /var/lib/pg-cdc/output/

replication:
  publication: pg_cdc_pub
  slot: pg_cdc_slot
```

### 2. Initialize — snapshot all tables

```bash
export PGCDC_PASSWORD=secret
pg-cdc init --config pg-cdc.yml
```

This creates:
- A PostgreSQL publication and replication slot
- A consistent snapshot of all tables as base Parquet files
- A `manifest.json` describing the table catalog

### 3. Start streaming

```bash
pg-cdc start --config pg-cdc.yml
```

Changes in PostgreSQL now appear as delta Parquet files in the output directory.

### 4. Verify

```bash
pg-cdc status --config pg-cdc.yml
```

## Quick Start (GCS)

For GCE VMs, the VM service account provides credentials automatically:

```yaml
# pg-cdc.yml
source:
  postgres:
    url: "postgresql://cdc_user@10.x.x.x:5432/mydb?sslmode=verify-full&sslcert=/etc/pg-cdc/tls/client.crt&sslkey=/etc/pg-cdc/tls/client.key&sslrootcert=/etc/pg-cdc/tls/ca.crt"

storage:
  type: gcs
  bucket: company-data-lake
  prefix: cdc/prod/
```

For on-prem, provide a service account key:

```yaml
storage:
  type: gcs
  bucket: company-data-lake
  prefix: cdc/prod/
  credentials_file: /etc/pg-cdc/gcp-sa-key.json
```

## Quick Start (S3)

For EC2, the instance role provides credentials automatically:

```yaml
# pg-cdc.yml
source:
  postgres:
    url: "postgresql://cdc_user:${RDS_AUTH_TOKEN}@mydb.xxx.rds.amazonaws.com:5432/mydb?sslmode=verify-full&sslrootcert=/etc/pg-cdc/rds-ca.pem"

storage:
  type: s3
  bucket: company-data-lake
  prefix: cdc/prod/
  region: us-east-1
```

## Running as a systemd Service

For production deployments, run pg-cdc as a systemd service.

### 1. Create the service user and directories

```bash
sudo useradd --system --shell /usr/sbin/nologin --home-dir /var/lib/pg-cdc pgcdc
sudo mkdir -p /var/lib/pg-cdc /etc/pg-cdc
sudo chown pgcdc:pgcdc /var/lib/pg-cdc
sudo chmod 750 /etc/pg-cdc
```

### 2. Install the binary and config

```bash
sudo cp pg-cdc /usr/local/bin/pg-cdc
sudo cp pg-cdc.yml /etc/pg-cdc/pg-cdc.yml
```

### 3. Install the service file

A ready-made unit file is included in [`deploy/systemd/pg-cdc.service`](../deploy/systemd/pg-cdc.service):

```bash
sudo cp deploy/systemd/pg-cdc.service /etc/systemd/system/
sudo systemctl daemon-reload
```

### 4. Enable and start

```bash
sudo systemctl enable --now pg-cdc
```

### 5. Verify

```bash
sudo systemctl status pg-cdc
sudo journalctl -u pg-cdc -f
```

To pass secrets without putting them in the config file, create `/etc/pg-cdc/pg-cdc.env`:

```bash
# /etc/pg-cdc/pg-cdc.env
PGCDC_PASSWORD=secure_password
```

Then add `EnvironmentFile=/etc/pg-cdc/pg-cdc.env` to the `[Service]` section of the unit file.

## Connect pg-warehouse

On a developer machine:

```yaml
# pg-warehouse.yml
source:
  storage: /var/lib/pg-cdc/output/    # or gs://bucket/cdc/prod/ or s3://bucket/cdc/prod/

duckdb:
  raw: raw.duckdb
  silver: silver.duckdb
  feature: feature.duckdb
```

```bash
pg-warehouse refresh
pg-warehouse build
```

## Next Steps

- [Configuration reference](02-configuration.md) — full `pg-cdc.yml` reference
- [Init & Snapshot](03-init.md) — what `pg-cdc init` does
- [Streaming](04-streaming.md) — start WAL streaming
- [Compaction](05-compaction.md) — merge deltas into base snapshots
- [Operations](08-operations.md) — status, discover, teardown, recovery
- [Commercial edition](commercial-edition.md) — governance, ACL registry, Lake Formation reconciliation (closed-source extension)
