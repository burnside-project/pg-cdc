# Configuration

pg-cdc is configured via `pg-cdc.yml`. Pass `--config <path>` to override the default location.

## Full Reference

```yaml
# pg-cdc.yml

source:
  postgres:
    url: "postgresql://cdc_user:${PGCDC_PASSWORD}@host:5432/db"
    schemas: ["public", "billing"]      # schemas to discover tables from

storage:
  type: filesystem                      # filesystem | gcs | s3
  path: /var/lib/pg-cdc/output/         # filesystem: output directory

  # ── GCS ──
  # type: gcs
  # bucket: company-data-lake           # GCS bucket name
  # prefix: cdc/prod/                   # object key prefix
  # credentials_file: /etc/pg-cdc/sa.json  # optional — uses ADC if omitted

  # ── S3 ──
  # type: s3
  # bucket: company-data-lake           # S3 bucket name
  # prefix: cdc/prod/                   # object key prefix
  # region: us-east-1                   # AWS region
  # endpoint: ""                        # custom endpoint (MinIO, R2)

  partition_by_tag: false               # group output by tag (for IAM enforcement)

replication:
  publication: pg_cdc_pub               # PostgreSQL publication name
  slot: pg_cdc_slot                     # replication slot name

flush:
  interval_sec: 10                      # write delta Parquet every N seconds
  max_rows: 1000                        # or every N rows (whichever comes first)

state:
  type: sqlite                          # sqlite (default)
  path: .pgcdc/state.db                 # SQLite file path

tables:
  exclude:                              # explicit exclusions (exact or glob)
    - public.tbl_sessions
    - "*.tbl_cache_*"

  tags:                                 # classify tables by tag
    pii:
      - public.tbl_cc
      - public.tbl_user_emails
      - "billing.*"
    high_volume:
      - public.tbl_events
      - public.tbl_audit_log
    ephemeral:
      - "*.tbl_session*"
      - "*.tbl_cache_*"

  policy:                               # what to do with each tag
    pii: exclude                        # never replicate PII tables
    ephemeral: exclude                  # skip ephemeral tables
    high_volume: include                # replicate (but may use different compaction)
    untagged: include                   # default for untagged tables

profiles:
  source: pg_acl                        # derive profiles from PostgreSQL GRANTs
  roles:                                # which Postgres roles to publish
    - analyst_role
    - ml_role
    - pii_admin
  aliases:                              # friendly names for roles
    analyst_role: analyst
    ml_role: ml
  sync_interval: 1h                     # how often to re-read ACLs

catalog:
  type: glue                            # "" (disabled) or "glue"
  database: my_db                       # Glue database name
  region: us-west-2                     # AWS region (defaults to storage.region)
  endpoint: ""                          # optional custom endpoint (LocalStack / testing)
```

## Sections

### source.postgres

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `url` | Yes | | PostgreSQL connection string (supports `${ENV_VAR}` substitution) |
| `schemas` | No | `["public"]` | Schemas to discover tables from |

### storage

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | No | `filesystem` | Storage backend: `filesystem`, `gcs`, `s3` |
| `path` | Yes (filesystem) | | Output directory path |
| `bucket` | Yes (gcs/s3) | | Cloud storage bucket name |
| `prefix` | No | `""` | Object key prefix (e.g., `cdc/prod/`) |
| `region` | Yes (s3) | | AWS region |
| `endpoint` | No | | Custom S3-compatible endpoint (MinIO, R2) |
| `credentials_file` | No | | GCS service account JSON path (uses ADC if omitted) |
| `partition_by_tag` | No | `false` | Group tables by tag in storage paths |

### replication

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `publication` | No | `pg_cdc_pub` | PostgreSQL publication name |
| `slot` | No | `pg_cdc_slot` | Replication slot name |

### flush

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `interval_sec` | No | `10` | Flush delta Parquet every N seconds |
| `max_rows` | No | `1000` | Or every N rows |

### state

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | No | `sqlite` | State backend (only `sqlite` currently) |
| `path` | No | `.pgcdc/state.db` | SQLite file path |

### tables

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `exclude` | No | `[]` | Tables to skip (exact names or glob patterns) |
| `tags` | No | `{}` | Tag definitions: tag name to list of table patterns |
| `policy` | No | `{}` | Tag policies: tag name to `include` or `exclude` |

### profiles

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `source` | No | | Set to `pg_acl` to derive from PostgreSQL |
| `roles` | No | `[]` | Which Postgres roles to publish in manifest |
| `aliases` | No | `{}` | Role name to friendly alias mapping |
| `sync_interval` | No | `1h` | How often to re-read ACLs |

### catalog

Optional metadata-catalog integration. When `type: glue`, `pg-cdc init` and `pg-cdc catalog register` register active tables in AWS Glue after the manifest is written.

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | No | `""` | `""` disables catalog registration; `glue` enables AWS Glue registration |
| `database` | Yes (glue) | | Glue database name |
| `region` | No | `storage.region` | AWS region for Glue calls |
| `endpoint` | No | | Custom endpoint (LocalStack / testing) |

For Layer-2 tag governance (required-tag enforcement, column-level overrides, Lake Formation reconciliation), see the [commercial edition](commercial-edition.md).

## Example Configurations

### On-prem PostgreSQL → local filesystem

```yaml
source:
  postgres:
    url: "postgresql://cdc_user@db-host:5432/mydb?sslmode=verify-full&sslcert=/etc/pg-cdc/tls/client.crt&sslkey=/etc/pg-cdc/tls/client.key&sslrootcert=/etc/pg-cdc/tls/ca.crt"

storage:
  type: filesystem
  path: /var/lib/pg-cdc/output/
```

### On-prem PostgreSQL → GCS (with service account key)

```yaml
source:
  postgres:
    url: "postgresql://cdc_user@db-host:5432/mydb?sslmode=verify-full&sslcert=/etc/pg-cdc/tls/client.crt&sslkey=/etc/pg-cdc/tls/client.key&sslrootcert=/etc/pg-cdc/tls/ca.crt"

storage:
  type: gcs
  bucket: company-data-lake
  prefix: cdc/prod/
  credentials_file: /etc/pg-cdc/gcp-sa-key.json
```

### GCE VM → GCS (VM service account — no key files)

```yaml
source:
  postgres:
    url: "postgresql://cdc_user@10.x.x.x:5432/mydb?sslmode=verify-full&sslcert=/etc/pg-cdc/tls/client.crt&sslkey=/etc/pg-cdc/tls/client.key&sslrootcert=/etc/pg-cdc/tls/ca.crt"

storage:
  type: gcs
  bucket: company-data-lake
  prefix: cdc/prod/
```

### Cloud SQL + Auth Proxy → GCS (fully managed)

```yaml
source:
  postgres:
    url: "postgresql://sa-pgcdc@myproject.iam@localhost:5432/mydb?sslmode=disable"

storage:
  type: gcs
  bucket: company-data-lake
  prefix: cdc/prod/
```

### AWS RDS → S3 (IAM everywhere)

```yaml
source:
  postgres:
    url: "postgresql://cdc_user:${RDS_AUTH_TOKEN}@mydb.xxx.us-east-1.rds.amazonaws.com:5432/mydb?sslmode=verify-full&sslrootcert=/etc/pg-cdc/rds-ca.pem"

storage:
  type: s3
  bucket: company-data-lake
  prefix: cdc/prod/
  region: us-east-1
```
