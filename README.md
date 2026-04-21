# pg-cdc

PostgreSQL CDC server. Streams WAL changes into typed, compacted Parquet files in cloud storage. Follows the native Postgres replica pattern.

Pure Go. No CGO. Single binary.

## Architecture

```
PostgreSQL ──WAL──→ pg-cdc ──Parquet──→ Cloud Storage
                      │                  (S3/GCS/fs)
                      │
                      └──→ AWS Glue (optional: table catalog)
```

## Commands

```bash
pg-cdc init                       # Snapshot tables → base Parquet + manifest + (optional) Glue catalog
pg-cdc start                      # Stream WAL → delta Parquet epochs
pg-cdc compact                    # Merge deltas → new base snapshot (applies I/U/D, soft-deletes on 30d TTL)
pg-cdc status                     # Health: lag, LSN, epochs, tables
pg-cdc discover                   # List tables from Postgres
pg-cdc discover --acl             # Show role → table → column access map from Postgres GRANTs
pg-cdc teardown                   # Drop publication + replication slot
pg-cdc catalog register           # Register manifest tables in Glue without re-snapshotting
pg-cdc version                    # Print version
```

Full operations guide: [`docs/08-operations.md`](docs/08-operations.md).

## Install

```bash
# Linux (amd64)
curl -fsSL https://github.com/burnside-project/pg-cdc/releases/latest/download/pg-cdc_linux_amd64.tar.gz | tar xz
sudo install -m 0755 pg-cdc-linux-amd64 /usr/local/bin/pg-cdc

# macOS (Apple Silicon)
curl -fsSL https://github.com/burnside-project/pg-cdc/releases/latest/download/pg-cdc_darwin_arm64.tar.gz | tar xz
sudo install -m 0755 pg-cdc-darwin-arm64 /usr/local/bin/pg-cdc
```

Or build from source:

```bash
git clone https://github.com/burnside-project/pg-cdc
cd pg-cdc
make build
```

## Configuration

Minimal config (filesystem sink):

```yaml
# pg-cdc.yml
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

With S3 + AWS Glue catalog:

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

See [`docs/02-configuration.md`](docs/02-configuration.md) for the full reference.

## Commercial edition

pg-cdc integrates into a larger Burnside data platform with a closed-source extension that adds the governance, access-control, and compliance features needed for regulated deployments:

- Layer-2 tag governance (policy-as-code)
- DynamoDB-backed ACL registry with versioned audit trail
- AWS Lake Formation reconciliation (`acl diff`, `acl sync`)
- Emergency-override workflows with expiry
- Terraform stack for IAM / OIDC / governance provisioning
- Extended CLI: `pg-cdc acl register|get|set|diff|sync|list`
- HIPAA-ready deployment topology

Details: [`docs/commercial-edition.md`](docs/commercial-edition.md).

## Related repos

| Repo | Role |
|------|------|
| **pg-cdc** (this repo) | CDC server — WAL streaming, Parquet writing, compaction |
| [burnside-go](https://github.com/burnside-project/burnside-go) | Shared types — manifest spec, storage interface |

## Versioning

Release candidates auto-increment on push to main: `v0.1.0-rc1`, `v0.1.0-rc2`, ...

Stable releases are promoted from RCs via workflow dispatch.

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

## License

[Apache License 2.0](LICENSE)
