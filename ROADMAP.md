# Roadmap

## pg-cdc Development Roadmap

### Phase 1 — Core Pipeline (Current)

- [x] CLI with Cobra (`init`, `start`, `compact`, `status`, `discover`, `teardown`, `catalog`, `version`)
- [x] PostgreSQL connectivity via `pgx/v5` with connection pooling
- [x] Logical replication via `pglogrepl` (publication + slot lifecycle)
- [x] Initial consistent snapshot with LSN anchoring
- [x] WAL event streaming (INSERT / UPDATE / DELETE)
- [x] Typed Parquet output (`parquet-go`, pure Go, no CGO)
- [x] Base + delta epoch model with per-table manifests
- [x] Compaction of deltas into new base snapshots, with soft-delete TTL
- [x] SQLite state database (LSN, epoch watermarks, table metadata)
- [x] Hexagonal architecture with clean port/adapter separation

### Phase 2 — Sinks and Catalog

- [x] Filesystem sink
- [x] S3 sink (incl. transfer-manager multipart upload)
- [x] GCS sink
- [x] AWS Glue catalog integration (`catalog register`)
- [x] Table include/exclude rules
- [x] Tag-based policy (`pii`, `ephemeral`, untagged defaults)
- [x] Role → table → column ACL discovery from PostgreSQL GRANTs (`discover --acl`)

### Phase 3 — Production Hardening

- [ ] Reconnect with exponential backoff on replication disconnects
- [ ] Schema change detection (column add/drop/type change) and re-snapshot flow
- [ ] Structured metrics and observability hooks (OpenTelemetry)
- [ ] Benchmarks and performance profiling harness
- [ ] Soak testing for long-running CDC streams
- [ ] Checkpoint resume testing across restarts and crash recovery
- [ ] Alerting hooks for slot lag thresholds

### Phase 4 — Ecosystem

- [ ] Docker Compose quickstart with sample schema and seeded data
- [ ] Example pipelines (multi-schema, high-throughput, wide tables)
- [ ] Iceberg sink
- [ ] Additional catalogs (Hive Metastore, Unity Catalog)
- [ ] Integration test suite against a real PostgreSQL matrix (13/14/15/16/17)
- [ ] Community contribution guides and first-good-issue labeling

---

## Commercial Edition Preview

The following capabilities ship with the closed-source commercial edition (see [`docs/commercial-edition.md`](docs/commercial-edition.md)):

- Layer-2 tag governance (policy-as-code)
- DynamoDB-backed ACL registry with versioned audit trail
- AWS Lake Formation reconciliation (`acl diff`, `acl sync`)
- Emergency-override workflows with expiry
- Terraform stack for IAM / OIDC / governance provisioning
- Extended CLI: `pg-cdc acl register|get|set|diff|sync|list`
- HIPAA-ready deployment topology

Commercial features extend the open-source core without modifying it.

---

*Roadmap items are subject to change based on community feedback and priorities.*
