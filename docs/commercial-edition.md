# Commercial Edition

The code in this repository — pg-cdc — is a working, standalone Postgres CDC tool published under Apache 2.0. The **commercial edition** is a closed-source extension built by Burnside on top of pg-cdc that adds the governance, access-control, audit, and compliance features required for regulated production deployments.

This document describes what the commercial edition adds. It is informational only; the code is not distributed in this repository.

## What the commercial edition adds

### 1. Layer-2 tag governance

A policy-as-code block (`governance:` in `pg-cdc.yml`) that maps tables and columns to a tag taxonomy. Strict mode refuses to write any resource missing a required tag; permissive mode logs and continues.

Capabilities:
- **Required-tag enforcement** — e.g., every resource must carry `sensitivity`, `domain`, `owner`.
- **Pattern-matched overrides** — glob rules like `clinical.*` or `*.audit_log` assign tags declaratively.
- **Column-level tags** — override a single column within a matched table (e.g., mark the `email` column `PII` while the rest of the table is `internal`).
- **Emergency overrides** — out-of-band tag changes carrying `reason`, `created_by`, `created_at`, and a hard `expires_at`. Reconciliation refuses expired entries.
- **JSON Schema authority** — the `governance:` block is pinned by a versioned schema; validation runs at config-load time so misconfigurations fail loud, not silent.

### 2. DynamoDB-backed ACL registry

A versioned classification store, one item per manifest resource (`db.schema.table`). Each item carries:

| Field | Purpose |
|---|---|
| `resource_id` | `db.schema.table` identifier |
| `version` | Monotonic; `0` = unclassified, increments on every intent change |
| `direct_tags` | Tags set *at this resource* |
| `resolved_tags` | `direct_tags` merged with inherited values |
| `state` | `active` / `archived` |
| `last_intent_at` / `last_intent_by` / `last_intent_reason` | Audit trail (RFC3339 + principal ARN + free-text justification) |

The registry is the authoritative source of tag intent; Lake Formation is treated as a projection of this store.

### 3. Extended CLI

The commercial binary wraps the open-source pg-cdc with an `acl` subcommand tree:

```bash
pg-cdc acl register <resource> [--sensitivity=...]
pg-cdc acl get <resource>
pg-cdc acl set <resource> --tag key=value --reason "INC-2345 …"
pg-cdc acl list [--unclassified]
pg-cdc acl diff                   # exit 2 on drift
pg-cdc acl sync                   # exit 3 on drift healed, 0 on clean
```

Exit codes are stable and designed for CI consumption — `acl-reconcile.yml` distinguishes "LF already matches intent" (0), "AWS API error" (2), and "drift healed, config reconciled" (3).

### 4. AWS Lake Formation reconciliation

`pg-cdc acl diff` computes the plan (`add`/`remove` ops) between DynamoDB intent and live Lake Formation tags. `pg-cdc acl sync` applies it through `AddLFTagsToResource` / `RemoveLFTagsFromResource`. Every plan item emits an audit event before apply.

Supports idempotent re-runs — the LF Add API is idempotent at the value level, so replayed plans converge rather than error.

### 5. Terraform infrastructure stack

Provisioning module (`deploy/terraform/governance/`) that stands up the full governance plane in one `terraform apply`:

| Component | Purpose |
|---|---|
| DynamoDB ACL table | The intent registry |
| DynamoDB Streams → Kinesis Firehose → S3 | Append-only audit pipeline (immutable log of every classification change) |
| AWS Glue database | Target catalog for registered tables |
| Lake Formation LF-Tag taxonomy | Layer-1 tag key/value namespace (e.g., `sensitivity: [public, internal, confidential, PII, PHI, PCI]`) |
| LF-Tag permissions | Role-keyed `ASSOCIATE` / `DESCRIBE` grants |
| GitHub OIDC provider | Keyless AWS auth for workflows |
| Per-role IAM scopes | pg-cdc worker, tag applier, ACL writer — each default-deny |

A starter root stack is shipped (`deploy/terraform/governance/examples/basic/main.tf`) showing the minimum variable set.

### 6. GitHub Actions workflows

| Workflow | Trigger | Purpose |
|---|---|---|
| `acl-apply.yml` | Manual (`workflow_dispatch`) | Authoritative write path: set ACL intent in DynamoDB + reconcile to LF in one audited run |
| `acl-reconcile.yml` | Schedule (every 6h) + manual | Periodic `pg-cdc acl sync` per deployment — heals drift from console edits, out-of-band writes, failed prior runs |
| `lf-tags-apply.yml` | Manual | Ad-hoc direct LF tag operations with audit-log commit (being superseded by the ACL-driven flow) |

Each workflow commits a CSV audit row to a git-tracked log before exiting, so every tag change is traceable to actor, reason, and Actions run URL.

### 7. Deployment topologies

| Topology | What it stands for |
|---|---|
| HIPAA | Default-deny IAM, KMS-CMK at rest, VPC-only egress, PHI tag taxonomy, column-level LF grants, immutable audit log |
| Least-privilege | Four-pillar posture (auth, authorization, audit, observability) — the baseline non-regulated deployment |

Each topology ships with Terraform variable overlays and a deployment runbook.

### 8. Cross-repo contracts

A governed spec directory (`docs/spec/`) holds cross-repo contracts:
- `governance.schema.json` — authoritative Layer-2 policy schema, consumed by pg-cdc validator and downstream consumers.
- `gold-layer-contract.md` — what every promoted gold-layer artifact must carry (tags, metadata, retention).

Changes to these specs go through PR review with security sign-off before implementations are adjusted.

## What is *not* in the commercial edition

These are in open-source pg-cdc and stay there:

- WAL streaming, logical decoding, publication management
- Parquet writing, schema evolution, compaction
- Manifest format + sink adapters (filesystem, S3, GCS)
- State store (SQLite)
- AWS Glue catalog registration (generic — no governance coupling)
- Postgres-native ACL introspection (`discover --acl` reads `information_schema`)
- Tag-based table filtering (`tables.tags` + `tables.policy`)

The commercial edition *depends on* open-source pg-cdc — it is built as an extension, not a fork.

## Licensing

The commercial edition is distributed by Burnside under a commercial license. For evaluation, pricing, or deployment assistance, contact your Burnside representative.
