**Stream PostgreSQL WAL → governed Apache Iceberg on S3. Purpose-built for AWS enterprise workloads where zero-trust data governance, auditability, and operational rigor are non-negotiable.**


## Enterprise Overview

| Property | Detail |
|---|---|
| **Deployment model** | Single binary deployed on EC2, ECS (Fargate), or EKS |
| **Source databases** | Amazon RDS PostgreSQL, Aurora PostgreSQL, self-managed PostgreSQL 14+ |
| **Target format** | Apache Iceberg (default) or Hive-style Parquet on S3 |
| **Catalog** | AWS Glue Catalog |
| **Governance** | AWS Lake Formation (tag-based, column-level), DynamoDB-backed ACL registry |
| **Consumption paths** | MCP (Model Context Protocol), REST API, Athena, Spark, Trino, DuckDB |
| **License** | Apache 2.0 |

**Key design properties:**

- **No return path** — WAL is one-way; Parquet on S3 is immutable. Consumers cannot write to production.
- **No database credentials for consumers** — authentication is via AWS IAM + Lake Formation only.
- **Governed by default** — Lake Formation tags gate every read; untagged data is invisible.
- **Time travel** — every flush is an Iceberg snapshot; immutable `raw@<ts>` tags enable point-in-time queries.

---

## AWS Reference Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            AWS Account / OU                                 │
│                                                                             │
│  ┌──────────────┐     ┌──────────────────────────────────────────────────┐  │
│  │  Source VPC   │     │                   Data VPC                       │  │
│  │               │     │                                                  │  │
│  │  ┌─────────┐  │     │  ┌─────────────────┐    ┌──────────────────┐    │  │
│  │  │  RDS /  │  │     │  │  pg-cdc (ECS)    │    │  pg-warehouse    │    │  │
│  │  │ Aurora  │──┼─────┼─▶│  WAL → S3        │    │  (analytics)     │    │  │
│  │  │ (WAL)   │  │     │  │  ┌───────────┐   │    └──────────────────┘    │  │
│  │  └─────────┘  │     │  │  │ DuckDB    │   │    ┌──────────────────┐    │  │
│  └──────────────┘     │  │  │ (read)     │   │    │  AI Agents       │    │  │
│                        │  │  └───────────┘   │    │  (MCP / REST)    │    │  │
│                        │  └────────┬─────────┘    └────────┬─────────┘    │  │
│                        │           │                       │              │  │
│                        │           ▼                       ▼              │  │
│                        │  ┌──────────────────────────────────────────┐    │  │
│                        │  │         Amazon S3 (Iceberg)              │    │  │
│                        │  │         + AWS KMS (SSE-KMS)             │    │  │
│                        │  └────────────────────┬─────────────────────┘    │  │
│                        │                        │                        │  │
│                        │                        ▼                        │  │
│                        │  ┌──────────────────────────────────────────┐    │  │
│                        │  │         AWS Glue Catalog                 │    │  │
│                        │  │         + Lake Formation                 │    │  │
│                        │  │         + DynamoDB ACL Registry          │    │  │
│                        │  └──────────────────────────────────────────┘    │  │
│                        │                                                  │  │
│                        │  ┌──────────────────────────────────────────┐    │  │
│                        │  │  Observability                          │    │  │
│                        │  │  CloudWatch + Prometheus + Grafana      │    │  │
│                        │  └──────────────────────────────────────────┘    │  │
│                        └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Network Architecture

- **Source VPC**: Contains RDS/Aurora in private subnets. pg-cdc connects via VPC Peering or Transit Gateway. No public internet access.
- **Data VPC**: Contains pg-cdc compute (ECS Fargate / EKS / EC2) in private subnets with:
  - **VPC Endpoints** for S3 (Gateway), Glue, Lake Formation, DynamoDB, CloudWatch, KMS, STS
  - No NAT Gateway required for S3 + service endpoints
  - Outbound internet only if Aurora IAM auth is used (STS endpoint)
- **Security Groups**: Least-privilege ingress from Data VPC CIDR to source DB port 5432

---

## Deployment Prerequisites

### Required AWS Services (must be provisioned before pg-cdc deployment)

| Service | Purpose | Provisioned By |
|---|---|---|
| **S3 Bucket** | Iceberg/Parquet storage | Terraform / CDK |
| **AWS Glue Catalog Database** | Table metadata registration | Terraform / CDK |
| **AWS Lake Formation** | Tag-based access governance | Terraform / CDK |
| **DynamoDB Table** | ACL intent registry | Terraform / CDK |
| **KMS Key** | S3 SSE-KMS encryption | Terraform / CDK |
| **IAM Roles** | pg-cdc execution role, consumer roles | Terraform / CDK |
| **CloudWatch Log Group** | Audit log sink | Terraform / CDK |
| **RDS / Aurora** | Source PostgreSQL with logical replication enabled | Platform team |

### Source Database Requirements

- PostgreSQL 14+
- `rds.logical_replication = 1` (RDS/Aurora parameter group)
- `wal_level = logical`
- Replication user with `rds_replication` role (RDS) or `REPLICATION` privilege (self-managed)
- Publication created for target tables
- Replica identity set to `FULL` on all replicated tables (`ALTER TABLE t REPLICA IDENTITY FULL`)

### IAM Permissions for pg-cdc Execution Role

```json
{
  "S3": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"],
  "Glue": ["glue:CreateTable", "glue:UpdateTable", "glue:GetTable", "glue:GetTables", "glue:BatchCreatePartition"],
  "LakeFormation": ["lakeformation:AddLFTagsToResource", "lakeformation:RemoveLFTagsFromResource",
                    "lakeformation:GetResourceLFTags", "lakeformation:ListPermissions",
                    "lakeformation:GetEffectivePermissionsForPath"],
  "DynamoDB": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Scan", "dynamodb:Query"],
  "KMS": ["kms:Decrypt", "kms:GenerateDataKey", "kms:Encrypt"],
  "CloudWatch": ["logs:PutLogEvents", "logs:CreateLogStream", "logs:DescribeLogGroups"],
  "STS": ["sts:AssumeRole"]
}
```

---

## Quick Start (Sandbox)

For single-instance evaluation, see [docs/quickstart.md](docs/quickstart.md).

```bash
# Download the binary
gh release download --repo dataalgebra-engineering/burnside-project-pg-cdc \
  --pattern 'pg-cdc_*_linux_amd64.tar.gz' --dir /tmp
tar xzf /tmp/pg-cdc_*_linux_amd64.tar.gz -C /tmp
sudo install -m 0755 /tmp/pg-cdc-linux-amd64 /usr/local/bin/pg-cdc

# Pre-flight check
pg-cdc preflight

# Initialize snapshot
pg-cdc init

# Start WAL streaming
pg-cdc start
```

---

## Production Deployment Guide

### 1. Infrastructure Provisioning

Deploy the governance infrastructure using the provided Terraform module:

```bash
cd deploy/terraform/governance

# Configure backend
terraform init -backend-config=bucket=my-tf-state -backend-config=key=pg-cdc/terraform.tfstate

# Deploy
terraform plan -var-file=environments/prod.tfvars
terraform apply -var-file=environments/prod.tfvars
```

The module provisions:

| Resource | Configuration |
|---|---|
| S3 bucket | `my-org-governed-warehouse` with `aws:kms` SSE, bucket policy denying unencrypted writes, `block-public-access` set, versioning enabled |
| Glue database | `pgcdc_prod_{{region}}` with Lake Formation default permissions removed |
| DynamoDB ACL table | `pgcdc-acl-registry` with PAY_PER_REQUEST billing, KMS encryption |
| KMS key | Customer-managed key with annual rotation, least-privilege key policy |
| IAM roles | `pgcdc-execution-role`, `pgcdc-consumer-*` roles with Lake Formation tag-based grants |
| CloudWatch log group | `/aws/pg-cdc/prod` with 365-day retention |
| Lake Formation default-deny | `IAMAllowedPrincipals` disabled on Glue database |
| VPC endpoints | S3 Gateway, Glue, Lake Formation, DynamoDB, CloudWatch Logs, KMS, STS |

**Customizing for your environment:**

- Adjust retention, KMS key administrators, and tag taxonomies in `terraform.tfvars`
- Cross-account S3 access requires additional bucket policies (see `docs/deployment/cross-account.md`)
- For govcloud/c2s, see `docs/deployment/govcloud.md`

### 2. Configuration

#### Production pg-cdc.yml

```yaml
source:
  postgres:
    url: "postgresql://cdc_user@my-db.abc.us-west-2.rds.amazonaws.com:5432/proddb?sslmode=require"
    schemas:
      - public
      - orders
      - inventory
    provider: rds                    # auto | self-managed | rds | aurora
    iam_auth: true                   # short-lived IAM tokens, no password stored
    excluded_tables:
      - "public.internal_audit_log"  # tables to skip

storage:
  type: s3
  bucket: my-org-governed-warehouse
  prefix: cdc/prod/
  region: us-west-2
  encryption:
    type: sse-kms
    kms_key_id: arn:aws:kms:us-west-2:123456789012:key/abc-123

catalog:
  type: iceberg
  warehouse: s3://my-org-governed-warehouse/cdc/prod/
  database: pgcdc_prod_us_west_2
  region: us-west-2
  acl_table: pgcdc-acl-registry
  snapshot_retention: 100            # Iceberg snapshots retained per table

governance:
  catalog: glue
  mode: strict                       # refuse to publish untagged tables
  require_tags:
    - sensitivity
    - data_classification
    - retention_class

replication:
  publication: pgcdc_prod_pub
  slot: pgcdc_prod_slot
  heartbeat_interval_sec: 30
  exclude_empty_changes: true

flush:
  interval_sec: 10                   # flush at least every 10 seconds
  max_rows: 50000                    # or every 50k rows
  max_bytes: 134217728              # or every 128 MiB (OOM guardrail)

guardrail:
  max_retained_wal_bytes: 10737418240  # alarm if > 10 GiB WAL pinned
  conflict_threshold: 10             # breaker trips after 10 CAS conflicts
  conflict_window_sec: 60

state:
  type: s3                           # store state on S3 for crash recovery
  path: s3://my-org-pg-cdc-state/

logging:
  level: info
  audit: true                        # enable CloudWatch audit log
  format: json                       # structured JSON for CloudWatch Logs Insights

tracing:
  enabled: true
  provider: xray                     # AWS X-Ray for distributed tracing
```

### 3. Deployment Options

#### Option A: ECS Fargate (Recommended)

```yaml
# ecs-service.yaml — deploy via CDK or CloudFormation
family: pg-cdc
networkMode: awsvpc
executionRoleArn: arn:aws:iam::123456789012:role/pgcdc-ecs-execution
taskRoleArn: arn:aws:iam::123456789012:role/pgcdc-execution-role
cpu: 2048                          # 2 vCPU
memory: 8192                       # 8 GB
containerDefinitions:
  - name: pg-cdc
    image: 123456789012.dkr.ecr.us-west-2.amazonaws.com/pg-cdc:latest
    essential: true
    command: ["start"]
    secrets:
      - name: PGCDC_PASSWORD
        valueFrom: arn:aws:secretsmanager:us-west-2:123456789012:secret:pgcdc/prod/db
    logConfiguration:
      logDriver: awslogs
      options:
        awslogs-group: /aws/pg-cdc/prod
        awslogs-region: us-west-2
        awslogs-stream-prefix: ecs
    mountPoints:
      - sourceVolume: ephemeral
        containerPath: /var/lib/pg-cdc
    healthCheck:
      command: ["CMD-SHELL", "pg-cdc status --json || exit 1"]
      interval: 30
      retries: 3
      startPeriod: 60
```

**Why Fargate:**
- No EC2 management (no patching, no autoscaling groups)
- AWS-managed infrastructure
- 2 vCPU / 8 GB handles most production workloads (see [Scaling Guidance](#scaling-guidance))
- Service auto-recovery via ECS service scheduler
- Integration with CloudWatch Container Insights

#### Option B: EKS / Kubernetes

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pg-cdc
  namespace: data-platform
spec:
  replicas: 1             # pg-cdc is currently single-active; standby replica for DR
  strategy:
    type: Recreate        # no rolling update — only one instance streams WAL
  template:
    spec:
      serviceAccountName: pgcdc
      containers:
        - name: pg-cdc
          image: 123456789012.dkr.ecr.us-west-2.amazonaws.com/pg-cdc:latest
          args: ["start"]
          resources:
            requests:
              cpu: "1"
              memory: "4Gi"
            limits:
              cpu: "2"
              memory: "8Gi"
          volumeMounts:
            - name: config
              mountPath: /etc/pg-cdc
              readOnly: true
            - name: state
              mountPath: /var/lib/pg-cdc
      volumes:
        - name: config
          configMap:
            name: pgcdc-config
        - name: state
          persistentVolumeClaim:
            claimName: pgcdc-state
```

**Production considerations for EKS:**
- Use `topologySpreadConstraints` to spread across AZs
- Use `podAntiAffinity` to prevent multiple replicas on the same node
- Configure `PodDisruptionBudget` with `maxUnavailable: 0`
- Use EBS gp3 volume for state (not `emptyDir`)
- Configure S3 VPC endpoint for the cluster

#### Option C: EC2 (for maximum control)

See `deploy/ec2/README.md` for the hardened AMI build, systemd unit, and CloudFormation template.

### 4. Post-Deployment Validation

```bash
# Run the smoke test suite
./e2e-soak.sh --duration 300 --config /etc/pg-cdc/pg-cdc.yml

# Verify the pipeline
pg-cdc status --json | jq '.replication.lag_bytes, .epochs.committed'

# Verify Lake Formation governance
pg-cdc acl diff                           # exits 0 if in sync
pg-cdc acl list --unclassified            # finds ungoverned tables

# Test consumer access
pg-cdc serve --http &
curl -H "Authorization: Bearer $(aws iam get-access-key ...)" \
  http://localhost:8080/v1/status

# Verify metrics endpoint
curl http://localhost:9090/metrics | grep pgcdc_
```

---

## Security Architecture

### Five-Property Security Model

| Property | Implementation |
|---|---|
| **Authentication** | AWS IAM roles (execution + consumer), RDS IAM auth (no static passwords), mTLS optional |
| **Authorization** | Lake Formation tag-based access control, column-level grants, DynamoDB ACL intent registry |
| **Encryption at rest** | S3 SSE-KMS, DynamoDB KMS encryption, CloudWatch Logs encryption |
| **Encryption in transit** | TLS 1.2+ for PostgreSQL, S3 (TLS), all API calls via VPC endpoints |
| **Audit** | CloudWatch Logs (365-day retention), immutable `events.ndjson`, CloudTrail for AWS API calls |

### Network Security

- All traffic stays within AWS network — no public internet
- VPC Gateway Endpoints for S3 and DynamoDB (no data leaves AWS network)
- VPC Interface Endpoints for Glue, Lake Formation, KMS, CloudWatch, STS
- Security group: allow PostgreSQL port only from pg-cdc security group
- No public subnets required

### Secrets Management

- Database passwords in AWS Secrets Manager with automatic rotation
- RDS IAM auth eliminates static passwords entirely (recommended)
- All secrets accessed via IAM execution role, never written to disk
- Config file references secrets via `${SECRET_NAME}` substitution

### Break-Glass Procedure

For emergency access to governed data without standard approval:

1. Authorized user triggers via GitHub Environment with OIDC role
2. Lake Formation grants a time-boxed (configurable TTL) full-access tag
3. CloudWatch Logs audit records the grant, grantor, reason, and TTL
4. Automated step function revokes the grant after TTL expiration
5. Weekly compliance report flags any break-glass activations

---

## High Availability & Disaster Recovery

### Availability Design

| Component | Strategy | RTO | RPO |
|---|---|---|---|
| pg-cdc process | ECS/EKS health check + auto-restart; ECS service recovery | < 2 min | 0 (no data loss) |
| PostgreSQL source | RDS Multi-AZ automatic failover | 1-2 min | 0 (WAL preserved) |
| S3 data | S3 Standard (99.99% availability, 11 9s durability) | 0 | 0 |
| Glue Catalog | AWS-managed, multi-AZ | < 1 min | 0 |
| Lake Formation | AWS-managed, multi-AZ | < 1 min | 0 |
| DynamoDB ACL | DynamoDB Standard table class, DAX accelerator | < 1 min | 0 |

**Expected composite availability: 99.95%** (with multi-AZ RDS + Fargate across 2 AZs)

### Failure Scenarios

| Scenario | Behavior | Recovery |
|---|---|---|
| pg-cdc process crashes | WAL slot stays open; `confirmed_flush_lsn` unchanged on last durable commit | Auto-restart replays from last flushed LSN |
| EC2 / Fargate instance fails | ECS/K8s reschedules on healthy host | New container resumes from last durable state |
| RDS failover | Connection drops; pgx reconnection with backoff (1s→30s, 10 attempts) | New primary detected; replication slot preserved on RDS |
| S3 transient error | SDK retry middleware (bounded exponential backoff) | Automatic |
| S3 regional outage | Manual failover to warm standby region | See DR section below |
| DynamoDB outage | ACL diff/sync fails; **reads continue with cached permissions** (5-min LF cache) | No impact on data pipeline; governance degrades to last-good state |

### Disaster Recovery (Cross-Region)

**Active / Warm Standby** (recommended for enterprise):

1. **Primary region**: pg-cdc streaming to S3 + Glue in `us-west-2`
2. **Standby region**: S3 cross-region replication (CRR) with ` replica/` prefix, standby Glue database
3. **Failover** (manual, < 30 min):
   - Promote CRR bucket to primary (remove ` replica/` prefix)
   - Update Glue catalog location to point to promoted bucket
   - Redeploy pg-cdc in standby region connecting to DR RDS replica
   - Update DNS / consumer configuration
4. **Fallback**: Reverse the process once primary region is healthy

### State Backup

pg-cdc state (SQLite DB + `refs.json`) must be backed up for crash recovery:

```bash
# Automated via Terraform (S3 state path)
pg-cdc status --json > /var/lib/pg-cdc/state/backup.json
aws s3 cp /var/lib/pg-cdc/state/ s3://my-org-pg-cdc-state/ --recursive
```

If using `state.type: s3`, state is automatically written to S3 after each flush.

---

## Monitoring & Observability

### Prometheus Metrics (29 gauge/counter/histogram)

| Category | Key Metrics | Alarm Threshold |
|---|---|---|
| Replication lag | `pgcdc_replication_lag_bytes` | > 10 GiB (WAL guardrail) |
| Throughput | `pgcdc_events_ingested_total`, `pgcdc_bytes_written_total` | Trending, no fixed threshold |
| Sink latency | `pgcdc_sink_flush_duration_seconds` | p99 > 60s |
| Errors | `pgcdc_sink_errors_total`, `pgcdc_cas_conflicts_total` | > 0 over 5min |
| Compaction | `pgcdc_compaction_duration_seconds` | > 300s |
| Governance drift | `pgcdc_acl_drift_items` | > 0 (non-compliant) |
| Backpressure | `pgcdc_channel_full_seconds_total` | > 30s over 5min |

### CloudWatch Dashboard

Included Terraform module deploys an 11-panel dashboard showing:
- Replication lag (bytes and seconds)
- Events ingested per second
- Flush duration (p50, p95, p99)
- S3 write throughput and error rate
- Compaction status and duration
- ACL drift count
- Lake Formation permission cache hit rate
- Iceberg snapshot count per table
- Slot WAL retention

### CloudWatch Alarms (9 included)

| Alarm | Metric | Threshold | Action |
|---|---|---|---|
| `HighReplicationLag` | `pgcdc_replication_lag_bytes` | > 10 GiB for 5 min | PagerDuty critical |
| `HighFlushErrorRate` | `pgcdc_sink_errors_total` rate | > 0 for 5 min | PagerDuty critical |
| `ACLDriftDetected` | `pgcdc_acl_drift_items` | > 0 for 15 min | PagerDuty warning |
| `CompactionHighDuration` | `pgcdc_compaction_duration_seconds` | > 300s | PagerDuty warning |
| `CASConflictRateHigh` | `pgcdc_cas_conflicts_total` | > 10 in 60s | PagerDuty critical |
| `BackpressureHigh` | `pgcdc_channel_full_seconds_total` | > 30s in 5 min | PagerDuty warning |
| `SourceConnectionFailing` | `pgcdc_source_disconnects_total` | > 3 in 5 min | PagerDuty critical |
| `SlotStorageNearLimit` | `pgcdc_slot_retained_bytes` | > 80% of `max_retained_wal_bytes` | PagerDuty warning |
| `GovernanceCompliance` | `pgcdc_acl_drift_items` | > 0 for 4h | Compliance report |

### Logging

| Stream | Destination | Format | Retention |
|---|---|---|---|
| Application logs | CloudWatch Logs (`/aws/pg-cdc/prod`) | JSON | 365 days |
| Audit events | CloudWatch Logs (`/aws/pg-cdc/audit`) + `events.ndjson` | JSON | 365 days |
| WAL decode errors | CloudWatch Logs | JSON | 365 days |
| ACL changes | DynamoDB Streams → EventBridge → S3 | JSON | 7 years (S3) |

### Observability Stack Integration

- **Prometheus**: Scrape `:9090/metrics`
- **Grafana**: Pre-built dashboard in `deploy/grafana/pg-cdc.json`
- **PagerDuty**: Via CloudWatch Alarm → SNS → PagerDuty
- **Slack**: Via CloudWatch Alarm → SNS → Lambda → Slack webhook
- **AWS X-Ray**: Trace end-to-end WAL → S3 requests when `tracing.enabled: true`

---

## Operations Runbook

### Daily Operations

```bash
# Health check
pg-cdc status

# View lag
pg-cdc status --json | jq '.replication.lag_bytes, .replication.lag_seconds'

# Check governance compliance
pg-cdc acl diff

# List registered tables
pg-cdc query

# View recent audit events
pg-cdc serve --http &
curl http://localhost:8080/v1/audit/summary
```

### Common Procedures

**Adding a new table:**

```bash
# 1. Add table to source publication
ALTER PUBLICATION pgcdc_prod_pub ADD TABLE public.new_table;

# 2. Run init with --tables flag to snapshot
pg-cdc init --tables public.new_table

# 3. Register ACL
pg-cdc acl register public.new_table
pg-cdc acl set public.new_table --tag sensitivity=PII --reason "New table added"
pg-cdc acl sync

# 4. Verify
pg-cdc query -t public.new_table
```

**Schema change (ALTER TABLE):**

```bash
# pg-cdc auto-detects ADD/DROP COLUMN during streaming
# One-shot reconciliation for hot tables:
pg-cdc reconcile --table public.orders --force

# Verify catalog updated:
pg-cdc catalog register
```

**Manual snapshot promotion:**

```bash
pg-cdc promote --from staging --to main --reason "Weekly EOD promotion"
pg-cdc promote --from raw@2026-07-03T120000Z --to main --force  # time-travel restore
```

**Compaction:**

```bash
# Merge delta epochs into a new base; safe to run alongside live streamer
pg-cdc compact --table public.orders
pg-cdc compact --all                           # compact all tables with deltas
pg-cdc compact --tombstone-ttl 7d              # soft-delete tombstones older than 7 days
```

**Teardown (decommission):**

```bash
pg-cdc teardown                                # drops slot, publication, S3 storage, state
# Manual: drop replication slot, drop publication, delete S3 prefix
```

### Emergency Procedures

**WAL guardrail tripped (`max_retained_wal_bytes` alarm):**
1. `pg-cdc status` — check slot lag
2. If consumer is lagging (e.g., S3 unavailable): resolve S3, let pg-cdc catch up
3. If pg-cdc is stuck: `kill -TERM <pid>` (graceful drain), restart
4. If immediate relief needed: `SELECT pg_drop_replication_slot('pgcdc_prod_slot');` (WARNING: data loss for unflushed WAL)

**CAS conflict storm:**
1. `pg-cdc status --json | jq '.cas_conflicts'` — check conflict rate
2. Check for dual pg-cdc instances (ETag contention)
3. Verify only one `pg-cdc start` is running
4. If conflicting with compaction: stagger compaction schedules

**pg-cdc will not start:**
1. Check config file syntax
2. `pg-cdc preflight` — source readiness
3. Verify replication slot exists: `SELECT * FROM pg_replication_slots WHERE slot_name = 'pgcdc_prod_slot';`
4. Check IAM execution role has all required permissions
5. Verify VPC endpoint connectivity

---

## Scaling Guidance

### Instance Sizing

| Workload Profile | Tables | WAL Volume | CPU | Memory | ECS Task Size |
|---|---|---|---|---|---|
| Low | < 10 | < 1 GB/hr | 0.5 vCPU | 2 GB | `cpu: 512, memory: 2048` |
| Medium | 10-50 | 1-10 GB/hr | 1 vCPU | 4 GB | `cpu: 1024, memory: 4096` |
| High | 50-200 | 10-50 GB/hr | 2 vCPU | 8 GB | `cpu: 2048, memory: 8192` |
| Extreme | 200+ | 50+ GB/hr | 4 vCPU | 16 GB | `cpu: 4096, memory: 16384` |

### Factors That Impact Capacity

- **Row width**: Wide rows (many columns, large JSONB) increase flush bytes and DuckDB read time
- **Table count**: Each table is a separate flush goroutine; 200+ tables may require tuning `flush.max_parallel`
- **TOAST columns**: Partially preserved; adds overhead to WAL parsing
- **Schema evolution frequency**: Each DDL triggers a manifest update + Glue catalog write
- **Compaction load**: Schedule compaction during low-write periods; compaction consumes CPU/memory

### Horizontal Scaling

pg-cdc is currently **single-active**. Horizontal scaling is achieved by:
- **Fan-out**: Multiple pg-cdc instances with different table sets (partition by schema)
- **Read replicas**: Multiple pg-cdc serve instances behind a load balancer for MCP/REST read capacity
- **Compaction on separate host**: Run `pg-cdc compact` on a separate instance to avoid resource contention

### Throughput Benchmarks

| Configuration | Events/sec | MB/s written | Latency p50 (flush) |
|---|---|---|---|
| 2 vCPU, 8 GB | 15,000 | 25 MB/s | 2.3s |
| 4 vCPU, 16 GB | 35,000 | 60 MB/s | 1.1s |
| 8 vCPU, 32 GB | 65,000 | 110 MB/s | 0.7s |

_Benchmarks use T3-like rows (20 columns, 500 bytes avg) with 10 tables, Iceberg sink, S3 in same region._

---

## Cost Model

### AWS Services Estimate ($50K events/sec, 10 tables, us-west-2)

| Service | Monthly Cost (est.) | Notes |
|---|---|---|
| ECS Fargate (2 vCPU, 8 GB) | $150-200 | Single task, On-Demand pricing |
| S3 Standard | $80-150 | ~5 TB data + Iceberg metadata, lifecycle to S3 Glacier after 90 days |
| S3 PUT/GET requests | $30-60 | ~50M PUT, ~2M GET |
| Glue Catalog | $10-20 | 10 tables, 100 snapshots/table |
| Lake Formation | $0 | No additional charge for tag-based governance |
| DynamoDB (ACL registry) | $5-10 | PAY_PER_REQUEST, < 1 GB |
| CloudWatch Logs | $50-100 | ~10 GB ingest, 365-day retention |
| KMS | $1-5 | Single CMK, < 100k API calls |
| Data transfer (cross-AZ) | $20-40 | RDS in different AZ than pg-cdc |
| **Total (estimated)** | **$350-585/mo** | Excludes RDS, networking, NAT Gateway |

### Cost Optimization

- **S3 lifecycle**: Transition data > 90 days to S3 Glacier Deep Archive ($1/TB/mo)
- **State on S3**: Use S3 Standard-IA after 30 days
- **Compression**: Iceberg already uses Parquet compression (snappy/zstd); tune `parquet.compression_codec`
- **Flush tuning**: Larger `flush.interval_sec` means fewer, larger writes (lower PUT costs, higher latency)
- **Cross-AZ traffic**: Deploy pg-cdc in the same AZ as RDS writer to avoid data transfer costs
- **CloudWatch Logs**: Reduce retention if compliance allows (90-day minimum recommended)

---

## Compliance & Governance

### Data Classification

The DynamoDB ACL registry supports a tag taxonomy that maps to compliance frameworks:

| Tag | Values | Purpose |
|---|---|---|
| `sensitivity` | public, internal, confidential, restricted | General data sensitivity |
| `data_classification` | pci, phi, pii, business, operational | Regulatory classification |
| `retention_class` | short_term, standard, long_term, indefinite | Data lifecycle policy |
| `steward` | team-name@domain | Data owner contact |

### HIPAA Readiness (Overlay)

When deployed in a HIPAA-eligible AWS account with a BAA:

1. **Enable S3 bucket logging** + Object-level CloudTrail
2. **Use KMS with automatic key rotation** (CMK)
3. **Enable S3 Object Lock** for WORM compliance
4. **Configure CloudTrail** for all data and management events
5. **Set CloudWatch Logs retention** to minimum 6 years
6. **Restrict IAM roles** with explicit `Deny` for `s3:DeleteObject*` on governed prefix
7. **Enable VPC Flow Logs** for network audit trail
8. **Apply `sensitivity=phi` tag** to any PHI-containing tables

See `docs/compliance/hipaa.md` for the full overlay checklist.

### SOC2

- pg-cdc produces immutable, append-only data (suitable for SOC2 A1/C1/C2 controls)
- All ACL changes are versioned and audited with provenance (`who / when / why`)
- Lake Formation provides logical access controls with audit trail
- CloudTrail provides infrastructure change audit
- See `docs/compliance/soc2.md` for control mapping

### Audit Trail

| Activity | Logged To | Detail |
|---|---|---|
| ACL changes | DynamoDB Streams → S3 | Resource, tags, version, who, when, why |
| Data access | CloudTrail + S3 Server Access | Principal, action, resource, timestamp |
| pg-cdc operations | CloudWatch Logs | Start/stop, config changes, schema evolution |
| Break-glass grants | CloudWatch Logs + SNS | Grantor, reason, TTL, resource |
| Governance drift | CloudWatch metric + event | `pgcdc_acl_drift_items` > 0 |

### Compliance Reporting

```bash
# Generate compliance report
pg-cdc acl diff --json > /tmp/acl-drift.json
pg-cdc status --json > /tmp/status.json
# Combined report includes: drift count, unclassified tables, stale grants, snapshot age
```

---

## Release & Change Management

### Versioning

| Type | Pattern | Frequency | Source |
|---|---|---|---|
| Release candidate | `v0.1.0-rc1` | Per push to `main` | GitHub Actions |
| Stable release | `v0.1.0` | Manual promotion from RC | Workflow dispatch |
| Patch | `v0.1.1` | Bugfix, no API change | Cherry-pick to `release/*` |

All releases are signed (cosign) and published to a private ECR repository for production consumption.

### Upgrade Procedure

**Minor/patch upgrade (no breaking changes):**

1. Review CHANGELOG.md and verify `migration/` notes
2. In staging: deploy new binary, run `pg-cdc status --json > pre-upgrade.json`, observe 5 min
3. Compare metric baselines (lag, throughput, error rates)
4. In production during maintenance window:
   ```bash
   kill -TERM $(pidof pg-cdc)            # drains buffer, writes final flush
   # Deploy new container/task
   # Verify
   pg-cdc status
   pg-cdc preflight
   ```
5. Monitor CloudWatch alarms for 15 min post-deployment

**Major upgrade (breaking changes):**
- Follow migration script in `migration/v0.2.0/`
- Full rollback plan is documented in the migration script
- Requires longer maintenance window and stakeholder sign-off

### Rollback Procedure

```bash
# 1. Stop current instance gracefully
kill -TERM $(pidof pg-cdc)

# 2. Deploy previous version
# (for ECS: update task definition to previous image tag)
# (for EKS: kubectl set image deployment/pg-cdc pg-cdc=pg-cdc:v0.1.0)

# 3. Verify
pg-cdc status

# 4. If rollback involves state format change, restore from pre-upgrade backup:
aws s3 cp s3://my-org-pg-cdc-state/pre-upgrade/ s3://my-org-pg-cdc-state/ --recursive
```

### Maintenance Windows

| Operation | Expected Downtime | Frequency |
|---|---|---|
| pg-cdc upgrade | < 1 min (WAL slot survives) | Bi-weekly |
| RDS maintenance | 1-2 min (auto-reconnect) | As scheduled by AWS |
| Compaction heavy | None (runs alongside streamer) | Daily |
| Schema evolution | None (live detection) | On demand |

---

## Troubleshooting

### Common Issues

| Symptom | Likely Cause | Resolution |
|---|---|---|
| `pg-cdc start` exits immediately | Config error or source unreachable | Run `pg-cdc preflight`, check `url` and secrets |
| Replication lag growing | Sink slow (S3/network) or insufficient flush rate | Increase `flush.max_*` or instance size; check S3 endpoint |
| CAS conflict errors | Dual pg-cdc instances or compaction conflict | Verify single `start` process; stagger compaction |
| `wal_level` error | Source not configured for logical replication | `SHOW wal_level; ALTER SYSTEM SET wal_level = logical;` |
| `permission denied` on source | Insufficient replication privileges | Grant `rds_replication` (RDS) or `REPLICATION` |
| Lake Formation tags invisible | `IAMAllowedPrincipals` not removed | Verify `LakeFormation:GetEffectivePermissionsForPath` |
| ACL drift alert | Tags changed outside pg-cdc (manual LF console) | Run `pg-cdc acl sync` to reconcile |
| DuckDB query slow on serve | Large Parquet files or many columns | Compact tables; limit columns in query |
| OOM on wide rows | `flush.max_bytes` too high for instance memory | Reduce `flush.max_bytes` to 64 MiB or lower |

### Diagnostic Commands

```bash
# Full state dump
pg-cdc status --json | jq '.'

# Replication slot details
PGPASSWORD=$PGPASSWORD psql -h ... -U cdc_user -d db \
  -c "SELECT slot_name, database, wal_status, safe_wal_size FROM pg_replication_slots;"

# Source WAL generation rate
PGPASSWORD=$PGPASSWORD psql -h ... -U cdc_user -d db \
  -c "SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint / extract(epoch FROM now() - pg_postmaster_start_time())::bigint || ' B/s') AS wal_rate;"

# S3 object count / size
aws s3api list-objects --bucket my-org-governed-warehouse --prefix cdc/prod/ \
  --query 'Contents[*].[Key,Size]' --output json | jq 'length'

# Prometheus metrics
curl -s http://localhost:9090/metrics | grep -E '^(pgcdc_|go_)'

# Glue table registration
aws glue get-tables --database-name pgcdc_prod_us_west_2 --query 'TableList[*].Name'

# Lake Formation permissions for a role
aws lakeformation list-permissions --principal DataLakePrincipalIdentifier=arn:aws:iam::...:role/my-consumer
```

---

## Reference

### Repositories

| Repository | Purpose |
|---|---|
| [`burnside-project/pg-cdc`](https://github.com/burnside-project/pg-cdc) | Public OSS mirror (no governance features) |
| Private full-source repo | Full source (includes governance/REST/MCP, contact Burnside Project) |
| [`burnside-project/pg-warehouse`](https://github.com/burnside-project/pg-warehouse) | Analytics client — refresh, model, version, export |
| [`burnside-project/burnside-go`](https://github.com/burnside-project/burnside-go) | Shared types — manifest spec, storage interface |

### Documentation Index

| Document | Content |
|---|---|
| `docs/00-features.md` | Exhaustive feature catalog |
| `docs/02-configuration.md` | Full configuration reference (all 200+ options) |
| `docs/07-operations.md` | Operations guide |
| `docs/08-authentication.md` | Authentication methods |
| `docs/09-smoke-test.md` | Smoke test runbook |
| `docs/10-security-architecture.md` | Five-property security model |
| `docs/11-ai-agent-consumption.md` | AI agent consumption paths (MCP, REST) |
| `docs/12-durability.md` | At-least-once delivery guarantee proof |
| `docs/metrics.md` | 29 Prometheus metrics reference |
| `docs/runbooks/` | 14 symptom-diagnosis-remediation runbooks |
| `docs/deployment/ecs.md` | ECS Fargate deployment guide |
| `docs/deployment/eks.md` | EKS deployment guide |
| `docs/deployment/cross-account.md` | Cross-account deployment |
| `docs/deployment/govcloud.md` | GovCloud/US commercial deployment |
| `docs/compliance/hipaa.md` | HIPAA readiness overlay |
| `docs/compliance/soc2.md` | SOC2 control mapping |
| `docs/compliance/data-lifecycle.md` | Data retention and purging |
| `deploy/terraform/governance/` | Terraform for infrastructure provisioning |
| `deploy/grafana/pg-cdc.json` | Grafana dashboard |
| `CHANGELOG.md` | Release history |

---

_pg-cdc is developed by [Burnside Project](https://burnsideproject.ai). Available on the [AWS Marketplace](https://aws.amazon.com/marketplace/pp/prodview-vpbhhtmdwzezy). For enterprise licensing, support SLAs, and professional services, contact your AWS account team or Burnside Project._
