# HIPAA Deployment Guide

> **Audience:** customer security and platform teams deploying pg-cdc + pg-warehouse for workloads that include Protected Health Information (PHI).
> **Status:** v1 (draft). Reviewed by an external compliance consultant prior to first production deployment.

This guide is the deployment recipe for HIPAA-grade Burnside deployments. It
overlays the [least-privilege deployment guide](least-privilege-deployment.md)
with the additional controls required for PHI handling under 45 CFR §164.

It is **not** legal advice. Engage your privacy counsel and compliance team to
validate that this deployment satisfies your organization's HIPAA risk
analysis (45 CFR §164.308(a)(1)(ii)(A)).

## What HIPAA actually requires (briefly)

| Safeguard category | What HIPAA expects | How Burnside delivers |
|---|---|---|
| Administrative — Risk Analysis | Documented risk assessment of PHI flows | Pipeline diagram is 5 components (Postgres → pg-cdc → S3+Glue → pg-warehouse → DuckDB); architectural attack surface is small |
| Administrative — Access Management | Role-based access; minimum necessary | Lake Formation tag-based grants, default-deny; identity-driven views per developer |
| Administrative — BAA | Signed BAA with each Business Associate | AWS, customer's MDM vendor; **no Burnside vendor in the data path** |
| Physical — Workstation Security | Workforce members' workstations protected | Customer's MDM (FileVault/BitLocker, screen lock, MDM enrollment) |
| Technical — Access Control | Unique user identification; emergency access; auto-logoff; encryption | AWS IAM Identity Center (SSO, MFA, audit); STS session TTLs; emergency tag breakglass; KMS encryption |
| Technical — Audit Controls | Hardware/software/procedural mechanisms recording PHI activity | CloudTrail + Lake Formation audit logs centralized; `pg-warehouse audit` |
| Technical — Integrity | PHI not improperly altered or destroyed | S3 versioning + Iceberg snapshot history; immutable audit logs |
| Technical — Transmission Security | Guard against unauthorized PHI access during transit | TLS everywhere (postgres→pg-cdc, S3, Glue API); no on-prem PHI movement |

## Architecture for HIPAA

The single-tenant deployment model is the right fit for HIPAA. Each customer
gets their own AWS account (or a dedicated organizational unit), pg-cdc daemon,
and Lake Formation catalog. No multi-tenant compute, no shared state across
customers.

```
       Customer's AWS Account (single-tenant)                    Customer's Endpoints
       ─────────────────────────────────────                    ────────────────────
       VPC (private subnets)                                    Workstations (MDM-managed)
        ├── pg-cdc EC2/ECS                                       ├── FileVault/BitLocker
        │     ├── reads Postgres via VPC endpoint                ├── pg-warehouse CLI
        │     └── writes Iceberg via VPC endpoint                ├── DuckDB local query
        ├── S3 bucket (KMS encrypted, default-deny)              └── SSO browser session
        ├── Glue catalog (LF tags applied)                              ↓
        ├── Lake Formation (governance enforcement)              AWS IAM Identity Center
        └── CloudTrail + LF audit logs                                  ↓
                  ↓                                              STS-vended creds (≤1h TTL)
            Centralized Logging Account                                 ↓
                                                                 LF-filtered S3 bytes
                                                                        ↓
                                                                 raw.duckdb (on laptop)
```

Critical invariants:

1. **PHI never leaves the customer's AWS account** for compute. It only flows
   to authorized workstations that pull via LF-vended credentials.
2. **No Burnside vendor is in the data path.** The pg-cdc and pg-warehouse
   binaries run inside customer infrastructure. Burnside has no access to PHI.
3. **STS session TTLs are short.** Default 1 hour; configurable down to 15
   minutes for highest-sensitivity data.

## BAA Requirements

Burnside is **not** a HIPAA Business Associate because Burnside has no access
to PHI under this architecture. The customer signs BAAs with:

| Vendor | What for | BAA-eligible service |
|---|---|---|
| **AWS** | S3, Glue, Lake Formation, IAM, KMS, CloudTrail, EC2/ECS | All services in this deployment are HIPAA-eligible under the AWS BAA |
| **Endpoint security vendor** (Jamf, Intune, etc.) | Workstation MDM | Customer's choice |
| **Identity provider** (Okta, AzureAD, etc.) | SSO for IAM Identity Center | Vendor-specific BAA |

**No BAA with Burnside is required** for this deployment model. Burnside
ships software; the customer operates it inside their own HIPAA boundary.

## Deployment Recipe

### Step 1 — AWS Foundation

In the customer's AWS account, region of choice (must support all services):

```hcl
# Apply the governance Terraform module:
module "pgcdc_governance" {
  source = "github.com/dataalgebra-engineering/burnside-project-pg-cdc//deploy/terraform/governance?ref=v0.1.0"

  deployment_name    = "burnside-prod"
  aws_account_id     = "111122223333"
  aws_region         = "us-west-2"
  data_bucket_name   = aws_s3_bucket.warehouse.id
  data_bucket_prefix = "burnside/"
  glue_database_name = "burnside_prod"

  data_lake_admin_principals = [
    "arn:aws:iam::111122223333:role/SecurityTeamAdmin",
  ]

  lf_tag_taxonomy = {
    sensitivity    = ["public", "internal", "confidential", "PII", "PHI"]
    domain         = ["clinical", "billing", "ops", "platform"]
    owner          = ["data-platform", "clinical-team", "billing-team"]
    classification = ["regulated", "general"]
    retention      = ["30d", "90d", "1y", "7y", "permanent"]
  }
}
```

### Step 2 — Bucket Hardening

Beyond what the module configures, the data bucket must have:

```hcl
resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.warehouse.arn   # Customer-managed key
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  versioning_configuration {
    status = "Enabled"   # required for forensics + recovery
  }
}

resource "aws_s3_bucket_logging" "warehouse" {
  bucket        = aws_s3_bucket.warehouse.id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "warehouse-access/"
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket                  = aws_s3_bucket.warehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
```

### Step 3 — IAM Identity Center

- Enable IAM Identity Center; integrate with the customer's IdP (Okta, AzureAD, etc.).
- Create permission sets corresponding to LF roles:
  - `pgcdc-clinician-readonly` → maps to LF role with `sensitivity ≤ PHI` grants
  - `pgcdc-billing-readonly` → `sensitivity ≤ PII` for billing-tagged data
  - `pgcdc-data-engineer` → broader, but PHI columns still require explicit LF grant
- Configure session duration ≤ 1 hour for any role that grants PHI access.
- Enforce MFA at the IdP layer; AWS will inherit it.

### Step 4 — pg-cdc Deployment

Deploy pg-cdc on EC2 or ECS in private subnets:

- VPC endpoints for `s3`, `glue`, `lakeformation`, `secretsmanager` — no public-internet egress required.
- Security group restricting outbound to the Postgres source and AWS service endpoints.
- Instance role: the `pg_cdc_role_arn` from the Terraform module.
- Postgres credentials in Secrets Manager (KMS-encrypted with the warehouse key).
- pg-cdc.yml `governance:` block with `mode: strict`, `on_missing: fail`, and the customer's overrides.

### Step 5 — Audit Log Centralization

Ship logs to a separate logging account:

- CloudTrail trail for the production account, organization-wide if applicable.
- Lake Formation audit logs (enabled by default; verify they're flowing).
- S3 access logs from the warehouse bucket.
- Lifecycle rule to retain audit logs ≥ 6 years (HIPAA §164.316(b)(2)(i) requires 6-year retention of relevant policies and procedures; many organizations apply the same retention to access logs as a defensive default).

### Step 6 — pg-warehouse Distribution

For each authorized workstation:

- pg-warehouse binary installed via the customer's package management (jamf, sccm, intune deployment).
- `pg-warehouse.yml` committed to a project repo using `${AWS_PROFILE}` path templating.
- AWS SSO profile configured for IAM Identity Center.
- Verify FileVault/BitLocker is enabled (MDM-enforced).
- Set `retention.expire_on_revocation: true` and `raw_max_age: 7d` (or shorter per data sensitivity).

### Step 7 — Access Provisioning

Initial Layer 3 grants per role:

```bash
# Clinicians can read clinical-domain data, including PHI
aws lakeformation grant-permissions \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::111122223333:role/pgcdc-clinician-readonly \
  --resource '{"LFTagPolicy": {"Expression": [
    {"TagKey": "domain", "TagValues": ["clinical"]},
    {"TagKey": "sensitivity", "TagValues": ["public", "internal", "PHI"]}
  ]}}' \
  --permissions SELECT DESCRIBE
```

For temporary elevation (incident response), use the `break-glass.yml`
GitHub Actions workflow (`.github/workflows/break-glass.yml`) — it
requires GitHub Environment approval, takes a mandatory `reason`, and
auto-revokes on a TTL. The per-table `pg-cdc emergency-tag` CLI shape
shown in earlier drafts of this doc is **not yet implemented**; until it
ships, the workflow is the supported path. See
[`docs/runbooks/manifest-conflicts.md`](../runbooks/manifest-conflicts.md)
for the operational shape of the break-glass role.

## Authentication Options

For automated tooling that needs to make AWS calls (the `Apply LF Tags` GitHub Actions workflow, CI jobs, downstream automation), AWS authentication needs deliberate design. Five patterns; pick the one that matches the customer's existing posture.

### 1. Baseline OIDC — single AWS account, GitHub.com

```
GitHub Actions → GitHub OIDC token → AWS STS AssumeRoleWithWebIdentity → temp creds (≤1h)
```

**When to use:** the default for mid-market customers. Single AWS account, GitHub.com, no exotic compliance constraints beyond HIPAA itself.

**How:** the governance Terraform module provisions both the OIDC provider and the tag-applier role when `create_github_oidc_provider = true` and `enable_tag_applier_role = true`. See `.github/workflows/README.md` in this repo for the end-to-end setup.

**Posture:** credentials are short-lived (≤1h) and scoped via the `sub` condition on `repo:owner/name:*`. No long-lived AWS keys exist anywhere.

### 2. Cross-account role chaining — multi-account AWS Organizations

```
GitHub OIDC → "github-entry" role in hub account → sts:AssumeRole into target workload role
```

**When to use:** the customer's AWS landing zone forbids direct external trust on workload accounts. Common in Control Tower deployments and large hospital systems.

**How:** OIDC provider + entry role live in a hub/security account. Each workload account's tag-applier role trusts only the hub-account role. Workflow uses two `aws-actions/configure-aws-credentials` steps: one for OIDC into the hub, one for `role-chaining: true` into the workload role.

**Posture:** every cross-account hop is explicit and auditable. Compromise of GitHub doesn't directly grant production access.

### 3. HashiCorp Vault as broker

```
GitHub Actions → OIDC auth to Vault → Vault AWS secrets engine → dynamic AWS creds
```

**When to use:** the customer already runs Vault for database/K8s secrets and wants unified credential lifecycle and audit.

**How:** workflow uses `hashicorp/vault-action` with OIDC auth instead of `aws-actions/configure-aws-credentials`. Vault returns short-lived AWS keys into env vars. AWS-side roles trust Vault's role-assumption identity, not GitHub.

**Posture:** Vault is the single broker for all cloud creds. Full audit trail in Vault's logs. Adds Vault as an operational dependency.

### 4. Self-hosted runners inside AWS — highest-security

```
GitHub Actions runner on EC2 / ECS / EKS → instance profile → direct AWS API
```

**When to use:** FedRAMP, defense health, or any environment where external GitHub.com is not allowed in the credential path.

**How:** runner runs inside the customer's AWS VPC with an instance role. Workflow uses `runs-on: self-hosted` and the OIDC step is removed entirely. The instance profile is picked up automatically.

**Posture:** strongest. No external OIDC trust at all. Tradeoff: customer owns the runner infrastructure (autoscaling, patching, image lifecycle).

### 5. Corporate IdP in the path (Okta / AzureAD)

```
GitHub → corporate IdP (SAML/OIDC bridge) → AWS
```

**When to use:** corporate security policy mandates that ALL access (human and machine) flow through the corporate IdP for centralized session and audit.

**How:** GitHub Actions authenticates the run to Okta/AzureAD (via OIDC); the IdP issues a SAML assertion AWS accepts. AWS-side trust is on the corporate IdP, not GitHub.

**Posture:** unified IdP audit, but more moving pieces. Reserve for environments where corporate security explicitly requires it.

### Decision matrix

| Customer profile | Recommended auth |
|---|---|
| Single AWS account, GitHub.com | **Baseline OIDC** |
| AWS Organizations / Control Tower | Cross-account chain |
| Already running Vault | Vault broker |
| FedRAMP-High / defense / paranoid | Self-hosted runner |
| Corporate-mandated IdP | Okta/AzureAD proxy |

The Terraform module ships with Baseline OIDC support out of the box. Other patterns require small workflow edits + documentation; not a module change.

## Audit Workflow

Run weekly (or on demand for compliance reviews):

```bash
# Per-identity view
pg-warehouse audit --since 7d --identity alice@hospital.example.com

# Catalog-wide view from CloudTrail
aws cloudtrail lookup-events \
    --lookup-attributes AttributeKey=EventSource,AttributeValue=lakeformation.amazonaws.com \
    --start-time $(date -u -v-7d +%FT%TZ)
```

The combination of CloudTrail + Lake Formation audit + S3 access logs + pg-warehouse local audit produces a complete chain-of-custody trail satisfying HIPAA §164.312(b).

## Incident Response

For suspected PHI exposure:

1. **Containment** — revoke the affected LF grant immediately:
   ```bash
   aws lakeformation revoke-permissions --principal ... --permissions SELECT
   ```
   On affected workstations, force expire local data:
   ```bash
   pg-warehouse expire --all
   ```
   `retention.expire_on_revocation: true` does this automatically on the next refresh, but explicit invocation is recommended for incident response.

2. **Investigation** — query CloudTrail + LF audit for the affected resource and time range.

3. **Notification** — follow your organization's HIPAA breach notification procedures (45 CFR §164.404). Burnside does not have visibility into your data; notification is solely the customer's responsibility.

4. **Documentation** — record findings in your audit log; Burnside artifacts (`pg-warehouse audit` output) attach to the incident record.

## Things This Architecture Does NOT Provide

Be honest with your compliance team about scope:

- **Data Loss Prevention (DLP).** Local laptops with PHI are an exposure path. Combine with endpoint DLP from the customer's MDM/security stack.
- **Network-level PHI inspection.** Bytes are TLS-encrypted end-to-end; you cannot inspect them in transit. Network controls focus on egress destinations.
- **Workforce training.** §164.530(b) requires training. Burnside ships the deployment; customer trains its workforce on safe use.
- **Risk Analysis.** Required by §164.308. This guide informs the analysis but does not replace it.
- **Business continuity / disaster recovery.** S3 cross-region replication, Glue cross-region catalog replication — design separately based on RPO/RTO.

## Validation Checklist (Pre-Production)

Run through this before going live with PHI:

- [ ] BAA with AWS executed and on file
- [ ] Customer-managed KMS key encrypts the warehouse bucket
- [ ] S3 versioning enabled; access logs flowing to a separate logging account
- [ ] Public access block configured on the warehouse bucket
- [ ] IAM Identity Center integrated with corporate IdP; MFA enforced
- [ ] STS session TTL ≤ 1 hour for PHI-grant roles
- [ ] pg-cdc deployed in private subnets with VPC endpoints; no public IP
- [ ] pg-cdc.yml `mode: strict`, `on_missing: fail`
- [ ] All Layer 1 LF tag definitions match `lf_tag_taxonomy` in pg-cdc.yml `require_tags`
- [ ] Bucket policy denies non-LF reads (verify via `terraform output` from the governance module)
- [ ] CloudTrail trail capturing org-wide events
- [ ] Lake Formation audit logs enabled
- [ ] All authorized workstations: FileVault/BitLocker enabled, MDM enrolled, pg-warehouse installed
- [ ] `retention.expire_on_revocation: true` and a defensible `raw_max_age` on every workstation config
- [ ] Initial Layer 3 grants issued; principle of least privilege applied
- [ ] Tabletop exercise: simulate PHI breach; verify revoke + expire flow works end-to-end
- [ ] Tabletop exercise: simulate workstation loss; verify session TTL expires + retention auto-purges
- [ ] HIPAA Risk Analysis updated to reflect this architecture

## Related

- [Least-Privilege Deployment](least-privilege-deployment.md) — the underlying default-deny posture
- [`docs/spec/governance.schema.json`](../spec/governance.schema.json) — Layer 2 config schema
- [`deploy/terraform/governance/`](../../deploy/terraform/governance/) — Terraform module that provisions Layers 1 and the bucket policy
