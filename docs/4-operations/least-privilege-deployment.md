# Least-Privilege Deployment

> **Audience:** customer security and platform teams. Pairs with the [HIPAA deployment guide](hipaa-deployment.md) for regulated workloads, and stands alone for any deployment that wants strict access control.
> **Status:** v1 (draft). Reviewed before any production deployment.

This guide makes the **default-deny posture** of the Burnside platform
operational: every database, table, and column without an explicit Lake
Formation grant is invisible. There is no implicit access. There is no
"untagged means accessible." Every access decision is deliberate.

## Why default-deny

Default-allow architectures fail the same way every time: a new table is
created, somebody forgets to apply restricting tags, and the table is silently
readable by everyone with general catalog access. By the time it's discovered,
the data has been queried, copied, possibly trained on, possibly sent
elsewhere.

Default-deny inverts the failure mode. A new table without grants is invisible
— nobody can query it, but more importantly, nobody can know it exists. The
worst case is "an analyst pings the data steward asking for access," which is
the desired conversation.

## The Four Pillars

For default-deny to actually hold, four cloud-side settings must be in place
**simultaneously**. Skip any one and the model leaks.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Pillar 1 — LF-Tag-based grants only                                     │
│  Use only LFTagPolicy expressions in grants. No named-resource grants.   │
│  Otherwise a single one-off grant on an "untagged" table leaks access.  │
├──────────────────────────────────────────────────────────────────────────┤
│  Pillar 2 — IAMAllowedPrincipals removed                                 │
│  Disable the legacy fallback that lets IAM permissions bypass LF.        │
│  Otherwise an IAM role with broad data-bucket access reads parquet      │
│  directly without LF ever being consulted.                              │
├──────────────────────────────────────────────────────────────────────────┤
│  Pillar 3 — Hybrid Access Mode = off                                     │
│  Force every access through LF. No IAM-only path.                       │
│  Otherwise users hitting Athena/Spark via IAM-direct see unfiltered data.│
├──────────────────────────────────────────────────────────────────────────┤
│  Pillar 4 — Bucket policy requires LF-vended credentials                 │
│  Direct S3 access (without LF session tag) is denied.                    │
│  Otherwise DuckDB or any other tool can read raw parquet from S3 with  │
│  vanilla IAM, bypassing LF column/row filtering entirely.               │
└──────────────────────────────────────────────────────────────────────────┘
```

The Terraform module in [`deploy/terraform/governance/`](../../deploy/terraform/governance/)
configures all four pillars by default. This guide walks through the *why* of
each, the verification steps, and how to debug when something doesn't work.

## Pillar 1 — LF-Tag-based Grants Only

### What this means

LF supports two grant models:

1. **Named-resource grants:** `GRANT SELECT on table burnside_prod.public.customers to role X`. The grant is a permission directly attached to a specific resource.
2. **Tag-based grants:** `GRANT SELECT on resources matching {sensitivity ∈ [PII]} to role X`. The grant is a predicate evaluated against tags at query time.

Default-deny requires **only** tag-based grants. Here's why:

If you allow named-resource grants alongside tag-based grants, then a steward
who wants to give Alice quick access to a table can `GrantPermissions` directly
on that table, bypassing the tag taxonomy entirely. The named grant works even
if the table has no tags (or has a `sensitivity=PHI` tag that should restrict
access). Tags become advisory rather than authoritative.

### How to enforce

Policy decision: **as an organization, commit to issuing only tag-based grants.** Document this as part of your access-management policy.

Practical enforcement:

- IAM policies for the data steward role should permit `lakeformation:GrantPermissions` only when the request body contains `LFTagPolicy` (use a Service Control Policy condition).
- Code review on every grant: any direct named-resource grant requires explicit security-team sign-off.
- Periodic audit: query `lakeformation:ListPermissions` and alert on any non-LFTagPolicy grants found.

### Verification

```bash
# List all permissions with their grant type
aws lakeformation list-permissions \
    --resource-type TABLE \
    --query 'PrincipalResourcePermissions[?Resource.Table != null && !LFTagPolicy].[Principal.DataLakePrincipalIdentifier, Resource.Table.Name, Permissions]' \
    --output table

# Should be empty (or show only Data Lake Admin grants).
```

## Pillar 2 — IAMAllowedPrincipals Removed

### What this means

By default, Lake Formation has a **legacy fallback** called `IAMAllowedPrincipals` that grants any IAM principal full access to a database/table whose default permissions include it. This is a backwards-compatibility hack from when LF was first introduced.

Default-deny requires this fallback **removed everywhere**: at the catalog level (data lake settings), on every database, on every table.

### How to enforce

The Terraform module sets `remove_iam_allowed_principals = true` (the default), which:

- Sets `aws_lakeformation_data_lake_settings.create_database_default_permissions = []`
- Sets `aws_lakeformation_data_lake_settings.create_table_default_permissions = []`
- Sets `aws_glue_catalog_database.create_table_default_permission = []`

For databases / tables that existed before this module was applied, you must
manually remove the IAMAllowedPrincipals fallback:

```bash
# Per-database
aws lakeformation revoke-permissions \
    --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS \
    --resource '{"Database": {"Name": "burnside_prod"}}' \
    --permissions ALL

# Per-table
aws lakeformation revoke-permissions \
    --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS \
    --resource '{"Table": {"DatabaseName": "burnside_prod", "Name": "public_customers"}}' \
    --permissions ALL
```

### Verification

```bash
# Catalog-wide setting
aws lakeformation get-data-lake-settings \
    --query 'DataLakeSettings.CreateDatabaseDefaultPermissions'
# expected: []

# Database-level
aws glue get-database --name burnside_prod \
    --query 'Database.CreateTableDefaultPermissions'
# expected: []

# Search for any lingering IAM_ALLOWED_PRINCIPALS grants
aws lakeformation list-permissions \
    --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS
# expected: empty
```

## Pillar 3 — Hybrid Access Mode Off

### What this means

LF supports three access modes:

- **Lake Formation only** — every access goes through LF, no IAM bypass.
- **Hybrid** — LF + IAM in parallel; IAM permissions can grant access independently.
- **IAM only** — LF doesn't enforce anything (legacy mode).

Default-deny requires **Lake Formation only**.

### How to enforce

Hybrid Access Mode is configured per-data-lake-location. The Terraform module's
`aws_lakeformation_resource` declaration uses `use_service_linked_role = true`,
which establishes the LF-only mode for the data path it registers.

If you've previously registered the location in Hybrid mode, deregister and
re-register:

```bash
aws lakeformation deregister-resource \
    --resource-arn "arn:aws:s3:::burnside-warehouse/burnside/"

# Then re-register via Terraform module (or:)
aws lakeformation register-resource \
    --resource-arn "arn:aws:s3:::burnside-warehouse/burnside/" \
    --use-service-linked-role
```

### Verification

```bash
aws lakeformation describe-resource \
    --resource-arn "arn:aws:s3:::burnside-warehouse/burnside/" \
    --query 'ResourceInfo.HybridAccessEnabled'
# expected: false (or absent)
```

## Pillar 4 — Bucket Policy Requires LF-Vended Credentials

### What this means

Even with the first three pillars in place, a vanilla IAM role with `s3:GetObject` on the data bucket can read parquet files directly from S3 — bypassing LF column masking entirely. The data has already left LF's authorization path.

Default-deny requires the data bucket to **deny** any read that isn't using LF-vended credentials (which carry a specific session tag).

### How to enforce

The Terraform module's `aws_s3_bucket_policy.default_deny` resource installs a `Deny` statement that:

- Applies to all `s3:GetObject` / `s3:GetObjectVersion` requests.
- Excepts the pg-cdc role itself (which writes data and needs direct access).
- Excepts data-lake-admin principals (for emergency access).
- Otherwise requires the request to carry the `LakeFormationAuthorizedCaller=true` session tag.

LF automatically applies that session tag when it vends credentials via
`GetTemporaryGlueTableCredentials`, so well-behaved clients (DuckDB iceberg
extension, Athena, Spark with the LF jar) work transparently. Misbehaving
clients (raw `aws s3 cp`, an unconfigured boto3 session) get denied.

### Verification

```bash
# Confirm the bucket policy is in place
aws s3api get-bucket-policy --bucket burnside-warehouse \
    | jq -r '.Policy' \
    | jq '.Statement[] | select(.Sid=="DenyAllExceptLakeFormationVendedAccess")'

# Negative test: raw IAM should fail
aws s3 cp s3://burnside-warehouse/burnside/burnside_prod.db/public_orders/data/00000.parquet /tmp/
# expected: An error occurred (AccessDenied)

# Positive test: same call via LF-vended creds should succeed
# (run from a workstation with valid SSO + LF grants)
pg-warehouse refresh
# expected: data materialized into raw.duckdb
```

## Migrating an Existing Default-Allow Deployment

If you're switching an existing Burnside deployment from default-allow to
default-deny, do this in stages:

### 1. Inventory current access

```bash
# What grants exist today?
aws lakeformation list-permissions --output table > current-grants.txt

# Which roles are accessing what?
aws cloudtrail lookup-events \
    --lookup-attributes AttributeKey=EventSource,AttributeValue=glue.amazonaws.com \
    --start-time $(date -u -v-30d +%FT%TZ) > past-30d-access.json
```

### 2. Simulate the switch

```bash
# Future feature: pg-cdc audit-grants --simulate-default-deny
# For now: cross-reference current grants against required tags.
# Identify roles that would lose access (don't have an LFTagPolicy grant matching their workload).
```

### 3. Pre-issue tag-based grants

For every workload that should retain access, issue an LFTagPolicy grant that
matches its current usage. Document each grant's owner.

### 4. Communicate

Notify all affected teams of the cutover date. Provide instructions:
- "Re-authenticate your AWS SSO session after the cutover."
- "If a previously-accessible table is missing, contact your data steward to issue a tag-based grant."

### 5. Cut over

Apply the Terraform module changes (or manually invoke the four-pillar
configuration). Have data stewards on standby to issue grants for any
missed workloads.

### 6. Watch CloudTrail

Look for `AccessDenied` errors in the first 24 hours; investigate each.

## Visibility Behavior on the Read Side

pg-warehouse implements default-deny by **silent skip**: tables you can't see
don't appear in `refresh` output, `inspect tables`, or any normal command.
This is the right default — it means the *existence* of a table is itself
governed by LF, not just the data.

For debugging, use the opt-in escape hatch:

```bash
pg-warehouse inspect catalog --include-inaccessible
```

This lists every table in the catalog with an "accessible / not accessible" column. Useful for:

- "Why don't I see table X?" — answer is in the `WHY` column ("no grant", "no LF tag matches your role", etc.).
- Onboarding a new analyst — they can see what they don't have access to and request specific grants.
- Audit reviews — confirm the default-deny boundary is correctly drawn.

For maximum-sensitivity environments where existence-of-table is itself
classified, the inspect escape hatch can be gated behind a separate LF tag
(e.g., `metadata-visible=true`); analysts without that grant get the silent
view only.

## What Pg-CDC Enforces at Write Time

Default-deny on the read side only works if every newly-created table arrives
with the right tags. pg-cdc enforces this at write time:

- `governance.mode: strict` (recommended for compliance) — refuse to register a table missing required tags.
- `governance.require_tags: [sensitivity, domain, owner]` — the keys that must resolve.
- `governance.on_missing: fail` — operation fails rather than proceeding with incomplete tags.

If pg-cdc cannot resolve tags for a new table or column (no override matches, no default applies), it refuses the operation. **No new untagged tables ever appear in the catalog.**

This composes with the four pillars: even if pg-cdc misbehaves and creates a
table without tags, the four pillars guarantee the table is invisible to
everyone except the (audited) data-lake-admin path.

## Audit Trail

Every catalog operation is logged. The combination of CloudTrail + LF audit + S3 access logs provides:

- **Who accessed what, when** — credential vends, table reads, schema changes.
- **What grants were evaluated** — LF audit shows the LFTagPolicy decision per request.
- **What changed in tag policy** — pg-cdc.yml is in git; CloudTrail captures `AddLFTagsToResource` calls.

For HIPAA / SOX / SOC2 audits, the workflow is:

```bash
# Per-identity, last 30 days
pg-warehouse audit --since 30d --identity alice@example.com

# Catalog-wide, last 30 days
aws cloudtrail lookup-events \
    --lookup-attributes AttributeKey=EventSource,AttributeValue=lakeformation.amazonaws.com \
    --start-time $(date -u -v-30d +%FT%TZ)

# Tag policy change history
git log --all --source -- pg-cdc.yml | grep -A 10 'governance:'
```

## Troubleshooting

### "I can't see table X but I should"

1. Confirm your IAM identity:
   ```bash
   aws sts get-caller-identity
   ```
2. Check what LF grants apply to your role:
   ```bash
   aws lakeformation list-permissions --principal DataLakePrincipalIdentifier=$(aws sts get-caller-identity --query Arn --output text)
   ```
3. Check what tags table X carries:
   ```bash
   aws lakeformation get-resource-lf-tags --resource '{"Table": {"DatabaseName": "burnside_prod", "Name": "public_x"}}'
   ```
4. If the tags don't satisfy any of your grants, request a new grant from the data steward.

### "DuckDB query returns AccessDenied"

1. Confirm STS session is fresh:
   ```bash
   aws sso login --profile <your-profile>
   ```
2. Confirm the bucket policy isn't denying — verify your call carries the LF session tag (this is automatic with `pg-warehouse refresh`; manual `aws s3 cp` won't work):
   ```bash
   aws sts get-session-token --query Credentials.SessionToken
   ```
3. Re-run via pg-warehouse rather than direct S3.

### "An IAM-direct query is succeeding when it shouldn't"

This means a pillar is broken. Check in order:

```bash
# Pillar 2 — IAMAllowedPrincipals removed?
aws lakeformation get-data-lake-settings --query 'DataLakeSettings.CreateDatabaseDefaultPermissions'

# Pillar 3 — Hybrid Mode off?
aws lakeformation describe-resource --resource-arn "arn:aws:s3:::burnside-warehouse/burnside/" --query 'ResourceInfo.HybridAccessEnabled'

# Pillar 4 — Bucket policy in place?
aws s3api get-bucket-policy --bucket burnside-warehouse | jq -r '.Policy'
```

## Pre-Production Checklist

Before any default-deny deployment goes live:

- [ ] All four pillars verified in place via the commands above
- [ ] pg-cdc.yml `mode: strict`, `on_missing: fail`
- [ ] Layer 1 LF tag taxonomy registered (Terraform applied)
- [ ] Layer 2 tag assignments applied (pg-cdc init complete; verify with `aws lakeformation get-resource-lf-tags`)
- [ ] Layer 3 grants issued for every authorized workload
- [ ] CloudTrail + LF audit logs flowing to centralized account
- [ ] Migration from default-allow (if applicable) staged, communicated, executed
- [ ] Post-cutover monitoring for `AccessDenied` in CloudTrail, first 24h
- [ ] Rollback plan documented (re-add IAMAllowedPrincipals, restore named grants from inventory)

## Related

- [HIPAA Deployment](hipaa-deployment.md) — additional controls for PHI workloads
- [`docs/spec/governance.schema.json`](../spec/governance.schema.json) — Layer 2 config schema
- [`deploy/terraform/governance/`](../../deploy/terraform/governance/) — Terraform module that configures the four pillars
