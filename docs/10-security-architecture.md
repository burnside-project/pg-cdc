# Security Architecture — CDC as an Air Gap

> **Audience:** CISOs, security architects, platform teams evaluating pg-cdc for AI agent data access.
>
> For deployment-specific hardening, see [Least-Privilege Deployment](security/least-privilege-deployment.md) and [HIPAA Deployment](security/hipaa-deployment.md).

## The Problem

Every approach to "give AI agents database access" looks like this:

```
AI Agent ──── credentials ────> Production Database
                                 (live, mutable, operational)
```

No matter how many read replicas, connection pools, or row-level policies you add, **the agent has a network path to production**. One prompt injection, one hallucinated `DROP TABLE`, one credential leak — and you're in an incident.

## How pg-cdc Solves It

pg-cdc creates a **physical air gap** between production data and AI consumption:

```
Production Database
       |
       |  WAL (one-way, append-only)
       v
    pg-cdc ──> S3 (immutable Parquet)
                     |
                     |  IAM (no database credentials exist)
                     v
                  AI Agent
```

There is no return path. The agent cannot write to production — not because of a policy, but because **the protocol doesn't exist**. The WAL is one-way. Parquet is immutable. S3 has no `UPDATE` verb. The agent doesn't have a connection string because there is no connection.

## Three Security Zones

```
+--------------------------------------------------------------+
|                    PRODUCTION ZONE                            |
|                                                              |
|   PostgreSQL (operational, mutable, sensitive)                |
|         |                                                    |
|         | WAL (one-way)                                      |
|         v                                                    |
|      pg-cdc (runs in production VPC)                         |
|         |                                                    |
+---------|----------------------------------------------------+
          |  S3 PUT (cross-account, write-only from here)
          v
+--------------------------------------------------------------+
|                 GOVERNED DATA ZONE                            |
|                                                              |
|   S3 (immutable Parquet) + Glue Catalog + LF Tags            |
|   DynamoDB ACL (intent store) + Audit Trail                  |
|         |                              |                     |
|         v                              v                     |
|   +-----------+                 +--------------+             |
|   |MCP Server |                 | pg-warehouse |             |
|   |(agents)   |                 | (developers) |             |
|   +-----------+                 +--------------+             |
|                                                              |
+--------------------------------------------------------------+
          |                              |
          v                              v
+--------------------------------------------------------------+
|                   CONSUMPTION ZONE                            |
|                                                              |
|   Claude, GPT, custom agents, notebooks, dashboards          |
|   (can read governed data, cannot reach production)          |
|                                                              |
+--------------------------------------------------------------+
```

Three zones. Two boundaries. One direction. The consumption zone has **no network path, no credentials, and no protocol** to reach production.

## Five Security Properties

### 1. Physical Write Isolation

| | Read replica (Neon, RDS, etc.) | pg-cdc + S3 |
|---|---|---|
| Protocol | Postgres wire protocol (supports writes) | S3 GET (read-only by design) |
| Write prevention | Policy must block writes | Protocol has no write verb |
| Failure mode | Misconfigured policy = writes succeed | No configuration can enable writes |

Policy-based security fails when policy is misconfigured. **Protocol-based isolation fails only if you rewrite the protocol.**

### 2. Blast Radius Containment

If an agent is compromised (prompt injection, credential theft, supply chain attack):

| Attack vector | Read replica | pg-cdc + S3 |
|---|---|---|
| Read all tables | Yes (unless RLS is perfect) | Only tables with matching LF tags |
| Read historical data | Current snapshot only | Only epochs in S3 (governed) |
| Write or delete data | Possible if policy misconfigured | **Impossible** — no write path exists |
| Escalate to production | Same network, same auth system | Different network, different auth system (IAM vs Postgres) |
| Exfiltrate connection string | Reusable against production | **No connection string exists** |
| DoS the database | Connection exhaustion | S3 is effectively infinite; production unaffected |

### 3. Temporal Isolation

The agent sees data **as of the last flush**, not as of right now. This is a security feature:

- An agent cannot race against transactions in progress
- An agent cannot observe partial writes (no dirty reads — ever)
- An agent cannot trigger lock contention on production tables
- An agent querying historical data has **zero impact on production IOPS**

The CDC flush interval (default 10s) is a tunable knob between freshness and isolation. For most agent use cases (analytics, monitoring, anomaly detection), 10-second-old data is indistinguishable from live.

### 4. Credential Elimination

| | Read replica | pg-cdc + S3 |
|---|---|---|
| Credential type | `POSTGRES_URL` (long-lived connection string) | IAM Role (assumed via STS) |
| Credential lifetime | Until rotated (days, weeks, never) | Temporary (1hr default, auto-rotated) |
| Credential scope | Full database access per pg_hba.conf | Specific S3 prefix + Glue tables |
| Leak surface | Logs, error messages, crash dumps, env vars | No credential to leak |
| Rotation | Manual or scheduled | Automatic (STS) |
| Audit | pg_stat_activity (if enabled) | CloudTrail (always on) |

There is no connection string to leak because there is no connection.

### 5. Governance as Physics, Not Policy

Traditional governance:
```
DBA writes GRANT/REVOKE --> hopes app respects it --> audits after the fact
```

pg-cdc governance:
```
Security tags table "PII" in DynamoDB
  --> pg-cdc syncs tag to Lake Formation
  --> LF physically prevents unauthorized reads
  --> Agent never sees the column (not filtered -- absent)
  --> DynamoDB Streams --> S3 audit log (immutable, COMPLIANCE lock)
```

The governance isn't a layer on top. It's the **only path to the data**. You can't bypass it because there's nothing to bypass — no alternative route to the bits.

## How This Composes with the Two Governance Models

pg-cdc supports two governance models (see [Data Governance](06-governance.md)), both operating within this security architecture:

| Model | What it gates | Where enforced | Air gap maintained? |
|---|---|---|---|
| **Postgres-ACL profiles** | Which tables/columns a developer sees | pg-warehouse at pull time | Yes — developer never touches production |
| **Lake Formation tags** | Which tables/columns an agent sees | LF at query time (MCP server, Athena) | Yes — agent never touches production |

Both models enforce access within the **Governed Data Zone**. Neither model requires or permits access to the Production Zone.

## Comparison: pg-cdc vs Common Alternatives

| Approach | Write isolation | Credential model | Production impact | Governance | Time travel |
|---|---|---|---|---|---|
| **Direct DB access** | None | Connection string | Full (shared compute) | RLS/RBAC | None |
| **Read replica** | Policy-based | Connection string | Reduced (separate compute) | RLS/RBAC | None |
| **DB proxy (PgBouncer)** | Policy-based | Connection string | Reduced (pooling) | Proxy rules | None |
| **ETL to warehouse** | Physical | Warehouse credentials | None after extract | Warehouse ACLs | If warehouse supports it |
| **pg-cdc + S3** | **Physical** | **IAM (no DB creds)** | **Zero** | **LF tags (default-deny)** | **Built-in (CDC epochs)** |

## The Enterprise Question

Every CISO evaluating AI agents asks:

> "How do I let agents see our data without giving them access to our databases?"

The answer today is either "you can't" or "build a bespoke ETL pipeline and hope."

pg-cdc makes the answer:

> `pg-cdc init` — and your production database has a governed, immutable, auditable, read-only projection that any AI agent can query without ever touching production.

## Related

- [Data Governance](06-governance.md) — two governance models and their enforcement points
- [AI Agent Consumption](11-ai-agent-consumption.md) ��� how agents and developers consume governed data
- [Least-Privilege Deployment](security/least-privilege-deployment.md) — the four pillars of default-deny
- [HIPAA Deployment](security/hipaa-deployment.md) — additional controls for PHI workloads
