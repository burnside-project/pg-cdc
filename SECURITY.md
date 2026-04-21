# Security Policy

## Reporting Vulnerabilities

If you discover a security vulnerability in pg-cdc, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, email: [security@burnsideproject.ai](mailto:security@burnsideproject.ai)

We will acknowledge receipt within 48 hours and provide an estimated timeline for a fix.

## Supported Versions

| Version | Supported |
|---------|-----------|
| latest  | Yes       |

Only the latest stable release receives security updates. Release candidates (`-rcN` suffixes) are for testing and do not receive backported security fixes.

## Security Best Practices

When deploying pg-cdc:

- **Credentials** — never commit PostgreSQL connection strings or cloud credentials to version control. Use environment variables or a secrets manager. The config loader expands `${VAR}` references.
- **PostgreSQL role** — grant the minimum needed: `CONNECT` + `SELECT` on published tables, plus `REPLICATION` attribute on the role. Avoid granting `SUPERUSER`.
- **Replication slot hygiene** — unused slots retain WAL indefinitely and can fill the Postgres disk. Use `pg-cdc teardown` (or drop the slot manually) when decommissioning a consumer.
- **Cloud sink IAM** — scope the S3/GCS role to the specific bucket/prefix pg-cdc writes to. For AWS Glue, grant only the database/table actions needed for `catalog register`.
- **Network exposure** — pg-cdc makes outbound connections only (Postgres, S3/GCS, Glue). It does not listen on any port.
- **Log hygiene** — logs may include table names, column names, and SQL values in debug mode. Do not enable debug-level logging in environments where log sinks are not access-controlled.
- **Keep pg-cdc updated** — follow [Releases](https://github.com/burnside-project/pg-cdc/releases) and upgrade promptly when security patches ship.