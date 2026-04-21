# Governance

This document describes the governance model for pg-cdc.

## Maintainers

pg-cdc is maintained by the Burnside Project team. Current maintainers are listed in the repository's GitHub team settings and have merge access to the `main` branch.

## Decision-Making

- **Architecture decisions** that change the port/adapter boundaries, public CLI surface, on-disk manifest format, or state-database schema should be proposed as an ADR in `docs/adr/` before implementation. An ADR is reviewed, discussed, and accepted or rejected before work starts.
- **Feature requests and bugs** are tracked as GitHub Issues. Design discussion happens in the issue or Discussions thread before work begins.
- **Pull requests** require at least one maintainer approval. CI (lint + test) must pass before merging.
- **Breaking changes** to the CLI, configuration schema, or Parquet/manifest format require a major-version bump and a migration note in `RELEASES.md`.

## Release Process

- pg-cdc follows [Semantic Versioning](https://semver.org/).
- Release candidates are produced automatically by CI on every push to `main` (e.g., `v0.1.0-rc1`, `v0.1.0-rc2`, ...). Each RC is a full, signed, multi-platform build suitable for pre-production validation.
- A stable release is promoted from a specific RC via the **Release** workflow (`workflow_dispatch`). The promotion copies RC assets to a stable tag, marks the release as `latest`, and writes a release note summarizing the promotion lineage.
- `VERSION` at the repo root pins the base version targeted by the current RC series. Bumping `VERSION` starts a new RC stream.

## Becoming a Maintainer

Contributors who demonstrate sustained, high-quality contributions may be invited to become maintainers. The criteria include:

- Meaningful contributions over multiple months (code, documentation, reviews, issue triage).
- Understanding of the hexagonal architecture and the open-core boundary.
- Alignment with the project's design principles and community standards.

Existing maintainers propose and approve new maintainers by consensus.

## Code of Conduct

All participants are expected to follow the project's [Code of Conduct](CODE_OF_CONDUCT.md). Maintainers are responsible for enforcement.