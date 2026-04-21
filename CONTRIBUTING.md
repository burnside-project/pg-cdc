# Contributing to pg-cdc

Thank you for your interest in contributing to pg-cdc!

## How to Contribute

### Reporting Issues

- Use [GitHub Issues](https://github.com/burnside-project/pg-cdc/issues) to report bugs or suggest features
- Search existing issues before creating a new one
- For bug reports, include:
  - PostgreSQL version and replication config (`wal_level`, `max_replication_slots`)
  - pg-cdc version (`pg-cdc version`)
  - Sink type (filesystem / S3 / GCS)
  - Steps to reproduce and expected vs. actual behavior
  - Relevant excerpts from logs (redact credentials)

For questions or usage discussion, use [GitHub Discussions](https://github.com/burnside-project/pg-cdc/discussions) instead of opening an issue.

### Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Make your changes
4. Run the full local check: `make all` (runs `vet`, `lint`, `test`, `build`)
5. Commit with a clear message (see [Commit Messages](#commit-messages))
6. Push and open a Pull Request against `main`

CI on the PR runs `lint` and `test` on Go 1.25. Both must be green before merge.

### Development Setup

```bash
# Clone
git clone https://github.com/burnside-project/pg-cdc.git
cd pg-cdc

# Build
make build

# Unit tests
make test

# Lint (golangci-lint)
make lint

# Everything
make all
```

Pure Go, no CGO — no extra toolchain required beyond Go 1.25.

For end-to-end work that needs a live PostgreSQL with logical replication, see [`docs/01-getting-started.md`](docs/01-getting-started.md) for a local setup walkthrough.

### Code Style

- Follow standard Go conventions (`gofmt`, `go vet`, `golangci-lint`)
- Keep functions small and focused
- Write tests for new functionality (table-driven where appropriate)
- Respect the hexagonal architecture: CLI → services → ports ← adapters
- New sinks, sources, or catalogs should implement existing port interfaces, not introduce parallel abstractions

### Commit Messages

Use clear, conventional-commit-style messages:

- `feat: add GCS sink adapter`
- `fix: handle NULL in composite type columns during snapshot`
- `docs: expand streaming.md with backpressure notes`
- `test: integration test for compaction TTL`
- `refactor: extract replication slot lifecycle into separate service`
- `chore(deps): bump pgx/v5 to 5.9.2`
- `ci: run lint on Go 1.25 only`

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).