# Releasing pg-cdc

This document describes how releases work and how to cut one. It's a maintainer-facing reference — end-user install instructions live in the [README](README.md) and [`docs/01-getting-started.md`](docs/01-getting-started.md).

## The model at a glance

Two phases:

| Phase | Tag | Trigger | Built by |
|---|---|---|---|
| **Release Candidate** | `v<x>.<y>.<z>-rc<n>` | Automatic on every push to `main` (excluding doc-only) | [`.github/workflows/ci-cd.yml`](.github/workflows/ci-cd.yml) |
| **Stable** | `v<x>.<y>.<z>` | Manual `workflow_dispatch` against an existing RC | [`.github/workflows/release.yml`](.github/workflows/release.yml) |

This keeps RCs cheap (one per merge for testing, no human in the loop) while gating stable on a maintainer checkmark.

## Day-to-day: what happens when a PR merges

```
PR opened ──► CI checks (lint, test, codeql) ──► reviewer approves
                                                       │
                                                       ▼
                                          squash-merge to main
                                                       │
                            ┌──────────────────────────┴─────────────────────────┐
                            ▼                                                    ▼
                   path-ignored?                                       full CI/CD pipeline
                  (only *.md, docs/**,                            (lint → test → resolve-rc
                   LICENSE, .gitignore)                            → build-binaries → publish-rc)
                            │                                                    │
                            ▼                                                    ▼
                   no RC built                                  v<X>.<Y>.<Z>-rc<N+1> published
```

Doc-only changes intentionally don't produce a new RC. That's `paths-ignore` in `ci-cd.yml`. Code or workflow changes do.

## Cutting a stable release

1. **Verify the latest RC.** Pull the binary for your platform from `https://github.com/burnside-project/pg-cdc/releases/tag/v<X>.<Y>.<Z>-rc<N>` and exercise the real path (`pg-cdc init`, `pg-cdc start`, `pg-cdc mcp`). Don't promote based on green CI alone — CI runs unit tests, not the demo.
2. **Trigger the Release workflow:**
   - GitHub → Actions → **Release** → **Run workflow**
   - `version`: `0.2.0` (no `v` prefix; the workflow adds it)
   - `rc`: leave empty to take the latest, or specify a number to promote a specific RC
3. **The workflow:**
   - Looks up the requested RC's tag
   - Downloads its binaries from the RC release
   - Renames `pg-cdc_v0.2.0-rc3_linux_amd64.tar.gz` → `pg-cdc_v0.2.0_linux_amd64.tar.gz`
   - Recomputes `checksums.txt`
   - Creates the bare tag `v0.2.0` at the same commit as the RC tag
   - Publishes the stable release with `--latest`

The promoted release shares the same SHA as the RC — no fresh build, just renaming and tagging. That guarantees the bytes you tested in the RC are exactly what ships as stable.

## Bumping VERSION for the next minor or major

When a stable `vX.Y.Z` has shipped and you want to start RCs for the next version, edit the `VERSION` file and merge a one-line PR:

```bash
echo 0.3.0 > VERSION
git checkout -b chore/bump-version-0.3.0
git add VERSION
git commit -m "chore: bump VERSION to 0.3.0"
gh pr create --base main --title "chore: bump VERSION to 0.3.0"
```

When that PR merges, the CD pipeline auto-publishes `v0.3.0-rc1`.

**Semver guidance:**
- **Patch** (`0.2.0 → 0.2.1`): bug fixes, no new public API or feature
- **Minor** (`0.2.0 → 0.3.0`): new feature, backwards-compatible (the v0.2.0 → v0.3.0 bump for the MCP server is the canonical example)
- **Major** (`0.x.y → 1.0.0`): backwards-incompatible change in CLI or config format

We're pre-1.0, so minor bumps are fine for breaking changes, but signal them clearly in the PR description and release notes.

## Gotchas

### Release Drafter can block auto-RCs

The [`release-drafter.yml`](.github/workflows/release-drafter.yml) workflow auto-creates a Draft release for the next version after each merge, accumulating notes. The "Resolve RC" step in `ci-cd.yml` short-circuits if a release matching the current `vX.Y.Z` exists — and a Draft counts as existing.

**Symptom:** PRs merge to main but no new RC builds. The CD job runs Lint + Test + Resolve RC (success) and then skips Build All Platforms and Publish RC.

**Fix:** delete the draft before bumping VERSION:
```bash
gh release delete vX.Y.Z --repo burnside-project/pg-cdc --yes
```

**Permanent fix (TODO):** patch the resolve-rc step to filter `isDraft` releases out:
```bash
# In ci-cd.yml's "Determine next RC" step, change:
if gh release view "$TAG_BASE" --json tagName --jq '.tagName' 2>/dev/null; then
# to:
if gh release view "$TAG_BASE" --json tagName,isDraft --jq 'select(.isDraft|not).tagName' 2>/dev/null | grep -q .; then
```

### README install URLs are version-pinned

The build job names artifacts `pg-cdc_v<X>.<Y>.<Z>_<os>_<arch>.tar.gz` (version-in-filename). GitHub's `releases/latest/download/` endpoint requires the exact asset filename, so generic URLs like `pg-cdc_linux_amd64.tar.gz` don't work — they 404.

The README pins to a specific version (e.g. `download/v0.2.0/pg-cdc_v0.2.0_linux_amd64.tar.gz`). After cutting a stable release, bump those URLs as a one-line follow-up PR.

A cleaner long-term fix is to make the workflow also produce versionless aliases (`pg-cdc_linux_amd64.tar.gz` symlink-style copies) — not yet implemented.

### Dependabot stack rebases

When Dependabot files multiple PRs at once (typical for AWS SDK weekly updates), only the first squash-merges cleanly. The rest become CONFLICTING because the first merge moved `go.sum` forward.

**Fix:** comment `@dependabot rebase` on each conflicting PR. Dependabot reopens with a fresh diff in 1–3 minutes. Auto-merge isn't enabled on this repo, so you merge each one by hand once it's CLEAN.

### CI/CD `paths-ignore` is intentional but easy to forget

Changes touching only `*.md`, `docs/**`, `LICENSE`, `.gitignore`, or `RELEASES.md` skip the workflow entirely — including the lint and test gates. This is intentional (no point burning CI on doc-only changes) but means a doc PR can't catch a Go compilation error.

If a PR mixes doc and code changes, the workflow runs (because the path filter is OR-mode, not AND-mode).

## Hygiene after merge

- **Remote branches:** auto-deleted by `gh pr merge --delete-branch` (use this on every merge)
- **Local branches:** clean up after pulling latest main:
  ```bash
  git checkout main
  git pull public main
  git branch --merged | grep -vE '^\*|^  main$' | xargs -I _ git branch -d _
  ```
- **Stale Draft releases:** delete with `gh release delete vX.Y.Z --yes` (see Release Drafter gotcha above)

## Reference files

| File | Purpose |
|---|---|
| `VERSION` | Source of truth for the next RC's `<X>.<Y>.<Z>` |
| [`.github/workflows/ci-cd.yml`](.github/workflows/ci-cd.yml) | Lint/test/codeql + auto-RC pipeline (push to main) |
| [`.github/workflows/release.yml`](.github/workflows/release.yml) | Stable promotion (`workflow_dispatch`) |
| [`.github/workflows/release-drafter.yml`](.github/workflows/release-drafter.yml) | Auto-accumulating release notes (creates the next-version Draft) |
| [`.github/workflows/codeql.yml`](.github/workflows/codeql.yml) | Security scan (runs alongside CI) |

## Past releases

See [Releases](https://github.com/burnside-project/pg-cdc/releases) for the full history. The most recent stable as of this writing is **v0.2.0** — the first release containing the local MCP server (`pg-cdc mcp`).
