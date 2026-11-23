# Releasing SoroTrail

## Overview

Pushing a `v*` tag triggers two parallel workflows:

1. **GoReleaser** (`.github/workflows/release.yml`) builds signed, checksummed
   binaries for Linux, macOS, and Windows (amd64 + arm64 each), generates a
   changelog, and publishes a GitHub Release with the archives attached.

2. **Docker publish** (`.github/workflows/docker-publish.yml`) builds and
   pushes multi-arch Docker images (linux/amd64, linux/arm64) to GHCR.

Both workflows run on the same tag and do not conflict. The GitHub Release is
created first (GoReleaser), and the Docker images are published independently.

## How to release

1. Ensure `main` is at the commit you want to release.

2. Tag and push:

   ```bash
   git checkout main
   git pull upstream main
   git tag -a v0.1.0 -m "v0.1.0"
   git push upstream v0.1.0
   ```

   Use [semver](https://semver.org/) for the tag name. Pre-release tags
   (e.g. `v0.1.0-rc.1`) create a pre-release on GitHub.

3. The release workflow runs automatically. Monitor progress at:

   - https://github.com/sorotrail/sorotrail/actions

4. After the workflows complete, the GitHub Release page has the binaries
   and checksums, and `ghcr.io/sorotrail/sorotrail:latest` is updated.

## Verifying a release

### Download a binary

```bash
# Linux amd64
curl -LO https://github.com/sorotrail/sorotrail/releases/download/v0.1.0/sorotrail_v0.1.0_linux_amd64.tar.gz
tar -xzf sorotrail_v0.1.0_linux_amd64.tar.gz
./sorotrail --version
```

### Verify checksums

```bash
curl -LO https://github.com/sorotrail/sorotrail/releases/download/v0.1.0/checksums.txt
sha256sum -c checksums.txt --ignore-missing
```

### Independently reproducing a release binary

Every release binary is built deterministically from its tagged commit:

- **Toolchain pin**: Go toolchain version is pinned in `go.mod` (`go 1.26.10`) and modules are pinned in `go.sum`.
- **Path & VCS normalization**: All builds pass `CGO_ENABLED=0`, `-trimpath`, `-buildvcs=false`, and `-ldflags="-s -w ..."`, eliminating working-directory, hostname, and worktree-status leaks.
- **Deterministic build timestamp**: Rather than reading the wall clock (`date -u`), `internal/buildinfo.BuildDate` is derived from the commit timestamp in UTC (`TZ=UTC git show -s --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ HEAD` in the `Makefile` / `Dockerfile`, and `{{ .CommitDate }}` in `.goreleaser.yaml`). That preserves `sorotrail version` stamping (`version`, `commit`, `build-date`) while keeping identical inputs across builders.

To reproduce a release binary locally and compare its SHA-256 digest against a published release or across two isolated build environments:

```bash
git checkout v0.1.0

# Run the automated two-environment reproducibility check (builds twice in
# separate directory depths, hostnames, users, and timezones, and compares
# SHA-256 digests, build IDs, ELF/Mach-O/PE sections, and string tables):
make reproducible-check

# Or build a specific target binary manually and compare against the release:
VERSION="$(git describe --tags --always --dirty)"
COMMIT="$(git rev-parse --short HEAD)"
BUILD_DATE="$(TZ=UTC git show -s --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ HEAD)"

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath -buildvcs=false \
  -ldflags="-s -w -X github.com/sorotrail/sorotrail/internal/buildinfo.Version=${VERSION} -X github.com/sorotrail/sorotrail/internal/buildinfo.Commit=${COMMIT} -X github.com/sorotrail/sorotrail/internal/buildinfo.BuildDate=${BUILD_DATE}" \
  -o sorotrail ./cmd/sorotrail

sha256sum sorotrail
```

#### Container image reproducibility scope

Both `Dockerfile` and `Dockerfile.sorotrail` pin base images by multi-arch manifest SHA-256 digest and compile `/usr/local/bin/sorotrail` with `CGO_ENABLED=0 -trimpath -buildvcs=false` and commit-derived `VERSION`, `COMMIT`, and `BUILD_DATE` build arguments, so the **Go binary inside the container** is bit-for-bit reproducible. Full OCI layer tarball bit-for-bit reproducibility is explicitly scoped out as a known limitation because the runtime stage executes `apk add --no-cache ca-certificates` and `adduser` against Alpine's live repository metadata and filesystem mtimes. To verify a published container image, extract `/usr/local/bin/sorotrail` from the container (`docker create` + `docker cp`) and compare its `sha256sum` against a local build of the same commit.

## Local snapshot builds

Build binaries for your local platform without a tag:

```bash
goreleaser release --snapshot --clean
```

Binaries appear in `dist/`. The version string will contain a `-SNAPSHOT-`
suffix derived from `git describe`.

## Release notes

GoReleaser generates changelog notes from commit messages. The grouping is
configured in `.goreleaser.yaml`. Merge commits are filtered out; group by
feature, fix, and maintenance labels extracted from conventional-commit-style
prefixes (feat:, fix:, etc.).

Edit the generated release notes on GitHub if the auto-generated grouping
needs adjustment. The PR is the source of truth for what changed; the release
notes are a summary for downstream consumers.

## Versioning the API client (pkg/client)

`pkg/client` is generated from `api/openapi.yaml` (see `pkg/client/README.md`),
and its `SpecVersion` const mirrors the spec's `info.version`. The client is
versioned in lockstep with the API: when a release changes the HTTP surface,
bump `info.version` in `api/openapi.yaml` (semver), then run `make spec` and
`make client` so the committed `internal/api/openapi.json` and
`pkg/client/client.gen.go` are regenerated together — the drift tests in
`pkg/docs` and `pkg/client` fail the build otherwise.

Downstream consumers import `github.com/sorotrail/sorotrail/pkg/client`
directly from the module at a release tag; because the module is the
repository root, every release tag publishes a coherent (spec, client) pair.
A breaking API change therefore ships as a spec version bump plus a new
release tag, and consumers pin the tag that matches the server version they
target (compare against the server's `/openapi.json`, which embeds the same
`info.version`).

Release checklist addition: if the release changes any route, schema, or
parameter, regenerate with `make spec && make client` and include the
regenerated files in the release commit.
