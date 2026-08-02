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
