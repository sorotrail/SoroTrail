# Base images are pinned by digest as well as tag: a floating tag lets two
# builds of the same commit resolve to different base layers, which undercuts
# the reproducibility the VERSION/COMMIT ldflags exist to provide. The tag stays
# on the reference so the release in use is still readable. Refresh a pin after
# bumping its tag with:
#
#   docker buildx imagetools inspect <image:tag> --format '{{.Manifest.Digest}}'
#
# These are the multi-arch index digests, not per-platform ones. Dependabot
# watches the docker ecosystem for this directory, so a moved digest arrives as
# a reviewable pull request instead of silently changing what a build consumes.

# Build stage runs on the build host's native platform and cross-compiles,
# so multi-arch builds don't pay for QEMU-emulated Go compilation.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine@sha256:fa3d596898d9f126af7c01d14ee7597bab3f39cccf392ef36b69fe77cd681f18 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
ARG VERSION=unknown
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
	go build -trimpath -buildvcs=false \
	-ldflags="-s -w -X github.com/sorotrail/sorotrail/internal/buildinfo.Version=$VERSION -X github.com/sorotrail/sorotrail/internal/buildinfo.Commit=$COMMIT -X github.com/sorotrail/sorotrail/internal/buildinfo.BuildDate=$BUILD_DATE" \
	-o /out/sorotrail ./cmd/sorotrail

FROM alpine:3.24

# OCI image metadata, so `docker inspect` reports where an image came from and
# what went into it. The publish workflow also sets these keys through
# docker/metadata-action; declaring them here means every build — compose, a
# fork's CI, or a plain `docker build` — yields a self-describing image instead
# of an anonymous one.
#
# The args are re-declared because an ARG belongs to the stage that declares it:
# the values used to stamp the binary above are out of scope below the FROM. The
# defaults keep an un-argumented build working instead of emitting empty labels,
# and an image built that way honestly says "unknown" rather than inventing a
# revision.
ARG VERSION=unknown
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="sorotrail" \
	org.opencontainers.image.description="Contract event indexer for Stellar/Soroban: stores events durably past the RPC's retention window and serves them over a JSON API." \
	org.opencontainers.image.source="https://github.com/sorotrail/SoroTrail" \
	org.opencontainers.image.revision="$COMMIT" \
	org.opencontainers.image.version="$VERSION" \
	org.opencontainers.image.created="$BUILD_DATE" \
	org.opencontainers.image.licenses="Apache-2.0"

# Runtime base, pinned by digest for the reason given at the top of the file.
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 sorotrail
USER sorotrail
COPY --from=build /out/sorotrail /usr/local/bin/sorotrail
EXPOSE 8080

# Container HEALTHCHECK runs against `/readyz`, so Docker reports the
# service healthy only when its database and RPC dependencies are ready.
# The probe uses the binary
# already shipped in the image — alpine has no curl/wget, and we
# deliberately do not install one to keep the image slim and the
# forensic surface tiny (no shell access required to probe).
#
#   interval=10s   matches the per-poll cadence (5s) with margin; logs
#                   show one probe line at most every 10s, not on
#                   every cycle.
#   timeout=5s     leaves ~2s above the in-binary 3s probe timeout so
#                   docker can always reap the process even if the
#                   probe hangs.
#   start_period=30s gives the container time to connect to Postgres,
#                   run migrations, and complete an initial RPC check
#                   before the first probe is counted.
#   retries=3      two hits allowed before marking unhealthy, which
#                   tolerates one transient RPC blip without flapping.
#
# --addr is intentionally omitted: the probe falls back to $HTTP_ADDR
# then 127.0.0.1:8080, so an operator who overrides HTTP_ADDR inside
# this image (or in compose env) gets a probe that follows the same
# port rather than silently failing on a stale hardcoded value.
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
	CMD ["sorotrail", "healthcheck", "--endpoint", "/readyz", "--timeout", "3s"]

ENTRYPOINT ["sorotrail"]
