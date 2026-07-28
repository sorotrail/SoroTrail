# Build stage runs on the build host's native platform and cross-compiles,
# so multi-arch builds don't pay for QEMU-emulated Go compilation.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
ARG VERSION=unknown
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
	go build \
	-ldflags="-X github.com/sorotrail/sorotrail/internal/buildinfo.Version=$VERSION -X github.com/sorotrail/sorotrail/internal/buildinfo.Commit=$COMMIT -X github.com/sorotrail/sorotrail/internal/buildinfo.BuildDate=$BUILD_DATE" \
	-o /out/sorotrail ./cmd/sorotrail

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 sorotrail
USER sorotrail
COPY --from=build /out/sorotrail /usr/local/bin/sorotrail
EXPOSE 8080

# Container HEALTHCHECK runs against the same `/health` endpoint the
# Helm chart's liveness/readiness probes target, so docker compose and
# k8s both examine one consistent signal. The probe uses the binary
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
#   start_period=10s gives the container time to bring up the Postgres
#                   connection pool and run migrations before the
#                   first probe is counted.
#   retries=3      two hits allowed before marking unhealthy, which
#                   tolerates one transient RPC blip without flapping.
#
# --addr is intentionally omitted: the probe falls back to $HTTP_ADDR
# then 127.0.0.1:8080, so an operator who overrides HTTP_ADDR inside
# this image (or in compose env) gets a probe that follows the same
# port rather than silently failing on a stale hardcoded value.
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD ["sorotrail", "healthcheck", "--endpoint", "/health", "--timeout", "3s"]

ENTRYPOINT ["sorotrail"]
