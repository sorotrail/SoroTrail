# Build stage runs on the build host's native platform and cross-compiles,
# so multi-arch builds don't pay for QEMU-emulated Go compilation.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN CGO_ENABLED=0 go build \
    -ldflags "-X github.com/khaylebfortune/sorotrail/internal/version.Version=${VERSION} -X github.com/khaylebfortune/sorotrail/internal/version.Commit=${COMMIT} -X github.com/khaylebfortune/sorotrail/internal/version.Date=${DATE}" \
    -o /out/sorotrail ./cmd/sorotrail
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
ENTRYPOINT ["sorotrail"]
