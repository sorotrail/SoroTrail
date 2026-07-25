# Build stage runs on the build host's native platform and cross-compiles,
# so multi-arch builds don't pay for QEMU-emulated Go compilation.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/sorotrail ./cmd/sorotrail

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 sorotrail
USER sorotrail
COPY --from=build /out/sorotrail /usr/local/bin/sorotrail
EXPOSE 8080
ENTRYPOINT ["sorotrail"]
