FROM golang:1.25-alpine AS build
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

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 sorotrail
USER sorotrail
COPY --from=build /out/sorotrail /usr/local/bin/sorotrail
EXPOSE 8080
ENTRYPOINT ["sorotrail"]
