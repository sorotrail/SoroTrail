# API Deprecation & Breaking-Change Policy

SoroTrail exposes a public HTTP API (`api/openapi.yaml`, served under both `/` and `/api/v1`), a read-only GraphQL API (`internal/api/graphql/schema.graphqls` at `/graphql`), WebSocket and webhook event payloads, and generated Go (`pkg/client`) and Python (`clients/python`) SDKs. This document defines what constitutes a breaking change, the minimum support window before removal, how deprecations are signalled on the wire and in generated clients, and how operators verify that deprecated traffic has drained before retiring an endpoint or field.

---

## 1. Breaking vs. Non-Breaking Changes

### Breaking changes (require a deprecation window or new `/api/vN` prefix)

Any change that can cause an existing, spec-compliant client to fail at compile time, fail to deserialize a response, or observe different semantics for the same request is **breaking**:

- **Routes & methods:** Removing or renaming an HTTP path, changing an HTTP method, or removing support for either the unprefixed (`/<path>`) or versioned (`/api/v1/<path>`) mount.
- **Request parameters & bodies:** Adding a new *required* query parameter, header, or JSON request property; narrowing the accepted type, format, or enum set of an existing parameter; or tightening validation bounds (for example, lowering the default `limit` ceiling).
- **Response payloads & status codes:** Removing or renaming a JSON response property; changing a property's type or nullability (for example, `string` to `integer`, or non-nullable to `null`); changing the pagination envelope shape (`{"data": [...], "next_cursor": "..."}`); or changing the HTTP status code or machine-readable error `code` returned for a documented condition.
- **Streaming & webhooks:** Removing or renaming fields in WebSocket (`/events/ws`) frames or webhook delivery payloads, or altering the `X-SoroTrail-Signature` HMAC signing format.
- **GraphQL (`internal/api/graphql/schema.graphqls`):** Removing a query field, object field, or enum value; changing a field's return type or making a nullable return type non-null where errors can propagate; or adding a required (non-null without default) argument to an existing field.

### Non-breaking (additive) changes (may ship in any minor or patch release)

Clients must tolerate additive evolution without failing:

- Adding a new HTTP endpoint or HTTP method on an existing path.
- Adding an *optional* query parameter or *optional* request body field with a backward-compatible default.
- Adding new fields to JSON response objects, WebSocket frames, or webhook payloads (both `pkg/client` and `clients/python` ignore unknown JSON keys during deserialization).
- Adding new optional fields, types, or arguments with default values to the GraphQL schema.
- Adding new HTTP response headers (such as `Deprecation`, `Sunset`, `Link`, or `X-RateLimit-*`).
- Adding new Prometheus metrics or new enum values to informational endpoints where clients are documented to treat unknown values as opaque strings.

---

## 2. Deprecation Support Window

No deprecated endpoint, query parameter, response field, or GraphQL field is removed without completing the full deprecation lifecycle:

1. **Minimum calendar window:** At least **90 days** between the release that marks the item deprecated and the release that removes it.
2. **Minimum release window:** At least **2 minor releases** must ship with the deprecation active before the item is removed in a subsequent release.
3. **Traffic verification gate:** Operators can monitor `sorotrail_http_deprecated_requests_total{route, method}` in Prometheus to confirm that client calls to a deprecated route have reached zero prior to upgrading to the release that removes it.

---

## 3. Signaling Deprecations in OpenAPI & HTTP Headers

`api/openapi.yaml` is the single source of truth for REST deprecations. Running `make spec` renders `api/openapi.yaml` into `internal/api/openapi.json` (embedded into the binary and served at `GET /openapi.json` and `GET /docs`). At startup, `internal/api/deprecation.go` (`DefaultDeprecationPolicy`) parses the embedded OpenAPI specification and configures `deprecationMiddleware` on the router automatically.

### OpenAPI Extensions

When deprecating an HTTP operation (or schema property), set `deprecated: true` alongside the following extensions in `api/openapi.yaml`:

| Field / Extension | Required | Format | Purpose |
| :--- | :--- | :--- | :--- |
| `deprecated` | **Yes** | `true` (boolean) | Marks the operation or schema property as deprecated in OpenAPI 3.1, Swagger UI (`/docs`), the HTTP middleware, and `cmd/clientgen`. |
| `x-deprecation-date` | Recommended | RFC 3339 timestamp (`2026-06-01T00:00:00Z`), `YYYY-MM-DD`, or `@<unix-seconds>` | Emitted in the HTTP `Deprecation` header as an [RFC 9745](https://www.rfc-editor.org/rfc/rfc9745) `@<unix-timestamp>` value. When omitted, `Deprecation: true` is sent. |
| `x-sunset-date` | Recommended | RFC 3339 timestamp (`2026-09-01T00:00:00Z`), `YYYY-MM-DD`, or HTTP-date | Converted to GMT and emitted in the HTTP `Sunset` header as an [RFC 8594](https://www.rfc-editor.org/rfc/rfc8594) HTTP-date (`Tue, 01 Sep 2026 00:00:00 GMT`). |
| `x-deprecation-replacement` | Recommended | Target URI or `<METHOD> <path>` (e.g. `/api/v1/events/aggregate`) | Emitted as an HTTP `Link: <URI>; rel="successor-version"` header and included in generated Go/Python SDK deprecation notices. |
| `x-deprecation-notice` | Optional | Human-readable sentence | Included verbatim in generated Go (`// Deprecated: ...`) and Python (`Deprecated: ...`) SDK doc comments. |

> [!IMPORTANT]
> Setting `x-deprecation-date` or `x-sunset-date` on an operation without `deprecated: true` is rejected by `ParseOpenAPIDeprecations` and fails `TestOpenAPIDeprecationConsistency` (`go test ./internal/api -run TestOpenAPIDeprecationConsistency`).

### Worked Example

Suppose `GET /events` is superseded by `GET /api/v1/events/aggregate`, deprecated on **2026-06-01**, and scheduled for removal on **2026-09-01** (92 days later):

```yaml
# api/openapi.yaml
paths:
  /events:
    get:
      operationId: listEvents
      summary: List indexed events
      deprecated: true
      x-deprecation-date: "2026-06-01T00:00:00Z"
      x-sunset-date: "2026-09-01T00:00:00Z"
      x-deprecation-replacement: "/api/v1/events/aggregate"
      x-deprecation-notice: "Legacy event listing is superseded."
```

After running `make spec` and `make client`, every HTTP response from `GET /events` (and `GET /api/v1/events`) includes the following headers:

```http
HTTP/1.1 200 OK
Content-Type: application/json
Deprecation: @1780272000
Sunset: Tue, 01 Sep 2026 00:00:00 GMT
Link: </api/v1/events/aggregate>; rel="successor-version"
```

### Operator Telemetry (`sorotrail_http_deprecated_requests_total`)

Every request matching a deprecated operation increments the Prometheus counter `sorotrail_http_deprecated_requests_total`, labelled by canonical OpenAPI `route` pattern (for example, `"/events/{id}"`, never raw path parameter values) and HTTP `method`:

```promql
# Request rate to deprecated endpoints over the last 5 minutes by route:
sum by (route, method) (rate(sorotrail_http_deprecated_requests_total[5m]))
```

Both unprefixed (`/events/123`) and versioned (`/api/v1/events/123`) requests record against the same canonical route label (`route="/events/{id}"`) so cardinality stays bounded to the routes in `api/openapi.yaml`.

---

## 4. Generated Clients (`pkg/client` & `clients/python`)

Both SDKs are generated from `api/openapi.yaml` via `make client` (`cmd/clientgen`, implemented in `internal/clientgen/clientgen.go` and `internal/clientgen/python.go`) and guarded in CI by `pkg/client/drift_test.go` and `pkg/client/python_drift_test.go`.

### What happens on regeneration (`make client`)

- **Go SDK (`pkg/client/generated.go`):**
  - Operations with `deprecated: true` receive a standard Go doc comment paragraph (`// Deprecated: <notice> Use <replacement> instead. Sunset: <date>.`) immediately above the method declaration. Go tooling (`go vet`, `staticcheck` `SA1019`, `golangci-lint`, and `gopls`) highlights calls to the deprecated method at build and edit time without breaking compilation.
  - Schema properties with `deprecated: true` receive a `// Deprecated: ...` doc comment directly above the struct field in the generated model type.
- **Python SDK (`clients/python/sorotrail/_generated.py`):**
  - Operations with `deprecated: true` include `Deprecated: <notice> Use <replacement> instead. Sunset: <date>.` in the generated method docstring.
  - Schema properties with `deprecated: true` emit a `# Deprecated: ...` comment above the dataclass field definition.

### What pinned consumers experience

| Lifecycle Phase | Consumer on Updated SDK | Consumer Pinned to Older SDK |
| :--- | :--- | :--- |
| **During deprecation window** (`Deprecation` active, before `Sunset`) | Sees `// Deprecated:` IDE/linter warnings (`SA1019` in Go, docstring notice in Python). All methods and fields continue to function identically on the wire. | Zero code or wire breakage. Requests continue to succeed with `2xx` responses while the server attaches `Deprecation`, `Sunset`, and `Link` headers on every response. |
| **After removal** (on or after `Sunset` date) | Regenerated SDK no longer contains the removed method or struct/dataclass field; callers must migrate to the `rel="successor-version"` replacement before upgrading the SDK. | Calling a removed endpoint returns `404 Not Found` (wrapped as `*client.APIError` in Go and `SoroTrailAPIError` in Python). Removed response fields deserialize to their Go/Python zero values (`""`, `0`, `nil`/`None`), while removed request fields are ignored or rejected with `400 Bad Request`. |

---

## 5. GraphQL Deprecation Policy (`/graphql`)

The read-only GraphQL schema in `internal/api/graphql/schema.graphqls` follows the same **90-day / 2-minor-release** support window using the built-in GraphQL `@deprecated` directive:

1. **Annotating deprecated fields and enum values:**
   Add `@deprecated(reason: "...")` in `internal/api/graphql/schema.graphqls` naming both the replacement field and the scheduled Sunset date (`YYYY-MM-DD`):
   ```graphql
   type Event {
     id: ID!
     topics: [String!]!
     legacyTopic: String @deprecated(reason: "Use `topics` instead (sunset 2026-09-01).")
   }
   ```
2. **Consumer discovery:**
   - GraphQL introspection (`__Schema`, `__Type`, `__Field`, `__EnumValue`) automatically exposes `isDeprecated: true` and `deprecationReason` to GraphiQL (`/graphiql`), code generators (`graphql-codegen`, Apollo, Relay), and IDE plugins.
   - Standard introspection queries omit deprecated fields and enum values unless `includeDeprecated: true` is passed, steering new queries toward the replacement while keeping existing queries working.
3. **Wire behavior during and after the window:**
   - Throughout the deprecation window, resolvers continue to populate deprecated fields so existing GraphQL queries succeed without errors.
   - After the Sunset date and at least two minor releases have elapsed, the field or enum value is removed from `internal/api/graphql/schema.graphqls` and `gqlgen` code is regenerated. Queries still selecting the removed field after Sunset receive a standard GraphQL validation error (`Cannot query field "..." on type "..."`) before resolver execution.
