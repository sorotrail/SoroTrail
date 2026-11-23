# GraphQL API Guide

SoroTrail exposes a read-only GraphQL transport at `/graphql` alongside the REST API. Both transports share the filter-construction and pagination validation logic in `internal/api/queries`, so event filtering, sorting, and cursor bounds behave identically on REST and GraphQL.

For the full schema definition, see [`internal/api/graphql/schema.graphqls`](../internal/api/graphql/schema.graphqls).

## Enabling the endpoint and the playground

The `/graphql` endpoint is mounted automatically whenever the HTTP API server starts (`cmd/sorotrail/main.go`). It accepts both `POST` and `GET` requests:

- `POST /graphql` expects a JSON request body:
  ```json
  {
    "query": "query ($id: ID!) { event(id: $id) { id ledger } }",
    "variables": { "id": "0001099511627776-0000000001" },
    "operationName": ""
  }
  ```
  Request bodies are capped at 1 MiB (`1 << 20` bytes).
- `GET /graphql?query=...` runs the query passed in the `query` URL parameter. Calling `GET /graphql` with no `query` parameter returns `{"message":"POST {\"query\":\"…\"} to this endpoint"}`.
- Any HTTP method other than `GET` or `POST` returns `405 Method Not Allowed` with `Allow: GET, POST` and a GraphQL error envelope.

### GraphiQL playground

SoroTrail includes an embedded GraphiQL playground at `GET /graphiql`. It is disabled by default so production instances do not serve an interactive browser UI unless requested.

Enable it by setting `GRAPHQL_PLAYGROUND=true`:

```sh
GRAPHQL_PLAYGROUND=true DATABASE_URL=sqlite:./sorotrail.db make run
```

When `GRAPHQL_PLAYGROUND` is unset or `false`, `GET /graphiql` returns `404 Not Found` with the GraphQL error envelope:

```json
{
  "data": {},
  "errors": [
    {
      "message": "playground is disabled"
    }
  ]
}
```

## Authentication

GraphQL sits behind the same authentication middleware as the REST read routes (see [API Keys and Scopes](api-keys.md) and [Multi-tenancy](multi-tenancy.md)):

- **Single-tenant mode (`MULTI_TENANT=false`, default):** Read endpoints, including `/graphql`, are public even when `API_KEY_AUTH_ENABLED=true` (which gates write, streaming, and subscription-management routes).
- **Multi-tenant mode (`MULTI_TENANT=true`):** Every request to `/graphql` must carry a valid tenant API key via `Authorization: Bearer <key>` or `X-API-Key: <key>`. Missing or invalid credentials are rejected by the middleware with `401 Unauthorized` before the GraphQL handler runs.

## Top-level query fields

The schema is strictly read-only (`Query` operations only; mutations and subscriptions are rejected with `only Query operations are supported`). A single HTTP request must contain exactly one operation.

```graphql
type Query {
  "List Soroban contract events."
  events(filter: EventFilterInput, page: PageInput): EventConnection!

  "Fetch a single event by ID. Returns null when the event is not in the store."
  event(id: ID!): Event

  "List events with spec-driven field decoding applied."
  tokenEvents(filter: EventFilterInput, page: PageInput): TokenEventConnection!

  "List watched contracts (the runtime subset of indexed contracts)."
  contracts(page: PageInput): ContractConnection!

  "Fetch a single watched contract by ID. Returns null when the contract is not in the watch list."
  contract(id: ID!): Contract
}
```

Note on execution: SoroTrail's GraphQL executor dispatches top-level `Query` fields directly and serializes each resolver's return struct (`EventConnection`, `EventResult`, `TokenEventConnection`, `ContractConnection`, `ContractResult`) as JSON. The full object shape for each top-level field is returned in `data.<field>` (or `data.<alias>` when aliased).

### 1. `events`

Lists raw Soroban contract events matching `filter` and `page`, returning an `EventConnection`.

Query:

```sh
curl -s -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query ListEvents($contractId: String!, $first: Int!) { events(filter: { contractId: $contractId, types: [\"contract\"] }, page: { first: $first, orderBy: LEDGER, order: ASC }) { edges { cursor node { id contractId ledger type txHash txIndex opIndex inSuccessfulCall topics value createdAt } } nodes { id ledger } pageInfo { hasNextPage endCursor } totalCount } }",
    "variables": {
      "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
      "first": 2
    }
  }'
```

Response:

```json
{
  "data": {
    "events": {
      "edges": [
        {
          "cursor": "eyJpZCI6IjAwMDEwOTk1MTE2Mjc3NzYtMDAwMDAwMDAwMSIsIm9yZGVyX2J5IjoibGVkZ2VyIiwib3JkZXIiOiJhc2MifQ==",
          "node": {
            "id": "0001099511627776-0000000001",
            "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
            "ledger": 256,
            "type": "contract",
            "txHash": "abc123",
            "txIndex": 1,
            "opIndex": 0,
            "inSuccessfulCall": true,
            "topics": [{"symbol": "transfer"}],
            "value": {"i128": "1000"},
            "createdAt": "2026-07-16T12:00:00Z"
          }
        }
      ],
      "nodes": [
        {
          "id": "0001099511627776-0000000001",
          "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
          "ledger": 256,
          "type": "contract",
          "txHash": "abc123",
          "txIndex": 1,
          "opIndex": 0,
          "inSuccessfulCall": true,
          "topics": [{"symbol": "transfer"}],
          "value": {"i128": "1000"},
          "createdAt": "2026-07-16T12:00:00Z"
        }
      ],
      "pageInfo": {
        "hasNextPage": false
      },
      "totalCount": 1
    }
  }
}
```

### 2. `event`

Fetches a single event by its TOID-based `id`. Returns `null` when the event does not exist or is outside the caller's tenant scope. Passing an empty `id` (`""`) returns a GraphQL error (`event id is required`).

Query:

```sh
curl -s -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query GetEvent($id: ID!) { event(id: $id) { id contractId ledger type txHash topics value createdAt } }",
    "variables": { "id": "0001099511627776-0000000001" }
  }'
```

Response:

```json
{
  "data": {
    "event": {
      "id": "0001099511627776-0000000001",
      "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
      "ledger": 256,
      "type": "contract",
      "txHash": "abc123",
      "txIndex": 1,
      "opIndex": 0,
      "inSuccessfulCall": true,
      "topics": [{"symbol": "transfer"}],
      "value": {"i128": "1000"},
      "createdAt": "2026-07-16T12:00:00Z"
    }
  }
}
```

### 3. `tokenEvents`

Accepts the exact same `filter` and `page` arguments as `events`, then passes the resulting page through `spec.Enricher` (the same enrichment path backing `GET /events?decoded=true`). Each item carries `decoded: true` and a populated `decodedEvent { name fields }` object when a contract spec matched the event, or `decoded: false` with `decodedEvent` omitted when no spec was available or decoding failed.

Query:

```sh
curl -s -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query ListTokenEvents($contractId: String!) { tokenEvents(filter: { contractId: $contractId }, page: { first: 5, orderBy: LEDGER, order: DESC }) { nodes { id decoded decodedEvent { name fields } topics value } pageInfo { hasNextPage endCursor } totalCount } }",
    "variables": {
      "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
    }
  }'
```

Response:

```json
{
  "data": {
    "tokenEvents": {
      "edges": [
        {
          "cursor": "eyJpZCI6IjAwMDEwOTk1MTE2Mjc3NzYtMDAwMDAwMDAwMSIsIm9yZGVyX2J5IjoibGVkZ2VyIiwib3JkZXIiOiJkZXNjIn0=",
          "node": {
            "id": "0001099511627776-0000000001",
            "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
            "ledger": 256,
            "type": "contract",
            "txHash": "abc123",
            "txIndex": 1,
            "opIndex": 0,
            "inSuccessfulCall": true,
            "topics": [{"symbol": "transfer"}],
            "value": {"i128": "1000"},
            "createdAt": "2026-07-16T12:00:00Z",
            "decoded": true,
            "decodedEvent": {
              "name": "transfer",
              "fields": { "amount": "1000" }
            }
          }
        }
      ],
      "nodes": [
        {
          "id": "0001099511627776-0000000001",
          "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
          "ledger": 256,
          "type": "contract",
          "txHash": "abc123",
          "txIndex": 1,
          "opIndex": 0,
          "inSuccessfulCall": true,
          "topics": [{"symbol": "transfer"}],
          "value": {"i128": "1000"},
          "createdAt": "2026-07-16T12:00:00Z",
          "decoded": true,
          "decodedEvent": {
            "name": "transfer",
            "fields": { "amount": "1000" }
          }
        }
      ],
      "pageInfo": {
        "hasNextPage": false
      },
      "totalCount": 1
    }
  }
}
```

### 4. `contracts`

Lists the runtime watched contracts (`store.ListWatchedContracts`), returning a `ContractConnection`. Note that `contracts` takes `first`, `after`, `last`, `before`, and `order` directly as field arguments (`ContractPageInput`), or no arguments for the default page size.

Query:

```sh
curl -s -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ contracts { edges { cursor node { contractId addedAt } } nodes { contractId addedAt } pageInfo { hasNextPage endCursor } totalCount } }"
  }'
```

Response:

```json
{
  "data": {
    "contracts": {
      "edges": [
        {
          "cursor": "eyJpZCI6IkNETFpGQzNTWUpZRFpUN0s2N1ZaNzVIUEpWSUVVVk5JVEY0N1pHMkZCMlJNUVFWVTJISEdDWVNDIn0=",
          "node": {
            "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
            "addedAt": "2026-07-16T12:00:00Z"
          }
        }
      ],
      "nodes": [
        {
          "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
          "addedAt": "2026-07-16T12:00:00Z"
        }
      ],
      "pageInfo": {
        "hasNextPage": false
      },
      "totalCount": 1
    }
  }
}
```

### 5. `contract`

Looks up a single watched contract by `id`. Returns `null` when the contract is not in the watch list. Passing an empty `id` (`""`) returns a GraphQL error (`contract id is required`).

Query:

```sh
curl -s -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query GetContract($id: ID!) { contract(id: $id) { contractId addedAt } }",
    "variables": { "id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC" }
  }'
```

Response:

```json
{
  "data": {
    "contract": {
      "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
      "addedAt": "2026-07-16T12:00:00Z"
    }
  }
}
```

## Pagination

All three list fields (`events`, `tokenEvents`, `contracts`) return a Relay-style connection object with four fields:

- `edges`: array of `{ cursor, node }` pairs.
- `nodes`: shorthand array of `node` objects, matching `edges[].node` in the same order.
- `pageInfo`: `{ hasNextPage, endCursor }`. When `hasNextPage` is `true`, pass `endCursor` back as `after` on the next request to resume after the last item on the page.
- `totalCount`: total matching records ignoring pagination (`CountEvents` for `events`/`tokenEvents`, or the length of the watch list for `contracts`).

### How cursors work

GraphQL cursors (`scalar Cursor`) are base64-encoded JSON objects produced by `EncodeCursor` (`internal/api/graphql/cursor.go`):

```json
{"id":"0001099511627776-0000000001","order_by":"id","order":"asc"}
```

For `contracts`, `order_by` and `order` are omitted (`{"id":"C..."}`). Clients must treat cursors as opaque strings and never construct or modify them by hand. Passing a malformed cursor returns a GraphQL error (`invalid cursor: not base64`, `invalid cursor: not JSON`, or `invalid cursor: missing id`). On `contracts`, if a valid cursor references a contract ID that is no longer in the watch list, the resolver returns an empty page (`edges: []`, `nodes: []`, `hasNextPage: false`) rather than failing the request.

### Page-size defaults and caps

- **Default page size:** `50` (`queries.DefaultPageSize` / `store.DefaultQueryLimit`) when `first` is omitted or `0`.
- **Maximum page size:** `500` (`queries.MaxPageSize` / `store.MaxQueryLimit`). Requesting `first: 501` (or a negative `first`) fails with `first must be in [1,500] (got 501)`.

### Backward pagination is not supported

Although `PageInput` declares `last` and `before` for Relay input shape compatibility, backward pagination is not implemented on either `events`/`tokenEvents` (`queries.ResolvePage`) or `contracts` (`resolveContracts`):

- Passing `last` or `before` to `events`, `tokenEvents`, or `contracts` returns an error:
  ```json
  {
    "data": { "events": null },
    "errors": [
      {
        "message": "backward pagination (last/before) is not supported",
        "path": ["events"]
      }
    ]
  }
  ```
- Combining forward and backward pagination (`first` with `last`, `after` with `before`, `first` with `before`, or `last` with `after`) is rejected with `cannot combine forward (first/after) and backward (last/before) pagination`.

### Sort order and columns

`events` and `tokenEvents` accept `order` (`ASC` or `DESC`, case-insensitive; default `ASC`) and `orderBy` (`ID`, `LEDGER`, or `CREATED_AT`; default `ID`). Every ordering appends `id` as a tiebreaker in SQL so keyset pagination stays deterministic when multiple events share a `ledger` or `created_at` timestamp.

## Limits: depth and complexity

Every incoming query is checked by `CheckDepth` and `CheckComplexity` (`internal/api/graphql/limits.go`) after AST parsing and before any resolver executes. Violations return HTTP `200 OK` with `data: {}` and a single entry in `errors`.

| Limit | Constant | Default | How it is computed |
| --- | --- | --- | --- |
| Depth | `graphql.DepthLimit` | `10` | The root operation's selection set starts at depth `1`. Each nested field, inline fragment, or fragment spread increments the depth by `1`. |
| Complexity | `graphql.ComplexityLimit` | `1000` | Starts with a per-operation base cost of `5` (`operationCost`). Top-level connection fields (`events`, `tokenEvents`, `contracts`) add `25` (`connectionCost`); every other field in the selection tree adds `1` (`leafFieldCost`). |

### Example: query exceeding the depth limit

A query nested to depth 11 (exceeding `DepthLimit = 10`):

```sh
curl -s -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ a { b { c { d { e { f { g { h { i { j { k } } } } } } } } } } }"
  }'
```

Response:

```json
{
  "data": {},
  "errors": [
    {
      "message": "query depth 11 exceeds depth limit 10"
    }
  ]
}
```

Similarly, a query whose field weights plus the `5`-unit operation overhead exceed `1000` is rejected before touching the store with:

```json
{
  "data": {},
  "errors": [
    {
      "message": "query complexity 1005 exceeds complexity limit 1000"
    }
  ]
}
```

## Persisted operations and allowlist mode

SoroTrail supports persisted GraphQL operations and an optional allowlist-only execution mode (`GRAPHQL_PERSISTED_ONLY=true`) on `/graphql`.

For production frontends whose GraphQL query set is known at build time, registering a persisted-query allowlist lets the server execute only pre-approved query documents identified by their canonical SHA-256 digest and refuse arbitrary ad-hoc query documents altogether.

### 1. Canonical Identifier & Registration Validation

Every persisted operation is identified by the **lowercase hex-encoded SHA-256 digest** of its GraphQL query document (`sha256(query)`):

```sh
printf '%s' 'query GetContract($id: ID!) { contract(id: $id) { contractId addedAt } }' | shasum -a 256
# e.g. 9f2c8b...
```

When an operation is registered (via startup manifest file, `POST / PUT /graphql/persisted`, or Automatic Persisted Queries):

1. **Hash verification**: If the caller supplies an `id` / `sha256Hash`, SoroTrail computes `sha256(query)` and verifies that the supplied identifier matches. Any mismatch is rejected (`400 Bad Request` on `/graphql/persisted`, or `PERSISTED_QUERY_HASH_MISMATCH` on `/graphql`), preventing callers from binding an arbitrary query to a chosen ID.
2. **AST & operation validation**: The document must parse as valid GraphQL containing exactly one read-only `Query` operation (`mutation` and `subscription` operations are rejected).
3. **Static depth & complexity guards**: The document must pass `CheckDepth` (`<= 10`) and static `CheckComplexity` (`<= 1000`) at registration time.

### 2. Persisted Operations Configuration

| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `GRAPHQL_PERSISTED_ONLY` | `false` | When `true`, enables strict allowlist mode: `/graphql` refuses ad-hoc query documents and unauthenticated on-the-fly APQ registrations. Only pre-registered operations may execute. |
| `GRAPHQL_PERSISTED_OPERATIONS_FILE` | *(unset)* | Optional path to a JSON manifest file of persisted operations loaded at startup and atomically updated when operations are registered, replaced, or deleted via `/graphql/persisted`. |
| `API_KEY` | *(unset)* | Operator API key required via `X-API-Key` or `Authorization: Bearer` on `POST`, `PUT`, and `DELETE` requests to `/graphql/persisted` (and for authenticated on-the-fly APQ registration when `GRAPHQL_PERSISTED_ONLY=true`). |

### 3. Registering, Inspecting, and Updating Persisted Operations

#### A. Startup Manifest File (`GRAPHQL_PERSISTED_OPERATIONS_FILE`)

A frontend build can export a JSON manifest and load it at server startup:

- **Map format (Apollo / Relay / GraphQL Codegen)**:
  ```json
  {
    "2c9a6f0b1a4e...": "query GetContracts { contracts { totalCount } }",
    "8d4e1f7c9b02...": "query GetEvent($id: ID!) { event(id: $id) { id ledger } }"
  }
  ```
- **Structured object or array format**:
  ```json
  {
    "operations": [
      {
        "id": "2c9a6f0b1a4e...",
        "query": "query GetContracts { contracts { totalCount } }"
      }
    ]
  }
  ```

#### B. Inspecting the Allowlist (`GET /graphql/persisted`)

Anyone with read access to the GraphQL transport can inspect the currently registered operations:

```sh
curl -s http://localhost:8080/graphql/persisted
```

Response (`200 OK`, sorted deterministically by `id`):

```json
{
  "allowlistOnly": true,
  "count": 1,
  "operations": [
    {
      "id": "2c9a6f0b1a4e...",
      "operationName": "GetContracts",
      "query": "query GetContracts { contracts { totalCount } }",
      "registeredAt": "2026-07-16T12:00:00Z"
    }
  ]
}
```

Fetch a single operation by SHA-256 ID:

```sh
curl -s http://localhost:8080/graphql/persisted/2c9a6f0b1a4e...
```

#### C. Registering or Merging Operations (`POST /graphql/persisted`)

Requires `X-API-Key: <API_KEY>` or `Authorization: Bearer <API_KEY>`:

```sh
curl -s -X POST http://localhost:8080/graphql/persisted \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{
    "query": "query GetEvent($id: ID!) { event(id: $id) { id contractId ledger } }"
  }'
```

If `"id"` is omitted, SoroTrail derives and returns the canonical SHA-256 digest. If `"id"` is provided, SoroTrail verifies that it matches the SHA-256 digest of `"query"`.

#### D. Atomic Release Replacement (`PUT /graphql/persisted`)

When deploying a new frontend release, replace the entire allowlist atomically with `PUT /graphql/persisted` (or `POST /graphql/persisted` with `"replace": true`). Every operation in the manifest is validated before the swap; if any operation is invalid or has a mismatched hash, the request fails with `400 Bad Request` and the existing allowlist remains untouched:

```sh
curl -s -X PUT http://localhost:8080/graphql/persisted \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{
    "operations": [
      {
        "id": "2c9a6f0b1a4e...",
        "query": "query GetContracts { contracts { totalCount } }"
      }
    ]
  }'
```

#### E. Removing a Retired Operation (`DELETE /graphql/persisted/{id}`)

```sh
curl -s -X DELETE http://localhost:8080/graphql/persisted/2c9a6f0b1a4e... \
  -H "X-API-Key: $API_KEY"
```

### 4. Executing a Persisted Query

Clients can execute a registered operation by supplying its SHA-256 identifier (along with `variables` and optional `operationName`) in place of `query` on either `POST /graphql` or `GET /graphql`.

#### A. Apollo Automatic Persisted Queries (APQ) Shape (`extensions.persistedQuery`)

```sh
curl -s -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "operationName": "GetEvent",
    "variables": { "id": "0001099511627776-0000000001" },
    "extensions": {
      "persistedQuery": {
        "version": 1,
        "sha256Hash": "8d4e1f7c9b02..."
      }
    }
  }'
```

Or via `GET /graphql`:

```sh
curl -s -G http://localhost:8080/graphql \
  --data-urlencode 'extensions={"persistedQuery":{"version":1,"sha256Hash":"8d4e1f7c9b02..."}}' \
  --data-urlencode 'variables={"id":"0001099511627776-0000000001"}'
```

#### B. Top-Level Identifier Shape (`id` / `documentId` / `persistedId` / `sha256Hash`)

```sh
curl -s -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "id": "8d4e1f7c9b02...",
    "variables": { "id": "0001099511627776-0000000001" }
  }'
```

### 5. Error Codes & Allowlist Mode Behavior

All GraphQL-level persisted-query errors return HTTP `200 OK` with the standard GraphQL envelope and a machine-readable `extensions.code`:

| `errors[0].extensions.code` | When it is returned |
| :--- | :--- |
| `PERSISTED_QUERY_NOT_FOUND` | The request supplied a persisted identifier without a `query` body, and the identifier is not in the registered allowlist (`message: "PersistedQueryNotFound"`). |
| `PERSISTED_QUERY_HASH_MISMATCH` | The request supplied both a persisted identifier and a `query` document whose SHA-256 digest does not match the identifier. |
| `PERSISTED_QUERY_ONLY` | `GRAPHQL_PERSISTED_ONLY=true` is enabled and the caller sent an ad-hoc `query` document or attempted unauthenticated on-the-fly APQ registration. |

#### Per-Request Variable Validation & Cost Checks

Persisted operations do **not** bypass runtime checks when executed by ID:

1. **Variable validation**: Required non-null variables (`$id: ID!`) must be provided and non-null, default variable values are applied, and runtime variable types (`Int`, `Int64`, `Float`, `Boolean`, `String`, `ID`, lists, and input objects) are validated before resolver execution.
2. **Variable-dependent complexity checks**: When a connection field (`events`, `tokenEvents`, `contracts`) receives a runtime `page.first` / `first` variable exceeding the default page size (`50`), `CheckComplexityWithVariables` scales that connection field's cost by `ceil(first / 50)`. A query that passed static registration checks is still rejected at execution time if runtime variables push its cost past `ComplexityLimit` (`1000`).

## Filter parity with `GET /events`

`EventFilterInput` and `PageInput` pass through `queries.BuildEventFilter`, the same function used by REST's `filterFromQuery`. `TestFilterParity_RESTAndGraphQLProduceIdenticalFilters` in [`internal/api/graphql/filter_parity_test.go`](../internal/api/graphql/filter_parity_test.go) enforces that equivalent inputs produce identical `store.EventFilter` structs.

### Shared filters (`GET /events` and GraphQL `events` / `tokenEvents`)

| `GET /events` query param | GraphQL `EventFilterInput` / `PageInput` | Notes |
| --- | --- | --- |
| `contract_id=C...` (single) | `filter.contractId: "C..."` | Must be a valid 56-char `C...` strkey. Mutually exclusive with `contractIds` and `contractIdPrefix`. |
| `contract_id=C1...,C2...` (comma-separated) | `filter.contractIds: ["C1...", "C2..."]` | Matches events from any listed contract. Mutually exclusive with `contractId` and `contractIdPrefix`. |
| `contract_id_prefix=CAB` | `filter.contractIdPrefix: "CAB"` | Prefix match (`C` + 1..55 uppercase base32 chars). Mutually exclusive with `contractId` and `contractIds`. |
| `type=contract,diagnostic` | `filter.types: ["contract", "diagnostic"]` | Allowed values: `contract`, `system`, `diagnostic`. |
| `topic=<json>` | `filter.topic: <JSON>` | Matches events whose `topics` array contains the value at any position. Cannot be combined with `topics.t0..t3`. |
| `topic0=<json>` .. `topic3=<json>` | `filter.topics: { t0, t1, t2, t3 }` | Positional topic match. Cannot be combined with `filter.topic`. |
| `topic_contains=<json-array>` | `filter.topicContains: <JSON>` | JSON array containment on `topics` (`@>`). Must be a JSON array. |
| `tx_hash=<hex>` | `filter.txHash: "<hex>"` | Exact match on transaction hash. |
| `tx_index=<int>` | `filter.txIndex: <Int>` | Exact match on transaction index within a ledger (`>= 0`). |
| `op_index=<int>` | `filter.opIndex: <Int>` | Exact match on operation index within a transaction (`>= 0`). |
| `in_successful_call=true\|false` | `filter.inSuccessfulCall: true\|false` | Tri-state boolean (`null`/omitted = no constraint). |
| `has_value=true\|false` | `filter.hasValue: true\|false` | `true` = `value IS NOT NULL`, `false` = `value IS NULL`. |
| `from_ledger=<int>` | `filter.fromLedger: <Int>` | Inclusive lower ledger bound (`> 0`). Mutually exclusive with `fromTime`. |
| `to_ledger=<int>` | `filter.toLedger: <Int>` | Inclusive upper ledger bound (`>= fromLedger`). |
| `from_time=<RFC3339>` | `filter.fromTime: "<RFC3339>"` | Inclusive lower timestamp bound. Mutually exclusive with `fromLedger`. |
| `to_time=<RFC3339>` | `filter.toTime: "<RFC3339>"` | Inclusive upper timestamp bound (`>= fromTime`). |
| `limit=<int>` | `page.first: <Int>` | `1..500` (default `50`). Note: `API_MAX_LIMIT` can lower the REST ceiling, while GraphQL always validates against `queries.MaxPageSize` (`500`). |
| `cursor=<id>` | `page.after: "<base64-cursor>"` | REST takes the raw event ID; GraphQL wraps `{id, order_by, order}` in base64. |
| `order=asc\|desc` | `page.order: ASC\|DESC` | Default `ASC`. |
| `order_by=id\|ledger\|created_at` | `page.orderBy: ID\|LEDGER\|CREATED_AT` | Default `ID`. |
| `decoded=true` | Use the `tokenEvents` query field | `tokenEvents` runs `spec.Enricher` on the page, matching `GET /events?decoded=true`. |

### Differences between `GET /events` and GraphQL

1. **Multi-tenant scope wiring (`Scope`) — active bug on `events`, `tokenEvents`, `contracts`, and `contract`:**
   - On REST `GET /events`, `filterFromQuery` attaches `f.Scope = scopeFrom(r.Context())` and rejects an explicitly requested `contract_id` outside the caller's grants with `403 Forbidden`.
   - On GraphQL (`internal/api/graphql/events.go` and `internal/api/graphql/contracts.go`), only `event(id)` currently passes `scopeFrom(ctx)` to `store.GetEvent`:
     - **`events` and `tokenEvents`** call `buildEventFilter(args)` without setting `filter.Scope = scopeFrom(ctx)`. Because the zero `store.Scope` fails closed (`DeniesAll() == true`), `Postgres.QueryEvents` and `ClickHouse.QueryEvents` return `0` rows for every `events` and `tokenEvents` GraphQL query (even in single-tenant mode or for wildcard tenants).
     - **`contracts` and `contract(id)`** call `store.ListWatchedContracts(ctx)`, which returns the unscoped global watch-list union across all tenants rather than filtering by `scopeFrom(ctx)`. This is tracked in [#1139](https://github.com/sorotrail/SoroTrail/issues/1139).
2. **`sep41_event` normalization:** REST `GET /events` calls `e.WithSEP41()` before serializing each row to populate the optional `sep41_event` envelope. GraphQL's `Event` and `TokenEvent` types do not expose `sep41_event`; use `tokenEvents` for spec-driven `decodedEvent` fields or REST `GET /events` for `sep41_event`.
3. **HTTP caching:** REST `GET /events` and `GET /events/{id}` emit `ETag` and `Cache-Control` headers and answer conditional `If-None-Match` requests with `304 Not Modified`. GraphQL responses (`POST /graphql` and `GET /graphql`) do not emit `ETag` headers.

## Multi-tenant behaviour by query field

The table below summarizes how each top-level GraphQL field interacts with tenant scope (`store.Scope`) in the current codebase:

| Field | Store call | Current `Scope` behaviour |
| --- | --- | --- |
| `event(id: ID!)` | `store.GetEvent(ctx, id, scopeFrom(ctx))` | **Enforced.** Events outside the caller's `Scope` return `null` (indistinguishable from a non-existent event ID, preventing TOID probing across tenants). |
| `events(filter, page)` | `store.QueryEvents(ctx, filter)` + `store.CountEvents` | **Fails closed (returns empty).** `resolveEvents` does not assign `filter.Scope = scopeFrom(ctx)`, leaving `filter.Scope` at its zero value (`DeniesAll() == true`), so Postgres and ClickHouse return an empty connection. |
| `tokenEvents(filter, page)` | `store.QueryEvents(ctx, filter)` + `store.CountEvents` | **Fails closed (returns empty).** Like `events`, `resolveTokenEvents` leaves `filter.Scope` at its zero value. |
| `contracts(page)` | `store.ListWatchedContracts(ctx)` | **Unscoped (tracked in [#1139](https://github.com/sorotrail/SoroTrail/issues/1139)).** Calls the unscoped ingestion union `ListWatchedContracts` without consulting `scopeFrom(ctx)`, returning every tenant's watched contracts. |
| `contract(id: ID!)` | `store.ListWatchedContracts(ctx)` | **Unscoped (tracked in [#1139](https://github.com/sorotrail/SoroTrail/issues/1139)).** Scans the unscoped `ListWatchedContracts` result for `id`. |

## When to use GraphQL instead of REST

Use **GraphQL (`/graphql`)** when:
- Your frontend or client already uses a Relay-compatible GraphQL client and benefits from standard `{ edges { cursor node }, pageInfo { hasNextPage endCursor }, totalCount }` connections.
- You want `totalCount` inline in the same response payload as the page of events (on REST, the count is either in the `X-Total-Count` response header or requires a separate `GET /events/count` call).

Use **REST (`/events`, `/contracts`, `/stats`)** when:
- You are running in multi-tenant mode (`MULTI_TENANT=true`) or against Postgres/ClickHouse and need scoped `GET /events` list queries before `filter.Scope` is wired in `resolveEvents` and [#1139](https://github.com/sorotrail/SoroTrail/issues/1139) is resolved.
- You rely on HTTP-level caching (`ETag`, `Cache-Control: immutable`, `304 Not Modified`), which works out of the box with CDNs and browser caches on `GET /events` and `GET /events/{id}`.
- You need endpoints that have no GraphQL equivalent: SEP-41 normalized `sep41_event` envelopes, time/ledger bucket aggregations (`GET /events/aggregate`), transaction sibling lookups (`GET /events/{id}/transaction`), address-indexed lookups (`GET /addresses/{address}/events`), bulk NDJSON/CSV export (`GET /contracts/{id}/export`), or live WebSocket streaming (`GET /events/ws`).
