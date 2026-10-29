# API error catalogue

Every failure the SoroTrail API can return, in one place: the condition
that produces it, the HTTP status, and the exact message a client sees.
Error bodies are the shared JSON envelope {"error": "<message>"} (the
ErrorResponse schema in `api/openapi.yaml`, with `Cache-Control: no-store`) — the exceptions
are listed under "Responses outside the shared envelope" at the end.

This file is generated from the catalogue in
`internal/api/error_catalogue_test.go`, which is itself checked against
the handlers' source and the shipped OpenAPI spec, so a new error cannot
appear in the code without appearing here. Regenerate it with:

    go test ./internal/api -run TestErrorCatalogueDocumentation -update-error-catalogue

Messages are templates: `%q`, `%s` and `%d` stand for the values the
server interpolates (the offending parameter, an id, a bound), and
`<detail>` or `<json error>` stand for a wrapped underlying error.
Entries marked "middleware-wide" can surface on any operation the
middleware wraps; the OpenAPI spec documents them on the operations where
they matter rather than on every path.

## Conventions

- Every envelope error carries `Cache-Control: no-store` so a CDN cannot pool
  a 4xx/5xx behind a success response's validator.
- 429 carries `Retry-After` (delta-seconds, at least 1) plus the
  `X-RateLimit-*` family.
- 401 carries `WWW-Authenticate`: `Bearer realm="sorotrail"` on the tenant
  credential, `Bearer` on the instance key — both per RFC 7235 §4.1.
- 500 messages are deliberately generic; the specifics are logged
  against the request's `X-Request-ID`.

## Errors by status

### 400 Bad Request

| Condition | Operations | Message |
| --- | --- | --- |
| POST /apikeys names a key longer than the store allows | POST /apikeys | `name must be at most 100 characters` |
| POST /apikeys cannot decode its JSON body | POST /apikeys | `invalid JSON: <json error>` |
| DELETE /apikeys/{id} is given a path id that is not a positive integer | DELETE /apikeys/{id} | `API key id must be a positive integer, got %q` |
| the export's format parameter names something other than csv or ndjson, or its terminator parameter is neither true nor false | GET /contracts/{id}/export | `invalid format %q (want csv or ndjson); invalid terminator (want true or false)` |
| the export is requested without explicit ledger bounds | GET /contracts/{id}/export | `from_ledger and to_ledger are required for export` |
| the export's ledger bounds are inverted | GET /contracts/{id}/export | `from_ledger %d is after to_ledger %d` |
| the export's from_ledger bound is not a positive integer | GET /contracts/{id}/export | `from_ledger <detail>` |
| the export is requested for a path contract id that is not a strkey | GET /contracts/{id}/export | `invalid contract id %q (want 56-char C... strkey)` |
| the export's ledger range is wider than the operator's cap | GET /contracts/{id}/export | `ledger range [%d,%d] spans %d ledgers, exceeding EXPORT_MAX_RANGE=%d` |
| the export's to_ledger bound is not a positive integer | GET /contracts/{id}/export | `to_ledger <detail>` |
| the CSV export's format parameter names something other than csv or ndjson, or its terminator parameter is neither true nor false | GET /events.csv | `invalid format %q (want csv or ndjson); invalid terminator (want true or false)` |
| the first watch would silently narrow ingestion from all contracts to a list, and the caller did not pass ?confirm=true | POST /watched-contracts | `adding the first watched contract would switch ingestion from 'all contract events' to a specific list — pass ?confirm=true to acknowledge` |
| the body's contract_id is not a strkey | POST /watched-contracts | `invalid contract_id %q (want 56-char C... strkey)` |
| the address-events filter fails shared validation | GET /addresses/{address}/events | `event-filter validation message (see "Event-filter validation messages" below)` |
| the address-events index offers id ordering only and the request asked for another sort | GET /addresses/{address}/events | `address events only support order_by=id (the default)` |
| the {address} path segment is neither an account nor a contract strkey | GET /addresses/{address}/events | `invalid address %q (want G... or C... strkey)` |
| the {address} path segment is neither an account nor a contract strkey | GET /addresses/{address}/summary | `invalid address %q (want G... or C... strkey)` |
| the aggregation filter fails shared validation | GET /events/aggregate | `event-filter validation message (see "Event-filter validation messages" below)` |
| the aggregation is requested without a ?bucket | GET /events/aggregate | `bucket parameter is required` |
| the ?bucket names neither the ledger bucket nor a Go duration | GET /events/aggregate | `invalid bucket %q: must be "ledger" or a duration` |
| the {id} path segment of /contracts/{id}/events is not a strkey | GET /contracts/{id}/events | `invalid contract ID %q` |
| DELETE /dead-letters/{id} is given a path id that is not a positive integer | DELETE /dead-letters/{id} | `dead-letter id must be a positive integer, got %q` |
| the bulk delete is requested without the mandatory before_ledger bound | DELETE /events | `before_ledger query parameter is required` |
| the bulk delete's before_ledger bound is not a positive integer | DELETE /events | `before_ledger must be a positive integer, got %q` |
| the ?fields= allowlist names a field the event object does not have | GET /events/{id}/transaction | `unknown field %q (valid: id, contract_id, ledger, type, tx_hash, op_index, in_successful_call, topics, value, created_at)` |
| the ?fields= allowlist names a field the event object does not have | GET /events/{id} | `unknown field %q (valid: id, contract_id, ledger, type, tx_hash, op_index, in_successful_call, topics, value, created_at)` |
| GET /contracts is asked to sort by a column it does not offer | GET /contracts | `invalid sort %q (want %s, %s, %s, %s, or %s)` |
| the contracts cursor fails the charset check, or passes it but does not decode | GET /contracts | `invalid cursor %q` |
| GET /contracts is asked to order by something other than asc or desc | GET /contracts | `invalid order %q (want asc or desc)` |
| the contracts page size is outside [1, limit] | GET /contracts | `limit must be an integer in [1,%d]` |
| the dead-letter cursor is not a cursor this endpoint produced | GET /dead-letters | `invalid cursor %q` |
| the dead-letter page size is outside [1, limit] | GET /dead-letters | `limit must be an integer in [1,%d]` |
| removing the last watch would silently widen ingestion back to all contracts, and the caller did not pass ?confirm=true | DELETE /watched-contracts/{id} | `removing the last watched contract would switch ingestion from 'a specific list' back to 'all contract events' — pass ?confirm=true to acknowledge` |
| the {id} path segment is not a strkey | DELETE /watched-contracts/{id} | `invalid contract id %q (want 56-char C... strkey)` |
| a cursor is replayed against a different ordering than the one that produced it (store.ErrInvalidCursor) | GET /events, GET /contracts/{id}/events | `invalid cursor for order_by=%s; use the cursor returned by the same ordering` |
| a list-shaped endpoint rejects a malformed query (shared filter validation); the forbidden-contract case is split out to 403 by the same helper | GET /events, GET /events/count, GET /events.csv, GET /events/ws, GET /contracts/{id}/events, GET /contracts/{id}/export, POST /subscriptions, PUT /subscriptions/{id} | `event-filter validation message (see "Event-filter validation messages" below)` |
| the shared request-body decoder rejects a malformed, empty, or trailing-data body | any authenticated or rate-limited operation (middleware-wide) | `request body is empty; invalid JSON body: <json error>; invalid JSON body: unexpected data after the JSON value; reading request body: <io error>` |
| the {id} path segment is not a strkey | GET /contracts/{id}/stats | `invalid contract ID %q` |
| the {id} path segment is not a strkey | GET /contracts/{id} | `invalid contract ID %q` |
| the {id} path segment is not a strkey | DELETE /contracts/{id}/spec | `invalid contract id %q (want 56-char C... strkey)` |
| the {id} path segment is not a strkey | GET /contracts/{id}/spec | `invalid contract id %q (want 56-char C... strkey)` |
| the override document parses as JSON but is not a valid contract spec (spec.ParseOverrideSpec) | PUT /contracts/{id}/spec | `parsing override spec JSON: <json error>; override spec: "events" array is required; override spec: event #<n>: <detail>; override spec: duplicate event name %q` |
| the PUT body is valid JSON but carries neither a "spec" wrapper nor a bare spec object | PUT /contracts/{id}/spec | `request body must contain a "spec" object` |
| neither wrapped nor bare body could be decoded as JSON | PUT /contracts/{id}/spec | `invalid JSON body: <json error>` |
| the {id} path segment is not a strkey | PUT /contracts/{id}/spec | `invalid contract id %q (want 56-char C... strkey)` |
| the {id} path segment is not a strkey | GET /contracts/{id}/spec/history | `invalid contract id %q (want 56-char C... strkey)` |
| the history page size is outside [1, limit] | GET /contracts/{id}/spec/history | `limit must be an integer in [1,%d]` |
| the ?cursor= is not a cursor this endpoint produced | GET /contracts/{id}/spec/history | `spec override history cursor is not valid base64url; spec override history cursor is not a version id` |
| the {id} path segment is not a strkey | POST /contracts/{id}/spec/revert | `invalid contract id %q (want 56-char C... strkey)` |
| the body names no history version to restore | POST /contracts/{id}/spec/revert | `version must be a positive integer` |
| the subscription is created without the HMAC signing secret | POST /subscriptions | `secret is required` |
| the subscription is created without a webhook URL | POST /subscriptions | `url is required` |
| DELETE /subscriptions/{id} is given a path id that is not a positive integer | DELETE /subscriptions/{id} | `subscription id must be a positive integer, got %q` |
| GET /subscriptions/{id} is given a path id that is not a positive integer | GET /subscriptions/{id} | `subscription id must be a positive integer, got %q` |
| GET /subscriptions/{id}/deliveries is given a path id that is not a positive integer | GET /subscriptions/{id}/deliveries | `subscription id must be a positive integer, got %q` |
| the delivery-history page size is outside [1, limit] | GET /subscriptions/{id}/deliveries | `limit must be an integer in [1,%d]` |
| PUT /subscriptions/{id} is given a path id that is not a positive integer | PUT /subscriptions/{id} | `subscription id must be a positive integer, got %q` |
| the update would empty the subscription's webhook URL | PUT /subscriptions/{id} | `url must not be empty` |
| the body's contract_id is not a strkey | POST /tenant/watch | `invalid contract_id %q` |
| the tenant's quota fields are inconsistent or negative (validateTenantQuotas) | POST /admin/tenants | `rate_limit_rps must be non-negative; rate_limit_burst must be non-negative; max_watched_contracts must be non-negative; rate_limit_rps and rate_limit_burst must be set together` |
| the tenant is created without a name | POST /admin/tenants | `name is required` |
| the grant names a contract_id that is not a strkey | POST /admin/tenants/{id}/grants | `invalid contract_id %q` |
| the {contract_id} path segment is not a strkey | DELETE /tenant/watch/{contract_id} | `invalid contract_id %q` |
| the update would leave the tenant's quota fields inconsistent or negative (validateTenantQuotas) | PATCH /admin/tenants/{id} | `rate_limit_rps must be non-negative; rate_limit_burst must be non-negative; max_watched_contracts must be non-negative; rate_limit_rps and rate_limit_burst must be set together` |
| the usage window is outside [1, days] | GET /tenant/usage, GET /admin/tenants/{id}/usage | `days must be an integer in [1,%d]` |
| an admin path id ({id} or {key_id}) is not an integer | GET /admin/tenants/{id}, PATCH /admin/tenants/{id}, GET /admin/tenants/{id}/usage, GET /admin/tenants/{id}/grants, POST /admin/tenants/{id}/grants, DELETE /admin/tenants/{id}/grants/{contract_id}, GET /admin/tenants/{id}/keys, POST /admin/tenants/{id}/keys, DELETE /admin/tenants/{id}, DELETE /admin/keys/{key_id} | `invalid %s %q` |

### 401 Unauthorized

| Condition | Operations | Message |
| --- | --- | --- |
| requireAPIKey sees a missing, malformed, unknown, revoked or mismatched key — one sentence for all five so the response is not an existence oracle | POST /apikeys, GET /apikeys, DELETE /apikeys/{id}, GET /events/ws, POST /subscriptions, PUT /subscriptions/{id}, DELETE /subscriptions/{id}, GET /subscriptions, GET /subscriptions/{id}, GET /subscriptions/{id}/deliveries | `missing or invalid API key` |
| multi-tenant authentication rejects the credential, with the reason that lets a legitimate caller fix it | any authenticated or rate-limited operation (middleware-wide) | `missing API key; malformed API key; unknown or revoked API key` |
| a management endpoint is called without the configured X-API-Key, or with the wrong one | DELETE /events, GET /watched-contracts, POST /watched-contracts, DELETE /watched-contracts/{id}, PUT /contracts/{id}/spec, GET /contracts/{id}/spec, DELETE /contracts/{id}/spec, GET /contracts/{id}/spec/history, POST /contracts/{id}/spec/revert, GET /dead-letters, DELETE /dead-letters/{id} | `invalid or missing X-API-Key header` |

### 403 Forbidden

| Condition | Operations | Message |
| --- | --- | --- |
| multi-tenant authentication holds a valid key for an account an administrator has suspended | any authenticated or rate-limited operation (middleware-wide) | `tenant is disabled` |
| a non-admin tenant calls an /admin endpoint | POST /admin/tenants, GET /admin/tenants, GET /admin/tenants/{id}, PATCH /admin/tenants/{id}, DELETE /admin/tenants/{id}, GET /admin/tenants/{id}/usage, GET /admin/tenants/{id}/grants, POST /admin/tenants/{id}/grants, DELETE /admin/tenants/{id}/grants/{contract_id}, GET /admin/tenants/{id}/keys, POST /admin/tenants/{id}/keys, DELETE /admin/keys/{key_id} | `admin privileges required` |
| the caller named a contract outside its grants — 403, not 404, because contract IDs are public and hiding an existing contract behind 404 would send operators debugging missing grants as missing data | GET /events, GET /events/count, GET /events.csv, GET /events/ws, GET /contracts/{id}/events, GET /contracts/{id}/export, POST /subscriptions, PUT /subscriptions/{id} | `contract %s is not granted to this tenant` |

### 404 Not Found

| Condition | Operations | Message |
| --- | --- | --- |
| the API key to revoke does not exist | DELETE /apikeys/{id} | `API key %d not found` |
| the admin API is called on a single-tenant instance, where the routes are deliberately inert | POST /admin/tenants, GET /admin/tenants, GET /admin/tenants/{id}, PATCH /admin/tenants/{id}, DELETE /admin/tenants/{id}, GET /admin/tenants/{id}/usage, GET /admin/tenants/{id}/grants, POST /admin/tenants/{id}/grants, DELETE /admin/tenants/{id}/grants/{contract_id}, GET /admin/tenants/{id}/keys, POST /admin/tenants/{id}/keys, DELETE /admin/keys/{key_id} | `admin API is available only when MULTI_TENANT=true` |
| the tenant self-service API is called on a single-tenant instance, where the routes are deliberately inert | GET /tenant, GET /tenant/usage, GET /tenant/watch, POST /tenant/watch, DELETE /tenant/watch/{contract_id} | `tenant API is available only when MULTI_TENANT=true` |
| the address has no rows in the activity index | GET /addresses/{address}/summary | `address %q not found` |
| the dead letter to delete does not exist (anymore) | DELETE /dead-letters/{id} | `dead letter %d not found` |
| the event exists but only its decoded form was kept, so no raw XDR can be served | GET /events/{id}/raw | `event %q has no stored raw XDR` |
| the event is not stored, or is invisible to the caller's scope (including on the conditional-GET path, so a 304 cannot become an existence oracle) | GET /events/{id}/raw | `event %q not found` |
| the anchor event is not stored, or is invisible to the caller's scope | GET /events/{id}/transaction | `event %q not found` |
| the event is not stored, or is invisible to the caller's scope (both the unconditional and conditional paths) | GET /events/{id} | `event %q not found` |
| the contract to stop watching is not on the watch list | DELETE /watched-contracts/{id} | `contract %q is not in the watch list` |
| the contract has no indexed events | GET /contracts/{id}/stats | `contract %q not found` |
| the contract has no indexed events | GET /contracts/{id} | `contract %q not found` |
| no override was ever stored for this contract | GET /contracts/{id}/spec | `no spec override for contract %q` |
| the named version is not in the contract's override history | POST /contracts/{id}/spec/revert | `no spec override version %d for contract %q` |
| the subscription does not exist or is not visible to the caller | DELETE /subscriptions/{id} | `subscription %d not found` |
| the subscription does not exist or is not visible to the caller | GET /subscriptions/{id} | `subscription %d not found` |
| the subscription does not exist or is not visible to the caller | GET /subscriptions/{id}/deliveries | `subscription %d not found` |
| the subscription does not exist or is not visible to the caller | PUT /subscriptions/{id} | `subscription %d not found` |
| the tenant to delete does not exist | DELETE /admin/tenants/{id} | `tenant %d not found` |
| the tenant key to revoke does not exist | DELETE /admin/keys/{key_id} | `api key %d not found` |
| the {id} tenant does not exist | GET /admin/tenants/{id}, PATCH /admin/tenants/{id}, GET /admin/tenants/{id}/usage, GET /admin/tenants/{id}/grants, POST /admin/tenants/{id}/grants, DELETE /admin/tenants/{id}/grants/{contract_id}, GET /admin/tenants/{id}/keys, POST /admin/tenants/{id}/keys | `tenant %d not found` |

### 409 Conflict

| Condition | Operations | Message |
| --- | --- | --- |
| a tenant with the same name exists (store.ErrDuplicate) | POST /admin/tenants | `already exists` |
| a tenant is authenticated as tries to delete its own account | DELETE /admin/tenants/{id} | `a tenant cannot delete itself` |
| the update would rename the tenant onto an existing name (store.ErrDuplicate) | PATCH /admin/tenants/{id} | `already exists` |

### 413 Request Entity Too Large

| Condition | Operations | Message |
| --- | --- | --- |
| the shared request-body decoder rejects an oversized body | any authenticated or rate-limited operation (middleware-wide) | `request body exceeds <limit> bytes` |

### 429 Too Many Requests

| Condition | Operations | Message |
| --- | --- | --- |
| the tenant's max_watched_contracts quota would be exceeded (store.ErrQuotaExceeded) | POST /tenant/watch | `quota exceeded` |
| the caller exceeded its request rate or burst (or, for tenants, its hourly/daily quota) | any authenticated or rate-limited operation (middleware-wide) | `rate limit exceeded; retry later` |

### 500 Internal Server Error

| Condition | Operations | Message |
| --- | --- | --- |
| the tenant store fails while looking up or scoping the presented key | any authenticated or rate-limited operation (middleware-wide) | `authentication failed` |
| the store fails while persisting a freshly minted key | POST /apikeys | `creating API key failed` |
| the server cannot generate the key material itself | POST /apikeys | `generating API key failed` |
| the store fails while listing API keys | GET /apikeys | `listing API keys failed` |
| the store fails while revoking an API key | DELETE /apikeys/{id} | `revoking API key failed` |
| the store fails while resolving the presented key (a backend fault, not a bad credential) | POST /apikeys, GET /apikeys, DELETE /apikeys/{id}, GET /events/ws, POST /subscriptions, PUT /subscriptions/{id}, DELETE /subscriptions/{id}, GET /subscriptions, GET /subscriptions/{id}, GET /subscriptions/{id}/deliveries | `checking API key failed` |
| the store fails before the first byte of an NDJSON export has been written (after that the stream is aborted instead) | GET /events.csv, GET /contracts/{id}/export | `export failed` |
| the store fails while adding the contract to the watch list | POST /watched-contracts | `adding watched contract failed` |
| the store fails while reading where historical replay would start | POST /watched-contracts | `loading ingestion state failed` |
| the store fails while reading the current watch list to decide the mode transition | POST /watched-contracts | `loading watched contracts failed` |
| the store fails while querying the address's events | GET /addresses/{address}/events | `querying address events failed` |
| the store fails while aggregating the address's summary | GET /addresses/{address}/summary | `loading address summary failed` |
| the store fails while aggregating events into buckets | GET /events/aggregate | `aggregating events failed` |
| the store fails while counting events matching the filter | GET /events/count | `counting events failed` |
| the store fails while deleting the dead letter | DELETE /dead-letters/{id} | `deleting dead letter failed` |
| the store fails while deleting the ledger range | DELETE /events | `deleting events failed` |
| the store fails while probing existence or loading the raw event | GET /events/{id}/raw | `loading event failed` |
| the store fails while loading the anchor event | GET /events/{id}/transaction | `loading event failed` |
| the store fails while loading the anchor transaction's sibling events | GET /events/{id}/transaction | `loading transaction events failed` |
| the store fails while probing existence or loading the event | GET /events/{id} | `loading event failed` |
| the store fails while listing contracts | GET /contracts | `listing contracts failed` |
| the store fails while listing dead letters | GET /dead-letters | `listing dead letters failed` |
| the store fails on the first NDJSON batch, before any 200 has been committed | GET /events | `querying events failed` |
| the store fails while listing the watch list | GET /watched-contracts | `loading watched contracts failed` |
| the store fails while reading the watch list to decide the mode transition | DELETE /watched-contracts/{id} | `loading watched contracts failed` |
| the store fails while removing the contract from the watch list | DELETE /watched-contracts/{id} | `removing watched contract failed` |
| the store fails while aggregating the stats counters | GET /stats | `loading stats failed` |
| the store fails while querying a page of events | GET /events, GET /contracts/{id}/events | `querying events failed` |
| the store fails while aggregating the contract's stats | GET /contracts/{id}/stats | `loading contract stats failed` |
| the store fails while loading the contract's summary | GET /contracts/{id} | `loading contract summary failed` |
| the store fails while deleting the override | DELETE /contracts/{id}/spec | `deleting contract spec override failed` |
| the store fails while loading the override | GET /contracts/{id}/spec | `loading contract spec override failed` |
| the store fails while persisting the override | PUT /contracts/{id}/spec | `saving contract spec override failed` |
| the store fails while listing the override history | GET /contracts/{id}/spec/history | `listing contract spec override history failed` |
| the store fails while listing the override history to find the version to restore | POST /contracts/{id}/spec/revert | `listing contract spec override history failed` |
| the store fails while writing the restored override | POST /contracts/{id}/spec/revert | `reverting contract spec override failed` |
| the store fails while creating the subscription | POST /subscriptions | `creating subscription failed` |
| the store fails while deleting the subscription | DELETE /subscriptions/{id} | `deleting subscription failed` |
| the store fails while loading the subscription | GET /subscriptions/{id} | `getting subscription failed` |
| the store fails while listing delivery attempts | GET /subscriptions/{id}/deliveries | `listing delivery attempts failed` |
| the store fails while listing subscriptions | GET /subscriptions | `listing subscriptions failed` |
| the store fails while loading the subscription before the merge | PUT /subscriptions/{id} | `getting subscription failed` |
| the store fails while persisting the merged subscription | PUT /subscriptions/{id} | `updating subscription failed` |
| the store fails while recording the tenant's watch | POST /tenant/watch | `adding watched contract failed` |
| the store fails while persisting the tenant key | POST /admin/tenants/{id}/keys | `creating api key failed` |
| the server cannot generate the tenant key material | POST /admin/tenants/{id}/keys | `generating api key failed` |
| the store fails while creating the tenant | POST /admin/tenants | `creating tenant failed` |
| the store fails while deleting the tenant | DELETE /admin/tenants/{id} | `deleting tenant failed` |
| the store fails while recording the grant | POST /admin/tenants/{id}/grants | `granting contract failed` |
| the store fails while listing the tenant's keys | GET /admin/tenants/{id}/keys | `listing api keys failed` |
| the store fails while listing tenants | GET /admin/tenants | `listing tenants failed` |
| the store fails while dropping the tenant's watch | DELETE /tenant/watch/{contract_id} | `removing watched contract failed` |
| the store fails while revoking the grant | DELETE /admin/tenants/{id}/grants/{contract_id} | `revoking contract failed` |
| the store fails while revoking the tenant key | DELETE /admin/keys/{key_id} | `revoking api key failed` |
| the store fails while updating the tenant | PATCH /admin/tenants/{id} | `updating tenant failed` |
| the store fails while loading the {id} tenant | GET /admin/tenants/{id}, PATCH /admin/tenants/{id}, GET /admin/tenants/{id}/usage, GET /admin/tenants/{id}/grants, POST /admin/tenants/{id}/grants, DELETE /admin/tenants/{id}/grants/{contract_id}, GET /admin/tenants/{id}/keys, POST /admin/tenants/{id}/keys | `loading tenant failed` |
| the store fails while listing a tenant's grants (on a read after the mutation, so the mutation itself may have landed) | GET /admin/tenants/{id}/grants, POST /admin/tenants/{id}/grants, DELETE /admin/tenants/{id}/grants/{contract_id} | `listing grants failed` |
| the store fails while listing the tenant's own watch list (on a read after the mutation, so the mutation itself may have landed) | GET /tenant/watch, POST /tenant/watch, DELETE /tenant/watch/{contract_id} | `listing watched contracts failed` |
| the store fails while tallying usage | GET /tenant/usage, GET /admin/tenants/{id}/usage | `listing usage failed` |
| a handler panicked; the recovery middleware answers with the generic sentence and logs the stack against X-Request-ID | any authenticated or rate-limited operation (middleware-wide) | `Internal Server Error` |

### 501 Not Implemented

| Condition | Operations | Message |
| --- | --- | --- |
| the store backend does not implement API keys (ErrUnsupported, e.g. ClickHouse) | POST /apikeys | `operation not supported by this store backend` |
| the store backend does not implement API keys (ErrUnsupported) | GET /apikeys | `operation not supported by this store backend` |
| the store backend does not implement API keys (ErrUnsupported) | DELETE /apikeys/{id} | `operation not supported by this store backend` |
| the live stream is requested on an instance with no broadcaster | GET /events/ws | `streaming not configured` |
| the store backend does not support listing contracts (ErrUnsupported) | GET /contracts | `operation not supported by this store backend` |
| the store backend cannot count a contract's events (ErrUnsupported, e.g. ClickHouse) | GET /contracts/{id}/stats | `operation not supported by this store backend` |
| the store backend does not support contract summaries (ErrUnsupported) | GET /contracts/{id} | `operation not supported by this store backend` |
| the store backend does not implement contract spec overrides (ErrUnsupported) | DELETE /contracts/{id}/spec | `operation not supported by this store backend` |
| the store backend does not implement contract spec overrides (ErrUnsupported) | PUT /contracts/{id}/spec | `operation not supported by this store backend` |
| the store backend does not implement the override history (ErrUnsupported) | GET /contracts/{id}/spec/history | `operation not supported by this store backend` |
| the store backend does not implement the override history (ErrUnsupported) | POST /contracts/{id}/spec/revert | `operation not supported by this store backend` |

### 503 Service Unavailable

| Condition | Operations | Message |
| --- | --- | --- |
| a management endpoint is called on an instance with no API_KEY — fail-closed, so writes are never open on a misconfigured deployment | DELETE /events, GET /watched-contracts, POST /watched-contracts, DELETE /watched-contracts/{id}, PUT /contracts/{id}/spec, GET /contracts/{id}/spec, DELETE /contracts/{id}/spec, GET /contracts/{id}/spec/history, POST /contracts/{id}/spec/revert, GET /dead-letters, DELETE /dead-letters/{id} | `API_KEY env var must be set to access watched-contracts endpoints` |
| /metrics is scraped on an instance with no metrics collector wired in | GET /metrics | `metrics not enabled` |

## Event-filter validation messages

List-shaped endpoints validate their query through one shared filter
builder; a malformed value is a 400 carrying exactly one of these
sentences, and a contract outside the caller's grants is the same
condition reported as 403 (see the writeForbiddenContract entry above).
The subscription create/update paths re-check their merged filters
through the same helper.

- `invalid contract_id %q`
- `invalid cursor %q`
- `invalid type %q (want contract|system|diagnostic)`
- `invalid order %q (want asc or desc)`
- `invalid order_by %q (want %s, %s or %s)`
- `invalid tx_index %d (want a non-negative integer)`
- `invalid op_index %d (want a non-negative integer)`
- `limit must be an integer in [1,%d]`
- `from_ledger %s / to_ledger %s (wrapping "must be a positive integer")`
- `from_time %s / to_time %s (wrapping "must be an RFC 3339 timestamp (e.g. 2026-07-21T00:00:00Z)" or "sub-second precision is not supported")`
- `from_ledger %d is after to_ledger %d`
- `from_time %s is after to_time %s`
- `topic and topic0..topic3 filters cannot be combined`
- `topic: %w / topic0: %w … topic3: %w (wrapping the JSON-parse failure of that filter)`
- `topic_contains must be valid JSON`
- `unknown field %q (valid: id, contract_id, ledger, type, tx_hash, op_index, in_successful_call, topics, value, created_at)`
- `unauthenticated (the subscription authorizer found no principal — POST/PUT /subscriptions)`
- `filters.contract_id is required: a subscription must name one of the contracts granted to this tenant`

## Responses outside the shared envelope

| Status | Kind | Condition | Message | Operations |
| --- | --- | --- | --- | --- |
| 404 | plain | any path the router does not register | `404 page not found` | any path or method the router does not accept |
| 405 | plain | a registered path is called with a method that path does not accept — chi answers with the status and an Allow header naming the methods that would work, and writes no body at all | `(empty body)` | any path or method the router does not accept |
| 503 | health | a dependency check behind /health fails: "degraded" when a dependency answered but reports a suboptimal state, "unready" when one did not answer at all — either way 503, with per-dependency status rather than an error envelope | `{"status":"degraded"\|"unready","checks":{…}}` | GET /health |
| 503 | health | a dependency check behind /readyz fails or ingestion lags past the readiness threshold — "degraded" or "unready" in the body, either way 503, with per-dependency status rather than an error envelope | `{"status":"degraded"\|"unready","checks":{…}}` | GET /readyz |
| 400 | plain | the request is a websocket upgrade attempt but its handshake is malformed — an unsupported protocol version, or a missing, duplicated or invalid Sec-WebSocket-Key — and the library rejects it after filter validation | `unsupported WebSocket protocol version (only 13 is supported): %q; WebSocket protocol violation: missing Sec-WebSocket-Key; WebSocket protocol violation: multiple Sec-WebSocket-Key headers; WebSocket protocol violation: invalid Sec-WebSocket-Key %q, must be a 16 byte base64 encoded string` | GET /events/ws |
| 426 | plain | the request is not a websocket upgrade attempt at all — HTTP/1.0, or the Connection and Upgrade headers are missing or do not name websocket — so the library refuses before negotiating (an HTTP/2 request, where connection headers are forbidden, lands here) | `WebSocket protocol violation: handshake request must be at least HTTP/1.1: %q; WebSocket protocol violation: Connection header %q does not contain Upgrade; WebSocket protocol violation: Upgrade header %q does not contain websocket` | GET /events/ws |
| 400 | graphql | the GraphQL transport cannot read or parse the request body | `{"errors":[{"message":…}]}` | not enumerated per operation in the spec |

Chi's router answers unknown paths and wrong methods with plain text,
not JSON — a client must branch on Content-Type only for these two. The
probe bodies are the health envelope (per-dependency status), not error
messages. GraphQL responses use the GraphQL spec's own
`{"errors": [...]}` envelope.
