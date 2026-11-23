# API Key Lifecycle and Scopes

SoroTrail protects its HTTP API with API keys. This document explains how keys
are generated, stored, and verified, how the initial administrative key is
bootstrapped, the role and enforcement of scopes, how to execute zero-downtime key
rotations, and the backend deployment constraints.

- [Key format and storage](#key-format-and-storage)
- [Where the first key comes from (bootstrap)](#where-the-first-key-comes-from-bootstrap)
- [Key management CLI](#key-management-cli)
- [Scopes and permissions](#scopes-and-permissions)
- [Under-scoped and unauthenticated responses](#under-scoped-and-unauthenticated-responses)
- [Key rotation procedure](#key-rotation-procedure)
- [Leaked key procedure](#leaked-key-procedure)
- [Backend constraint: PostgreSQL only](#backend-constraint-postgresql-only)
- [Idempotency keys on write endpoints](#idempotency-keys-on-write-endpoints)
- [Secret handling guidance](#secret-handling-guidance)

## Key format and storage

A SoroTrail API key is a high-entropy bearer token with the structure:

```
sorotrail_<prefix>_<secret>
```

For bootstrap keys, the prefix is `st_<prefix>_<secret>`.

| Component | Length | Storage | Purpose |
| --- | --- | --- | --- |
| `Scheme` | 10 chars (`sorotrail_`) | None | Identifies the token type in logs and headers. |
| `Prefix` | 16 hex chars (8 random bytes) | Plaintext (indexed) | Locates the database row in a single query. |
| `Secret` | 64 hex chars (32 random bytes) | bcrypt hash (cost 10) | Authenticates the presenter. |

### Plaintext is shown once and is unrecoverable

When a key is created:
1. The plaintext token is printed to stdout or returned in the creation response.
2. The database stores only the prefix and the bcrypt digest of the secret.
3. The plaintext secret is dropped from memory immediately.

> [!IMPORTANT]
> The full plaintext key cannot be retrieved from the database or CLI after
> creation. If an operator loses a key, it cannot be recovered — it must be
> revoked and replaced.

Listing commands and API responses deliberately return only key IDs, names,
prefixes, and creation timestamps.

## Where the first key comes from (bootstrap)

When SoroTrail is deployed with authentication enabled, incoming requests demand
a key. But creating keys through the HTTP API itself requires administrative
permissions. SoroTrail provides two bootstrap paths:

### 1. Direct database CLI bootstrap

The operator runs the CLI directly against the database host (using `DATABASE_URL`):

```sh
sorotrail apikey create --name "initial-operator"
```

The CLI bypasses the HTTP API, connects directly to PostgreSQL, generates the key,
hashes the secret, inserts the record, and prints the full key to stdout.

### 2. Startup environment bootstrap (`MULTI_TENANT_BOOTSTRAP_KEY`)

In multi-tenant deployments (`MULTI_TENANT=true`), an operator can supply a
bootstrap credential via the environment variable `MULTI_TENANT_BOOTSTRAP_KEY`.

At process startup, `cmd/sorotrail/main.go` invokes `bootstrapAdminKey`:
- It parses the key format (`st_<prefix>_<secret>`).
- It looks up the seeded `"default"` admin tenant.
- It calls `CreateTenantAPIKeyIfAbsent`.

#### Startup and restart behavior

- **Idempotent**: If a key with that prefix already exists in the database,
  `CreateTenantAPIKeyIfAbsent` is a no-op. Restarting or redeploying the container
  **never mints a second key** or overwrites existing keys.
- **Log output**: The server logs a `WARN` message:
  ```
  WARN bootstrap admin key installed from MULTI_TENANT_BOOTSTRAP_KEY; mint per-tenant keys via POST /admin/tenants/{id}/keys and revoke this one tenant=default prefix=...
  ```
  The log output includes the key prefix and tenant name, but **never prints the secret**.
- **Ephemeral intent**: The bootstrap key is intended to stand up the service,
  mint the first real tenant keys via the admin API, and then be revoked and
  removed from configuration.

## Key management CLI

The `sorotrail apikey` subcommand provides operational control without touching
the HTTP server:

### Issue a new key

```sh
sorotrail apikey create --name "analytics-prod"
```

Output:
```
created API key 4 (analytics-prod)
prefix: 7f3b891a4c02e19d
key:    sorotrail_7f3b891a4c02e19d_9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b
store this key now — it will not be shown again.
```

### List keys

```sh
sorotrail apikey list
```

Output:
```
ID  NAME            PREFIX            CREATED              STATUS
1   initial-admin   1a2b3c4d5e6f7a8b  2026-07-20 10:15:00  active
2   metrics-agent   9f8e7d6c5b4a3a2b  2026-07-21 14:30:22  active
3   stale-worker    4c5d6e7f8a9b0c1d  2026-07-22 09:00:11  revoked
```

### Revoke a key

```sh
sorotrail apikey revoke 3
```

Revocation takes effect **immediately** on the very next request.

## Scopes and permissions

Authentication establishes the caller's identity (the principal), while scopes
determine authorization. In SoroTrail, scopes and multi-tenancy interact directly:

| Scope | Granted by | Permissions |
| --- | --- | --- |
| **Admin** | Tenant `admin: true` flag | Access to `/admin/*` endpoints (create/disable tenants, manage grants, mint/revoke tenant keys). |
| **Wildcard** | Tenant `wildcard: true` flag | Unrestricted read access to events, counts, and summaries across **all** contracts on the network. |
| **Granted (Contract-scoped)** | Per-tenant contract grants in `tenant_grants` | Read access restricted strictly to events from contracts explicitly granted to the tenant. |
| **Watch list** | Tenant watch entries (`/tenant/watch`) | Controls which contracts the background ingester polls from Stellar RPC. Independent of read grants. |

Every event read path in the server routes through `scopeFrom(ctx)`. The returned
`store.Scope` acts as a mandatory filter in the database query.

## Under-scoped and unauthenticated responses

When a request does not satisfy authentication or authorization requirements:

| Condition | Status code | Error message / Behavior |
| --- | --- | --- |
| Missing credential | `401 Unauthorized` | `{"error": "missing API key"}` |
| Malformed key format | `401 Unauthorized` | `{"error": "malformed API key"}` |
| Unknown or revoked key | `401 Unauthorized` | `{"error": "unknown or revoked API key"}` |
| Disabled tenant account | `403 Forbidden` | `{"error": "tenant is disabled"}` |
| Non-admin calling `/admin/*` | `403 Forbidden` | `{"error": "admin access required"}` |
| Single-tenant unconfigured key | `503 Service Unavailable` | `{"error": "API_KEY env var must be set..."}` |
| Ungranted contract read | Filtered or `403` | `GET /events` filters out ungranted events. `GET /contracts/{id}/events` on ungranted contract returns `403 Forbidden`. |

For further details on how scopes interact with tenants and caching, see
[Multi-tenancy](multi-tenancy.md).

## Key rotation procedure

To rotate a credential in production without downtime, follow a
**create-then-revoke** workflow with an overlap:

1. **Mint the replacement key**:
   ```sh
   sorotrail apikey create --name "indexer-worker-v2"
   ```
2. **Deploy the new key to the client**:
   Update your client service's configuration or secret store with the newly
   generated key. Deploy the change.
3. **Verify traffic on the new key**:
   Inspect service logs and SoroTrail metrics to confirm requests are
   authenticating cleanly under the new key prefix.
4. **Revoke the deprecated key**:
   ```sh
   sorotrail apikey revoke <old-key-id>
   ```

Because key lookups consult the store on each request, revocation is
**immediately consistent** — the old key stops working on its next request with
zero propagation delay.

## Leaked key procedure

If an API key is committed to source control, leaked in logs, or otherwise compromised:

1. **Identify the compromised key ID**:
   Match the key's prefix (the first 16 characters after `sorotrail_`) using:
   ```sh
   sorotrail apikey list
   ```
2. **Revoke immediately**:
   ```sh
   sorotrail apikey revoke <leaked-key-id>
   ```
3. **Verify invalidation**:
   Test a request with the leaked key to confirm it receives `401 Unauthorized`:
   ```sh
   curl -i -H "Authorization: Bearer <leaked-key>" http://localhost:8080/events
   ```
4. **Issue a fresh key**:
   Issue a replacement key and update legitimate consumers.
5. **Remediate environment variables**:
   If a bootstrap key was exposed, remove `MULTI_TENANT_BOOTSTRAP_KEY` from the
   environment configuration and restart the container.

## Backend constraint: PostgreSQL only

> [!WARNING]
> **API keys are not supported by the SQLite backend.**
> 
> The SQLite storage driver deliberately refuses all API key methods:
> `CreateAPIKey`, `GetAPIKey`, `LookupAPIKeyByPrefix`, `ListAPIKeys`, and
> `RevokeAPIKey` all return an unsupported backend error.

The SQLite backend is intended for single-node embedded development. If you are
running SoroTrail on SQLite:
- Single-node authentication must be handled externally at your reverse proxy
  (e.g., Caddy, NGINX, Envoy, or an API Gateway).
- Any production deployment requiring multi-tenancy, per-tenant scopes, or
  built-in API key management **must use PostgreSQL**.

## Idempotency keys on write endpoints

All SoroTrail write endpoints (`POST`, `PUT`, `PATCH`, and `DELETE`), including
`POST /apikeys`, `POST /admin/tenants/{id}/keys`, `POST /subscriptions`, and
`POST /watched-contracts`, accept an optional `Idempotency-Key` request header
(printable ASCII, up to 255 characters).

- **Replay on retry (`2xx` / `4xx`)**: When a write request succeeds or fails
  with a deterministic `4xx` client error, SoroTrail stores the status code,
  `Content-Type`, `Location`, and response body keyed by
  `(scope_key, idempotency_key)` together with a SHA-256 hash of the HTTP
  method, request path, and request body. Retrying the exact same request with
  the same `Idempotency-Key` within the retention window returns the stored
  response verbatim (including one-time plaintext API key secrets or generated
  subscription IDs) without re-executing the handler, and sets
  `Idempotency-Replayed: true`.
- **Conflict on payload mismatch (`409 Conflict`)**: Reusing the same
  `Idempotency-Key` with a different method, path, or request body within the
  retention window returns `409 Conflict`.
- **Concurrent in-flight requests**: Concurrent requests carrying the same
  `(scope_key, idempotency_key)` coalesce onto a single execution so at most one
  underlying write runs.
- **Transient server failures (`5xx`)**: `5xx` responses are not cached; the
  in-progress reservation is released so the client can safely retry with the
  same `Idempotency-Key`.
- **Retention window (24 hours)**: Stored idempotency records expire after 24
  hours (`DefaultIdempotencyTTL = 24h`) and expired rows are pruned
  opportunistically on write operations. Twenty-four hours covers automated
  client retry budgets, webhook orchestrator backoffs, and CI/CD deployment
  pipelines while bounding storage growth in `idempotency_keys`.

## Secret handling guidance

- **Secret managers**: Store plaintext keys in dedicated secret stores (AWS
  Secrets Manager, HashiCorp Vault, Kubernetes Secrets) rather than hardcoding
  them in deployment files or repository commits.
- **Shell history**: Avoid passing keys as CLI command arguments where shell
  history may record them.
- **Logs**: Never log full API keys. If your application logs requests, mask
  the secret portion and log only the 16-character prefix.
