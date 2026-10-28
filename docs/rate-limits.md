# Rate limits and quotas

SoroTrail applies three request-limiting mechanisms to the HTTP API:

1. a per-client **token bucket** (`RATE_LIMIT_RPS` / `RATE_LIMIT_BURST`);
2. optional **hourly and daily quotas** (`HOURLY_QUOTA` / `DAILY_QUOTA`);
3. optional **per-tenant overrides** when `MULTI_TENANT=true`.

They are separate gates and are checked in that order. A request must pass the
token bucket before its quota is examined. The limits live in process memory:
they are not stored in Postgres and are not shared between replicas.

## At a glance

| Mechanism | Configuration | Scope | Window | Failure |
| --- | --- | --- | --- | --- |
| Token bucket | `RATE_LIMIT_RPS`, `RATE_LIMIT_BURST`; tenant `rate_limit_rps`, `rate_limit_burst` | Client key | Continuous refill | `429` + `Retry-After` |
| Hourly quota | `HOURLY_QUOTA` | Client key | Fixed 1 hour from the first request in the window | `429` + `Retry-After` |
| Daily quota | `DAILY_QUOTA` | Client key | Fixed 24 hours from the first request in the window | `429` + `Retry-After` |
| Per-tenant limits | `MULTI_TENANT=true` plus tenant overrides | Tenant | Same as the underlying mechanism | `429`; a tenant configured with `0`/`0` is denied |

`0` disables a quota. `RATE_LIMIT_RPS` and `RATE_LIMIT_BURST` must be set
together; setting only one fails startup. Negative values are rejected.

## How a client is identified

The limiter first asks the multi-tenant resolver for a key. When
`MULTI_TENANT=true` and the request carries a valid tenant credential, the key
is `tenant:<tenant-id>`. That makes the tenant's budget follow its identity
rather than its network address.

If there is no authenticated tenant, or multi-tenancy is disabled, the key is
derived from the request:

1. an API key presented on the request, if any (`api:<key>`);
2. otherwise the client IP.

The client IP is `r.RemoteAddr` by default. When
`RATE_LIMIT_TRUSTED_PROXY=true`, the leftmost parseable address in
`X-Forwarded-For` is used instead, falling back to `RemoteAddr` if the header is
missing or malformed. Only enable that flag behind a proxy that strips or
overwrites the header. On an Internet-facing service, accepting
`X-Forwarded-For` from clients lets a caller pick its own bucket.

In multi-tenant mode an unauthenticated request, or one made with an untenanted
key, falls back to the API-key/IP rules. The tenant resolver only wins when the
request actually has a tenant principal.

Keys are opaque implementation details. Do not rely on their format in client
code or dashboards.

## Order of checks

For every request that is not exempt:

1. Resolve the bucket key and limits.
2. Reserve one token from the token bucket. If the token is not available now,
   return `429` and do not examine the quotas.
3. If quotas are enabled, check the hourly window, then the daily window. A
   rejected request does not increment either counter, but the token reserved in
   step 2 is consumed.
4. Otherwise increment both counters, set the rate-limit headers, and call the
   handler.

An exempt path is never limited. The exempt paths are `/health`, `/livez`,
`/readyz` and `/metrics`. WebSocket upgrades on `/events/ws` bypass the
middleware entirely, as described in the main README.

## Token bucket

The bucket starts full with `RATE_LIMIT_BURST` tokens and refills continuously
at `RATE_LIMIT_RPS` tokens per second (fractional rates are allowed). A request
costs one token. Once the bucket is empty, the request is refused until enough
time has passed for one token to accrue.

For example, with `RATE_LIMIT_RPS=5` and `RATE_LIMIT_BURST=10`:

- the first 10 requests in a burst are admitted immediately;
- after that, one request is admitted every 200 ms on average;
- a client that stays idle refills up to 10 tokens, not more.

A refusal carries `Retry-After` rounded up to whole seconds, with a minimum of
one second. The value is the time until one token is available.

When a tenant sets `rate_limit_rps` and `rate_limit_burst`, those values replace
the instance defaults for that tenant. The two tenant fields must be set
together. A tenant explicitly configured with `0` and `0` is denied all
requests; it is not treated as "unset". A tenant with neither field uses the
instance-wide defaults, and if those are unset the tenant falls back to the
IP/API-key bucket.

## Hourly and daily quotas

Quotas are fixed windows, not rolling windows. The first request that creates a
bucket starts the hour and day windows. The counters reset when the window has
elapsed:

- hourly: one hour after `hourStart`;
- daily: 24 hours after `dayStart`.

A request at or after the boundary starts a new window and is counted against
the new counters. The implementation keeps one hourly and one daily counter per
bucket and checks the hourly quota before the daily one.

The quota is enforced after the token bucket. A request that is refused by the
token bucket does not consume quota. A request that passes the token bucket but
is refused by a quota has already consumed its token.

Quota refusals use `Retry-After` set to the remaining time in the window that
rejected the request, rounded up to whole seconds. While a quota is enabled,
`X-RateLimit-Remaining` reports the smaller of the hourly and daily remaining
counts, so a client can see which budget is tighter.

Two consequences are worth planning for:

- **Idle eviction resets quota counters.** An untouched bucket is evicted after
  the idle TTL (five minutes by default). Because the quota counters live in
  that bucket, a client that goes idle longer than the TTL gets a fresh quota
  window. This is tracked in [#1141](https://github.com/sorotrail/SoroTrail/issues/1141).
- **Fixed windows allow a boundary burst.** A client can spend its full hourly
  budget at the end of one window and its full budget again at the start of the
  next. The token bucket still smooths the request rate, but it does not spread
  the quota evenly across the window.

Neither quota is persisted. Restarting the process resets both counters for
every client.

## Per-tenant limits

`MULTI_TENANT=true` wires the tenant resolver in front of the limiter. The
resolver:

- keys authenticated tenant requests as `tenant:<id>`;
- uses the tenant's `rate_limit_rps` and `rate_limit_burst` when both are set;
- otherwise uses the instance-wide `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST`;
- falls back to the IP/API-key path for unauthenticated or untenanted requests.

The quota environment variables are instance-wide settings, but because the
bucket key is the tenant in multi-tenant mode, the hourly and daily counters are
effectively per tenant for authenticated traffic. Unauthenticated traffic shares
the fallback IP/API-key buckets.

Tenant overrides are managed through the admin tenant endpoints; see
[Multi-tenancy](multi-tenancy.md) for the tenant model and
`rate_limit_rps` / `rate_limit_burst` fields. The per-tenant
`max_watched_contracts` cap is a separate watch-list limit and does not affect
HTTP request admission.

## Response headers

On an admitted request the limiter writes:

| Header | Meaning |
| --- | --- |
| `X-RateLimit-Limit` | The burst size of the bucket, when one is configured. This is not the hourly or daily quota. |
| `X-RateLimit-Remaining` | Tokens left in the bucket, or the smaller remaining hourly/daily quota when quotas are enabled. Never negative. |
| `X-RateLimit-Reset` | Unix seconds at which the bucket or quota becomes usable again. On an admitted quota request this is the current time; on a refusal it is the end of the rejecting window. |
| `Retry-After` | On a `429` only: whole seconds until a retry can succeed, minimum 1. |

A refusal looks like this:

```http
HTTP/1.1 429 Too Many Requests
Content-Type: application/json
Retry-After: 2
X-RateLimit-Limit: 10
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1760000002

{"error":"rate limit exceeded; retry later"}
```

The body is the standard error envelope, so clients can parse the same `error`
field they use elsewhere. `X-Request-ID` is still present on the response and is
the best value to quote in a support request.

`CORS_EXPOSED_HEADERS` includes `X-RateLimit-Limit`,
`X-RateLimit-Remaining` and `X-RateLimit-Reset` by default, so browser clients
on an allowed origin can read them. Add `Retry-After` to that list if the browser
needs to read it directly.

## Per-process limits and multiple replicas

Each SoroTrail process owns its own bucket map. With N replicas behind a load
balancer, a client can get up to N times the configured rate, and a quota can be
spent N times over, because each replica counts independently. A client can also
land on different replicas from one request to the next.

For a strict global limit, enforce it at the gateway or use a shared limiter
outside SoroTrail. Sticky sessions reduce the skew but do not make the counters
global.

The bucket map is bounded by evicting entries that have been idle longer than
the idle TTL (five minutes by default; the sweep runs once a minute). Eviction
is what makes long-running processes safe against address churn, but it is also
the source of the quota-reset behavior described above.

## Tuning examples

### Public read API

```sh
RATE_LIMIT_RPS=20
RATE_LIMIT_BURST=40
HOURLY_QUOTA=100000
DAILY_QUOTA=1000000
```

This admits short bursts of 40 requests, sustains 20 requests/second per client,
and caps a client at 100,000 requests per hour or 1,000,000 per day. The hourly
cap binds first for a client that runs continuously at the sustained rate.

### Multi-tenant deployment

```sh
MULTI_TENANT=true
RATE_LIMIT_RPS=10
RATE_LIMIT_BURST=20
HOURLY_QUOTA=50000
DAILY_QUOTA=500000
```

Every authenticated tenant gets its own `10/20` bucket and its own quota
counters. A tenant that needs more capacity can be given
`rate_limit_rps=50`, `rate_limit_burst=100`; a tenant that should be suspended
can be given `0`/`0`. The quotas remain the instance-wide hourly and daily
values, applied per tenant.

### Behind a proxy

```sh
RATE_LIMIT_RPS=50
RATE_LIMIT_BURST=100
RATE_LIMIT_TRUSTED_PROXY=true
```

Only do this when the proxy overwrites `X-Forwarded-For` with the address it
observed. If clients can set the header themselves, they can choose their own
bucket and bypass per-IP throttling.

### Retry behavior

A `429` is not a signal to retry immediately. Honor `Retry-After`, add jitter,
and back off exponentially if the retry is also refused. A client that ignores
`Retry-After` spends its tokens on refusals and can keep its quota window
saturated.

## Operator checklist

- Set both `RATE_LIMIT_RPS` and `RATE_LIMIT_BURST`, or neither.
- Use `HOURLY_QUOTA` / `DAILY_QUOTA` for long-horizon budgets; use the token
  bucket for request-rate smoothing.
- In multi-tenant mode, decide whether the instance defaults or tenant
  overrides should apply before creating tenants.
- Watch the `429` rate and the `X-RateLimit-*` values when tuning; the metrics
  and request logs are the source of truth.
- Remember that limits are per process. Multiply by the replica count when
  sizing a deployment, or move the limit to the gateway.
- Plan for the idle-eviction quota reset until #1141 is resolved.

## Request and response size limits

In addition to request-rate and quota enforcement, SoroTrail bounds request and
response payload sizes so oversized inputs, compression bombs, and unbounded
JSON responses fail predictably with `413 Request Entity Too Large`:

| Bound | Configuration / Header | Default | Scope | Failure |
| --- | --- | --- | --- | --- |
| Request wire size | `HTTP_REQUEST_BODY_LIMIT` | `1048576` (1 MiB) | Every request body (`PUT /contracts/{id}/spec` defaults to 2 MiB when the global default is unchanged) | `413` + `reason="request_body"` |
| Decompressed request size | `HTTP_REQUEST_BODY_LIMIT` (`Content-Encoding: gzip` or `deflate`) | Same as endpoint request limit | Incremental decompression stream | `413` + `reason="request_decompressed"` |
| Non-streaming JSON response size | `HTTP_RESPONSE_BODY_LIMIT` and optional `X-Max-Response-Bytes` request header | `16777216` (16 MiB) | Non-streaming JSON responses (`writeJSON`); streaming endpoints (`/events.csv`, `/contracts/{id}/export`, `/events?stream=true`, `/events/ws`) flush incrementally and are exempt | `413` + `reason="response_body"` |

Every `413` rejection increments `sorotrail_http_limit_rejections_total{reason="request_body|request_decompressed|response_body"}`.

## Source of truth

The behavior above is implemented in:

- `internal/api/ratelimit.go` - token bucket, quotas, key derivation and
  response headers;
- `internal/api/limits.go` - request/response size bounds, decompression bomb
  protection, and per-endpoint overrides;
- `internal/api/auth.go` - `TenantLimitResolver`;
- `cmd/sorotrail/main.go` - wiring of the limiter and tenant resolver;
- `internal/config/config.go` and `internal/config/validation.go` - startup
  validation of the configuration values.
