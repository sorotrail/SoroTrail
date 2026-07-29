# Multi-tenancy

SoroTrail can run as shared infrastructure: several consumers on one
deployment, each seeing only the contracts they have been granted, each with
its own quotas and usage accounting.

It is **off by default**. Without `MULTI_TENANT=true` there is no
authentication, no tenant boundary and no accounting, and the instance
behaves exactly as it did before this existed. Everything below applies only
when it is switched on.

- [The model](#the-model)
- [How the boundary is enforced](#how-the-boundary-is-enforced)
- [Getting started](#getting-started)
- [Admin API](#admin-api)
- [Tenant self-service API](#tenant-self-service-api)
- [Watch lists and ingestion](#watch-lists-and-ingestion)
- [Quotas and usage](#quotas-and-usage)
- [Streams](#streams)
- [Webhook subscriptions](#webhook-subscriptions)
- [Caching](#caching)
- [Configuration](#configuration)
- [Upgrading an existing deployment](#upgrading-an-existing-deployment)
- [Adding an endpoint](#adding-an-endpoint)

## The model

| Concept | Meaning |
| --- | --- |
| **Tenant** | One consumer of the deployment. Owns keys, grants, a watch list and quotas. |
| **API key** | A credential belonging to exactly one tenant. Only its SHA-256 digest is stored. |
| **Grant** | A contract ID a tenant may *read*. |
| **Watch-list entry** | A contract a tenant has asked to have *ingested*. |
| **Wildcard** | A tenant that reads every contract, granted or not. |
| **Admin** | A tenant that may call `/admin/*`. |

Two distinctions do most of the work, and both are deliberate:

**Reading is not watching.** A grant lets a tenant read a contract's events.
A watch-list entry asks the ingester to fetch that contract. They are
separate because auto-granting whatever a tenant watches would let any
tenant give itself access to any contract on the network — the boundary
undone by a convenience. A tenant may watch a contract it cannot read
(events are ingested, it sees none of them) and read one it does not watch
(someone else, or the operator, is paying for the ingestion).

**Wildcard is not admin.** Read breadth and management rights are different
powers. An auditing tenant can be given wildcard reads without the ability
to mint keys or delete other tenants.

## How the boundary is enforced

Enforcement lives in the store's query construction, not in per-handler
checks. `store.EventFilter` carries a `Scope`, and `QueryEvents`,
`GetEvent`, `EventExists` and `Stats` all AND it into their SQL. An endpoint
cannot forget to apply it, because it never applies it in the first place —
the store does.

The important property is which way it fails.

```go
// store.Scope — fields unexported on purpose
type Scope struct {
    wildcard  bool
    contracts []string
}
```

The obvious design is a plain `AllowedContracts []string`. That **fails
open**: its zero value is an empty slice, an empty slice reads naturally as
"no constraint", and a handler that forgets to populate it serves every
tenant's data to whoever asked — silently, while looking like a working
endpoint.

`Scope` inverts that. The zero value grants nothing, and because the fields
are unexported no code outside `internal/store` can construct a permissive
one by struct literal. The only ways to get read access are
`store.WildcardScope()` and `store.SystemScope()`, both of which are
greppable — **that grep is the complete audit surface for "where is the
boundary not applied"**. A read path that forgets its scope returns an empty
page, in every deployment, which is loud and caught by the first test that
looks at it.

The scope is resolved once per request, at authentication, and reaches the
store through exactly one function:

```
request → authenticate (resolve tenant → Scope) → Principal in context
        → filterFromQuery  (list reads)   ─┐
        → scopeFrom        (single reads) ─┴→ store → SQL: contract_id = ANY($n)
```

Defense in depth, in three layers, so removing any one of them narrows the
error message rather than opening a leak:

1. A request naming an ungranted contract is refused before the query runs.
2. The store ANDs the scope in regardless of what the filter asked for.
3. An empty scope short-circuits before any SQL is issued at all.

### Status codes

| Case | Status | Why |
| --- | --- | --- |
| `/contracts/{id}/events`, `?contract_id=` — ungranted | `403` | The caller already has the contract ID and contract IDs are public on-chain. Confirming "exists, not yours" discloses nothing, and `404` would leave operators debugging a missing grant as missing data. |
| `/events/{id}` — event of an ungranted contract | `404` | Event IDs are TOIDs: dense and guessable. A distinguishable `403` would be an enumeration oracle for other tenants' events. |
| Missing / bad / revoked key | `401` | With `WWW-Authenticate: Bearer`. |
| Disabled tenant | `403` | The credential is genuine; the account is suspended. |
| Over quota | `429` | With `Retry-After`. |

## Getting started

```bash
# 1. Mint a bootstrap key. Any st_<16 base32 chars>_<secret> value works;
#    generate one however you like.
export MULTI_TENANT=true
export MULTI_TENANT_BOOTSTRAP_KEY="st_$(head -c10 /dev/urandom | base32 | tr -d '=')_$(head -c32 /dev/urandom | base32 | tr -d '=')"

# 2. Start. The bootstrap key is installed for the seeded "default" admin
#    tenant, which is what breaks the chicken-and-egg of needing an admin
#    key to mint your first key.
./sorotrail

# 3. Create a tenant.
curl -sX POST localhost:8080/admin/tenants \
  -H "Authorization: Bearer $MULTI_TENANT_BOOTSTRAP_KEY" \
  -d '{"name":"acme","max_watched_contracts":10}'

# 4. Grant it a contract.
curl -sX POST localhost:8080/admin/tenants/2/grants \
  -H "Authorization: Bearer $MULTI_TENANT_BOOTSTRAP_KEY" \
  -d '{"contract_id":"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}'

# 5. Mint its key. The plaintext appears in this response and nowhere else.
curl -sX POST localhost:8080/admin/tenants/2/keys \
  -H "Authorization: Bearer $MULTI_TENANT_BOOTSTRAP_KEY" \
  -d '{"name":"production"}'

# 6. Revoke the bootstrap key once real keys exist.
curl -sX DELETE localhost:8080/admin/keys/1 \
  -H "Authorization: Bearer $MULTI_TENANT_BOOTSTRAP_KEY"
```

Credentials go in `Authorization: Bearer <key>` or `X-API-Key: <key>`.
`/health` stays public so orchestrator probes keep working.

## Admin API

Requires a tenant with `admin: true`.

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/admin/tenants` | Create a tenant |
| `GET` | `/admin/tenants` | List tenants |
| `GET` | `/admin/tenants/{id}` | Read one tenant |
| `PATCH` | `/admin/tenants/{id}` | Update quotas, flags, name |
| `DELETE` | `/admin/tenants/{id}` | Delete a tenant and its keys, grants, watch list and usage |
| `GET` | `/admin/tenants/{id}/grants` | List grants |
| `POST` | `/admin/tenants/{id}/grants` | Grant a contract |
| `DELETE` | `/admin/tenants/{id}/grants/{contract_id}` | Revoke a contract |
| `GET` | `/admin/tenants/{id}/keys` | List keys (never secrets) |
| `POST` | `/admin/tenants/{id}/keys` | Mint a key — **the only time the secret is returned** |
| `DELETE` | `/admin/keys/{key_id}` | Revoke a key |
| `GET` | `/admin/tenants/{id}/usage?days=N` | Read usage |

`PATCH` merges: fields you omit keep their stored value. `rate_limit_rps` and
`rate_limit_burst` must be set together — `null` inherits the instance
default, `0` is an explicit deny.

Deleting a tenant never deletes ingested events; they may be shared with
other tenants.

## Tenant self-service API

Available to any authenticated tenant, and always scoped to *its own*
identity taken from the credential — there is no path parameter to tamper
with.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/tenant` | Identity, grants, quotas |
| `GET` | `/tenant/usage?days=N` | Own usage (flushes pending counters first) |
| `GET` | `/tenant/watch` | Own watch list |
| `POST` | `/tenant/watch` | Ask for a contract to be indexed |
| `DELETE` | `/tenant/watch/{contract_id}` | Drop the request |

## Watch lists and ingestion

Ingestion follows the **union** of the operator's `WATCHED_CONTRACTS` list
and every tenant's watch list:

```sql
SELECT contract_id FROM watched_contracts
UNION
SELECT contract_id FROM tenant_watched_contracts
```

Two tenants watching the same contract share one set of ingested rows, and
both read them. Removal needs no refcount: the union is recomputed on read,
so a contract stays watched exactly as long as at least one row anywhere
names it, and one tenant dropping its claim cannot stop ingestion for
another that still holds one. Deleting a tenant cascades its claims away
with the same effect.

`MULTI_TENANT_MAX_WATCHED` caps the size of the union, not any single list,
because what costs the operator RPC budget is the number of distinct
contracts polled. A contract another tenant already watches is therefore
free to add.

## Quotas and usage

Rate limits key on the **tenant**, not the source IP. IP keying is wrong on
shared infrastructure in both directions: one tenant behind a serverless
platform presents many addresses and gets many times its quota, while
several tenants behind one corporate egress share a bucket and throttle each
other.

Resolution order per request: the tenant's `rate_limit_rps`/`burst` if set,
otherwise `RATE_LIMIT_RPS`/`RATE_LIMIT_BURST`, otherwise no limit. Lowering
a tenant's quota takes effect on its next request, not when its bucket next
ages out.

Usage is tracked per tenant per UTC day: `requests`, `events_served`,
`stream_seconds`. Counters accumulate in memory and flush on
`MULTI_TENANT_USAGE_FLUSH`, so a busy tenant costs one `UPSERT` per flush
rather than one write per request. The store applies increments with `+=`,
so several API servers behind a load balancer can flush independently. An
ungraceful termination loses at most one interval; shutdown flushes
explicitly.

## Streams

`GET /events/ws` is scoped at subscribe time **and** on every dispatch, and
the filter cannot widen it — authorization is evaluated before, and
independently of, the user's filter.

Grants change while a stream is open, so the subscription re-resolves them
every `MULTI_TENANT_STREAM_SCOPE_SYNC` (default 30s):

| Event | Effect |
| --- | --- |
| Contract granted mid-stream | Starts flowing within one interval, no reconnect |
| Contract revoked mid-stream | Stops within one interval |
| Tenant disabled or deleted mid-stream | Stream goes silent immediately on the next sync |
| Transient database error during sync | Previous scope retained — it was correct as of the last successful resolve |

A snapshot taken at connect time would make revocation advisory: a revoked
tenant would keep reading for as long as it held the connection open, which
on a live feed is indefinitely.

## Webhook subscriptions

A subscription is a read path whose sink is a URL the subscriber chooses,
which makes an unowned one worse than a read leak — it is an exfiltration
primitive. Two rules apply under multi-tenancy.

**Ownership.** A subscription belongs to the tenant whose credential created
it, taken from the key and never from the request body. `GET`, `PUT`,
`DELETE` and the delivery history are all filtered by owner in the query, so
a tenant cannot enumerate, read, modify or delete another's callbacks — nor
their signing secrets. Admin tenants see all of them; single-tenant
deployments keep the previous behavior, with subscriptions owned by nobody.

**A tenant may only subscribe to what it may read.** A non-wildcard tenant
must set `filters.contract_id` to one of its granted contracts:

| Caller | `filters.contract_id` | Result |
| --- | --- | --- |
| Granted tenant | a granted contract | `201` |
| Granted tenant | an ungranted contract | `403` |
| Granted tenant | omitted | `400` — it would match every ingested event |
| Wildcard tenant | anything, or omitted | `201` |

Updates are re-checked against the same rule after merging, so `PUT` cannot
widen a subscription past what its owner could have created.

This is what lets the delivery worker run unscoped: because a subscription
can only ever match events its owner could have fetched from `/events`
anyway, delivery needs no per-event authorization on the hot path.

Note that `enabled` and `failure_count` bookkeeping (auto-disable after
repeated delivery failures) is unowned machinery and unchanged.

## Caching

Two tenants issuing byte-identical requests get different bodies, so a
response is no longer a function of its URL. Three things change together,
and all three are needed:

- `Cache-Control: private` — forced regardless of `CACHE_PRIVATE`, so no CDN
  pools a tenant-scoped body for the next caller.
- `Vary: Accept-Encoding, Authorization, X-API-Key` — a cache that does
  store the response keys it per credential.
- The `ETag` incorporates a digest of the scope, so a conditional request
  carrying another tenant's validator cannot be answered `304`.

The third is the one that does not involve a CDN at all: without it, this
server would hand out a `304` for a page the caller has never been entitled
to see.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `MULTI_TENANT` | `false` | Enable tenant isolation. Off means no auth and no boundary. |
| `MULTI_TENANT_MAX_WATCHED` | `250` | Cap on the union of all watch lists. `0` disables the cap. |
| `MULTI_TENANT_USAGE_FLUSH` | `10s` | Usage flush interval. |
| `MULTI_TENANT_STREAM_SCOPE_SYNC` | `30s` | How often open streams re-resolve grants. |
| `MULTI_TENANT_BOOTSTRAP_KEY` | unset | Installs an admin key for the `default` tenant at startup. Rejected unless `MULTI_TENANT=true`. |

## Upgrading an existing deployment

Migration `0008` adds the tenancy tables and seeds a `default` tenant that is
wildcard and admin. Nothing else changes: with `MULTI_TENANT` unset, every
request runs as an untenanted wildcard principal and the API is
byte-for-byte what it was.

When you flip `MULTI_TENANT=true`:

- **Every endpoint except `/health` starts requiring a key.** Existing
  clients get `401` until issued one. Plan the cutover.
- Set `MULTI_TENANT_BOOTSTRAP_KEY` for the first boot, or you will have no
  way to mint the first key.
- Responses become `private`; if you front SoroTrail with a CDN, expect the
  hit rate to drop. That is correct — those responses were never safely
  shareable.
- Consider narrowing the `default` tenant to non-wildcard once real tenants
  exist, so a leaked legacy key is not a full-store credential.

## Adding an endpoint

If your endpoint returns event data:

1. Build its filter with `filterFromQuery(r)` (list reads) or take
   `scopeFrom(r.Context())` (single-object reads). Do not construct an
   `EventFilter` by hand.
2. Pass the scope to the store. Never filter in the handler after an
   unscoped query — that works until someone adds pagination.
3. If the endpoint names a contract, return `403` for one outside the scope
   rather than silently returning nothing.
4. If it registers a delivery target (a webhook, an export sink), enforce
   both halves of the subscription rule above: ownership from the
   credential, and a filter the caller could have read directly.
5. Add it to the leak matrix in `internal/api/tenant_test.go`.

If you forget all of this, the endpoint returns nothing rather than
everything. That is the intended failure mode, but it is still a bug.
