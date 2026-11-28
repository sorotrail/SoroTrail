# ClickHouse backend support

SoroTrail can be pointed at ClickHouse with a `clickhouse://` `DATABASE_URL`.
This page describes what that backend can and cannot do today.

ClickHouse is an **optional analytical read-side backend**, not a drop-in
replacement for Postgres. It is a good fit when the workload is large
aggregate queries over historical events and the deployment is comfortable
running a separate Postgres instance for the control-plane tables. It is not
ready to be the only database behind a full SoroTrail installation: several
`Store` methods return a zero value instead of refusing, so a caller can
believe an operation succeeded when nothing was written or read.

That distinction — **refusing** versus **silently doing nothing** — is the
whole point of the matrix below. A refusal is visible in a log line and a
non-2xx response. A silent no-op looks like success, and the first symptom is
usually a missing row or an empty dashboard far from the call that caused it.
If you run ClickHouse today, monitor the frontend against Postgres for those
surfaces, or avoid the endpoints listed as silent.

The backend's own source comment still calls it "intentionally minimal for
now". This page is the operator-facing version of that sentence. Several
surfaces that were silent no-ops when this page was proposed — subscriptions,
dead letters, contract cursors, watched-contract mutations, audit and
ingestion state, and `Ping` — now issue real queries; the matrix marks the
ones that still do not.

## How to read the matrix

| State | Meaning | What to do |
| --- | --- | --- |
| **implemented** | The method issues a real ClickHouse query or mutation and returns its result. | Safe to use, subject to the caveats in the row. |
| **refuses** | The method returns an error. | Safe: the caller sees the failure. The error may or may not wrap `ErrUnsupported`. |
| **silent no-op** | The method returns a zero value and a nil error. | Treat as unavailable. The API will report success with empty or zero data. |

## Support matrix

### Events and queries

| Capability | Methods | State | Notes |
| --- | --- | --- | --- |
| Event writes | `UpsertEvents`, `ReplaceEventsInRange` | **implemented** | Idempotency is enforced in memory because the events table is a plain MergeTree. |
| Event lookups | `GetEvent`, `GetEventsByTxHash`, `EventExists` | **implemented** | Scope predicates are applied in SQL, matching Postgres. |
| Event listing | `QueryEvents`, `CountEvents` | **implemented** | Contract, type, ledger, time and cursor filters work. Topic filters take a separate refusing path below. |
| Topic filters | `QueryEvents` | **refuses** | `topic`, `topic_contains` and `topic0`..`topic3` return `ErrUnsupported` because `topics` is an opaque JSON string with no jsonb containment operator. |
| Event aggregation | `AggregateEvents`, `LedgerRangeCensus` | **refuses** | Both return `ErrUnsupported` so callers and auditors do not receive empty false success. |
| Stats | `Stats` | **implemented** | `table_size_bytes` stays 0. Tenant watch-list entries are not counted because ClickHouse has no tenant table. |
| Migration version | `MigrationVersion` | **implemented** | Queries schema_migrations table version and dirty status. |

### Contract inventory and metadata

| Capability | Methods | State | Notes |
| --- | --- | --- | --- |
| Contract inventory | `ListContracts` | **implemented** | Returns contract summaries ordered by last seen. |
| Contract inventory count | `CountContracts` | **implemented** | Counts distinct contract IDs in events, matching Postgres. |
| Contract summary | `GetContractSummary` | **implemented** | Exact `count()`, `min(ledger)`, `max(ledger)` and `max(created_at)`; unknown contract returns `ErrNotFound`. |
| Per-type breakdown | `ContractEventTypeCounts` | **implemented** | Aggregated in ClickHouse with `GROUP BY type`, ordered by count descending. |
| Contract metadata | `ListContractIDs`, `GetContractMeta`, `UpsertContractMeta`, `CountContractEvents`, `ListContractsNeedingRefresh` | **refuses** | All five return `ErrUnsupported`. The backend stores no contract metadata, so token enrichment cannot run here; a caller that recognises the sentinel stands down instead of retrying a capability that will never arrive. `GET /contracts/{id}/stats` answers 501 rather than an empty stats object. [Tracking search](https://github.com/sorotrail/SoroTrail/issues?q=is%3Aissue+clickhouse+metadata). |
| Contract specs | `GetContractSpec`, `GetContractSpecOverride` | **refuses** | Refuses with `ErrUnsupported` to prevent silent caching misses. |
| Contract spec writes | `SetContractSpec`, `SetContractSpecOverride`, `DeleteContractSpecOverride` | **refuses** | Refuses with `ErrUnsupported` instead of silent no-ops. |
| Contract spec override history | `SetContractSpecOverrideWithHistory`, `DeleteContractSpecOverrideWithHistory`, `ListContractSpecOverrideHistory` | **refuses** | All three return `ErrUnsupported`, so the API answers 501 rather than reporting an empty history. The history is written in the same transaction as the override it records, and the backend has neither. |

### Ingestion, audit and watch state

| Capability | Methods | State | Notes |
| --- | --- | --- | --- |
| Ingestion state | `GetIngestionState`, `SaveIngestionState` | **implemented** | ReplacingMergeTree rows read with `FINAL`; saves append a newer version. |
| Audit state | `GetAuditState`, `SaveAuditState`, `SaveAuditStateIfGreater` | **implemented** | The "if greater" compare-and-set is a read-then-write, not atomic across instances. |
| Watched contracts | `ListWatchedContracts`, `ListWatchedContractsScoped`, `AddWatchedContract`, `RemoveWatchedContract` | **implemented** | Removal is a soft delete; `FINAL` collapses versions. `ListWatchedContractsScoped` filters to the caller's `Scope`. |
| Per-contract cursors | `GetContractCursor`, `SaveContractCursor`, `DeleteContractCursor`, `ListContractCursors` | **implemented** | Delete is a synchronous ClickHouse mutation. |
| Audit findings | `RecordAuditFinding`, `UpdateAuditFinding`, `ListOpenFindingsByRange` | **implemented** | IDs are minted in-process because the HTTP interface has no sequence or `RETURNING`. |

### Subscriptions and delivery

| Capability | Methods | State | Notes |
| --- | --- | --- | --- |
| Subscription management | `CreateSubscription`, `GetSubscription`, `ListSubscriptions`, `UpdateSubscription`, `DeleteSubscription`, `ListEnabledSubscriptions` | **implemented** | Owner predicates are part of the SQL, not a post-filter. |
| Failure counters | `IncrementSubscriptionFailures`, `ResetSubscriptionFailures` | **implemented** | Read-modify-write; not atomic across concurrent workers. |
| Delivery history | `RecordDeliveryAttempt`, `ListDeliveryAttempts`, `CountDeliveryAttempts` | **implemented** | Owner-filtered like subscriptions. |
| Dead letters | `DeadLetterEvent`, `ListDeadLetters`, `CountDeadLetters`, `GetDeadLetter`, `DeleteDeadLetter` | **implemented** | Same retry and cursor semantics as Postgres. |

### Retention, address index and health

| Capability | Methods | State | Notes |
| --- | --- | --- | --- |
| Retention | `DeleteEventsBeforeLedger`, `DeleteEventsBefore`, `CountEventsBefore` | **implemented** | ClickHouse deletes are asynchronous mutations, so the reported row count is a snapshot taken before the mutation. |
| Address index | `UpsertAddressRefs`, `QueryAddressEvents`, `CountAddressEvents`, `GetAddressSummary` | **silent no-op** | Address lookups always return nothing. [Tracking search](https://github.com/sorotrail/SoroTrail/issues?q=is%3Aissue+clickhouse+address). |
| Health | `Ping` | **implemented** | Opens a TCP connection to the native port. It does not authenticate or execute a query, so a listening-but-unhealthy server still reports healthy; treat it as a liveness check, not a database health check. |
| API keys | `CreateAPIKey`, `GetAPIKey`, `LookupAPIKeyByPrefix`, `ListAPIKeys`, `RevokeAPIKey` | **refuses** | Returns a plain error that does **not** wrap `ErrUnsupported`, so API handlers cannot map it to the shared 501 response. Authentication is expected to live in front of a ClickHouse deployment. [Tracking search](https://github.com/sorotrail/SoroTrail/issues?q=is%3Aissue+clickhouse+ErrUnsupported). |

## What this means for a deployment

- **Do not rely on `GET /contracts` with ClickHouse.** The count can be
  non-zero while the page is empty. `GET /contracts/{id}/stats` and the
  per-type breakdown work, because those two methods are aggregate queries
  over the events table.
- **Readiness is only a TCP check.** `Ping` verifies that something is
  listening on the native port. Pair it with an external query-level probe if
  you need to detect a wedged server.
- **The conformance suite skips ClickHouse wholesale.** The shared suite
  truncates tables between tests, which this backend cannot yet do over its
  HTTP interface, and topic-filtered queries refuse. The registration in
  `internal/store/conformance_test.go` therefore declares every behavioural
  test unsupported until a live `TEST_CLICKHOUSE_URL` server can be used to
  verify and narrow that declaration.
- **Contract metadata is unavailable, and says so.** Token names, symbols and
  decimals live in a Postgres-only table. On ClickHouse the whole contract
  metadata surface refuses with `ErrUnsupported`, the API answers 501 on the
  endpoints that need it, and any enrichment worker recognises the sentinel
  and stands down instead of reporting success while writing nothing. If you
  need token names/symbols, keep those reads on Postgres.
- **Address lookups silently do nothing.** If you need address history, keep
  those reads on Postgres.

## Tracking the gaps

The open work is split across focused issues rather than one "finish
ClickHouse" issue:

- Contract summary and type-count queries (this page's two newly implemented
  reads): [#1085](https://github.com/sorotrail/SoroTrail/issues/1085).
- Documentation of the backend's actual support (this page): [#1083](https://github.com/sorotrail/SoroTrail/issues/1083).
- Contract metadata and the remaining read-path gaps:
  [open ClickHouse issues](https://github.com/sorotrail/SoroTrail/issues?q=is%3Aissue+clickhouse).
- The `ErrUnsupported` mapping for methods that still return plain errors:
  [open ClickHouse issues mentioning ErrUnsupported](https://github.com/sorotrail/SoroTrail/issues?q=is%3Aissue+clickhouse+ErrUnsupported).
- Address-index support:
  [open ClickHouse issues mentioning address](https://github.com/sorotrail/SoroTrail/issues?q=is%3Aissue+clickhouse+address).

If a row above is a silent no-op with no matching issue, that is a
documentation bug as much as a backend bug — open one rather than treating
the silence as intended behaviour.

## Verifying this page against the code

The method names in the matrix are not hand-maintained prose. The test
`TestClickHouseSupportMatrixMatchesStoreInterface` in `docs/docs_test.go`
walks the `Store` interface with reflection and fails when:

- a method named in the matrix no longer exists on the interface, or
- a method on the interface is missing from the matrix.

The state column is still a human judgement, but the list of capabilities is
derived from the interface, so a new `Store` method cannot be added without
either documenting its ClickHouse status or failing CI.
