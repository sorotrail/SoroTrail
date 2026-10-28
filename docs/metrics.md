# Metrics catalogue

Every Prometheus metric SoroTrail exports, in one place: its type, its
labels, and what the number means. Two registries serve them, both in a
single scrape of `GET /metrics` (`ENABLE_METRICS=true`):

- the **global registry** — every `sorotrail_*` counter, gauge and
  histogram, registered when `internal/metrics` is imported, so every
  process that links the package exports them;
- the **per-server registry** — `http_request_duration_seconds`, one
  histogram per `metrics.HTTPMetrics` instance, so tests and parallel
  servers do not collide on the global one.

A scrape also carries the Prometheus client library's own runtime
families (`go_*`, `process_*`, `promhttp_*`) — the library's
instrumentation rather than SoroTrail's, listed here only so nobody
mistakes them for undocumented project metrics.

This file is generated from the catalogue in
`internal/metrics/metrics_catalogue_test.go`, which is checked against
`metrics.go`'s declarations and against what the registries actually
serve, so a renamed, removed or newly added metric fails the build until
it is documented. Regenerate it with:

    go test ./internal/metrics -run TestMetricsDocumentation -update-metrics-doc

## Conventions

- Every metric name is lower-case snake_case; global metrics carry the
  `sorotrail_` prefix (the sole exception, `http_request_duration_seconds`,
  predates the prefix and is quoted in runbooks, so it is pinned by test
  rather than renamed).
- Counters end `_total`; histograms carry their unit (`_seconds`);
  gauges are plain unit-suffixed names.
- Labels come from a closed set (see "Label sets") and no label ever
  carries entity identity: no event id, transaction hash, address,
  credential or raw path. `route` is the matched chi route pattern (or
  `unmatched`), so path probes cannot grow cardinality; `provider` is a
  configured RPC URL's hostname with scheme, credentials, path and query
  stripped.
- Histograms use the Prometheus default buckets
  (`DefBuckets`, 0.005s … 10s), which suit RPC round trips and database
  writes; the `_count` and `_sum` series every histogram exports are
  how call totals and mean latency are read without a separate counter.

## Ingestion metrics

| Metric | Type | Labels | Help (as served) | Meaning |
| --- | --- | --- | --- | --- |
| `sorotrail_events_ingested_total` | counter | — | Total number of events ingested (including duplicates resolved by idempotent upsert). | Throughput of the ingestion pipeline. Duplicates resolved by the idempotent upsert still count, so this measures pipeline work rather than net-new rows; subtract the store's own counts when net-new is what matters. Also read by the /stats endpoint as the in-process ingest counter. |
| `sorotrail_ingest_errors_total` | counter | — | Total number of ingestion passes that failed with an error. | Ingestion passes that ended in an error — an RPC failure, a decode failure or a database write failure. Retries are not counted, only passes that ended badly. Registered but not yet incremented by any call site, so it reads 0; it is pinned here so the wiring lands against a documented contract. |
| `sorotrail_ingestion_lag_ledgers` | gauge | — | Number of ledgers the indexer is behind the chain head. | Chain head minus last ingested ledger, updated after every ingestion pass that knows the chain head. The direct alert for a stalled pipeline: anything above zero for a sustained window means ingestion is falling behind. |

## RPC metrics

| Metric | Type | Labels | Help (as served) | Meaning |
| --- | --- | --- | --- | --- |
| `sorotrail_rpc_call_duration_seconds` | histogram | `method` (getEvents \| getLatestLedger \| getHealth \| getLedgerEntries \| simulateTransaction); `outcome` (success \| error) | RPC call latency in seconds (HTTP round trip + body read + parse), labelled by method and outcome. | Latency of a single JSON-RPC call. The histogram's _count for a (method, outcome) pair is that pair's call total, so one metric answers both "how slow" and "how many, and how many failed" — when ingestion falls behind, this says whether the upstream is slow or failing. |
| `sorotrail_rpc_retries_total` | counter | `method` (getEvents \| getLatestLedger \| getHealth \| getLedgerEntries \| simulateTransaction); `reason` (backoff \| retry_after) | Total number of RPC retry attempts, labelled by method and reason (backoff \| retry_after). | Every retry attempt after the first. reason says where the wait came from: the computed exponential backoff, or a provider-supplied 429 Retry-After hint — a lot of retry_after means the provider is explicitly throttling us. |
| `sorotrail_rpc_backoff_seconds_total` | counter | `method` (getEvents \| getLatestLedger \| getHealth \| getLedgerEntries \| simulateTransaction) | Total seconds spent sleeping between RPC retries, labelled by method. | Cumulative wall-clock time lost to retry waits. Priced in seconds rather than counts so a long tail is visible: sustained growth means the provider (or our own rate limiter) is throttling us. |
| `sorotrail_rpc_failovers_total` | counter | `reason` (switch \| reanchor) | Total number of RPC provider failover events, labelled by reason (switch \| reanchor). | Provider failovers in the multi-provider client: "switch" when traffic moved to a different provider, "reanchor" when a cursor request hit the failover-reanchor path and the caller had to discard its cursor (the expensive kind — reanchors mean lost progress, not just a new connection). |
| `sorotrail_rpc_circuit_breaker_state` | gauge | `state` (closed \| open \| half-open) | Circuit breaker state as a 0/1 gauge per state (closed \| open \| half-open). | Exactly one state series is 1 at a time. Alert on {state="open"} == 1: the breaker is refusing calls outright, which is why ingestion has stopped rather than merely slowed. |
| `sorotrail_rpc_provider_state` | gauge | `provider` (the hostname of one RPC_URLS entry — scheme, credentials, path and query are stripped, so nothing client-identifying is exported); `state` (active \| degraded \| down) | Per-provider failover health state as a 0/1 gauge per state (active \| degraded \| down). | One 0/1 series per (provider, state). Only present in multi-provider mode (RPC_URLS). Shows which provider traffic is on and which have failed, without reading logs. |

## Storage metrics

| Metric | Type | Labels | Help (as served) | Meaning |
| --- | --- | --- | --- | --- |
| `sorotrail_db_write_duration_seconds` | histogram | — | Database write latency in seconds (batch insert/upsert/repair). | Write path only: batch upserts, replace-in-range repairs. Compared against the ingester's write-latency budget it tells you whether backpressure is justified by the database or by our own accounting. |
| `sorotrail_db_query_duration_seconds` | histogram | `operation` (store.<Store method> — a fixed table of method names (store.UpsertEvents, store.QueryEvents, …), one per guarded Store operation) | Store operation duration in seconds, labelled by operation. | Every guarded Store operation, reads and writes alike, so a slow database is attributable to a specific operation rather than showing up only as a slow API. |
| `sorotrail_db_operations_total` | counter | `operation` (store.<Store method> — the same fixed table as sorotrail_db_query_duration_seconds); `outcome` (success \| error) | Total store operations, labelled by operation and outcome (success \| error). | Count of guarded Store calls. Errors are counted separately from successes so a failing backend is a direct query rather than something inferred by subtracting histograms. |

## Batching metrics

| Metric | Type | Labels | Help (as served) | Meaning |
| --- | --- | --- | --- | --- |
| `sorotrail_event_batch_writes_total` | counter | — | Total number of event store writes (UpsertEvents calls) issued by the ingester. | One per non-empty page with batching disabled; with batching enabled it is how many chunks the adaptive controller split pages into. Rising against a flat events-ingested counter means writes are being shaved smaller. |
| `sorotrail_event_batch_size` | gauge | — | Current adaptive event batch size used for each store write. | The size the batch controller is currently feeding the store. It sits at the configured maximum while writes are inside the latency budget and steps down under pressure, so a falling gauge is the earliest signal that the store is starting to strain. |
| `sorotrail_event_backpressure_total` | counter | — | Total number of throttle sleeps inserted between event batch writes. | How many times the ingester deliberately slept between writes because the store fell behind the latency budget. Non-zero proves backpressure is firing; paired with sorotrail_event_backpressure_seconds_total it separates brief hesitations from sustained throttling. |
| `sorotrail_event_backpressure_seconds_total` | counter | — | Total seconds spent throttling event writes under backpressure. | Cumulative sleep time under backpressure — the surge priced in seconds. A long tail here means the database was slow for a sustained stretch, not just a burst. |

## Other pipeline metrics

| Metric | Type | Labels | Help (as served) | Meaning |
| --- | --- | --- | --- | --- |
| `sorotrail_spec_invalidations_received_total` | counter | — | Total number of spec cache invalidations received from other replicas. | Spec-cache invalidations this process applied because another replica said the contract's decoded shape changed. A non-zero value proves the cross-replica fan-out is working; a replica that never invalidates anything while its peers change specs is serving stale decodes. Stays at zero on a single-replica deployment, where there is nobody to hear from. |
| `sorotrail_http_limit_rejections_total` | counter | `reason` (request_body \| request_decompressed \| response_body) | Total number of HTTP requests or responses rejected for exceeding a configured size limit, labelled by reason (request_body \| request_decompressed \| response_body). | Requests or responses refused with 413 Request Entity Too Large because they exceeded a configured size bound: request_body for raw request bytes over the endpoint or global HTTP_REQUEST_BODY_LIMIT, request_decompressed for gzip/deflate payloads whose decompressed stream expanded past the limit, and response_body for non-streaming JSON responses over HTTP_RESPONSE_BODY_LIMIT or the caller's X-Max-Response-Bytes bound. |

## HTTP metrics (per-server registry)

| Metric | Type | Labels | Help (as served) | Meaning |
| --- | --- | --- | --- | --- |
| `http_request_duration_seconds` | histogram | `route` (the matched chi route pattern (e.g. /events/{id}), or "unmatched" for a path no route registered — never the raw path, so probes cannot grow cardinality); `method` (the HTTP method (GET \| POST \| PUT \| PATCH \| DELETE)); `status` (the response status code as a decimal string (200 \| 404 \| 500 \| …)) | HTTP request duration in seconds, labeled by route, method, and status code. | Per-request latency for the whole API. One instance per metrics.HTTPMetrics (i.e. per server), merged with the global registry on scrape, so tests and parallel servers do not collide on the default registry. |

## Label sets

Every label value is drawn from a closed set, so series counts stay
predictable and alert rules can name a value with confidence:

| Label | Values | Used by |
| --- | --- | --- |
| `method` | getEvents \| getLatestLedger \| getHealth \| getLedgerEntries \| simulateTransaction | `sorotrail_rpc_call_duration_seconds`, `sorotrail_rpc_retries_total`, `sorotrail_rpc_backoff_seconds_total`, `http_request_duration_seconds` |
| `operation` | store.<Store method> — a fixed table of method names (store.UpsertEvents, store.QueryEvents, …), one per guarded Store operation | `sorotrail_db_query_duration_seconds`, `sorotrail_db_operations_total` |
| `outcome` | success \| error | `sorotrail_rpc_call_duration_seconds`, `sorotrail_db_operations_total` |
| `provider` | the hostname of one RPC_URLS entry — scheme, credentials, path and query are stripped, so nothing client-identifying is exported | `sorotrail_rpc_provider_state` |
| `reason` | request_body \| request_decompressed \| response_body | `sorotrail_http_limit_rejections_total`, `sorotrail_rpc_retries_total`, `sorotrail_rpc_failovers_total` |
| `route` | the matched chi route pattern (e.g. /events/{id}), or "unmatched" for a path no route registered — never the raw path, so probes cannot grow cardinality | `http_request_duration_seconds` |
| `state` | closed \| open \| half-open | `sorotrail_rpc_circuit_breaker_state`, `sorotrail_rpc_provider_state` |
| `status` | the response status code as a decimal string (200 \| 404 \| 500 \| …) | `http_request_duration_seconds` |

The `unmatched` route value and the hostname-only `provider` value
are the two sets described in prose above; both are produced by the
server, never by the client, which is what keeps their cardinality
bounded.
