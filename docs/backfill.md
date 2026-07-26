# Horizon backfill

`sorotrail backfill` ingests historical contract events from a Stellar
Horizon instance — events older than the RPC's retention window — so the
indexer can fill in the gap between the earliest RPC-retained ledger and
the contract's deployment. The live RPC ingester can only realistically
start from `latest − RETENTION_LEDGERS` (~24h on the public testnet);
this command reaches back further on networks whose Horizon deployment
retains historical transaction meta.

The contract that gets you there:

```
                       ┌── older than RPC retention ──┐
  untouched by live RPC │      backed by Horizon       │ touched by live RPC
                       └────────────────────────────────┘
                on-chain history ────────────────▶ now
                  ^                              ^
                  backfill picks up here         live ingester picks up here
```

## When to run it

Run a backfill for any of:

- **Pre-deployment catch-up.** You configured `WATCHED_CONTRACTS` early
  enough that the live ingester now covers all activity since startup;
  you want historical events from before startup too.
- **A new contract.** A contract was added to `WATCHED_CONTRACTS`
  after the indexer started; backfill closes its missing-prefix without
  requiring a recover-from-zero on the live ingester.
- **Recovery from a long outage.** The indexer was down long enough
  that its resume point is outside the live RPC window; live ingestion
  gives up with a one-line warning, and backfill covers the gap.

If you've never run a backfill against this deployment, start with
`--dry-run`:

```sh
sorotrail backfill --contract CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC \
  --from-ledger 1 --to-ledger 100000 --dry-run
```

## Usage

```sh
sorotrail backfill --contract C... --from-ledger N [--to-ledger M] [flags]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--contract` | — (required) | Contract ID to backfill events for (`C...` strkey). |
| `--from-ledger` | — (required) | First ledger to backfill, inclusive. |
| `--to-ledger` | `0` | Last ledger to backfill, inclusive. `0` = no upper bound (run until Horizon returns an empty page). |
| `--batch-size` | `200` | Transactions per Horizon page. Capped at Horizon's per-page limit. |
| `--rps` | from env | Override `BACKFILL_RATE_RPS` for this run only. |
| `--horizon-url` | from env | Override `HORIZON_URL` for this run only. |
| `--include-failed` | `true` | Include transactions whose tx-level result code was failed (they may still emit events from a contract call that succeeded within a fee-bump inner tx). |
| `--dry-run` | `false` | Walk the range and report what would be written; do not touch the database. Resets any partial progress. |
| `--restart` | `false` | Discard saved progress for this run and start fresh from `--from-ledger`. |

Configuration (notably `DATABASE_URL`) comes from the same environment
variables the indexer uses. Backfill-specific vars:

| Variable | Default | Meaning |
| --- | --- | --- |
| `HORIZON_URL` | `https://horizon-testnet.stellar.org` | Horizon REST endpoint. Point this at a private Horizon instance for mainnet backfill speed. |
| `BACKFILL_RATE_RPS` | `10` | Pace against Horizon. 10 req/s is the public-instance cap; private deployments can lift this. |

Example — backfill a contract's full history at a slower rate:

```sh
DATABASE_URL=postgres://... \
HORIZON_URL=https://horizon-testnet.stellar.org \
BACKFILL_RATE_RPS=5 \
  sorotrail backfill --contract CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC \
    --from-ledger 1
```

Output:

```
backfill completed
  pages fetched:   23
  transactions:   4581
  events skipped:  4225 (no meta or no events emitted)
  events failed:   0 (XDR decode error)
  events extracted: 712
  events inserted: 712 (idempotent upsert: dedupes rencounters)
  through ledger: 250000
  duration:       4m12s
```

- **pages fetched** — Horizon round trips; useful when investigating
  pagination or rate-limit issues.
- **transactions** — every tx fetched, including skipped and failed
  ones.
- **skipped** — txs whose `result_meta_xdr` carried no events (V1/V2
  classic, or Soroban txs whose contract call didn't emit one). Normal
  for a mixed-history run.
- **failed** — txs whose XDR we couldn't decode. Zero is the target; a
  non-zero value deserves investigation (probably a Horizon field
  shape we haven't seen yet).
- **extracted** — events produced from successfully decoded txs.
- **inserted** — events actually written. Idempotent: a re-run over a
  range with already-known events inserts `0`.
- **through ledger** — inclusive highest ledger we processed this run.

Exit codes: `0` completed, `2` interrupted (re-run to resume), `1` error.

## Interrupting and resuming

Backfill is batched and resumable. Each page's events commit, then the
`backfill_state.last_ledger` row advances. Pressing Ctrl-C between pages
is safe — the in-flight upsert commits and the next page exits cleanly
so progress stays consistent:

```sh
sorotrail backfill --contract C... --from-ledger 1     # ^C partway through
sorotrail backfill --contract C... --from-ledger 1     # resumes where it stopped
```

Saved progress is **only** picked up for an identical unfinished run.
Changing `--contract`, `--from-ledger`, or `--to-ledger` starts fresh,
because resuming into a row whose bounds moved would silently skip rows
in the gap. `--restart` forces a fresh start over the same range.

Backfill is idempotent against itself: running it twice leaves the same
end state and the second run writes nothing new (`Inserted == 0`) once
the first run finished.

### What we persist

`backfill_state` is a single-row table that holds exactly:

- `contract_id` — the contract the run was started for.
- `from_ledger` and `to_ledger` — the inclusive range bounds.
- `last_ledger` — inclusive highest ledger whose events have
  committed.
- `started_at`, `updated_at`, `completed_at` — timestamps; `completed_at`
  is non-null only when the run finished its whole range.

We deliberately persist `last_ledger` rather than Horizon's opaque
`paging_token`: a `last_ledger`-based cursor is portable across Horizon
URLs and survives schema migrations, while the opaque token is bound to
the URL/version that issued it. The cost is re-fetching the resume
boundary when you resume, which is harmless because `UpsertEvents`
idempotently dedupes any duplicate events.

## Overlap with live ingestion

Live ingestion starts from `latest ledger − RETENTION_LEDGERS`; backfill
typically reaches further back. Where they meet — anywhere within the
RPC's retention window — both paths will write the same on-chain event.
Their row IDs differ by design:

- Live ingest writes events with TOID-style IDs (`%020d-%010d`) it
  receives from the RPC.
- Backfill writes events with IDs derived from
  `{tx_hash}-{ledger:020d}-{op_index:05d}-{event_index:05d}` synthesized
  from the Horizon row. Horizon doesn't expose the RPC's internal
  TOID position, so a different stable ID is the simplest path to a
  crash-free run.

End result: the events table can contain both rows for the same on-chain
emission. The API returns both; clients dedupe via
`(tx_hash, op_index, event_index)`. The raw XDR in the row is the same —
both rows are replayable identically.

If you want strict dedupe of the overlap region, run backfill **after**
the live ingester has covered the bounds:

```sh
# Live ingester reaches 100, then backfill up to 90, then live catches
# up the rest. The 90..100 range is written twice — by both paths —
# but the API's idempotent upserts keep the table consistent on each
# path independently.
```

## Source limitations

These are intrinsic to using Horizon transaction meta as a backfill
source, not bugs to chase. Document them for the operator before
launching a long backfill so you don't get a false alarm from an empty
range:

- **Horizon retention policy.** Public Stellar testnet Horizon retains
  everything from protocol 17 onward (Soroban launch era). Public
  mainnet instances vary — some prune old meta. Confirm your target
  Horizon's policy before scheduling a backfill that's expected to
  span years of history. When a tx falls outside retention, we count
  it as Skipped (no error), but its events are unrecoverable.
- **Speed.** Bound by Horizon's pagination (~200 txs per page) and
  per-tx XDR decoding. Expect a low-thousands events/second throughput
  on a public instance; private deployments can hit tens of thousands.
- **V1/V2 classic transactions** carry no events. They're counted as
  Skipped, not Failed.
- **Failed Soroban transactions** may carry an empty
  `result_meta_xdr` — we count them as Skipped.
- **Soroban fee-bump transactions** wrap a single inner tx. We recurse
  into V4 meta's `InnerTransactions[]` so events emitted by the
  inner tx's ops are captured (with the inner op's index, not the
  outer wrap's).
- **Contract-as-account quirk.** Horizon treats contract IDs the same
  as `G...` account IDs for indexing: `/accounts/{contract_id}/transactions`
  returns any tx where the contract is in the operation footprint, the
  source, or a destination. This is the right behavior for the
  "backfill every tx that touched the contract" requirement, but it
  means events emitted by *sibling* contracts that share a tx with the
  target are filtered out at extraction time rather than written
  alongside the target's events.

## Rate limiting

The backfill constructs an `intervalLimiter` from `BACKFILL_RATE_RPS`
(or `--rps`) so every Horizon page is spaced evenly. The limiter blocks
the per-page goroutine, so overdriving Horizon doesn't happen: if the
rate is too low you simply take longer, never more requests.

Public Horizon's effective cap is ~10 req/s; private deployments can
depend on your provider's policy. There's no automatic ramp-up — if you
want to start slow, set `BACKFILL_RATE_RPS=2` for the first page
manually and bump it once you're confident the connection is healthy.

## Running alongside live ingestion

Backfill is designed to run with the live indexer or the API still
serving:

- **No table-level locks.** Every commit is per-row upserts on the
  events table by primary key.
- **Short transactions.** Each page commits its events in one batch
  but with constrained ledger and event cardinality.
- **Live ingestion is untouched.** Backfill's `backfill_state` table
  is private to the backfill tool; live ingestion reads `ingestion_state`,
  not this one.

The only contention point is `events` itself — live and backfill may
write the same event ID concurrently in the overlap zone. The store's
`UpsertEvents` is idempotent on the primary key, so the loser of any
per-row race sees a successful no-op rather than a duplicate.
