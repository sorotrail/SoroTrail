# Decoder replay

`sorotrail replay` re-runs the **current** decoder pipeline over the raw XDR
stored alongside every event and rewrites the decoded columns in place.

Decoders improve: new ScVal types get handled, per-standard decoders (SEP-41
and friends) gain edge cases, new normalized tables appear. Without replay,
those improvements only ever apply to events ingested *after* the change, and
the database slowly becomes a mix of decodings from different eras. Replay is
what applies them to everything already stored.

## When to run it

Run a replay after any change that alters decoder output:

- A new ScVal type is handled, so rows previously stored as the lossless
  `{"unknown": {...}}` fallback can now be decoded properly.
- A decoding bug is fixed (wrong number rendering, wrong address form, …).
- A per-standard decoder changes which events it recognizes, or a new derived
  table is added and needs backfilling.

You do **not** need a replay for changes that don't touch decoding — schema
indexes, API changes, ingestion tuning.

Start with a dry run to see the blast radius:

```sh
sorotrail replay --from-ledger 250000 --dry-run
```

## Usage

```sh
sorotrail replay --from-ledger N [--to-ledger M] [flags]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--from-ledger` | — (required) | First ledger to replay, inclusive. |
| `--to-ledger` | `0` | Last ledger to replay, inclusive. `0` = no upper bound. |
| `--batch-size` | `500` | Events re-decoded per transaction. |
| `--restart` | `false` | Discard saved progress and replay the range from the start. |
| `--dry-run` | `false` | Report what would change; write nothing. |

Configuration (notably `DATABASE_URL`) comes from the same environment
variables the indexer uses.

Example — replay everything from ledger 250000 onward in small batches:

```sh
DATABASE_URL=postgres://... sorotrail replay --from-ledger 250000 --batch-size 200
```

Output:

```
replay completed
  rows processed: 18234
  rows changed:   4021
  rows skipped:   118 (no raw XDR stored)
  rows failed:    0 (stored XDR could not be decoded)
  duration:       12.481s
```

- **processed** — every row read in the range, skipped ones included.
- **changed** — rows whose re-decoded columns differed and were rewritten.
  Unchanged rows are never written, so a replay against already-current data
  costs no writes.
- **skipped** — rows with no raw XDR to replay. Expected, not an error: rows
  ingested before raw-XDR retention landed, and events the RPC delivered as
  JSON (`xdrFormat: "json"`), have no XDR to re-decode. Their stored decoding
  is left untouched.
- **failed** — rows whose stored XDR could not be decoded at all. Logged with
  the event ID and left untouched, so one malformed row can't wedge a replay
  forever. A non-zero count deserves investigation.

Exit codes: `0` completed, `2` interrupted (re-run to resume), `1` error.

## Interrupting and resuming

Replay is batched and resumable. Each batch's rewrites and its progress
marker commit in the **same transaction** (`replay_state`, a single row), so
committed progress can never run ahead of committed rewrites.

Press Ctrl-C and the run stops between batches; the in-flight batch rolls
back whole. Re-running the same command resumes from the last committed
event:

```sh
sorotrail replay --from-ledger 250000     # ^C partway through
sorotrail replay --from-ledger 250000     # resumes where it stopped
```

Saved progress is only picked up for an **identical, unfinished** range.
Changing `--from-ledger`/`--to-ledger` starts fresh, because resuming into a
range whose bounds moved would silently skip rows. `--restart` forces a fresh
start over the same range.

Replay is idempotent: running it twice leaves the identical end state, and
the second run rewrites nothing.

## Running against a live database

Replay is designed to run while ingestion and the API keep serving:

- **Short transactions.** One transaction per batch, touching only that
  batch's own rows by primary key. No table-level locks, no long-running
  snapshot. Lower `--batch-size` if you want to shorten them further.
- **No write amplification.** Rows whose decoding is unchanged are not
  written at all, so a re-run over current data is read-only in practice.
- **Ingestion is untouched.** Replay only rewrites decoded columns of
  existing rows; it never inserts, deletes, or moves the ingestion cursor.

### Advisory-lock strategy

Only one replay may run at a time. The guard is a **session-level Postgres
advisory lock** (`pg_try_advisory_lock`) on a dedicated pooled connection,
held for the whole run and released when the run ends.

- The key is `0x536F726F5265706C` — the ASCII bytes of `"SoroRepl"` — stable
  across deployments and unlikely to collide with another application's
  advisory locks on a shared database (see `store.ReplayAdvisoryLockKey`).
- It is a `try` lock, not a blocking one: a second replay **fails
  immediately** with "another replay is already running" instead of queueing
  up behind the first.
- Session-level, so Postgres drops it automatically when the connection dies.
  `kill -9` on a replay leaves no stale lock and no cleanup path to get
  wrong — which is exactly why this is an advisory lock and not a "running"
  status column.
- It is held on one connection out of the pool; ingestion and API queries use
  the rest of the pool untouched.

Two concurrent replays would produce the same rewrites (replay is a pure
function of the raw XDR), but they would interleave writes to the shared
`replay_state` row and corrupt each other's resume point. The lock guards the
*run*, not the rows.

## Derivation order

Replay rewrites the canonical decoded columns first, then everything derived
from them. The order is explicit in code — see the contract on
`store.ReplayBatch` and the write order in `store.CommitReplayBatch`:

1. `events` — the decoded `topics` / `value` columns every dependent table
   reads.
2. Dependent tables, in registration order. Each is derived from the **new**
   decoding, never from what is currently in the database, so a replay stays
   a pure function of the raw XDR.
3. `replay_state` — last, in the same transaction.

There are no dependent tables yet. When one lands (for example `token_events`
from a SEP-41 decoder), add it as a field on `store.ReplayBatch` and write it
in `CommitReplayBatch` between steps 1 and 3 — deleting the batch's rows
before re-inserting, so a derivation that now emits fewer rows removes the
stale ones. Keeping both the field and the write in those two places is what
keeps the order in one reviewable spot.

## Rows without raw XDR

Raw XDR (`events.topics_xdr`, `events.value_xdr`) is stored from the
migration `0004_raw_xdr_and_replay` onward. Rows ingested before that have
`NULL` there and can never be replayed — the XDR is gone and the RPC dropped
the ledger long ago. Replay counts them as *skipped* and leaves their stored
decoding alone.

The same applies to events the RPC delivered already-decoded via
`xdrFormat: "json"`: there was no XDR to keep, so there is nothing to
re-decode.
