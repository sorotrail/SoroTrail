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

## Decoder version marker

Each event's decoded columns are recorded with a `decoder_version` marker (derived from `decode.CurrentDecoderVersion`). Replay can target events below a specific version marker rather than scanning the entire historical table:

```sh
sorotrail replay --from-ledger 250000 --target-decoder-version 1
```

Backends that cannot support decoder replay (such as backends lacking stored raw XDR or mutable storage for decoded columns) refuse honestly via the project's unsupported-operation sentinel rather than silently doing nothing.

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
| `--progress-interval` | `0` | Emit periodic progress to stderr (e.g. `30s`, `1m`). `0` disables. |

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
- **failed** — rows whose stored XDR could not be decoded at all. Recorded and logged with
  the event ID and left untouched, consistent with the ingester dead-letter path, so one malformed row can't wedge a replay forever. A non-zero count deserves investigation.
- **Identity invariant** — replay never changes an event's identity (`id`, `contract_id`, `ledger`, `tx_hash`, etc.) or its ordering. Only the decoded `topics` and `value` columns (and decoder version) change.

Exit codes: `0` completed, `2` interrupted (re-run to resume), `1` error.

## Archival caveat

Archived chunks hold the old decoding state and will not be replayed. History retrieved from the archive may therefore be decoded differently than live database queries until restored chunks are reprocessed.

## The workflow, end to end

A replay is five steps: know what changed, size the job, dry-run it, run it,
verify it. The commands below are the whole loop, in order.

### 1. Confirm the new decoder is the one deployed

Replay runs the decoder compiled into the binary you invoke — not the one
running in the indexer. Running an old binary against a database rewrites
rows *backwards*.

```sh
# The running indexer reports the build it was compiled from.
curl -s localhost:8080/version | jq .
# {
#   "version": "v1.9.0",
#   "commit": "4f2ab19",
#   "build_date": "2026-09-18T10:22:04Z"
# }
```

Check that commit contains the decoder change you are replaying for, and
invoke `replay` from a binary built from the same commit or newer. In
Kubernetes, run the replay from the same image tag as the deployment:

```sh
kubectl run sorotrail-replay --rm -it --restart=Never \
  --image=ghcr.io/sorotrail/sorotrail:v1.9.0 \
  --env="DATABASE_URL=$DATABASE_URL" \
  -- replay --from-ledger 250000 --progress-interval 1m
```

### 2. Size the job

Only rows that still have their raw XDR can be replayed, so the row count in
the range is an upper bound, not the real one:

```sql
SELECT count(*)                                   AS rows_in_range,
       count(*) FILTER (WHERE raw_value_xdr IS NOT NULL) AS replayable
  FROM events
 WHERE ledger >= 250000;
```

If `replayable` is far below `rows_in_range`, most of the range predates raw
XDR retention and a replay will mostly report *skipped* — see
[Rows without raw XDR](#rows-without-raw-xdr).

### 3. Dry-run the range

```sh
DATABASE_URL=postgres://... sorotrail replay --from-ledger 250000 --dry-run
```

```
replay completed (dry run — nothing written)
  rows processed: 18234
  rows changed:   4021
  rows skipped:   118 (no raw XDR stored)
  rows failed:    0 (stored XDR could not be decoded)
  duration:       9.203s
```

Read `rows changed` as the blast radius. Two answers are worth pausing on:

- **Zero changed.** Either the decoder change does not affect stored data,
  or you are running the wrong binary. Go back to step 1.
- **Every row changed.** Expected for a change to a common ScVal type;
  surprising for a narrow fix. Spot-check one row (step 4) before writing.

### 4. Spot-check a single event first

Take one event ID out of the range and compare what is stored now against
what the new decoder produces from the same XDR:

```sh
# What is stored today — ?decoded=false returns the stored columns with
# nothing derived layered on top.
curl -s 'localhost:8080/events/0000250013-0000000001?decoded=false' | jq '{topics, value}'

# The raw XDR those columns were decoded from.
curl -s 'localhost:8080/events/0000250013-0000000001/raw' | jq .
```

Then dry-run just that event's ledger and confirm it is counted as changed:

```sh
sorotrail replay --from-ledger 250013 --to-ledger 250013 --dry-run
```

### 5. Run it

Start with a bounded slice rather than the whole history — a small range
proves the pipeline end to end and is cheap to inspect:

```sh
sorotrail replay --from-ledger 250000 --to-ledger 260000 --progress-interval 30s
```

```
  replay: 8000  rate 194.2/s  elapsed 41s
  replay: 16000  rate 193.5/s  elapsed 1m23s
replay completed
  rows processed: 18234
  rows changed:   4021
  rows skipped:   118 (no raw XDR stored)
  rows failed:    0 (stored XDR could not be decoded)
  duration:       94.481s
```

Then let the rest run unbounded:

```sh
sorotrail replay --from-ledger 260001 --progress-interval 1m
```

For a very large history, walk it in chunks so each run is independently
resumable and its results independently reviewable:

```sh
for start in $(seq 250000 100000 950000); do
  end=$((start + 99999))
  echo "── ledgers $start..$end"
  sorotrail replay --from-ledger "$start" --to-ledger "$end" --batch-size 200 \
    || exit 1   # exit 2 = interrupted; re-run the same command to resume
done
```

Under Docker Compose the replay is a one-off container sharing the service's
environment:

```sh
docker compose run --rm sorotrail replay --from-ledger 250000
```

### 6. Verify

```sh
# Re-running is the cheapest verification: a completed replay rewrites
# nothing the second time (rows changed: 0).
sorotrail replay --from-ledger 250000 --to-ledger 260000 --dry-run

# And the spot-checked event now reads back with the new decoding.
curl -s 'localhost:8080/events/0000250013-0000000001?decoded=false' | jq '{topics, value}'
```

A second dry run reporting `rows changed: 0` is the end-state check: the
stored decoding is now a fixed point of the current decoder.

Caches are the one thing a replay does not touch. `GET /events/{id}`
responses are served with a one-year `immutable` cache lifetime, so a
replayed event can still be served from a CDN or client cache in its old
decoding. Purge the affected paths, or accept the lag, after a replay that
changed a lot of rows.

### Where it fits with the other commands

| Symptom | Command |
| --- | --- |
| Decoder improved; stored decodings are stale | `sorotrail replay` (this doc) |
| Ledger range was never ingested at all | `sorotrail backfill` ([backfill.md](backfill.md)) |
| Stored rows disagree with the chain | the auditor's repair path ([runbook.md](runbook.md)) |

Replay never fetches from the RPC and never inserts rows. If the data is
missing rather than mis-decoded, replay is the wrong tool — it can only
re-decode what is already stored.

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

Raw XDR (`events.raw_topic_xdr`, `events.raw_value_xdr`) is stored from the
migration `0007_partition_events` onward. Rows ingested before that have
`NULL` there and can never be replayed — the XDR is gone and the RPC dropped
the ledger long ago. Replay counts them as *skipped* and leaves their stored
decoding alone.

The same applies to events the RPC delivered already-decoded via
`xdrFormat: "json"`: there was no XDR to keep, so there is nothing to
re-decode.
