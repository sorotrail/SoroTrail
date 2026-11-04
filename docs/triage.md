# Issue Triage & Labeling Guide

This guide defines how SoroTrail maintainers and contributors classify, label,
and route issues and pull requests. It exists so a new maintainer can triage
consistently without tribal knowledge.

## Principles

- **Label by signal, not by volume.** A correctly-labeled issue is easier to
  find, assign, and prioritize than a long discussion.
- **Every actionable issue gets `type/*` + `area/*` + a priority.** The type
  and area drive filtering; the priority drives ordering.
- **Bug reports and feature requests must use the issue forms.** Blank issues
  are disabled (`blank_issues_enabled: false` in
  `.github/ISSUE_TEMPLATE/config.yml`); security issues go through private
  vulnerability reporting and Q&A goes to Discussions.
- **Be kind on close.** `duplicate`, `invalid`, and `wontfix` are labels, not
  judgments — always leave a one-line comment explaining the call.

## Label taxonomy

### `type/*` — what kind of work

| Label | Use for |
| --- | --- |
| `type/bug` | Wrong or unexpected behavior; data loss or corruption is `P0`. |
| `type/enhancement` | New feature or capability, behavior change requested by users. |
| `type/docs` | Documentation only — README, `docs/`, `CONTRIBUTING.md`, code comments. |
| `type/chore` | Build, tooling, dependency bumps, refactors with no behavior change. |
| `type/ci` | CI workflow / pipeline changes. |
| `type/test` | Adding or fixing tests, coverage, fuzzing, simtests. |
| `type/question` | Usage question — usually redirected to Discussions, then closed. |
| `type/security` | Vulnerability, hardening, or audit-adjacent work. |

### `area/*` — which seam

Map the issue to one of the architecture seams (see
[`docs/architecture-overview.md`](architecture-overview.md)). These are also
applied automatically to PRs by path via `.github/labeler.yml`.

| Label | Maps to |
| --- | --- |
| `area/ingester` | `internal/ingester`, polling, backoff, cursor, reorg. |
| `area/store` | `internal/store`, migrations, schema, partitions. |
| `area/api` | `internal/api`, HTTP handlers, OpenAPI, caching. |
| `area/spec` | `internal/spec`, contract-spec enrichment. |
| `area/rpc` | `internal/rpc`, RPC client, failover, rate limiting. |
| `area/decode` | `internal/decode`, ScVal decoding, fuzzing. |
| `area/replay` | `internal/replay`, decoder replay. |
| `area/audit` | `internal/audit`, verifier, findings. |
| `area/webhook` | `internal/webhook`, subscriptions, delivery. |
| `area/build` | `Makefile`, `Dockerfile*`, goreleaser, `go.mod`. |
| `area/docs` | Everything under `docs/` and top-level `.md`. |

### Priority — how urgent

| Label | Meaning | SLA (guideline) |
| --- | --- | --- |
| `P0` | Data loss, security hole, total outage. Triage immediately. | same day |
| `P1` | Core feature broken or major regression; blocks users. | within a few days |
| `P2` | Normal bug or useful enhancement; scheduled into a milestone. | next milestone |
| `P3` | Nice-to-have, polish, low-impact. | backlog |

If no priority is set after triage, treat it as `P2` until discussed.

### Workflow / status labels

| Label | Meaning |
| --- | --- |
| `good first issue` | Self-contained, well-scoped; safe for new contributors. Pair with a clear `area/*`. |
| `help wanted` | Maintainer welcomes outside contributions; not assigned internally. |
| `blocked` | Waiting on another issue/PR, upstream, or a decision. Add a comment with the blocker. |
| `needs-info` | Reporter must supply logs, config, or reproduction. Auto-close after inactivity (see below). |
| `duplicate` | Superseded by another issue — comment with the canonical link before applying. |
| `wontfix` | Deliberately not pursued; comment with the rationale. |
| `invalid` | Not actionable / not a bug / out of scope. Redirect to Discussions when appropriate. |
| `stale` | No activity past the stale window; closed by the stale bot or manually. |

## Triage workflow

1. **Inbox review (daily-ish).** New issues land via the bug/feature forms.
2. **Classify.** Apply exactly one `type/*` and one `area/*`. If it touches
   multiple areas, pick the primary one and mention the others in a comment.
3. **Prioritize.** Add `P0`–`P3` based on the table above.
4. **Scope.** If it's a clean, contained task suitable for a newcomer, add
   `good first issue` and (optionally) `help wanted`. If it needs design input,
   start a discussion and label `blocked` with a link.
5. **Assign.** Attach to the current milestone if it's `P0`/`P1` or a committed
   `P2`. Leave `P3` unassigned in the backlog.
6. **Acknowledge.** A one-line comment ("Thanks — reproduced, targeting vX")
   closes the loop and sets expectations.

### Pull requests

- PRs opened against the default branch are auto-labeled with `area/*` by path
  via the labeler workflow. Maintainers still add `type/*` and priority.
- Require the standard checks from [`CONTRIBUTING.md`](../CONTRIBUTING.md):
  `go build ./...`, `make test`, `make test-integration`, `make lint`.
- A PR that changes the events table or OpenAPI must update the corresponding
  migration test / `api/openapi.yaml` + `make spec`.

## Closing issues

- **Duplicate:** comment `Duplicate of #<id>`, apply `duplicate`, close.
- **Wontfix:** comment rationale, apply `wontfix`, close.
- **Invalid / question:** redirect to Discussions or SECURITY.md, apply
  `invalid`, close.
- **Stale:** issues with `needs-info` and no response for ~14 days, or any
  issue with no activity for ~60 days, may be labeled `stale` and closed after
  a grace period. Always leave a final comment inviting re-open.

## Keeping labels in sync

The canonical label set is documented above and mirrored for automation in
`.github/labeler.yml` (PR path → `area/*` rules). When you add a new
`area/*` or `type/*` label, update both this guide and the labeler config so
the two don't drift.

## Diagnosing a Failover-Induced Gap

When triaging an `area/rpc` or `area/ingester` report where events appear
missing or delayed around an RPC provider switch, check the failover metrics
and structured logs in the following order:

1. **Confirm whether a provider switch or cursor re-anchor occurred:**
   - Query `rate(sorotrail_rpc_failovers_total[5m])` by `reason`:
     - `reason="switch"` indicates the active provider changed between calls.
     - `reason="reanchor"` indicates a switch occurred mid-pagination while a
       cursor was active; the ingester discards the foreign cursor and
       re-anchors from the last ingested ledger.
   - Check `sorotrail_rpc_provider_state{provider, state}` to see which
     providers are currently `active`, `degraded`, or `down`.

2. **Inspect per-provider demotion reasons and promotions:**
   - Query `increase(sorotrail_rpc_provider_demotions_total[1h])` by `provider`
     and `reason`:
     - `reason="consecutive_errors"`: transport errors or HTTP 5xx responses
       exceeded `maxConsecutiveErrors`.
     - `reason="behind_cursor"`: candidate provider reported a `LatestLedger`
       behind the current ingestion cursor (or lagged the highest observed
       chain head beyond `headSkewTolerance`) and was skipped before advancing
       the cursor.
     - `reason="retention_gap"`: candidate provider's `OldestLedger` started
       after the requested `StartLedger` (or returned `IsLedgerOutOfRange`). If
       every configured provider has aged past the cursor, the ingester logs
       `resume ledger fell outside RPC retention window; skipping ahead` and
       increments `sorotrail_ledger_gaps_detected_total`.
     - `reason="network_mismatch"`: candidate provider's `getNetwork`
       passphrase did not match the configured Stellar network; the provider is
       permanently pinned to `down` and never promoted.
     - `reason="divergence"`: candidate provider returned event IDs or payloads
       for an overlapping ledger range that disagreed with events already
       observed from the previous provider.
   - Query `increase(sorotrail_rpc_provider_promotions_total[1h])` to spot
     flapping providers that repeatedly recover via `getHealth` probes and get
     re-demoted on live traffic.

3. **Correlate with structured log lines:**
   - `provider behind cursor on selection path — skipping` / `provider response latestLedger is behind cursor — skipping`
   - `provider retention gap detected in getEvents response — skipping` / `provider returned ledger out of range — failing over`
   - `provider network passphrase mismatch — rejecting provider`
   - `provider returned divergent event data — demoting to down`
   - `provider switch with active cursor — re-anchor required`

