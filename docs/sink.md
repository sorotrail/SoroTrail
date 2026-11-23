# Message-bus sink

This document describes the intended message-bus sink. The current proposed
changes only provide a transport-neutral follower; Kafka/NATS adapters,
configuration, durable cursor storage, and process wiring must be implemented
before this feature is enabled.

The completed sink must provide:

- store-first, at-least-once delivery;
- ascending event-ID traversal with per-contract ordering;
- Kafka producer acknowledgements with `acks=all` and idempotence;
- NATS JetStream publish acknowledgements;
- a durable `sink_state` row in the same database as events;
- `sorotrail_sink_lag_events` reporting the actual number of unpublished stored
  events;
- explicit detection and operator guidance when the cursor points before the
  retention horizon;
- Kafka and NATS TLS/SASL or credential configuration;
- a `--from-ledger` reset flow; and
- HA leadership enforcement so only the active leader publishes.

Consumers must deduplicate on the event `id` because a crash after broker
acknowledgement and before cursor persistence intentionally replays the event.
The payload must be identical to the API event JSON, with headers `id`,
`ledger`, and `type`, and the broker key must be `contract_id`.

---

## Webhook Delivery Deduplication Contract

### Delivery guarantee: at-least-once with a deterministic deduplication key

SoroTrail webhook delivery provides **at-least-once delivery with a signed deduplication key**, not "exactly-once" delivery. Promising exactly-once over HTTP would be false: if a subscriber commits a side effect (crediting an account, firing an alert, placing an order) and the TCP connection drops or times out before SoroTrail receives the `2xx` response, SoroTrail **must** retry. Effective-once processing is achieved by combining SoroTrail's deterministic idempotency key with idempotent consumer storage.

### HTTP headers sent on every webhook delivery

| Header | Example | Description |
| --- | --- | --- |
| `Idempotency-Key` | `sub-42:00001073741824-0000000001` | Deterministic deduplication key scoped to `(subscription_id, event.id)`. Identical across all retries and dead-letter replays of the same delivery; distinct for every new event or different subscription. |
| `X-SoroTrail-Idempotency-Key` | `sub-42:00001073741824-0000000001` | Mirror of `Idempotency-Key` in the `X-SoroTrail-*` header namespace. |
| `X-SoroTrail-Delivery-Attempt` | `1` | 1-based attempt counter (`1` on first delivery, `2..5` on retries). |
| `X-SoroTrail-Delivery-Timestamp` | `2026-07-24T12:00:00Z` | RFC3339 UTC timestamp of the original delivery attempt. Remains fixed across retries and dead-letter replays so consumers can measure end-to-end delivery lag and distinguish a fresh event from a retry. |
| `X-SoroTrail-Signature` | `<64-char hex HMAC-SHA256>` | Hex-encoded HMAC-SHA256 signature keyed with the subscription's `secret` covering the deduplication headers and raw JSON body. |

### Why `sub-<subscription_id>:<event.id>` is stable

1. **Zero-padded event coordinate (`event.id`):** Every SoroTrail event has a canonical, zero-padded identifier (`<ledger>-<index>`, e.g. `00001073741824-0000000001`) derived from its position in the ledger rather than a random UUID generated at delivery time.
2. **Subscription scoping (`sub-<subscription_id>:`):** When a single contract event matches multiple subscriptions, SoroTrail fans out one delivery per subscription. Prefixing `sub-<subscription_id>:` ensures that two subscriptions pointing at the same receiver (or sharing a deduplication table) never collide with each other.
3. **Dead-letter replay stability:** Replaying a failed delivery (`Notifier.ReplayDelivery` / `Notifier.EnqueueReplay`) recomputes the exact same `sub-<subscription_id>:<event.id>` key and preserves the original `X-SoroTrail-Delivery-Timestamp`, so replaying after an endpoint outage never looks like new traffic to a consumer that already processed attempt 1 before a timeout.

### Signature coverage

An idempotency key outside the HMAC payload would allow an attacker or misconfigured middlebox to mutate `Idempotency-Key` in transit and force duplicate execution or suppress legitimate events. `X-SoroTrail-Signature` therefore signs the canonical newline-delimited envelope:

```text
<Idempotency-Key>\n<X-SoroTrail-Delivery-Attempt>\n<X-SoroTrail-Delivery-Timestamp>\n<raw_json_body>
```

Consumers **must verify `X-SoroTrail-Signature` before consulting their deduplication store**.

### Reorg and replay behaviour

If a ledger range is retracted and re-applied (or re-ingested via `sorotrail replay` or an auditor repair), an event at the same canonical coordinate (`event.id`) **reuses the same `Idempotency-Key` (`sub-<subscription_id>:<event.id>`)**.

- **Why we reuse the key:** Stellar Consensus Protocol (SCP) finalizes ledgers deterministically in a single round, so in practice re-deliveries in SoroTrail arise from operator replays (`sorotrail replay`) or audit repairs rather than PoW-style chain forks. Because HTTP webhooks are forward-only `POST` notifications with no retraction/tombstone message, generating a new key on replay would cause downstream consumers (accounting, alerting, trading signals) to execute duplicate transfers or alerts on every repair.
- **Consequence for consumers:** Standard forward-only consumers automatically deduplicate re-applied events and never double-apply side effects. If a consumer maintains ledger-indexed state and performs an explicit out-of-band rollback for a ledger range `[L_from, L_to]`, it must also delete its stored deduplication keys for `ledger >= L_from` as part of that rollback transaction so the subsequent re-delivery is applied.

### Recommended consumer deduplication pattern and retention window

- **Retention window:** Keep processed `Idempotency-Key` values for **at least 24 hours (recommended 7 days)**. The in-process retry schedule spans ~31 seconds (`1s + 2s + 4s + 8s + 16s`), while operator dead-letter replays and `sorotrail replay` operations typically occur within hours or days of an incident.
- **Atomic deduplication:** Insert the `Idempotency-Key` into the same database transaction as the downstream business side effect (`INSERT INTO processed_webhooks ... ON CONFLICT DO NOTHING`).

```go
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"net/http"
)

func verifyWebhook(r *http.Request, secret string, body []byte) bool {
	sig := r.Header.Get("X-SoroTrail-Signature")
	key := r.Header.Get("Idempotency-Key")
	soroKey := r.Header.Get("X-SoroTrail-Idempotency-Key")
	attempt := r.Header.Get("X-SoroTrail-Delivery-Attempt")
	timestamp := r.Header.Get("X-SoroTrail-Delivery-Timestamp")
	if sig == "" || key == "" || attempt == "" || timestamp == "" {
		return false
	}
	if soroKey != "" && soroKey != key {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(key + "\n" + attempt + "\n" + timestamp + "\n"))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sig), []byte(expected))
}

func handleWebhook(db *sql.DB, secret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		if !verifyWebhook(r, secret, body) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		idempotencyKey := r.Header.Get("Idempotency-Key")
		tx, err := db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		defer tx.Rollback()

		res, err := tx.ExecContext(r.Context(), `
			INSERT INTO processed_webhooks (idempotency_key, first_delivered_at, expires_at)
			VALUES ($1, $2, NOW() + INTERVAL '7 days')
			ON CONFLICT (idempotency_key) DO NOTHING`,
			idempotencyKey, r.Header.Get("X-SoroTrail-Delivery-Timestamp"),
		)
		if err != nil {
			http.Error(w, "dedup error", http.StatusInternalServerError)
			return
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			// Duplicate retry or dead-letter replay: acknowledge 200 OK without re-running side effects.
			w.WriteHeader(http.StatusOK)
			return
		}

		// Apply downstream side effect inside the same transaction, then commit.
		if err := tx.Commit(); err != nil {
			http.Error(w, "commit error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
```

