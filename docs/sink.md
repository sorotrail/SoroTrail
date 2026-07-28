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
