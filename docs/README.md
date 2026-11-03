# Documentation

This directory holds the project documentation for operators and contributors.
Use this index to find the relevant page by topic before drilling into the
full details.

## Runbooks

- [replay.md](replay.md) — explains how to re-run the decoder over stored raw
  XDR and refresh decoded event data after a decoder change.
- [backfill.md](backfill.md) — procedures for historical ledger backfills and
  navigating Stellar RPC retention limits.
- [runbook.md](runbook.md) — procedures for health probes, scaling, dead-letter
  queue maintenance, and backups.
- [triage.md](triage.md) — immediate step-by-step triage guide for production
  outages and lag alerts.
- [troubleshooting.md](troubleshooting.md) — diagnosis and remedies for RPC errors,
  database lock contention, decode failures, and startup issues.

## Reference

- [api-keys.md](api-keys.md) — key generation, storage hashing, startup
  bootstrap behavior, CLI management, scope enforcement, rotation procedures,
  and the PostgreSQL requirement.
- [clickhouse.md](clickhouse.md) — capability matrix for the optional
  analytical backend, distinguishing implemented methods from refusals and silent
  no-ops.
- [deprecation.md](deprecation.md) — API deprecation and breaking-change policy,
  `Deprecation` (RFC 9745) and `Sunset` (RFC 8594) HTTP headers, OpenAPI extensions,
  generated Go/Python client behavior, and GraphQL `@deprecated` conventions.
- [errors.md](errors.md) — error code reference and handling guide.
- [graphql.md](graphql.md) — read-only GraphQL endpoint (`/graphql`) and GraphiQL
  playground, top-level query fields, Relay cursor pagination, depth/complexity limits,
  filter parity with `GET /events`, and multi-tenant scope behavior.
- [indexes.md](indexes.md) — explanations of composite indexes and query plans.
- [logging.md](logging.md) — structured logging conventions and tracing configuration.
- [metrics.md](metrics.md) — exported Prometheus metrics and alerting rules.
- [rate-limits.md](rate-limits.md) — token bucket, fixed hourly/daily quotas,
  per-tenant overrides, response headers, and multi-replica caveats.
- [spec-overrides.md](spec-overrides.md) — uploading, inspecting, and deleting
  user-supplied contract event specs, JSON schema validation, precedence over
  on-chain Wasm specs, and cache invalidation behavior.
- [subscriptions.md](subscriptions.md) — real-time event streaming over WebSocket,
  pub-sub broker architecture, at-most-once delivery semantics, backpressure handling,
  and the cursor catch-up pattern.

## Design

- [architecture.md](architecture.md) — complete internal system architecture,
  component boundaries, data flow from Stellar RPC to storage, request lifecycles,
  and sequence diagrams.
- [architecture-overview.md](architecture-overview.md) — high-level architectural summary
  and system topology.
- [multi-tenancy.md](multi-tenancy.md) — multi-tenant isolation model, tenant boundaries,
  contract grants, quotas, and accounting.
- [local-setup.md](local-setup.md) — setting up local development dependencies,
  PostgreSQL, and Stellar RPC testnet environments.
- [performance.md](performance.md) — latency profiles, indexing strategies, and
  throughput tuning.
- [archival.md](archival.md) — data retention, partition pruning, and historical archiving.
- [sink.md](sink.md) — bulk event export configurations and external sinks.

# SoroTrail Documentation

Welcome to the SoroTrail documentation index. This directory contains detailed
technical guides, architectural specifications, runbooks, and operational manuals
for operators and developers.

## Architecture and Core Concepts

- [System Architecture](architecture.md): Complete internal system architecture,
  component boundaries, data flow from Stellar RPC to storage, request lifecycles,
  and sequence diagrams.
- [Architecture Overview](architecture-overview.md): High-level architectural summary
  and system topology.
- [Multi-tenancy](multi-tenancy.md): Multi-tenant isolation model, tenant boundaries,
  contract grants, quotas, and accounting.
- [ClickHouse Backend Support](clickhouse.md): Capability matrix for the optional
  analytical backend, distinguishing implemented methods from refusals and silent
  no-ops.

## Real-Time and Authentication APIs

- [GraphQL API Guide](graphql.md): Read-only GraphQL endpoint (`/graphql`) and GraphiQL
  playground, top-level query fields, Relay cursor pagination, depth/complexity limits,
  filter parity with `GET /events`, and multi-tenant scope behavior.
- [Live Subscriptions](subscriptions.md): Real-time event streaming over WebSocket,
  pub-sub broker architecture, at-most-once delivery semantics, backpressure handling,
  and the cursor catch-up pattern.
- [API Keys and Scopes](api-keys.md): Key generation, storage hashing, startup
  bootstrap behavior, CLI management, scope enforcement, rotation procedures,
  and the PostgreSQL requirement.
- [Contract Spec Overrides](spec-overrides.md): Uploading, inspecting, and deleting
  user-supplied contract event specs (`PUT|GET|DELETE /contracts/{id}/spec`), JSON
  schema validation, precedence over on-chain Wasm specs, and cache invalidation behavior.

## Ingestion, Backfill, and Replay

- [Backfill Guide](backfill.md): Procedures for historical ledger backfills and
  navigating Stellar RPC retention limits.
- [Replay Guide](replay.md): Re-running decoders against stored raw XDR payloads
  when contract ABIs change or parser bugs are fixed.
- [Local Setup](local-setup.md): Setting up local development dependencies,
  PostgreSQL, and Stellar RPC testnet environments.
- [onboarding.md](onboarding.md): End-to-end guide for new contributors —
  environment setup, test tiers, issue assignment, review conventions, and common
  first-PR mistakes with their CI failures.

## Operations and Runbooks

- [Operational Runbook](runbook.md): Procedures for health probes, scaling, dead-letter
  queue maintenance, and backups.
- [Incident Triage](triage.md): Immediate step-by-step triage guide for production
  outages and lag alerts.
- [Troubleshooting](troubleshooting.md): Diagnosis and remedies for RPC errors,
  database lock contention, decode failures, and startup issues.
- [Logging](logging.md): Structured logging conventions and tracing configuration.
- [Performance](performance.md): Latency profiles, indexing strategies, and
  throughput tuning.
- [Database Indexes](indexes.md): Explanations of composite indexes and query plans.
- [Event Archival](archival.md): Data retention, partition pruning, and historical archiving.
- [Export Sink](sink.md): Bulk event export configurations and external sinks.