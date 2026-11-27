# Spec-Aware Event Decoding

This document describes the relationship between structural event decoding and spec-aware event decoding in SoroTrail.

## Structural vs. Spec-Aware Decoding

- **Structural Decoding:** The immutable source of truth. It translates raw XDR event payloads into universal types (symbols, addresses, i128 values, etc.) without requiring external contract declarations.
- **Spec-Aware Decoding:** An optional interpretation layer. When a contract specification is available, SoroTrail maps the structural fields to the human-readable names and declared types defined by the contract author.

## Honesty & Graceful Degradation

- If a spec and payload disagree in field counts or shapes, structural decoding remains untouched, and a mismatch flag and error are reported rather than hiding the raw data.
- Missing, partial, and stale specs degrade gracefully to structural-only output.
- Spec versioning is respected via historical lookups so that historical events are matched against the specification active at their ledger sequence.
