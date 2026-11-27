# Audit Trail Triage and Verification

SoroTrail maintains a tamper-evident, cryptographically chained audit trail covering sensitive actions such as credential lifecycle management, tenant configuration changes, spec overrides, and deletions.

## Chain Verification

Operators can verify the integrity of the audit log chain at any time using the `VerifyChain` verifier function or CLI diagnostic tooling. Verification proceeds sequentially, verifying that:
1. Each record's `prev_hash` matches the hash of its immediate predecessor.
2. Re-computing the SHA-256 hash over the canonicalised record payload matches its stored `hash`.

Multi-tenant isolation is enforced: tenants querying audit logs or running verification receive only their own tenant-scoped entries (`tenant_id` matching or global admin queries).

## Retention Sweeps and Checkpoints

To prevent retention sweeps and log pruning from appearing as tampering or breaking the hash chain, a periodic external checkpoint is emitted and stored in `audit_checkpoints` prior to pruning historical audit records. The verifier validates against these checkpoints if early records have been pruned under legitimate retention policies.

## Responding to Verification Failures

If verification fails:
1. **Divergence Point:** The verifier returns the exact sequence number and reason (hash mismatch, prev_hash mismatch, or unexpected missing gap).
2. **External Checkpoint Comparison:** Compare the latest database chain head against emitted external log checkpoints or external monitoring stores.
3. **Investigation:** Inspect database access logs and query history around the failing sequence number. Treat any uncheckpointed divergence as potential security compromise or unauthorised tampering.