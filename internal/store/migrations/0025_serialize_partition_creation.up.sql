-- ensure_event_partitions is called by every UpsertEvents batch before
-- writing, and backfill and the live ingester call it concurrently by
-- design (issue #810). The function's CREATE TABLE IF NOT EXISTS
-- ... PARTITION OF is not concurrency-safe: two sessions can both pass
-- the existence check, and the loser then fails with
-- "relation ... already exists" (SQLSTATE 42P07) instead of skipping.
--
-- The fix serializes partition creation on a constant transaction-scoped
-- advisory lock, making the check-then-create atomic across sessions.
-- Every partition in a batch is created under the one lock, so a slow
-- creator holds it briefly per batch — far cheaper than the deadlock or
-- duplicate-partition failures the race could otherwise produce. The
-- lock is transaction-scoped, so it releases on commit or abort and can
-- never leak across pool connections.
CREATE OR REPLACE FUNCTION ensure_event_partitions(from_ledger bigint, to_ledger bigint, partition_span bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    partition_from bigint;
    partition_to   bigint;
    partition_name text;
BEGIN
    IF from_ledger IS NULL OR to_ledger IS NULL OR from_ledger > to_ledger THEN
        RETURN;
    END IF;
    IF partition_span <= 0 THEN
        RAISE EXCEPTION 'partition_span must be positive';
    END IF;

    PERFORM pg_advisory_xact_lock(hashtext('sorotrail.events.partitions'));

    partition_from := (from_ledger / partition_span) * partition_span;
    WHILE partition_from <= to_ledger LOOP
        partition_to := partition_from + partition_span;
        partition_name := format('events_%s_%s', partition_from, partition_to - 1);
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF events FOR VALUES FROM (%s) TO (%s)',
            partition_name,
            partition_from,
            partition_to
        );
        partition_from := partition_to;
    END LOOP;
END;
$$;
