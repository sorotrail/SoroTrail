-- Placeholder migration.
--
-- Version 0005 was lost in a merge; the migration set is renumbered up to
-- 0025 and golang-migrate requires sequential versions with no gaps, so a
-- fresh database would refuse to migrate without this file. The original
-- 0005 migration's changes (if any remain relevant) were folded into the
-- migrations that follow it, so this is intentionally a no-op.
SELECT 1;
