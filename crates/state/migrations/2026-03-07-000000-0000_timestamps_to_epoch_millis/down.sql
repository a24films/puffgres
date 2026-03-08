-- This migration is not reversible without data loss (precision).
-- Re-running the original migrations would recreate the TEXT columns.
SELECT RAISE(ABORT, 'down migration not supported for timestamp conversion');
