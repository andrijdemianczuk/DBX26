-- ============================================================================
-- AUDIT CLOSER — governance evidence in ~15 seconds.
-- "Who queried the corridor data, what did they run, and when."
-- Shows my recent activity against the governed schema, including the moment
-- the policy was applied. Runs against Databricks system tables (immutable).
-- ============================================================================
SELECT
    executed_by                          AS who,
    statement_type                       AS action,
    date_format(start_time, 'HH:mm:ss')  AS ran_at,
    left(statement_text, 70)             AS statement
FROM system.query.history
WHERE statement_text ILIKE '%stantec_grid_ops%'
  AND statement_text NOT ILIKE '%system.query.history%'   -- hide this query itself
  AND start_time >= current_timestamp() - INTERVAL 2 HOURS
ORDER BY start_time DESC
LIMIT 12;
