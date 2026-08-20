-- ============================================================================
-- RESET governance to the "before" state (run between rehearsals).
-- Drops the two policies; tables, tags, and functions remain in place so the
-- demo is instantly re-armable via demo_apply_policy.sql.
-- ============================================================================

-- (DROP POLICY does not support IF EXISTS; if a policy is already gone this line
--  errors harmlessly — safe to ignore when re-running.)
DROP POLICY grid_client_scope ON SCHEMA ademianczuk_uc_1_catalog.stantec_grid_ops;
DROP POLICY grid_pii_mask     ON SCHEMA ademianczuk_uc_1_catalog.stantec_grid_ops;
