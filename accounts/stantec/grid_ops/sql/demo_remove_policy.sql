-- ============================================================================
-- RESET governance to the "before" state (run between rehearsals).
-- Drops the table-level row filters + column mask; tables, tags, and the
-- filter/mask functions remain in place so the demo is instantly re-armable via
-- demo_apply_policy.sql.
--
-- (DROP ROW FILTER / DROP MASK on a table that has none errors harmlessly —
--  safe to ignore when re-running.)
-- ============================================================================

ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.corridors   DROP ROW FILTER;
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.detections  DROP ROW FILTER;
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.work_orders DROP ROW FILTER;
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.inspections DROP ROW FILTER;

ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.detections ALTER COLUMN landowner_contact DROP MASK;
