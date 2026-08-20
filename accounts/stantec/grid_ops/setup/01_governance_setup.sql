-- ============================================================================
-- Grid Corridor demo — GOVERNANCE SETUP (run once, up front)
-- Creates the row-filter + column-mask functions and applies governed tags to
-- the tables so they are visible in Catalog Explorer BEFORE the demo.
-- The policies themselves are NOT created here — they are applied live on
-- screen via demo_apply_policy.sql. This file is idempotent.
-- Governed tags used (created at account level by setup):
--   grid_client = 'scoped'   -> marks the client-scoping column (row filter)
--   grid_pii    = 'contact'  -> marks landowner PII columns   (column mask)
-- ============================================================================

-- 1) Row-filter function: keep only rows for the scoped client.
CREATE OR REPLACE FUNCTION ademianczuk_uc_1_catalog.stantec_grid_ops.rf_client_scope(client STRING)
  RETURNS BOOLEAN
  COMMENT 'Row filter: restrict visible rows to Fictional Utility A'
  RETURN client = 'Fictional Utility A';

-- 2) Column-mask function: redact landowner PII.
CREATE OR REPLACE FUNCTION ademianczuk_uc_1_catalog.stantec_grid_ops.mask_contact(v STRING)
  RETURNS STRING
  COMMENT 'Column mask: redact landowner contact PII'
  RETURN '*** MASKED (grid_pii) ***';

-- 3) Tag the client-scoping column on every table that carries it.
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.corridors   ALTER COLUMN client_name SET TAGS ('grid_client' = 'scoped');
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.detections  ALTER COLUMN client_name SET TAGS ('grid_client' = 'scoped');
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.work_orders ALTER COLUMN client_name SET TAGS ('grid_client' = 'scoped');
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.inspections ALTER COLUMN client_name SET TAGS ('grid_client' = 'scoped');

-- 4) Tag the PII column.
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.detections  ALTER COLUMN landowner_contact SET TAGS ('grid_pii' = 'contact');
