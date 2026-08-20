-- ============================================================================
-- GOVERNANCE, APPLIED LIVE.  Run this on screen during the demo.
-- Two policies, keyed off governed tags, scoped to the whole schema.
-- They take effect on the very next query — no table, Genie, or agent change.
-- ============================================================================

-- (1) Row filter: every table whose client column is tagged `grid_client=scoped`
--     is restricted to Fictional Utility A.
CREATE OR REPLACE POLICY grid_client_scope
  ON SCHEMA ademianczuk_uc_1_catalog.stantec_grid_ops
  COMMENT 'Restrict rows to the caller''s authorized client (Fictional Utility A)'
  ROW FILTER ademianczuk_uc_1_catalog.stantec_grid_ops.rf_client_scope
  TO `andrij.demianczuk@databricks.com`
  FOR TABLES
  MATCH COLUMNS has_tag_value('grid_client', 'scoped') AS client_col
  USING COLUMNS (client_col);

-- (2) Column mask: every column tagged `grid_pii=contact` is redacted.
CREATE OR REPLACE POLICY grid_pii_mask
  ON SCHEMA ademianczuk_uc_1_catalog.stantec_grid_ops
  COMMENT 'Mask landowner contact PII'
  COLUMN MASK ademianczuk_uc_1_catalog.stantec_grid_ops.mask_contact
  TO `andrij.demianczuk@databricks.com`
  FOR TABLES
  MATCH COLUMNS has_tag_value('grid_pii', 'contact') AS pii_col
  ON COLUMN pii_col;
