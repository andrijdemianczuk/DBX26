-- ============================================================================
-- GOVERNANCE, APPLIED LIVE.  Run this on screen during the demo.
-- Table-level row filter + column mask, applied to every fact table.
-- They take effect on the very next query — no Genie or agent change.
--
-- Why table-level (ALTER TABLE ... SET ROW FILTER) rather than a principal-scoped
-- ABAC policy (CREATE POLICY ... TO <user>): a table-level filter applies to
-- EVERY caller — the SQL editor, the Genie UI, AND the served agent behind the
-- chat app (which queries under its own service principal). A `TO <user>` policy
-- only governs that user, so the app path would bypass it. This makes the
-- before/after flip visible from all surfaces. (The governed tags grid_client /
-- grid_pii remain on the columns for discovery/classification.)
-- ============================================================================

-- (1) Row filter: restrict every fact table to Fictional Utility A.
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.corridors   SET ROW FILTER ademianczuk_uc_1_catalog.stantec_grid_ops.rf_client_scope ON (client_name);
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.detections  SET ROW FILTER ademianczuk_uc_1_catalog.stantec_grid_ops.rf_client_scope ON (client_name);
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.work_orders SET ROW FILTER ademianczuk_uc_1_catalog.stantec_grid_ops.rf_client_scope ON (client_name);
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.inspections SET ROW FILTER ademianczuk_uc_1_catalog.stantec_grid_ops.rf_client_scope ON (client_name);

-- (2) Column mask: redact landowner contact PII.
ALTER TABLE ademianczuk_uc_1_catalog.stantec_grid_ops.detections ALTER COLUMN landowner_contact SET MASK ademianczuk_uc_1_catalog.stantec_grid_ops.mask_contact;
