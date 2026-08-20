-- ============================================================================
-- THE HERO QUERY — run this identically BEFORE and AFTER demo_apply_policy.sql.
-- BEFORE: all three clients, landowner phone numbers in the clear.
-- AFTER : only Fictional Utility A, landowner_contact masked. Same SQL, no edits.
-- Business question: "unresolved high/critical encroachments this quarter,
--                     and whose land they're on."
-- ============================================================================
SELECT
    d.client_name,
    c.name              AS corridor,
    c.criticality,
    d.type,
    d.severity,
    d.status,
    d.detected_at::date AS detected,
    d.landowner_contact
FROM ademianczuk_uc_1_catalog.stantec_grid_ops.detections d
JOIN ademianczuk_uc_1_catalog.stantec_grid_ops.corridors  c
  ON c.corridor_id = d.corridor_id
WHERE d.status IN ('open', 'dispatched')
  AND d.severity IN ('high', 'critical')
  AND d.detected_at >= date_trunc('QUARTER', current_date())
ORDER BY d.detected_at DESC
LIMIT 20;
