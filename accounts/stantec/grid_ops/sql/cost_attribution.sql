-- ============================================================================
-- COST ATTRIBUTION — "what did this demo cost, and who do we bill it to?"
-- Every serverless query in this demo runs on a warehouse tagged
-- project=corridor-demo, so its spend rolls up under one tag.
-- Estimated $ = DBUs x list price (system.billing.list_prices).
-- NOTE: billing/usage system tables lag a few hours; run this the morning of
--       the demo (usage tagged the day before will have landed).
-- ============================================================================
SELECT
    u.custom_tags['project']          AS project,
    u.billing_origin_product          AS product,
    u.sku_name,
    round(sum(u.usage_quantity), 2)   AS dbus,
    round(sum(u.usage_quantity * lp.pricing.default), 2) AS est_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices lp
       ON u.sku_name = lp.sku_name
      AND u.usage_start_time >= lp.price_start_time
      AND (u.usage_start_time < lp.price_end_time OR lp.price_end_time IS NULL)
WHERE u.custom_tags['project'] = 'corridor-demo'
  AND u.usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY 1, 2, 3
ORDER BY est_cost_usd DESC;

-- --------------------------------------------------------------------------
-- FALLBACK (tag not yet propagated): attribute by the demo warehouse id
-- directly. Works as soon as usage lands.
-- --------------------------------------------------------------------------
-- SELECT u.sku_name, round(sum(u.usage_quantity),2) AS dbus
-- FROM system.billing.usage u
-- WHERE u.usage_metadata.warehouse_id = 'c6250844810982c2'
--   AND u.usage_date >= current_date() - INTERVAL 7 DAYS
-- GROUP BY 1 ORDER BY dbus DESC;
