-- ============================================================================
-- 04_quality — data quality gate
-- Each check raises an error on violation; a failed job goes red.
-- These assertions encode the CONTRACT each layer promises.
-- ============================================================================

-- volume sanity: expect ~400k-900k generated events at this scale
SELECT raise_error(
  'dq.bronze_volume: ' || (SELECT COUNT(*) FROM workspace.bronze.campaign_events)
  || ' rows outside expected range'
)
WHERE (SELECT COUNT(*) FROM workspace.bronze.campaign_events)
      NOT BETWEEN 400000 AND 900000;

-- the mess exists in bronze: NULL channels must be present (~1%)
SELECT raise_error('dq.bronze_null_channel: expected ~1% NULL channel in bronze')
WHERE (SELECT COUNT(*) FROM workspace.bronze.campaign_events
       WHERE channel IS NULL) < 1000;

-- silver dedup worked: zero duplicate event_id
SELECT raise_error('dq.silver_dupes: duplicate event_id survived silver')
WHERE EXISTS (
  SELECT event_id FROM workspace.silver.campaign_events
  GROUP BY event_id HAVING COUNT(*) > 1
);

-- silver actually cleaned: zero NULL channels after COALESCE
SELECT raise_error('dq.silver_null_channel: NULL channel survived silver')
WHERE EXISTS (
  SELECT 1 FROM workspace.silver.campaign_events WHERE channel IS NULL
);

-- funnel monotonicity per campaign
SELECT raise_error('dq.funnel_monotonic: stage counts out of order')
WHERE EXISTS (
  SELECT 1 FROM workspace.gold.campaign_funnel
  WHERE converted > clicked
     OR clicked   > opened
     OR opened    > delivered
     OR delivered > sent
);

-- referential integrity: every silver event maps to a known customer
SELECT raise_error('dq.orphan_events: silver event without customer')
WHERE EXISTS (
  SELECT 1 FROM workspace.silver.campaign_events e
  LEFT JOIN workspace.silver.customers c ON e.client_id = c.client_id
  WHERE c.client_id IS NULL
);

-- ROI sanity: no campaign with zero/negative cost producing fake ROI
SELECT raise_error('dq.roi_sanity: non-positive cost_total in campaign_roi')
WHERE EXISTS (
  SELECT 1 FROM workspace.gold.campaign_roi WHERE cost_total <= 0
);
