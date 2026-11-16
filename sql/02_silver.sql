-- ============================================================================
-- 02_silver — conformance layer: what analytics can trust
-- bronze = raw/as-landed (with deliberate mess)
-- silver = one row per real-world event, typed, cleaned, derived fields
-- Dimensions first (events join them for channel recovery).
-- ============================================================================

-- silver.customers / silver.campaigns — conformance pass-through
CREATE OR REPLACE TABLE workspace.silver.customers AS
SELECT
  client_id, segment, region, income_band, tenure_months,
  credit_score_band, has_loan, has_credit_card, email_opt_in, propensity
FROM workspace.bronze.customers;

CREATE OR REPLACE TABLE workspace.silver.campaigns AS
SELECT * FROM workspace.bronze.campaigns;

-- silver.campaign_events — dedup on event_id, recover NULL channel from the
-- campaign dimension (bronze.campaigns — guaranteed to exist post-01),
-- derive event_date
CREATE OR REPLACE TABLE workspace.silver.campaign_events AS
WITH ranked AS (
  SELECT
    e.*,
    row_number() OVER (PARTITION BY e.event_id ORDER BY e.event_ts) AS _rn
  FROM workspace.bronze.campaign_events e
)
SELECT
  r.event_id,
  r.client_id,
  r.campaign_id,
  r.event_type,
  r.event_ts,
  to_date(r.event_ts) AS event_date,
  COALESCE(r.channel, c.channel) AS channel,
  r.product,
  r.creative_variant,
  r.creative_text,
  r.event_cost
FROM ranked r
LEFT JOIN workspace.bronze.campaigns c ON r.campaign_id = c.campaign_id
WHERE r._rn = 1;
