-- ============================================================================
-- 03_gold — business-ready marts (denormalized analytics layer)
-- Gold answers business questions, not storage questions.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- gold.campaign_funnel — per-campaign stage counts, rates, targeting shares
-- opted_out excluded (exit event, not a stage)
-- target_contact_share / target_conversion_share measure targeting accuracy:
-- did the campaign actually reach/convert the segment it aimed at?
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.gold.campaign_funnel AS
WITH ev AS (
  SELECT campaign_id, client_id, event_type
  FROM workspace.silver.campaign_events
  WHERE event_type <> 'opted_out'
),
agg AS (
  SELECT
    campaign_id,
    COUNT(DISTINCT CASE WHEN event_type = 'sent'      THEN client_id END) AS sent,
    COUNT(DISTINCT CASE WHEN event_type = 'delivered' THEN client_id END) AS delivered,
    COUNT(DISTINCT CASE WHEN event_type = 'opened'    THEN client_id END) AS opened,
    COUNT(DISTINCT CASE WHEN event_type = 'clicked'   THEN client_id END) AS clicked,
    COUNT(DISTINCT CASE WHEN event_type = 'converted' THEN client_id END) AS converted
  FROM ev
  GROUP BY campaign_id
),
seg AS (
  SELECT e.campaign_id, cu.segment,
         COUNT(DISTINCT e.client_id) AS contacts,
         COUNT(DISTINCT CASE WHEN e.event_type = 'converted'
                            THEN e.client_id END) AS conversions
  FROM workspace.silver.campaign_events e
  JOIN workspace.silver.customers cu ON e.client_id = cu.client_id
  WHERE e.event_type IN ('sent', 'converted')
  GROUP BY e.campaign_id, cu.segment
),
tgt AS (
  SELECT s.campaign_id,
         SUM(CASE WHEN s.segment = c.target_segment
                   AND c.target_segment <> 'all'
                  THEN s.contacts ELSE 0 END)
           / NULLIF(SUM(s.contacts), 0) AS target_contact_share,
         SUM(CASE WHEN s.segment = c.target_segment
                   AND c.target_segment <> 'all'
                  THEN s.conversions ELSE 0 END)
           / NULLIF(SUM(s.conversions), 0) AS target_conversion_share
  FROM seg s
  JOIN workspace.silver.campaigns c ON s.campaign_id = c.campaign_id
  GROUP BY s.campaign_id
)
SELECT
  c.campaign_id,
  c.product,
  c.channel,
  c.target_segment,
  c.start_date,
  c.end_date,
  c.budget,
  a.sent,
  a.delivered,
  a.opened,
  a.clicked,
  a.converted,
  ROUND(a.opened    / NULLIF(a.sent, 0), 4) AS open_rate,
  ROUND(a.clicked   / NULLIF(a.opened, 0), 4) AS click_rate,
  ROUND(a.converted / NULLIF(a.sent, 0), 4)  AS end_to_end_rate,
  ROUND(t.target_contact_share, 4)      AS target_contact_share,
  ROUND(t.target_conversion_share, 4)   AS target_conversion_share
FROM agg a
JOIN workspace.silver.campaigns c ON a.campaign_id = c.campaign_id
LEFT JOIN tgt t ON a.campaign_id = t.campaign_id;

-- ---------------------------------------------------------------------------
-- gold.segment_performance — conversion rate by segment x credit band
-- avg_propensity exposes the synthetic ground truth: high-propensity
-- segments MUST convert more — that is the signal a real pipeline hunts.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.gold.segment_performance AS
WITH contacted AS (
  SELECT DISTINCT client_id
  FROM workspace.silver.campaign_events
  WHERE event_type = 'sent'
),
converted AS (
  SELECT DISTINCT client_id
  FROM workspace.silver.campaign_events
  WHERE event_type = 'converted'
)
SELECT
  cu.segment,
  cu.credit_score_band,
  COUNT(*)                                              AS contacted_clients,
  SUM(CASE WHEN cv.client_id IS NOT NULL THEN 1 ELSE 0 END) AS converted_clients,
  ROUND(SUM(CASE WHEN cv.client_id IS NOT NULL THEN 1 ELSE 0 END)
        / COUNT(*), 4)                                  AS conversion_rate,
  ROUND(AVG(cu.propensity), 4)                          AS avg_propensity
FROM contacted ct
JOIN workspace.silver.customers cu ON ct.client_id = cu.client_id
LEFT JOIN converted cv ON ct.client_id = cv.client_id
GROUP BY cu.segment, cu.credit_score_band;

-- ---------------------------------------------------------------------------
-- gold.campaign_roi — cost vs conversions vs revenue per campaign
-- cost      = sent events x cost_per_send
-- revenue   = conversions x conversion_value (campaign attribute)
-- roi       = (revenue - cost) / cost ; roas = revenue / cost
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.gold.campaign_roi AS
WITH costed AS (
  SELECT
    campaign_id,
    SUM(CASE WHEN event_type = 'sent' THEN event_cost ELSE 0 END) AS cost_total,
    COUNT(DISTINCT CASE WHEN event_type = 'converted' THEN client_id END) AS conversions
  FROM workspace.silver.campaign_events
  GROUP BY campaign_id
)
SELECT
  c.campaign_id,
  c.product,
  c.channel,
  c.target_segment,
  c.budget,
  ROUND(costed.cost_total, 2)                                        AS cost_total,
  costed.conversions,
  ROUND(costed.conversions * c.conversion_value, 2)                  AS revenue_total,
  ROUND((costed.conversions * c.conversion_value - costed.cost_total)
        / NULLIF(costed.cost_total, 0), 4)                           AS roi,
  ROUND(costed.conversions * c.conversion_value
        / NULLIF(costed.cost_total, 0), 4)                            AS roas
FROM costed
JOIN workspace.silver.campaigns c ON costed.campaign_id = c.campaign_id;

-- ---------------------------------------------------------------------------
-- gold.creative_analysis — variant-level performance + text features
-- Sets up LLM enrichment (ai_query) on environments where model services
-- are available — see README "AI enrichment (designed)".
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.gold.creative_analysis AS
WITH ev AS (
  SELECT campaign_id, client_id, event_type, product, creative_variant, creative_text
  FROM workspace.silver.campaign_events
  WHERE event_type <> 'opted_out'
),
per_variant AS (
  SELECT
    product,
    creative_variant,
    COUNT(DISTINCT CASE WHEN event_type = 'sent'      THEN client_id END) AS sent,
    COUNT(DISTINCT CASE WHEN event_type = 'opened'    THEN client_id END) AS opened,
    COUNT(DISTINCT CASE WHEN event_type = 'clicked'   THEN client_id END) AS clicked,
    COUNT(DISTINCT CASE WHEN event_type = 'converted' THEN client_id END) AS converted,
    AVG(LENGTH(creative_text)) AS avg_text_length
  FROM ev
  GROUP BY product, creative_variant
)
SELECT
  product,
  creative_variant,
  sent, opened, clicked, converted,
  ROUND(opened   / NULLIF(sent, 0), 4) AS open_rate,
  ROUND(clicked  / NULLIF(opened, 0), 4) AS click_rate,
  ROUND(converted / NULLIF(sent, 0), 4) AS conversion_rate,
  ROUND(avg_text_length, 1) AS avg_text_length
FROM per_variant
ORDER BY product, creative_variant;
