-- ============================================================================
-- 01_generate — synthetic data generator (bronze layer)
-- Domain: digital-bank marketing campaigns (BR)
--
-- Determinism: every random draw is xxhash64(<key parts>) mapped to [0,1).
-- Same inputs => identical data on every run, regardless of partitioning.
--
-- Latent ground truth: each customer gets a `propensity` score computed
-- from attributes; funnel thresholds are driven by it, so gold analytics
-- surface real segment effects instead of noise.
--
-- Scale: 30k customers x 20 campaigns -> ~300k events.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- bronze.customers (30k)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.bronze.customers AS
WITH ids AS (
  SELECT explode(sequence(1, 30000)) AS client_id
),
attrs AS (
  SELECT
    client_id,
    pmod(xxhash64('cust', client_id, 'income'), 1000000) / 1000000.0 AS u_income,
    pmod(xxhash64('cust', client_id, 'seg'),    1000000) / 1000000.0 AS u_seg,
    pmod(xxhash64('cust', client_id, 'region'), 1000000) / 1000000.0 AS u_region,
    pmod(xxhash64('cust', client_id, 'tenure'), 1000000) / 1000000.0 AS u_tenure,
    pmod(xxhash64('cust', client_id, 'cred'),   1000000) / 1000000.0 AS u_cred,
    pmod(xxhash64('cust', client_id, 'loan'),   1000000) / 1000000.0 AS u_loan,
    pmod(xxhash64('cust', client_id, 'card'),   1000000) / 1000000.0 AS u_card,
    pmod(xxhash64('cust', client_id, 'optin'),  1000000) / 1000000.0 AS u_optin
  FROM ids
),
scored AS (
  SELECT
    client_id,
    CASE WHEN u_income < 0.20 THEN 5 WHEN u_income < 0.45 THEN 4
         WHEN u_income < 0.70 THEN 3 WHEN u_income < 0.90 THEN 2 ELSE 1 END AS income_band,
    CASE WHEN u_seg < 0.45 THEN 'mass_market' WHEN u_seg < 0.72 THEN 'affluent'
         WHEN u_seg < 0.92 THEN 'premium' ELSE 'private' END                  AS segment,
    CASE WHEN u_region < 0.30 THEN 'southeast' WHEN u_region < 0.55 THEN 'south'
         WHEN u_region < 0.75 THEN 'northeast' WHEN u_region < 0.90 THEN 'centerwest'
         ELSE 'north' END                                                    AS region,
    CAST(floor(u_tenure * 120) AS INT)                                       AS tenure_months,
    CASE WHEN u_cred < 0.20 THEN 'low' WHEN u_cred < 0.75 THEN 'medium'
         ELSE 'high' END                                                     AS credit_score_band,
    u_loan < 0.28                                                            AS has_loan,
    u_card < 0.55                                                            AS has_credit_card,
    u_optin < 0.80                                                           AS email_opt_in
  FROM attrs
)
SELECT
  client_id,
  segment, region, income_band, tenure_months, credit_score_band,
  has_loan, has_credit_card, email_opt_in,
  least(0.95, greatest(0.02,
    0.30 * (income_band / 5.0)
  + 0.20 * (least(tenure_months, 120) / 120.0)
  + 0.25 * (CASE credit_score_band WHEN 'high' THEN 1.0 WHEN 'medium' THEN 0.55 ELSE 0.20 END)
  + 0.25 * (CASE segment WHEN 'private' THEN 1.0 WHEN 'premium' THEN 0.80
                        WHEN 'affluent' THEN 0.55 ELSE 0.30 END)
  )) AS propensity
FROM scored;

-- ---------------------------------------------------------------------------
-- bronze.campaigns (20 campaigns, 3 products x 4 channels, Sep-Dec 2026)
-- contact_rate  = fraction of customers reached
-- target_segment= intended audience ('all' = untargeted)
-- conversion_value = expected revenue per conversion (BRL) — feeds ROI mart
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.bronze.campaigns AS
SELECT * FROM (VALUES
  ( 1, 'credit_preapproved', 'email',    'premium',   DATE'2026-09-01', DATE'2026-09-30',  50000.00, 0.12, 0.45, 2500.00),
  ( 2, 'credit_preapproved', 'sms',      'affluent',  DATE'2026-09-05', DATE'2026-10-05',  35000.00, 0.22, 0.38, 2500.00),
  ( 3, 'credit_preapproved', 'push',     'all',       DATE'2026-10-01', DATE'2026-10-31',  28000.00, 0.05, 0.52, 2500.00),
  ( 4, 'premium_account',    'email',    'private',   DATE'2026-09-10', DATE'2026-10-10',  80000.00, 0.15, 0.40, 1800.00),
  ( 5, 'premium_account',    'whatsapp', 'premium',   DATE'2026-09-15', DATE'2026-10-15',  60000.00, 0.30, 0.35, 1800.00),
  ( 6, 'premium_account',    'sms',      'affluent',  DATE'2026-10-05', DATE'2026-11-05',  42000.00, 0.22, 0.30, 1800.00),
  ( 7, 'investment',         'email',    'private',   DATE'2026-09-20', DATE'2026-10-20',  90000.00, 0.15, 0.42, 5000.00),
  ( 8, 'investment',         'push',     'all',       DATE'2026-10-10', DATE'2026-11-10',  30000.00, 0.05, 0.48, 5000.00),
  ( 9, 'investment',         'email',    'premium',   DATE'2026-11-01', DATE'2026-11-28',  75000.00, 0.15, 0.36, 5000.00),
  (10, 'credit_preapproved', 'email',    'mass_market', DATE'2026-11-05', DATE'2026-11-30', 55000.00, 0.12, 0.50, 2500.00),
  (11, 'premium_account',    'push',     'all',       DATE'2026-11-10', DATE'2026-11-30',  25000.00, 0.05, 0.44, 1800.00),
  (12, 'investment',         'whatsapp', 'private',   DATE'2026-11-12', DATE'2026-11-30',  40000.00, 0.30, 0.28, 5000.00),
  (13, 'credit_preapproved', 'whatsapp', 'affluent',  DATE'2026-09-08', DATE'2026-10-08',  48000.00, 0.30, 0.33, 2500.00),
  (14, 'premium_account',    'email',    'mass_market', DATE'2026-10-12', DATE'2026-11-12', 52000.00, 0.15, 0.46, 1800.00),
  (15, 'investment',         'sms',      'premium',   DATE'2026-10-18', DATE'2026-11-18',  38000.00, 0.22, 0.31, 5000.00),
  (16, 'credit_preapproved', 'push',     'private',   DATE'2026-11-02', DATE'2026-11-30',  22000.00, 0.05, 0.55, 2500.00),
  (17, 'premium_account',    'whatsapp', 'all',       DATE'2026-09-22', DATE'2026-10-22',  58000.00, 0.30, 0.41, 1800.00),
  (18, 'investment',         'email',    'affluent',  DATE'2026-10-25', DATE'2026-11-25',  66000.00, 0.15, 0.39, 5000.00),
  (19, 'credit_preapproved', 'sms',      'all',       DATE'2026-11-08', DATE'2026-11-30',  30000.00, 0.22, 0.47, 2500.00),
  (20, 'investment',         'push',     'mass_market', DATE'2026-11-15', DATE'2026-11-30', 19000.00, 0.05, 0.34, 5000.00)
) AS t(campaign_id, product, channel, target_segment, start_date, end_date,
       budget, cost_per_send, contact_rate, conversion_value);

-- ---------------------------------------------------------------------------
-- bronze.campaign_events (~300k rows)
-- Same funnel model as POC + contact bias toward target_segment (~1.5x rate)
-- Mess: ~2% duplicate rows, ~1% NULL channel, ~3% NULL creative_variant,
-- NULL event_cost on non-'sent' stages.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.bronze.campaign_events AS
WITH contacts AS (
  SELECT
    c.client_id, c.segment, c.income_band, c.credit_score_band, c.propensity,
    m.campaign_id, m.product, m.channel AS campaign_channel,
    m.target_segment, m.start_date AS campaign_start, m.end_date AS campaign_end,
    m.cost_per_send, m.contact_rate, m.conversion_value,
    pmod(xxhash64('contact', c.client_id, m.campaign_id), 1000) / 1000.0 AS u_contact,
    pmod(xxhash64('ev', c.client_id, m.campaign_id, 'del'),   1000000) / 1000000.0 AS u_del,
    pmod(xxhash64('ev', c.client_id, m.campaign_id, 'open'),  1000000) / 1000000.0 AS u_open,
    pmod(xxhash64('ev', c.client_id, m.campaign_id, 'clk'),   1000000) / 1000000.0 AS u_clk,
    pmod(xxhash64('ev', c.client_id, m.campaign_id, 'conv'),  1000000) / 1000000.0 AS u_conv,
    pmod(xxhash64('ev', c.client_id, m.campaign_id, 'opt'),   1000000) / 1000000.0 AS u_opt,
    pmod(xxhash64('ev', c.client_id, m.campaign_id, 'day'),   1000000) / 1000000.0 AS u_day,
    pmod(xxhash64('ev', c.client_id, m.campaign_id, 'hour'),  1000000) / 1000000.0 AS u_hour,
    pmod(xxhash64('ev', c.client_id, m.campaign_id, 'vnull'), 1000000) / 1000000.0 AS u_vnull,
    pmod(xxhash64('ev', c.client_id, m.campaign_id, 'vnum'),  1000000) / 1000000.0 AS u_vnum,
    pmod(xxhash64('ev', c.client_id, m.campaign_id, 'cnull'), 1000000) / 1000000.0 AS u_cnull
  FROM workspace.bronze.customers c
  JOIN workspace.bronze.campaigns m
),
eligible AS (
  -- target_segment bias: matching customers ~1.5x more likely to be contacted
  SELECT * FROM contacts
  WHERE u_contact < least(0.95,
    contact_rate * CASE
      WHEN target_segment <> 'all' AND segment = target_segment THEN 1.5
      ELSE 1.0 END)
),
flags AS (
  -- per-stage success draws; staged nests them into a real funnel
  SELECT
    *,
    u_del  < 0.97                         AS p_delivered,
    u_open < (0.20 + 0.50 * propensity)   AS p_opened,
    u_clk  < (0.10 + 0.35 * propensity)   AS p_clicked,
    u_conv < (0.015 + 0.085 * propensity) AS p_converted,
    u_opt  < 0.02                         AS p_opted_out
  FROM eligible
),
staged AS (
  SELECT
    *,
    p_delivered                                        AS reached_delivered,
    p_delivered AND p_opened                           AS reached_opened,
    p_delivered AND p_opened AND p_clicked             AS reached_clicked,
    p_delivered AND p_opened AND p_clicked
      AND p_converted                                  AS reached_converted,
    p_opted_out                                        AS reached_opted_out,
    -- ~1% NULL channel (silver must recover it from the campaign)
    CASE WHEN u_cnull < 0.01 THEN NULL ELSE campaign_channel END AS channel,
    CASE WHEN pmod(xxhash64('ev', client_id, campaign_id, 'vnull'), 1000) < 30
         THEN NULL
         ELSE CAST(floor(u_vnum * 3) AS INT) END AS creative_variant,
    CASE WHEN pmod(xxhash64('ev', client_id, campaign_id, 'vnull'), 1000) < 30
         THEN NULL
         ELSE CASE product
           WHEN 'credit_preapproved' THEN CASE
             WHEN floor(u_vnum * 3) = 0 THEN 'Credito pre-aprovado: ate R$ 50 mil, taxa a partir de 1,9% a.m.'
             WHEN floor(u_vnum * 3) = 1 THEN 'Voce ja tem credito pre-aprovado. Simule em 2 minutos.'
             ELSE 'Oferta exclusiva: credito pessoal com aprovacao instantanea.' END
           WHEN 'premium_account' THEN CASE
             WHEN floor(u_vnum * 3) = 0 THEN 'Conta Premium: zero anuidade, sala VIP e concierge exclusivo.'
             WHEN floor(u_vnum * 3) = 1 THEN 'Eleve seu banco: pacote premium para quem quer mais.'
             ELSE 'Convite especial: beneficios premium por tempo limitado.' END
           ELSE CASE
             WHEN floor(u_vnum * 3) = 0 THEN 'Invista a partir de R$ 100: CDB com liquidez diaria acima do CDI.'
             WHEN floor(u_vnum * 3) = 1 THEN 'Seu dinheiro trabalhando: conheca nossa plataforma de investimentos.'
             ELSE 'Oferta relampago: taxa promocional em CDB para novos investidores.' END
         END END AS creative_text,
    CAST(concat(
      date_add(campaign_start, CAST(floor(u_day * 21) AS INT)),
      ' ',
      lpad(CAST(8 + CAST(floor(u_hour * 12) AS INT) AS STRING), 2, '0'),
      ':00:00'
    ) AS TIMESTAMP) AS event_ts
  FROM flags
),
events AS (
  SELECT concat('ev-', campaign_id, '-', client_id, '-', 'sent') AS event_id,
         client_id, campaign_id, 'sent' AS event_type, event_ts,
         channel, product, creative_variant, creative_text,
         cost_per_send AS event_cost
  FROM staged
  UNION ALL
  SELECT concat('ev-', campaign_id, '-', client_id, '-', 'delivered'),
         client_id, campaign_id, 'delivered', event_ts,
         channel, product, creative_variant, creative_text, NULL
  FROM staged WHERE reached_delivered
  UNION ALL
  SELECT concat('ev-', campaign_id, '-', client_id, '-', 'opened'),
         client_id, campaign_id, 'opened', event_ts,
         channel, product, creative_variant, creative_text, NULL
  FROM staged WHERE reached_opened
  UNION ALL
  SELECT concat('ev-', campaign_id, '-', client_id, '-', 'clicked'),
         client_id, campaign_id, 'clicked', event_ts,
         channel, product, creative_variant, creative_text, NULL
  FROM staged WHERE reached_clicked
  UNION ALL
  SELECT concat('ev-', campaign_id, '-', client_id, '-', 'converted'),
         client_id, campaign_id, 'converted', event_ts,
         channel, product, creative_variant, creative_text, NULL
  FROM staged WHERE reached_converted
  UNION ALL
  SELECT concat('ev-', campaign_id, '-', client_id, '-', 'opted_out'),
         client_id, campaign_id, 'opted_out', event_ts,
         channel, product, creative_variant, creative_text, NULL
  FROM staged WHERE reached_opted_out
)
SELECT * FROM events
UNION ALL
-- ~2% exact duplicates (the mess silver must clean)
SELECT * FROM events
WHERE pmod(xxhash64(event_id, 'dup'), 1000) < 20;
