-- =====================================================================================
-- Sentinel AI · SQL Analytics Showcase
-- 06_executive_scorecard.sql — one-page KPI view and response performance
-- =====================================================================================
-- Business questions
--   * What are the headline complaint KPIs for the latest month and year to date?
--   * How concentrated are complaints, and what dominates by company, issue and state?
--   * How did companies respond in months with complete response data?
--
-- Sources (all EXACT CFPB population counts)
--   gold_monthly_volume, gold_company_monthly, gold_issue_monthly, gold_state_monthly,
--   gold_response_monthly
--
-- Metric definitions
--   complaint_concentration_hhi  10,000 x SUM(company share^2) for the latest month —
--                                COMPLAINT concentration, NOT market share
--   response_reference_month     latest month in which < 1% of complaints are still
--                                'In progress' (reporting rule; NOT proof of completeness).
--                                All response KPIs are reported for this month only.
--   timely_response_pct          share of complaints with timely = 'Yes'
--   *_relief_pct / explanation   share of complaints by company_response category
--   ytd_pct_growth               percentage growth, year to date vs the same months a year
--                                earlier (see 05_growth_drivers.sql for net contributions)
--
-- Caveats
--   * Provisional months (>= 1% 'In progress') are excluded from every response metric:
--     their response, timeliness and channel data are incomplete.
--   * Volume, company, issue and state KPIs use the latest month (counted at intake) and
--     can still be revised by later CFPB updates.
--   * KPIs describe complaint submissions; they are not findings of wrongdoing.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Q6.1  Executive scorecard (one row)
-- -------------------------------------------------------------------------------------
WITH response_status AS (
  SELECT v.month_label,
         COALESCE(ip.complaints, 0) / v.population_complaints >= 0.01 AS is_provisional
  FROM workspace.sentinel.gold_monthly_volume AS v
  LEFT JOIN workspace.sentinel.gold_response_monthly AS ip
    ON ip.month_label = v.month_label
   AND ip.dimension = 'company_response'
   AND ip.category = 'In progress'
),
latest AS (
  SELECT * FROM workspace.sentinel.gold_monthly_volume
  WHERE month = (SELECT MAX(month) FROM workspace.sentinel.gold_monthly_volume)
),
ytd AS (
  SELECT SUM(CASE WHEN v.month BETWEEN CAST(DATE_TRUNC('YEAR', l.month) AS DATE) AND l.month
                  THEN v.population_complaints ELSE 0 END) AS current_ytd,
         SUM(CASE WHEN v.month BETWEEN ADD_MONTHS(CAST(DATE_TRUNC('YEAR', l.month) AS DATE), -12)
                                   AND ADD_MONTHS(l.month, -12)
                  THEN v.population_complaints ELSE 0 END) AS prior_ytd
  FROM workspace.sentinel.gold_monthly_volume AS v
  CROSS JOIN latest AS l
),
company_latest AS (
  SELECT MAX_BY(company, complaints)                                              AS top_company,
         ROUND(100 * MAX(share_of_month), 2)                                      AS top_company_pct,
         ROUND(100 * SUM(CASE WHEN rank_in_month <= 3 THEN share_of_month ELSE 0 END), 2) AS top3_company_pct,
         ROUND(10000 * SUM(share_of_month * share_of_month), 0)                   AS complaint_concentration_hhi
  FROM workspace.sentinel.gold_company_monthly
  WHERE NOT is_residual AND month_label = (SELECT month_label FROM latest)
),
issue_latest AS (
  SELECT MAX_BY(issue, complaints) AS top_issue, ROUND(100 * MAX(share_of_month), 2) AS top_issue_pct
  FROM workspace.sentinel.gold_issue_monthly
  WHERE NOT is_residual AND month_label = (SELECT month_label FROM latest)
),
state_latest AS (
  SELECT MAX_BY(state_code, complaints) AS top_state, ROUND(100 * MAX(share_of_month), 2) AS top_state_pct
  FROM workspace.sentinel.gold_state_monthly
  WHERE NOT is_residual AND month_label = (SELECT month_label FROM latest)
),
reference_month AS (
  SELECT MAX(month_label) AS month_label FROM response_status WHERE NOT is_provisional
),
response_kpis AS (
  SELECT
    ROUND(100 * SUM(CASE WHEN dimension = 'timely' AND category = 'Yes' THEN share_of_month ELSE 0 END), 2)
      AS timely_response_pct,
    ROUND(100 * SUM(CASE WHEN dimension = 'company_response' AND category = 'Closed with monetary relief'
                         THEN share_of_month ELSE 0 END), 2)     AS monetary_relief_pct,
    ROUND(100 * SUM(CASE WHEN dimension = 'company_response' AND category = 'Closed with non-monetary relief'
                         THEN share_of_month ELSE 0 END), 2)     AS non_monetary_relief_pct,
    ROUND(100 * SUM(CASE WHEN dimension = 'company_response' AND category = 'Closed with explanation'
                         THEN share_of_month ELSE 0 END), 2)     AS closed_with_explanation_pct
  FROM workspace.sentinel.gold_response_monthly
  WHERE month_label = (SELECT month_label FROM reference_month)
)
SELECT l.month_label                                               AS latest_month,
       l.population_complaints                                     AS latest_month_complaints,
       ROUND(100 * l.mom_pct_change, 1)                            AS mom_pct,
       ROUND(100 * l.yoy_pct_change, 1)                            AS yoy_pct,
       ROUND(l.rolling_3m_avg, 0)                                  AS rolling_3m_avg,
       y.current_ytd                                               AS ytd_complaints,
       y.prior_ytd                                                 AS prior_ytd_complaints,
       ROUND(100 * (y.current_ytd / y.prior_ytd - 1), 1)           AS ytd_pct_growth,
       c.top_company, c.top_company_pct, c.top3_company_pct, c.complaint_concentration_hhi,
       i.top_issue, i.top_issue_pct,
       s.top_state, s.top_state_pct,
       r.month_label                                               AS response_reference_month,
       k.timely_response_pct, k.monetary_relief_pct, k.non_monetary_relief_pct, k.closed_with_explanation_pct,
       (SELECT ARRAY_JOIN(ARRAY_SORT(COLLECT_LIST(month_label)), ', ')
          FROM response_status WHERE is_provisional)
                                                                   AS provisional_response_months
FROM latest AS l
CROSS JOIN ytd AS y
CROSS JOIN company_latest AS c
CROSS JOIN issue_latest AS i
CROSS JOIN state_latest AS s
CROSS JOIN reference_month AS r
CROSS JOIN response_kpis AS k;


-- -------------------------------------------------------------------------------------
-- Q6.2  Response performance by month — complete-response months only (exact)
-- -------------------------------------------------------------------------------------
-- Provisional months are excluded rather than shown with incomplete outcomes.
WITH response_status AS (
  SELECT v.month_label,
         COALESCE(ip.complaints, 0) / v.population_complaints >= 0.01 AS is_provisional
  FROM workspace.sentinel.gold_monthly_volume AS v
  LEFT JOIN workspace.sentinel.gold_response_monthly AS ip
    ON ip.month_label = v.month_label
   AND ip.dimension = 'company_response'
   AND ip.category = 'In progress'
)
SELECT r.month_label,
       ROUND(100 * SUM(CASE WHEN r.dimension = 'timely' AND r.category = 'Yes' THEN r.share_of_month ELSE 0 END), 2)
         AS timely_response_pct,
       ROUND(100 * SUM(CASE WHEN r.dimension = 'company_response' AND r.category = 'Closed with monetary relief'
                            THEN r.share_of_month ELSE 0 END), 3) AS monetary_relief_pct,
       ROUND(100 * SUM(CASE WHEN r.dimension = 'company_response' AND r.category = 'Closed with non-monetary relief'
                            THEN r.share_of_month ELSE 0 END), 2) AS non_monetary_relief_pct,
       ROUND(100 * SUM(CASE WHEN r.dimension = 'company_response' AND r.category = 'Closed with explanation'
                            THEN r.share_of_month ELSE 0 END), 2) AS closed_with_explanation_pct,
       ROUND(100 * SUM(CASE WHEN r.dimension = 'company_response' AND r.category = 'Untimely response'
                            THEN r.share_of_month ELSE 0 END), 3) AS untimely_response_pct
FROM workspace.sentinel.gold_response_monthly AS r
JOIN response_status AS s ON s.month_label = r.month_label
WHERE NOT s.is_provisional
GROUP BY r.month_label
ORDER BY r.month_label;
