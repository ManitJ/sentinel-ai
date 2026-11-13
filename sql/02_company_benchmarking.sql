-- =====================================================================================
-- Sentinel AI · SQL Analytics Showcase
-- 02_company_benchmarking.sql — which companies receive the most complaints, how
-- concentrated complaints are, and how companies' response profiles compare
-- =====================================================================================
-- Business questions
--   * Which companies received the most credit-reporting complaints in Jan 2025 – Oct 2026?
--   * How concentrated are complaints among the largest companies, and is that changing?
--   * How have the leading companies' ranks and complaint shares moved year over year?
--   * How do leading companies' complaint outcomes compare (estimated, complete months only)?
--
-- Sources
--   EXACT     : gold_company_monthly, gold_monthly_volume
--   ESTIMATED : gold_company_response_estimates (weighted stratified sample, 95% CIs)
--
-- Metric definitions
--   pct_of_all_complaints  company complaints / all complaints over the same months
--                          (pooled: SUM of counts / SUM of totals, never an average of shares)
--   complaint_concentration_hhi
--                          Herfindahl–Hirschman index of COMPLAINT concentration across companies,
--                          10,000 x SUM(share_of_month^2). It measures how concentrated complaints
--                          are among companies. It is NOT market share and says nothing about the
--                          companies' size in the credit-reporting market.
--   top3_complaint_pct     share of the month's complaints received by its three largest companies
--   est_share / ci95_*     estimated share of a company's complaints with a given outcome and its
--                          95% confidence interval (Wilson). Only rows with is_reliable = TRUE.
--
-- Caveats
--   * Company names are CFPB's labels; related entities are not merged.
--   * Complaint counts reflect consumer submissions, not verified misconduct, and are not
--     adjusted for company size or number of customers.
--   * Response estimates use period '2025' only: the 'ALL' and '2026-YTD' periods include
--     September–October 2026, whose response data is provisional (>= 1% still 'In progress').
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Q2.1  Top 10 companies by complaints, Jan 2025 – Oct 2026 (exact)
-- -------------------------------------------------------------------------------------
WITH all_complaints AS (
  SELECT SUM(population_complaints) AS total FROM workspace.sentinel.gold_monthly_volume
),
company_totals AS (
  SELECT company,
         SUM(complaints) AS complaints,
         COUNT(*)        AS months_with_complaints
  FROM workspace.sentinel.gold_company_monthly
  WHERE NOT is_residual
  GROUP BY company
),
ranked AS (
  SELECT c.*,
         ROW_NUMBER() OVER (ORDER BY c.complaints DESC, c.company)                        AS company_rank,
         SUM(c.complaints) OVER (ORDER BY c.complaints DESC, c.company
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)         AS cumulative_complaints
  FROM company_totals AS c
)
SELECT r.company_rank,
       r.company,
       r.complaints,
       r.months_with_complaints,
       ROUND(100 * r.complaints / a.total, 2)            AS pct_of_all_complaints,
       ROUND(100 * r.cumulative_complaints / a.total, 2) AS cumulative_pct_of_all_complaints
FROM ranked AS r
CROSS JOIN all_complaints AS a
WHERE r.company_rank <= 10
ORDER BY r.company_rank;


-- -------------------------------------------------------------------------------------
-- Q2.2  Complaint concentration by month (exact) — HHI is complaint concentration,
--       NOT market share
-- -------------------------------------------------------------------------------------
SELECT month_label,
       COUNT(*)                                                                AS companies_with_complaints,
       ROUND(100 * MAX(share_of_month), 2)                                     AS top1_complaint_pct,
       ROUND(100 * SUM(CASE WHEN rank_in_month <= 3 THEN share_of_month ELSE 0 END), 2)
                                                                               AS top3_complaint_pct,
       ROUND(10000 * SUM(share_of_month * share_of_month), 0)                  AS complaint_concentration_hhi
FROM workspace.sentinel.gold_company_monthly
WHERE NOT is_residual
GROUP BY month_label
ORDER BY month_label;


-- -------------------------------------------------------------------------------------
-- Q2.3  Leading companies: latest month vs the same month a year earlier (exact)
-- -------------------------------------------------------------------------------------
-- Same-month comparison avoids seasonal differences. Months are derived from the data.
WITH latest AS (
  SELECT MAX(month) AS latest_month FROM workspace.sentinel.gold_monthly_volume
),
months AS (
  SELECT DATE_FORMAT(latest_month, 'yyyy-MM')                 AS current_label,
         DATE_FORMAT(ADD_MONTHS(latest_month, -12), 'yyyy-MM') AS prior_label
  FROM latest
),
top_companies AS (
  SELECT company
  FROM workspace.sentinel.gold_company_monthly
  WHERE NOT is_residual
  GROUP BY company
  ORDER BY SUM(complaints) DESC, company
  LIMIT 10
)
SELECT t.company,
       m.prior_label                                            AS prior_month,
       p.rank_in_month                                          AS prior_rank,
       p.complaints                                             AS prior_complaints,
       ROUND(100 * p.share_of_month, 2)                         AS prior_pct_of_complaints,
       m.current_label                                          AS current_month,
       c.rank_in_month                                          AS current_rank,
       c.complaints                                             AS current_complaints,
       ROUND(100 * c.share_of_month, 2)                         AS current_pct_of_complaints,
       COALESCE(c.complaints, 0) - COALESCE(p.complaints, 0)    AS net_change_complaints,
       ROUND(100 * (COALESCE(c.share_of_month, 0) - COALESCE(p.share_of_month, 0)), 2)
                                                                AS share_change_pct_points
FROM top_companies AS t
CROSS JOIN months AS m
LEFT JOIN workspace.sentinel.gold_company_monthly AS p
  ON p.company = t.company AND p.month_label = m.prior_label
LEFT JOIN workspace.sentinel.gold_company_monthly AS c
  ON c.company = t.company AND c.month_label = m.current_label
ORDER BY c.rank_in_month NULLS LAST, t.company;


-- -------------------------------------------------------------------------------------
-- Q2.4  Complaint outcomes of the leading companies, 2025 (ESTIMATED, reliable only)
-- -------------------------------------------------------------------------------------
-- Period '2025' contains only months whose response data meets the < 1% 'In progress'
-- reporting rule. Shares are of each company's own complaints; compare companies only
-- where confidence intervals do not overlap.
WITH top_companies AS (
  SELECT company
  FROM workspace.sentinel.gold_company_monthly
  WHERE NOT is_residual
  GROUP BY company
  ORDER BY SUM(complaints) DESC, company
  LIMIT 10
)
SELECT e.company,
       e.dimension,
       e.category,
       ROUND(100 * e.est_share, 2)  AS est_pct_of_company_complaints,
       ROUND(100 * e.ci95_low, 2)   AS ci95_low_pct,
       ROUND(100 * e.ci95_high, 2)  AS ci95_high_pct,
       e.n_domain_sample            AS company_sample_records,
       e.measure_type
FROM workspace.sentinel.gold_company_response_estimates AS e
JOIN top_companies AS t ON t.company = e.company
WHERE e.period = '2025'
  AND e.is_reliable
ORDER BY e.company, e.dimension, e.est_share DESC;
