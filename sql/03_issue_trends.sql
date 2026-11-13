-- =====================================================================================
-- Sentinel AI · SQL Analytics Showcase
-- 03_issue_trends.sql — what consumers complain about and how the mix is shifting
-- =====================================================================================
-- Business questions
--   * Which credit-reporting issues account for the most complaints?
--   * How is the issue mix changing month to month and year over year?
--   * Within issues, which specific sub-issues are most common (estimated)?
--
-- Sources
--   EXACT     : gold_issue_monthly, gold_issue_category_map, gold_monthly_volume
--   ESTIMATED : gold_sub_issue_estimates (weighted stratified sample, 95% CIs)
--
-- Metric definitions
--   issue          original CFPB issue label (always preserved)
--   issue_group    documented analysis grouping: the only combined group joins
--                  "...investigation into an existing problem" and "...existing issue"
--                  (both labels occur every month; see gold_issue_category_map.note)
--   pct_of_complaints  issue complaints / all complaints in the same months (pooled counts)
--   share_change_pct_points  difference between two shares, in percentage points
--   est_share / ci95_*        estimated share of ALL complaints for an issue > sub-issue
--
-- Caveats
--   * Issue and sub-issue are recorded at intake, so provisional months are usable here,
--     but snapshot counts can still be revised by later CFPB updates.
--   * Year-over-year comparisons use matched months (Jan–latest month of each year).
--   * Sub-issue figures are estimates: use only is_reliable rows and report the 95% CI.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Q3.1  Issues ranked by complaints, Jan 2025 – Oct 2026 (exact, original labels)
-- -------------------------------------------------------------------------------------
WITH all_complaints AS (
  SELECT SUM(population_complaints) AS total FROM workspace.sentinel.gold_monthly_volume
)
SELECT RANK() OVER (ORDER BY SUM(i.complaints) DESC) AS issue_rank,
       i.issue,
       i.issue_group,
       i.is_combined_group,
       SUM(i.complaints)                              AS complaints,
       ROUND(100 * SUM(i.complaints) / MAX(a.total), 2) AS pct_of_complaints
FROM workspace.sentinel.gold_issue_monthly AS i
CROSS JOIN all_complaints AS a
WHERE NOT i.is_residual
GROUP BY i.issue, i.issue_group, i.is_combined_group
ORDER BY complaints DESC;


-- -------------------------------------------------------------------------------------
-- Q3.2  Monthly issue mix by issue group (exact)
-- -------------------------------------------------------------------------------------
SELECT month_label,
       issue_group,
       SUM(complaints)                                                       AS complaints,
       ROUND(100 * SUM(share_of_month), 2)                                   AS pct_of_month,
       RANK() OVER (PARTITION BY month_label ORDER BY SUM(complaints) DESC)  AS rank_in_month
FROM workspace.sentinel.gold_issue_monthly
WHERE NOT is_residual
GROUP BY month_label, issue_group
ORDER BY month_label, rank_in_month;


-- -------------------------------------------------------------------------------------
-- Q3.3  Issue-mix shift: year-to-date vs the same months a year earlier (exact)
-- -------------------------------------------------------------------------------------
WITH bounds AS (
  SELECT MAX(month)                         AS latest_month,
         DATE_TRUNC('YEAR', MAX(month))     AS current_start
  FROM workspace.sentinel.gold_monthly_volume
),
periods AS (
  SELECT 'current' AS period, CAST(current_start AS DATE) AS start_month, latest_month AS end_month FROM bounds
  UNION ALL
  SELECT 'prior', ADD_MONTHS(CAST(current_start AS DATE), -12), ADD_MONTHS(latest_month, -12) FROM bounds
),
issue_period AS (
  SELECT p.period, i.issue_group, SUM(i.complaints) AS complaints
  FROM workspace.sentinel.gold_issue_monthly AS i
  JOIN periods AS p ON i.month BETWEEN p.start_month AND p.end_month
  WHERE NOT i.is_residual
  GROUP BY p.period, i.issue_group
),
period_totals AS (
  SELECT p.period, SUM(v.population_complaints) AS total
  FROM workspace.sentinel.gold_monthly_volume AS v
  JOIN periods AS p ON v.month BETWEEN p.start_month AND p.end_month
  GROUP BY p.period
)
SELECT g.issue_group,
       COALESCE(pr.complaints, 0)                                          AS prior_complaints,
       COALESCE(cu.complaints, 0)                                          AS current_complaints,
       ROUND(100 * COALESCE(pr.complaints, 0) / tp.total, 2)               AS prior_pct_of_complaints,
       ROUND(100 * COALESCE(cu.complaints, 0) / tc.total, 2)               AS current_pct_of_complaints,
       ROUND(100 * (COALESCE(cu.complaints, 0) / tc.total
                    - COALESCE(pr.complaints, 0) / tp.total), 2)           AS share_change_pct_points
FROM (SELECT DISTINCT issue_group FROM issue_period) AS g
LEFT JOIN issue_period AS pr ON pr.issue_group = g.issue_group AND pr.period = 'prior'
LEFT JOIN issue_period AS cu ON cu.issue_group = g.issue_group AND cu.period = 'current'
CROSS JOIN (SELECT total FROM period_totals WHERE period = 'prior')   AS tp
CROSS JOIN (SELECT total FROM period_totals WHERE period = 'current') AS tc
ORDER BY current_complaints DESC;


-- -------------------------------------------------------------------------------------
-- Q3.4  Most common issue > sub-issue combinations, Jan 2025 – Oct 2026
--       (ESTIMATED, reliable only, with 95% CI)
-- -------------------------------------------------------------------------------------
SELECT issue,
       COALESCE(sub_issue, '(no sub-issue)')  AS sub_issue,
       ROUND(100 * est_share, 2)              AS est_pct_of_complaints,
       ROUND(100 * ci95_low, 2)               AS ci95_low_pct,
       ROUND(100 * ci95_high, 2)              AS ci95_high_pct,
       n_category_sample                      AS sample_records,
       measure_type
FROM workspace.sentinel.gold_sub_issue_estimates
WHERE period = 'ALL'
  AND is_reliable
ORDER BY est_share DESC
LIMIT 15;
