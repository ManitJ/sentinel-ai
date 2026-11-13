-- =====================================================================================
-- Sentinel AI · SQL Analytics Showcase
-- 01_monthly_trends.sql — monthly complaint volume, growth and seasonality
-- =====================================================================================
-- Business questions
--   * How has monthly credit-reporting complaint volume developed since January 2025?
--   * How fast is it growing month over month, year over year and by quarter?
--   * Which months were the highest and lowest, and how do the two years compare?
--
-- Sources (all EXACT CFPB population counts)
--   workspace.sentinel.gold_monthly_volume, workspace.sentinel.gold_response_monthly
--
-- Metric definitions
--   population_complaints  exact complaints received in the month (CFPB API totals)
--   mom_pct / yoy_pct      percentage change vs previous month / same month a year earlier
--                          (NULL where no comparison month exists; YoY exists for 2026 only)
--   rolling_3m_avg         mean of the month and the two months before it (NULL before Mar 2025)
--   response_data_provisional
--                          TRUE when >= 1% of the month's complaints are still 'In progress'.
--                          A REPORTING RULE, not proof that other months are complete.
--
-- Caveats
--   * Volume is counted at intake, so provisional months remain usable for volume, but the
--     reference snapshot (2026-11-12) can still be revised by later CFPB updates.
--   * 2026 covers January–October only: compare averages or matched months, not annual totals.
--   * Complaint counts are consumer submissions; they are not verified findings of wrongdoing.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Q1.1  Monthly volume, growth and rank
-- -------------------------------------------------------------------------------------
WITH response_status AS (
  SELECT v.month_label,
         COALESCE(ip.complaints, 0) / v.population_complaints          AS in_progress_share,
         COALESCE(ip.complaints, 0) / v.population_complaints >= 0.01  AS response_data_provisional
  FROM workspace.sentinel.gold_monthly_volume AS v
  LEFT JOIN workspace.sentinel.gold_response_monthly AS ip
    ON ip.month_label = v.month_label
   AND ip.dimension = 'company_response'
   AND ip.category = 'In progress'
)
SELECT v.month,
       v.month_label,
       v.population_complaints,
       v.mom_change,
       ROUND(100 * v.mom_pct_change, 1)                                     AS mom_pct,
       v.yoy_change,
       ROUND(100 * v.yoy_pct_change, 1)                                     AS yoy_pct,
       ROUND(v.rolling_3m_avg, 0)                                           AS rolling_3m_avg,
       RANK() OVER (ORDER BY v.population_complaints DESC)                  AS volume_rank,
       rs.response_data_provisional
FROM workspace.sentinel.gold_monthly_volume AS v
JOIN response_status AS rs ON rs.month_label = v.month_label
ORDER BY v.month_label;


-- -------------------------------------------------------------------------------------
-- Q1.2  Quarterly volume and quarter-over-quarter growth (complete quarters only)
-- -------------------------------------------------------------------------------------
-- A quarter is 'complete' when all three months are in the data. QoQ growth is shown only
-- when both the quarter and the previous quarter are complete (Q4 2026 has only October).
WITH quarters AS (
  SELECT CONCAT(CAST(YEAR(month) AS STRING), '-Q', CAST(QUARTER(month) AS STRING)) AS quarter_label,
         MIN(month)                  AS quarter_start,
         COUNT(*)                    AS months_in_data,
         SUM(population_complaints)  AS complaints
  FROM workspace.sentinel.gold_monthly_volume
  GROUP BY YEAR(month), QUARTER(month)
),
with_previous AS (
  SELECT q.*,
         LAG(complaints)     OVER (ORDER BY quarter_start) AS prev_complaints,
         LAG(months_in_data) OVER (ORDER BY quarter_start) AS prev_months_in_data
  FROM quarters AS q
)
SELECT quarter_label,
       months_in_data,
       complaints,
       CASE WHEN months_in_data = 3 THEN 'complete' ELSE 'partial quarter' END AS quarter_status,
       CASE WHEN months_in_data = 3 AND prev_months_in_data = 3
            THEN complaints - prev_complaints END                                AS qoq_change,
       CASE WHEN months_in_data = 3 AND prev_months_in_data = 3
            THEN ROUND(100 * (complaints - prev_complaints) / prev_complaints, 1) END AS qoq_pct
FROM with_previous
ORDER BY quarter_start;


-- -------------------------------------------------------------------------------------
-- Q1.3  Year comparison: average monthly volume, peak and lowest month
-- -------------------------------------------------------------------------------------
-- Averages (not totals) make 2025 (12 months) and 2026 (10 months) comparable.
SELECT YEAR(month)                                         AS calendar_year,
       COUNT(*)                                            AS months_in_data,
       SUM(population_complaints)                          AS complaints,
       ROUND(AVG(population_complaints), 0)                AS avg_monthly_complaints,
       MAX_BY(month_label, population_complaints)          AS peak_month,
       MAX(population_complaints)                          AS peak_month_complaints,
       MIN_BY(month_label, population_complaints)          AS lowest_month,
       MIN(population_complaints)                          AS lowest_month_complaints
FROM workspace.sentinel.gold_monthly_volume
GROUP BY YEAR(month)
ORDER BY calendar_year;
