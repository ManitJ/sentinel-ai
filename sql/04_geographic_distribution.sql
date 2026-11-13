-- =====================================================================================
-- Sentinel AI · SQL Analytics Showcase
-- 04_geographic_distribution.sql — where complaints come from
-- =====================================================================================
-- Business questions
--   * How are complaints distributed across states, territories and military addresses,
--     and how many complaints have no state?
--   * Which states (and DC) account for the most complaints?
--   * How has each state's complaint volume changed year to date?
--
-- Sources (all EXACT CFPB population counts)
--   workspace.sentinel.gold_state_monthly, workspace.sentinel.gold_monthly_volume
--
-- Metric definitions
--   state_category          state / district (DC) / territory / military (USPS lists);
--                           NULL = no state given (a residual row, is_residual = TRUE)
--   pct_of_all_complaints   denominator = ALL complaints, including those with no state
--   pct_of_state_and_dc     denominator = complaints from the 50 states + DC only
--   pct_growth              (current - prior) / prior; NULL when prior < 100 complaints
--                           (small bases make percentages unstable)
--   net_change_complaints   current - prior, in complaints
--
-- Caveats
--   * No population data is used: these are complaint counts, NOT per-capita rates, so large
--     states rank high partly because they are large.
--   * The state is the consumer's reported state; it is not verified.
--   * Year-to-date comparisons use matched months (January to the latest month).
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Q4.1  Coverage by location type, Jan 2025 – Oct 2026 (exact, sums to all complaints)
-- -------------------------------------------------------------------------------------
WITH all_complaints AS (
  SELECT SUM(population_complaints) AS total FROM workspace.sentinel.gold_monthly_volume
)
SELECT COALESCE(s.state_category, 'no state given')        AS location_type,
       COUNT(DISTINCT s.state)                             AS distinct_locations,
       SUM(s.complaints)                                   AS complaints,
       ROUND(100 * SUM(s.complaints) / MAX(a.total), 3)    AS pct_of_all_complaints
FROM workspace.sentinel.gold_state_monthly AS s
CROSS JOIN all_complaints AS a
GROUP BY s.state_category
ORDER BY complaints DESC;


-- -------------------------------------------------------------------------------------
-- Q4.2  States and DC ranked by complaints, Jan 2025 – Oct 2026 (exact)
-- -------------------------------------------------------------------------------------
WITH all_complaints AS (
  SELECT SUM(population_complaints) AS total FROM workspace.sentinel.gold_monthly_volume
),
state_totals AS (
  SELECT state_code, state_category, SUM(complaints) AS complaints
  FROM workspace.sentinel.gold_state_monthly
  WHERE state_category IN ('state', 'district')
  GROUP BY state_code, state_category
)
SELECT RANK() OVER (ORDER BY st.complaints DESC)                        AS state_rank,
       st.state_code,
       st.state_category,
       st.complaints,
       ROUND(100 * st.complaints / a.total, 2)                         AS pct_of_all_complaints,
       ROUND(100 * st.complaints / SUM(st.complaints) OVER (), 2)      AS pct_of_state_and_dc
FROM state_totals AS st
CROSS JOIN all_complaints AS a
ORDER BY state_rank, st.state_code;


-- -------------------------------------------------------------------------------------
-- Q4.3  Year-to-date change by state and DC (exact): net change vs percentage growth
-- -------------------------------------------------------------------------------------
WITH bounds AS (
  SELECT MAX(month) AS latest_month, CAST(DATE_TRUNC('YEAR', MAX(month)) AS DATE) AS current_start
  FROM workspace.sentinel.gold_monthly_volume
),
by_period AS (
  SELECT s.state_code,
         SUM(CASE WHEN s.month BETWEEN b.current_start AND b.latest_month THEN s.complaints ELSE 0 END)
           AS current_complaints,
         SUM(CASE WHEN s.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)
                  THEN s.complaints ELSE 0 END)
           AS prior_complaints
  FROM workspace.sentinel.gold_state_monthly AS s
  CROSS JOIN bounds AS b
  WHERE s.state_category IN ('state', 'district')
  GROUP BY s.state_code
)
SELECT state_code,
       prior_complaints,
       current_complaints,
       current_complaints - prior_complaints                                   AS net_change_complaints,
       CASE WHEN prior_complaints >= 100
            THEN ROUND(100 * (current_complaints - prior_complaints) / prior_complaints, 1) END
                                                                               AS pct_growth,
       RANK() OVER (ORDER BY current_complaints - prior_complaints DESC)       AS net_change_rank
FROM by_period
ORDER BY net_change_complaints DESC;
