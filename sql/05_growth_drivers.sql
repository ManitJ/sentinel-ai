-- =====================================================================================
-- Sentinel AI · SQL Analytics Showcase
-- 05_growth_drivers.sql — how much did complaints grow, and what drove the change?
-- =====================================================================================
-- Business questions
--   * How much did year-to-date complaint volume change versus the same months last year?
--   * Which companies, issue groups and states account for that net change?
--
-- Sources (all EXACT CFPB population counts)
--   gold_monthly_volume, gold_company_monthly, gold_issue_monthly, gold_state_monthly
--
-- Periods (derived from the data)
--   current = January .. latest month of the latest year (currently Jan–Oct 2026)
--   prior   = the same months one year earlier            (currently Jan–Oct 2025)
--
-- Metric definitions — two different questions, never mixed:
--   pct_growth              (current - prior) / prior for ONE segment: how fast that segment
--                           grew. NULL when prior < 100 complaints (unstable base).
--   net_change_complaints   current - prior, in complaints.
--   contribution_to_net_change_pct
--                           segment net change / TOTAL net change x 100: how much of the
--                           overall change the segment accounts for. Contributions of all
--                           segments sum to 100%; individual values can be negative or
--                           exceed 100% when other segments moved the opposite way.
--   A small segment can have a high pct_growth but a tiny contribution, and vice versa.
--
-- Caveats
--   * Every breakdown includes all segments (companies with no complaints in one period
--     count as 0; complaints with no state are a 'no state given' segment), so
--     contributions reconcile exactly to the total change.
--   * Company names are CFPB labels; related entities are not merged.
--   * Net change is not a measure of company conduct; volumes are not size-adjusted.
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- Q5.1  Headline: year-to-date vs the same months a year earlier
-- -------------------------------------------------------------------------------------
WITH bounds AS (
  SELECT MAX(month) AS latest_month, CAST(DATE_TRUNC('YEAR', MAX(month)) AS DATE) AS current_start
  FROM workspace.sentinel.gold_monthly_volume
)
SELECT DATE_FORMAT(ADD_MONTHS(b.current_start, -12), 'yyyy-MM') || ' to ' ||
         DATE_FORMAT(ADD_MONTHS(b.latest_month, -12), 'yyyy-MM')                     AS prior_period,
       DATE_FORMAT(b.current_start, 'yyyy-MM') || ' to ' ||
         DATE_FORMAT(b.latest_month, 'yyyy-MM')                                      AS current_period,
       SUM(CASE WHEN v.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)
                THEN v.population_complaints ELSE 0 END)                             AS prior_complaints,
       SUM(CASE WHEN v.month BETWEEN b.current_start AND b.latest_month
                THEN v.population_complaints ELSE 0 END)                             AS current_complaints,
       SUM(CASE WHEN v.month BETWEEN b.current_start AND b.latest_month
                THEN v.population_complaints ELSE 0 END)
       - SUM(CASE WHEN v.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)
                  THEN v.population_complaints ELSE 0 END)                           AS net_change_complaints,
       ROUND(100 * (SUM(CASE WHEN v.month BETWEEN b.current_start AND b.latest_month
                             THEN v.population_complaints ELSE 0 END)
                    / SUM(CASE WHEN v.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)
                               THEN v.population_complaints ELSE 0 END) - 1), 1)     AS pct_growth
FROM workspace.sentinel.gold_monthly_volume AS v
CROSS JOIN bounds AS b
GROUP BY b.current_start, b.latest_month;


-- -------------------------------------------------------------------------------------
-- Q5.2  Company contributions to the net change (top 15 by absolute net change)
-- -------------------------------------------------------------------------------------
WITH bounds AS (
  SELECT MAX(month) AS latest_month, CAST(DATE_TRUNC('YEAR', MAX(month)) AS DATE) AS current_start
  FROM workspace.sentinel.gold_monthly_volume
),
segments AS (
  SELECT c.company AS segment,
         SUM(CASE WHEN c.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)
                  THEN c.complaints ELSE 0 END) AS prior_complaints,
         SUM(CASE WHEN c.month BETWEEN b.current_start AND b.latest_month
                  THEN c.complaints ELSE 0 END) AS current_complaints
  FROM workspace.sentinel.gold_company_monthly AS c
  CROSS JOIN bounds AS b
  GROUP BY c.company
),
scored AS (
  SELECT segment, prior_complaints, current_complaints,
         current_complaints - prior_complaints                              AS net_change_complaints,
         SUM(current_complaints - prior_complaints) OVER ()                 AS total_net_change
  FROM segments
)
SELECT segment                                                              AS company,
       prior_complaints,
       current_complaints,
       net_change_complaints,
       CASE WHEN prior_complaints >= 100
            THEN ROUND(100 * net_change_complaints / prior_complaints, 1) END AS pct_growth,
       ROUND(100 * net_change_complaints / NULLIF(total_net_change, 0), 2) AS contribution_to_net_change_pct
FROM scored
ORDER BY ABS(net_change_complaints) DESC, segment
LIMIT 15;


-- -------------------------------------------------------------------------------------
-- Q5.3  Issue-group contributions to the net change (all groups)
-- -------------------------------------------------------------------------------------
WITH bounds AS (
  SELECT MAX(month) AS latest_month, CAST(DATE_TRUNC('YEAR', MAX(month)) AS DATE) AS current_start
  FROM workspace.sentinel.gold_monthly_volume
),
segments AS (
  SELECT COALESCE(i.issue_group, 'no issue given') AS segment,
         SUM(CASE WHEN i.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)
                  THEN i.complaints ELSE 0 END) AS prior_complaints,
         SUM(CASE WHEN i.month BETWEEN b.current_start AND b.latest_month
                  THEN i.complaints ELSE 0 END) AS current_complaints
  FROM workspace.sentinel.gold_issue_monthly AS i
  CROSS JOIN bounds AS b
  GROUP BY COALESCE(i.issue_group, 'no issue given')
),
scored AS (
  SELECT segment, prior_complaints, current_complaints,
         current_complaints - prior_complaints              AS net_change_complaints,
         SUM(current_complaints - prior_complaints) OVER () AS total_net_change
  FROM segments
)
SELECT segment                                                              AS issue_group,
       prior_complaints,
       current_complaints,
       net_change_complaints,
       CASE WHEN prior_complaints >= 100
            THEN ROUND(100 * net_change_complaints / prior_complaints, 1) END AS pct_growth,
       ROUND(100 * net_change_complaints / NULLIF(total_net_change, 0), 2) AS contribution_to_net_change_pct
FROM scored
ORDER BY net_change_complaints DESC;


-- -------------------------------------------------------------------------------------
-- Q5.4  State contributions to the net change (top 15 by absolute net change)
-- -------------------------------------------------------------------------------------
WITH bounds AS (
  SELECT MAX(month) AS latest_month, CAST(DATE_TRUNC('YEAR', MAX(month)) AS DATE) AS current_start
  FROM workspace.sentinel.gold_monthly_volume
),
segments AS (
  SELECT COALESCE(s.state_code, 'no state given') AS segment,
         MAX(COALESCE(s.state_category, 'none'))   AS state_category,
         SUM(CASE WHEN s.month BETWEEN ADD_MONTHS(b.current_start, -12) AND ADD_MONTHS(b.latest_month, -12)
                  THEN s.complaints ELSE 0 END) AS prior_complaints,
         SUM(CASE WHEN s.month BETWEEN b.current_start AND b.latest_month
                  THEN s.complaints ELSE 0 END) AS current_complaints
  FROM workspace.sentinel.gold_state_monthly AS s
  CROSS JOIN bounds AS b
  GROUP BY COALESCE(s.state_code, 'no state given')
),
scored AS (
  SELECT segment, state_category, prior_complaints, current_complaints,
         current_complaints - prior_complaints              AS net_change_complaints,
         SUM(current_complaints - prior_complaints) OVER () AS total_net_change
  FROM segments
)
SELECT segment                                                              AS state_code,
       state_category,
       prior_complaints,
       current_complaints,
       net_change_complaints,
       CASE WHEN prior_complaints >= 100
            THEN ROUND(100 * net_change_complaints / prior_complaints, 1) END AS pct_growth,
       ROUND(100 * net_change_complaints / NULLIF(total_net_change, 0), 2) AS contribution_to_net_change_pct
FROM scored
ORDER BY ABS(net_change_complaints) DESC, segment
LIMIT 15;
