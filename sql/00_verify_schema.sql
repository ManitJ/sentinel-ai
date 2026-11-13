-- =====================================================================================
-- Sentinel AI · SQL Analytics Showcase
-- 00_verify_schema.sql — read-only verification of the Gold layer before analytics
-- =====================================================================================
-- Purpose : confirm the actual Databricks schemas, row counts, month coverage and exact
--           reconciliation of the Gold tables that the analytics queries (01–06) will use.
-- Catalog : workspace.sentinel   (Databricks SQL / Unity Catalog)
-- Safety  : READ-ONLY. Only DESCRIBE and SELECT statements; nothing is created or changed.
--
-- Expected values below come from the Gold build (notebook 04), which checked these same
-- row counts against the tested Python reference before writing. A mismatch is a signal to
-- investigate, not something to "fix" in this file.
--
-- How to read the results: every check query returns one row per check with
-- `expected`, `actual` and `status` ('PASS' / 'FAIL' / 'INFO').
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- A. Schemas
-- -------------------------------------------------------------------------------------
-- A1. One consolidated result with every column of the 10 Gold tables (share this one).
SELECT table_name,
       ordinal_position,
       column_name,
       full_data_type,
       is_nullable
FROM workspace.information_schema.columns
WHERE table_schema = 'sentinel'
  AND table_name IN ('gold_monthly_volume', 'gold_company_monthly', 'gold_issue_monthly',
                     'gold_issue_category_map', 'gold_state_monthly', 'gold_response_monthly',
                     'gold_company_response_estimates', 'gold_sub_issue_estimates',
                     'gold_estimator_calibration', 'gold_insight_facts')
ORDER BY table_name, ordinal_position;

-- A2. Individual DESCRIBE TABLE statements (same information, one table at a time).
DESCRIBE TABLE workspace.sentinel.gold_monthly_volume;
DESCRIBE TABLE workspace.sentinel.gold_company_monthly;
DESCRIBE TABLE workspace.sentinel.gold_issue_monthly;
DESCRIBE TABLE workspace.sentinel.gold_issue_category_map;
DESCRIBE TABLE workspace.sentinel.gold_state_monthly;
DESCRIBE TABLE workspace.sentinel.gold_response_monthly;
DESCRIBE TABLE workspace.sentinel.gold_company_response_estimates;
DESCRIBE TABLE workspace.sentinel.gold_sub_issue_estimates;
DESCRIBE TABLE workspace.sentinel.gold_estimator_calibration;
DESCRIBE TABLE workspace.sentinel.gold_insight_facts;


-- -------------------------------------------------------------------------------------
-- B. Row counts (expected values from the Gold build's parity fingerprints)
-- -------------------------------------------------------------------------------------
WITH counts AS (
  SELECT 'gold_monthly_volume'             AS table_name, 22    AS expected, COUNT(*) AS actual FROM workspace.sentinel.gold_monthly_volume
  UNION ALL SELECT 'gold_company_monthly',             17594, COUNT(*) FROM workspace.sentinel.gold_company_monthly
  UNION ALL SELECT 'gold_issue_monthly',               182,   COUNT(*) FROM workspace.sentinel.gold_issue_monthly
  UNION ALL SELECT 'gold_issue_category_map',          13,    COUNT(*) FROM workspace.sentinel.gold_issue_category_map
  UNION ALL SELECT 'gold_state_monthly',               1326,  COUNT(*) FROM workspace.sentinel.gold_state_monthly
  UNION ALL SELECT 'gold_response_monthly',            538,   COUNT(*) FROM workspace.sentinel.gold_response_monthly
  UNION ALL SELECT 'gold_company_response_estimates',  5453,  COUNT(*) FROM workspace.sentinel.gold_company_response_estimates
  UNION ALL SELECT 'gold_sub_issue_estimates',         713,   COUNT(*) FROM workspace.sentinel.gold_sub_issue_estimates
  UNION ALL SELECT 'gold_estimator_calibration',       19640, COUNT(*) FROM workspace.sentinel.gold_estimator_calibration
  UNION ALL SELECT 'gold_insight_facts',               4284,  COUNT(*) FROM workspace.sentinel.gold_insight_facts
  -- Not a row count: total complaints across the 22 months (exact API totals).
  UNION ALL SELECT 'population_total (SUM of gold_monthly_volume.population_complaints)',
                   10500160, SUM(population_complaints) FROM workspace.sentinel.gold_monthly_volume
)
SELECT table_name, expected, actual,
       CASE WHEN expected = actual THEN 'PASS' ELSE 'FAIL' END AS status
FROM counts
ORDER BY table_name;


-- -------------------------------------------------------------------------------------
-- C. Month coverage per table (expected: 22 months, 2025-01 to 2026-10)
-- -------------------------------------------------------------------------------------
-- Estimate tables use `period` instead of months: 'ALL', '2025', '2026-YTD' (company
-- estimates) and 'ALL' + 22 monthly periods (sub-issue estimates).
WITH coverage AS (
  SELECT 'gold_monthly_volume' AS table_name, MIN(month_label) AS first_period, MAX(month_label) AS last_period,
         COUNT(DISTINCT month_label) AS distinct_periods, 22 AS expected_periods
  FROM workspace.sentinel.gold_monthly_volume
  UNION ALL SELECT 'gold_company_monthly',  MIN(month_label), MAX(month_label), COUNT(DISTINCT month_label), 22 FROM workspace.sentinel.gold_company_monthly
  UNION ALL SELECT 'gold_issue_monthly',    MIN(month_label), MAX(month_label), COUNT(DISTINCT month_label), 22 FROM workspace.sentinel.gold_issue_monthly
  UNION ALL SELECT 'gold_state_monthly',    MIN(month_label), MAX(month_label), COUNT(DISTINCT month_label), 22 FROM workspace.sentinel.gold_state_monthly
  UNION ALL SELECT 'gold_response_monthly', MIN(month_label), MAX(month_label), COUNT(DISTINCT month_label), 22 FROM workspace.sentinel.gold_response_monthly
  UNION ALL SELECT 'gold_estimator_calibration', MIN(month_label), MAX(month_label), COUNT(DISTINCT month_label), 22 FROM workspace.sentinel.gold_estimator_calibration
  UNION ALL SELECT 'gold_company_response_estimates', MIN(period), MAX(period), COUNT(DISTINCT period), 3 FROM workspace.sentinel.gold_company_response_estimates
  UNION ALL SELECT 'gold_sub_issue_estimates', MIN(period), MAX(period), COUNT(DISTINCT period), 23 FROM workspace.sentinel.gold_sub_issue_estimates
)
SELECT table_name, first_period, last_period, distinct_periods, expected_periods,
       CASE WHEN distinct_periods = expected_periods THEN 'PASS' ELSE 'FAIL' END AS status
FROM coverage
ORDER BY table_name;


-- -------------------------------------------------------------------------------------
-- D. Exact reconciliation with monthly population totals (residual rows included)
-- -------------------------------------------------------------------------------------
-- For every month, SUM(complaints) in each breakdown — including residual rows
-- (is_residual = true, e.g. complaints with no state given) — must equal
-- gold_monthly_volume.population_complaints, and shares must sum to 1.
-- gold_response_monthly holds five dimensions, so it is checked per dimension.
WITH vol AS (
  SELECT month_label, population_complaints FROM workspace.sentinel.gold_monthly_volume
),
breakdowns AS (
  SELECT 'company' AS breakdown, month_label, complaints, share_of_month, is_residual FROM workspace.sentinel.gold_company_monthly
  UNION ALL SELECT 'issue', month_label, complaints, share_of_month, is_residual FROM workspace.sentinel.gold_issue_monthly
  UNION ALL SELECT 'state', month_label, complaints, share_of_month, is_residual FROM workspace.sentinel.gold_state_monthly
  UNION ALL SELECT CONCAT('response:', dimension), month_label, complaints, share_of_month, is_residual
            FROM workspace.sentinel.gold_response_monthly
),
monthly AS (
  SELECT breakdown, month_label,
         SUM(complaints)                                     AS breakdown_total,
         SUM(share_of_month)                                 AS share_total,
         SUM(CASE WHEN is_residual THEN 1 ELSE 0 END)        AS residual_rows,
         SUM(CASE WHEN is_residual THEN complaints ELSE 0 END) AS residual_complaints
  FROM breakdowns
  GROUP BY breakdown, month_label
),
expected_breakdowns AS (
  SELECT b.breakdown, v.month_label, v.population_complaints
  FROM (SELECT DISTINCT breakdown FROM monthly) b CROSS JOIN vol v
)
SELECT e.breakdown,
       COUNT(*)                                                                    AS months_expected,
       COUNT(m.month_label)                                                        AS months_present,
       SUM(CASE WHEN m.breakdown_total = e.population_complaints THEN 1 ELSE 0 END) AS months_reconciled,
       MAX(ABS(COALESCE(m.share_total, 0) - 1))                                    AS max_share_deviation,
       SUM(COALESCE(m.residual_rows, 0))                                           AS residual_rows,
       SUM(COALESCE(m.residual_complaints, 0))                                     AS residual_complaints,
       CASE WHEN COUNT(m.month_label) = COUNT(*)
             AND SUM(CASE WHEN m.breakdown_total = e.population_complaints THEN 1 ELSE 0 END) = COUNT(*)
             AND MAX(ABS(COALESCE(m.share_total, 0) - 1)) < 1e-9
            THEN 'PASS' ELSE 'FAIL' END                                            AS status
FROM expected_breakdowns e
LEFT JOIN monthly m ON m.breakdown = e.breakdown AND m.month_label = e.month_label
GROUP BY e.breakdown
ORDER BY e.breakdown;


-- -------------------------------------------------------------------------------------
-- E. Key uniqueness (each grain must appear once)
-- -------------------------------------------------------------------------------------
WITH dupes AS (
  SELECT 'gold_monthly_volume (month_label)' AS table_and_key, COUNT(*) AS duplicate_keys
  FROM (SELECT month_label FROM workspace.sentinel.gold_monthly_volume GROUP BY month_label HAVING COUNT(*) > 1) AS d1
  UNION ALL SELECT 'gold_company_monthly (month_label, company)', COUNT(*)
  FROM (SELECT month_label, company FROM workspace.sentinel.gold_company_monthly GROUP BY month_label, company HAVING COUNT(*) > 1) AS d2
  UNION ALL SELECT 'gold_issue_monthly (month_label, issue)', COUNT(*)
  FROM (SELECT month_label, issue FROM workspace.sentinel.gold_issue_monthly GROUP BY month_label, issue HAVING COUNT(*) > 1) AS d3
  UNION ALL SELECT 'gold_state_monthly (month_label, state)', COUNT(*)
  FROM (SELECT month_label, state FROM workspace.sentinel.gold_state_monthly GROUP BY month_label, state HAVING COUNT(*) > 1) AS d4
  UNION ALL SELECT 'gold_response_monthly (month_label, dimension, category)', COUNT(*)
  FROM (SELECT month_label, dimension, category FROM workspace.sentinel.gold_response_monthly
        GROUP BY month_label, dimension, category HAVING COUNT(*) > 1) AS d5
  UNION ALL SELECT 'gold_company_response_estimates (period, dimension, company, category)', COUNT(*)
  FROM (SELECT period, dimension, company, category FROM workspace.sentinel.gold_company_response_estimates
        GROUP BY period, dimension, company, category HAVING COUNT(*) > 1) AS d6
  UNION ALL SELECT 'gold_sub_issue_estimates (period, issue_sub_issue)', COUNT(*)
  FROM (SELECT period, issue_sub_issue FROM workspace.sentinel.gold_sub_issue_estimates
        GROUP BY period, issue_sub_issue HAVING COUNT(*) > 1) AS d7
)
SELECT table_and_key, 0 AS expected, duplicate_keys AS actual,
       CASE WHEN duplicate_keys = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM dupes
ORDER BY table_and_key;


-- -------------------------------------------------------------------------------------
-- F. Distinct values the analytics will filter on (INFO: review, no pass/fail)
-- -------------------------------------------------------------------------------------
-- `detail` = total complaints for response and state rows; number of RELIABLE estimate
-- rows for company-estimate rows; NULL elsewhere.
SELECT 'response dimension / category' AS field, CONCAT(dimension, ' / ', COALESCE(category, '<no value>')) AS value,
       COUNT(*) AS row_count, SUM(complaints) AS detail
FROM workspace.sentinel.gold_response_monthly
GROUP BY dimension, category
UNION ALL
SELECT 'state_category', COALESCE(state_category, '<no state given>'), COUNT(*), SUM(complaints)
FROM workspace.sentinel.gold_state_monthly
GROUP BY state_category
UNION ALL
SELECT 'company estimate period / dimension', CONCAT(period, ' / ', dimension), COUNT(*),
       SUM(CASE WHEN is_reliable THEN 1 ELSE 0 END)
FROM workspace.sentinel.gold_company_response_estimates
GROUP BY period, dimension
UNION ALL
SELECT 'issue groups (combined = true)', CONCAT(issue_group, ' | combined=', CAST(is_combined_group AS STRING)),
       COUNT(*), NULL
FROM workspace.sentinel.gold_issue_category_map
GROUP BY issue_group, is_combined_group
UNION ALL
SELECT 'measure_type by table', CONCAT(t, ': ', measure_type), COUNT(*), NULL
FROM (
  SELECT 'monthly_volume' AS t, measure_type FROM workspace.sentinel.gold_monthly_volume
  UNION ALL SELECT 'company_monthly', measure_type FROM workspace.sentinel.gold_company_monthly
  UNION ALL SELECT 'issue_monthly', measure_type FROM workspace.sentinel.gold_issue_monthly
  UNION ALL SELECT 'state_monthly', measure_type FROM workspace.sentinel.gold_state_monthly
  UNION ALL SELECT 'response_monthly', measure_type FROM workspace.sentinel.gold_response_monthly
  UNION ALL SELECT 'company_response_estimates', measure_type FROM workspace.sentinel.gold_company_response_estimates
  UNION ALL SELECT 'sub_issue_estimates', measure_type FROM workspace.sentinel.gold_sub_issue_estimates
) AS m
GROUP BY t, measure_type
ORDER BY field, value;


-- -------------------------------------------------------------------------------------
-- G. Response-data reporting rule by month (INFO)
-- -------------------------------------------------------------------------------------
-- Months where >= 1% of complaints still have company response 'In progress' are treated
-- as PROVISIONAL for response, public-response, timeliness and channel metrics. This is a
-- reporting rule, NOT proof that months below 1% are complete. Volume, company, issue and
-- state counts are recorded at intake and remain usable (they can still be revised slightly).
SELECT v.month_label,
       v.population_complaints,
       COALESCE(r.complaints, 0)                                   AS in_progress_complaints,
       ROUND(COALESCE(r.complaints, 0) / v.population_complaints, 4) AS in_progress_share,
       CASE WHEN COALESCE(r.complaints, 0) / v.population_complaints >= 0.01
            THEN 'PROVISIONAL (response metrics withheld)'
            ELSE 'below 1% (reporting rule met)' END               AS response_data_status
FROM workspace.sentinel.gold_monthly_volume v
LEFT JOIN workspace.sentinel.gold_response_monthly r
  ON r.month_label = v.month_label
 AND r.dimension = 'company_response'
 AND r.category = 'In progress'
ORDER BY v.month_label;
