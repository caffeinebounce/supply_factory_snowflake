create schema prod_sf_tables.reporting;

CREATE OR REPLACE TABLE date_spine AS
WITH date_range AS (
  SELECT DATE('2019-01-01') AS date_value
  UNION ALL
  SELECT dateadd(day, 1, date_value)
  FROM date_range
  WHERE date_value < DATE('2030-12-31')
)
SELECT 
  date_value AS date,
  (DAYOFWEEK(date_value) + 1) AS day_of_week,
  UPPER(TO_CHAR(date_value, 'dy')) AS day_of_week_text,
  (DAYOFWEEK(date_value) + 1) NOT IN (7, 1) AS is_weekday,
  CASE
    WHEN DATEADD(day, 1, date_value) > DATEADD(month, 1, DATE_TRUNC('month', date_value))-1 
      AND (DAYOFWEEK(date_value) + 1) NOT IN (7, 1)
    THEN date_value
    ELSE NULL
  END AS last_weekday_of_month,
  DATE_TRUNC('week', date_value) AS week_first_day,
  DATEADD(day, 6, DATE_TRUNC('week', date_value)) AS week_last_day,
  DATE_TRUNC('month', date_value) AS month_first_day,
  DATEADD(month, 1, DATE_TRUNC('month', date_value))-1 AS month_last_day,
  UPPER(TO_CHAR(date_value, 'mmmm')) AS month_text,
  DATE_TRUNC('quarter', date_value) AS quarter_first_day,
  DATEADD(quarter, 1, DATE_TRUNC('quarter', date_value))-1 AS quarter_last_day,
  YEAR(date_value) AS year,
  MONTH(date_value) AS month_number,
  QUARTER(date_value) AS quarter_number,
  CONCAT('M', MONTH(date_value), ' ', YEAR(date_value)) AS month_year,
  CONCAT('Q', QUARTER(date_value), ' ', YEAR(date_value)) AS quarter_year
FROM date_range
ORDER BY date_value;

CREATE OR REPLACE VIEW date_spine_to_today AS
SELECT *
FROM date_spine
WHERE date >= DATE('2020-01-01') AND date <= CURRENT_DATE();
CREATE OR REPLACE VIEW date_spine_projections AS
SELECT *
FROM date_spine
WHERE date >= CURRENT_DATE()
  AND date <= LAST_DAY(DATE_FROM_PARTS(YEAR(CURRENT_DATE()) + 2, 12, 31));

CREATE OR REPLACE VIEW date_spine_historical_and_projections AS
SELECT *
FROM date_spine
WHERE date >= date >= DATE('2020-01-01')
  AND date <= LAST_DAY(DATE_FROM_PARTS(YEAR(CURRENT_DATE()) + 2, 12, 31));

-- Create the stored procedure that updates the views
CREATE OR REPLACE PROCEDURE reporting.update_date_spine_views()
RETURNS VARCHAR
LANGUAGE SQL
AS $$
BEGIN
  CREATE OR REPLACE VIEW date_spine_to_today AS
    SELECT *
    FROM date_spine
    WHERE date >= DATE('2020-01-01') AND date <= CURRENT_DATE();
    
  CREATE OR REPLACE VIEW date_spine_projections AS
    SELECT *
    FROM date_spine
    WHERE date >= CURRENT_DATE()
      AND date <= LAST_DAY(DATE_FROM_PARTS(YEAR(CURRENT_DATE()) + 2, 12, 31));
  
  CREATE OR REPLACE VIEW date_spine_historical_and_projections AS
    SELECT *
    FROM date_spine
    WHERE date >= DATE('2020-01-01')
      AND date <= LAST_DAY(DATE_FROM_PARTS(YEAR(CURRENT_DATE()) + 2, 12, 31));
  
  RETURN 'Date spine views created/updated successfully.';
END;
$$;

-- Create the task that executes the stored procedure once a week on Sunday at 1am ET
CREATE OR REPLACE TASK update_date_spine_views_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 1 * * 0 America/New_York'
AS
CALL update_date_spine_views();

alter task update_date_spine_views_task resume;