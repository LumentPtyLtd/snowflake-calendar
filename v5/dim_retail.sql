/*==============================================================================
  DIM_RETAIL - RETAIL CALENDAR GENERATOR (SINGLE PATTERN)

  This script generates retail fiscal calendar attributes by joining to the DATE_SPINE
  table. It supports:
  - Configurable retail year start month and day
  - Configurable start-of-week day (e.g., Monday)
  - Single retail pattern (e.g., '4-4-5')
  - Handles 53-week years

  USAGE:
  ------
  Run this script to create or replace the DIM_RETAIL table.

  Usage Example:
  --------------
  ```sql
  CREATE OR REPLACE TABLE DIM_RETAIL AS
  SELECT ...
  FROM DATE_SPINE;
  ```

  STRUCTURE:
  ----------
  - Retail fiscal year, quarter, month, week, day-in-year
  - 4-4-5 or similar pattern applied uniformly
  - 53-week year flag
  - Pattern string stored for reference

  NOTES:
  ------
  - Retail year starts on first specified weekday on or after fiscal start date
  - Designed for retail industry fiscal calendars
  - Can be extended with additional retail attributes

  AUTHOR: Andrew Exley, Lument Pty Ltd
  DATE CREATED: 2025-04-05
  LAST MODIFIED: 2025-04-06
==============================================================================*/

WITH params AS (
    SELECT
        7 AS fiscal_start_month, -- July
        1 AS fiscal_start_day,   -- 1st
        'MONDAY' AS week_start_day,
        '4-4-5' AS pattern_str
),
month_pattern AS (
    SELECT
        quarter_num,
        seq4()+1 AS month_in_quarter,
        TO_NUMBER(SPLIT_PART((SELECT pattern_str FROM params), '-', seq4()+1)) AS weeks_in_month
    FROM (SELECT seq4()+1 AS quarter_num FROM TABLE(GENERATOR(ROWCOUNT=>4))),
         LATERAL (SELECT seq4() FROM TABLE(GENERATOR(ROWCOUNT=>3)))
),
fiscal_years AS (
    SELECT
        y,
        -- Retail year starts on first week_start_day on or after fiscal_start_month/fiscal_start_day
        CASE
            WHEN DAYNAME(DATE_FROM_PARTS(y-1, (SELECT fiscal_start_month FROM params), (SELECT fiscal_start_day FROM params))) = (SELECT week_start_day FROM params)
                THEN DATE_FROM_PARTS(y-1, (SELECT fiscal_start_month FROM params), (SELECT fiscal_start_day FROM params))
            ELSE DATEADD(
                DAY,
                (7 + DECODE(UPPER((SELECT week_start_day FROM params)),
                            'SUNDAY',0,'MONDAY',1,'TUESDAY',2,'WEDNESDAY',3,'THURSDAY',4,'FRIDAY',5,'SATURDAY',6) 
                - DAYOFWEEK(DATE_FROM_PARTS(y-1, (SELECT fiscal_start_month FROM params), (SELECT fiscal_start_day FROM params))) ) % 7,
                DATE_FROM_PARTS(y-1, (SELECT fiscal_start_month FROM params), (SELECT fiscal_start_day FROM params))
            )
        END AS fiscal_year_start,
        (SELECT pattern_str FROM params) AS retail_pattern
    FROM (SELECT SEQ4()+1995 AS y FROM TABLE(GENERATOR(ROWCOUNT=>50)))
),
fiscal_weeks AS (
    SELECT
        fy.y AS fiscal_year,
        fy.fiscal_year_start,
        fy.retail_pattern,
        week_num,
        DATEADD(WEEK, week_num-1, fy.fiscal_year_start) AS week_start,
        DATEADD(DAY, 6, DATEADD(WEEK, week_num-1, fy.fiscal_year_start)) AS week_end
    FROM fiscal_years fy,
    LATERAL (
        SELECT seq4()+1 AS week_num
        FROM TABLE(GENERATOR(ROWCOUNT=>53))
    )
),
fiscal_weeks_flagged AS (
    SELECT *,
        CASE WHEN week_num=53 AND week_start >= DATEADD(DAY,-6,fiscal_year_start) THEN TRUE ELSE FALSE END AS is_53_week
    FROM fiscal_weeks
    WHERE week_num <= 52 OR (week_num=53 AND DATEDIFF(DAY,fiscal_year_start,week_start)<370)
),
month_assignments AS (
    SELECT
        *,
        SUM(weeks_in_month) OVER (ORDER BY quarter_num, month_in_quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS week_cum_sum
    FROM month_pattern
),
fiscal_months AS (
    SELECT
        fw.fiscal_year,
        fw.fiscal_year_start,
        fw.retail_pattern,
        fw.week_num,
        fw.week_start,
        fw.week_end,
        fw.is_53_week,
        ma.quarter_num,
        ma.month_in_quarter,
        ROW_NUMBER() OVER (PARTITION BY fw.fiscal_year ORDER BY fw.week_num) AS week_seq,
        ma.month_in_quarter + (ma.quarter_num -1)*3 AS fiscal_month
    FROM fiscal_weeks_flagged fw
    JOIN month_assignments ma
      ON fw.week_num BETWEEN
            CASE WHEN ma.week_cum_sum - ma.weeks_in_month +1 < 1 THEN 1 ELSE ma.week_cum_sum - ma.weeks_in_month +1 END
            AND ma.week_cum_sum
),
fiscal_quarters AS (
    SELECT *,
        quarter_num AS fiscal_quarter
    FROM fiscal_months
),
date_with_fiscal AS (
    SELECT
        s.DATE_KEY,
        f.fiscal_year AS RETAIL_YEAR_NUMBER,
        f.fiscal_quarter AS RETAIL_QUARTER_NUMBER,
        CONCAT('Q', f.fiscal_quarter) AS RETAIL_QUARTER_NUMBER_DESCRIPTION,
        f.fiscal_quarter AS RETAIL_QUARTER_NUMBER_SORT,
        f.fiscal_month AS RETAIL_MONTH_NUMBER_IN_YEAR,
        LPAD(f.fiscal_month,2,'0') AS RETAIL_MONTH_NUMBER_DESCRIPTION,
        f.fiscal_month AS RETAIL_MONTH_NUMBER_SORT,
        f.week_num AS RETAIL_WEEK_NUMBER_IN_YEAR,
        DATEDIFF(DAY, f.fiscal_year_start, s.DATE_KEY)+1 AS RETAIL_DAY_NUMBER_IN_YEAR,
        f.fiscal_year_start AS RETAIL_YEAR_START_DATE,
        DATEADD(DAY, 6, DATEADD(WEEK, (CASE WHEN f.is_53_week THEN 53 ELSE 52 END)-1, f.fiscal_year_start)) AS RETAIL_YEAR_END_DATE,
        CONCAT('Q', f.fiscal_quarter) AS RETAIL_QUARTER_NAME,
        f.is_53_week AS IS_53_WEEK_YEAR,
        f.retail_pattern AS RETAIL_PATTERN
    FROM DATE_SPINE s
    JOIN fiscal_months f ON s.DATE_KEY BETWEEN f.week_start AND f.week_end
)

SELECT * FROM date_with_fiscal;