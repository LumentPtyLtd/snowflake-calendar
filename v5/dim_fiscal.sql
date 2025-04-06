/*==============================================================================
  DIM_FISCAL - CORPORATE FISCAL CALENDAR ATTRIBUTES

  This script generates corporate fiscal calendar attributes by joining to the DATE_SPINE
  table. It supports:
  - Configurable fiscal year start month and day (default July 1st)
  - Month-based fiscal quarters and months
  - No 4-4-5 pattern (see retail calendar for that)

  USAGE:
  ------
  Run this script to create or replace the DIM_FISCAL table.

  Usage Example:
  --------------
  ```sql
  CREATE OR REPLACE TABLE DIM_FISCAL AS
  SELECT ...
  FROM DATE_SPINE;
  ```

  STRUCTURE:
  ----------
  - Fiscal year, quarter, month, day-in-year
  - Fiscal year start/end dates
  - No 53-week handling

  NOTES:
  ------
  - Designed for corporate fiscal calendars aligned to calendar months
  - Can be extended with additional fiscal attributes
  - Not suitable for retail 4-4-5 calendars

  AUTHOR: Andrew Exley, Lument Pty Ltd
  DATE CREATED: 2025-04-05
  LAST MODIFIED: 2025-04-06
==============================================================================*/

WITH params AS (
    SELECT
        7 AS fiscal_start_month, -- July
        1 AS fiscal_start_day    -- 1st
),
fiscal_years AS (
    SELECT
        y,
        DATE_FROM_PARTS(y-1, (SELECT fiscal_start_month FROM params), (SELECT fiscal_start_day FROM params)) AS fiscal_year_start
    FROM (SELECT SEQ4()+1995 AS y FROM TABLE(GENERATOR(ROWCOUNT=>50)))
),
date_with_fiscal AS (
    SELECT
        s.DATE_KEY,
        fy.y AS FISCAL_YEAR_NUMBER,
        QUARTER(DATEADD(MONTH, -((SELECT fiscal_start_month FROM params)-1), s.DATE_KEY)) AS FISCAL_QUARTER_NUMBER,
        CONCAT('Q', QUARTER(DATEADD(MONTH, -((SELECT fiscal_start_month FROM params)-1), s.DATE_KEY))) AS FISCAL_QUARTER_NUMBER_DESCRIPTION,
        QUARTER(DATEADD(MONTH, -((SELECT fiscal_start_month FROM params)-1), s.DATE_KEY)) AS FISCAL_QUARTER_NUMBER_SORT,
        MOD(MONTH(DATEADD(MONTH, -((SELECT fiscal_start_month FROM params)-1), s.DATE_KEY))-1,12)+1 AS FISCAL_MONTH_NUMBER_IN_YEAR,
        LPAD(MOD(MONTH(DATEADD(MONTH, -((SELECT fiscal_start_month FROM params)-1), s.DATE_KEY))-1,12)+1,2,'0') AS FISCAL_MONTH_NUMBER_DESCRIPTION,
        MOD(MONTH(DATEADD(MONTH, -((SELECT fiscal_start_month FROM params)-1), s.DATE_KEY))-1,12)+1 AS FISCAL_MONTH_NUMBER_SORT,
        NULL AS FISCAL_WEEK_NUMBER_IN_YEAR,
        DATEDIFF(DAY, fy.fiscal_year_start, s.DATE_KEY)+1 AS FISCAL_DAY_NUMBER_IN_YEAR,
        fy.fiscal_year_start AS FISCAL_YEAR_START_DATE,
        DATEADD(DAY, -1, DATEADD(YEAR,1,fy.fiscal_year_start)) AS FISCAL_YEAR_END_DATE,
        CONCAT('Q', QUARTER(DATEADD(MONTH, -((SELECT fiscal_start_month FROM params)-1), s.DATE_KEY))) AS FISCAL_QUARTER_NAME
    FROM DATE_SPINE s
    JOIN fiscal_years fy ON s.DATE_KEY >= fy.fiscal_year_start AND s.DATE_KEY < DATEADD(YEAR,1,fy.fiscal_year_start)
)

SELECT * FROM date_with_fiscal;