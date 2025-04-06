/*==============================================================================
  DIM_GREGORIAN - GREGORIAN CALENDAR ATTRIBUTES

  This script generates Gregorian calendar attributes by joining to the DATE_SPINE
  table. It is designed to be modular and joined with fiscal and retail calendars.

  USAGE:
  ------
  Run this script to create or replace the DIM_GREGORIAN table.

  Usage Example:
  --------------
  ```sql
  CREATE OR REPLACE TABLE DIM_GREGORIAN AS
  SELECT ...
  FROM DATE_SPINE;
  ```

  STRUCTURE:
  ----------
  - Gregorian calendar attributes (year, quarter, month, week, day)
  - Leap year and weekend flags
  - Same-period-last-year references

  NOTES:
  ------
  - Serves as the base for fiscal and retail calendar joins
  - Can be extended with additional Gregorian attributes
  - Designed for conformed dimensional modelling

  AUTHOR: Andrew Exley, Lument Pty Ltd
  DATE CREATED: 2025-04-05
  LAST MODIFIED: 2025-04-06
==============================================================================*/

CREATE OR REPLACE TABLE DIM_GREGORIAN AS
SELECT
    s.DATE_KEY,
    s.DATE_INT_KEY,

    TO_VARCHAR(DATE_KEY, 'YYYY-MM-DD') AS FULL_DATE_DESC,
    TO_VARCHAR(DATE_KEY, 'DAY') AS DAY_NAME,
    DAYOFWEEKISO(DATE_KEY) AS DAY_NUMBER_IN_WEEK_ISO,
    DAY(DATE_KEY) AS DAY_NUMBER_IN_MONTH,
    DAYOFYEAR(DATE_KEY) AS DAY_NUMBER_IN_YEAR,
    WEEKISO(DATE_KEY) AS WEEK_NUMBER_IN_YEAR_ISO,
    MONTH(DATE_KEY) AS MONTH_NUMBER_IN_YEAR,
    LPAD(MONTH(DATE_KEY),2,'0') AS MONTH_NUMBER_DESCRIPTION,
    MONTH(DATE_KEY) AS MONTH_NUMBER_SORT,
    TO_VARCHAR(DATE_KEY, 'MMMM') AS MONTH_NAME,
    QUARTER(DATE_KEY) AS QUARTER_NUMBER_IN_YEAR,
    CONCAT('Q', QUARTER(DATE_KEY)) AS QUARTER_NUMBER_DESCRIPTION,
    QUARTER(DATE_KEY) AS QUARTER_NUMBER_SORT,
    YEAR(DATE_KEY) AS YEAR_NUMBER,
    DATE_TRUNC('MONTH', DATE_KEY) AS MONTH_START_DATE,
    LAST_DAY(DATE_KEY, 'MONTH') AS MONTH_END_DATE,
    DATE_TRUNC('QUARTER', DATE_KEY) AS QUARTER_START_DATE,
    LAST_DAY(DATE_KEY, 'QUARTER') AS QUARTER_END_DATE,
    DATE_TRUNC('YEAR', DATE_KEY) AS YEAR_START_DATE,
    LAST_DAY(DATE_KEY, 'YEAR') AS YEAR_END_DATE,
    CASE WHEN DAYOFWEEK(DATE_KEY) IN (0,6) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
    CASE WHEN MOD(YEAR(DATE_KEY),4)=0 AND (MOD(YEAR(DATE_KEY),100)<>0 OR MOD(YEAR(DATE_KEY),400)=0) THEN TRUE ELSE FALSE END AS IS_LEAP_YEAR,
    DATEADD(YEAR,-1,DATE_KEY) AS SAME_DATE_LAST_YEAR,
    NULL AS SAME_DAY_LAST_YEAR -- Can be calculated if needed

FROM DATE_SPINE s;