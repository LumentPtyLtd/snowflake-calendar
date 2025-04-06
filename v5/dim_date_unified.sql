/*==============================================================================
  DIM_DATE_UNIFIED - UNIFIED BUSINESS CALENDAR DIMENSION

  This view joins the modular calendar components:
  - DATE_SPINE: continuous date sequence
  - DIM_GREGORIAN: Gregorian calendar attributes
  - DIM_FISCAL: corporate fiscal calendar (month-based)
  - DIM_RETAIL: retail fiscal calendar (4-4-5, 4-5-4, 5-4-4 patterns, 53-week years)

  USAGE:
  ------
  Run this script to create or replace the DIM_DATE_UNIFIED view.

  Usage Example:
  --------------
  ```sql
  SELECT * FROM DIM_DATE_UNIFIED WHERE FISCAL_YEAR_NUMBER = 2025;
  ```

  STRUCTURE:
  ----------
  - Combines Gregorian, Fiscal, and Retail calendar attributes
  - Supports multiple calendar perspectives in one conformed dimension
  - Designed for BI-friendly filtering and joining

  NOTES:
  ------
  - Can be extended with additional calendar types or attributes

  AUTHOR: Andrew Exley, Lument Pty Ltd
  DATE CREATED: 2025-04-05
  LAST MODIFIED: 2025-04-06
==============================================================================*/

CREATE OR REPLACE VIEW DIM_DATE_UNIFIED AS
SELECT
    s.DATE_KEY,
    s.DATE_INT_KEY,

    -- Gregorian
    g.FULL_DATE_DESC,
    g.DAY_NAME,
    g.DAY_NUMBER_IN_WEEK_ISO,
    g.DAY_NUMBER_IN_MONTH,
    g.DAY_NUMBER_IN_YEAR,
    g.WEEK_NUMBER_IN_YEAR_ISO,
    g.MONTH_NUMBER_IN_YEAR,
    g.MONTH_NUMBER_DESCRIPTION,
    g.MONTH_NUMBER_SORT,
    g.MONTH_NAME,
    g.QUARTER_NUMBER_IN_YEAR,
    g.QUARTER_NUMBER_DESCRIPTION,
    g.QUARTER_NUMBER_SORT,
    g.YEAR_NUMBER,
    g.MONTH_START_DATE,
    g.MONTH_END_DATE,
    g.QUARTER_START_DATE,
    g.QUARTER_END_DATE,
    g.YEAR_START_DATE,
    g.YEAR_END_DATE,
    g.IS_WEEKEND,
    g.IS_LEAP_YEAR,
    g.SAME_DATE_LAST_YEAR,
    g.SAME_DAY_LAST_YEAR,

    -- Fiscal (corporate)
    f.FISCAL_YEAR_NUMBER,
    f.FISCAL_QUARTER_NUMBER,
    f.FISCAL_QUARTER_NUMBER_DESCRIPTION,
    f.FISCAL_QUARTER_NUMBER_SORT,
    f.FISCAL_MONTH_NUMBER_IN_YEAR,
    f.FISCAL_MONTH_NUMBER_DESCRIPTION,
    f.FISCAL_MONTH_NUMBER_SORT,
    f.FISCAL_WEEK_NUMBER_IN_YEAR,
    f.FISCAL_DAY_NUMBER_IN_YEAR,
    f.FISCAL_YEAR_START_DATE,
    f.FISCAL_YEAR_END_DATE,
    f.FISCAL_QUARTER_NAME,
    
    -- Retail (4-4-5)
    r.RETAIL_YEAR_NUMBER,
    r.RETAIL_QUARTER_NUMBER,
    r.RETAIL_QUARTER_NUMBER_DESCRIPTION,
    r.RETAIL_QUARTER_NUMBER_SORT,
    r.RETAIL_MONTH_NUMBER_IN_YEAR,
    r.RETAIL_MONTH_NUMBER_DESCRIPTION,
    r.RETAIL_MONTH_NUMBER_SORT,
    r.RETAIL_WEEK_NUMBER_IN_YEAR,
    r.RETAIL_DAY_NUMBER_IN_YEAR,
    r.RETAIL_YEAR_START_DATE,
    r.RETAIL_YEAR_END_DATE,
    r.RETAIL_IS_53_WEEK_YEAR,
    r.RETAIL_QUARTER_NAME,
    r.RETAIL_PATTERN

FROM DATE_SPINE s
LEFT JOIN DIM_GREGORIAN g ON s.DATE_KEY = g.DATE_KEY
LEFT JOIN DIM_FISCAL f ON s.DATE_KEY = f.DATE_KEY
LEFT JOIN DIM_RETAIL r ON s.DATE_KEY = r.DATE_KEY;