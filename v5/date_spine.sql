/*==============================================================================
  DATE_SPINE TABLE - BUSINESS CALENDAR SYSTEM

  This script creates a continuous date spine table, serving as the foundation
  for all calendar calculations. It contains a sequence of dates for the desired
  range.

  USAGE:
  ------
  Run this script to create or replace the DATE_SPINE table.

  Usage Example:
  --------------
  ```sql
  CREATE OR REPLACE TABLE DATE_SPINE AS
  WITH spine AS (
      SELECT DATEADD(DAY, SEQ4(), '2000-01-01') AS DATE_KEY
      FROM TABLE(GENERATOR(ROWCOUNT => 36525))
  )
  SELECT
      DATE_KEY,
      TO_NUMBER(TO_CHAR(DATE_KEY, 'YYYYMMDD')) AS DATE_INT_KEY
  FROM spine;
  ```

  STRUCTURE:
  ----------
  - Continuous sequence of dates
  - Natural DATE primary key
  - Integer date key (YYYYMMDD)

  NOTES:
  ------
  - Serves as the base for Gregorian, Fiscal, Retail calendars
  - Adjust date range as needed
  - Add clustering if desired

  AUTHOR: Andrew Exley, Lument Pty Ltd
  DATE CREATED: 2025-04-05
  LAST MODIFIED: 2025-04-06
==============================================================================*/

CREATE OR REPLACE TABLE DATE_SPINE AS
WITH spine AS (
    SELECT
        DATEADD(DAY, SEQ4(), '2000-01-01') AS DATE_KEY
    FROM TABLE(GENERATOR(ROWCOUNT => 36525)) -- ~100 years
)
SELECT
    DATE_KEY,
    TO_NUMBER(TO_CHAR(DATE_KEY, 'YYYYMMDD')) AS DATE_INT_KEY
FROM spine;

-- Add clustering if desired
-- ALTER TABLE DATE_SPINE CLUSTER BY (DATE_KEY);