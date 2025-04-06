/*==============================================================================
  DIM_DATE TABLE DEFINITION - BUSINESS CALENDAR SYSTEM

  This script defines the static conformed date dimension table (DIM_DATE),
  following Kimball principles. It stores Gregorian, optional Fiscal and Retail
  calendar attributes, including same-period-last-year references, with a natural
  DATE primary key.

  USAGE:
  ------
  Run this script to create or replace the DIM_DATE table structure.
  Populate the table using the SP_GENERATE_DIM_DATE stored procedure.

  STRUCTURE:
  ----------
  - Static attributes only (no dynamic flags)
  - Optional Fiscal and Retail columns (nullable)
  - Includes descriptive and sort columns for BI friendliness
  - Clustered on DATE_KEY for query performance

  NOTES:
  ------
  - Designed for integration with dynamic views for relative flags
  - Fiscal and Retail columns populated conditionally
  - Extensible for future attributes

  AUTHOR: Andrew Exley, Lument Pty Ltd
  DATE CREATED: 2025-04-05
  LAST MODIFIED: 2025-04-06
==============================================================================*/

CREATE OR REPLACE TABLE DIM_DATE
(
    -- Primary Key: The actual calendar date
    DATE_KEY DATE NOT NULL,

    -- Integer representation of date (YYYYMMDD)
    DATE_INT_KEY INT,

    -- Gregorian Calendar Attributes
    FULL_DATE_DESC STRING NOT NULL, -- User-friendly full date description (e.g., '2025-04-05')
    DAY_NAME STRING NOT NULL, -- Full weekday name (e.g., 'Monday')
    DAY_NUMBER_IN_WEEK_ISO TINYINT NOT NULL, -- ISO weekday number (1=Mon, 7=Sun)
    DAY_NUMBER_IN_MONTH TINYINT NOT NULL, -- Day of month (1-31)
    DAY_NUMBER_IN_YEAR SMALLINT NOT NULL, -- Day of year (1-366)
    WEEK_NUMBER_IN_YEAR_ISO TINYINT NOT NULL, -- ISO week number (1-53)
    MONTH_NUMBER_IN_YEAR TINYINT NOT NULL, -- Month number (1-12)
    MONTH_NUMBER_DESCRIPTION VARCHAR(2) NOT NULL, -- Zero-padded month ('01'-'12') for display
    MONTH_NUMBER_SORT TINYINT NOT NULL, -- Month number for sorting (1-12)
    MONTH_NAME STRING NOT NULL, -- Full month name (e.g., 'January')
    QUARTER_NUMBER_IN_YEAR TINYINT NOT NULL, -- Calendar quarter (1-4)
    QUARTER_NUMBER_DESCRIPTION VARCHAR(2) NOT NULL, -- Quarter label ('Q1'-'Q4') for display
    QUARTER_NUMBER_SORT TINYINT NOT NULL, -- Quarter number for sorting (1-4)
    YEAR_NUMBER SMALLINT NOT NULL, -- Gregorian year (e.g., 2025)
    MONTH_START_DATE DATE NOT NULL, -- First day of Gregorian month
    MONTH_END_DATE DATE NOT NULL, -- Last day of Gregorian month
    QUARTER_START_DATE DATE NOT NULL, -- First day of Gregorian quarter
    QUARTER_END_DATE DATE NOT NULL, -- Last day of Gregorian quarter
    YEAR_START_DATE DATE NOT NULL, -- First day of Gregorian year
    YEAR_END_DATE DATE NOT NULL, -- Last day of Gregorian year
    IS_WEEKEND BOOLEAN NOT NULL, -- TRUE if Saturday or Sunday
    IS_LEAP_YEAR BOOLEAN NOT NULL, -- TRUE if leap year
    SAME_DATE_LAST_YEAR DATE, -- Same calendar date prior year
    SAME_DAY_LAST_YEAR DATE, -- Same ISO week/day prior year

    -- Fiscal Calendar Attributes (Nullable)
    FISCAL_YEAR_NUMBER SMALLINT, -- Fiscal year designation
    FISCAL_QUARTER_NUMBER TINYINT, -- Fiscal quarter (1-4)
    FISCAL_QUARTER_NUMBER_DESCRIPTION VARCHAR(2), -- Fiscal quarter label ('Q1'-'Q4') for display
    FISCAL_QUARTER_NUMBER_SORT TINYINT, -- Fiscal quarter number for sorting (1-4)
    FISCAL_MONTH_NUMBER_IN_YEAR TINYINT, -- Fiscal month (1-12)
    FISCAL_MONTH_NUMBER_DESCRIPTION VARCHAR(2), -- Fiscal month zero-padded ('01'-'12') for display
    FISCAL_MONTH_NUMBER_SORT TINYINT, -- Fiscal month number for sorting (1-12)
    FISCAL_WEEK_NUMBER_IN_YEAR TINYINT, -- Fiscal week number
    FISCAL_DAY_NUMBER_IN_YEAR SMALLINT, -- Fiscal day of year
    FISCAL_YEAR_START_DATE DATE, -- Fiscal year start date
    FISCAL_YEAR_END_DATE DATE, -- Fiscal year end date
    FISCAL_QUARTER_NAME STRING, -- Fiscal quarter description

    -- Retail Calendar Attributes (Nullable)
    RETAIL_YEAR_NUMBER SMALLINT, -- Retail year designation
    RETAIL_QUARTER_NUMBER TINYINT, -- Retail quarter (1-4)
    RETAIL_QUARTER_NUMBER_DESCRIPTION VARCHAR(2), -- Retail quarter label ('Q1'-'Q4') for display
    RETAIL_QUARTER_NUMBER_SORT TINYINT, -- Retail quarter number for sorting (1-4)
    RETAIL_PERIOD_NUMBER_IN_YEAR TINYINT, -- Retail period/month (1-12/13)
    RETAIL_PERIOD_NUMBER_DESCRIPTION VARCHAR(2), -- Retail period zero-padded ('01'-'13') for display
    RETAIL_PERIOD_NUMBER_SORT TINYINT, -- Retail period number for sorting (1-13)
    RETAIL_WEEK_NUMBER_IN_YEAR TINYINT, -- Retail week number (1-53)
    RETAIL_DAY_NUMBER_IN_YEAR SMALLINT, -- Retail day of year
    RETAIL_YEAR_START_DATE DATE, -- Retail year start date
    RETAIL_YEAR_END_DATE DATE, -- Retail year end date
    RETAIL_IS_53_WEEK_YEAR BOOLEAN, -- TRUE if retail year has 53 weeks
    RETAIL_QUARTER_NAME STRING, -- Retail quarter description

    -- Audit Columns
    ETL_INSERT_TS TIMESTAMP_NTZ NOT NULL, -- Timestamp when row generated
    ETL_UPDATE_TS TIMESTAMP_NTZ -- Timestamp when row last modified

)
CLUSTER BY (DATE_KEY);

COMMENT ON TABLE DIM_DATE IS 'Static Business Calendar Dimension Table with Gregorian, Fiscal, Retail attributes, including descriptive and sort columns.';