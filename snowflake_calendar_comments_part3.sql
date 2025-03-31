/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - COLUMN COMMENTS (PART 3)
======================================================================================

This script adds comments to the fiscal calendar and retail calendar tables.
Execute this script after all other scripts have been executed.

Version: 2.0
Date: 31/03/2025
*/

-- Add comments to FISCAL_CALENDAR table
COMMENT ON TABLE FISCAL_CALENDAR IS 'Fiscal calendar table with configurable fiscal year start date. Contains attributes specific to fiscal periods.';

COMMENT ON COLUMN FISCAL_CALENDAR.DATE IS 'Calendar date.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_YEAR IS 'Fiscal year number.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_YEAR_SHORT IS 'Short fiscal year description (e.g., FY24).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_YEAR_LONG IS 'Long fiscal year description (e.g., FY2024).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_YEAR_START_DATE IS 'Start date of the fiscal year.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_YEAR_END_DATE IS 'End date of the fiscal year.';
COMMENT ON COLUMN FISCAL_CALENDAR.DAY_OF_FISCAL_YEAR IS 'Day number within the fiscal year (1-366).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_MONTH_NUM IS 'Month number within the fiscal year (1-12).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_QUARTER_NUM IS 'Quarter number within the fiscal year (1-4).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_HALF_NUM IS 'Half number within the fiscal year (1-2).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_WEEK_NUM IS 'Week number within the fiscal year (1-53).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_MONTH_START_DATE IS 'Start date of the fiscal month.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_MONTH_END_DATE IS 'End date of the fiscal month.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_QUARTER_START_DATE IS 'Start date of the fiscal quarter.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_QUARTER_END_DATE IS 'End date of the fiscal quarter.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_HALF_START_DATE IS 'Start date of the fiscal half.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_HALF_END_DATE IS 'End date of the fiscal half.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_WEEK_START_DATE IS 'Start date of the fiscal week.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_WEEK_END_DATE IS 'End date of the fiscal week.';
COMMENT ON COLUMN FISCAL_CALENDAR.IS_FISCAL_YEAR_START IS 'Flag indicating whether the date is the start of a fiscal year (1=Yes, 0=No).';
COMMENT ON COLUMN FISCAL_CALENDAR.IS_FISCAL_YEAR_END IS 'Flag indicating whether the date is the end of a fiscal year (1=Yes, 0=No).';
COMMENT ON COLUMN FISCAL_CALENDAR.IS_FISCAL_MONTH_START IS 'Flag indicating whether the date is the start of a fiscal month (1=Yes, 0=No).';
COMMENT ON COLUMN FISCAL_CALENDAR.IS_FISCAL_MONTH_END IS 'Flag indicating whether the date is the end of a fiscal month (1=Yes, 0=No).';
COMMENT ON COLUMN FISCAL_CALENDAR.IS_FISCAL_QUARTER_START IS 'Flag indicating whether the date is the start of a fiscal quarter (1=Yes, 0=No).';
COMMENT ON COLUMN FISCAL_CALENDAR.IS_FISCAL_QUARTER_END IS 'Flag indicating whether the date is the end of a fiscal quarter (1=Yes, 0=No).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_YEAR_MONTH_KEY IS 'Integer key for fiscal year and month (YYYYMM).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_YEAR_QUARTER_KEY IS 'Integer key for fiscal year and quarter (YYYYQ).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_YEAR_WEEK_KEY IS 'Integer key for fiscal year and week (YYYYWW).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_MONTH_NAME IS 'Name of the fiscal month (e.g., Month 01).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_QUARTER_NAME IS 'Name of the fiscal quarter (e.g., Q1).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_HALF_NAME IS 'Name of the fiscal half (e.g., H1).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_WEEK_NAME IS 'Name of the fiscal week (e.g., W01).';
COMMENT ON COLUMN FISCAL_CALENDAR.CREATED_AT IS 'Timestamp when the record was created.';
COMMENT ON COLUMN FISCAL_CALENDAR.CREATED_BY IS 'Process that created the record.';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_YEAR_START_MONTH IS 'Month when the fiscal year starts (1-12).';
COMMENT ON COLUMN FISCAL_CALENDAR.FISCAL_YEAR_START_DAY IS 'Day when the fiscal year starts (1-31).';

-- Function to add comments to RETAIL_CALENDAR tables
CREATE OR REPLACE PROCEDURE ADD_RETAIL_CALENDAR_COMMENTS(
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    PATTERN VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    table_name VARCHAR;
    comment_sql VARCHAR;
BEGIN
    -- Set table name
    table_name := DATABASE_NAME || '.' || SCHEMA_NAME || '.RETAIL_CALENDAR_' || PATTERN;
    
    -- Add table comment
    comment_sql := 'COMMENT ON TABLE ' || table_name || ' IS ''Retail calendar table with ' || PATTERN || ' pattern. Contains attributes specific to retail periods.''';
    EXECUTE IMMEDIATE comment_sql;
    
    -- Add column comments
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DATE IS ''Calendar date.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_YEAR IS ''Retail year number.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_YEAR_SHORT IS ''Short retail year description (e.g., R24).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_YEAR_LONG IS ''Long retail year description (e.g., R2024).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_YEAR_START_DATE IS ''Start date of the retail year.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_YEAR_END_DATE IS ''End date of the retail year.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_WEEK_NUM IS ''Week number within the retail year (1-53).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_WEEK_START_DATE IS ''Start date of the retail week.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_WEEK_END_DATE IS ''End date of the retail week.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_MONTH_NUM IS ''Month number within the retail year (1-12).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_MONTH_START_DATE IS ''Start date of the retail month.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_MONTH_END_DATE IS ''End date of the retail month.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_QUARTER_NUM IS ''Quarter number within the retail year (1-4).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_QUARTER_START_DATE IS ''Start date of the retail quarter.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_QUARTER_END_DATE IS ''End date of the retail quarter.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_HALF_NUM IS ''Half number within the retail year (1-2).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_HALF_START_DATE IS ''Start date of the retail half.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_HALF_END_DATE IS ''End date of the retail half.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.WEEKS_IN_MONTH IS ''Number of weeks in the retail month based on the ' || PATTERN || ' pattern.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TOTAL_WEEKS_IN_MONTH IS ''Total number of weeks in the retail month.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TOTAL_WEEKS_IN_QUARTER IS ''Total number of weeks in the retail quarter.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TOTAL_WEEKS_IN_HALF IS ''Total number of weeks in the retail half.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.WEEKS_IN_YEAR IS ''Total number of weeks in the retail year.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.IS_RETAIL_YEAR_START IS ''Flag indicating whether the date is the start of a retail year (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.IS_RETAIL_YEAR_END IS ''Flag indicating whether the date is the end of a retail year (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.IS_RETAIL_WEEK_START IS ''Flag indicating whether the date is the start of a retail week (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.IS_RETAIL_WEEK_END IS ''Flag indicating whether the date is the end of a retail week (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.IS_RETAIL_MONTH_START IS ''Flag indicating whether the date is the start of a retail month (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.IS_RETAIL_MONTH_END IS ''Flag indicating whether the date is the end of a retail month (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.IS_RETAIL_QUARTER_START IS ''Flag indicating whether the date is the start of a retail quarter (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.IS_RETAIL_QUARTER_END IS ''Flag indicating whether the date is the end of a retail quarter (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.IS_RETAIL_HALF_START IS ''Flag indicating whether the date is the start of a retail half (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.IS_RETAIL_HALF_END IS ''Flag indicating whether the date is the end of a retail half (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_RETAIL_YEAR IS ''Day number within the retail year (1-366).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_RETAIL_WEEK IS ''Day number within the retail week (1-7).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_RETAIL_MONTH IS ''Day number within the retail month (1-35).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_RETAIL_QUARTER IS ''Day number within the retail quarter (1-98).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_RETAIL_HALF IS ''Day number within the retail half (1-183).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_YEAR_MONTH_KEY IS ''Integer key for retail year and month (YYYYMM).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_YEAR_QUARTER_KEY IS ''Integer key for retail year and quarter (YYYYQ).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_YEAR_WEEK_KEY IS ''Integer key for retail year and week (YYYYWW).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_MONTH_NAME IS ''Name of the retail month (e.g., Month 01).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_QUARTER_NAME IS ''Name of the retail quarter (e.g., Q1).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_HALF_NAME IS ''Name of the retail half (e.g., H1).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_WEEK_NAME IS ''Name of the retail week (e.g., W01).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.CREATED_AT IS ''Timestamp when the record was created.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.CREATED_BY IS ''Process that created the record.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_PATTERN IS ''Retail calendar pattern (' || PATTERN || ').''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_START_MONTH IS ''Month when the retail year starts (1-12).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.RETAIL_WEEK_START_DAY IS ''Day when the retail week starts (0=Sunday, 1=Monday).''';
    
    RETURN 'Comments added to ' || table_name;
END;
$$;