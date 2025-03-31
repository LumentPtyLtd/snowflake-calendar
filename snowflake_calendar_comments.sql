/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - COLUMN COMMENTS
======================================================================================

This script adds detailed comments to all tables and columns in the calendar system.
Execute this script after all other scripts have been executed.

Version: 2.0
Date: 31/03/2025
*/

-- Add comments to CALENDAR_ERROR_LOG table
COMMENT ON TABLE CALENDAR_ERROR_LOG IS 'Error logging table for the calendar system. Records all errors that occur during calendar generation and usage.';

COMMENT ON COLUMN CALENDAR_ERROR_LOG.ERROR_ID IS 'Unique identifier for the error record.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.ERROR_TIMESTAMP IS 'Timestamp when the error occurred.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.PROCEDURE_NAME IS 'Name of the procedure or function where the error occurred.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.ERROR_MESSAGE IS 'Detailed error message.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.ERROR_STATE IS 'State or category of the error.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.ERROR_CONTEXT IS 'JSON object containing contextual information about the error.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.STACK_TRACE IS 'Stack trace for the error, if available.';

-- Add comments to CALENDAR_CONFIG table
COMMENT ON TABLE CALENDAR_CONFIG IS 'Configuration table for the calendar system. Stores default values and settings.';

COMMENT ON COLUMN CALENDAR_CONFIG.CONFIG_ID IS 'Unique identifier for the configuration record.';
COMMENT ON COLUMN CALENDAR_CONFIG.CONFIG_NAME IS 'Name of the configuration parameter.';
COMMENT ON COLUMN CALENDAR_CONFIG.CONFIG_VALUE IS 'JSON object containing the configuration value.';
COMMENT ON COLUMN CALENDAR_CONFIG.DESCRIPTION IS 'Description of the configuration parameter.';
COMMENT ON COLUMN CALENDAR_CONFIG.CREATED_AT IS 'Timestamp when the configuration was created.';
COMMENT ON COLUMN CALENDAR_CONFIG.UPDATED_AT IS 'Timestamp when the configuration was last updated.';
COMMENT ON COLUMN CALENDAR_CONFIG.CREATED_BY IS 'User who created the configuration.';
COMMENT ON COLUMN CALENDAR_CONFIG.UPDATED_BY IS 'User who last updated the configuration.';
COMMENT ON COLUMN CALENDAR_CONFIG.ACTIVE IS 'Flag indicating whether the configuration is active.';

-- Add comments to AU_PUBLIC_HOLIDAYS table
COMMENT ON TABLE AU_PUBLIC_HOLIDAYS IS 'Australian public holidays loaded from data.gov.au. Contains holidays for all Australian jurisdictions.';

COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.HOLIDAY_ID IS 'Unique identifier for the holiday record.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.HOLIDAY_DATE IS 'Date of the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.HOLIDAY_NAME IS 'Name of the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.INFORMATION IS 'Additional information about the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.MORE_INFORMATION IS 'URL or reference for more information about the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.JURISDICTION IS 'Australian jurisdiction (state/territory) where the holiday applies.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.LOADED_AT IS 'Timestamp when the holiday record was loaded.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.LOAD_BATCH_ID IS 'Batch identifier for the load process.';

-- Add comments to AU_PUBLIC_HOLIDAYS_VW view
COMMENT ON VIEW AU_PUBLIC_HOLIDAYS_VW IS 'View of Australian public holidays with simplified column names.';

COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS_VW.DATE IS 'Date of the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS_VW.HOLIDAY_NAME IS 'Name of the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS_VW.STATE IS 'Australian jurisdiction (state/territory) where the holiday applies.';

-- Function to add comments to CALENDAR_SPINE tables
CREATE OR REPLACE PROCEDURE ADD_CALENDAR_SPINE_COMMENTS(
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    TIME_GRAIN VARCHAR
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
    table_name := DATABASE_NAME || '.' || SCHEMA_NAME || '.CALENDAR_SPINE_' || TIME_GRAIN;
    
    -- Add table comment
    comment_sql := 'COMMENT ON TABLE ' || table_name || ' IS ''Calendar spine table for ' || TIME_GRAIN || ' grain. Contains basic date attributes for ' || TIME_GRAIN || '-level analysis.''';
    EXECUTE IMMEDIATE comment_sql;
    
    -- Add column comments based on time grain
    CASE UPPER(TIME_GRAIN)
        WHEN 'SECOND' THEN
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_LTZ IS ''Timestamp in local timezone at second precision.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_TZ IS ''Timestamp converted to the specified timezone at second precision.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DATE IS ''Date part of the timestamp.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.YEAR IS ''Year part of the timestamp.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MONTH IS ''Month part of the timestamp (1-12).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY IS ''Day part of the timestamp (1-31).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.HOUR IS ''Hour part of the timestamp (0-23).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MINUTE IS ''Minute part of the timestamp (0-59).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.SECOND IS ''Second part of the timestamp (0-59).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_WEEK IS ''Day of week (0=Sunday, 1=Monday, ..., 6=Saturday).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_YEAR IS ''Day of year (1-366).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.WEEK_OF_YEAR IS ''Week of year (1-53).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.QUARTER IS ''Quarter (1-4).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_KEY IS ''Integer representation of the timestamp (YYYYMMDDHHMMSS).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMEZONE IS ''Timezone used for the timestamp conversion.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIME_GRAIN IS ''Time grain of the calendar spine (SECOND).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.CREATED_AT IS ''Timestamp when the record was created.''';
            
        WHEN 'MINUTE' THEN
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_LTZ IS ''Timestamp in local timezone at minute precision.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_TZ IS ''Timestamp converted to the specified timezone at minute precision.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DATE IS ''Date part of the timestamp.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.YEAR IS ''Year part of the timestamp.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MONTH IS ''Month part of the timestamp (1-12).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY IS ''Day part of the timestamp (1-31).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.HOUR IS ''Hour part of the timestamp (0-23).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MINUTE IS ''Minute part of the timestamp (0-59).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_WEEK IS ''Day of week (0=Sunday, 1=Monday, ..., 6=Saturday).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_YEAR IS ''Day of year (1-366).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.WEEK_OF_YEAR IS ''Week of year (1-53).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.QUARTER IS ''Quarter (1-4).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_KEY IS ''Integer representation of the timestamp (YYYYMMDDHHMM).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMEZONE IS ''Timezone used for the timestamp conversion.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIME_GRAIN IS ''Time grain of the calendar spine (MINUTE).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.CREATED_AT IS ''Timestamp when the record was created.''';
            
        WHEN 'HOUR' THEN
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_LTZ IS ''Timestamp in local timezone at hour precision.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_TZ IS ''Timestamp converted to the specified timezone at hour precision.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DATE IS ''Date part of the timestamp.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.YEAR IS ''Year part of the timestamp.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MONTH IS ''Month part of the timestamp (1-12).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY IS ''Day part of the timestamp (1-31).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.HOUR IS ''Hour part of the timestamp (0-23).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_WEEK IS ''Day of week (0=Sunday, 1=Monday, ..., 6=Saturday).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_YEAR IS ''Day of year (1-366).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.WEEK_OF_YEAR IS ''Week of year (1-53).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.QUARTER IS ''Quarter (1-4).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_KEY IS ''Integer representation of the timestamp (YYYYMMDDHH).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMEZONE IS ''Timezone used for the timestamp conversion.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIME_GRAIN IS ''Time grain of the calendar spine (HOUR).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.CREATED_AT IS ''Timestamp when the record was created.''';
            
        WHEN 'DAY' THEN
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DATE IS ''Calendar date.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_TZ IS ''Timestamp at start of day in the specified timezone.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.YEAR IS ''Year part of the date.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MONTH IS ''Month part of the date (1-12).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY IS ''Day part of the date (1-31).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_WEEK IS ''Day of week (0=Sunday, 1=Monday, ..., 6=Saturday).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DAY_OF_YEAR IS ''Day of year (1-366).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.WEEK_OF_YEAR IS ''Week of year (1-53).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.QUARTER IS ''Quarter (1-4).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.DATE_KEY IS ''Integer representation of the date (YYYYMMDD).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMEZONE IS ''Timezone used for the timestamp conversion.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIME_GRAIN IS ''Time grain of the calendar spine (DAY).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.CREATED_AT IS ''Timestamp when the record was created.''';
            
        WHEN 'WEEK' THEN
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.WEEK_START_DATE IS ''Start date of the week.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.WEEK_END_DATE IS ''End date of the week.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_TZ IS ''Timestamp at start of week in the specified timezone.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.YEAR IS ''Year part of the week start date.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MONTH IS ''Month part of the week start date (1-12).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.WEEK_OF_YEAR IS ''Week of year (1-53).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.QUARTER IS ''Quarter (1-4).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.WEEK_KEY IS ''Integer representation of the week (YYYYWW).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMEZONE IS ''Timezone used for the timestamp conversion.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIME_GRAIN IS ''Time grain of the calendar spine (WEEK).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.CREATED_AT IS ''Timestamp when the record was created.''';
            
        WHEN 'MONTH' THEN
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MONTH_START_DATE IS ''Start date of the month.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MONTH_END_DATE IS ''End date of the month.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_TZ IS ''Timestamp at start of month in the specified timezone.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.YEAR IS ''Year part of the month.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MONTH IS ''Month part of the date (1-12).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.QUARTER IS ''Quarter (1-4).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.MONTH_KEY IS ''Integer representation of the month (YYYYMM).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMEZONE IS ''Timezone used for the timestamp conversion.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIME_GRAIN IS ''Time grain of the calendar spine (MONTH).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.CREATED_AT IS ''Timestamp when the record was created.''';
            
        WHEN 'YEAR' THEN
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.YEAR_START_DATE IS ''Start date of the year.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.YEAR_END_DATE IS ''End date of the year.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMESTAMP_TZ IS ''Timestamp at start of year in the specified timezone.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.YEAR IS ''Year.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.YEAR_KEY IS ''Integer representation of the year (YYYY).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIMEZONE IS ''Timezone used for the timestamp conversion.''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.TIME_GRAIN IS ''Time grain of the calendar spine (YEAR).''';
            EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || table_name || '.CREATED_AT IS ''Timestamp when the record was created.''';
    END CASE;
    
    RETURN 'Comments added to ' || table_name;
END;
$$;

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
