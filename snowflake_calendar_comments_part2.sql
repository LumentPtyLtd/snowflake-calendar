/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - COLUMN COMMENTS (PART 2)
======================================================================================

This script adds a procedure to add comments to the calendar spine tables.
Execute this script after all other scripts have been executed.

Version: 2.0
Date: 31/03/2025
*/

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