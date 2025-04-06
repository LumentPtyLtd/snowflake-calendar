/*
======================================================================================
SNOWFLAKE CALENDAR SYSTEM - EXECUTION SCRIPT
======================================================================================

This script provides a simple way to run the parameterized calendar system script
with your specific warehouse, database, and schema values.

Instructions:
1. Set the values for WAREHOUSE_NAME, DATABASE_NAME, and SCHEMA_NAME below
2. Run this script in Snowflake
*/

-- Set your values here
SET WAREHOUSE_NAME = 'COMPUTE_WH';  -- e.g., 'COMPUTE_WH'
SET DATABASE_NAME = 'SPG_DAP01';    -- e.g., 'CALENDAR_DB'
SET SCHEMA_NAME = 'PBI';        -- e.g., 'PUBLIC'

-- Verify the values (optional)
SELECT $WAREHOUSE_NAME as WAREHOUSE, $DATABASE_NAME as DATABASE, $SCHEMA_NAME as SCHEMA;

-- Run the main script with these parameters
!source snowflake_calendar_parameterized.sql