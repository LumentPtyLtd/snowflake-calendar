/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA
======================================================================================

This script implements a comprehensive calendar system for Australian business use cases,
including support for:
- Public holidays from data.gov.au
- Multiple time grains (seconds to years)
- Timezone support with daylight saving handling
- Gregorian, Fiscal, and Retail calendars
- Multiple retail calendar patterns (4-4-5, 4-5-4, 5-4-4)
- Business day helper functions
- Comprehensive error handling and logging

Version: 2.0
Date: 31/03/2025
*/

-- Enable script-level error handling
USE ROLE ACCOUNTADMIN;

-- Set session parameters
ALTER SESSION SET TIMEZONE = 'Australia/Adelaide';
ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = FALSE;
ALTER SESSION SET WEEK_START = 1; -- Monday as start of week (ISO standard)

/*
======================================================================================
STEP 1: CREATE ERROR LOGGING TABLE
======================================================================================
*/

CREATE OR REPLACE TABLE CALENDAR_ERROR_LOG (
    ERROR_ID NUMBER IDENTITY(1,1),
    ERROR_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCEDURE_NAME VARCHAR(100),
    ERROR_MESSAGE VARCHAR(5000),
    ERROR_STATE VARCHAR(50),
    ERROR_CONTEXT VARIANT,
    STACK_TRACE VARCHAR(5000)
);

/*
======================================================================================
STEP 2: SETUP NETWORK RULES FOR DATA.GOV.AU ACCESS
======================================================================================
*/

CREATE OR REPLACE NETWORK RULE allow_data_gov_au 
MODE = EGRESS 
TYPE = HOST_PORT 
VALUE_LIST = ('data.gov.au:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION apis_access_integration
  ALLOWED_NETWORK_RULES = (allow_data_gov_au)
  ENABLED = true;

/*
======================================================================================
STEP 3: CREATE CONFIGURATION TABLE FOR CALENDAR PARAMETERS
======================================================================================
*/

CREATE OR REPLACE TABLE CALENDAR_CONFIG (
    CONFIG_ID NUMBER IDENTITY(1,1),
    CONFIG_NAME VARCHAR(100) NOT NULL,
    CONFIG_VALUE VARIANT NOT NULL,
    DESCRIPTION VARCHAR(1000),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    UPDATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    ACTIVE BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (CONFIG_ID),
    UNIQUE (CONFIG_NAME)
);

-- Insert default configuration values
INSERT INTO CALENDAR_CONFIG (CONFIG_NAME, CONFIG_VALUE, DESCRIPTION)
VALUES 
    ('DATE_RANGE', PARSE_JSON('{"start_date": "2015-01-01", "end_date": "2035-12-31"}'), 'Default date range for calendar generation'),
    ('TIMEZONE', PARSE_JSON('{"timezone": "Australia/Adelaide"}'), 'Default timezone for calendar'),
    ('TIME_GRAIN', PARSE_JSON('{"grain": "day"}'), 'Default time grain (second, minute, hour, day, week, month, year)'),
    ('FISCAL_YEAR', PARSE_JSON('{"start_month": 7, "start_day": 1}'), 'Fiscal year start (July 1)'),
    ('RETAIL_CALENDAR', PARSE_JSON('{"pattern": "445", "start_month": 7, "week_start_day": 1}'), 'Retail calendar pattern (445, 454, 544)');

/*
======================================================================================
STEP 4: CREATE ENHANCED PUBLIC HOLIDAY LOADING PROCEDURE
======================================================================================
*/

CREATE OR REPLACE PROCEDURE LOAD_AU_HOLIDAYS(
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    VALIDATE_ONLY BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
PACKAGES=('pandas==2.2.3','requests==2.32.3','snowflake-snowpark-python==*')
HANDLER='main'
COMMENT='Enhanced procedure to load Australian holidays from data.gov.au with comprehensive error handling'
EXTERNAL_ACCESS_INTEGRATIONS = (apis_access_integration)
AS
$$
import requests
import pandas as pd
import json
import traceback
from datetime import datetime
from snowflake.snowpark import Session

# Constants
STAGE_NAME = "AU_PUBLIC_HOLIDAYS_STAGE"
TABLE_NAME = "AU_PUBLIC_HOLIDAYS"
API_URL = "https://data.gov.au/data/api/action/datastore_search?resource_id=4d4d744b-50ed-45b9-ae77-760bc478ad75"
ERROR_LOG_TABLE = "CALENDAR_ERROR_LOG"

def log_error(session, procedure_name, error_message, error_state, error_context=None, stack_trace=None):
    """
    Log errors to the error logging table
    """
    try:
        error_context_json = json.dumps(error_context) if error_context else None
        
        insert_sql = f"""
        INSERT INTO {DATABASE_NAME}.{SCHEMA_NAME}.{ERROR_LOG_TABLE} (
            PROCEDURE_NAME, ERROR_MESSAGE, ERROR_STATE, ERROR_CONTEXT, STACK_TRACE
        ) VALUES (
            '{procedure_name}', '{error_message.replace("'", "''")}', '{error_state}', 
            PARSE_JSON('{error_context_json if error_context_json else "null"}'), 
            '{stack_trace.replace("'", "''") if stack_trace else None}'
        )
        """
        session.sql(insert_sql).collect()
        return True
    except Exception as e:
        print(f"Error logging to error table: {e}")
        return False

def validate_inputs(database_name, schema_name):
    """
    Validate input parameters
    """
    if not database_name or not isinstance(database_name, str):
        return False, "DATABASE_NAME must be a non-empty string"
    
    if not schema_name or not isinstance(schema_name, str):
        return False, "SCHEMA_NAME must be a non-empty string"
    
    return True, "Validation successful"

def fetch_api_data(session, api_url):
    """
    Fetch data from the data.gov.au API endpoint with enhanced error handling
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        if data['success']:
            return True, data['result']['records'], None
        else:
            error_msg = "API request was not successful"
            log_error(session, "LOAD_AU_HOLIDAYS.fetch_api_data", error_msg, "API_ERROR", 
                     {"api_url": api_url, "response": data})
            return False, None, error_msg

    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching data: {str(e)}"
        stack_trace = traceback.format_exc()
        log_error(session, "LOAD_AU_HOLIDAYS.fetch_api_data", error_msg, "REQUEST_ERROR", 
                 {"api_url": api_url}, stack_trace)
        return False, None, error_msg

def create_stage(session, stage_name, database_name, schema_name):
    """
    Create an internal stage if it doesn't exist
    """
    try:
        fully_qualified_stage = f"{database_name}.{schema_name}.{stage_name}"
        session.sql(f"CREATE STAGE IF NOT EXISTS {fully_qualified_stage}").collect()
        return True, None
    except Exception as e:
        error_msg = f"Error creating stage: {str(e)}"
        stack_trace = traceback.format_exc()
        log_error(session, "LOAD_AU_HOLIDAYS.create_stage", error_msg, "STAGE_ERROR", 
                 {"stage_name": stage_name, "database_name": database_name, "schema_name": schema_name}, 
                 stack_trace)
        return False, error_msg

def load_data_to_table(session, df, table_name, database_name, schema_name):
    """
    Load DataFrame to Snowflake table with enhanced error handling
    """
    try:
        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
            HOLIDAY_ID NUMBER,
            HOLIDAY_DATE DATE,
            HOLIDAY_NAME VARCHAR(255),
            INFORMATION VARCHAR(1000),
            MORE_INFORMATION VARCHAR(255),
            JURISDICTION VARCHAR(10),
            LOADED_AT TIMESTAMP_NTZ,
            LOAD_BATCH_ID VARCHAR(36)
        )
        """
        session.sql(create_table_sql).collect()

        # Process the date column
        df['HOLIDAY_DATE'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.date

        # Rename columns to match table schema
        df = df.rename(columns={
            '_id': 'HOLIDAY_ID',
            'Holiday Name': 'HOLIDAY_NAME',
            'Information': 'INFORMATION',
            'More Information': 'MORE_INFORMATION',
            'Jurisdiction': 'JURISDICTION'
        })

        # Add load timestamp and batch ID
        load_batch_id = datetime.now().strftime('%Y%m%d%H%M%S')
        df['LOADED_AT'] = datetime.now()
        df['LOAD_BATCH_ID'] = load_batch_id

        # Select and reorder columns to match table schema
        df = df[[
            'HOLIDAY_ID',
            'HOLIDAY_DATE',
            'HOLIDAY_NAME',
            'INFORMATION',
            'MORE_INFORMATION',
            'JURISDICTION',
            'LOADED_AT',
            'LOAD_BATCH_ID'
        ]]

        # Convert pandas DataFrame to Snowpark DataFrame
        snowdf = session.create_dataframe(df)

        # Write DataFrame to Snowflake table
        snowdf.write.mode("append").save_as_table(f"{database_name}.{schema_name}.{table_name}")

        return True, f"Successfully loaded {len(df)} rows into {database_name}.{schema_name}.{table_name}", load_batch_id
    
    except Exception as e:
        error_msg = f"Error loading data to table: {str(e)}"
        stack_trace = traceback.format_exc()
        log_error(session, "LOAD_AU_HOLIDAYS.load_data_to_table", error_msg, "DATA_LOAD_ERROR", 
                 {"table_name": table_name, "database_name": database_name, "schema_name": schema_name}, 
                 stack_trace)
        return False, error_msg, None

def main(session, DATABASE_NAME, SCHEMA_NAME, VALIDATE_ONLY=False):
    """
    Main entry point with enhanced error handling and validation
    """
    result = {
        "status": "SUCCESS",
        "message": "",
        "details": {},
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        # Validate inputs
        valid, message = validate_inputs(DATABASE_NAME, SCHEMA_NAME)
        if not valid:
            result["status"] = "ERROR"
            result["message"] = message
            return result
        
        # If validate only, return success
        if VALIDATE_ONLY:
            result["message"] = "Validation successful"
            return result
        
        # Fetch data from API
        success, data, error_msg = fetch_api_data(session, API_URL)
        if not success:
            result["status"] = "ERROR"
            result["message"] = error_msg
            return result
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        result["details"]["record_count"] = len(df)
        
        # Create stage
        success, error_msg = create_stage(session, STAGE_NAME, DATABASE_NAME, SCHEMA_NAME)
        if not success:
            result["status"] = "ERROR"
            result["message"] = error_msg
            return result
        
        # Load data to table
        success, message, batch_id = load_data_to_table(session, df, TABLE_NAME, DATABASE_NAME, SCHEMA_NAME)
        if not success:
            result["status"] = "ERROR"
            result["message"] = message
            return result
        
        result["message"] = message
        result["details"]["batch_id"] = batch_id
        
        # Create view if it doesn't exist
        try:
            create_view_sql = f"""
            CREATE OR REPLACE VIEW {DATABASE_NAME}.{SCHEMA_NAME}.AU_PUBLIC_HOLIDAYS_VW AS
            SELECT
                HOLIDAY_DATE as date,
                HOLIDAY_NAME as holiday_name,
                JURISDICTION as state
            FROM {DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_NAME}
            """
            session.sql(create_view_sql).collect()
            result["details"]["view_created"] = True
        except Exception as e:
            log_error(session, "LOAD_AU_HOLIDAYS.main", f"Error creating view: {str(e)}", "VIEW_ERROR")
            result["details"]["view_created"] = False
            result["details"]["view_error"] = str(e)
        
        return result
        
    except Exception as e:
        error_msg = f"Error in main process: {str(e)}"
        stack_trace = traceback.format_exc()
        log_error(session, "LOAD_AU_HOLIDAYS.main", error_msg, "GENERAL_ERROR", 
                 {"database_name": DATABASE_NAME, "schema_name": SCHEMA_NAME}, 
                 stack_trace)
        
        result["status"] = "ERROR"
        result["message"] = error_msg
        result["details"]["stack_trace"] = stack_trace
        return result
$$;

/*
======================================================================================
STEP 5: CREATE ENHANCED CALENDAR GENERATION PROCEDURE WITH MULTIPLE TIME GRAINS
======================================================================================
*/

CREATE OR REPLACE PROCEDURE BUILD_CALENDAR_SPINE(
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    START_DATE VARCHAR DEFAULT NULL,
    END_DATE VARCHAR DEFAULT NULL,
    TIME_GRAIN VARCHAR DEFAULT 'day',
    TIMEZONE VARCHAR DEFAULT 'Australia/Adelaide'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
    v_start_date DATE;
    v_end_date DATE;
    v_time_grain VARCHAR;
    v_timezone VARCHAR;
    v_table_name VARCHAR;
    v_error_message VARCHAR;
    v_current_timestamp TIMESTAMP_LTZ;
BEGIN
    -- Initialize result object
    SELECT OBJECT_CONSTRUCT(
        'status', 'PENDING',
        'message', '',
        'details', OBJECT_CONSTRUCT(),
        'timestamp', CURRENT_TIMESTAMP()
    ) INTO result;
    
    -- Set current timestamp for logging
    v_current_timestamp := CURRENT_TIMESTAMP();
    
    -- Input validation and parameter setup
    BEGIN
        -- Validate database and schema
        IF DATABASE_NAME IS NULL OR TRIM(DATABASE_NAME) = '' THEN
            v_error_message := 'DATABASE_NAME cannot be null or empty';
            RAISE USING STMTMGR = v_error_message;
        END IF;
        
        IF SCHEMA_NAME IS NULL OR TRIM(SCHEMA_NAME) = '' THEN
            v_error_message := 'SCHEMA_NAME cannot be null or empty';
            RAISE USING STMTMGR = v_error_message;
        END IF;
        
        -- Get default values from config if not provided
        IF START_DATE IS NULL THEN
            SELECT CONFIG_VALUE:start_date::VARCHAR 
            INTO v_start_date 
            FROM CALENDAR_CONFIG 
            WHERE CONFIG_NAME = 'DATE_RANGE' AND ACTIVE = TRUE;
        ELSE
            v_start_date := TRY_TO_DATE(START_DATE);
            IF v_start_date IS NULL THEN
                v_error_message := 'Invalid START_DATE format. Expected YYYY-MM-DD';
                RAISE USING STMTMGR = v_error_message;
            END IF;
        END IF;
        
        IF END_DATE IS NULL THEN
            SELECT CONFIG_VALUE:end_date::VARCHAR 
            INTO v_end_date 
            FROM CALENDAR_CONFIG 
            WHERE CONFIG_NAME = 'DATE_RANGE' AND ACTIVE = TRUE;
        ELSE
            v_end_date := TRY_TO_DATE(END_DATE);
            IF v_end_date IS NULL THEN
                v_error_message := 'Invalid END_DATE format. Expected YYYY-MM-DD';
                RAISE USING STMTMGR = v_error_message;
            END IF;
        END IF;
        
        -- Validate time grain
        v_time_grain := UPPER(TIME_GRAIN);
        IF v_time_grain NOT IN ('SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR') THEN
            v_error_message := 'Invalid TIME_GRAIN. Expected one of: SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR';
            RAISE USING STMTMGR = v_error_message;
        END IF;
        
        -- Validate timezone
        v_timezone := TIMEZONE;
        -- Check if timezone is valid by attempting to use it
        BEGIN
            EXECUTE IMMEDIATE 'SELECT CONVERT_TIMEZONE(''' || v_timezone || ''', CURRENT_TIMESTAMP())';
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := 'Invalid TIMEZONE: ' || v_timezone;
                RAISE USING STMTMGR = v_error_message;
        END;
        
        -- Set table name based on time grain
        v_table_name := DATABASE_NAME || '.' || SCHEMA_NAME || '.CALENDAR_SPINE_' || v_time_grain;
        
        -- Update result with validated parameters
        SELECT OBJECT_CONSTRUCT(
            'status', 'VALIDATED',
            'message', 'Parameters validated successfully',
            'details', OBJECT_CONSTRUCT(
                'database', DATABASE_NAME,
                'schema', SCHEMA_NAME,
                'start_date', v_start_date,
                'end_date', v_end_date,
                'time_grain', v_time_grain,
                'timezone', v_timezone,
                'table_name', v_table_name
            ),
            'timestamp', v_current_timestamp
        ) INTO result;
        
    EXCEPTION
        WHEN OTHER THEN
            -- Log error
            INSERT INTO CALENDAR_ERROR_LOG (
                PROCEDURE_NAME, ERROR_MESSAGE, ERROR_STATE, ERROR_CONTEXT
            ) VALUES (
                'BUILD_CALENDAR_SPINE.validation', 
                SQLSTATE || ': ' || SQLERRM, 
                'VALIDATION_ERROR',
                PARSE_JSON(OBJECT_CONSTRUCT(
                    'database', DATABASE_NAME,
                    'schema', SCHEMA_NAME,
                    'start_date', START_DATE,
                    'end_date', END_DATE,
                    'time_grain', TIME_GRAIN,
                    'timezone', TIMEZONE
                ))
            );
            
            -- Update result with error
            SELECT OBJECT_CONSTRUCT(
                'status', 'ERROR',
                'message', SQLSTATE || ': ' || SQLERRM,
                'details', OBJECT_CONSTRUCT(
                    'error_state', 'VALIDATION_ERROR',
                    'database', DATABASE_NAME,
                    'schema', SCHEMA_NAME,
                    'start_date', START_DATE,
                    'end_date', END_DATE,
                    'time_grain', TIME_GRAIN,
                    'timezone', TIMEZONE
                ),
                'timestamp', v_current_timestamp
            ) INTO result;
            
            RETURN result;
    END;
    
    -- Generate calendar spine based on time grain
    BEGIN
        -- Create appropriate calendar spine based on time grain
        CASE v_time_grain
            WHEN 'SECOND' THEN
                -- Create second-level calendar spine
                EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || v_table_name || ' AS
                WITH RECURSIVE date_range AS (
                    SELECT TO_TIMESTAMP_LTZ(''' || v_start_date || ''') AS ts
                    UNION ALL
                    SELECT DATEADD(SECOND, 1, ts) AS ts
                    FROM date_range
                    WHERE ts < TO_TIMESTAMP_LTZ(''' || v_end_date || ' 23:59:59'')
                )
                SELECT
                    ts AS timestamp_ltz,
                    CONVERT_TIMEZONE(''' || v_timezone || ''', ts) AS timestamp_tz,
                    DATE(timestamp_tz) AS date,
                    YEAR(timestamp_tz) AS year,
                    MONTH(timestamp_tz) AS month,
                    DAY(timestamp_tz) AS day,
                    HOUR(timestamp_tz) AS hour,
                    MINUTE(timestamp_tz) AS minute,
                    SECOND(timestamp_tz) AS second,
                    DAYOFWEEK(timestamp_tz) AS day_of_week,
                    DAYOFYEAR(timestamp_tz) AS day_of_year,
                    WEEKOFYEAR(timestamp_tz) AS week_of_year,
                    QUARTER(timestamp_tz) AS quarter,
                    TO_NUMBER(TO_CHAR(timestamp_tz, ''YYYYMMDDHHMMSS'')) AS timestamp_key,
                    ''' || v_timezone || ''' AS timezone,
                    ''SECOND'' AS time_grain,
                    CURRENT_TIMESTAMP() AS created_at
                FROM date_range
                ORDER BY timestamp_ltz';
                
            WHEN 'MINUTE' THEN
                -- Create minute-level calendar spine
                EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || v_table_name || ' AS
                WITH RECURSIVE date_range AS (
                    SELECT DATE_TRUNC(''MINUTE'', TO_TIMESTAMP_LTZ(''' || v_start_date || ''')) AS ts
                    UNION ALL
                    SELECT DATEADD(MINUTE, 1, ts) AS ts
                    FROM date_range
                    WHERE ts < DATE_TRUNC(''MINUTE'', TO_TIMESTAMP_LTZ(''' || v_end_date || ' 23:59:59''))
                )
                SELECT
                    ts AS timestamp_ltz,
                    CONVERT_TIMEZONE(''' || v_timezone || ''', ts) AS timestamp_tz,
                    DATE(timestamp_tz) AS date,
                    YEAR(timestamp_tz) AS year,
                    MONTH(timestamp_tz) AS month,
                    DAY(timestamp_tz) AS day,
                    HOUR(timestamp_tz) AS hour,
                    MINUTE(timestamp_tz) AS minute,
                    DAYOFWEEK(timestamp_tz) AS day_of_week,
                    DAYOFYEAR(timestamp_tz) AS day_of_year,
                    WEEKOFYEAR(timestamp_tz) AS week_of_year,
                    QUARTER(timestamp_tz) AS quarter,
                    TO_NUMBER(TO_CHAR(timestamp_tz, ''YYYYMMDDHHMM'')) AS timestamp_key,
                    ''' || v_timezone || ''' AS timezone,
                    ''MINUTE'' AS time_grain,
                    CURRENT_TIMESTAMP() AS created_at
                FROM date_range
                ORDER BY timestamp_ltz';
                
            WHEN 'HOUR' THEN
                -- Create hour-level calendar spine
                EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || v_table_name || ' AS
                WITH RECURSIVE date_range AS (
                    SELECT DATE_TRUNC(''HOUR'', TO_TIMESTAMP_LTZ(''' || v_start_date || ''')) AS ts
                    UNION ALL
                    SELECT DATEADD(HOUR, 1, ts) AS ts
                    FROM date_range
                    WHERE ts < DATE_TRUNC(''HOUR'', TO_TIMESTAMP_LTZ(''' || v_end_date || ' 23:59:59''))
                )
                SELECT
                    ts AS timestamp_ltz,
                    CONVERT_TIMEZONE(''' || v_timezone || ''', ts) AS timestamp_tz,
                    DATE(timestamp_tz) AS date,
                    YEAR(timestamp_tz) AS year,
                    MONTH(timestamp_tz) AS month,
                    DAY(timestamp_tz) AS day,
                    HOUR(timestamp_tz) AS hour,
                    DAYOFWEEK(timestamp_tz) AS day_of_week,
                    DAYOFYEAR(timestamp_tz) AS day_of_year,
                    WEEKOFYEAR(timestamp_tz) AS week_of_year,
                    QUARTER(timestamp_tz) AS quarter,
                    TO_NUMBER(TO_CHAR(timestamp_tz, ''YYYYMMDDHH'')) AS timestamp_key,
                    ''' || v_timezone || ''' AS timezone,
                    ''HOUR'' AS time_grain,
                    CURRENT_TIMESTAMP() AS created_at
                FROM date_range
                ORDER BY timestamp_ltz';
                
            WHEN 'DAY' THEN
                -- Create day-level calendar spine
                EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || v_table_name || ' AS
                WITH RECURSIVE date_range AS (
                    SELECT TO_DATE(''' || v_start_date || ''') AS dt
                    UNION ALL
                    SELECT DATEADD(DAY, 1, dt) AS dt
                    FROM date_range
                    WHERE dt < TO_DATE(''' || v_end_date || ''')
                )
                SELECT
                    dt AS date,
                    CONVERT_TIMEZONE(''' || v_timezone || ''', dt::TIMESTAMP_LTZ) AS timestamp_tz,
                    YEAR(dt) AS year,
                    MONTH(dt) AS month,
                    DAY(dt) AS day,
                    DAYOFWEEK(dt) AS day_of_week,
                    DAYOFYEAR(dt) AS day_of_year,
                    WEEKOFYEAR(dt) AS week_of_year,
                    QUARTER(dt) AS quarter,
                    TO_NUMBER(TO_CHAR(dt, ''YYYYMMDD'')) AS date_key,
                    ''' || v_timezone || ''' AS timezone,
                    ''DAY'' AS time_grain,
                    CURRENT_TIMESTAMP() AS created_at
                FROM date_range
                ORDER BY dt';
                
            WHEN 'WEEK' THEN
                -- Create week-level calendar spine
                EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || v_table_name || ' AS
                WITH RECURSIVE date_range AS (
                    SELECT DATE_TRUNC(''WEEK'', TO_DATE(''' || v_start_date || ''')) AS dt
                    UNION ALL
                    SELECT DATEADD(WEEK, 1, dt) AS dt
                    FROM date_range
                    WHERE dt < DATE_TRUNC(''WEEK'', TO_DATE(''' || v_end_date || '''))
                )
                SELECT
                    dt AS week_start_date,
                    DATEADD(DAY, 6, dt) AS week_end_date,
                    CONVERT_TIMEZONE(''' || v_timezone || ''', dt::TIMESTAMP_LTZ) AS timestamp_tz,
                    YEAR(dt) AS year,
                    MONTH(dt) AS month,
                    WEEKOFYEAR(dt) AS week_of_year,
                    QUARTER(dt) AS quarter,
                    TO_NUMBER(TO_CHAR(dt, ''YYYYWW'')) AS week_key,
                    ''' || v_timezone || ''' AS timezone,
                    ''WEEK'' AS time_grain,
                    CURRENT_TIMESTAMP() AS created_at
                FROM date_range
                ORDER BY dt';
                
            WHEN 'MONTH' THEN
                -- Create month-level calendar spine
                EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || v_table_name || ' AS
                WITH RECURSIVE date_range AS (
                    SELECT DATE_TRUNC(''MONTH'', TO_DATE(''' || v_start_date || ''')) AS dt
                    UNION ALL
                    SELECT DATEADD(MONTH, 1, dt) AS dt
                    FROM date_range
                    WHERE dt < DATE_TRUNC(''MONTH'', TO_DATE(''' || v_end_date || '''))
                )
                SELECT
                    dt AS month_start_date,
                    LAST_DAY(dt) AS month_end_date,
                    CONVERT_TIMEZONE(''' || v_timezone || ''', dt::TIMESTAMP_LTZ) AS timestamp_tz,
                    YEAR(dt) AS year,
                    MONTH(dt) AS month,
                    QUARTER(dt) AS quarter,
                    TO_NUMBER(TO_CHAR(dt, ''YYYYMM'')) AS month_key,
                    ''' || v_timezone || ''' AS timezone,
                    ''MONTH'' AS time_grain,
                    CURRENT_TIMESTAMP() AS created_at
                FROM date_range
                ORDER BY dt';
                
            WHEN 'YEAR' THEN
                -- Create year-level calendar spine
                EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || v_table_name || ' AS
                WITH RECURSIVE date_range AS (
                    SELECT DATE_TRUNC(''YEAR'', TO_DATE(''' || v_start_date || ''')) AS dt
                    UNION ALL
                    SELECT DATEADD(YEAR, 1, dt) AS dt
                    FROM date_range
                    WHERE dt < DATE_TRUNC(''YEAR'', TO_DATE(''' || v_end_date || '''))
                )
                SELECT
                    dt AS year_start_date,
                    DATEADD(YEAR, 1, dt) - 1 AS year_end_date,
                    CONVERT_TIMEZONE(''' || v_timezone || ''', dt::TIMESTAMP_LTZ) AS timestamp_tz,
                    YEAR(dt) AS year,
                    TO_NUMBER(TO_CHAR(dt, ''YYYY'')) AS year_key,
                    ''' || v_timezone || ''' AS timezone,
                    ''YEAR'' AS time_grain,
                    CURRENT_TIMESTAMP() AS created_at
                FROM date_range
                ORDER BY dt';
        END CASE;
        
        -- Add clustering to the table
        EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table_name || ' CLUSTER BY (' || 
            CASE v_time_grain
                WHEN 'SECOND' THEN 'timestamp_ltz'
                WHEN 'MINUTE' THEN 'timestamp_ltz'
                WHEN 'HOUR' THEN 'timestamp_ltz'
                WHEN 'DAY' THEN 'date'
                WHEN 'WEEK' THEN 'week_start_date'
                WHEN 'MONTH' THEN 'month_start_date'
                WHEN 'YEAR' THEN 'year_start_date'
            END || ')';
        
        -- Update result with success
