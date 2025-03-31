/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - PART 2
======================================================================================

This script implements the second part of a comprehensive calendar system for Australian 
business use cases, including:
- Date spine creation with multiple time grains
- Timezone support
- Daylight saving handling

Version: 2.0
Date: 31/03/2025
*/

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
    v_row_count NUMBER;
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
        
        -- Get row count
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_table_name INTO v_row_count;
        
        -- Update result with success
        SELECT OBJECT_CONSTRUCT(
            'status', 'SUCCESS',
            'message', 'Calendar spine created successfully',
            'details', OBJECT_CONSTRUCT(
                'database', DATABASE_NAME,
                'schema', SCHEMA_NAME,
                'table_name', v_table_name,
                'time_grain', v_time_grain,
                'timezone', v_timezone,
                'start_date', v_start_date,
                'end_date', v_end_date,
                'row_count', v_row_count
            ),
            'timestamp', CURRENT_TIMESTAMP()
        ) INTO result;
        
    EXCEPTION
        WHEN OTHER THEN
            -- Log error
            INSERT INTO CALENDAR_ERROR_LOG (
                PROCEDURE_NAME, ERROR_MESSAGE, ERROR_STATE, ERROR_CONTEXT
            ) VALUES (
                'BUILD_CALENDAR_SPINE.generation', 
                SQLSTATE || ': ' || SQLERRM, 
                'GENERATION_ERROR',
                PARSE_JSON(OBJECT_CONSTRUCT(
                    'database', DATABASE_NAME,
                    'schema', SCHEMA_NAME,
                    'time_grain', v_time_grain,
                    'timezone', v_timezone,
                    'start_date', v_start_date,
                    'end_date', v_end_date
                ))
            );
            
            -- Update result with error
            SELECT OBJECT_CONSTRUCT(
                'status', 'ERROR',
                'message', SQLSTATE || ': ' || SQLERRM,
                'details', OBJECT_CONSTRUCT(
                    'error_state', 'GENERATION_ERROR',
                    'database', DATABASE_NAME,
                    'schema', SCHEMA_NAME,
                    'time_grain', v_time_grain,
                    'timezone', v_timezone,
                    'start_date', v_start_date,
                    'end_date', v_end_date
                ),
                'timestamp', CURRENT_TIMESTAMP()
            ) INTO result;
    END;
    
    RETURN result;
END;
$$;

/*
======================================================================================
STEP 6: CREATE HELPER FUNCTION TO DETECT DST TRANSITIONS
======================================================================================
*/

CREATE OR REPLACE FUNCTION IS_DST_TRANSITION_DAY(
    check_date DATE,
    timezone VARCHAR
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
DECLARE
    prev_offset NUMBER;
    curr_offset NUMBER;
    next_offset NUMBER;
BEGIN
    -- Get timezone offset for previous day at 12:00
    SELECT EXTRACT(TIMEZONE_HOUR FROM CONVERT_TIMEZONE(timezone, 
           DATEADD(DAY, -1, check_date) || ' 12:00:00'::TIMESTAMP_LTZ))
    INTO prev_offset;
    
    -- Get timezone offset for current day at 12:00
    SELECT EXTRACT(TIMEZONE_HOUR FROM CONVERT_TIMEZONE(timezone, 
           check_date || ' 12:00:00'::TIMESTAMP_LTZ))
    INTO curr_offset;
    
    -- Get timezone offset for next day at 12:00
    SELECT EXTRACT(TIMEZONE_HOUR FROM CONVERT_TIMEZONE(timezone, 
           DATEADD(DAY, 1, check_date) || ' 12:00:00'::TIMESTAMP_LTZ))
    INTO next_offset;
    
    -- If either transition happens, it's a DST transition day
    RETURN (prev_offset != curr_offset) OR (curr_offset != next_offset);
END;
$$;

/*
======================================================================================
STEP 7: CREATE TIMEZONE INFORMATION VIEW
======================================================================================
*/

CREATE OR REPLACE VIEW TIMEZONE_INFO AS
WITH timezone_list AS (
    SELECT 'Australia/Adelaide' AS timezone UNION ALL
    SELECT 'Australia/Brisbane' UNION ALL
    SELECT 'Australia/Broken_Hill' UNION ALL
    SELECT 'Australia/Canberra' UNION ALL
    SELECT 'Australia/Currie' UNION ALL
    SELECT 'Australia/Darwin' UNION ALL
    SELECT 'Australia/Eucla' UNION ALL
    SELECT 'Australia/Hobart' UNION ALL
    SELECT 'Australia/Lindeman' UNION ALL
    SELECT 'Australia/Lord_Howe' UNION ALL
    SELECT 'Australia/Melbourne' UNION ALL
    SELECT 'Australia/Perth' UNION ALL
    SELECT 'Australia/Sydney'
)
SELECT 
    timezone,
    EXTRACT(TIMEZONE_HOUR FROM CONVERT_TIMEZONE(timezone, CURRENT_TIMESTAMP())) AS current_offset_hours,
    EXTRACT(TIMEZONE_MINUTE FROM CONVERT_TIMEZONE(timezone, CURRENT_TIMESTAMP())) AS current_offset_minutes,
    CASE 
        WHEN IS_DST_TRANSITION_DAY(CURRENT_DATE(), timezone) THEN 'DST Transition Day'
        WHEN EXTRACT(TIMEZONE_HOUR FROM CONVERT_TIMEZONE(timezone, CURRENT_TIMESTAMP())) > 
             EXTRACT(TIMEZONE_HOUR FROM CONVERT_TIMEZONE(timezone, DATEADD(MONTH, 6, CURRENT_TIMESTAMP())))
        THEN 'DST Active'
        ELSE 'Standard Time'
    END AS dst_status
FROM timezone_list;