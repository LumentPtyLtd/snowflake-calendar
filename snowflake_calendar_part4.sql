/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - PART 4
======================================================================================

This script implements the fourth part of a comprehensive calendar system for Australian 
business use cases, including:
- Helper functions for business day calculations
- Unified procedure that brings everything together

Version: 2.0
Date: 31/03/2025
*/

/*
======================================================================================
STEP 10: CREATE HELPER FUNCTIONS FOR BUSINESS DAY CALCULATIONS
======================================================================================
*/

-- Function to add business days to a date
CREATE OR REPLACE FUNCTION ADD_BUSINESS_DAYS(
    start_date DATE,
    num_days INTEGER,
    calendar_table VARCHAR DEFAULT 'BUSINESS_CALENDAR_BASE',
    database_name VARCHAR DEFAULT NULL,
    schema_name VARCHAR DEFAULT NULL
)
RETURNS DATE
LANGUAGE SQL
AS
$$
DECLARE
    v_database VARCHAR;
    v_schema VARCHAR;
    v_result DATE;
BEGIN
    -- Set default database and schema if not provided
    v_database := COALESCE(database_name, CURRENT_DATABASE());
    v_schema := COALESCE(schema_name, CURRENT_SCHEMA());
    
    -- Execute dynamic SQL to add business days
    EXECUTE IMMEDIATE 
    'WITH date_series AS (
        SELECT 
            date, 
            is_trading_day,
            SUM(is_trading_day) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS running_trading_days
        FROM ' || v_database || '.' || v_schema || '.' || calendar_table || '
        WHERE date >= ''' || start_date || '''
        ORDER BY date
    )
    SELECT date
    FROM date_series
    WHERE running_trading_days = (
        SELECT running_trading_days 
        FROM date_series 
        WHERE date = ''' || start_date || '''
    ) + ' || num_days || '
    ORDER BY date
    LIMIT 1'
    INTO v_result;
    
    RETURN v_result;
END;
$$;

-- Function to subtract business days from a date
CREATE OR REPLACE FUNCTION SUBTRACT_BUSINESS_DAYS(
    start_date DATE,
    num_days INTEGER,
    calendar_table VARCHAR DEFAULT 'BUSINESS_CALENDAR_BASE',
    database_name VARCHAR DEFAULT NULL,
    schema_name VARCHAR DEFAULT NULL
)
RETURNS DATE
LANGUAGE SQL
AS
$$
DECLARE
    v_database VARCHAR;
    v_schema VARCHAR;
    v_result DATE;
BEGIN
    -- Set default database and schema if not provided
    v_database := COALESCE(database_name, CURRENT_DATABASE());
    v_schema := COALESCE(schema_name, CURRENT_SCHEMA());
    
    -- Execute dynamic SQL to subtract business days
    EXECUTE IMMEDIATE 
    'WITH date_series AS (
        SELECT 
            date, 
            is_trading_day,
            SUM(is_trading_day) OVER (ORDER BY date DESC ROWS UNBOUNDED PRECEDING) AS running_trading_days
        FROM ' || v_database || '.' || v_schema || '.' || calendar_table || '
        WHERE date <= ''' || start_date || '''
        ORDER BY date DESC
    )
    SELECT date
    FROM date_series
    WHERE running_trading_days = (
        SELECT running_trading_days 
        FROM date_series 
        WHERE date = ''' || start_date || '''
    ) + ' || num_days || '
    ORDER BY date DESC
    LIMIT 1'
    INTO v_result;
    
    RETURN v_result;
END;
$$;

-- Function to count business days between two dates
CREATE OR REPLACE FUNCTION COUNT_BUSINESS_DAYS(
    start_date DATE,
    end_date DATE,
    calendar_table VARCHAR DEFAULT 'BUSINESS_CALENDAR_BASE',
    database_name VARCHAR DEFAULT NULL,
    schema_name VARCHAR DEFAULT NULL
)
RETURNS INTEGER
LANGUAGE SQL
AS
$$
DECLARE
    v_database VARCHAR;
    v_schema VARCHAR;
    v_result INTEGER;
BEGIN
    -- Set default database and schema if not provided
    v_database := COALESCE(database_name, CURRENT_DATABASE());
    v_schema := COALESCE(schema_name, CURRENT_SCHEMA());
    
    -- Execute dynamic SQL to count business days
    EXECUTE IMMEDIATE 
    'SELECT COUNT(*)
    FROM ' || v_database || '.' || v_schema || '.' || calendar_table || '
    WHERE date BETWEEN ''' || start_date || ''' AND ''' || end_date || '''
    AND is_trading_day = 1'
    INTO v_result;
    
    RETURN v_result;
END;
$$;

-- Function to get the next business day
CREATE OR REPLACE FUNCTION NEXT_BUSINESS_DAY(
    start_date DATE,
    calendar_table VARCHAR DEFAULT 'BUSINESS_CALENDAR_BASE',
    database_name VARCHAR DEFAULT NULL,
    schema_name VARCHAR DEFAULT NULL
)
RETURNS DATE
LANGUAGE SQL
AS
$$
DECLARE
    v_database VARCHAR;
    v_schema VARCHAR;
    v_result DATE;
BEGIN
    -- Set default database and schema if not provided
    v_database := COALESCE(database_name, CURRENT_DATABASE());
    v_schema := COALESCE(schema_name, CURRENT_SCHEMA());
    
    -- Execute dynamic SQL to get next business day
    EXECUTE IMMEDIATE 
    'SELECT MIN(date)
    FROM ' || v_database || '.' || v_schema || '.' || calendar_table || '
    WHERE date > ''' || start_date || '''
    AND is_trading_day = 1'
    INTO v_result;
    
    RETURN v_result;
END;
$$;

-- Function to get the previous business day
CREATE OR REPLACE FUNCTION PREVIOUS_BUSINESS_DAY(
    start_date DATE,
    calendar_table VARCHAR DEFAULT 'BUSINESS_CALENDAR_BASE',
    database_name VARCHAR DEFAULT NULL,
    schema_name VARCHAR DEFAULT NULL
)
RETURNS DATE
LANGUAGE SQL
AS
$$
DECLARE
    v_database VARCHAR;
    v_schema VARCHAR;
    v_result DATE;
BEGIN
    -- Set default database and schema if not provided
    v_database := COALESCE(database_name, CURRENT_DATABASE());
    v_schema := COALESCE(schema_name, CURRENT_SCHEMA());
    
    -- Execute dynamic SQL to get previous business day
    EXECUTE IMMEDIATE 
    'SELECT MAX(date)
    FROM ' || v_database || '.' || v_schema || '.' || calendar_table || '
    WHERE date < ''' || start_date || '''
    AND is_trading_day = 1'
    INTO v_result;
    
    RETURN v_result;
END;
$$;

-- Function to check if a date is a business day
CREATE OR REPLACE FUNCTION IS_BUSINESS_DAY(
    check_date DATE,
    calendar_table VARCHAR DEFAULT 'BUSINESS_CALENDAR_BASE',
    database_name VARCHAR DEFAULT NULL,
    schema_name VARCHAR DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
DECLARE
    v_database VARCHAR;
    v_schema VARCHAR;
    v_result BOOLEAN;
BEGIN
    -- Set default database and schema if not provided
    v_database := COALESCE(database_name, CURRENT_DATABASE());
    v_schema := COALESCE(schema_name, CURRENT_SCHEMA());
    
    -- Execute dynamic SQL to check if date is a business day
    EXECUTE IMMEDIATE 
    'SELECT CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END
    FROM ' || v_database || '.' || v_schema || '.' || calendar_table || '
    WHERE date = ''' || check_date || '''
    AND is_trading_day = 1'
    INTO v_result;
    
    RETURN v_result;
END;
$$;

-- Function to get the same day in previous period (month, quarter, year)
CREATE OR REPLACE FUNCTION SAME_DAY_PREVIOUS_PERIOD(
    start_date DATE,
    period_type VARCHAR, -- 'MONTH', 'QUARTER', 'YEAR'
    num_periods INTEGER DEFAULT 1,
    calendar_table VARCHAR DEFAULT 'BUSINESS_CALENDAR_BASE',
    database_name VARCHAR DEFAULT NULL,
    schema_name VARCHAR DEFAULT NULL
)
RETURNS DATE
LANGUAGE SQL
AS
$$
DECLARE
    v_database VARCHAR;
    v_schema VARCHAR;
    v_result DATE;
BEGIN
    -- Set default database and schema if not provided
    v_database := COALESCE(database_name, CURRENT_DATABASE());
    v_schema := COALESCE(schema_name, CURRENT_SCHEMA());
    
    -- Execute dynamic SQL to get same day in previous period
    EXECUTE IMMEDIATE 
    'WITH date_info AS (
        SELECT 
            date, 
            day_of_month_num,
            month_num,
            quarter_num,
            year_num,
            days_in_month_count
        FROM ' || v_database || '.' || v_schema || '.' || calendar_table || '
        WHERE date = ''' || start_date || '''
    )
    SELECT 
        CASE UPPER(''' || period_type || ''')
            WHEN ''MONTH'' THEN 
                CASE 
                    -- Handle month-end dates correctly
                    WHEN di.day_of_month_num = di.days_in_month_count THEN
                        LAST_DAY(DATEADD(MONTH, -' || num_periods || ', di.date))
                    ELSE
                        -- Try to get the same day of month, but adjust if it doesn''t exist
                        CASE
                            WHEN di.day_of_month_num <= DAY(LAST_DAY(DATEADD(MONTH, -' || num_periods || ', di.date))) THEN
                                DATEADD(DAY, di.day_of_month_num - 1, DATE_TRUNC(''MONTH'', DATEADD(MONTH, -' || num_periods || ', di.date)))
                            ELSE
                                LAST_DAY(DATEADD(MONTH, -' || num_periods || ', di.date))
                        END
                END
            WHEN ''QUARTER'' THEN 
                CASE 
                    -- Handle quarter-end dates correctly
                    WHEN di.date = LAST_DAY(DATE_TRUNC(''QUARTER'', di.date) + INTERVAL ''2 MONTHS'') THEN
                        LAST_DAY(DATEADD(QUARTER, -' || num_periods || ', di.date))
                    ELSE
                        -- Try to get the same relative day in quarter
                        DATEADD(DAY, DATEDIFF(DAY, DATE_TRUNC(''QUARTER'', di.date), di.date), 
                                DATE_TRUNC(''QUARTER'', DATEADD(QUARTER, -' || num_periods || ', di.date)))
                END
            WHEN ''YEAR'' THEN 
                CASE 
                    -- Handle Feb 29 in leap years
                    WHEN di.month_num = 2 AND di.day_of_month_num = 29 AND 
                         NOT IS_LEAP_YEAR(YEAR(DATEADD(YEAR, -' || num_periods || ', di.date))) THEN
                        DATE_FROM_PARTS(YEAR(DATEADD(YEAR, -' || num_periods || ', di.date)), 2, 28)
                    ELSE
                        DATEADD(YEAR, -' || num_periods || ', di.date)
                END
            ELSE NULL
        END
    FROM date_info di'
    INTO v_result;
    
    RETURN v_result;
END;
$$;

-- Function to get the same business day in previous period
CREATE OR REPLACE FUNCTION SAME_BUSINESS_DAY_PREVIOUS_PERIOD(
    start_date DATE,
    period_type VARCHAR, -- 'MONTH', 'QUARTER', 'YEAR'
    num_periods INTEGER DEFAULT 1,
    calendar_table VARCHAR DEFAULT 'BUSINESS_CALENDAR_BASE',
    database_name VARCHAR DEFAULT NULL,
    schema_name VARCHAR DEFAULT NULL
)
RETURNS DATE
LANGUAGE SQL
AS
$$
DECLARE
    v_database VARCHAR;
    v_schema VARCHAR;
    v_result DATE;
    v_same_day DATE;
BEGIN
    -- Set default database and schema if not provided
    v_database := COALESCE(database_name, CURRENT_DATABASE());
    v_schema := COALESCE(schema_name, CURRENT_SCHEMA());
    
    -- Get the same calendar day in previous period
    v_same_day := SAME_DAY_PREVIOUS_PERIOD(start_date, period_type, num_periods, calendar_table, v_database, v_schema);
    
    -- If the same day is a business day, return it
    IF IS_BUSINESS_DAY(v_same_day, calendar_table, v_database, v_schema) THEN
        RETURN v_same_day;
    END IF;
    
    -- Otherwise, find the next business day
    v_result := NEXT_BUSINESS_DAY(v_same_day, calendar_table, v_database, v_schema);
    
    RETURN v_result;
END;
$$;

/*
======================================================================================
STEP 11: CREATE UNIFIED PROCEDURE TO BUILD COMPLETE CALENDAR SYSTEM
======================================================================================
*/

CREATE OR REPLACE PROCEDURE BUILD_CALENDAR_SYSTEM(
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    START_DATE VARCHAR DEFAULT NULL,
    END_DATE VARCHAR DEFAULT NULL,
    TIMEZONE VARCHAR DEFAULT 'Australia/Adelaide',
    FISCAL_YEAR_START_MONTH NUMBER DEFAULT 7,
    FISCAL_YEAR_START_DAY NUMBER DEFAULT 1,
    RETAIL_PATTERN VARCHAR DEFAULT '445',
    RETAIL_START_MONTH NUMBER DEFAULT 7,
    RETAIL_WEEK_START_DAY NUMBER DEFAULT 1,
    TIME_GRAINS ARRAY DEFAULT ARRAY_CONSTRUCT('DAY')
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
    v_start_date DATE;
    v_end_date DATE;
    v_start_year NUMBER;
    v_end_year NUMBER;
    v_current_timestamp TIMESTAMP_LTZ;
    v_error_message VARCHAR;
    v_step_result VARIANT;
    v_step_status VARCHAR;
    v_step_message VARCHAR;
    v_step_details VARIANT;
BEGIN
    -- Initialize result object
    SELECT OBJECT_CONSTRUCT(
        'status', 'PENDING',
        'message', 'Starting calendar system build',
        'details', OBJECT_CONSTRUCT(
            'steps', ARRAY_CONSTRUCT(),
            'errors', ARRAY_CONSTRUCT()
        ),
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
        
        -- Extract years for fiscal and retail calendars
        v_start_year := YEAR(v_start_date);
        v_end_year := YEAR(v_end_date);
        
        -- Update result with validated parameters
        SELECT OBJECT_CONSTRUCT(
            'status', 'VALIDATED',
            'message', 'Parameters validated successfully',
            'details', OBJECT_CONSTRUCT(
                'database', DATABASE_NAME,
                'schema', SCHEMA_NAME,
                'start_date', v_start_date,
                'end_date', v_end_date,
                'timezone', TIMEZONE,
                'fiscal_year_start_month', FISCAL_YEAR_START_MONTH,
                'fiscal_year_start_day', FISCAL_YEAR_START_DAY,
                'retail_pattern', RETAIL_PATTERN,
                'retail_start_month', RETAIL_START_MONTH,
                'retail_week_start_day', RETAIL_WEEK_START_DAY,
                'time_grains', TIME_GRAINS,
                'steps', ARRAY_CONSTRUCT(),
                'errors', ARRAY_CONSTRUCT()
            ),
            'timestamp', v_current_timestamp
        ) INTO result;
        
    EXCEPTION
        WHEN OTHER THEN
            -- Log error
            INSERT INTO CALENDAR_ERROR_LOG (
                PROCEDURE_NAME, ERROR_MESSAGE, ERROR_STATE, ERROR_CONTEXT
            ) VALUES (
                'BUILD_CALENDAR_SYSTEM.validation', 
                SQLSTATE || ': ' || SQLERRM, 
                'VALIDATION_ERROR',
                PARSE_JSON(OBJECT_CONSTRUCT(
                    'database', DATABASE_NAME,
                    'schema', SCHEMA_NAME,
                    'start_date', START_DATE,
                    'end_date', END_DATE,
                    'timezone', TIMEZONE,
                    'fiscal_year_start_month', FISCAL_YEAR_START_MONTH,
                    'fiscal_year_start_day', FISCAL_YEAR_START_DAY,
                    'retail_pattern', RETAIL_PATTERN,
                    'retail_start_month', RETAIL_START_MONTH,
                    'retail_week_start_day', RETAIL_WEEK_START_DAY,
                    'time_grains', TIME_GRAINS
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
                    'timezone', TIMEZONE,
                    'fiscal_year_start_month', FISCAL_YEAR_START_MONTH,
                    'fiscal_year_start_day', FISCAL_YEAR_START_DAY,
                    'retail_pattern', RETAIL_PATTERN,
                    'retail_start_month', RETAIL_START_MONTH,
                    'retail_week_start_day', RETAIL_WEEK_START_DAY,
                    'time_grains', TIME_GRAINS
                ),
                'timestamp', v_current_timestamp
            ) INTO result;
            
            RETURN result;
    END;
    
    -- Step 1: Load Australian public holidays
    BEGIN
        CALL LOAD_AU_HOLIDAYS(DATABASE_NAME, SCHEMA_NAME) INTO v_step_result;
        
        v_step_status := v_step_result:status::VARCHAR;
        v_step_message := v_step_result:message::VARCHAR;
        v_step_details := v_step_result:details;
        
        -- Update result with step outcome
        SELECT OBJECT_CONSTRUCT(
            'status', CASE WHEN v_step_status = 'SUCCESS' THEN result:status ELSE 'ERROR' END,
            'message', result:message,
            'details', OBJECT_CONSTRUCT(
                'database', result:details:database,
                'schema', result:details:schema,
                'start_date', result:details:start_date,
                'end_date', result:details:end_date,
                'timezone', result:details:timezone,
                'fiscal_year_start_month', result:details:fiscal_year_start_month,
                'fiscal_year_start_day', result:details:fiscal_year_start_day,
                'retail_pattern', result:details:retail_pattern,
                'retail_start_month', result:details:retail_start_month,
                'retail_week_start_day', result:details:retail_week_start_day,
                'time_grains', result:details:time_grains,
                'steps', ARRAY_APPEND(result:details:steps, OBJECT_CONSTRUCT(
                    'name', 'Load Australian Public Holidays',
                    'status', v_step_status,
                    'message', v_step_message,
                    'details', v_step_details,
                    'timestamp', CURRENT_TIMESTAMP()
                )),
                'errors', CASE 
                    WHEN v_step_status = 'ERROR' 
                    THEN ARRAY_APPEND(result:details:errors, OBJECT_CONSTRUCT(
                        'step', 'Load Australian Public Holidays',
                        'message', v_step_message,
                        'details', v_step_details,
                        'timestamp', CURRENT_TIMESTAMP()
                    ))
                    ELSE result:details:errors
                END
            ),
            'timestamp', CURRENT_TIMESTAMP()
        ) INTO result;
        
        -- If error, return early
        IF v_step_status = 'ERROR' THEN
            RETURN result;
        END IF;
    END;
    
    -- Step 2: Build calendar spines for each time grain
    BEGIN
        -- Loop through each time grain
        FOR i IN 0 TO ARRAY_SIZE(TIME_GRAINS) - 1 DO
            CALL BUILD_CALENDAR_SPINE(
                DATABASE_NAME, 
                SCHEMA_NAME, 
                START_DATE, 
                END_DATE, 
                TIME_GRAINS[i]::VARCHAR, 
                TIMEZONE
            ) INTO v_step_result;
            
            v_step_status := v_step_result:status::VARCHAR;
            v_step_message := v_step_result:message::VARCHAR;
            v_step_details := v_step_result:details;
            
            -- Update result with step outcome
            SELECT OBJECT_CONSTRUCT(
                'status', CASE WHEN v_step_status = 'SUCCESS' THEN result:status ELSE 'ERROR' END,
                'message', result:message,
                'details', OBJECT_CONSTRUCT(
                    'database', result:details:database,
                    'schema', result:details:schema,
                    'start_date', result:details:start_date,
                    'end_date', result:details:end_date,
                    'timezone', result:details:timezone,
                    'fiscal_year_start_month', result:details:fiscal_year_start_month,
                    'fiscal_year_start_day', result:details:fiscal_year_start_day,
                    'retail_pattern', result:details:retail_pattern,
                    'retail_start_month', result:details:retail_start_month,
                    'retail_week_start_day', result:details:retail_week_start_day,
                    'time_grains', result:details:time_grains,
                    'steps', ARRAY_APPEND(result:details:steps, OBJECT_CONSTRUCT(
                        'name', 'Build Calendar Spine - ' || TIME_GRAINS[i]::VARCHAR,
                        'status', v_step_status,
                        'message', v_step_message,
                        'details', v_step_details,
                        'timestamp', CURRENT_TIMESTAMP()
                    )),
                    'errors', CASE 
                        WHEN v_step_status = 'ERROR' 
                        THEN ARRAY_APPEND(result:details:errors, OBJECT_CONSTRUCT(
                            'step', 'Build Calendar Spine - ' || TIME_GRAINS[i]::VARCHAR,
                            'message', v_step_message,
                            'details', v_step_details,
                            'timestamp', CURRENT_TIMESTAMP()
                        ))
                        ELSE result:details:errors
                    END
                ),
                'timestamp', CURRENT_TIMESTAMP()
            ) INTO result;
            
            -- If error, continue with next time grain
            IF v_step_status = 'ERROR' THEN
                CONTINUE;
            END IF;
        END FOR;
    END;
    
    -- Step 3: Build fiscal calendar
    BEGIN
        CALL BUILD_FISCAL_CALENDAR(
            DATABASE_NAME, 
            SCHEMA_NAME, 
            FISCAL_YEAR_START_MONTH, 
            FISCAL_YEAR_START_DAY, 
            v_start_year, 
            v_end_year
        ) INTO v_step_result;
        
        v_step_status := v_step_result:status::VARCHAR;
        v_step_message := v_step_result:message::VARCHAR;
        v_step_details := v_step_result:details;
        
        -- Update result with step outcome
        SELECT OBJECT_CONSTRUCT(
            'status', CASE WHEN v_step_status = 'SUCCESS' THEN result:status ELSE 'ERROR' END,
            'message', result:message,
            'details', OBJECT_CONSTRUCT(
                'database', result:details:database,
                'schema', result:details:schema,
                'start_date', result:details:start_date,
                'end_date', result:details:end_date,
                'timezone', result:details:timezone,
                'fiscal_year_start_month', result:details:fiscal_year_start_month,
                'fiscal_year_start_day', result:details:fiscal_year_start_day,
                'retail_pattern', result:details:retail_pattern,
                'retail_start_month', result:details:retail_start_month,
                'retail_week_start_day', result:details:retail_week_start_day,
                'time_grains', result:details:time_grains,
                'steps', ARRAY_APPEND(result:details:steps, OBJECT_CONSTRUCT(
                    'name', 'Build Fiscal Calendar',
                    'status', v_step_status,
                    'message', v_step_message,
                    'details', v_step_details,
                    'timestamp', CURRENT_TIMESTAMP()
                )),
                'errors', CASE 
                    WHEN v_step_status = 'ERROR' 
                    THEN ARRAY_APPEND(result:details:errors, OBJECT_CONSTRUCT(
                        'step', 'Build Fiscal Calendar',
                        'message', v_step_message,
                        'details', v_step_details,
                        'timestamp', CURRENT_TIMESTAMP()
                    ))
                    ELSE result:details:errors
                END
            ),
            'timestamp', CURRENT_TIMESTAMP()
        ) INTO result;
    END;
    
    -- Step 4: Build retail calendar
    BEGIN
        CALL BUILD_RETAIL_CALENDAR(
            DATABASE_NAME, 
            SCHEMA_NAME, 
            RETAIL_PATTERN, 
            RETAIL_START_MONTH, 
            RETAIL_WEEK_START_DAY, 
            v_start_year, 
            v_end_year
        ) INTO v_step_result;
        
        v_step_status := v_step_result:status::VARCHAR;
        v_step_message := v_step_result:message::VARCHAR;
        v_step_details := v_step_result:details;
        
        -- Update result with step outcome
        SELECT OBJECT_CONSTRUCT(
            'status', CASE WHEN v_step_status = 'SUCCESS' THEN result:status ELSE 'ERROR' END,
            'message', result:message,
            'details', OBJECT_CONSTRUCT(
                'database', result:details:database,
                'schema', result:details:schema,
                'start_date', result:details:start_date,
                'end_date', result:details:end_date,
                'timezone', result:details:timezone,
                'fiscal_year_start_month', result:details:fiscal_year_start_month,
                'fiscal_year_start_day', result:details:fiscal_year_start_day,
                'retail_pattern', result:details:retail_pattern,
                'retail_start_month', result:details:retail_start_month,
                'retail_week_start_day', result:details:retail_week_start_day,
                'time_grains', result:details:time_grains,
                'steps', ARRAY_APPEND(result:details:steps, OBJECT_CONSTRUCT(
                    'name', 'Build Retail Calendar - ' || RETAIL_PATTERN,
                    'status', v_step_status,
                    'message', v_step_message,
                    'details', v_step_details,
                    'timestamp', CURRENT_TIMESTAMP()
                )),
                'errors', CASE 
                    WHEN v_step_status = 'ERROR' 
                    THEN ARRAY_APPEND(result:details:errors, OBJECT_CONSTRUCT(
                        'step', 'Build Retail Calendar - ' || RETAIL_PATTERN,
                        'message', v_step_message,
                        'details', v_step_details,
                        'timestamp', CURRENT_TIMESTAMP()
                    ))
                    ELSE result:details:errors
                END
            ),
            'timestamp', CURRENT_TIMESTAMP()
        ) INTO result;
    END;
    
    -- Step 5: Create unified calendar view
    BEGIN
        -- Create unified calendar view that joins all calendars
        EXECUTE IMMEDIATE '
        CREATE OR REPLACE VIEW ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.UNIFIED_CALENDAR AS
        SELECT
            cs.date,
            cs.date_key,
            cs.year,
            cs.month,
            cs.day,
            cs.day_of_week,
            cs.day_of_year,
            cs.week_of_year,
            cs.quarter,
            cs.timezone,
            h.is_holiday,
            h.is_holiday_nsw,
            h.is_holiday_vic,
            h.is_holiday_qld,
            h.is_holiday_sa,
            h.is_holiday_wa,
            h.is_holiday_tas,
            h.is_holiday_act,
            h.is_holiday_nt,
            h.is_holiday_national,
            h.holiday_names,
            CASE WHEN DAYOFWEEKISO(cs.date) IN (6, 7) THEN 0 ELSE 1 END AS is_weekday,
            CASE WHEN DAYOFWEEKISO(cs.date) IN (6, 7) OR h.is_holiday = 1 THEN 0 ELSE 1 END AS is_trading_day,
            -- Fiscal calendar columns
            fc.fiscal_year,
            fc.fiscal_year_short,
            fc.fiscal_year_long,
            fc.fiscal_month_num,
            fc.fiscal_month_name,
