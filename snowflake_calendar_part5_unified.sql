/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - PART 5 (UNIFIED PROCEDURE)
======================================================================================

This script implements the unified procedure that brings everything together for the
Australian calendar system.

Version: 2.0
Date: 31/03/2025
*/

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
            fc.fiscal_quarter_num,
            fc.fiscal_quarter_name,
            fc.fiscal_half_num,
            fc.fiscal_half_name,
            fc.fiscal_week_num,
            fc.fiscal_week_name,
            fc.fiscal_year_start_date,
            fc.fiscal_year_end_date,
            fc.fiscal_month_start_date,
            fc.fiscal_month_end_date,
            fc.fiscal_quarter_start_date,
            fc.fiscal_quarter_end_date,
            fc.fiscal_half_start_date,
            fc.fiscal_half_end_date,
            fc.fiscal_week_start_date,
            fc.fiscal_week_end_date,
            fc.is_fiscal_year_start,
            fc.is_fiscal_year_end,
            fc.is_fiscal_month_start,
            fc.is_fiscal_month_end,
            fc.is_fiscal_quarter_start,
            fc.is_fiscal_quarter_end,
            fc.fiscal_year_month_key,
            fc.fiscal_year_quarter_key,
            -- Retail calendar columns
            rc.retail_year,
            rc.retail_year_short,
            rc.retail_year_long,
            rc.retail_month_num,
            rc.retail_month_name,
            rc.retail_quarter_num,
            rc.retail_quarter_name,
            rc.retail_half_num,
            rc.retail_half_name,
            rc.retail_week_num,
            rc.retail_week_name,
            rc.retail_year_start_date,
            rc.retail_year_end_date,
            rc.retail_month_start_date,
            rc.retail_month_end_date,
            rc.retail_quarter_start_date,
            rc.retail_quarter_end_date,
            rc.retail_half_start_date,
            rc.retail_half_end_date,
            rc.retail_week_start_date,
            rc.retail_week_end_date,
            rc.is_retail_year_start,
            rc.is_retail_year_end,
            rc.is_retail_month_start,
            rc.is_retail_month_end,
            rc.is_retail_quarter_start,
            rc.is_retail_quarter_end,
            rc.is_retail_half_start,
            rc.is_retail_half_end,
            rc.retail_year_month_key,
            rc.retail_year_quarter_key,
            rc.retail_pattern
        FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.CALENDAR_SPINE_DAY cs
        LEFT JOIN (
            SELECT 
                date,
                is_holiday,
                is_holiday_nsw,
                is_holiday_vic,
                is_holiday_qld,
                is_holiday_sa,
                is_holiday_wa,
                is_holiday_tas,
                is_holiday_act,
                is_holiday_nt,
                is_holiday_national,
                holiday_desc AS holiday_names
            FROM ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.BUSINESS_CALENDAR_BASE
        ) h ON cs.date = h.date
        LEFT JOIN ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.FISCAL_CALENDAR fc ON cs.date = fc.date
        LEFT JOIN ' || DATABASE_NAME || '.' || SCHEMA_NAME || '.RETAIL_CALENDAR_' || RETAIL_PATTERN || ' rc ON cs.date = rc.date';
        
        -- Update result with success
        SELECT OBJECT_CONSTRUCT(
            'status', 'SUCCESS',
            'message', 'Calendar system built successfully',
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
                    'name', 'Create Unified Calendar View',
                    'status', 'SUCCESS',
                    'message', 'Unified calendar view created successfully',
                    'details', OBJECT_CONSTRUCT(
                        'view_name', DATABASE_NAME || '.' || SCHEMA_NAME || '.UNIFIED_CALENDAR'
                    ),
                    'timestamp', CURRENT_TIMESTAMP()
                )),
                'errors', result:details:errors
            ),
            'timestamp', CURRENT_TIMESTAMP()
        ) INTO result;
    EXCEPTION
        WHEN OTHER THEN
            -- Log error
            INSERT INTO CALENDAR_ERROR_LOG (
                PROCEDURE_NAME, ERROR_MESSAGE, ERROR_STATE, ERROR_CONTEXT
            ) VALUES (
                'BUILD_CALENDAR_SYSTEM.create_view', 
                SQLSTATE || ': ' || SQLERRM, 
                'VIEW_ERROR',
                PARSE_JSON(OBJECT_CONSTRUCT(
                    'database', DATABASE_NAME,
                    'schema', SCHEMA_NAME,
                    'retail_pattern', RETAIL_PATTERN
                ))
            );
            
            -- Update result with error
            SELECT OBJECT_CONSTRUCT(
                'status', 'ERROR',
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
                    'steps', result:details:steps,
                    'errors', ARRAY_APPEND(result:details:errors, OBJECT_CONSTRUCT(
                        'step', 'Create Unified Calendar View',
                        'message', SQLSTATE || ': ' || SQLERRM,
                        'details', OBJECT_CONSTRUCT(
                            'view_name', DATABASE_NAME || '.' || SCHEMA_NAME || '.UNIFIED_CALENDAR'
                        ),
                        'timestamp', CURRENT_TIMESTAMP()
                    ))
                ),
                'timestamp', CURRENT_TIMESTAMP()
            ) INTO result;
    END;
    
    RETURN result;
END;
$$;

-- Example usage
-- CALL BUILD_CALENDAR_SYSTEM('MY_DATABASE', 'MY_SCHEMA', '2015-01-01', '2035-12-31', 'Australia/Sydney', 7, 1, '445', 7, 1, ARRAY_CONSTRUCT('DAY', 'MONTH', 'YEAR'));