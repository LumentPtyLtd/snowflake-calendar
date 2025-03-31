/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - PART 3
======================================================================================

This script implements the third part of a comprehensive calendar system for Australian 
business use cases, including:
- Enhanced fiscal calendar with configurable options
- Retail calendar with multiple pattern options (4-4-5, 4-5-4, 5-4-4)

Version: 2.0
Date: 31/03/2025
*/

/*
======================================================================================
STEP 8: CREATE FISCAL CALENDAR GENERATION PROCEDURE
======================================================================================
*/

CREATE OR REPLACE PROCEDURE BUILD_FISCAL_CALENDAR(
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    FISCAL_YEAR_START_MONTH NUMBER DEFAULT 7,
    FISCAL_YEAR_START_DAY NUMBER DEFAULT 1,
    START_YEAR NUMBER DEFAULT NULL,
    END_YEAR NUMBER DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
    v_start_year NUMBER;
    v_end_year NUMBER;
    v_fiscal_year_start_month NUMBER;
    v_fiscal_year_start_day NUMBER;
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
        
        -- Validate fiscal year start
        v_fiscal_year_start_month := FISCAL_YEAR_START_MONTH;
        IF v_fiscal_year_start_month < 1 OR v_fiscal_year_start_month > 12 THEN
            v_error_message := 'FISCAL_YEAR_START_MONTH must be between 1 and 12';
            RAISE USING STMTMGR = v_error_message;
        END IF;
        
        v_fiscal_year_start_day := FISCAL_YEAR_START_DAY;
        IF v_fiscal_year_start_day < 1 OR v_fiscal_year_start_day > 31 THEN
            v_error_message := 'FISCAL_YEAR_START_DAY must be between 1 and 31';
            RAISE USING STMTMGR = v_error_message;
        END IF;
        
        -- Validate the day is valid for the month
        BEGIN
            EXECUTE IMMEDIATE 'SELECT DATE_FROM_PARTS(2000, ' || v_fiscal_year_start_month || ', ' || v_fiscal_year_start_day || ')';
        EXCEPTION
            WHEN OTHER THEN
                v_error_message := 'Invalid day ' || v_fiscal_year_start_day || ' for month ' || v_fiscal_year_start_month;
                RAISE USING STMTMGR = v_error_message;
        END;
        
        -- Get default values from config if not provided
        IF START_YEAR IS NULL THEN
            SELECT YEAR(CONFIG_VALUE:start_date::DATE) 
            INTO v_start_year 
            FROM CALENDAR_CONFIG 
            WHERE CONFIG_NAME = 'DATE_RANGE' AND ACTIVE = TRUE;
        ELSE
            v_start_year := START_YEAR;
        END IF;
        
        IF END_YEAR IS NULL THEN
            SELECT YEAR(CONFIG_VALUE:end_date::DATE) 
            INTO v_end_year 
            FROM CALENDAR_CONFIG 
            WHERE CONFIG_NAME = 'DATE_RANGE' AND ACTIVE = TRUE;
        ELSE
            v_end_year := END_YEAR;
        END IF;
        
        -- Set table name
        v_table_name := DATABASE_NAME || '.' || SCHEMA_NAME || '.FISCAL_CALENDAR';
        
        -- Update result with validated parameters
        SELECT OBJECT_CONSTRUCT(
            'status', 'VALIDATED',
            'message', 'Parameters validated successfully',
            'details', OBJECT_CONSTRUCT(
                'database', DATABASE_NAME,
                'schema', SCHEMA_NAME,
                'fiscal_year_start_month', v_fiscal_year_start_month,
                'fiscal_year_start_day', v_fiscal_year_start_day,
                'start_year', v_start_year,
                'end_year', v_end_year,
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
                'BUILD_FISCAL_CALENDAR.validation', 
                SQLSTATE || ': ' || SQLERRM, 
                'VALIDATION_ERROR',
                PARSE_JSON(OBJECT_CONSTRUCT(
                    'database', DATABASE_NAME,
                    'schema', SCHEMA_NAME,
                    'fiscal_year_start_month', FISCAL_YEAR_START_MONTH,
                    'fiscal_year_start_day', FISCAL_YEAR_START_DAY,
                    'start_year', START_YEAR,
                    'end_year', END_YEAR
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
                    'fiscal_year_start_month', FISCAL_YEAR_START_MONTH,
                    'fiscal_year_start_day', FISCAL_YEAR_START_DAY,
                    'start_year', START_YEAR,
                    'end_year', END_YEAR
                ),
                'timestamp', v_current_timestamp
            ) INTO result;
            
            RETURN result;
    END;
    
    -- Generate fiscal calendar
    BEGIN
        -- Create fiscal calendar table
        EXECUTE IMMEDIATE '
        CREATE OR REPLACE TABLE ' || v_table_name || ' AS
        WITH fiscal_years AS (
            SELECT
                fiscal_year,
                DATE_FROM_PARTS(
                    CASE 
                        WHEN ' || v_fiscal_year_start_month || ' = 1 THEN fiscal_year
                        ELSE fiscal_year - 1
                    END,
                    ' || v_fiscal_year_start_month || ',
                    ' || v_fiscal_year_start_day || '
                ) AS fiscal_year_start_date,
                DATEADD(YEAR, 1, DATE_FROM_PARTS(
                    CASE 
                        WHEN ' || v_fiscal_year_start_month || ' = 1 THEN fiscal_year
                        ELSE fiscal_year - 1
                    END,
                    ' || v_fiscal_year_start_month || ',
                    ' || v_fiscal_year_start_day || '
                )) - 1 AS fiscal_year_end_date
            FROM (
                SELECT YEAR AS fiscal_year
                FROM TABLE(GENERATOR(ROWCOUNT => (' || v_end_year || ' - ' || v_start_year || ' + 1)))
                ORDER BY fiscal_year
            ) years
            WHERE fiscal_year BETWEEN ' || v_start_year || ' AND ' || v_end_year || '
        ),
        date_range AS (
            SELECT
                MIN(fiscal_year_start_date) AS min_date,
                MAX(fiscal_year_end_date) AS max_date
            FROM fiscal_years
        ),
        all_dates AS (
            SELECT
                DATEADD(DAY, seq, min_date) AS date
            FROM date_range, TABLE(GENERATOR(ROWCOUNT => DATEDIFF(DAY, min_date, max_date) + 1)) seq
        ),
        fiscal_calendar_base AS (
            SELECT
                d.date,
                fy.fiscal_year,
                fy.fiscal_year_start_date,
                fy.fiscal_year_end_date,
                DATEDIFF(DAY, fy.fiscal_year_start_date, d.date) + 1 AS day_of_fiscal_year,
                DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) + 1 AS fiscal_month_num,
                CASE
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 3 THEN 1
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 6 THEN 2
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 9 THEN 3
                    ELSE 4
                END AS fiscal_quarter_num,
                CASE
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 6 THEN 1
                    ELSE 2
                END AS fiscal_half_num,
                FLOOR(DATEDIFF(DAY, fy.fiscal_year_start_date, d.date) / 7) + 1 AS fiscal_week_num,
                ''FY'' || RIGHT(fy.fiscal_year, 2) AS fiscal_year_short,
                ''FY'' || fy.fiscal_year AS fiscal_year_long,
                DATEADD(MONTH, DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date), fy.fiscal_year_start_date) AS fiscal_month_start_date,
                DATEADD(DAY, -1, DATEADD(MONTH, DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) + 1, fy.fiscal_year_start_date)) AS fiscal_month_end_date,
                DATEADD(QUARTER, CASE
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 3 THEN 0
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 6 THEN 1
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 9 THEN 2
                    ELSE 3
                END, fy.fiscal_year_start_date) AS fiscal_quarter_start_date,
                DATEADD(DAY, -1, DATEADD(QUARTER, CASE
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 3 THEN 1
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 6 THEN 2
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 9 THEN 3
                    ELSE 4
                END, fy.fiscal_year_start_date)) AS fiscal_quarter_end_date,
                CASE
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 6 
                    THEN fy.fiscal_year_start_date
                    ELSE DATEADD(MONTH, 6, fy.fiscal_year_start_date)
                END AS fiscal_half_start_date,
                CASE
                    WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 6 
                    THEN DATEADD(DAY, -1, DATEADD(MONTH, 6, fy.fiscal_year_start_date))
                    ELSE fy.fiscal_year_end_date
                END AS fiscal_half_end_date,
                DATEADD(WEEK, FLOOR(DATEDIFF(DAY, fy.fiscal_year_start_date, d.date) / 7), fy.fiscal_year_start_date) AS fiscal_week_start_date,
                DATEADD(DAY, 6, DATEADD(WEEK, FLOOR(DATEDIFF(DAY, fy.fiscal_year_start_date, d.date) / 7), fy.fiscal_year_start_date)) AS fiscal_week_end_date,
                CASE 
                    WHEN MONTH(d.date) = ' || v_fiscal_year_start_month || ' AND DAY(d.date) = ' || v_fiscal_year_start_day || '
                    THEN 1 ELSE 0 
                END AS is_fiscal_year_start,
                CASE 
                    WHEN MONTH(d.date) = MONTH(fy.fiscal_year_end_date) AND DAY(d.date) = DAY(fy.fiscal_year_end_date)
                    THEN 1 ELSE 0 
                END AS is_fiscal_year_end,
                CASE 
                    WHEN DAY(d.date) = 1 THEN 1 ELSE 0 
                END AS is_fiscal_month_start,
                CASE 
                    WHEN d.date = DATEADD(DAY, -1, DATEADD(MONTH, DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) + 1, fy.fiscal_year_start_date))
                    THEN 1 ELSE 0 
                END AS is_fiscal_month_end,
                CASE 
                    WHEN d.date = DATEADD(QUARTER, CASE
                        WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 3 THEN 0
                        WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 6 THEN 1
                        WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 9 THEN 2
                        ELSE 3
                    END, fy.fiscal_year_start_date)
                    THEN 1 ELSE 0 
                END AS is_fiscal_quarter_start,
                CASE 
                    WHEN d.date = DATEADD(DAY, -1, DATEADD(QUARTER, CASE
                        WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 3 THEN 1
                        WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 6 THEN 2
                        WHEN DATEDIFF(MONTH, fy.fiscal_year_start_date, d.date) < 9 THEN 3
                        ELSE 4
                    END, fy.fiscal_year_start_date))
                    THEN 1 ELSE 0 
                END AS is_fiscal_quarter_end,
                CURRENT_TIMESTAMP() AS created_at,
                ''BUILD_FISCAL_CALENDAR'' AS created_by,
                ' || v_fiscal_year_start_month || ' AS fiscal_year_start_month,
                ' || v_fiscal_year_start_day || ' AS fiscal_year_start_day
            FROM all_dates d
            JOIN fiscal_years fy
                ON d.date BETWEEN fy.fiscal_year_start_date AND fy.fiscal_year_end_date
        )
        SELECT
            date,
            fiscal_year,
            fiscal_year_short,
            fiscal_year_long,
            fiscal_year_start_date,
            fiscal_year_end_date,
            day_of_fiscal_year,
            fiscal_month_num,
            fiscal_quarter_num,
            fiscal_half_num,
            fiscal_week_num,
            fiscal_month_start_date,
            fiscal_month_end_date,
            fiscal_quarter_start_date,
            fiscal_quarter_end_date,
            fiscal_half_start_date,
            fiscal_half_end_date,
            fiscal_week_start_date,
            fiscal_week_end_date,
            is_fiscal_year_start,
            is_fiscal_year_end,
            is_fiscal_month_start,
            is_fiscal_month_end,
            is_fiscal_quarter_start,
            is_fiscal_quarter_end,
            fiscal_year * 100 + fiscal_month_num AS fiscal_year_month_key,
            fiscal_year * 10 + fiscal_quarter_num AS fiscal_year_quarter_key,
            fiscal_year * 100 + fiscal_week_num AS fiscal_year_week_key,
            CASE 
                WHEN fiscal_month_num = 1 THEN ''Month 01''
                WHEN fiscal_month_num = 2 THEN ''Month 02''
                WHEN fiscal_month_num = 3 THEN ''Month 03''
                WHEN fiscal_month_num = 4 THEN ''Month 04''
                WHEN fiscal_month_num = 5 THEN ''Month 05''
                WHEN fiscal_month_num = 6 THEN ''Month 06''
                WHEN fiscal_month_num = 7 THEN ''Month 07''
                WHEN fiscal_month_num = 8 THEN ''Month 08''
                WHEN fiscal_month_num = 9 THEN ''Month 09''
                WHEN fiscal_month_num = 10 THEN ''Month 10''
                WHEN fiscal_month_num = 11 THEN ''Month 11''
                WHEN fiscal_month_num = 12 THEN ''Month 12''
            END AS fiscal_month_name,
            ''Q'' || fiscal_quarter_num AS fiscal_quarter_name,
            ''H'' || fiscal_half_num AS fiscal_half_name,
            ''W'' || LPAD(fiscal_week_num, 2, ''0'') AS fiscal_week_name,
            created_at,
            created_by,
            fiscal_year_start_month,
            fiscal_year_start_day
        FROM fiscal_calendar_base
        ORDER BY date';
        
        -- Add clustering to the table
        EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table_name || ' CLUSTER BY (date)';
        
        -- Get row count
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_table_name INTO v_row_count;
        
        -- Update result with success
        SELECT OBJECT_CONSTRUCT(
            'status', 'SUCCESS',
            'message', 'Fiscal calendar created successfully',
            'details', OBJECT_CONSTRUCT(
                'database', DATABASE_NAME,
                'schema', SCHEMA_NAME,
                'table_name', v_table_name,
                'fiscal_year_start_month', v_fiscal_year_start_month,
                'fiscal_year_start_day', v_fiscal_year_start_day,
                'start_year', v_start_year,
                'end_year', v_end_year,
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
                'BUILD_FISCAL_CALENDAR.generation', 
                SQLSTATE || ': ' || SQLERRM, 
                'GENERATION_ERROR',
                PARSE_JSON(OBJECT_CONSTRUCT(
                    'database', DATABASE_NAME,
                    'schema', SCHEMA_NAME,
                    'fiscal_year_start_month', v_fiscal_year_start_month,
                    'fiscal_year_start_day', v_fiscal_year_start_day,
                    'start_year', v_start_year,
                    'end_year', v_end_year
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
                    'fiscal_year_start_month', v_fiscal_year_start_month,
                    'fiscal_year_start_day', v_fiscal_year_start_day,
                    'start_year', v_start_year,
                    'end_year', v_end_year
                ),
                'timestamp', CURRENT_TIMESTAMP()
            ) INTO result;
    END;
    
    RETURN result;
END;
$$;

/*
======================================================================================
STEP 9: CREATE RETAIL CALENDAR GENERATION PROCEDURE WITH MULTIPLE PATTERNS
======================================================================================
*/

CREATE OR REPLACE PROCEDURE BUILD_RETAIL_CALENDAR(
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    PATTERN VARCHAR DEFAULT '445',
    START_MONTH NUMBER DEFAULT 7,
    WEEK_START_DAY NUMBER DEFAULT 1,
    START_YEAR NUMBER DEFAULT NULL,
    END_YEAR NUMBER DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
    v_start_year NUMBER;
    v_end_year NUMBER;
    v_pattern VARCHAR;
    v_start_month NUMBER;
    v_week_start_day NUMBER;
    v_table_name VARCHAR;
    v_error_message VARCHAR;
    v_current_timestamp TIMESTAMP_LTZ;
    v_row_count NUMBER;
    v_pattern_array ARRAY;
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
        
        -- Validate pattern
        v_pattern := UPPER(PATTERN);
        IF v_pattern NOT IN ('445', '454', '544') THEN
            v_error_message := 'PATTERN must be one of: 445, 454, 544';
            RAISE USING STMTMGR = v_error_message;
        END IF;
        
        -- Set pattern array based on selected pattern
        CASE v_pattern
            WHEN '445' THEN
                v_pattern_array := ARRAY_CONSTRUCT(4, 4, 5);
            WHEN '454' THEN
                v_pattern_array := ARRAY_CONSTRUCT(4, 5, 4);
            WHEN '544' THEN
                v_pattern_array := ARRAY_CONSTRUCT(5, 4, 4);
        END CASE;
        
        -- Validate start month
        v_start_month := START_MONTH;
        IF v_start_month < 1 OR v_start_month > 12 THEN
            v_error_message := 'START_MONTH must be between 1 and 12';
            RAISE USING STMTMGR = v_error_message;
        END IF;
        
        -- Validate week start day
        v_week_start_day := WEEK_START_DAY;
        IF v_week_start_day < 0 OR v_week_start_day > 6 THEN
            v_error_message := 'WEEK_START_DAY must be between 0 (Sunday) and 6 (Saturday)';
            RAISE USING STMTMGR = v_error_message;
        END IF;
        
        -- Get default values from config if not provided
        IF START_YEAR IS NULL THEN
            SELECT YEAR(CONFIG_VALUE:start_date::DATE) 
            INTO v_start_year 
            FROM CALENDAR_CONFIG 
            WHERE CONFIG_NAME = 'DATE_RANGE' AND ACTIVE = TRUE;
        ELSE
            v_start_year := START_YEAR;
        END IF;
        
        IF END_YEAR IS NULL THEN
            SELECT YEAR(CONFIG_VALUE:end_date::DATE) 
            INTO v_end_year 
            FROM CALENDAR_CONFIG 
            WHERE CONFIG_NAME = 'DATE_RANGE' AND ACTIVE = TRUE;
        ELSE
            v_end_year := END_YEAR;
        END IF;
        
        -- Set table name
        v_table_name := DATABASE_NAME || '.' || SCHEMA_NAME || '.RETAIL_CALENDAR_' || v_pattern;
        
        -- Update result with validated parameters
        SELECT OBJECT_CONSTRUCT(
            'status', 'VALIDATED',
            'message', 'Parameters validated successfully',
            'details', OBJECT_CONSTRUCT(
                'database', DATABASE_NAME,
                'schema', SCHEMA_NAME,
                'pattern', v_pattern,
                'start_month', v_start_month,
                'week_start_day', v_week_start_day,
                'start_year', v_start_year,
                'end_year', v_end_year,
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
                'BUILD_RETAIL_CALENDAR.validation', 
                SQLSTATE || ': ' || SQLERRM, 
                'VALIDATION_ERROR',
                PARSE_JSON(OBJECT_CONSTRUCT(
                    'database', DATABASE_NAME,
                    'schema', SCHEMA_NAME,
                    'pattern', PATTERN,
                    'start_month', START_MONTH,
                    'week_start_day', WEEK_START_DAY,
                    'start_year', START_YEAR,
                    'end_year', END_YEAR
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
                    'pattern', PATTERN,
                    'start_month', START_MONTH,
                    'week_start_day', WEEK_START_DAY,
                    'start_year', START_YEAR,
                    'end_year', END_YEAR
                ),
                'timestamp', v_current_timestamp
            ) INTO result;
            
            RETURN result;
    END;
    
    -- Generate retail calendar
    BEGIN
        -- Create retail calendar table
        EXECUTE IMMEDIATE '
        CREATE OR REPLACE TABLE ' || v_table_name || ' AS
        WITH retail_years AS (
            SELECT
                retail_year,
                -- Find the first day of the start month
                DATE_FROM_PARTS(
                    CASE 
                        WHEN ' || v_start_month || ' = 1 THEN retail_year
                        ELSE retail_year - 1
                    END,
                    ' || v_start_month || ',
                    1
                ) AS month_start_date,
                -- Find the first occurrence of the week start day
                DATEADD(DAY, 
                    CASE
                        -- If the first day of month is already the week start day
                        WHEN DAYOFWEEK(month_start_date) = ' || v_week_start_day || ' THEN 0
                        -- Otherwise, find days until next week start day
                        WHEN DAYOFWEEK(month_start_date) < ' || v_week_start_day || ' 
                            THEN ' || v_week_start_day || ' - DAYOFWEEK(month_start_date)
                        ELSE 7 - (DAYOFWEEK(month_start_date) - ' || v_week_start_day || ')
                    END,
                    month_start_date
                ) AS retail_year_start_date
            FROM (
                SELECT YEAR AS retail_year
                FROM TABLE(GENERATOR(ROWCOUNT => (' || v_end_year || ' - ' || v_start_year || ' + 1)))
                ORDER BY retail_year
            ) years
            WHERE retail_year BETWEEN ' || v_start_year || ' AND ' || v_end_year || '
        ),
        retail_years_processed AS (
            SELECT
                retail_year,
                retail_year_start_date,
                LEAD(retail_year_start_date, 1, DATEADD(YEAR, 1, retail_year_start_date)) OVER (ORDER BY retail_year) - 1 AS retail_year_end_date,
                DATEDIFF(WEEK, retail_year_start_date, LEAD(retail_year_start_date, 1, DATEADD(YEAR, 1, retail_year_start_date)) OVER (ORDER BY retail_year)) AS weeks_in_year
            FROM retail_years
        ),
        date_range AS (
            SELECT
                MIN(retail_year_start_date) AS min_date,
                MAX(retail_year_end_date) AS max_date
            FROM retail_years_processed
        ),
        all_dates AS (
            SELECT
                DATEADD(DAY, seq, min_date) AS date
            FROM date_range, TABLE(GENERATOR(ROWCOUNT => DATEDIFF(DAY, min_date, max_date) + 1)) seq
        ),
        retail_weeks AS (
            SELECT
                ry.retail_year,
                ry.retail_year_start_date,
                ry.retail_year_end_date,
                ry.weeks_in_year,
                week_num,
                DATEADD(WEEK, week_num - 1, ry.retail_year_start_date) AS week_start_date,
                DATEADD(DAY, 6, DATEADD(WEEK, week_num - 1, ry.retail_year_start_date)) AS week_end_date,
                CASE
                    WHEN week_num <= 4 THEN 1
                    WHEN week_num <= 8 THEN 2
