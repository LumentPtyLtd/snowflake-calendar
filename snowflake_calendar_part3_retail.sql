/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - PART 3 (RETAIL CALENDAR)
======================================================================================

This script implements the retail calendar part of a comprehensive calendar system 
for Australian business use cases, including:
- Retail calendar with multiple pattern options (4-4-5, 4-5-4, 5-4-4)

Version: 2.0
Date: 31/03/2025
*/

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
                    WHEN week_num <= 13 THEN 3
                    WHEN week_num <= 17 THEN 4
                    WHEN week_num <= 21 THEN 5
                    WHEN week_num <= 26 THEN 6
                    WHEN week_num <= 30 THEN 7
                    WHEN week_num <= 34 THEN 8
                    WHEN week_num <= 39 THEN 9
                    WHEN week_num <= 43 THEN 10
                    WHEN week_num <= 47 THEN 11
                    ELSE 12
                END AS retail_month_num,
                CASE
                    WHEN week_num <= 13 THEN 1
                    WHEN week_num <= 26 THEN 2
                    WHEN week_num <= 39 THEN 3
                    ELSE 4
                END AS retail_quarter_num,
                CASE
                    WHEN week_num <= 26 THEN 1
                    ELSE 2
                END AS retail_half_num,
                -- Pattern-specific month calculations
                CASE
                    -- 4-4-5 pattern
                    WHEN ''' || v_pattern || ''' = ''445'' THEN
                        CASE
                            WHEN MOD(retail_month_num - 1, 3) = 0 THEN 4 -- First month: 4 weeks
                            WHEN MOD(retail_month_num - 1, 3) = 1 THEN 4 -- Second month: 4 weeks
                            WHEN MOD(retail_month_num - 1, 3) = 2 THEN 5 -- Third month: 5 weeks
                        END
                    -- 4-5-4 pattern
                    WHEN ''' || v_pattern || ''' = ''454'' THEN
                        CASE
                            WHEN MOD(retail_month_num - 1, 3) = 0 THEN 4 -- First month: 4 weeks
                            WHEN MOD(retail_month_num - 1, 3) = 1 THEN 5 -- Second month: 5 weeks
                            WHEN MOD(retail_month_num - 1, 3) = 2 THEN 4 -- Third month: 4 weeks
                        END
                    -- 5-4-4 pattern
                    WHEN ''' || v_pattern || ''' = ''544'' THEN
                        CASE
                            WHEN MOD(retail_month_num - 1, 3) = 0 THEN 5 -- First month: 5 weeks
                            WHEN MOD(retail_month_num - 1, 3) = 1 THEN 4 -- Second month: 4 weeks
                            WHEN MOD(retail_month_num - 1, 3) = 2 THEN 4 -- Third month: 4 weeks
                        END
                END AS weeks_in_month
            FROM retail_years_processed ry,
            TABLE(GENERATOR(ROWCOUNT => ry.weeks_in_year)) week_gen
            CROSS JOIN LATERAL (SELECT week_gen.SEQ + 1 AS week_num) w
        ),
        retail_months AS (
            SELECT
                retail_year,
                retail_month_num,
                MIN(week_start_date) AS month_start_date,
                MAX(week_end_date) AS month_end_date,
                retail_quarter_num,
                retail_half_num,
                SUM(weeks_in_month) AS total_weeks_in_month
            FROM retail_weeks
            GROUP BY retail_year, retail_month_num, retail_quarter_num, retail_half_num
        ),
        retail_quarters AS (
            SELECT
                retail_year,
                retail_quarter_num,
                MIN(month_start_date) AS quarter_start_date,
                MAX(month_end_date) AS quarter_end_date,
                retail_half_num,
                SUM(total_weeks_in_month) AS total_weeks_in_quarter
            FROM retail_months
            GROUP BY retail_year, retail_quarter_num, retail_half_num
        ),
        retail_halves AS (
            SELECT
                retail_year,
                retail_half_num,
                MIN(quarter_start_date) AS half_start_date,
                MAX(quarter_end_date) AS half_end_date,
                SUM(total_weeks_in_quarter) AS total_weeks_in_half
            FROM retail_quarters
            GROUP BY retail_year, retail_half_num
        ),
        retail_calendar_base AS (
            SELECT
                d.date,
                ry.retail_year,
                ry.retail_year_start_date,
                ry.retail_year_end_date,
                rw.week_num AS retail_week_num,
                rw.week_start_date AS retail_week_start_date,
                rw.week_end_date AS retail_week_end_date,
                rw.retail_month_num,
                rm.month_start_date AS retail_month_start_date,
                rm.month_end_date AS retail_month_end_date,
                rw.retail_quarter_num,
                rq.quarter_start_date AS retail_quarter_start_date,
                rq.quarter_end_date AS retail_quarter_end_date,
                rw.retail_half_num,
                rh.half_start_date AS retail_half_start_date,
                rh.half_end_date AS retail_half_end_date,
                rw.weeks_in_month,
                rm.total_weeks_in_month,
                rq.total_weeks_in_quarter,
                rh.total_weeks_in_half,
                ry.weeks_in_year,
                CASE 
                    WHEN d.date = ry.retail_year_start_date THEN 1 ELSE 0 
                END AS is_retail_year_start,
                CASE 
                    WHEN d.date = ry.retail_year_end_date THEN 1 ELSE 0 
                END AS is_retail_year_end,
                CASE 
                    WHEN d.date = rw.week_start_date THEN 1 ELSE 0 
                END AS is_retail_week_start,
                CASE 
                    WHEN d.date = rw.week_end_date THEN 1 ELSE 0 
                END AS is_retail_week_end,
                CASE 
                    WHEN d.date = rm.month_start_date THEN 1 ELSE 0 
                END AS is_retail_month_start,
                CASE 
                    WHEN d.date = rm.month_end_date THEN 1 ELSE 0 
                END AS is_retail_month_end,
                CASE 
                    WHEN d.date = rq.quarter_start_date THEN 1 ELSE 0 
                END AS is_retail_quarter_start,
                CASE 
                    WHEN d.date = rq.quarter_end_date THEN 1 ELSE 0 
                END AS is_retail_quarter_end,
                CASE 
                    WHEN d.date = rh.half_start_date THEN 1 ELSE 0 
                END AS is_retail_half_start,
                CASE 
                    WHEN d.date = rh.half_end_date THEN 1 ELSE 0 
                END AS is_retail_half_end,
                DATEDIFF(DAY, ry.retail_year_start_date, d.date) + 1 AS day_of_retail_year,
                DATEDIFF(DAY, rw.week_start_date, d.date) + 1 AS day_of_retail_week,
                DATEDIFF(DAY, rm.month_start_date, d.date) + 1 AS day_of_retail_month,
                DATEDIFF(DAY, rq.quarter_start_date, d.date) + 1 AS day_of_retail_quarter,
                DATEDIFF(DAY, rh.half_start_date, d.date) + 1 AS day_of_retail_half,
                CURRENT_TIMESTAMP() AS created_at,
                ''BUILD_RETAIL_CALENDAR'' AS created_by,
                ''' || v_pattern || ''' AS retail_pattern,
                ' || v_start_month || ' AS retail_start_month,
                ' || v_week_start_day || ' AS retail_week_start_day
            FROM all_dates d
            JOIN retail_years_processed ry
                ON d.date BETWEEN ry.retail_year_start_date AND ry.retail_year_end_date
            JOIN retail_weeks rw
                ON ry.retail_year = rw.retail_year
                AND d.date BETWEEN rw.week_start_date AND rw.week_end_date
            JOIN retail_months rm
                ON ry.retail_year = rm.retail_year
                AND rw.retail_month_num = rm.retail_month_num
            JOIN retail_quarters rq
                ON ry.retail_year = rq.retail_year
                AND rw.retail_quarter_num = rq.retail_quarter_num
            JOIN retail_halves rh
                ON ry.retail_year = rh.retail_year
                AND rw.retail_half_num = rh.retail_half_num
        )
        SELECT
            date,
            retail_year,
            ''R'' || RIGHT(retail_year, 2) AS retail_year_short,
            ''R'' || retail_year AS retail_year_long,
            retail_year_start_date,
            retail_year_end_date,
            retail_week_num,
            retail_week_start_date,
            retail_week_end_date,
            retail_month_num,
            retail_month_start_date,
            retail_month_end_date,
            retail_quarter_num,
            retail_quarter_start_date,
            retail_quarter_end_date,
            retail_half_num,
            retail_half_start_date,
            retail_half_end_date,
            weeks_in_month,
            total_weeks_in_month,
            total_weeks_in_quarter,
            total_weeks_in_half,
            weeks_in_year,
            is_retail_year_start,
            is_retail_year_end,
            is_retail_week_start,
            is_retail_week_end,
            is_retail_month_start,
            is_retail_month_end,
            is_retail_quarter_start,
            is_retail_quarter_end,
            is_retail_half_start,
            is_retail_half_end,
            day_of_retail_year,
            day_of_retail_week,
            day_of_retail_month,
            day_of_retail_quarter,
            day_of_retail_half,
            retail_year * 100 + retail_month_num AS retail_year_month_key,
            retail_year * 10 + retail_quarter_num AS retail_year_quarter_key,
            retail_year * 100 + retail_week_num AS retail_year_week_key,
            CASE 
                WHEN retail_month_num = 1 THEN ''Month 01''
                WHEN retail_month_num = 2 THEN ''Month 02''
                WHEN retail_month_num = 3 THEN ''Month 03''
                WHEN retail_month_num = 4 THEN ''Month 04''
                WHEN retail_month_num = 5 THEN ''Month 05''
                WHEN retail_month_num = 6 THEN ''Month 06''
                WHEN retail_month_num = 7 THEN ''Month 07''
                WHEN retail_month_num = 8 THEN ''Month 08''
                WHEN retail_month_num = 9 THEN ''Month 09''
                WHEN retail_month_num = 10 THEN ''Month 10''
                WHEN retail_month_num = 11 THEN ''Month 11''
                WHEN retail_month_num = 12 THEN ''Month 12''
            END AS retail_month_name,
            ''Q'' || retail_quarter_num AS retail_quarter_name,
            ''H'' || retail_half_num AS retail_half_name,
            ''W'' || LPAD(retail_week_num, 2, ''0'') AS retail_week_name,
            created_at,
            created_by,
            retail_pattern,
            retail_start_month,
            retail_week_start_day
        FROM retail_calendar_base
        ORDER BY date';
        
        -- Add clustering to the table
        EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table_name || ' CLUSTER BY (date)';
        
        -- Get row count
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_table_name INTO v_row_count;
        
        -- Update result with success
        SELECT OBJECT_CONSTRUCT(
            'status', 'SUCCESS',
            'message', 'Retail calendar created successfully',
            'details', OBJECT_CONSTRUCT(
                'database', DATABASE_NAME,
                'schema', SCHEMA_NAME,
                'table_name', v_table_name,
                'pattern', v_pattern,
                'start_month', v_start_month,
                'week_start_day', v_week_start_day,
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
                'BUILD_RETAIL_CALENDAR.generation', 
                SQLSTATE || ': ' || SQLERRM, 
                'GENERATION_ERROR',
                PARSE_JSON(OBJECT_CONSTRUCT(
                    'database', DATABASE_NAME,
                    'schema', SCHEMA_NAME,
                    'pattern', v_pattern,
                    'start_month', v_start_month,
                    'week_start_day', v_week_start_day,
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
                    'pattern', v_pattern,
                    'start_month', v_start_month,
                    'week_start_day', v_week_start_day,
                    'start_year', v_start_year,
                    'end_year', v_end_year
                ),
                'timestamp', CURRENT_TIMESTAMP()
            ) INTO result;
    END;
    
    RETURN result;
END;
$$;