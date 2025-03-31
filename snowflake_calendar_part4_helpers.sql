/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - PART 4 (HELPER FUNCTIONS)
======================================================================================

This script implements the helper functions for business day calculations for the
Australian calendar system.

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

-- Function to get the same day in previous fiscal period
CREATE OR REPLACE FUNCTION SAME_DAY_PREVIOUS_FISCAL_PERIOD(
    start_date DATE,
    period_type VARCHAR, -- 'MONTH', 'QUARTER', 'YEAR'
    num_periods INTEGER DEFAULT 1,
    calendar_table VARCHAR DEFAULT 'FISCAL_CALENDAR',
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
    
    -- Execute dynamic SQL to get same day in previous fiscal period
    EXECUTE IMMEDIATE 
    'WITH date_info AS (
        SELECT 
            date, 
            fiscal_year,
            fiscal_month_num,
            fiscal_quarter_num,
            day_of_fiscal_year
        FROM ' || v_database || '.' || v_schema || '.' || calendar_table || '
        WHERE date = ''' || start_date || '''
    )
    SELECT 
        CASE UPPER(''' || period_type || ''')
            WHEN ''MONTH'' THEN 
                (SELECT date 
                 FROM ' || v_database || '.' || v_schema || '.' || calendar_table || ' fc
                 WHERE fc.fiscal_year = di.fiscal_year AND fc.fiscal_month_num = di.fiscal_month_num - ' || num_periods || '
                 AND DATEDIFF(DAY, fc.fiscal_month_start_date, fc.date) = 
                     DATEDIFF(DAY, (SELECT fiscal_month_start_date FROM ' || v_database || '.' || v_schema || '.' || calendar_table || ' 
                                    WHERE date = di.date), di.date)
                 LIMIT 1)
            WHEN ''QUARTER'' THEN 
                (SELECT date 
                 FROM ' || v_database || '.' || v_schema || '.' || calendar_table || ' fc
                 WHERE fc.fiscal_year = di.fiscal_year AND fc.fiscal_quarter_num = di.fiscal_quarter_num - ' || num_periods || '
                 AND DATEDIFF(DAY, fc.fiscal_quarter_start_date, fc.date) = 
                     DATEDIFF(DAY, (SELECT fiscal_quarter_start_date FROM ' || v_database || '.' || v_schema || '.' || calendar_table || ' 
                                    WHERE date = di.date), di.date)
                 LIMIT 1)
            WHEN ''YEAR'' THEN 
                (SELECT date 
                 FROM ' || v_database || '.' || v_schema || '.' || calendar_table || ' fc
                 WHERE fc.fiscal_year = di.fiscal_year - ' || num_periods || '
                 AND fc.day_of_fiscal_year = di.day_of_fiscal_year
                 LIMIT 1)
            ELSE NULL
        END
    FROM date_info di'
    INTO v_result;
    
    RETURN v_result;
END;
$$;

-- Function to get the same day in previous retail period
CREATE OR REPLACE FUNCTION SAME_DAY_PREVIOUS_RETAIL_PERIOD(
    start_date DATE,
    period_type VARCHAR, -- 'MONTH', 'QUARTER', 'YEAR'
    num_periods INTEGER DEFAULT 1,
    calendar_table VARCHAR DEFAULT 'RETAIL_CALENDAR_445',
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
    
    -- Execute dynamic SQL to get same day in previous retail period
    EXECUTE IMMEDIATE 
    'WITH date_info AS (
        SELECT 
            date, 
            retail_year,
            retail_month_num,
            retail_quarter_num,
            day_of_retail_year
        FROM ' || v_database || '.' || v_schema || '.' || calendar_table || '
        WHERE date = ''' || start_date || '''
    )
    SELECT 
        CASE UPPER(''' || period_type || ''')
            WHEN ''MONTH'' THEN 
                (SELECT date 
                 FROM ' || v_database || '.' || v_schema || '.' || calendar_table || ' rc
                 WHERE rc.retail_year = di.retail_year AND rc.retail_month_num = di.retail_month_num - ' || num_periods || '
                 AND DATEDIFF(DAY, rc.retail_month_start_date, rc.date) = 
                     DATEDIFF(DAY, (SELECT retail_month_start_date FROM ' || v_database || '.' || v_schema || '.' || calendar_table || ' 
                                    WHERE date = di.date), di.date)
                 LIMIT 1)
            WHEN ''QUARTER'' THEN 
                (SELECT date 
                 FROM ' || v_database || '.' || v_schema || '.' || calendar_table || ' rc
                 WHERE rc.retail_year = di.retail_year AND rc.retail_quarter_num = di.retail_quarter_num - ' || num_periods || '
                 AND DATEDIFF(DAY, rc.retail_quarter_start_date, rc.date) = 
                     DATEDIFF(DAY, (SELECT retail_quarter_start_date FROM ' || v_database || '.' || v_schema || '.' || calendar_table || ' 
                                    WHERE date = di.date), di.date)
                 LIMIT 1)
            WHEN ''YEAR'' THEN 
                (SELECT date 
                 FROM ' || v_database || '.' || v_schema || '.' || calendar_table || ' rc
                 WHERE rc.retail_year = di.retail_year - ' || num_periods || '
                 AND rc.day_of_retail_year = di.day_of_retail_year
                 LIMIT 1)
            ELSE NULL
        END
    FROM date_info di'
    INTO v_result;
    
    RETURN v_result;
END;
$$;