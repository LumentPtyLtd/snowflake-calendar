/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - COLUMN COMMENTS (PART 4)
======================================================================================

This script adds comments to the unified calendar view and provides a procedure to apply all comments.
Execute this script after all other scripts have been executed.

Version: 2.0
Date: 31/03/2025
*/

-- Procedure to add comments to the UNIFIED_CALENDAR view
CREATE OR REPLACE PROCEDURE ADD_UNIFIED_CALENDAR_COMMENTS(
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    view_name VARCHAR;
BEGIN
    -- Set view name
    view_name := DATABASE_NAME || '.' || SCHEMA_NAME || '.UNIFIED_CALENDAR';
    
    -- Add view comment
    EXECUTE IMMEDIATE 'COMMENT ON VIEW ' || view_name || ' IS ''Unified calendar view that joins all calendar types together. Provides a comprehensive view of dates with attributes from all calendars.''';
    
    -- Add column comments
    -- Basic date attributes
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.DATE IS ''Calendar date.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.DATE_KEY IS ''Integer representation of the date (YYYYMMDD).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.YEAR IS ''Year part of the date.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.MONTH IS ''Month part of the date (1-12).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.DAY IS ''Day part of the date (1-31).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.DAY_OF_WEEK IS ''Day of week (0=Sunday, 1=Monday, ..., 6=Saturday).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.DAY_OF_YEAR IS ''Day of year (1-366).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.WEEK_OF_YEAR IS ''Week of year (1-53).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.QUARTER IS ''Quarter (1-4).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.TIMEZONE IS ''Timezone used for the timestamp conversion.''';
    
    -- Holiday information
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_HOLIDAY IS ''Flag indicating whether the date is a holiday in any Australian jurisdiction (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_HOLIDAY_NSW IS ''Flag indicating whether the date is a holiday in New South Wales (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_HOLIDAY_VIC IS ''Flag indicating whether the date is a holiday in Victoria (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_HOLIDAY_QLD IS ''Flag indicating whether the date is a holiday in Queensland (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_HOLIDAY_SA IS ''Flag indicating whether the date is a holiday in South Australia (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_HOLIDAY_WA IS ''Flag indicating whether the date is a holiday in Western Australia (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_HOLIDAY_TAS IS ''Flag indicating whether the date is a holiday in Tasmania (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_HOLIDAY_ACT IS ''Flag indicating whether the date is a holiday in Australian Capital Territory (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_HOLIDAY_NT IS ''Flag indicating whether the date is a holiday in Northern Territory (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_HOLIDAY_NATIONAL IS ''Flag indicating whether the date is a national holiday in Australia (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.HOLIDAY_NAMES IS ''Names of holidays on this date, with jurisdictions in parentheses.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_WEEKDAY IS ''Flag indicating whether the date is a weekday (Monday-Friday) (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_TRADING_DAY IS ''Flag indicating whether the date is a trading day (weekday and not a holiday) (1=Yes, 0=No).''';
    
    -- Fiscal calendar attributes
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_YEAR IS ''Fiscal year number.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_YEAR_SHORT IS ''Short fiscal year description (e.g., FY24).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_YEAR_LONG IS ''Long fiscal year description (e.g., FY2024).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_MONTH_NUM IS ''Month number within the fiscal year (1-12).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_MONTH_NAME IS ''Name of the fiscal month (e.g., Month 01).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_QUARTER_NUM IS ''Quarter number within the fiscal year (1-4).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_QUARTER_NAME IS ''Name of the fiscal quarter (e.g., Q1).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_HALF_NUM IS ''Half number within the fiscal year (1-2).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_HALF_NAME IS ''Name of the fiscal half (e.g., H1).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_WEEK_NUM IS ''Week number within the fiscal year (1-53).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_WEEK_NAME IS ''Name of the fiscal week (e.g., W01).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_YEAR_START_DATE IS ''Start date of the fiscal year.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_YEAR_END_DATE IS ''End date of the fiscal year.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_MONTH_START_DATE IS ''Start date of the fiscal month.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_MONTH_END_DATE IS ''End date of the fiscal month.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_QUARTER_START_DATE IS ''Start date of the fiscal quarter.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_QUARTER_END_DATE IS ''End date of the fiscal quarter.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_HALF_START_DATE IS ''Start date of the fiscal half.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_HALF_END_DATE IS ''End date of the fiscal half.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_WEEK_START_DATE IS ''Start date of the fiscal week.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_WEEK_END_DATE IS ''End date of the fiscal week.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_FISCAL_YEAR_START IS ''Flag indicating whether the date is the start of a fiscal year (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_FISCAL_YEAR_END IS ''Flag indicating whether the date is the end of a fiscal year (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_FISCAL_MONTH_START IS ''Flag indicating whether the date is the start of a fiscal month (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_FISCAL_MONTH_END IS ''Flag indicating whether the date is the end of a fiscal month (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_FISCAL_QUARTER_START IS ''Flag indicating whether the date is the start of a fiscal quarter (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_FISCAL_QUARTER_END IS ''Flag indicating whether the date is the end of a fiscal quarter (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_YEAR_MONTH_KEY IS ''Integer key for fiscal year and month (YYYYMM).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.FISCAL_YEAR_QUARTER_KEY IS ''Integer key for fiscal year and quarter (YYYYQ).''';
    
    -- Retail calendar attributes
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_YEAR IS ''Retail year number.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_YEAR_SHORT IS ''Short retail year description (e.g., R24).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_YEAR_LONG IS ''Long retail year description (e.g., R2024).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_MONTH_NUM IS ''Month number within the retail year (1-12).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_MONTH_NAME IS ''Name of the retail month (e.g., Month 01).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_QUARTER_NUM IS ''Quarter number within the retail year (1-4).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_QUARTER_NAME IS ''Name of the retail quarter (e.g., Q1).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_HALF_NUM IS ''Half number within the retail year (1-2).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_HALF_NAME IS ''Name of the retail half (e.g., H1).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_WEEK_NUM IS ''Week number within the retail year (1-53).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_WEEK_NAME IS ''Name of the retail week (e.g., W01).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_YEAR_START_DATE IS ''Start date of the retail year.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_YEAR_END_DATE IS ''End date of the retail year.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_MONTH_START_DATE IS ''Start date of the retail month.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_MONTH_END_DATE IS ''End date of the retail month.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_QUARTER_START_DATE IS ''Start date of the retail quarter.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_QUARTER_END_DATE IS ''End date of the retail quarter.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_HALF_START_DATE IS ''Start date of the retail half.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_HALF_END_DATE IS ''End date of the retail half.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_WEEK_START_DATE IS ''Start date of the retail week.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_WEEK_END_DATE IS ''End date of the retail week.''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_RETAIL_YEAR_START IS ''Flag indicating whether the date is the start of a retail year (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_RETAIL_YEAR_END IS ''Flag indicating whether the date is the end of a retail year (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_RETAIL_MONTH_START IS ''Flag indicating whether the date is the start of a retail month (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_RETAIL_MONTH_END IS ''Flag indicating whether the date is the end of a retail month (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_RETAIL_QUARTER_START IS ''Flag indicating whether the date is the start of a retail quarter (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_RETAIL_QUARTER_END IS ''Flag indicating whether the date is the end of a retail quarter (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_RETAIL_HALF_START IS ''Flag indicating whether the date is the start of a retail half (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.IS_RETAIL_HALF_END IS ''Flag indicating whether the date is the end of a retail half (1=Yes, 0=No).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_YEAR_MONTH_KEY IS ''Integer key for retail year and month (YYYYMM).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_YEAR_QUARTER_KEY IS ''Integer key for retail year and quarter (YYYYQ).''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || view_name || '.RETAIL_PATTERN IS ''Retail calendar pattern (445, 454, or 544).''';
    
    RETURN 'Comments added to ' || view_name;
END;
$$;

-- Procedure to apply all comments
CREATE OR REPLACE PROCEDURE APPLY_ALL_COMMENTS(
    DATABASE_NAME VARCHAR,
    SCHEMA_NAME VARCHAR,
    TIME_GRAINS ARRAY DEFAULT ARRAY_CONSTRUCT('DAY'),
    RETAIL_PATTERNS ARRAY DEFAULT ARRAY_CONSTRUCT('445')
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
    v_time_grain VARCHAR;
    v_retail_pattern VARCHAR;
BEGIN
    -- Initialize result object
    SELECT OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'message', 'Starting comment application',
        'details', OBJECT_CONSTRUCT(
            'steps', ARRAY_CONSTRUCT(),
            'errors', ARRAY_CONSTRUCT()
        ),
        'timestamp', CURRENT_TIMESTAMP()
    ) INTO result;
    
    -- Apply comments to calendar spine tables
    FOR i IN 0 TO ARRAY_SIZE(TIME_GRAINS) - 1 DO
        v_time_grain := TIME_GRAINS[i]::VARCHAR;
        BEGIN
            CALL ADD_CALENDAR_SPINE_COMMENTS(DATABASE_NAME, SCHEMA_NAME, v_time_grain);
            
            -- Update result
            SELECT OBJECT_CONSTRUCT(
                'status', result:status,
                'message', result:message,
                'details', OBJECT_CONSTRUCT(
                    'steps', ARRAY_APPEND(result:details:steps, OBJECT_CONSTRUCT(
                        'name', 'Add Comments to Calendar Spine - ' || v_time_grain,
                        'status', 'SUCCESS',
                        'timestamp', CURRENT_TIMESTAMP()
                    )),
                    'errors', result:details:errors
                ),
                'timestamp', CURRENT_TIMESTAMP()
            ) INTO result;
        EXCEPTION
            WHEN OTHER THEN
                -- Update result with error
                SELECT OBJECT_CONSTRUCT(
                    'status', 'ERROR',
                    'message', result:message,
                    'details', OBJECT_CONSTRUCT(
                        'steps', ARRAY_APPEND(result:details:steps, OBJECT_CONSTRUCT(
                            'name', 'Add Comments to Calendar Spine - ' || v_time_grain,
                            'status', 'ERROR',
                            'message', SQLSTATE || ': ' || SQLERRM,
                            'timestamp', CURRENT_TIMESTAMP()
                        )),
                        'errors', ARRAY_APPEND(result:details:errors, OBJECT_CONSTRUCT(
                            'step', 'Add Comments to Calendar Spine - ' || v_time_grain,
                            'message', SQLSTATE || ': ' || SQLERRM,
                            'timestamp', CURRENT_TIMESTAMP()
                        ))
                    ),
                    'timestamp', CURRENT_TIMESTAMP()
                ) INTO result;
        END;
    END FOR;
    
    -- Apply comments to retail calendar tables
    FOR i IN 0 TO ARRAY_SIZE(RETAIL_PATTERNS) - 1 DO
        v_retail_pattern := RETAIL_PATTERNS[i]::VARCHAR;
        BEGIN
            CALL ADD_RETAIL_CALENDAR_COMMENTS(DATABASE_NAME, SCHEMA_NAME, v_retail_pattern);
            
            -- Update result
            SELECT OBJECT_CONSTRUCT(
                'status', result:status,
                'message', result:message,
                'details', OBJECT_CONSTRUCT(
                    'steps', ARRAY_APPEND(result:details:steps, OBJECT_CONSTRUCT(
                        'name', 'Add Comments to Retail Calendar - ' || v_retail_pattern,
                        'status', 'SUCCESS',
                        'timestamp', CURRENT_TIMESTAMP()
                    )),
                    'errors', result:details:errors
                ),
                'timestamp', CURRENT_TIMESTAMP()
            ) INTO result;
        EXCEPTION
            WHEN OTHER THEN
                -- Update result with error
                SELECT OBJECT_CONSTRUCT(
                    'status', 'ERROR',
                    'message', result:message,
                    'details', OBJECT_CONSTRUCT(
                        'steps', ARRAY_APPEND(result:details:steps, OBJECT_CONSTRUCT(
                            'name', 'Add Comments to Retail Calendar - ' || v_retail_pattern,
                            'status', 'ERROR',
                            'message', SQLSTATE || ': ' || SQLERRM,
                            'timestamp', CURRENT_TIMESTAMP()
                        )),
                        'errors', ARRAY_APPEND(result:details:errors, OBJECT_CONSTRUCT(
                            'step', 'Add Comments to Retail Calendar - ' || v_retail_pattern,
                            'message', SQLSTATE || ': ' || SQLERRM,
                            'timestamp', CURRENT_TIMESTAMP()
                        ))
                    ),
                    'timestamp', CURRENT_TIMESTAMP()
                ) INTO result;
        END;
    END FOR;
    
    -- Apply comments to unified calendar view
    BEGIN
        CALL ADD_UNIFIED_CALENDAR_COMMENTS(DATABASE_NAME, SCHEMA_NAME);
        
        -- Update result
        SELECT OBJECT_CONSTRUCT(
            'status', result:status,
            'message', result:message,
            'details', OBJECT_CONSTRUCT(
                'steps', ARRAY_APPEND(result:details:steps, OBJECT_CONSTRUCT(
                    'name', 'Add Comments to Unified Calendar',
                    'status', 'SUCCESS',
                    'timestamp', CURRENT_TIMESTAMP()
                )),
                'errors', result:details:errors
            ),
            'timestamp', CURRENT_TIMESTAMP()
        ) INTO result;
    EXCEPTION
        WHEN OTHER THEN
            -- Update result with error
            SELECT OBJECT_CONSTRUCT(
                'status', 'ERROR',
                'message', result:message,
                'details', OBJECT_CONSTRUCT(
                    'steps', ARRAY_APPEND(result:details:steps, OBJECT_CONSTRUCT(
                        'name', 'Add Comments to Unified Calendar',
                        'status', 'ERROR',
                        'message', SQLSTATE || ': ' || SQLERRM,
                        'timestamp', CURRENT_TIMESTAMP()
                    )),
                    'errors', ARRAY_APPEND(result:details:errors, OBJECT_CONSTRUCT(
                        'step', 'Add Comments to Unified Calendar',
                        'message', SQLSTATE || ': ' || SQLERRM,
                        'timestamp', CURRENT_TIMESTAMP()
                    ))
                ),
                'timestamp', CURRENT_TIMESTAMP()
            ) INTO result;
    END;
    
    -- Update final message
    SELECT OBJECT_CONSTRUCT(
        'status', result:status,
        'message', CASE 
            WHEN result:status = 'SUCCESS' THEN 'All comments applied successfully'
            ELSE 'Some errors occurred while applying comments'
        END,
        'details', result:details,
        'timestamp', CURRENT_TIMESTAMP()
    ) INTO result;
    
    RETURN result;
END;
$$;

-- Example usage
-- CALL APPLY_ALL_COMMENTS('MY_DATABASE', 'MY_SCHEMA', ARRAY_CONSTRUCT('DAY', 'MONTH', 'YEAR'), ARRAY_CONSTRUCT('445', '454', '544'));