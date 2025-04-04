/*==============================================================================
  BUSINESS CALENDAR SYSTEM - MASTER ORCHESTRATION SCRIPT
  
  This script serves as the main entry point for generating a comprehensive
  business calendar system in Snowflake. It coordinates the execution of all 
  calendar component modules in the correct sequence and provides a unified
  interface for configuration.
  
  USAGE:
  ------
  1. Review and adjust configuration parameters below
  2. Execute this script to generate all calendar components
  3. To refresh with different parameters, modify config and run again
  
  COMPONENTS CREATED:
  ------------------
  1. Date spine tables at specified grain
  2. Standard Gregorian calendar tables
  3. Fiscal calendar tables (optional)
  4. Retail calendar tables (optional)
  5. Unified calendar tables (combines all active components)
  6. Dynamic period views (for time-relative calculations)
  
  AUTHOR: [Your Name]
  DATE CREATED: 2024-04-04
  LAST MODIFIED: 2024-04-04
==============================================================================*/

-- Set configuration variables
SET CALENDAR_START_DATE = '2020-01-01'; -- Beginning of date range to generate
SET CALENDAR_END_DATE = '2030-12-31';   -- End of date range to generate
SET TIME_GRAIN = 'DAY';                 -- Base time grain (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR)

-- Business calendar parameters
SET INCLUDE_FISCAL = TRUE;              -- Generate fiscal calendar
SET FISCAL_YEAR_START_MONTH = 7;        -- Month when fiscal year starts (1-12)
SET INCLUDE_RETAIL = TRUE;              -- Generate retail calendar
SET RETAIL_PATTERN = '4-5-4';           -- Retail calendar pattern (4-5-4, 4-4-5, 5-4-4)
SET RETAIL_YEAR_END_MONTH = 1;          -- Month when retail year ends (1-12)

-- Dynamic periods parameters
SET INCLUDE_DYNAMIC_PERIODS = TRUE;     -- Generate dynamic period views
SET RELATIVE_PERIODS_COUNT = 12;        -- Number of historical periods to generate
SET INCLUDE_MULTI_TIMEZONE = FALSE;     -- Support multiple timezones
SET TIMEZONE_LIST = ARRAY_CONSTRUCT('Etc/UTC', 'Europe/London', 'America/New_York'); -- Timezones to support

-- Warehouse to use for calendar generation (adjust as needed)
SET CALENDAR_WAREHOUSE = 'COMPUTE_WH';
USE WAREHOUSE IDENTIFIER($CALENDAR_WAREHOUSE);

-- Create orchestration stored procedure
CREATE OR REPLACE PROCEDURE GENERATE_BUSINESS_CALENDAR(
    START_DATE DATE,
    END_DATE DATE,
    TIME_GRAIN VARCHAR(10),
    INCLUDE_FISCAL BOOLEAN DEFAULT TRUE,
    FISCAL_YEAR_START_MONTH FLOAT DEFAULT 7.0,
    INCLUDE_RETAIL BOOLEAN DEFAULT TRUE,
    RETAIL_PATTERN VARCHAR(5) DEFAULT '4-5-4',
    RETAIL_YEAR_END_MONTH FLOAT DEFAULT 1.0,
    INCLUDE_DYNAMIC_PERIODS BOOLEAN DEFAULT TRUE,
    RELATIVE_PERIODS_COUNT FLOAT DEFAULT 12.0,
    INCLUDE_MULTI_TIMEZONE BOOLEAN DEFAULT FALSE,
    TIMEZONE_LIST ARRAY DEFAULT ARRAY_CONSTRUCT('Etc/UTC')
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Validate inputs
    const validTimeGrains = ['SECOND', 'MINUTE', 'HOUR', 'DAY', 'MONTH', 'YEAR'];
    const grainUpper = TIME_GRAIN.toUpperCase();
    if (validTimeGrains.indexOf(grainUpper) === -1) {
        return `ERROR: Invalid time grain. Must be one of: ${validTimeGrains.join(', ')}`;
    }
    
    // Initialize execution log
    let executionLog = [];
    const log = (step, result) => {
        executionLog.push(`${step}: ${result}`);
        return result;
    };
    
    try {
        // STEP 1: Generate date spine table
        let result = snowflake.execute({
            sqlText: `CALL GENERATE_DATE_SPINE(
                TO_TIMESTAMP_NTZ('${START_DATE}'), 
                TO_TIMESTAMP_NTZ('${END_DATE}'), 
                '${grainUpper}')`
        });
        result.next();
        log('STEP 1 - Generate date spine', result.getColumnValue(1));
        
        // STEP 2: Generate standard Gregorian calendar
        result = snowflake.execute({
            sqlText: `CALL GENERATE_CALENDAR('${grainUpper}')`
        });
        result.next();
        log('STEP 2 - Generate standard calendar', result.getColumnValue(1));
        
        // STEP 3: Materialize standard calendar as a table
        result = snowflake.execute({
            sqlText: `CALL MATERIALIZE_CALENDAR_TABLE('${grainUpper}')`
        });
        result.next();
        log('STEP 3 - Materialize standard calendar', result.getColumnValue(1));
        
        // STEP 4: Generate fiscal calendar if requested
        if (INCLUDE_FISCAL) {
            result = snowflake.execute({
                sqlText: `CALL GENERATE_FISCAL_CALENDAR('${grainUpper}', ${FISCAL_YEAR_START_MONTH})`
            });
            result.next();
            log('STEP 4 - Generate fiscal calendar', result.getColumnValue(1));
            
            // Materialize fiscal calendar as a table
            result = snowflake.execute({
                sqlText: `CALL MATERIALIZE_FISCAL_CALENDAR_TABLE('${grainUpper}')`
            });
            result.next();
            log('STEP 4b - Materialize fiscal calendar', result.getColumnValue(1));
        } else {
            log('STEP 4 - Generate fiscal calendar', 'Skipped (not requested)');
        }
        
        // STEP 5: Generate retail calendar if requested
        if (INCLUDE_RETAIL) {
            result = snowflake.execute({
                sqlText: `CALL GENERATE_RETAIL_CALENDAR(
                    '${grainUpper}', 
                    '${RETAIL_PATTERN}', 
                    ${RETAIL_YEAR_END_MONTH})`
            });
            result.next();
            log('STEP 5 - Generate retail calendar', result.getColumnValue(1));
            
            // Materialize retail calendar as a table
            const patternSuffix = RETAIL_PATTERN.replace(/-/g, '');
            result = snowflake.execute({
                sqlText: `CALL MATERIALIZE_RETAIL_CALENDAR_TABLE('${grainUpper}', '${patternSuffix}')`
            });
            result.next();
            log('STEP 5b - Materialize retail calendar', result.getColumnValue(1));
        } else {
            log('STEP 5 - Generate retail calendar', 'Skipped (not requested)');
        }
        
        // STEP 6: Generate unified calendar
        result = snowflake.execute({
            sqlText: `CALL GENERATE_UNIFIED_CALENDAR(
                '${grainUpper}', 
                ${INCLUDE_FISCAL}, 
                ${FISCAL_YEAR_START_MONTH}, 
                ${INCLUDE_RETAIL}, 
                '${RETAIL_PATTERN}', 
                ${RETAIL_YEAR_END_MONTH})`
        });
        result.next();
        log('STEP 6 - Generate unified calendar', result.getColumnValue(1));
        
        // STEP 7: Materialize unified calendar as a table
        result = snowflake.execute({
            sqlText: `CALL MATERIALIZE_UNIFIED_CALENDAR_TABLE('${grainUpper}')`
        });
        result.next();
        log('STEP 7 - Materialize unified calendar', result.getColumnValue(1));
        
        // STEP 8: Generate dynamic periods view if requested
        if (INCLUDE_DYNAMIC_PERIODS) {
            if (INCLUDE_MULTI_TIMEZONE) {
                // Multi-timezone dynamic periods
                result = snowflake.execute({
                    sqlText: `CALL GENERATE_DYNAMIC_PERIODS(
                        '${grainUpper}', 
                        PARSE_JSON('${JSON.stringify(TIMEZONE_LIST)}'), 
                        ${RELATIVE_PERIODS_COUNT})`
                });
                result.next();
                log('STEP 8 - Generate multi-timezone dynamic periods', result.getColumnValue(1));
            } else {
                // Simple dynamic periods
                result = snowflake.execute({
                    sqlText: `CALL GENERATE_SIMPLE_DYNAMIC_PERIODS(
                        'UNIFIED_CALENDAR_TABLE_${grainUpper}', 
                        '${grainUpper}', 
                        ${RELATIVE_PERIODS_COUNT})`
                });
                result.next();
                log('STEP 8 - Generate simple dynamic periods', result.getColumnValue(1));
            }
        } else {
            log('STEP 8 - Generate dynamic periods', 'Skipped (not requested)');
        }
        
        // STEP 9: Generate calendar data dictionary
        result = snowflake.execute({
            sqlText: `CALL GENERATE_CALENDAR_DATA_DICTIONARY()`
        });
        result.next();
        log('STEP 9 - Generate calendar data dictionary', result.getColumnValue(1));
        
        // Summarize and return results
        const calendarObjects = [
            `DATE_SPINE_${grainUpper} (Table)`,
            `CALENDAR_TABLE_${grainUpper} (Table)`,
            `CALENDAR_${grainUpper} (View)`
        ];
        
        if (INCLUDE_FISCAL) {
            calendarObjects.push(
                `FISCAL_CALENDAR_TABLE_${grainUpper} (Table)`,
                `FISCAL_CALENDAR_${grainUpper} (View)`
            );
        }
        
        if (INCLUDE_RETAIL) {
            const patternSuffix = RETAIL_PATTERN.replace(/-/g, '');
            calendarObjects.push(
                `RETAIL_CALENDAR_TABLE_${patternSuffix}_${grainUpper} (Table)`,
                `RETAIL_CALENDAR_${patternSuffix}_${grainUpper} (View)`
            );
        }
        
        calendarObjects.push(
            `UNIFIED_CALENDAR_TABLE_${grainUpper} (Table)`,
            `UNIFIED_CALENDAR_${grainUpper} (View)`
        );
        
        if (INCLUDE_DYNAMIC_PERIODS) {
            if (INCLUDE_MULTI_TIMEZONE) {
                calendarObjects.push(`DYNAMIC_PERIODS_${grainUpper} (View)`);
            } else {
                calendarObjects.push(`DYNAMIC_PERIODS_SIMPLE_${grainUpper} (View)`);
            }
        }
        
        calendarObjects.push(`CALENDAR_DATA_DICTIONARY (View)`);
        
        return `
Business calendar system generated successfully!

EXECUTION LOG:
${executionLog.join('\n')}

OBJECTS CREATED:
${calendarObjects.join('\n')}

USAGE EXAMPLES:
- For financial reporting: SELECT * FROM UNIFIED_CALENDAR_TABLE_${grainUpper} WHERE FISCAL_YEAR = 2023
- For dynamic date filters: SELECT * FROM DYNAMIC_PERIODS_SIMPLE_${grainUpper} WHERE IS_CURRENT_MONTH = TRUE
- To find column definitions: SELECT * FROM CALENDAR_DATA_DICTIONARY WHERE column_name LIKE '%FISCAL%'

For detailed usage examples, see documentation.
        `;
    } catch (err) {
        return `ERROR generating business calendar: ${err.code} - ${err.message}\n\nExecution log:\n${executionLog.join('\n')}`;
    }
$$;

-- Create procedure to generate data dictionary
CREATE OR REPLACE PROCEDURE GENERATE_CALENDAR_DATA_DICTIONARY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Create view to document all calendar system objects
    CREATE OR REPLACE VIEW CALENDAR_DATA_DICTIONARY AS
    SELECT 
        table_catalog,
        table_schema,
        table_name,
        column_name,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        is_nullable,
        column_default,
        comment
    FROM 
        information_schema.columns
    WHERE 
        table_name ILIKE 'DATE_SPINE_%' 
        OR table_name ILIKE 'CALENDAR_%'
        OR table_name ILIKE 'FISCAL_CALENDAR_%'
        OR table_name ILIKE 'RETAIL_CALENDAR_%'
        OR table_name ILIKE 'UNIFIED_CALENDAR_%'
        OR table_name ILIKE 'DYNAMIC_PERIODS_%'
    ORDER BY 
        table_name, 
        ordinal_position;
        
    RETURN 'Calendar data dictionary created successfully';
END;
$$;

-- Create procedure to materialize standard calendar
CREATE OR REPLACE PROCEDURE MATERIALIZE_CALENDAR_TABLE(TIME_GRAIN VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    try {
        const grainUpper = TIME_GRAIN.toUpperCase();
        
        // Create table from view
        snowflake.execute({
            sqlText: `
                CREATE OR REPLACE TABLE CALENDAR_TABLE_${grainUpper} AS
                SELECT * FROM CALENDAR_${grainUpper};
                
                -- Add clustering key for better performance
                ALTER TABLE CALENDAR_TABLE_${grainUpper} CLUSTER BY (DATE_KEY);
                
                -- Add comments
                COMMENT ON TABLE CALENDAR_TABLE_${grainUpper} IS 'Materialized standard calendar table at ${grainUpper} grain. Contains comprehensive date and time attributes.';
            `
        });
        
        return `Standard calendar materialized successfully as CALENDAR_TABLE_${grainUpper}`;
    } catch (err) {
        return `Error materializing calendar table: ${err.code} - ${err.message}`;
    }
$$;

-- Create procedure to materialize fiscal calendar
CREATE OR REPLACE PROCEDURE MATERIALIZE_FISCAL_CALENDAR_TABLE(TIME_GRAIN VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    try {
        const grainUpper = TIME_GRAIN.toUpperCase();
        
        // Create table from view
        snowflake.execute({
            sqlText: `
                CREATE OR REPLACE TABLE FISCAL_CALENDAR_TABLE_${grainUpper} AS
                SELECT * FROM FISCAL_CALENDAR_${grainUpper};
                
                -- Add clustering key for better performance
                ALTER TABLE FISCAL_CALENDAR_TABLE_${grainUpper} CLUSTER BY (DATE_KEY);
                
                -- Add comments
                COMMENT ON TABLE FISCAL_CALENDAR_TABLE_${grainUpper} IS 'Materialized fiscal calendar table at ${grainUpper} grain. Contains fiscal year, quarter, month, and other fiscal period attributes.';
            `
        });
        
        return `Fiscal calendar materialized successfully as FISCAL_CALENDAR_TABLE_${grainUpper}`;
    } catch (err) {
        return `Error materializing fiscal calendar table: ${err.code} - ${err.message}`;
    }
$$;

-- Create procedure to materialize retail calendar
CREATE OR REPLACE PROCEDURE MATERIALIZE_RETAIL_CALENDAR_TABLE(TIME_GRAIN VARCHAR, PATTERN_SUFFIX VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    try {
        const grainUpper = TIME_GRAIN.toUpperCase();
        
        // Create table from view
        snowflake.execute({
            sqlText: `
                CREATE OR REPLACE TABLE RETAIL_CALENDAR_TABLE_${PATTERN_SUFFIX}_${grainUpper} AS
                SELECT * FROM RETAIL_CALENDAR_${PATTERN_SUFFIX}_${grainUpper};
                
                -- Add clustering key for better performance
                ALTER TABLE RETAIL_CALENDAR_TABLE_${PATTERN_SUFFIX}_${grainUpper} CLUSTER BY (DATE_KEY);
                
                -- Add comments
                COMMENT ON TABLE RETAIL_CALENDAR_TABLE_${PATTERN_SUFFIX}_${grainUpper} IS 'Materialized retail calendar table with ${PATTERN_SUFFIX} pattern at ${grainUpper} grain. Contains retail year, quarter, month, and week attributes.';
            `
        });
        
        return `Retail calendar materialized successfully as RETAIL_CALENDAR_TABLE_${PATTERN_SUFFIX}_${grainUpper}`;
    } catch (err) {
        return `Error materializing retail calendar table: ${err.code} - ${err.message}`;
    }
$$;

-- Create procedure to materialize unified calendar
CREATE OR REPLACE PROCEDURE MATERIALIZE_UNIFIED_CALENDAR_TABLE(TIME_GRAIN VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    try {
        const grainUpper = TIME_GRAIN.toUpperCase();
        
        // Create table from view
        snowflake.execute({
            sqlText: `
                CREATE OR REPLACE TABLE UNIFIED_CALENDAR_TABLE_${grainUpper} AS
                SELECT * FROM UNIFIED_CALENDAR_${grainUpper};
                
                -- Add clustering key for better performance
                ALTER TABLE UNIFIED_CALENDAR_TABLE_${grainUpper} CLUSTER BY (DATE_KEY);
                
                -- Add comments
                COMMENT ON TABLE UNIFIED_CALENDAR_TABLE_${grainUpper} IS 'Materialized unified calendar table at ${grainUpper} grain. Combines standard, fiscal, and retail calendars into a single table.';
            `
        });
        
        return `Unified calendar materialized successfully as UNIFIED_CALENDAR_TABLE_${grainUpper}`;
    } catch (err) {
        return `Error materializing unified calendar table: ${err.code} - ${err.message}`;
    }
$$;

-- Execute the calendar generation
CALL GENERATE_BUSINESS_CALENDAR(
    $CALENDAR_START_DATE,
    $CALENDAR_END_DATE,
    $TIME_GRAIN,
    $INCLUDE_FISCAL,
    $FISCAL_YEAR_START_MONTH,
    $INCLUDE_RETAIL,
    $RETAIL_PATTERN,
    $RETAIL_YEAR_END_MONTH,
    $INCLUDE_DYNAMIC_PERIODS,
    $RELATIVE_PERIODS_COUNT,
    $INCLUDE_MULTI_TIMEZONE,
    $TIMEZONE_LIST
);
