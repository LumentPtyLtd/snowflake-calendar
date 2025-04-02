CREATE OR REPLACE PROCEDURE GENERATE_CALENDAR(TIME_GRAIN VARCHAR(10))
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // ----- Input Validation -----
    const validTimeGrains = ['SECOND', 'MINUTE', 'HOUR', 'DAY', 'MONTH', 'YEAR'];
    const grainUpper = TIME_GRAIN.toUpperCase();
    if (validTimeGrains.indexOf(grainUpper) === -1) {
        return "ERROR: Invalid time grain. Must be one of: SECOND, MINUTE, HOUR, DAY, MONTH, YEAR";
    }

    // ----- Dynamic Name Construction -----
    const baseTableName = "DATE_SPINE_" + grainUpper; // Name of the source date spine table
    const viewName = "CALENDAR_" + grainUpper;       // Name of the calendar view to be created

    // Declare SQL parts outside the try block
    let createViewSql = '';
    let columnDefinitions = ''; // For column names and comments: col_name COMMENT '...'
    let selectExpressions = ''; // For SELECT list: expression AS col_name

    try {
        // ----- Column Definition and SELECT Expression Logic -----

        // --- Initialize Lists ---
        let colDefs = []; // Array to hold "COLUMN_NAME COMMENT '...'" strings
        let selExprs = []; // Array to hold "expression AS COLUMN_NAME" strings

        // --- Helper function to add column ---
        function addColumn(name, expression, comment) {
            colDefs.push(`${name} COMMENT '${comment.replace(/'/g, "''")}'`); // Escape single quotes in comments
            selExprs.push(`${expression} AS ${name}`);
        }

        // --- Define Columns ---
        addColumn('DATE_TIME', 'DATE_TIME', 'The base timestamp at the specified grain (TIMESTAMP_NTZ).');
        addColumn('DATE_KEY', 'DATE(DATE_TIME)', 'The date part of the timestamp (DATE, YYYY-MM-DD). Primary key candidate.');
        addColumn('FULL_DATE_DESCRIPTION', "TO_VARCHAR(DATE(DATE_TIME), 'DD MMMM YYYY')", 'Human-readable full date (e.g., "27 October 2023").');
        addColumn('YEAR', 'YEAR(DATE_TIME)', 'The four-digit year (e.g., 2023).');

        // Year-Based
        addColumn('IS_LEAP_YEAR', '(MOD(YEAR(DATE_TIME), 4) = 0 AND MOD(YEAR(DATE_TIME), 100) != 0 OR MOD(YEAR(DATE_TIME), 400) = 0)', 'Boolean flag indicating if the year is a leap year.');
        addColumn('FIRST_DAY_OF_YEAR', "DATE(DATE_TRUNC('YEAR', DATE_TIME))", 'The first day of the year (DATE).');
        addColumn('LAST_DAY_OF_YEAR', "DATE(DATEADD(day, -1, DATEADD(year, 1, DATE_TRUNC('YEAR', DATE_TIME))))", 'The last day of the year (DATE).');

        // Quarter-Based (Not applicable for YEAR grain)
        if (grainUpper !== 'YEAR') {
            addColumn('QUARTER', 'QUARTER(DATE_TIME)', 'Quarter number of the year (1-4).');
            addColumn('QUARTER_NAME', "'Q' || QUARTER(DATE_TIME)::STRING", 'Descriptive quarter name (e.g., "Q1").');
            addColumn('FIRST_DAY_OF_QUARTER', "DATE(DATE_TRUNC('QUARTER', DATE_TIME))", 'The first day of the quarter (DATE).');
            addColumn('LAST_DAY_OF_QUARTER', "DATE(LAST_DAY(DATE_TIME, 'QUARTER'))", 'The last day of the quarter (DATE).');
        }

        // Month-Based (Not applicable for YEAR grain)
        if (grainUpper !== 'YEAR') {
            addColumn('MONTH', 'MONTH(DATE_TIME)', 'Month number (1-12).');
            addColumn('MONTH_NAME', "TO_VARCHAR(DATE_TIME, 'MMMM')", 'Full name of the month (e.g., "January").');
            addColumn('MONTH_NAME_SHORT', "TO_VARCHAR(DATE_TIME, 'Mon')", 'Abbreviated month name (e.g., "Jan").');
            addColumn('MONTH_SORT', 'MONTH(DATE_TIME)', 'Month number for sorting MonthName (1-12).');
            addColumn('FIRST_DAY_OF_MONTH', "DATE(DATE_TRUNC('MONTH', DATE_TIME))", 'The first day of the month (DATE).');
            addColumn('LAST_DAY_OF_MONTH', "DATE(LAST_DAY(DATE_TIME, 'MONTH'))", 'The last day of the month (DATE).');
            addColumn('MONTH_YEAR', "TO_VARCHAR(DATE_TIME, 'YYYY-MM')", 'Year and month combination for grouping (e.g., "2023-10").');
        }

        // Week/Day-Based (Vary by grain)
        if (['DAY', 'HOUR', 'MINUTE', 'SECOND'].includes(grainUpper)) {
             addColumn('DAY_OF_YEAR', 'DAYOFYEAR(DATE_TIME)', 'Sequential day number within the year (1-366).');
             addColumn('DAY_OF_MONTH', 'DAY(DATE_TIME)', 'Day number within the month (1-31).');
             addColumn('DAY_OF_WEEK', 'DAYOFWEEKISO(DATE_TIME)', 'ISO day number within the week (Monday=1, Sunday=7).');
             addColumn('DAY_OF_WEEK_NAME', "TO_VARCHAR(DATE_TIME, 'Day')", 'Full name of the day of the week (e.g., "Monday").');
             addColumn('DAY_OF_WEEK_NAME_SHORT', "TO_VARCHAR(DATE_TIME, 'Dy')", 'Abbreviated day name (e.g., "Mon").');
             addColumn('DAY_OF_WEEK_SORT', 'DAYOFWEEKISO(DATE_TIME)', 'ISO day number for sorting DayOfWeekName (1-7).');
             addColumn('WEEK_OF_YEAR', 'WEEKOFYEAR(DATE_TIME)', 'Week number within the year (using Snowflake default, often ISO-like).'); // Clarified comment
             addColumn('IS_WEEKEND', '(DAYOFWEEKISO(DATE_TIME) IN (6, 7))', 'Boolean flag indicating if the day is a Saturday or Sunday.');
             addColumn('FIRST_DAY_OF_WEEK', "DATE(DATE_TRUNC('WEEK', DATE_TIME))", 'The first day (Monday) of the ISO week (DATE).'); // Clarified comment
             addColumn('LAST_DAY_OF_WEEK', "DATE(DATEADD(day, 6, DATE_TRUNC('WEEK', DATE_TIME)))", 'The last day (Sunday) of the ISO week (DATE).'); // Clarified comment
             addColumn('SAME_DATE_LAST_YEAR', "DATE(DATEADD(year, -1, DATE(DATE_TIME)))", 'The exact calendar date one year prior (handles leap years).');
             // *** MODIFIED LOGIC for SAME_DAY_LAST_YEAR ***
             addColumn('SAME_DAY_LAST_YEAR',
                       "DATE(DATEADD(day, DAYOFWEEKISO(DATE(DATE_TIME)) - 1, DATE_TRUNC('WEEK', DATE(DATEADD(year, -1, DATE(DATE_TIME))))))",
                       'The date falling on the same ISO day of the week in the same ISO week number relative to the date one year prior.');
        } else if (grainUpper === 'MONTH') {
             addColumn('DAY_OF_YEAR', 'DAYOFYEAR(DATE_TIME)', 'Sequential day number within the year (always 1st of month).');
             addColumn('DAY_OF_MONTH', 'DAY(DATE_TIME)', 'Day number within the month (always 1).');
             addColumn('SAME_DATE_LAST_YEAR', "DATE(DATEADD(year, -1, DATE(DATE_TIME)))", 'The exact calendar date one year prior.');
             // SAME_DAY_LAST_YEAR is less clearly defined for MONTH grain start date, omitting for clarity.
        } else if (grainUpper === 'YEAR') {
             addColumn('DAY_OF_YEAR', 'DAYOFYEAR(DATE_TIME)', 'Sequential day number within the year (always 1).');
             addColumn('SAME_DATE_LAST_YEAR', "DATE(DATEADD(year, -1, DATE(DATE_TIME)))", 'The exact calendar date one year prior.');
             // SAME_DAY_LAST_YEAR is not meaningful at YEAR grain level.
        }

        // Time Component Columns (Applicable for finer grains)
        if (['HOUR', 'MINUTE', 'SECOND'].includes(grainUpper)) {
             addColumn('TIME', 'TIME(DATE_TIME)', 'The time part of the timestamp.');
        }
        switch (grainUpper) {
            case 'SECOND':
                addColumn('HOUR', 'HOUR(DATE_TIME)', 'Hour of the day (0-23).');
                addColumn('MINUTE', 'MINUTE(DATE_TIME)', 'Minute of the hour (0-59).');
                addColumn('SECOND', 'SECOND(DATE_TIME)', 'Second of the minute (0-59).');
                break;
            case 'MINUTE':
                addColumn('HOUR', 'HOUR(DATE_TIME)', 'Hour of the day (0-23).');
                addColumn('MINUTE', 'MINUTE(DATE_TIME)', 'Minute of the hour (0-59).');
                break;
            case 'HOUR':
                addColumn('HOUR', 'HOUR(DATE_TIME)', 'Hour of the day (0-23).');
                break;
        }

        // ----- Join Definitions and Expressions -----
        columnDefinitions = colDefs.join(',\n    ');
        selectExpressions = selExprs.join(',\n    ');

        // ----- View Creation -----
        const viewComment = `Standard calendar view based on ${grainUpper} grain date spine. Contains comprehensive date and time attributes.`;
        createViewSql = `
            CREATE OR REPLACE VIEW ${viewName} (
                ${columnDefinitions}
            )
            COMMENT = '${viewComment.replace(/'/g, "''")}'
            AS
            SELECT
                ${selectExpressions}
            FROM ${baseTableName}
        `;

        // Execute the CREATE VIEW statement
        let createViewStmt = snowflake.createStatement({
            sqlText: createViewSql
        });
        createViewStmt.execute();

        return "Calendar view generated successfully: " + viewName + " based on " + baseTableName;

    } catch (err) {
        // Now createViewSql is accessible here and properly formatted
        return "ERROR creating view " + viewName + ": " + err.code + " - " + err.message + "\nAttempted SQL:\n" + createViewSql;
    }
$$
;