/*==============================================================================
  RETAIL CALENDAR GENERATOR
  
  This script creates a retail calendar view that follows standardized retail
  calendar patterns (4-5-4, 4-4-5, or 5-4-4). Retail calendars are commonly used
  in the retail industry to ensure consistent week-based reporting periods.
  
  The resulting retail calendar includes:
  - Retail year, quarter, and month identifiers
  - Retail year start and end dates
  - Retail week numbers
  - Number of weeks in the retail year (52 or 53)
  - Support for 53-week year handling
  
  PATTERNS:
  --------
  4-5-4: Each quarter has 13 weeks with months of 4, 5, and 4 weeks
  4-4-5: Each quarter has 13 weeks with months of 4, 4, and 5 weeks
  5-4-4: Each quarter has 13 weeks with months of 5, 4, and 4 weeks
  
  USAGE:
  ------
  CALL GENERATE_RETAIL_CALENDAR(
      'DAY',    -- Time grain
      '4-5-4',  -- Retail pattern
      1         -- Month when retail year approximately ends (1-12)
  );
  
  PARAMETERS:
  -----------
  TIME_GRAIN: The granularity level (SECOND, MINUTE, HOUR, DAY)
  RETAIL_PATTERN: The week distribution pattern (4-5-4, 4-4-5, 5-4-4)
  RETAIL_YEAR_END_MONTH: The month when retail year approximately ends (1-12)
  
  RETURNS:
  --------
  A string indicating success or failure with details
  
  PREREQUISITES:
  -------------
  Requires the CALENDAR_{TIME_GRAIN} view to exist
  
  AUTHOR: [Your Name]
  DATE CREATED: 2024-04-04
  LAST MODIFIED: 2024-04-04
==============================================================================*/

-- Stored procedure to generate a Retail Calendar view (Handles week 53 basic assignment)
CREATE OR REPLACE PROCEDURE GENERATE_RETAIL_CALENDAR(
    TIME_GRAIN VARCHAR(10),
    RETAIL_PATTERN VARCHAR(5),
    RETAIL_YEAR_END_MONTH_ARG FLOAT
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // ----- Input Validation -----
    const validTimeGrains = ['SECOND', 'MINUTE', 'HOUR', 'DAY'];
    const grainUpper = TIME_GRAIN.toUpperCase();
    if (validTimeGrains.indexOf(grainUpper) === -1) {
        return `ERROR: Retail Calendar generation currently supports grains: ${validTimeGrains.join(', ')}. Grain provided: ${TIME_GRAIN}`;
    }
    const validPatterns = ['4-5-4', '4-4-5', '5-4-4'];
    const patternUpper = RETAIL_PATTERN ? RETAIL_PATTERN.toUpperCase() : null;
    if (!patternUpper || validPatterns.indexOf(patternUpper) === -1) {
        return `ERROR: Invalid or missing RETAIL_PATTERN. Must be one of: ${validPatterns.join(', ')}.`;
    }
    if (RETAIL_YEAR_END_MONTH_ARG === undefined || RETAIL_YEAR_END_MONTH_ARG === null || isNaN(RETAIL_YEAR_END_MONTH_ARG)) {
        return "ERROR: RETAIL_YEAR_END_MONTH_ARG must be provided as a valid number.";
    }
    const endMonthInt = Math.trunc(RETAIL_YEAR_END_MONTH_ARG);
    if (endMonthInt < 1 || endMonthInt > 12) {
        return `ERROR: RETAIL_YEAR_END_MONTH must be between 1 and 12 (derived value: ${endMonthInt}).`;
    }

    // ----- Dynamic Name Construction -----
    const baseViewName = "CALENDAR_" + grainUpper;
    const patternSuffix = patternUpper.replace(/-/g, '');
    const retailViewName = `RETAIL_CALENDAR_${patternSuffix}_${grainUpper}`;

    let createViewSql = '';
    let columnDefinitions = '';
    let selectExpressions = 'base.*';

    try {
        let colDefs = [];
        let selExprs = [];

        function addColumn(name, expression, comment) {
            colDefs.push(`${name} COMMENT '${comment.replace(/'/g, "''")}'`);
            selExprs.push(`${expression} AS ${name}`);
        }

        // ----- Retail Calculation Logic -----
        const retailYearExpr = `IFF(MONTH(base.DATE_KEY) >= ${endMonthInt + 1}, YEAR(base.DATE_KEY), YEAR(base.DATE_KEY) - 1)`;
        addColumn('RETAIL_YEAR', retailYearExpr, 'Retail Year identifier based on the NRF 4-5-4 calendar');

        const firstDayOfMonthForRetailYearStart = `DATE_FROM_PARTS(${retailYearExpr}, ${endMonthInt + 1}, 1)`;
        const startOfRetailYearExpr = `DATE(DATE_TRUNC('WEEK', ${firstDayOfMonthForRetailYearStart}))`;
        addColumn('RETAIL_YEAR_START_DATE', startOfRetailYearExpr, 'First date of the Retail Year (always a Sunday)');

        // Determine End Date based on potential 53 weeks
        const startOfNextRetailYearExpr = `DATE(DATE_TRUNC('WEEK', DATE_FROM_PARTS(${retailYearExpr} + 1, ${endMonthInt + 1}, 1)))`;
        const endOfRetailYearExpr = `DATEADD(day, -1, ${startOfNextRetailYearExpr})`;
        addColumn('RETAIL_YEAR_END_DATE', endOfRetailYearExpr, 'Last date of the Retail Year (always a Saturday)');

        // Determine if it's a 53-week year
        const weeksInRetailYearExpr = `DATEDIFF(week, ${startOfRetailYearExpr}, ${endOfRetailYearExpr}) + 1`;
        addColumn('WEEKS_IN_RETAIL_YEAR', weeksInRetailYearExpr, 'Number of weeks (usually 52 or 53) in this Retail Year');

        // Calculate Retail Week of Year (relative to start)
        const retailWeekOfYearExpr = `FLOOR(DATEDIFF(day, ${startOfRetailYearExpr}, base.DATE_KEY) / 7) + 1`;
        addColumn('RETAIL_WEEK_OF_YEAR', retailWeekOfYearExpr, 'Retail Week number (1-53) within the Retail Year');

        let weeksInMonths = [];
        switch(patternUpper) {
            case '4-5-4': weeksInMonths = [4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5, 4]; break;
            case '4-4-5': weeksInMonths = [4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5]; break;
            case '5-4-4': weeksInMonths = [5, 4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4]; break;
        }
        let monthEndWeeks = [];
        let currentWeekCount = 0;
        for (let i = 0; i < 12; i++) {
            currentWeekCount += weeksInMonths[i];
            monthEndWeeks.push(currentWeekCount);
        }
        // Adjust last month boundary if it's a 53 week year
        const lastMonthBoundary = `IFF(${weeksInRetailYearExpr} = 53, 53, ${monthEndWeeks[11]})`;

        // Map Week to Retail Month and Quarter (including basic 53 week handling)
        const retailMonthCaseExpr = `
            CASE
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[0]} THEN 1
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[1]} THEN 2
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[2]} THEN 3
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[3]} THEN 4
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[4]} THEN 5
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[5]} THEN 6
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[6]} THEN 7
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[7]} THEN 8
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[8]} THEN 9
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[9]} THEN 10
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[10]} THEN 11
                WHEN ${retailWeekOfYearExpr} <= ${lastMonthBoundary} THEN 12 -- Assign week 53 to month 12
                ELSE NULL -- Should not happen if week calc is correct
            END`;
        addColumn('RETAIL_MONTH_OF_YEAR', retailMonthCaseExpr, `Retail month number (1-12) based on ${patternUpper} pattern, week 53 assigned to M12`);

        const retailQuarterCaseExpr = `
            CASE
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[2]} THEN 1
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[5]} THEN 2
                WHEN ${retailWeekOfYearExpr} <= ${monthEndWeeks[8]} THEN 3
                WHEN ${retailWeekOfYearExpr} <= ${lastMonthBoundary} THEN 4 -- Assign week 53 to Q4
                ELSE NULL -- Should not happen
            END`;
        addColumn('RETAIL_QUARTER', retailQuarterCaseExpr, `Retail quarter number (1-4) based on ${patternUpper} pattern, week 53 assigned to Q4`);

        addColumn('RETAIL_PATTERN', `'${patternUpper}'`, 'The retail week grouping pattern used');

        // ----- View Creation -----
        const viewComment = `Retail calendar view (${patternUpper} Pattern, ISO Weeks) based on ${grainUpper} grain standard calendar. Retail year ends approx month ${endMonthInt}. Includes basic 53-week year handling (assigns week 53 to M12/Q4).`;
        createViewSql = `
            CREATE OR REPLACE VIEW ${retailViewName}
            COMMENT = '${viewComment.replace(/'/g, "''")}'
            AS
            SELECT
                base.*,
                ${selExprs.join(',\n    ')}
            FROM ${baseViewName} base
        `;

        let createViewStmt = snowflake.createStatement({ sqlText: createViewSql });
        createViewStmt.execute();

        return `Retail Calendar view '${retailViewName}' generated successfully based on '${baseViewName}' using ${patternUpper} pattern with year end month ${endMonthInt}. Access the view with SELECT * FROM ${retailViewName}.`;

    } catch (err) {
        return `ERROR creating retail calendar view '${retailViewName}': ${err.code} - ${err.message}\nAttempted SQL:\n${createViewSql}`;
    }
$$;
