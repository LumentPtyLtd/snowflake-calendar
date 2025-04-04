/*==============================================================================
  FISCAL CALENDAR GENERATOR
  
  This script creates a fiscal calendar view that extends the standard Gregorian
  calendar with fiscal year, quarter, month, and other fiscal period attributes.
  The fiscal calendar can be customized to start in any month of the year.
  
  The resulting fiscal calendar includes:
  - Fiscal year and quarter identifiers
  - Fiscal year start and end dates
  - Fiscal month, day, and week numbers
  - Fiscal quarter names (e.g., "FY2023-Q1")
  
  USAGE:
  ------
  CALL GENERATE_FISCAL_CALENDAR(
      'DAY',  -- Time grain
      7       -- Fiscal year start month (1-12, 7 = July)
  );
  
  PARAMETERS:
  -----------
  TIME_GRAIN: The granularity level (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR)
  FISCAL_YEAR_START_MONTH: The month when the fiscal year starts (1-12)
  
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

-- Stored procedure to generate the fiscal calendar view (with FLOAT argument handling)
CREATE OR REPLACE PROCEDURE GENERATE_FISCAL_CALENDAR(
    TIME_GRAIN VARCHAR(10),
    FISCAL_YEAR_START_MONTH_ARG FLOAT -- Argument as FLOAT for JavaScript integration
)
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

    if (FISCAL_YEAR_START_MONTH_ARG === undefined || FISCAL_YEAR_START_MONTH_ARG === null || isNaN(FISCAL_YEAR_START_MONTH_ARG)) {
         return "ERROR: FISCAL_YEAR_START_MONTH must be provided as a valid number.";
    }

    // Convert FLOAT argument to Integer inside JavaScript
    const startMonthInt = Math.trunc(FISCAL_YEAR_START_MONTH_ARG);

    // Validate the integer value
    if (startMonthInt < 1 || startMonthInt > 12) {
        return "ERROR: FISCAL_YEAR_START_MONTH must be between 1 and 12 (derived value: " + startMonthInt + ").";
    }

    // ----- Dynamic Name Construction -----
    const baseViewName = "CALENDAR_" + grainUpper;
    const fiscalViewName = "FISCAL_CALENDAR_" + grainUpper;

    // Declare SQL parts outside the try block
    let createViewSql = '';
    let selectExpressions = `base.*`; // Select all from base alias

    try {
        // ----- Fiscal Calculation Logic -----
        // Ensure all references use startMonthInt
        const fiscalYearExpr = `YEAR(DATEADD(month, 13 - ${startMonthInt}, base.DATE_KEY))`;
        const firstDayFYExpr = `DATE_FROM_PARTS(${fiscalYearExpr} - 1, ${startMonthInt}, 1)`;
        const lastDayFYCorrectExpr = `DATEADD(day, -1, DATEADD(year, 1, ${firstDayFYExpr}))`;
        const fiscalMonthExpr = `MOD(MONTH(base.DATE_KEY) - ${startMonthInt} + 12, 12) + 1`;
        const fiscalQuarterExpr = `FLOOR(DATEDIFF(month, ${firstDayFYExpr}, base.DATE_KEY) / 3) + 1`;
        const fiscalQuarterNameExpr = `CONCAT('FY', ${fiscalYearExpr}, '-Q', ${fiscalQuarterExpr})`;
        const fiscalDayOfYearExpr = `DATEDIFF(day, ${firstDayFYExpr}, base.DATE_KEY) + 1`;
        const fiscalWeekOfYearExpr = `FLOOR(DATEDIFF(day, ${firstDayFYExpr}, base.DATE_KEY) / 7) + 1`;

        // ----- Build Fiscal Column Definitions with Comments -----
        let fiscalColumns = [];

        fiscalColumns.push(`${fiscalYearExpr} AS FISCAL_YEAR /* The fiscal year based on a start month of ${startMonthInt} */`);
        fiscalColumns.push(`${firstDayFYExpr} AS FIRST_DAY_OF_FISCAL_YEAR /* First calendar date in the fiscal year */`);
        fiscalColumns.push(`${lastDayFYCorrectExpr} AS LAST_DAY_OF_FISCAL_YEAR /* Last calendar date in the fiscal year */`);

        if (grainUpper !== 'YEAR') {
            fiscalColumns.push(`${fiscalQuarterExpr} AS FISCAL_QUARTER /* Fiscal quarter (1-4) */`);
            fiscalColumns.push(`${fiscalQuarterNameExpr} AS FISCAL_QUARTER_NAME /* Fiscal quarter with year, e.g., FY2023-Q1 */`);
            fiscalColumns.push(`${fiscalMonthExpr} AS FISCAL_MONTH_OF_YEAR /* Fiscal month number (1-12) */`);
        }

        if (['DAY', 'HOUR', 'MINUTE', 'SECOND'].includes(grainUpper)) {
             fiscalColumns.push(`${fiscalDayOfYearExpr} AS FISCAL_DAY_OF_YEAR /* Sequential day number (1-366) in the fiscal year */`);
             fiscalColumns.push(`${fiscalWeekOfYearExpr} AS FISCAL_WEEK_OF_YEAR /* Week number (1-53) in the fiscal year */`);
        } else if (grainUpper === 'MONTH') {
             fiscalColumns.push(`1 AS FISCAL_DAY_OF_YEAR /* Always 1 for MONTH grain */`);
             fiscalColumns.push(`${fiscalWeekOfYearExpr} AS FISCAL_WEEK_OF_YEAR /* Week number in the fiscal year of the first day of month */`);
        }

        // ----- Combine SELECT expressions -----
        if (fiscalColumns.length > 0) {
             selectExpressions += `,\n    ` + fiscalColumns.join(',\n    ');
        }

        // ----- View Creation -----
        const viewComment = `Fiscal calendar view based on ${grainUpper} grain standard calendar, assuming fiscal year starts month ${startMonthInt}/1.`;
        createViewSql = `
            CREATE OR REPLACE VIEW ${fiscalViewName}
            COMMENT = '${viewComment.replace(/'/g, "''")}'
            AS
            SELECT
                ${selectExpressions}
            FROM ${baseViewName} base
        `;

        // Execute the CREATE VIEW statement
        let createViewStmt = snowflake.createStatement({
            sqlText: createViewSql
        });
        createViewStmt.execute();

        return `Fiscal Calendar view '${fiscalViewName}' generated successfully based on '${baseViewName}' (Fiscal Year Start Month: ${startMonthInt}). Access the view with SELECT * FROM ${fiscalViewName}.`;

    } catch (err) {
        return `ERROR creating fiscal calendar view '${fiscalViewName}': ${err.code} - ${err.message}\nAttempted SQL:\n${createViewSql}`;
    }
$$;
