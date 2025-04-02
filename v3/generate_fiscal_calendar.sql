-- Stored procedure to generate the fiscal calendar view (Refined FLOAT argument handling)
CREATE OR REPLACE PROCEDURE GENERATE_FISCAL_CALENDAR(
    TIME_GRAIN VARCHAR(10),
    FISCAL_YEAR_START_MONTH_ARG FLOAT -- Argument as FLOAT
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

    // *** Convert FLOAT argument to Integer inside JavaScript ***
    // Use a distinct name for the internal integer variable
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
        // *** Ensure all references use startMonthInt ***
        const fiscalYearExpr = `YEAR(DATEADD(month, 13 - ${startMonthInt}, base.DATE_KEY))`;
        const firstDayFYExpr = `DATE_FROM_PARTS(${fiscalYearExpr} - 1, ${startMonthInt}, 1)`;
        const lastDayFYCorrectExpr = `DATEADD(day, -1, DATEADD(year, 1, ${firstDayFYExpr}))`;
        const fiscalMonthExpr = `MOD(MONTH(base.DATE_KEY) - ${startMonthInt} + 12, 12) + 1`;
        const fiscalQuarterExpr = `FLOOR(DATEDIFF(month, ${firstDayFYExpr}, base.DATE_KEY) / 3) + 1`;
        const fiscalQuarterNameExpr = `CONCAT('FY', ${fiscalYearExpr}, '-Q', ${fiscalQuarterExpr})`;
        const fiscalDayOfYearExpr = `DATEDIFF(day, ${firstDayFYExpr}, base.DATE_KEY) + 1`;
        const fiscalWeekOfYearExpr = `FLOOR(DATEDIFF(day, ${firstDayFYExpr}, base.DATE_KEY) / 7) + 1`;

        // ----- Build Fiscal Column Definitions -----
        let fiscalColumns = [];

        fiscalColumns.push(`${fiscalYearExpr} AS FISCAL_YEAR`);
        fiscalColumns.push(`${firstDayFYExpr} AS FIRST_DAY_OF_FISCAL_YEAR`);
        fiscalColumns.push(`${lastDayFYCorrectExpr} AS LAST_DAY_OF_FISCAL_YEAR`);

        if (grainUpper !== 'YEAR') {
            fiscalColumns.push(`${fiscalQuarterExpr} AS FISCAL_QUARTER`);
            fiscalColumns.push(`${fiscalQuarterNameExpr} AS FISCAL_QUARTER_NAME`);
            fiscalColumns.push(`${fiscalMonthExpr} AS FISCAL_MONTH_OF_YEAR`);
        }

        if (['DAY', 'HOUR', 'MINUTE', 'SECOND'].includes(grainUpper)) {
             fiscalColumns.push(`${fiscalDayOfYearExpr} AS FISCAL_DAY_OF_YEAR`);
             fiscalColumns.push(`${fiscalWeekOfYearExpr} AS FISCAL_WEEK_OF_YEAR`);
        } else if (grainUpper === 'MONTH') {
             fiscalColumns.push(`1 AS FISCAL_DAY_OF_YEAR`);
             fiscalColumns.push(`${fiscalWeekOfYearExpr} AS FISCAL_WEEK_OF_YEAR`);
        }

        // ----- Combine SELECT expressions -----
        if (fiscalColumns.length > 0) {
             selectExpressions += `,\n    ` + fiscalColumns.join(',\n    ');
        }

        // ----- View Creation -----
        // *** Added startMonthInt to the view comment for verification ***
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

        return "Fiscal Calendar view generated successfully: " + fiscalViewName + " based on " + baseViewName + " (Fiscal Start Month: " + startMonthInt + ")";

    } catch (err) {
        return `ERROR creating view ${fiscalViewName}: ${err.code} - ${err.message}\nAttempted SQL:\n${createViewSql}`;
    }
$$
;