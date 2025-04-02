-- Stored procedure to generate a Simple Dynamic Time Periods view on top of an existing calendar
CREATE OR REPLACE PROCEDURE GENERATE_SIMPLE_DYNAMIC_PERIODS(
    BASE_CALENDAR_NAME VARCHAR,
    TIME_GRAIN VARCHAR(10),
    RELATIVE_PERIODS_COUNT_ARG FLOAT DEFAULT 12.0 -- Using FLOAT for compatibility
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // ----- Input Validation -----
    const validTimeGrains = ['SECOND', 'MINUTE', 'HOUR', 'DAY'];
    const grainUpper = TIME_GRAIN.toUpperCase();
    if (validTimeGrains.indexOf(grainUpper) === -1) {
        return `ERROR: Dynamic Periods generation currently supports grains: ${validTimeGrains.join(', ')}. Grain provided: ${TIME_GRAIN}`;
    }

    // Validate base calendar name
    if (!BASE_CALENDAR_NAME || BASE_CALENDAR_NAME.trim() === '') {
        return "ERROR: BASE_CALENDAR_NAME must be provided.";
    }

    // Convert FLOAT to INTEGER for RELATIVE_PERIODS_COUNT
    if (RELATIVE_PERIODS_COUNT_ARG === undefined || RELATIVE_PERIODS_COUNT_ARG === null || isNaN(RELATIVE_PERIODS_COUNT_ARG)) {
        return "ERROR: RELATIVE_PERIODS_COUNT_ARG must be provided as a valid number.";
    }

    const RELATIVE_PERIODS_COUNT = Math.trunc(RELATIVE_PERIODS_COUNT_ARG);

    // Validate relative periods count
    if (RELATIVE_PERIODS_COUNT < 1) {
        return "ERROR: RELATIVE_PERIODS_COUNT must be at least 1.";
    }

    // ----- Dynamic Name Construction -----
    const dynamicViewName = `DYNAMIC_PERIODS_SIMPLE_${grainUpper}`;
    const finalViewName = dynamicViewName;

    // ----- SQL Construction -----
    let createViewSql = '';
    let currentStep = 'Starting';

    try {
        currentStep = 'Constructing dynamic period expressions';

        // Basic period flags
        const periodFlags = `
            -- Basic period flags
            c.DATE_TIME = DATE_TRUNC('${grainUpper}', CURRENT_TIMESTAMP()) AS IS_NOW,
            c.DATE_KEY = CURRENT_DATE() AS IS_TODAY,
            c.DATE_KEY = DATEADD(day, -1, CURRENT_DATE()) AS IS_YESTERDAY,
            c.DATE_KEY = DATEADD(day, 1, CURRENT_DATE()) AS IS_TOMORROW,

            -- Week periods
            c.DATE_KEY BETWEEN DATE_TRUNC('week', CURRENT_DATE()) AND LAST_DAY(CURRENT_DATE(), 'week') AS IS_THIS_WEEK,
            c.DATE_KEY BETWEEN DATE_TRUNC('week', DATEADD(week, -1, CURRENT_DATE())) AND LAST_DAY(DATEADD(week, -1, CURRENT_DATE()), 'week') AS IS_LAST_WEEK,
            c.DATE_KEY BETWEEN DATE_TRUNC('week', DATEADD(week, 1, CURRENT_DATE())) AND LAST_DAY(DATEADD(week, 1, CURRENT_DATE()), 'week') AS IS_NEXT_WEEK,

            -- Month periods
            c.DATE_KEY BETWEEN DATE_TRUNC('month', CURRENT_DATE()) AND LAST_DAY(CURRENT_DATE(), 'month') AS IS_THIS_MONTH,
            c.DATE_KEY BETWEEN DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE())) AND LAST_DAY(DATEADD(month, -1, CURRENT_DATE()), 'month') AS IS_LAST_MONTH,
            c.DATE_KEY BETWEEN DATE_TRUNC('month', DATEADD(month, 1, CURRENT_DATE())) AND LAST_DAY(DATEADD(month, 1, CURRENT_DATE()), 'month') AS IS_NEXT_MONTH,

            -- Quarter periods
            c.DATE_KEY BETWEEN DATE_TRUNC('quarter', CURRENT_DATE()) AND LAST_DAY(CURRENT_DATE(), 'quarter') AS IS_THIS_QUARTER,
            c.DATE_KEY BETWEEN DATE_TRUNC('quarter', DATEADD(quarter, -1, CURRENT_DATE())) AND LAST_DAY(DATEADD(quarter, -1, CURRENT_DATE()), 'quarter') AS IS_LAST_QUARTER,
            c.DATE_KEY BETWEEN DATE_TRUNC('quarter', DATEADD(quarter, 1, CURRENT_DATE())) AND LAST_DAY(DATEADD(quarter, 1, CURRENT_DATE()), 'quarter') AS IS_NEXT_QUARTER,

            -- Year periods
            c.DATE_KEY BETWEEN DATE_TRUNC('year', CURRENT_DATE()) AND LAST_DAY(CURRENT_DATE(), 'year') AS IS_THIS_YEAR,
            c.DATE_KEY BETWEEN DATE_TRUNC('year', DATEADD(year, -1, CURRENT_DATE())) AND LAST_DAY(DATEADD(year, -1, CURRENT_DATE()), 'year') AS IS_LAST_YEAR,
            c.DATE_KEY BETWEEN DATE_TRUNC('year', DATEADD(year, 1, CURRENT_DATE())) AND LAST_DAY(DATEADD(year, 1, CURRENT_DATE()), 'year') AS IS_NEXT_YEAR,

            -- Year-to-date, Quarter-to-date, Month-to-date
            c.DATE_KEY BETWEEN DATE_TRUNC('year', CURRENT_DATE()) AND CURRENT_DATE() AS IS_YTD,
            c.DATE_KEY BETWEEN DATE_TRUNC('quarter', CURRENT_DATE()) AND CURRENT_DATE() AS IS_QTD,
            c.DATE_KEY BETWEEN DATE_TRUNC('month', CURRENT_DATE()) AND CURRENT_DATE() AS IS_MTD,
            c.DATE_KEY BETWEEN DATE_TRUNC('week', CURRENT_DATE()) AND CURRENT_DATE() AS IS_WTD,

            -- Rolling periods
            c.DATE_KEY BETWEEN DATEADD(day, -6, CURRENT_DATE()) AND CURRENT_DATE() AS IS_LAST_7_DAYS,
            c.DATE_KEY BETWEEN DATEADD(day, -29, CURRENT_DATE()) AND CURRENT_DATE() AS IS_LAST_30_DAYS,
            c.DATE_KEY BETWEEN DATEADD(day, -89, CURRENT_DATE()) AND CURRENT_DATE() AS IS_LAST_90_DAYS,
            c.DATE_KEY BETWEEN DATEADD(day, -364, CURRENT_DATE()) AND CURRENT_DATE() AS IS_LAST_365_DAYS,

            -- Relative distance calculations - CORRECTED ORDER!
            -- Dates in the future should have positive values, past dates negative
            DATEDIFF('day', CURRENT_DATE(), c.DATE_KEY) AS DAYS_FROM_TODAY,
            DATEDIFF('week', DATE_TRUNC('week', CURRENT_DATE()), DATE_TRUNC('week', c.DATE_KEY)) AS WEEKS_FROM_THIS_WEEK,
            DATEDIFF('month', DATE_TRUNC('month', CURRENT_DATE()), DATE_TRUNC('month', c.DATE_KEY)) AS MONTHS_FROM_THIS_MONTH,
            DATEDIFF('quarter', DATE_TRUNC('quarter', CURRENT_DATE()), DATE_TRUNC('quarter', c.DATE_KEY)) AS QUARTERS_FROM_THIS_QUARTER,
            DATEDIFF('year', DATE_TRUNC('year', CURRENT_DATE()), DATE_TRUNC('year', c.DATE_KEY)) AS YEARS_FROM_THIS_YEAR
        `;

        // Generate relative period flags
        let relativePeriods = [];

        for (let i = 1; i <= RELATIVE_PERIODS_COUNT; i++) {
            // Past periods (negative values)
            relativePeriods.push(`
            -- Period minus ${i}
            c.DATE_KEY BETWEEN DATE_TRUNC('week', DATEADD(week, -${i}, CURRENT_DATE())) AND
                LAST_DAY(DATEADD(week, -${i}, CURRENT_DATE()), 'week')
                AS IS_WEEK_MINUS_${i},
            c.DATE_KEY BETWEEN DATE_TRUNC('month', DATEADD(month, -${i}, CURRENT_DATE())) AND
                LAST_DAY(DATEADD(month, -${i}, CURRENT_DATE()), 'month')
                AS IS_MONTH_MINUS_${i},
            c.DATE_KEY BETWEEN DATE_TRUNC('quarter', DATEADD(quarter, -${i}, CURRENT_DATE())) AND
                LAST_DAY(DATEADD(quarter, -${i}, CURRENT_DATE()), 'quarter')
                AS IS_QUARTER_MINUS_${i},
            c.DATE_KEY BETWEEN DATE_TRUNC('year', DATEADD(year, -${i}, CURRENT_DATE())) AND
                LAST_DAY(DATEADD(year, -${i}, CURRENT_DATE()), 'year')
                AS IS_YEAR_MINUS_${i}`);

            // Future periods (positive values) - only for a smaller subset
            if (i <= Math.min(4, RELATIVE_PERIODS_COUNT)) {
                relativePeriods.push(`
                -- Period plus ${i}
                c.DATE_KEY BETWEEN DATE_TRUNC('week', DATEADD(week, ${i}, CURRENT_DATE())) AND
                    LAST_DAY(DATEADD(week, ${i}, CURRENT_DATE()), 'week')
                    AS IS_WEEK_PLUS_${i},
                c.DATE_KEY BETWEEN DATE_TRUNC('month', DATEADD(month, ${i}, CURRENT_DATE())) AND
                    LAST_DAY(DATEADD(month, ${i}, CURRENT_DATE()), 'month')
                    AS IS_MONTH_PLUS_${i},
                c.DATE_KEY BETWEEN DATE_TRUNC('quarter', DATEADD(quarter, ${i}, CURRENT_DATE())) AND
                    LAST_DAY(DATEADD(quarter, ${i}, CURRENT_DATE()), 'quarter')
                    AS IS_QUARTER_PLUS_${i},
                c.DATE_KEY BETWEEN DATE_TRUNC('year', DATEADD(year, ${i}, CURRENT_DATE())) AND
                    LAST_DAY(DATEADD(year, ${i}, CURRENT_DATE()), 'year')
                    AS IS_YEAR_PLUS_${i}`);
            }
        }

        // Period descriptions for user-friendly labels - CORRECTED!
        const periodDescriptions = `
            -- Period descriptions (for display purposes)
            CASE
                WHEN IS_TODAY THEN 'Today'
                WHEN IS_YESTERDAY THEN 'Yesterday'
                WHEN IS_TOMORROW THEN 'Tomorrow'
                WHEN IS_THIS_WEEK THEN 'This Week'
                WHEN IS_LAST_WEEK THEN 'Last Week'
                WHEN IS_NEXT_WEEK THEN 'Next Week'
                WHEN IS_THIS_MONTH THEN 'This Month'
                WHEN IS_LAST_MONTH THEN 'Last Month'
                WHEN IS_NEXT_MONTH THEN 'Next Month'
                WHEN IS_THIS_QUARTER THEN 'This Quarter'
                WHEN IS_LAST_QUARTER THEN 'Last Quarter'
                WHEN IS_NEXT_QUARTER THEN 'Next Quarter'
                WHEN IS_THIS_YEAR THEN 'This Year'
                WHEN IS_LAST_YEAR THEN 'Last Year'
                WHEN IS_NEXT_YEAR THEN 'Next Year'
                WHEN IS_YTD THEN 'Year to Date'
                WHEN IS_QTD THEN 'Quarter to Date'
                WHEN IS_MTD THEN 'Month to Date'
                WHEN IS_LAST_7_DAYS THEN 'Last 7 Days'
                WHEN IS_LAST_30_DAYS THEN 'Last 30 Days'
                WHEN IS_LAST_90_DAYS THEN 'Last 90 Days'
                WHEN IS_LAST_365_DAYS THEN 'Last 365 Days'
                ELSE 'Other Period'
            END AS CURRENT_PERIOD_DESCRIPTION,

            -- Dynamic month/quarter/year offset descriptions - CORRECTED ORDER
            CASE
                WHEN MONTHS_FROM_THIS_MONTH = 0 THEN 'Current Month'
                WHEN MONTHS_FROM_THIS_MONTH = -1 THEN 'Previous Month'
                WHEN MONTHS_FROM_THIS_MONTH = 1 THEN 'Next Month'
                WHEN MONTHS_FROM_THIS_MONTH < 0 THEN ABS(MONTHS_FROM_THIS_MONTH) || ' Months Ago'
                ELSE MONTHS_FROM_THIS_MONTH || ' Months From Now'
            END AS RELATIVE_MONTH_DESCRIPTION,

            CASE
                WHEN QUARTERS_FROM_THIS_QUARTER = 0 THEN 'Current Quarter'
                WHEN QUARTERS_FROM_THIS_QUARTER = -1 THEN 'Previous Quarter'
                WHEN QUARTERS_FROM_THIS_QUARTER = 1 THEN 'Next Quarter'
                WHEN QUARTERS_FROM_THIS_QUARTER < 0 THEN ABS(QUARTERS_FROM_THIS_QUARTER) || ' Quarters Ago'
                ELSE QUARTERS_FROM_THIS_QUARTER || ' Quarters From Now'
            END AS RELATIVE_QUARTER_DESCRIPTION,

            CASE
                WHEN YEARS_FROM_THIS_YEAR = 0 THEN 'Current Year'
                WHEN YEARS_FROM_THIS_YEAR = -1 THEN 'Previous Year'
                WHEN YEARS_FROM_THIS_YEAR = 1 THEN 'Next Year'
                WHEN YEARS_FROM_THIS_YEAR < 0 THEN ABS(YEARS_FROM_THIS_YEAR) || ' Years Ago'
                ELSE YEARS_FROM_THIS_YEAR || ' Years From Now'
            END AS RELATIVE_YEAR_DESCRIPTION,

            -- Simple fiscal period indicators (if fiscal columns exist in base calendar)
            CASE
                WHEN EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = UPPER('${BASE_CALENDAR_NAME}')
                            AND COLUMN_NAME = 'FISCAL_YEAR')
                THEN c.FISCAL_YEAR = YEAR(CURRENT_DATE())
                ELSE FALSE
            END AS IS_CURRENT_FISCAL_YEAR,

            CASE
                WHEN EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = UPPER('${BASE_CALENDAR_NAME}')
                            AND COLUMN_NAME = 'FISCAL_QUARTER')
                THEN c.FISCAL_YEAR = YEAR(CURRENT_DATE()) AND
                    c.FISCAL_QUARTER = QUARTER(CURRENT_DATE())
                ELSE FALSE
            END AS IS_CURRENT_FISCAL_QUARTER
        `;

        // ----- View Creation -----
        currentStep = `Constructing CREATE VIEW statement for ${finalViewName}`;

        const viewComment = `Simple dynamic time periods view based on ${grainUpper} grain with ${RELATIVE_PERIODS_COUNT} relative periods. Uses the system current date for all calculations.`;

        createViewSql = `
            CREATE OR REPLACE VIEW ${finalViewName}
            COMMENT = '${viewComment.replace(/'/g, "''")}'
            AS
            SELECT
                c.*,
                ${periodFlags},
                ${relativePeriods.join(',\n')},
                ${periodDescriptions}
            FROM ${BASE_CALENDAR_NAME} c
        `;

        currentStep = `Executing CREATE VIEW statement for ${finalViewName}`;
        snowflake.createStatement({ sqlText: createViewSql }).execute();

        return `Simple Dynamic Periods view generated successfully: ${finalViewName}. Based on: ${BASE_CALENDAR_NAME}.`;

    } catch (err) {
        return `ERROR during Dynamic Periods generation (Step: ${currentStep}): ${err.code} - ${err.message}\nAttempted SQL (if applicable):\n${createViewSql}`;
    }
$$
;