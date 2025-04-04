/*==============================================================================
  MULTI-TIMEZONE DYNAMIC PERIODS GENERATOR
  
  This script creates a dynamic periods view that adds time-relative flags and
  calculations to any calendar with support for multiple time zones. These flags
  automatically update based on the current system time in each specified timezone.
  
  The multi-timezone dynamic periods view includes:
  - Current period flags for each timezone (is_[tz]_today, is_[tz]_this_month, etc.)
  - Relative period flags for each timezone
  - Rolling period flags for each timezone
  - Relative distance calculations for each timezone
  - Period descriptions for user displays
  
  USAGE:
  ------
  CALL GENERATE_DYNAMIC_PERIODS(
      'DAY',                                   -- Time grain
      ARRAY_CONSTRUCT('Etc/UTC', 'Europe/London', 'America/New_York'), -- Time zones
      12                                       -- Number of relative periods to generate
  );
  
  PARAMETERS:
  -----------
  TIME_GRAIN: The granularity level (SECOND, MINUTE, HOUR, DAY)
  TIME_ZONES: Array of time zone names to support
  RELATIVE_PERIODS_COUNT: Number of historical relative periods to generate (default: 12)
  
  RETURNS:
  --------
  A string indicating success or failure with details
  
  NOTES:
  ------
  - Use this when you need to support users across multiple time zones
  - Time zone identifiers follow the IANA timezone database format
  - Common options: 'Etc/UTC', 'Europe/London', 'America/New_York', 'Australia/Sydney'
  - Column names are dynamically generated with timezone prefixes
  - View name includes timezone abbreviations or count for easy identification
  
  AUTHOR: [Your Name]
  DATE CREATED: 2024-04-04
  LAST MODIFIED: 2024-04-04
==============================================================================*/

-- Stored procedure to generate a Dynamic Time Periods view with multi-timezone support
CREATE OR REPLACE PROCEDURE GENERATE_DYNAMIC_PERIODS(
    TIME_GRAIN VARCHAR(10),
    TIME_ZONES ARRAY DEFAULT ARRAY_CONSTRUCT('Etc/UTC'),
    RELATIVE_PERIODS_COUNT_ARG FLOAT DEFAULT 12.0 -- Using FLOAT instead of INTEGER
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
    const baseViewName = "UNIFIED_CALENDAR_TABLE_" + grainUpper;

    // Validate time zones array is not empty
    if (!TIME_ZONES || TIME_ZONES.length === 0) {
        return "ERROR: At least one time zone must be specified in TIME_ZONES array.";
    }

    // Handle FLOAT to INTEGER conversion for RELATIVE_PERIODS_COUNT
    if (RELATIVE_PERIODS_COUNT_ARG === undefined || RELATIVE_PERIODS_COUNT_ARG === null || isNaN(RELATIVE_PERIODS_COUNT_ARG)) {
        return "ERROR: RELATIVE_PERIODS_COUNT_ARG must be provided as a valid number.";
    }

    // Convert FLOAT to INTEGER
    const RELATIVE_PERIODS_COUNT = Math.trunc(RELATIVE_PERIODS_COUNT_ARG);

    // Validate relative periods count
    if (RELATIVE_PERIODS_COUNT < 1) {
        return "ERROR: RELATIVE_PERIODS_COUNT must be at least 1.";
    }

    // ----- Dynamic Name Construction -----
    const dynamicViewName = `DYNAMIC_PERIODS_${grainUpper}`;
    let viewNameSuffix = '';

    // Add timezone abbreviation to view name if not default UTC only
    if (TIME_ZONES.length === 1 && TIME_ZONES[0] === 'Etc/UTC') {
        // Default case, no suffix needed
    } else if (TIME_ZONES.length <= 3) {
        // For a small number of timezones, include abbreviated codes in the name
        const tzCodes = TIME_ZONES.map(tz => {
            // Extract abbreviation: e.g., Australia/Adelaide -> ADEL
            const parts = tz.split('/');
            const lastPart = parts[parts.length - 1];
            return lastPart.substring(0, 4).toUpperCase();
        });
        viewNameSuffix = `_${tzCodes.join('_')}`;
    } else {
        // For many timezones, just indicate the count
        viewNameSuffix = `_${TIME_ZONES.length}TZ`;
    }

    const finalViewName = `${dynamicViewName}${viewNameSuffix}`;

    // ----- SQL Construction -----
    let createViewSql = '';
    let currentStep = 'Starting';

    try {
        currentStep = 'Constructing time zone case statements';

        // Generate the time zone conversion expressions
        let tzCaseStatements = [];

        TIME_ZONES.forEach(tz => {
            const safeColumnName = tz.replace(/[^a-zA-Z0-9_]/g, '_');
            const tzColumnPrefix = `CURRENT_${safeColumnName}`;

            // Current timestamp in the time zone - this is the anchor for all relative calculations
            tzCaseStatements.push(`
                -- Current timestamp expressions for ${tz}
                CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()) AS ${tzColumnPrefix}_TIMESTAMP,
                DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())) AS ${tzColumnPrefix}_DATE,

                -- Basic period flags for ${tz}
                c.DATE_TIME = DATE_TRUNC('${grainUpper}', CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())) AS IS_${tzColumnPrefix}_NOW,
                c.DATE_KEY = DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())) AS IS_${tzColumnPrefix}_TODAY,
                c.DATE_KEY = DATEADD(day, -1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AS IS_${tzColumnPrefix}_YESTERDAY,
                c.DATE_KEY = DATEADD(day, 1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AS IS_${tzColumnPrefix}_TOMORROW,

                -- Week periods for ${tz}
                c.DATE_KEY BETWEEN DATE_TRUNC('week', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    LAST_DAY(DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())), 'week')
                    AS IS_${tzColumnPrefix}_THIS_WEEK,
                c.DATE_KEY BETWEEN DATE_TRUNC('week', DATEADD(week, -1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                    LAST_DAY(DATEADD(week, -1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'week')
                    AS IS_${tzColumnPrefix}_LAST_WEEK,
                c.DATE_KEY BETWEEN DATE_TRUNC('week', DATEADD(week, 1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                    LAST_DAY(DATEADD(week, 1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'week')
                    AS IS_${tzColumnPrefix}_NEXT_WEEK,

                -- Month periods for ${tz}
                c.DATE_KEY BETWEEN DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    LAST_DAY(DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())), 'month')
                    AS IS_${tzColumnPrefix}_THIS_MONTH,
                c.DATE_KEY BETWEEN DATE_TRUNC('month', DATEADD(month, -1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                    LAST_DAY(DATEADD(month, -1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'month')
                    AS IS_${tzColumnPrefix}_LAST_MONTH,
                c.DATE_KEY BETWEEN DATE_TRUNC('month', DATEADD(month, 1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                    LAST_DAY(DATEADD(month, 1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'month')
                    AS IS_${tzColumnPrefix}_NEXT_MONTH,

                -- Quarter periods for ${tz}
                c.DATE_KEY BETWEEN DATE_TRUNC('quarter', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    LAST_DAY(DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())), 'quarter')
                    AS IS_${tzColumnPrefix}_THIS_QUARTER,
                c.DATE_KEY BETWEEN DATE_TRUNC('quarter', DATEADD(quarter, -1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                    LAST_DAY(DATEADD(quarter, -1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'quarter')
                    AS IS_${tzColumnPrefix}_LAST_QUARTER,

                -- Year periods for ${tz}
                c.DATE_KEY BETWEEN DATE_TRUNC('year', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    LAST_DAY(DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())), 'year')
                    AS IS_${tzColumnPrefix}_THIS_YEAR,
                c.DATE_KEY BETWEEN DATE_TRUNC('year', DATEADD(year, -1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                    LAST_DAY(DATEADD(year, -1, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'year')
                    AS IS_${tzColumnPrefix}_LAST_YEAR,

                -- Year-to-date, Quarter-to-date, Month-to-date for ${tz}
                c.DATE_KEY BETWEEN DATE_TRUNC('year', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))
                    AS IS_${tzColumnPrefix}_YTD,
                c.DATE_KEY BETWEEN DATE_TRUNC('quarter', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))
                    AS IS_${tzColumnPrefix}_QTD,
                c.DATE_KEY BETWEEN DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))
                    AS IS_${tzColumnPrefix}_MTD,
                c.DATE_KEY BETWEEN DATE_TRUNC('week', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))
                    AS IS_${tzColumnPrefix}_WTD,

                -- Rolling periods for ${tz}
                c.DATE_KEY BETWEEN DATEADD(day, -6, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))
                    AS IS_${tzColumnPrefix}_LAST_7_DAYS,
                c.DATE_KEY BETWEEN DATEADD(day, -29, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))
                    AS IS_${tzColumnPrefix}_LAST_30_DAYS,
                c.DATE_KEY BETWEEN DATEADD(day, -89, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))
                    AS IS_${tzColumnPrefix}_LAST_90_DAYS,
                c.DATE_KEY BETWEEN DATEADD(day, -364, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AND
                    DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))
                    AS IS_${tzColumnPrefix}_LAST_365_DAYS,

                -- Relative distance calculations for ${tz}
                DATEDIFF('day', c.DATE_KEY, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))) AS ${tzColumnPrefix}_DAYS_FROM_TODAY,
                DATEDIFF('week', DATE_TRUNC('week', c.DATE_KEY), DATE_TRUNC('week', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AS ${tzColumnPrefix}_WEEKS_FROM_THIS_WEEK,
                DATEDIFF('month', DATE_TRUNC('month', c.DATE_KEY), DATE_TRUNC('month', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AS ${tzColumnPrefix}_MONTHS_FROM_THIS_MONTH,
                DATEDIFF('quarter', DATE_TRUNC('quarter', c.DATE_KEY), DATE_TRUNC('quarter', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AS ${tzColumnPrefix}_QUARTERS_FROM_THIS_QUARTER,
                DATEDIFF('year', DATE_TRUNC('year', c.DATE_KEY), DATE_TRUNC('year', DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AS ${tzColumnPrefix}_YEARS_FROM_THIS_YEAR`
            );

            // Generate relative period flags based on RELATIVE_PERIODS_COUNT
            let relativePeriods = [];

            for (let i = 1; i <= RELATIVE_PERIODS_COUNT; i++) {
                // Past periods (negative values)
                relativePeriods.push(`
                -- Period minus ${i} for ${tz}
                c.DATE_KEY BETWEEN DATE_TRUNC('week', DATEADD(week, -${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                    LAST_DAY(DATEADD(week, -${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'week')
                    AS IS_${tzColumnPrefix}_WEEK_MINUS_${i},
                c.DATE_KEY BETWEEN DATE_TRUNC('month', DATEADD(month, -${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                    LAST_DAY(DATEADD(month, -${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'month')
                    AS IS_${tzColumnPrefix}_MONTH_MINUS_${i},
                c.DATE_KEY BETWEEN DATE_TRUNC('quarter', DATEADD(quarter, -${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                    LAST_DAY(DATEADD(quarter, -${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'quarter')
                    AS IS_${tzColumnPrefix}_QUARTER_MINUS_${i},
                c.DATE_KEY BETWEEN DATE_TRUNC('year', DATEADD(year, -${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                    LAST_DAY(DATEADD(year, -${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'year')
                    AS IS_${tzColumnPrefix}_YEAR_MINUS_${i}`);

                // Future periods (positive values) - only for a smaller subset
                if (i <= Math.min(4, RELATIVE_PERIODS_COUNT)) {
                    relativePeriods.push(`
                    -- Period plus ${i} for ${tz}
                    c.DATE_KEY BETWEEN DATE_TRUNC('week', DATEADD(week, ${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                        LAST_DAY(DATEADD(week, ${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'week')
                        AS IS_${tzColumnPrefix}_WEEK_PLUS_${i},
                    c.DATE_KEY BETWEEN DATE_TRUNC('month', DATEADD(month, ${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                        LAST_DAY(DATEADD(month, ${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'month')
                        AS IS_${tzColumnPrefix}_MONTH_PLUS_${i},
                    c.DATE_KEY BETWEEN DATE_TRUNC('quarter', DATEADD(quarter, ${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                        LAST_DAY(DATEADD(quarter, ${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'quarter')
                        AS IS_${tzColumnPrefix}_QUARTER_PLUS_${i},
                    c.DATE_KEY BETWEEN DATE_TRUNC('year', DATEADD(year, ${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP())))) AND
                        LAST_DAY(DATEADD(year, ${i}, DATE(CONVERT_TIMEZONE('Etc/UTC', '${tz}', CURRENT_TIMESTAMP()))), 'year')
                        AS IS_${tzColumnPrefix}_YEAR_PLUS_${i}`);
                }
            }

            // Add relative periods to case statements
            tzCaseStatements.push(relativePeriods.join(',\n'));
        });

        // Common period description logic (independent of time zone)
        // NOTE: These use base.UTC time so are global descriptions
        // For specific time zone period descriptions, would need to add more
        const periodDescriptions = `
            -- Period descriptions (for display purposes)
            CASE
                WHEN IS_CURRENT_Etc_UTC_TODAY THEN 'Today'
                WHEN IS_CURRENT_Etc_UTC_YESTERDAY THEN 'Yesterday'
                WHEN IS_CURRENT_Etc_UTC_TOMORROW THEN 'Tomorrow'
                WHEN IS_CURRENT_Etc_UTC_THIS_WEEK THEN 'This Week'
                WHEN IS_CURRENT_Etc_UTC_LAST_WEEK THEN 'Last Week'
                WHEN IS_CURRENT_Etc_UTC_NEXT_WEEK THEN 'Next Week'
                WHEN IS_CURRENT_Etc_UTC_THIS_MONTH THEN 'This Month'
                WHEN IS_CURRENT_Etc_UTC_LAST_MONTH THEN 'Last Month'
                WHEN IS_CURRENT_Etc_UTC_NEXT_MONTH THEN 'Next Month'
                WHEN IS_CURRENT_Etc_UTC_THIS_QUARTER THEN 'This Quarter'
                WHEN IS_CURRENT_Etc_UTC_LAST_QUARTER THEN 'Last Quarter'
                WHEN IS_CURRENT_Etc_UTC_THIS_YEAR THEN 'This Year'
                WHEN IS_CURRENT_Etc_UTC_LAST_YEAR THEN 'Last Year'
                WHEN IS_CURRENT_Etc_UTC_YTD THEN 'Year to Date'
                WHEN IS_CURRENT_Etc_UTC_QTD THEN 'Quarter to Date'
                WHEN IS_CURRENT_Etc_UTC_MTD THEN 'Month to Date'
                WHEN IS_CURRENT_Etc_UTC_LAST_7_DAYS THEN 'Last 7 Days'
                WHEN IS_CURRENT_Etc_UTC_LAST_30_DAYS THEN 'Last 30 Days'
                WHEN IS_CURRENT_Etc_UTC_LAST_90_DAYS THEN 'Last 90 Days'
                WHEN IS_CURRENT_Etc_UTC_LAST_365_DAYS THEN 'Last 365 Days'
                ELSE 'Other Period'
            END AS CURRENT_PERIOD_DESCRIPTION,

            -- Dynamic month/quarter/year offset descriptions
            CASE
                WHEN CURRENT_Etc_UTC_MONTHS_FROM_THIS_MONTH = 0 THEN 'Current Month'
                WHEN CURRENT_Etc_UTC_MONTHS_FROM_THIS_MONTH = -1 THEN 'Previous Month'
                WHEN CURRENT_Etc_UTC_MONTHS_FROM_THIS_MONTH = 1 THEN 'Next Month'
                WHEN CURRENT_Etc_UTC_MONTHS_FROM_THIS_MONTH < 0 THEN ABS(CURRENT_Etc_UTC_MONTHS_FROM_THIS_MONTH) || ' Months Ago'
                ELSE CURRENT_Etc_UTC_MONTHS_FROM_THIS_MONTH || ' Months From Now'
            END AS RELATIVE_MONTH_DESCRIPTION,

            CASE
                WHEN CURRENT_Etc_UTC_QUARTERS_FROM_THIS_QUARTER = 0 THEN 'Current Quarter'
                WHEN CURRENT_Etc_UTC_QUARTERS_FROM_THIS_QUARTER = -1 THEN 'Previous Quarter'
                WHEN CURRENT_Etc_UTC_QUARTERS_FROM_THIS_QUARTER = 1 THEN 'Next Quarter'
                WHEN CURRENT_Etc_UTC_QUARTERS_FROM_THIS_QUARTER < 0 THEN ABS(CURRENT_Etc_UTC_QUARTERS_FROM_THIS_QUARTER) || ' Quarters Ago'
                ELSE CURRENT_Etc_UTC_QUARTERS_FROM_THIS_QUARTER || ' Quarters From Now'
            END AS RELATIVE_QUARTER_DESCRIPTION,

            CASE
                WHEN CURRENT_Etc_UTC_YEARS_FROM_THIS_YEAR = 0 THEN 'Current Year'
                WHEN CURRENT_Etc_UTC_YEARS_FROM_THIS_YEAR = -1 THEN 'Previous Year'
                WHEN CURRENT_Etc_UTC_YEARS_FROM_THIS_YEAR = 1 THEN 'Next Year'
                WHEN CURRENT_Etc_UTC_YEARS_FROM_THIS_YEAR < 0 THEN ABS(CURRENT_Etc_UTC_YEARS_FROM_THIS_YEAR) || ' Years Ago'
                ELSE CURRENT_Etc_UTC_YEARS_FROM_THIS_YEAR || ' Years From Now'
            END AS RELATIVE_YEAR_DESCRIPTION`;

        // ----- View Creation -----
        currentStep = `Constructing CREATE VIEW statement for ${finalViewName}`;

        // Build comment string based on included time zones
        const tzList = TIME_ZONES.join(', ');
        const viewComment = `Dynamic time periods view based on ${grainUpper} grain, supporting time zones: ${tzList}. Generated with a relative periods count of ${RELATIVE_PERIODS_COUNT}. The flags in this view automatically update based on the current date in each time zone, making it ideal for global date-relative filtering in reports and dashboards.`;

        createViewSql = `
            CREATE OR REPLACE VIEW ${finalViewName}
            COMMENT = '${viewComment.replace(/'/g, "''")}'
            AS
            SELECT
                c.*,
                ${tzCaseStatements.join(',\n')},
                ${periodDescriptions}
            FROM ${baseViewName} c
        `;

        currentStep = `Executing CREATE VIEW statement for ${finalViewName}`;
        snowflake.createStatement({ sqlText: createViewSql }).execute();

        return `Multi-Timezone Dynamic Periods view '${finalViewName}' generated successfully based on '${baseViewName}', supporting time zones: ${tzList}. This view provides automatically updating time-relative flags for each supported time zone. Access the view with SELECT * FROM ${finalViewName}.`;

    } catch (err) {
        return `ERROR during Multi-Timezone Dynamic Periods generation (Step: ${currentStep}): ${err.code} - ${err.message}\nAttempted SQL (if applicable):\n${createViewSql}`;
    }
$$;
