/*==============================================================================
  UNIFIED CALENDAR GENERATOR
  
  This script creates a unified calendar view that combines elements from the
  standard Gregorian calendar, fiscal calendar, and retail calendar into a single
  comprehensive view for business reporting and analysis.
  
  The resulting unified calendar includes:
  - All standard date attributes (year, quarter, month, day)
  - Fiscal calendar attributes (if requested)
  - Retail calendar attributes (if requested)
  
  USAGE:
  ------
  CALL GENERATE_UNIFIED_CALENDAR(
      'DAY',            -- Time grain
      TRUE,             -- Include fiscal calendar
      7,                -- Fiscal year start month (1-12)
      TRUE,             -- Include retail calendar
      '4-5-4',          -- Retail pattern
      1                 -- Retail year end month (1-12)
  );
  
  PARAMETERS:
  -----------
  TIME_GRAIN: The granularity level (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR)
  INCLUDE_FISCAL: Whether to include fiscal calendar attributes
  FISCAL_YEAR_START_MONTH: Month when fiscal year starts (1-12)
  INCLUDE_RETAIL: Whether to include retail calendar attributes
  RETAIL_PATTERN: Retail calendar pattern (4-5-4, 4-4-5, 5-4-4)
  RETAIL_YEAR_END_MONTH: Month when retail year approximately ends (1-12)
  
  RETURNS:
  --------
  A string indicating success or failure with details
  
  PREREQUISITES:
  -------------
  Requires CALENDAR_{TIME_GRAIN} view to exist
  If INCLUDE_FISCAL=TRUE, requires FISCAL_CALENDAR_{TIME_GRAIN} view to exist
  If INCLUDE_RETAIL=TRUE, requires RETAIL_CALENDAR_{PATTERN}_{TIME_GRAIN} view to exist
  
  AUTHOR: [Your Name]
  DATE CREATED: 2024-04-04
  LAST MODIFIED: 2024-04-04
==============================================================================*/

-- Stored procedure to generate a Unified Calendar view with selectable components
CREATE OR REPLACE PROCEDURE GENERATE_UNIFIED_CALENDAR(
    TIME_GRAIN VARCHAR(10),
    INCLUDE_FISCAL BOOLEAN DEFAULT TRUE,
    FISCAL_YEAR_START_MONTH_ARG FLOAT DEFAULT 7.0, -- Default needed if param is optional based on INCLUDE_FISCAL
    INCLUDE_RETAIL BOOLEAN DEFAULT TRUE,
    RETAIL_PATTERN VARCHAR(5) DEFAULT '4-5-4', -- Default needed if param is optional based on INCLUDE_RETAIL
    RETAIL_YEAR_END_MONTH_ARG FLOAT DEFAULT 1.0 -- Default needed if param is optional based on INCLUDE_RETAIL
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$
    // ----- Input Validation -----
    const validTimeGrains = ['SECOND', 'MINUTE', 'HOUR', 'DAY', 'MONTH', 'YEAR'];
    const grainUpper = TIME_GRAIN.toUpperCase();
    if (validTimeGrains.indexOf(grainUpper) === -1) {
        return `ERROR: Unified Calendar generation currently supports grains: ${validTimeGrains.join(', ')}. Grain provided: ${TIME_GRAIN}`;
    }

    let fiscalStartMonthInt = null;
    let patternUpper = null;
    let retailEndMonthInt = null;
    let viewNameSuffix = ''; // For dynamic view naming
    let viewCommentParts = [`Unified calendar view for ${grainUpper} grain, joining Standard`];

    // Validate Fiscal params only if requested
    if (INCLUDE_FISCAL) {
        if (FISCAL_YEAR_START_MONTH_ARG === undefined || FISCAL_YEAR_START_MONTH_ARG === null || isNaN(FISCAL_YEAR_START_MONTH_ARG)) {
            return "ERROR: FISCAL_YEAR_START_MONTH_ARG must be provided when INCLUDE_FISCAL is TRUE.";
        }
        fiscalStartMonthInt = Math.trunc(FISCAL_YEAR_START_MONTH_ARG);
        if (fiscalStartMonthInt < 1 || fiscalStartMonthInt > 12) {
            return `ERROR: FISCAL_YEAR_START_MONTH must be between 1 and 12 (derived value: ${fiscalStartMonthInt}).`;
        }
        viewNameSuffix += `_FY${fiscalStartMonthInt}`;
        viewCommentParts.push(`Fiscal (FY starts ${fiscalStartMonthInt}/1)`);
    }

    // Validate Retail params only if requested
    if (INCLUDE_RETAIL) {
        const validPatterns = ['4-5-4', '4-4-5', '5-4-4'];
        patternUpper = RETAIL_PATTERN ? RETAIL_PATTERN.toUpperCase() : null;
        if (!patternUpper || validPatterns.indexOf(patternUpper) === -1) {
            return `ERROR: Invalid or missing RETAIL_PATTERN when INCLUDE_RETAIL is TRUE. Must be one of: ${validPatterns.join(', ')}.`;
        }

        if (RETAIL_YEAR_END_MONTH_ARG === undefined || RETAIL_YEAR_END_MONTH_ARG === null || isNaN(RETAIL_YEAR_END_MONTH_ARG)) {
            return "ERROR: RETAIL_YEAR_END_MONTH_ARG must be provided when INCLUDE_RETAIL is TRUE.";
        }
        retailEndMonthInt = Math.trunc(RETAIL_YEAR_END_MONTH_ARG);
        if (retailEndMonthInt < 1 || retailEndMonthInt > 12) {
            return `ERROR: RETAIL_YEAR_END_MONTH must be between 1 and 12 (derived value: ${retailEndMonthInt}).`;
        }
        const patternSuffix = patternUpper.replace(/-/g, '');
        viewNameSuffix += `_R${patternSuffix}_END${retailEndMonthInt}`;
        viewCommentParts.push(`Retail (${patternUpper}, ends month ${retailEndMonthInt})`);
    }

    // ----- Dynamic Name Construction -----
    const calendarViewName = "CALENDAR_" + grainUpper;
    const fiscalViewName = "FISCAL_CALENDAR_" + grainUpper; // Name is consistent per grain
    const retailPatternSuffix = patternUpper ? patternUpper.replace(/-/g, '') : '';
    const retailViewName = `RETAIL_CALENDAR_${retailPatternSuffix}_${grainUpper}`; // Name depends on pattern
    const unifiedViewName = `UNIFIED_CALENDAR_${grainUpper}${viewNameSuffix}`;

    // ----- SQL Construction -----
    let createViewSql = '';
    let selectList = ['c.*']; // Start with all standard columns
    let fromClause = `${calendarViewName} c`;
    let currentStep = 'Starting';

    try {
        // ----- Run Prerequisites Conditionally -----
        currentStep = `Checking prerequisites`;

        // Check if necessary views exist
        let checkViewsStmt = snowflake.createStatement({
            sqlText: `
                SELECT 
                    COUNT(*) AS VIEWS_EXIST
                FROM 
                    INFORMATION_SCHEMA.VIEWS
                WHERE 
                    TABLE_NAME = '${calendarViewName}'
                    ${INCLUDE_FISCAL ? `OR TABLE_NAME = '${fiscalViewName}'` : ''}
                    ${INCLUDE_RETAIL ? `OR TABLE_NAME = '${retailViewName}'` : ''}
            `
        });
        
        let checkResult = checkViewsStmt.execute();
        checkResult.next();
        let viewsExist = checkResult.getColumnValue(1);
        
        let expectedViews = 1 + (INCLUDE_FISCAL ? 1 : 0) + (INCLUDE_RETAIL ? 1 : 0);
        
        if (viewsExist < expectedViews) {
            return `ERROR: Not all required prerequisite views exist. Required: ${calendarViewName}${INCLUDE_FISCAL ? ', ' + fiscalViewName : ''}${INCLUDE_RETAIL ? ', ' + retailViewName : ''}. Please generate these views first.`;
        }

        // Add Fiscal columns and join if requested
        if (INCLUDE_FISCAL) {
            currentStep = `Adding fiscal calendar columns`;
            const fiscalCols = [
                'f.FISCAL_YEAR', 'f.FIRST_DAY_OF_FISCAL_YEAR', 'f.LAST_DAY_OF_FISCAL_YEAR',
                'f.FISCAL_QUARTER', 'f.FISCAL_QUARTER_NAME', 'f.FISCAL_MONTH_OF_YEAR',
                'f.FISCAL_DAY_OF_YEAR', 'f.FISCAL_WEEK_OF_YEAR'
            ];
            let relevantFiscalCols = fiscalCols.slice(0, 6); // Year, Dates, Quarter, Month always needed if Fiscal included (for DAY grain+)
            if (['DAY', 'HOUR', 'MINUTE', 'SECOND'].includes(grainUpper)) {
                relevantFiscalCols = relevantFiscalCols.concat(fiscalCols.slice(6, 8)); // Day, Week
            }
            selectList = selectList.concat(relevantFiscalCols);
            fromClause += `\nLEFT JOIN ${fiscalViewName} f ON c.DATE_TIME = f.DATE_TIME`;
        }

        // Add Retail columns and join if requested
        if (INCLUDE_RETAIL) {
            currentStep = `Adding retail calendar columns`;
            const retailCols = [
                'r.RETAIL_YEAR', 'r.RETAIL_YEAR_START_DATE', 'r.RETAIL_YEAR_END_DATE',
                'r.WEEKS_IN_RETAIL_YEAR', 'r.RETAIL_WEEK_OF_YEAR', 'r.RETAIL_MONTH_OF_YEAR',
                'r.RETAIL_QUARTER', 'r.RETAIL_PATTERN'
            ];
            selectList = selectList.concat(retailCols);
            fromClause += `\nLEFT JOIN ${retailViewName} r ON c.DATE_TIME = r.DATE_TIME`;
        }

        // ----- Unified View Creation -----
        currentStep = `Constructing CREATE VIEW statement for ${unifiedViewName}`;
        
        // Build comment string based on included parts
        let finalViewComment = viewCommentParts[0]; // Start with "Standard"
        if (viewCommentParts.length > 1) {
             finalViewComment += ' and ' + viewCommentParts.slice(1).join(' and ');
        }
        finalViewComment += ' calendars.';

        createViewSql = `
            CREATE OR REPLACE VIEW ${unifiedViewName}
            COMMENT = '${finalViewComment.replace(/'/g, "''")}'
            AS
            SELECT
                ${selectList.join(',\n                ')}
            FROM ${fromClause}
            ORDER BY c.DATE_TIME
        `;

        currentStep = `Executing CREATE VIEW statement for ${unifiedViewName}`;
        snowflake.createStatement({ sqlText: createViewSql }).execute();

        return `Unified Calendar view '${unifiedViewName}' generated successfully combining standard${INCLUDE_FISCAL ? ', fiscal' : ''}${INCLUDE_RETAIL ? ', retail' : ''} calendars. Access the view with SELECT * FROM ${unifiedViewName}.`;

    } catch (err) {
         if (err.message.includes("does not exist or not authorized") && currentStep.startsWith('Constructing')) {
             return `ERROR creating view ${unifiedViewName}: Underlying view potentially missing. Check prerequisite generation. Step: ${currentStep}. Original error: ${err.code} - ${err.message}`;
        }
        return `ERROR during Unified Calendar generation (Step: ${currentStep}): ${err.code} - ${err.message}\nAttempted SQL (if applicable):\n${createViewSql}`;
    }
$;
