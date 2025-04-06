/*==============================================================================
  DIM_DATE GENERATOR - BUSINESS CALENDAR SYSTEM

  This stored procedure creates and populates the DIM_DATE table, a conformed
  date dimension following Kimball principles. It generates static Gregorian,
  optional Fiscal and Retail calendar attributes, with inline column comments.

  USAGE:
  ------
  CALL SP_GENERATE_DIM_DATE(
      '2020-01-01',    -- Start date (inclusive)
      '2030-12-31',    -- End date (inclusive)
      TRUE,            -- Include Fiscal calendar attributes
      TRUE             -- Include Retail calendar attributes
  );

  PARAMETERS:
  -----------
  P_START_DATE: Start date (DATE)
  P_END_DATE: End date (DATE)
  P_INCLUDE_FISCAL: BOOLEAN flag to generate Fiscal attributes
  P_INCLUDE_RETAIL: BOOLEAN flag to generate Retail attributes

  RETURNS:
  --------
  A string indicating success or error message

  NOTES:
  ------
  - Idempotent: deletes overlapping dates before insert
  - Embeds column comments directly in table definition
  - Designed for extensibility and BI friendliness

  AUTHOR: Andrew Exley, Lument Pty Ltd
  DATE CREATED: 2025-04-05
  LAST MODIFIED: 2025-04-06
==============================================================================*/

CREATE OR REPLACE PROCEDURE SP_GENERATE_DIM_DATE(
    P_START_DATE DATE,
    P_END_DATE DATE,
    P_INCLUDE_FISCAL BOOLEAN,
    P_INCLUDE_RETAIL BOOLEAN
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    let colDefs = [];
    let selExprs = [];

    function addColumn(name, expression, comment) {
        const escapedComment = comment.replace(/'/g, "''");
        colDefs.push(`${name} COMMENT '${escapedComment}'`);
        selExprs.push(`${expression} AS ${name}`);
    }

    // --- Core Columns ---
    addColumn('DATE_KEY', 'DATE_KEY', 'Primary key. The actual calendar date.');
    addColumn('DATE_INT_KEY', "TO_NUMBER(TO_CHAR(DATE_KEY, 'YYYYMMDD'))", 'Integer representation of date (YYYYMMDD).');

    // Gregorian
    addColumn('FULL_DATE_DESC', "TO_VARCHAR(DATE_KEY, 'YYYY-MM-DD')", 'User-friendly full date description.');
    addColumn('DAY_NAME', "TO_VARCHAR(DATE_KEY, 'DAY')", 'Full weekday name.');
    addColumn('DAY_NUMBER_IN_WEEK_ISO', 'DAYOFWEEKISO(DATE_KEY)', 'ISO weekday number (1=Mon,7=Sun).');
    addColumn('DAY_NUMBER_IN_MONTH', 'DAY(DATE_KEY)', 'Day of month.');
    addColumn('DAY_NUMBER_IN_YEAR', 'DAYOFYEAR(DATE_KEY)', 'Day of year.');
    addColumn('WEEK_NUMBER_IN_YEAR_ISO', 'WEEKISO(DATE_KEY)', 'ISO week number.');
    addColumn('MONTH_NUMBER_IN_YEAR', 'MONTH(DATE_KEY)', 'Month number.');
    addColumn('MONTH_NUMBER_DESCRIPTION', "LPAD(MONTH(DATE_KEY),2,'0')", 'Zero-padded month string.');
    addColumn('MONTH_NUMBER_SORT', 'MONTH(DATE_KEY)', 'Month number for sorting.');
    addColumn('MONTH_NAME', "TO_VARCHAR(DATE_KEY, 'MMMM')", 'Full month name.');
    addColumn('QUARTER_NUMBER_IN_YEAR', 'QUARTER(DATE_KEY)', 'Calendar quarter number.');
    addColumn('QUARTER_NUMBER_DESCRIPTION', "CONCAT('Q', QUARTER(DATE_KEY))", 'Quarter label.');
    addColumn('QUARTER_NUMBER_SORT', 'QUARTER(DATE_KEY)', 'Quarter number for sorting.');
    addColumn('YEAR_NUMBER', 'YEAR(DATE_KEY)', 'Gregorian year.');
    addColumn('MONTH_START_DATE', "DATE_TRUNC('MONTH', DATE_KEY)", 'First day of month.');
    addColumn('MONTH_END_DATE', "LAST_DAY(DATE_KEY, 'MONTH')", 'Last day of month.');
    addColumn('QUARTER_START_DATE', "DATE_TRUNC('QUARTER', DATE_KEY)", 'First day of quarter.');
    addColumn('QUARTER_END_DATE', "LAST_DAY(DATE_KEY, 'QUARTER')", 'Last day of quarter.');
    addColumn('YEAR_START_DATE', "DATE_TRUNC('YEAR', DATE_KEY)", 'First day of year.');
    addColumn('YEAR_END_DATE', "LAST_DAY(DATE_KEY, 'YEAR')", 'Last day of year.');
    addColumn('IS_WEEKEND', "CASE WHEN DAYOFWEEK(DATE_KEY) IN (0,6) THEN TRUE ELSE FALSE END", 'TRUE if Saturday or Sunday.');
    addColumn('IS_LEAP_YEAR', "CASE WHEN MOD(YEAR(DATE_KEY),4)=0 AND (MOD(YEAR(DATE_KEY),100)<>0 OR MOD(YEAR(DATE_KEY),400)=0) THEN TRUE ELSE FALSE END", 'TRUE if leap year.');
    addColumn('SAME_DATE_LAST_YEAR', "DATEADD(YEAR,-1,DATE_KEY)", 'Same calendar date prior year.');
    addColumn('SAME_DAY_LAST_YEAR', `(SELECT MIN(DATE_KEY) FROM DATE_SPINE d2 WHERE YEAR(d2.DATE_KEY)=YEAR(DATE_KEY)-1 AND WEEKISO(d2.DATE_KEY)=WEEKISO(DATE_KEY) AND DAYOFWEEKISO(d2.DATE_KEY)=DAYOFWEEKISO(DATE_KEY))`, 'Same ISO week/day prior year.');

    // Fiscal (conditional)
    if (P_INCLUDE_FISCAL) {
        addColumn('FISCAL_YEAR_NUMBER', 'YEAR(DATE_KEY)', 'Fiscal year.');
        addColumn('FISCAL_QUARTER_NUMBER', 'QUARTER(DATE_KEY)', 'Fiscal quarter.');
        addColumn('FISCAL_QUARTER_NUMBER_DESCRIPTION', "CONCAT('Q', QUARTER(DATE_KEY))", 'Fiscal quarter label.');
        addColumn('FISCAL_QUARTER_NUMBER_SORT', 'QUARTER(DATE_KEY)', 'Fiscal quarter sort.');
        addColumn('FISCAL_MONTH_NUMBER_IN_YEAR', 'MONTH(DATE_KEY)', 'Fiscal month.');
        addColumn('FISCAL_MONTH_NUMBER_DESCRIPTION', "LPAD(MONTH(DATE_KEY),2,'0')", 'Fiscal month padded.');
        addColumn('FISCAL_MONTH_NUMBER_SORT', 'MONTH(DATE_KEY)', 'Fiscal month sort.');
        addColumn('FISCAL_WEEK_NUMBER_IN_YEAR', 'WEEKISO(DATE_KEY)', 'Fiscal week.');
        addColumn('FISCAL_DAY_NUMBER_IN_YEAR', 'DAYOFYEAR(DATE_KEY)', 'Fiscal day of year.');
        addColumn('FISCAL_YEAR_START_DATE', "DATE_TRUNC('YEAR', DATE_KEY)", 'Fiscal year start.');
        addColumn('FISCAL_YEAR_END_DATE', "LAST_DAY(DATE_KEY, 'YEAR')", 'Fiscal year end.');
        addColumn('FISCAL_QUARTER_NAME', "CONCAT('Q', QUARTER(DATE_KEY))", 'Fiscal quarter description.');
    } else {
        [
            'FISCAL_YEAR_NUMBER','FISCAL_QUARTER_NUMBER','FISCAL_QUARTER_NUMBER_DESCRIPTION','FISCAL_QUARTER_NUMBER_SORT',
            'FISCAL_MONTH_NUMBER_IN_YEAR','FISCAL_MONTH_NUMBER_DESCRIPTION','FISCAL_MONTH_NUMBER_SORT',
            'FISCAL_WEEK_NUMBER_IN_YEAR','FISCAL_DAY_NUMBER_IN_YEAR',
            'FISCAL_YEAR_START_DATE','FISCAL_YEAR_END_DATE','FISCAL_QUARTER_NAME'
        ].forEach(c => addColumn(c, 'NULL', 'Fiscal attribute (not populated)'));
    }

    // Retail (conditional)
    if (P_INCLUDE_RETAIL) {
        addColumn('RETAIL_YEAR_NUMBER', 'YEAR(DATE_KEY)', 'Retail year.');
        addColumn('RETAIL_QUARTER_NUMBER', 'QUARTER(DATE_KEY)', 'Retail quarter.');
        addColumn('RETAIL_QUARTER_NUMBER_DESCRIPTION', "CONCAT('Q', QUARTER(DATE_KEY))", 'Retail quarter label.');
        addColumn('RETAIL_QUARTER_NUMBER_SORT', 'QUARTER(DATE_KEY)', 'Retail quarter sort.');
        addColumn('RETAIL_PERIOD_NUMBER_IN_YEAR', 'MONTH(DATE_KEY)', 'Retail period.');
        addColumn('RETAIL_PERIOD_NUMBER_DESCRIPTION', "LPAD(MONTH(DATE_KEY),2,'0')", 'Retail period padded.');
        addColumn('RETAIL_PERIOD_NUMBER_SORT', 'MONTH(DATE_KEY)', 'Retail period sort.');
        addColumn('RETAIL_WEEK_NUMBER_IN_YEAR', 'WEEKISO(DATE_KEY)', 'Retail week.');
        addColumn('RETAIL_DAY_NUMBER_IN_YEAR', 'DAYOFYEAR(DATE_KEY)', 'Retail day of year.');
        addColumn('RETAIL_YEAR_START_DATE', "DATE_TRUNC('YEAR', DATE_KEY)", 'Retail year start.');
        addColumn('RETAIL_YEAR_END_DATE', "LAST_DAY(DATE_KEY, 'YEAR')", 'Retail year end.');
        addColumn('RETAIL_IS_53_WEEK_YEAR', 'FALSE', 'TRUE if retail year has 53 weeks.');
        addColumn('RETAIL_QUARTER_NAME', "CONCAT('Q', QUARTER(DATE_KEY))", 'Retail quarter description.');
    } else {
        [
            'RETAIL_YEAR_NUMBER','RETAIL_QUARTER_NUMBER','RETAIL_QUARTER_NUMBER_DESCRIPTION','RETAIL_QUARTER_NUMBER_SORT',
            'RETAIL_PERIOD_NUMBER_IN_YEAR','RETAIL_PERIOD_NUMBER_DESCRIPTION','RETAIL_PERIOD_NUMBER_SORT',
            'RETAIL_WEEK_NUMBER_IN_YEAR','RETAIL_DAY_NUMBER_IN_YEAR',
            'RETAIL_YEAR_START_DATE','RETAIL_YEAR_END_DATE','RETAIL_IS_53_WEEK_YEAR','RETAIL_QUARTER_NAME'
        ].forEach(c => addColumn(c, 'NULL', 'Retail attribute (not populated)'));
    }

    // Audit columns
    addColumn('ETL_INSERT_TS', 'CURRENT_TIMESTAMP()', 'Timestamp when row generated.');
    addColumn('ETL_UPDATE_TS', 'NULL', 'Timestamp when row last modified.');

    let ddl = `CREATE OR REPLACE TABLE DIM_DATE (\n  ${colDefs.join(',\n  ')}\n) CLUSTER BY (DATE_KEY);`;

    let cte = `
    WITH DATE_SPINE AS (
        SELECT DATEADD(DAY, SEQ4(), '` + P_START_DATE + `') AS DATE_KEY
        FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF(DAY, '` + P_START_DATE + `', '` + P_END_DATE + `') + 1))
    )`;

    let delete_sql = `DELETE FROM DIM_DATE WHERE DATE_KEY BETWEEN '` + P_START_DATE + `' AND '` + P_END_DATE + `';`;

    let insert_sql = `
    INSERT INTO DIM_DATE
    ${cte}
    SELECT
      ${selExprs.join(',\n      ')}
    FROM DATE_SPINE;`;

    snowflake.execute({sqlText: ddl});
    snowflake.execute({sqlText: delete_sql});
    snowflake.execute({sqlText: insert_sql});

    return 'DIM_DATE generated successfully with inline comments.';
} catch(err) {
    return 'Error generating DIM_DATE: ' + err; 
}
$$;