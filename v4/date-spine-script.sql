/*==============================================================================
  DATE SPINE GENERATOR - FOUNDATION FOR BUSINESS CALENDAR SYSTEM
  
  This script creates the fundamental date spine tables that serve as the foundation
  for all calendar views and tables in the system. It generates a sequence of dates
  or timestamps at the specified grain between the start and end dates.
  
  The date spine table contains only a single DATE_TIME column with appropriate
  grain (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR).
  
  USAGE:
  ------
  CALL GENERATE_DATE_SPINE(
      TO_TIMESTAMP_NTZ('2020-01-01'),  -- Start date
      TO_TIMESTAMP_NTZ('2030-12-31'),  -- End date
      'DAY'                            -- Time grain
  );
  
  PARAMETERS:
  -----------
  START_DATETIME: The beginning timestamp for the date spine
  END_DATETIME: The ending timestamp for the date spine
  TIME_GRAIN: The granularity level (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR)
  
  RETURNS:
  --------
  A string indicating success or failure with details
  
  NOTES:
  ------
  - Choose a date range wide enough for your business needs
  - DAY grain is recommended for most business applications
  - For large ranges with fine grains (SECOND), generation time may be significant
  
  AUTHOR: [Your Name]
  DATE CREATED: 2024-04-04
  LAST MODIFIED: 2024-04-04
==============================================================================*/

-- Stored procedure to generate the date spine
CREATE OR REPLACE PROCEDURE GENERATE_DATE_SPINE(
    START_DATETIME TIMESTAMP_NTZ,
    END_DATETIME TIMESTAMP_NTZ,
    TIME_GRAIN VARCHAR(10)
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  // Validate inputs
  if (!START_DATETIME || !END_DATETIME || !TIME_GRAIN) {
    return "ERROR: Start datetime, end datetime, and time grain must be provided.";
  }

  // Validate Time Grain
  const validTimeGrains = ['SECOND', 'MINUTE', 'HOUR', 'DAY', 'MONTH', 'YEAR'];
  const grainUpper = TIME_GRAIN.toUpperCase();
  if (validTimeGrains.indexOf(grainUpper) === -1) {
    return "ERROR: Invalid time grain. Must be one of: SECOND, MINUTE, HOUR, DAY, MONTH, YEAR";
  }

  // Determine the time difference function based on the time grain
  let timeDiffFunction;
  switch (grainUpper) {
    case 'SECOND':
      timeDiffFunction = 'SECOND';
      break;
    case 'MINUTE':
      timeDiffFunction = 'MINUTE';
      break;
    case 'HOUR':
      timeDiffFunction = 'HOUR';
      break;
    case 'DAY':
      timeDiffFunction = 'DAY';
      break;
    case 'MONTH':
      timeDiffFunction = 'MONTH';
      break;
    case 'YEAR':
      timeDiffFunction = 'YEAR';
      break;
    default:
      return "ERROR: Invalid time grain (internal error)"; // Should never happen, but good to have
  }

  // Helper function to convert Snowflake TIMESTAMP_NTZ to ISO string
  function toIsoString(timestamp) {
    const year = timestamp.getFullYear();
    const month = String(timestamp.getMonth() + 1).padStart(2, '0'); // Months are 0-indexed
    const day = String(timestamp.getDate()).padStart(2, '0');
    const hour = String(timestamp.getHours()).padStart(2, '0');
    const minute = String(timestamp.getMinutes()).padStart(2, '0');
    const second = String(timestamp.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
  }

  // Construct the table name
  const tableName = "DATE_SPINE_" + grainUpper;

  try {
    // Convert TIMESTAMP_NTZ to ISO string
    const startDateTimeString = String(toIsoString(START_DATETIME));
    const endDateTimeString = String(toIsoString(END_DATETIME));

    // Calculate Row Count in Javascript
    let rowCountStmt = snowflake.createStatement({
      sqlText: `SELECT DATEDIFF(${timeDiffFunction}, TO_TIMESTAMP_NTZ('${startDateTimeString}'), TO_TIMESTAMP_NTZ('${endDateTimeString}')) + 1`
    });

    let rowCountResult = rowCountStmt.execute();
    rowCountResult.next(); // Advance to the first (and only) row
    let rowCount = rowCountResult.getColumnValue(1); // Get the row count as a number

    // Construct the CREATE TABLE statement with comments
    const createTableSql = `
      CREATE OR REPLACE TABLE ${tableName} (
          DATE_TIME TIMESTAMP_NTZ COMMENT 'Primary timestamp at ${grainUpper} grain'
      ) COMMENT = 'Date spine table at ${grainUpper} grain from ${startDateTimeString} to ${endDateTimeString}, containing ${rowCount} rows.'
    `;

    // Execute the CREATE TABLE statement
    let createTableStmt = snowflake.createStatement({
      sqlText: createTableSql
    });
    createTableStmt.execute();

    // Construct the INSERT statement
    const insertSql = `
      INSERT INTO ${tableName} (DATE_TIME)
      SELECT
          DATEADD(${timeDiffFunction}, SEQ4(), TO_TIMESTAMP_NTZ('${startDateTimeString}')) AS DATE_TIME
      FROM TABLE(GENERATOR(ROWCOUNT => ${rowCount}));
    `;

    // Execute the INSERT statement
    let insertStmt = snowflake.createStatement({
      sqlText: insertSql
    });
    insertStmt.execute();
    
    // Add clustering key for better performance
    const alterSql = `
      ALTER TABLE ${tableName} CLUSTER BY (DATE_TIME);
    `;
    
    // Execute the ALTER TABLE statement
    let alterStmt = snowflake.createStatement({
      sqlText: alterSql
    });
    alterStmt.execute();

    return `Date spine table '${tableName}' generated successfully with ${rowCount} rows, range: ${startDateTimeString} to ${endDateTimeString}, at ${grainUpper} grain.`;

  } catch (err) {
    return `ERROR creating date spine table: ${err.code} - ${err.message}`;
  }
$$;
