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
  if (validTimeGrains.indexOf(TIME_GRAIN.toUpperCase()) === -1) {
    return "ERROR: Invalid time grain.  Must be one of: SECOND, MINUTE, HOUR, DAY, MONTH, YEAR";
  }

  // Determine the time difference function based on the time grain
  let timeDiffFunction;
  switch (TIME_GRAIN.toUpperCase()) {
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
      return "ERROR: Invalid time grain (internal error)"; //Should never happen, but good to have
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
  const tableName = "DATE_SPINE_" + TIME_GRAIN.toUpperCase();

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

    // Construct the CREATE TABLE statement
    const createTableSql = `
      CREATE OR REPLACE TABLE ${tableName} (
          DATE_TIME TIMESTAMP_NTZ
      )
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
          DATEADD(${timeDiffFunction}, SEQ4(), TO_TIMESTAMP_NTZ('${startDateTimeString}')) -- Datetime
      FROM TABLE(GENERATOR(ROWCOUNT => ${rowCount}));
    `;

    // Execute the INSERT statement
    let insertStmt = snowflake.createStatement({
      sqlText: insertSql
    });
    insertStmt.execute();

    return "Date spine generated successfully for table: " + tableName + ", range: " + START_DATETIME + " to " + END_DATETIME + ", and time grain: " + TIME_GRAIN.toUpperCase();

  } catch (err) {
    return "ERROR: " + err.code + " - " + err.message;
  }
$$
;
