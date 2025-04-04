# Snowflake Calendar System - Instructions

## Overview

The Snowflake Calendar System has been modified to accept parameters for warehouse, database, and schema. This allows you to run the script in your specific Snowflake environment without hardcoding these values.

## Files

1. `snowflake_calendar_parameterized.sql` - The main script with parameterization added
2. `run_snowflake_calendar.sql` - A simple script to set parameters and run the main script

## How to Run

### Option 1: Using the Run Script (Recommended)

1. Open `run_snowflake_calendar.sql` in your Snowflake worksheet
2. Update the parameter values:
   ```sql
   SET WAREHOUSE_NAME = 'YOUR_WAREHOUSE_NAME';  -- e.g., 'COMPUTE_WH'
   SET DATABASE_NAME = 'YOUR_DATABASE_NAME';    -- e.g., 'CALENDAR_DB'
   SET SCHEMA_NAME = 'YOUR_SCHEMA_NAME';        -- e.g., 'PUBLIC'
   ```
3. Run the entire script

### Option 2: Direct Parameter Setting

1. Open `snowflake_calendar_parameterized.sql` in your Snowflake worksheet
2. At the beginning of the script, modify these lines with your values:
   ```sql
   SET WAREHOUSE_NAME = 'YOUR_WAREHOUSE_NAME';
   SET DATABASE_NAME = 'YOUR_DATABASE_NAME';
   SET SCHEMA_NAME = 'YOUR_SCHEMA_NAME';
   ```
3. Run the entire script

### Option 3: Using Snowflake Variables UI

If your Snowflake interface supports variable prompting:

1. Open `snowflake_calendar_parameterized.sql` in your Snowflake worksheet
2. When you run the script, you'll be prompted to enter values for:
   - `&WAREHOUSE_NAME`
   - `&DATABASE_NAME`
   - `&SCHEMA_NAME`
3. Enter your values and continue execution

## Verification

After running the script, you can verify the objects were created by running:

```sql
SHOW TABLES IN SCHEMA IDENTIFIER($DATABASE_NAME).IDENTIFIER($SCHEMA_NAME);
SHOW PROCEDURES IN SCHEMA IDENTIFIER($DATABASE_NAME).IDENTIFIER($SCHEMA_NAME);
```

## Troubleshooting

If you encounter errors:

1. Ensure you have ACCOUNTADMIN role or sufficient privileges
2. Verify the warehouse, database, and schema exist
3. Check that the warehouse is running and has sufficient credits
4. Ensure network access is allowed for external API calls

## Next Steps

After successfully running Part 1, proceed to the next parts of the calendar system implementation.