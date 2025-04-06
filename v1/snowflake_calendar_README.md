# Enhanced Snowflake Calendar System for Australia

This project implements a comprehensive calendar system in Snowflake, tailored for Australian business use cases. It includes support for multiple calendar types, timezone handling, and helper functions for business day calculations.

## Features

- **Public Holidays**: Automatically loads Australian public holidays from data.gov.au
- **Multiple Time Grains**: Supports seconds, minutes, hours, days, weeks, months, and years
- **Timezone Support**: Full timezone support with daylight saving handling
- **Calendar Types**:
  - Gregorian Calendar
  - Fiscal Calendar (configurable start date)
  - Retail Calendar (supports 4-4-5, 4-5-4, and 5-4-4 patterns)
- **Helper Functions**: Business day calculations, period comparisons, etc.
- **Comprehensive Error Handling**: Detailed logging and error tracking

## Installation

The calendar system is implemented as a series of SQL scripts that should be executed in the following order:

1. `snowflake_calendar_part1.sql` - Setup, error logging, and public holiday loading
2. `snowflake_calendar_part2.sql` - Date spine with multiple time grains and timezone support
3. `snowflake_calendar_part3_fiscal.sql` - Enhanced fiscal calendar
4. `snowflake_calendar_part3_retail.sql` - Retail calendar with multiple patterns
5. `snowflake_calendar_part4_helpers.sql` - Helper functions for business day calculations
6. `snowflake_calendar_part5_unified.sql` - Unified procedure that brings everything together

## Usage

### Building the Calendar System

The calendar system can be built using the `BUILD_CALENDAR_SYSTEM` procedure, which takes the following parameters:

```sql
CALL BUILD_CALENDAR_SYSTEM(
    'MY_DATABASE',                          -- Database name
    'MY_SCHEMA',                            -- Schema name
    '2015-01-01',                           -- Start date (optional)
    '2035-12-31',                           -- End date (optional)
    'Australia/Sydney',                     -- Timezone (optional)
    7,                                      -- Fiscal year start month (optional)
    1,                                      -- Fiscal year start day (optional)
    '445',                                  -- Retail pattern (optional)
    7,                                      -- Retail start month (optional)
    1,                                      -- Retail week start day (optional)
    ARRAY_CONSTRUCT('DAY', 'MONTH', 'YEAR') -- Time grains (optional)
);
```

This will create all the necessary tables and views in the specified database and schema.

### Using the Calendar

Once built, the calendar system provides several tables and views:

- `CALENDAR_SPINE_<GRAIN>` - Date spine tables for each time grain
- `FISCAL_CALENDAR` - Fiscal calendar table
- `RETAIL_CALENDAR_<PATTERN>` - Retail calendar tables for each pattern
- `UNIFIED_CALENDAR` - View that joins all calendars together

Example query using the unified calendar:

```sql
SELECT
    date,
    is_holiday,
    is_trading_day,
    fiscal_year,
    fiscal_quarter_num,
    retail_year,
    retail_month_num
FROM MY_DATABASE.MY_SCHEMA.UNIFIED_CALENDAR
WHERE date BETWEEN '2023-01-01' AND '2023-12-31'
ORDER BY date;
```

### Helper Functions

The calendar system provides several helper functions for business day calculations:

#### Add Business Days

```sql
SELECT ADD_BUSINESS_DAYS('2023-01-01', 5);
-- Returns the date 5 business days after January 1, 2023
```

#### Subtract Business Days

```sql
SELECT SUBTRACT_BUSINESS_DAYS('2023-01-10', 3);
-- Returns the date 3 business days before January 10, 2023
```

#### Count Business Days

```sql
SELECT COUNT_BUSINESS_DAYS('2023-01-01', '2023-01-31');
-- Returns the number of business days in January 2023
```

#### Next/Previous Business Day

```sql
SELECT NEXT_BUSINESS_DAY('2023-01-01');
-- Returns the next business day after January 1, 2023

SELECT PREVIOUS_BUSINESS_DAY('2023-01-10');
-- Returns the previous business day before January 10, 2023
```

#### Same Day in Previous Period

```sql
SELECT SAME_DAY_PREVIOUS_PERIOD('2023-03-15', 'MONTH', 1);
-- Returns February 15, 2023

SELECT SAME_DAY_PREVIOUS_PERIOD('2023-03-15', 'QUARTER', 1);
-- Returns December 15, 2022

SELECT SAME_DAY_PREVIOUS_PERIOD('2023-03-15', 'YEAR', 1);
-- Returns March 15, 2022
```

#### Same Business Day in Previous Period

```sql
SELECT SAME_BUSINESS_DAY_PREVIOUS_PERIOD('2023-03-15', 'MONTH', 1);
-- Returns the same business day in the previous month
```

#### Same Day in Previous Fiscal/Retail Period

```sql
SELECT SAME_DAY_PREVIOUS_FISCAL_PERIOD('2023-03-15', 'MONTH', 1);
-- Returns the same day in the previous fiscal month

SELECT SAME_DAY_PREVIOUS_RETAIL_PERIOD('2023-03-15', 'MONTH', 1);
-- Returns the same day in the previous retail month
```

## Customization

The calendar system is highly customizable:

- **Fiscal Calendar**: Adjust the fiscal year start month and day
- **Retail Calendar**: Choose between 4-4-5, 4-5-4, and 5-4-4 patterns
- **Time Grains**: Select which time grains to generate
- **Date Range**: Specify the start and end dates for the calendar

## Error Handling

All errors are logged to the `CALENDAR_ERROR_LOG` table, which includes:

- Timestamp
- Procedure name
- Error message
- Error state
- Error context
- Stack trace

This makes it easy to troubleshoot any issues that may arise during calendar generation.

## Maintenance

The calendar system should be refreshed periodically to:

1. Load new public holidays as they become available
2. Extend the date range as needed
3. Update any configuration changes

This can be done by simply calling the `BUILD_CALENDAR_SYSTEM` procedure again with the desired parameters.