# Snowflake Business Calendar System

A comprehensive, modular business calendar system for Snowflake that supports standard, fiscal, and retail calendars with multi-timezone support and dynamic relative date capabilities.

## Overview

This system provides a complete solution for business date management in Snowflake, enabling:

- Standard Gregorian calendar with comprehensive date attributes
- Fiscal calendar with customizable fiscal year start month
- Retail calendar following industry standard patterns (4-5-4, 4-4-5, 5-4-4)
- Dynamic time-relative calculations that automatically update
- Multi-timezone support for global businesses
- Hybrid approach with materialized tables for performance and views for dynamic calculations

## Architecture

The system follows a modular, layered architecture:

1. **Foundation Layer**
   - Date spine tables (materialized)
   - Core date attributes

2. **Business Calendars Layer**
   - Standard calendar (materialized)
   - Fiscal calendar (materialized)
   - Retail calendar (materialized)
   - Unified calendar (materialized)

3. **Dynamic Layer**
   - Simple dynamic periods (view)
   - Multi-timezone dynamic periods (view)

4. **Metadata Layer**
   - Calendar data dictionary

## Files

- `01_generate_business_calendar.sql` - Master orchestration script
- `02_generate_date_spine.sql` - Creates the date spine tables
- `03_generate_gregorian_calendar.sql` - Creates the standard calendar
- `04_generate_fiscal_calendar.sql` - Creates the fiscal calendar
- `05_generate_retail_calendar.sql` - Creates the retail calendar
- `06_generate_unified_calendar.sql` - Creates the unified calendar
- `07_generate_simple_dynamic_periods.sql` - Creates simple dynamic periods
- `08_generate_dynamic_periods_multi-tz.sql` - Creates multi-timezone dynamic periods
- `09_example_business_calendar_queries.sql` - Example usage queries

## Getting Started

### Prerequisites

- Snowflake account with necessary privileges
- A compute warehouse with sufficient resources

### Installation

1. Run the master orchestration script, adjusting configuration parameters as needed:

```sql
-- Set configuration variables
SET CALENDAR_START_DATE = '2020-01-01'; -- Beginning of date range to generate
SET CALENDAR_END_DATE = '2030-12-31';   -- End of date range to generate
SET TIME_GRAIN = 'DAY';                 -- Base time grain (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR)

-- Business calendar parameters
SET INCLUDE_FISCAL = TRUE;              -- Generate fiscal calendar
SET FISCAL_YEAR_START_MONTH = 7;        -- Month when fiscal year starts (1-12)
SET INCLUDE_RETAIL = TRUE;              -- Generate retail calendar
SET RETAIL_PATTERN = '4-5-4';           -- Retail calendar pattern (4-5-4, 4-4-5, 5-4-4)
SET RETAIL_YEAR_END_MONTH = 1;          -- Month when retail year ends (1-12)

-- Dynamic periods parameters
SET INCLUDE_DYNAMIC_PERIODS = TRUE;     -- Generate dynamic period views
SET RELATIVE_PERIODS_COUNT = 12;        -- Number of historical periods to generate
SET INCLUDE_MULTI_TIMEZONE = FALSE;     -- Support multiple timezones
SET TIMEZONE_LIST = ARRAY_CONSTRUCT('Etc/UTC', 'Europe/London', 'America/New_York'); -- Timezones to support

-- Warehouse to use for calendar generation (adjust as needed)
SET CALENDAR_WAREHOUSE = 'COMPUTE_WH';

-- Execute the orchestration script
CALL GENERATE_BUSINESS_CALENDAR(
    $CALENDAR_START_DATE,
    $CALENDAR_END_DATE,
    $TIME_GRAIN,
    $INCLUDE_FISCAL,
    $FISCAL_YEAR_START_MONTH,
    $INCLUDE_RETAIL,
    $RETAIL_PATTERN,
    $RETAIL_YEAR_END_MONTH,
    $INCLUDE_DYNAMIC_PERIODS,
    $RELATIVE_PERIODS_COUNT,
    $INCLUDE_MULTI_TIMEZONE,
    $TIMEZONE_LIST
);
```

2. Alternatively, run individual scripts in the following order for more control:
   - `02_generate_date_spine.sql`
   - `03_generate_gregorian_calendar.sql`
   - `04_generate_fiscal_calendar.sql` (optional)
   - `05_generate_retail_calendar.sql` (optional)
   - `06_generate_unified_calendar.sql`
   - `07_generate_simple_dynamic_periods.sql` or `08_generate_dynamic_periods_multi-tz.sql`

### Configuration Options

#### Time Grains
- `SECOND` - Second-level granularity
- `MINUTE` - Minute-level granularity
- `HOUR` - Hour-level granularity
- `DAY` - Day-level granularity (recommended for most business uses)
- `MONTH` - Month-level granularity
- `YEAR` - Year-level granularity

#### Fiscal Calendar
- `FISCAL_YEAR_START_MONTH` - Month when fiscal year starts (1-12)
  - Common options: 7 (July, for Jul-Jun fiscal year), 10 (October, for Oct-Sep fiscal year)

#### Retail Calendar
- `RETAIL_PATTERN` - Week distribution pattern
  - `4-5-4` - Each quarter has 13 weeks with months of 4, 5, and 4 weeks (NRF standard)
  - `4-4-5` - Each quarter has 13 weeks with months of 4, 4, and 5 weeks
  - `5-4-4` - Each quarter has 13 weeks with months of 5, 4, and 4 weeks
- `RETAIL_YEAR_END_MONTH` - Month when retail year approximately ends (1-12)
  - Common option: 1 (January, for traditional retail year ending in January)

#### Dynamic Periods
- `RELATIVE_PERIODS_COUNT` - Number of historical periods to generate (12 recommended)
- `TIMEZONE_LIST` - List of timezones to support for multi-timezone option

## Usage Examples

### Basic Period Filtering

```sql
-- Current month data
SELECT * FROM fact_table
JOIN DYNAMIC_PERIODS_SIMPLE_DAY d ON fact_table.date_col = d.DATE_KEY
WHERE d.IS_THIS_MONTH = TRUE;

-- Year-to-date data
SELECT * FROM fact_table
JOIN DYNAMIC_PERIODS_SIMPLE_DAY d ON fact_table.date_col = d.DATE_KEY
WHERE d.IS_YTD = TRUE;
```

### Period Comparisons

```sql
-- Month-over-month comparison
SELECT
    'Current Month' as period,
    SUM(amount) as total
FROM fact_table
JOIN DYNAMIC_PERIODS_SIMPLE_DAY d ON fact_table.date_col = d.DATE_KEY
WHERE d.IS_THIS_MONTH = TRUE
UNION ALL
SELECT
    'Previous Month' as period,
    SUM(amount) as total
FROM fact_table
JOIN DYNAMIC_PERIODS_SIMPLE_DAY d ON fact_table.date_col = d.DATE_KEY
WHERE d.IS_LAST_MONTH = TRUE;
```

### Fiscal Calendar

```sql
-- Fiscal quarter breakdown
SELECT
    c.FISCAL_YEAR,
    c.FISCAL_QUARTER,
    SUM(amount) as total
FROM fact_table f
JOIN UNIFIED_CALENDAR_TABLE_DAY c ON f.date_col = c.DATE_KEY
GROUP BY c.FISCAL_YEAR, c.FISCAL_QUARTER
ORDER BY c.FISCAL_YEAR, c.FISCAL_QUARTER;
```

### More Examples

See `09_example_business_calendar_queries.sql` for more comprehensive examples.

## Maintenance

### Refreshing Materialized Tables

Periodic refreshing of the materialized tables is recommended to ensure the dynamic period flags stay current:

```sql
-- Schedule daily refresh
CREATE OR REPLACE TASK refresh_calendar_tables
WAREHOUSE = YOUR_WAREHOUSE
SCHEDULE = 'USING CRON 0 0 * * * Europe/London'
AS CALL REFRESH_CALENDAR_TABLES();
```

### Extending Date Range

If you need to extend the date range of your calendar system:

```sql
-- Extend date range
CALL GENERATE_DATE_SPINE(
    TO_TIMESTAMP_NTZ('2030-01-01'), 
    TO_TIMESTAMP_NTZ('2040-12-31'), 
    'DAY'
);

-- Regenerate all calendar components
CALL GENERATE_BUSINESS_CALENDAR(
    '2020-01-01',  -- Your original start date
    '2040-12-31',  -- Your new end date
    'DAY',
    TRUE, 7,       -- Fiscal calendar settings
    TRUE, '4-5-4', 1,  -- Retail calendar settings
    TRUE, 12,      -- Dynamic periods settings
    FALSE, ARRAY_CONSTRUCT('Etc/UTC')
);
```

## Performance Considerations

- The hybrid approach (materialized tables + views) optimizes both performance and flexibility
- Tables are clustered by DATE_KEY for efficient querying
- For extremely large fact tables, consider adding DATE_KEY as a clustering key
- For reporting-heavy workloads, create aggregate tables at higher grains (MONTH, QUARTER)

## Best Practices

1. **Choose the right time grain**
   - Use DAY grain for most business applications
   - Use MONTH grain for long-range planning
   - Use HOUR or MINUTE grain only if you have intraday analysis needs

2. **Optimize date filtering**
   - Use the dynamic period flags for relative date filters
   - Join to the appropriate calendar table based on your needs
   - Use the data dictionary to understand available attributes

3. **Maintenance schedule**
   - Refresh materialized tables daily or weekly
   - Extend date range yearly or as needed
   - Monitor query patterns and add clustering keys as needed

4. **Custom extensions**
   - For public holidays, create a separate holiday table and join as needed
   - For working days calculation, extend the calendar with business day counters
   - For custom business periods, create additional views on top of the calendar

## Troubleshooting

| Issue | Possible Solution |
|-------|------------------|
| "View does not exist" error | Ensure you've run the calendar generation scripts in the correct order |
| Slow query performance | Check if you're using the materialized table version (e.g., CALENDAR_TABLE_DAY instead of CALENDAR_DAY) |
| Incorrect dynamic period flags | Verify the materialized tables have been refreshed recently |
| Missing time zones | Regenerate the dynamic periods view with the required time zones |

## Calendar System Components

### Tables (Materialized)

- `DATE_SPINE_DAY` - Base date sequence
- `CALENDAR_TABLE_DAY` - Standard calendar attributes
- `FISCAL_CALENDAR_TABLE_DAY` - Fiscal calendar attributes
- `RETAIL_CALENDAR_TABLE_454_DAY` - Retail calendar attributes (4-5-4 pattern)
- `UNIFIED_CALENDAR_TABLE_DAY` - Combined calendar attributes

### Views (Dynamic)

- `CALENDAR_DAY` - View of standard calendar
- `FISCAL_CALENDAR_DAY` - View of fiscal calendar
- `RETAIL_CALENDAR_454_DAY` - View of retail calendar
- `UNIFIED_CALENDAR_DAY` - View of unified calendar
- `DYNAMIC_PERIODS_SIMPLE_DAY` - Simple dynamic periods
- `DYNAMIC_PERIODS_DAY` - Multi-timezone dynamic periods
- `CALENDAR_DATA_DICTIONARY` - Documentation of all calendar objects

## Column Reference

The calendar system includes hundreds of columns across all components. Key groups include:

- **Date keys and identifiers**: DATE_TIME, DATE_KEY
- **Standard calendar**: YEAR, QUARTER, MONTH, WEEK_OF_YEAR, DAY_OF_WEEK
- **Fiscal calendar**: FISCAL_YEAR, FISCAL_QUARTER, FISCAL_MONTH_OF_YEAR
- **Retail calendar**: RETAIL_YEAR, RETAIL_WEEK_OF_YEAR, RETAIL_MONTH_OF_YEAR
- **Period flags**: IS_TODAY, IS_THIS_MONTH, IS_LAST_QUARTER, etc.
- **Time-relative calculations**: DAYS_FROM_TODAY, MONTHS_FROM_THIS_MONTH

For a complete list of columns and descriptions, query the data dictionary:

```sql
SELECT * FROM CALENDAR_DATA_DICTIONARY
ORDER BY table_name, column_name;
```

## Contributing

Enhancements to this calendar system are welcome. Some potential areas for improvement:

- Additional calendar types (ISO, 445, 13-period)
- Support for custom holiday calendars
- Advanced business day calculations
- Enhanced visualization/reporting examples

## License

This business calendar system is released under the MIT License.

## Acknowledgments

- Special thanks to the Snowflake community for best practices
- Calendar design patterns inspired by various data warehousing methodologies
- Retail calendar patterns based on National Retail Federation (NRF) standards
