# 📅 Australian Snowflake Calendar System

[![Snowflake](https://img.shields.io/badge/Snowflake-0.1.0-29B5E8.svg)](https://www.snowflake.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive calendar system for Snowflake, tailored specifically for Australian business use cases. This system provides multiple calendar types, timezone handling, and helper functions for business day calculations.

## 🌟 Features

- **🇦🇺 Australian Public Holidays**: Automatically loads holidays from data.gov.au with jurisdiction-specific flags
- **⏱️ Multiple Time Grains**: Supports seconds, minutes, hours, days, weeks, months, and years
- **🌐 Timezone Support**: Full timezone support with daylight saving time handling
- **📆 Multiple Calendar Types**:
  - **Gregorian Calendar**: Standard calendar with extensive attributes (implemented in date spine and business calendar base tables)
  - **Fiscal Calendar**: Configurable start date (default: July 1)
  - **Retail Calendar**: Supports 4-4-5, 4-5-4, and 5-4-4 patterns
- **🧮 Helper Functions**: Comprehensive business day calculations and period comparisons
- **🛡️ Error Handling**: Detailed logging and error tracking
- **🔄 Unified Setup**: Single procedure to configure and generate all calendars

## 📋 Table of Contents

- [Installation](#-installation)
- [Usage](#-usage)
  - [Building the Calendar System](#building-the-calendar-system)
  - [Using the Calendar](#using-the-calendar)
  - [Helper Functions](#helper-functions)
- [Calendar Structure](#-calendar-structure)
  - [Date Spine](#date-spine)
  - [Fiscal Calendar](#fiscal-calendar)
  - [Retail Calendar](#retail-calendar)
  - [Unified Calendar](#unified-calendar)
- [Customization](#-customization)
- [Error Handling](#-error-handling)
- [Maintenance](#-maintenance)
- [Contributing](#-contributing)
- [License](#-license)

## 🚀 Installation

The calendar system is implemented as a series of SQL scripts that should be executed in the following order:

1. `snowflake_calendar_part1.sql` - Setup, error logging, and public holiday loading
2. `snowflake_calendar_part2.sql` - Date spine with multiple time grains and timezone support
3. `snowflake_calendar_part3_fiscal.sql` - Enhanced fiscal calendar
4. `snowflake_calendar_part3_retail.sql` - Retail calendar with multiple patterns
5. `snowflake_calendar_part4_helpers.sql` - Helper functions for business day calculations
6. `snowflake_calendar_part5_unified.sql` - Unified procedure that brings everything together
7. `snowflake_calendar_comments_part1-4.sql` - Adds detailed comments to all tables and columns

### Prerequisites

- Snowflake account with ACCOUNTADMIN role or equivalent permissions
- Network access to data.gov.au (for public holiday loading)

### Quick Start

```sql
-- Execute all SQL files in order
-- Then call the unified procedure
CALL BUILD_CALENDAR_SYSTEM(
    'MY_DATABASE',                          -- Database name
    'MY_SCHEMA',                            -- Schema name
    '2015-01-01',                           -- Start date
    '2035-12-31',                           -- End date
    'Australia/Sydney',                     -- Timezone
    7,                                      -- Fiscal year start month
    1,                                      -- Fiscal year start day
    '445',                                  -- Retail pattern
    7,                                      -- Retail start month
    1,                                      -- Retail week start day
    ARRAY_CONSTRUCT('DAY', 'MONTH', 'YEAR') -- Time grains
);
```

## 🔍 Usage

### Building the Calendar System

The calendar system can be built using the `BUILD_CALENDAR_SYSTEM` procedure, which takes the following parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `DATABASE_NAME` | VARCHAR | (required) | Database where calendar tables will be created |
| `SCHEMA_NAME` | VARCHAR | (required) | Schema where calendar tables will be created |
| `START_DATE` | VARCHAR | From config | Start date for the calendar (YYYY-MM-DD) |
| `END_DATE` | VARCHAR | From config | End date for the calendar (YYYY-MM-DD) |
| `TIMEZONE` | VARCHAR | 'Australia/Adelaide' | Timezone for the calendar |
| `FISCAL_YEAR_START_MONTH` | NUMBER | 7 | Month when fiscal year starts (1-12) |
| `FISCAL_YEAR_START_DAY` | NUMBER | 1 | Day when fiscal year starts (1-31) |
| `RETAIL_PATTERN` | VARCHAR | '445' | Retail calendar pattern ('445', '454', '544') |
| `RETAIL_START_MONTH` | NUMBER | 7 | Month when retail year starts (1-12) |
| `RETAIL_WEEK_START_DAY` | NUMBER | 1 | Day when retail week starts (0=Sunday, 1=Monday) |
| `TIME_GRAINS` | ARRAY | ['DAY'] | Array of time grains to generate |

Example:

```sql
CALL BUILD_CALENDAR_SYSTEM(
    'ANALYTICS',
    'DIM',
    '2020-01-01',
    '2030-12-31',
    'Australia/Sydney',
    7,
    1,
    '445',
    7,
    1,
    ARRAY_CONSTRUCT('DAY', 'WEEK', 'MONTH')
);
```

### Using the Calendar

Once built, the calendar system provides several tables and views:

- `CALENDAR_SPINE_<GRAIN>` - Date spine tables for each time grain
- `BUSINESS_CALENDAR_BASE` - Base calendar with Gregorian attributes and holiday information
- `FISCAL_CALENDAR` - Fiscal calendar table
- `RETAIL_CALENDAR_<PATTERN>` - Retail calendar tables for each pattern
- `UNIFIED_CALENDAR` - View that joins all calendars together

#### Example Queries

**Basic date filtering:**

```sql
SELECT
    date,
    day_of_week_num,
    is_holiday,
    is_trading_day,
    holiday_names
FROM UNIFIED_CALENDAR
WHERE date BETWEEN '2023-01-01' AND '2023-01-31'
ORDER BY date;
```

**Fiscal period analysis:**

```sql
SELECT
    fiscal_year,
    fiscal_quarter_num,
    MIN(date) AS start_date,
    MAX(date) AS end_date,
    COUNT(*) AS days,
    SUM(is_trading_day) AS trading_days
FROM UNIFIED_CALENDAR
WHERE fiscal_year = 2023
GROUP BY fiscal_year, fiscal_quarter_num
ORDER BY fiscal_quarter_num;
```

**Retail calendar reporting:**

```sql
SELECT
    retail_year,
    retail_month_num,
    retail_month_name,
    retail_month_start_date,
    retail_month_end_date,
    COUNT(*) AS days,
    SUM(is_trading_day) AS trading_days
FROM UNIFIED_CALENDAR
WHERE retail_year = 2023
GROUP BY 
    retail_year, 
    retail_month_num, 
    retail_month_name, 
    retail_month_start_date, 
    retail_month_end_date
ORDER BY retail_month_num;
```

**Holiday analysis by jurisdiction:**

```sql
SELECT
    date,
    holiday_names,
    CASE WHEN is_holiday_nsw = 1 THEN 'Yes' ELSE 'No' END AS nsw,
    CASE WHEN is_holiday_vic = 1 THEN 'Yes' ELSE 'No' END AS vic,
    CASE WHEN is_holiday_qld = 1 THEN 'Yes' ELSE 'No' END AS qld,
    CASE WHEN is_holiday_sa = 1 THEN 'Yes' ELSE 'No' END AS sa,
    CASE WHEN is_holiday_wa = 1 THEN 'Yes' ELSE 'No' END AS wa,
    CASE WHEN is_holiday_tas = 1 THEN 'Yes' ELSE 'No' END AS tas,
    CASE WHEN is_holiday_act = 1 THEN 'Yes' ELSE 'No' END AS act,
    CASE WHEN is_holiday_nt = 1 THEN 'Yes' ELSE 'No' END AS nt,
    CASE WHEN is_holiday_national = 1 THEN 'Yes' ELSE 'No' END AS national
FROM UNIFIED_CALENDAR
WHERE is_holiday = 1
AND date BETWEEN '2023-01-01' AND '2023-12-31'
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

#### Is Business Day

```sql
SELECT IS_BUSINESS_DAY('2023-01-01');
-- Returns TRUE if January 1, 2023 is a business day, FALSE otherwise
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

## 📊 Calendar Structure

### Date Spine

The date spine is the foundation of the calendar system. It provides basic date attributes for various time grains:

- `CALENDAR_SPINE_SECOND` - Second-level date spine
- `CALENDAR_SPINE_MINUTE` - Minute-level date spine
- `CALENDAR_SPINE_HOUR` - Hour-level date spine
- `CALENDAR_SPINE_DAY` - Day-level date spine
- `CALENDAR_SPINE_WEEK` - Week-level date spine
- `CALENDAR_SPINE_MONTH` - Month-level date spine
- `CALENDAR_SPINE_YEAR` - Year-level date spine

Each date spine table includes appropriate attributes for its grain, such as:

- Date/timestamp
- Year, month, day, hour, minute, second (as applicable)
- Day of week, day of year, week of year
- Quarter
- Timezone information

### Gregorian Calendar

The Gregorian calendar is implemented through two main components:

1. **Date Spine Tables**: The `CALENDAR_SPINE_DAY` table (and other time grain tables) contain all standard Gregorian calendar attributes such as year, month, day, day of week, etc.

2. **Business Calendar Base**: The `BUSINESS_CALENDAR_BASE` table created in the `SP_BUILD_BUSINESS_CALENDAR` procedure contains comprehensive Gregorian calendar attributes, including:
   - Date and date key
   - Year, quarter, month information
   - Week information
   - Day of month, day of week, day of year
   - Month/quarter/year start and end dates
   - Holiday information

Unlike the fiscal and retail calendars, the Gregorian calendar is not implemented as a separate table, but rather as the foundation upon which the other calendars are built.

### Fiscal Calendar

The fiscal calendar provides attributes specific to fiscal periods:

- Fiscal year, quarter, month, week
- Fiscal period start and end dates
- Fiscal period indicators (is_fiscal_year_start, is_fiscal_month_end, etc.)
- Fiscal period keys for joining

The fiscal calendar can be customized by specifying the fiscal year start month and day.

### Retail Calendar

The retail calendar implements the 4-4-5, 4-5-4, or 5-4-4 patterns commonly used in retail:

- Retail year, quarter, month, week
- Retail period start and end dates
- Retail period indicators
- Retail period keys for joining
- Pattern-specific attributes

### Unified Calendar

The unified calendar view joins all calendar types together, providing a comprehensive view of dates with attributes from all calendars:

- Basic date attributes (Gregorian calendar)
- Holiday information
- Fiscal calendar attributes
- Retail calendar attributes
- Trading day indicators

## ⚙️ Customization

The calendar system is highly customizable:

- **Fiscal Calendar**: Adjust the fiscal year start month and day
- **Retail Calendar**: Choose between 4-4-5, 4-5-4, and 5-4-4 patterns
- **Time Grains**: Select which time grains to generate
- **Date Range**: Specify the start and end dates for the calendar
- **Timezone**: Set the timezone for the calendar

All customization options are available as parameters to the `BUILD_CALENDAR_SYSTEM` procedure.

## 🛡️ Error Handling

All errors are logged to the `CALENDAR_ERROR_LOG` table, which includes:

- Timestamp
- Procedure name
- Error message
- Error state
- Error context
- Stack trace

This makes it easy to troubleshoot any issues that may arise during calendar generation.

## 🔄 Maintenance

The calendar system should be refreshed periodically to:

1. Load new public holidays as they become available
2. Extend the date range as needed
3. Update any configuration changes

This can be done by simply calling the `BUILD_CALENDAR_SYSTEM` procedure again with the desired parameters.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
