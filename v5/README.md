# Business Calendar System for Snowflake

## Overview

This solution provides a modular, conformed business calendar in Snowflake, following Kimball principles. It supports Gregorian, corporate fiscal, and retail fiscal calendars, and exposes dynamic, time-relative flags for BI and analytics. It also integrates Australian public holiday data from data.gov.au.

---

## Components

- **`DATE_SPINE` Table**: Continuous date sequence.
- **`DIM_GREGORIAN` Table**: Gregorian calendar attributes.
- **`DIM_FISCAL` Table**: Corporate fiscal calendar (month-based).
- **`DIM_RETAIL` Table**: Retail fiscal calendar (4-4-5 pattern).
- **`DIM_DATE_UNIFIED` View**: Unified conformed calendar combining all above.
- **`VW_DIM_DATE_WITH_DYNAMIC_FLAGS` View**: Adds dynamic flags and offsets.
- **`LOAD_AU_HOLIDAYS` Stored Procedure**: Loads Australian public holidays into `PUBLIC_HOLIDAYS_AU`.

---

## Setup Instructions

### 1. Create the Modular Calendar Assets

Run these scripts in order:

- `date_spine.sql`
- `dim_gregorian.sql`
- `dim_fiscal.sql`
- `dim_retail.sql`
- `dim_date_unified.sql`
- `vw_dim_date_with_dynamic_flags.sql`

### 2. Load Australian Public Holidays

Run the holiday loader stored procedure (replace with your database and schema):

```sql
CALL LOAD_AU_HOLIDAYS('YOUR_DATABASE','YOUR_SCHEMA');
```

---

## Usage Examples

### Query the Unified Calendar

```sql
SELECT * FROM DIM_DATE_UNIFIED WHERE FISCAL_YEAR_NUMBER = 2025;
```

### Query with Dynamic Flags

```sql
SELECT *
FROM VW_DIM_DATE_WITH_DYNAMIC_FLAGS
WHERE IS_YTD = TRUE;
```

### Filter for Last 30 Days

```sql
SELECT *
FROM VW_DIM_DATE_WITH_DYNAMIC_FLAGS
WHERE IS_LAST_30_DAYS = TRUE;
```

### Filter for Public Holidays

```sql
SELECT *
FROM VW_DIM_DATE_WITH_DYNAMIC_FLAGS
WHERE IS_PUBLIC_HOLIDAY = TRUE;
```

---

## Notes

- Fiscal calendar is month-based, no 53-week handling.
- Retail calendar supports 4-4-5 pattern with 53-week years.
- Dynamic flags are calculated at query time using `CURRENT_DATE()`.
- Public holiday info is joined dynamically from `PUBLIC_HOLIDAYS_AU`.
- Designed for extensibility — add columns or flags as needed.
- No daily update jobs required; refresh by rerunning the procedures if extending date range or updating holidays.

---

## Maintenance

- To extend the calendar, regenerate `DATE_SPINE` and downstream tables/views.
- To refresh holidays, rerun `LOAD_AU_HOLIDAYS`.
- To add new attributes, alter the relevant calendar tables and update the unified view.
- To modify dynamic flags or holiday logic, edit the dynamic view.

---

## Author

Andrew Exley, Lument Pty Ltd  
Date: 2025-04-06