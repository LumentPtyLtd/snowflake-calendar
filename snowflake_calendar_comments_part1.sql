/*
======================================================================================
ENHANCED SNOWFLAKE CALENDAR SYSTEM FOR AUSTRALIA - COLUMN COMMENTS (PART 1)
======================================================================================

This script adds detailed comments to the error logging, configuration, and holiday tables.
Execute this script after all other scripts have been executed.

Version: 2.0
Date: 31/03/2025
*/

-- Add comments to CALENDAR_ERROR_LOG table
COMMENT ON TABLE CALENDAR_ERROR_LOG IS 'Error logging table for the calendar system. Records all errors that occur during calendar generation and usage.';

COMMENT ON COLUMN CALENDAR_ERROR_LOG.ERROR_ID IS 'Unique identifier for the error record.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.ERROR_TIMESTAMP IS 'Timestamp when the error occurred.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.PROCEDURE_NAME IS 'Name of the procedure or function where the error occurred.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.ERROR_MESSAGE IS 'Detailed error message.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.ERROR_STATE IS 'State or category of the error.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.ERROR_CONTEXT IS 'JSON object containing contextual information about the error.';
COMMENT ON COLUMN CALENDAR_ERROR_LOG.STACK_TRACE IS 'Stack trace for the error, if available.';

-- Add comments to CALENDAR_CONFIG table
COMMENT ON TABLE CALENDAR_CONFIG IS 'Configuration table for the calendar system. Stores default values and settings.';

COMMENT ON COLUMN CALENDAR_CONFIG.CONFIG_ID IS 'Unique identifier for the configuration record.';
COMMENT ON COLUMN CALENDAR_CONFIG.CONFIG_NAME IS 'Name of the configuration parameter.';
COMMENT ON COLUMN CALENDAR_CONFIG.CONFIG_VALUE IS 'JSON object containing the configuration value.';
COMMENT ON COLUMN CALENDAR_CONFIG.DESCRIPTION IS 'Description of the configuration parameter.';
COMMENT ON COLUMN CALENDAR_CONFIG.CREATED_AT IS 'Timestamp when the configuration was created.';
COMMENT ON COLUMN CALENDAR_CONFIG.UPDATED_AT IS 'Timestamp when the configuration was last updated.';
COMMENT ON COLUMN CALENDAR_CONFIG.CREATED_BY IS 'User who created the configuration.';
COMMENT ON COLUMN CALENDAR_CONFIG.UPDATED_BY IS 'User who last updated the configuration.';
COMMENT ON COLUMN CALENDAR_CONFIG.ACTIVE IS 'Flag indicating whether the configuration is active.';

-- Add comments to AU_PUBLIC_HOLIDAYS table
COMMENT ON TABLE AU_PUBLIC_HOLIDAYS IS 'Australian public holidays loaded from data.gov.au. Contains holidays for all Australian jurisdictions.';

COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.HOLIDAY_ID IS 'Unique identifier for the holiday record.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.HOLIDAY_DATE IS 'Date of the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.HOLIDAY_NAME IS 'Name of the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.INFORMATION IS 'Additional information about the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.MORE_INFORMATION IS 'URL or reference for more information about the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.JURISDICTION IS 'Australian jurisdiction (state/territory) where the holiday applies.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.LOADED_AT IS 'Timestamp when the holiday record was loaded.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS.LOAD_BATCH_ID IS 'Batch identifier for the load process.';

-- Add comments to AU_PUBLIC_HOLIDAYS_VW view
COMMENT ON VIEW AU_PUBLIC_HOLIDAYS_VW IS 'View of Australian public holidays with simplified column names.';

COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS_VW.DATE IS 'Date of the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS_VW.HOLIDAY_NAME IS 'Name of the holiday.';
COMMENT ON COLUMN AU_PUBLIC_HOLIDAYS_VW.STATE IS 'Australian jurisdiction (state/territory) where the holiday applies.';