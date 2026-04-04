/*--
  Step 3: Load data from staged CSV files into tables.
  Run this in a Snowflake worksheet or via SnowSQL.
--*/

USE ROLE cortex_user_role;
USE DATABASE cortex_analyst_demo;
USE SCHEMA revenue_timeseries;
USE WAREHOUSE cortex_analyst_wh;

COPY INTO daily_revenue
FROM @raw_data
FILES = ('daily_revenue.csv')
FILE_FORMAT = (
    TYPE = CSV,
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    TRIM_SPACE = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE,
    REPLACE_INVALID_CHARACTERS = TRUE,
    DATE_FORMAT = AUTO,
    TIME_FORMAT = AUTO,
    TIMESTAMP_FORMAT = AUTO,
    EMPTY_FIELD_AS_NULL = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = CONTINUE
FORCE = TRUE;

COPY INTO product_dim
FROM @raw_data
FILES = ('product.csv')
FILE_FORMAT = (
    TYPE = CSV,
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    TRIM_SPACE = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE,
    REPLACE_INVALID_CHARACTERS = TRUE,
    DATE_FORMAT = AUTO,
    TIME_FORMAT = AUTO,
    TIMESTAMP_FORMAT = AUTO,
    EMPTY_FIELD_AS_NULL = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = CONTINUE
FORCE = TRUE;

COPY INTO region_dim
FROM @raw_data
FILES = ('region.csv')
FILE_FORMAT = (
    TYPE = CSV,
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    TRIM_SPACE = FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY = NONE,
    REPLACE_INVALID_CHARACTERS = TRUE,
    DATE_FORMAT = AUTO,
    TIME_FORMAT = AUTO,
    TIMESTAMP_FORMAT = AUTO,
    EMPTY_FIELD_AS_NULL = FALSE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = CONTINUE
FORCE = TRUE;

-- Verify row counts
SELECT 'daily_revenue' AS table_name, COUNT(*) AS row_count FROM daily_revenue
UNION ALL
SELECT 'product_dim', COUNT(*) FROM product_dim
UNION ALL
SELECT 'region_dim', COUNT(*) FROM region_dim;
