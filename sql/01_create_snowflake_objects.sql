/*--
  Step 1: Create roles, database, schema, warehouse, stage, and tables.

  IMPORTANT: Replace <your_user> with your Snowflake username before running.
  Run this script in a Snowflake worksheet or via SnowSQL.
--*/

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS cortex_user_role;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE cortex_user_role;

-- TODO: Replace <your_user> with your Snowflake username
GRANT ROLE cortex_user_role TO USER <your_user>;

USE ROLE SYSADMIN;

-- Create demo database
CREATE OR REPLACE DATABASE cortex_analyst_demo;

-- Create schema
CREATE OR REPLACE SCHEMA cortex_analyst_demo.revenue_timeseries;

-- Create warehouse (XSMALL is sufficient for demo data)
CREATE OR REPLACE WAREHOUSE cortex_analyst_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Warehouse for Cortex Analyst demo';

GRANT USAGE ON WAREHOUSE cortex_analyst_wh TO ROLE cortex_user_role;
GRANT OPERATE ON WAREHOUSE cortex_analyst_wh TO ROLE cortex_user_role;

GRANT OWNERSHIP ON DATABASE cortex_analyst_demo TO ROLE cortex_user_role;
GRANT OWNERSHIP ON SCHEMA cortex_analyst_demo.revenue_timeseries TO ROLE cortex_user_role;

USE ROLE cortex_user_role;
USE WAREHOUSE cortex_analyst_wh;
USE DATABASE cortex_analyst_demo;
USE SCHEMA cortex_analyst_demo.revenue_timeseries;

-- Create stage for raw data
CREATE OR REPLACE STAGE raw_data DIRECTORY = (ENABLE = TRUE);

/*--
  Fact and Dimension Tables
--*/

-- Fact table: daily_revenue
CREATE OR REPLACE TABLE daily_revenue (
    date DATE,
    revenue FLOAT,
    cogs FLOAT,
    forecasted_revenue FLOAT,
    product_id INT,
    region_id INT
);

-- Dimension table: product_dim
CREATE OR REPLACE TABLE product_dim (
    product_id INT,
    product_line VARCHAR(16777216)
);

-- Dimension table: region_dim
CREATE OR REPLACE TABLE region_dim (
    region_id INT,
    sales_region VARCHAR(16777216),
    state VARCHAR(16777216)
);
