/*--
  Step 2: Upload local CSV data files and semantic model YAML to the Snowflake stage.

  Run this via SnowSQL from the project directory:
    cd /Users/mendelsohn/Desktop/claude_code_snowflake_cortex_project
    snowsql -a <account> -u <user> -f sql/02_upload_data_and_model.sql

  Or update the file paths below if your project directory is different.
--*/

USE ROLE cortex_user_role;
USE DATABASE cortex_analyst_demo;
USE SCHEMA revenue_timeseries;
USE WAREHOUSE cortex_analyst_wh;

-- Upload CSV data files to stage
PUT file://data/daily_revenue.csv @raw_data AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://data/product.csv @raw_data AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://data/region.csv @raw_data AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Upload semantic model YAML to stage
PUT file://semantic_models/revenue_timeseries.yaml @raw_data AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Verify uploaded files
LIST @raw_data;
