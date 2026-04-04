/*--
  Step 4: Create Cortex Search service for semantic search on product lines.
  Run this in a Snowflake worksheet or via SnowSQL.
--*/

USE ROLE cortex_user_role;
USE DATABASE cortex_analyst_demo;
USE SCHEMA revenue_timeseries;

CREATE OR REPLACE CORTEX SEARCH SERVICE product_line_search_service
  ON product_dimension
  WAREHOUSE = cortex_analyst_wh
  TARGET_LAG = '1 hour'
  AS (
      SELECT DISTINCT product_line AS product_dimension FROM product_dim
  );
