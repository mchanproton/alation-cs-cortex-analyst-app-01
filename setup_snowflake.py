"""
Snowflake Setup Script
======================
Uploads local data files and semantic model to the Snowflake stage,
then loads data into tables.

Prerequisites:
  - .env file with valid Snowflake credentials
  - Script 01 (sql/01_create_snowflake_objects.sql) has already been run
    in a Snowflake worksheet to create the database, schema, warehouse,
    tables, and stage.

Usage:
  source venv/bin/activate
  python setup_snowflake.py
"""

import os
import sys

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "CORTEX_ANALYST_WH"),
        role=os.getenv("SNOWFLAKE_ROLE", "CORTEX_USER_ROLE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "CORTEX_ANALYST_DEMO"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "REVENUE_TIMESERIES"),
    )


def run_step(cursor, description, sql):
    print(f"\n{'='*60}")
    print(f"  {description}")
    print(f"{'='*60}")
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            print(f"  {row}")
        print("  ✓ Done")
    except Exception as e:
        print(f"  ✗ Error: {e}")
        raise


def upload_files(cursor):
    """Upload CSV data files and semantic model YAML to the Snowflake stage."""
    files_to_upload = [
        ("data/daily_revenue.csv", "Daily revenue data"),
        ("data/product.csv", "Product dimension data"),
        ("data/region.csv", "Region dimension data"),
        ("semantic_models/revenue_timeseries.yaml", "Semantic model"),
    ]
    for file_path, description in files_to_upload:
        full_path = os.path.join(PROJECT_DIR, file_path)
        if not os.path.exists(full_path):
            print(f"  ✗ File not found: {full_path}")
            sys.exit(1)
        print(f"\n  Uploading {description} ({file_path})...")
        cursor.execute(
            f"PUT file://{full_path} @raw_data AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        result = cursor.fetchall()
        for row in result:
            print(f"    {row[0]} -> {row[1]} ({row[4]})")
    print("\n  ✓ All files uploaded")


def load_data(cursor):
    """Load data from staged CSV files into tables."""
    copy_statements = [
        ("daily_revenue", "daily_revenue.csv"),
        ("product_dim", "product.csv"),
        ("region_dim", "region.csv"),
    ]
    for table, csv_file in copy_statements:
        print(f"\n  Loading {csv_file} -> {table}...")
        cursor.execute(f"""
            COPY INTO {table}
            FROM @raw_data
            FILES = ('{csv_file}')
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
            FORCE = TRUE
        """)
        result = cursor.fetchall()
        for row in result:
            print(f"    Loaded: {row}")
    print("\n  ✓ All data loaded")


def verify_data(cursor):
    """Verify row counts in each table."""
    tables = ["daily_revenue", "product_dim", "region_dim"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} rows")


def create_cortex_search(cursor):
    """Create the Cortex Search service for product line semantic search."""
    cursor.execute("""
        CREATE OR REPLACE CORTEX SEARCH SERVICE product_line_search_service
          ON product_dimension
          WAREHOUSE = cortex_analyst_wh
          TARGET_LAG = '1 hour'
          AS (
              SELECT DISTINCT product_line AS product_dimension FROM product_dim
          )
    """)
    print("  ✓ Cortex Search service created")


def main():
    print("Snowflake Setup Script")
    print("=" * 60)
    print(f"Account:   {os.getenv('SNOWFLAKE_ACCOUNT')}")
    print(f"User:      {os.getenv('SNOWFLAKE_USER')}")
    print(f"Database:  {os.getenv('SNOWFLAKE_DATABASE')}")
    print(f"Schema:    {os.getenv('SNOWFLAKE_SCHEMA')}")
    print(f"Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
    print()

    conn = get_connection()
    cursor = conn.cursor()
    print("✓ Connected to Snowflake\n")

    try:
        # Step 1: Upload files to stage
        print("STEP 1: Uploading files to @raw_data stage")
        upload_files(cursor)

        # Step 2: Load data into tables
        print("\n\nSTEP 2: Loading data into tables")
        load_data(cursor)

        # Step 3: Verify data
        print("\n\nSTEP 3: Verifying row counts")
        verify_data(cursor)

        # Step 4: Create Cortex Search service
        print("\n\nSTEP 4: Creating Cortex Search service")
        create_cortex_search(cursor)

        print("\n\n" + "=" * 60)
        print("  Setup complete! You can now run the app:")
        print("    streamlit run app.py")
        print("=" * 60)

    except Exception as e:
        print(f"\n\n✗ Setup failed: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
