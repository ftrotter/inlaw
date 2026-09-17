# InLaw - Data Validation Framework

InLaw creates validation tests to ensure data quality using Great Expectations. Initial creation of ETLs should not include InLaw statements, but once the user requests that they be added, use this framework.

## Command-Line Usage

InLaw can be run from the command line to validate data in your database tables. Create a Python script with your InLaw validation classes and execute it directly.

### Basic Script Structure

```python
#!/usr/bin/env python3
"""
This script tests our basic expectations for what the data looks like.
It validates record counts and data integrity.
"""

import os
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv
from inlaw import InLaw
from dbtable import DBTable

# Load environment variables from .env file
load_dotenv()

# Set up database connection from environment
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/dbname')
engine = create_engine(DATABASE_URL)

# Load configuration from environment (optional, but useful for table references)
config = {
    'MY_SCHEMA': os.getenv('MY_SCHEMA', 'public'),
    'MY_TABLE': os.getenv('MY_TABLE', 'my_table'),
}

# Run all InLaw classes defined in this file
# InLaw.run_all will auto-detect classes in the current file
if __name__ == "__main__":
    InLaw.run_all(engine=engine, config=config)


class ValidateRowCount(InLaw):
    title = "Table should have expected number of rows"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Runs a Great Expectations test to validate the integrity of the data.
        """
        if config is None:
            print("Error: This test requires config. It needs to know the DB Tables")
            exit()

        # Reference your database table
        my_DBTable = DBTable(schema=config['MY_SCHEMA'], table=config['MY_TABLE'])

        sql = f"SELECT COUNT(*) as row_count FROM {my_DBTable}"
        gx_df = InLaw.to_gx_dataframe(sql, engine)

        count_floor = 1  # you choose these manually
        count_ceiling = 100000

        # Use Great Expectations to validate the data
        result = gx_df.expect_column_values_to_be_between(
            column="row_count", 
            min_value=count_floor, 
            max_value=count_ceiling
        )
        
        if result.success:
            return True
        return f"Row count validation failed: expected {count_floor}-{count_ceiling} rows"
```

### Example .env File

```bash
# Database connection
DATABASE_URL=postgresql://user:password@localhost:5432/dbname

# Table configuration
MY_SCHEMA=public
MY_TABLE=my_table

# Optional: Skip all tests (useful for testing/development)
# SKIP_TESTS=1
```

### Running Your Validation Script

```bash
# Make the script executable
chmod +x my_validation_tests.py

# Run the validation tests
./my_validation_tests.py

# Or run with python directly
python my_validation_tests.py
```

## Key InLaw Pattern

1. **Write SQL that returns data to validate** (not violations)
2. **Use `InLaw.to_gx_dataframe(sql, engine)`** to convert to GX DataFrame
3. **Use Great Expectations methods** like `expect_column_values_to_be_between()`, `expect_column_values_to_be_unique()`, etc.
4. **Check `result.success`** and return True/False with descriptive error messages

## Available GX Expectations (Commonly Used)

- `expect_column_values_to_be_between(column, min_value, max_value)` - Range validation
- `expect_column_values_to_be_unique(column)` - Uniqueness validation
- `expect_column_values_to_not_be_null(column)` - Non-null validation
- `expect_column_values_to_be_null(column)` - Null validation
- `expect_table_row_count_to_equal(value)` - Exact row count
- `expect_table_row_count_to_be_between(min_value, max_value)` - Row count range
- `expect_column_sum_to_be_between(column, min_value, max_value)` - Sum validation
- `expect_column_values_to_match_regex(column, regex)` - Pattern matching

## Tips for Equality Testing

- Use `expect_column_values_to_be_between(column, min_value=0, max_value=0)` to test for exactly 0
- Use `expect_table_row_count_to_equal(value)` for exact row counts
- See `../npd_plainerflow/docs/InLaw_README.md` for complete list of available expectations


## Advanced Example: Suite of Related Tests

```python
class ValidateJoinsToMainFile(InLaw):
    """
    Runs a suite of validation tests that depend on joining to another table.
    It first checks if the dependency table is present and valid before running tests.
    """
    title = "Suite of tests joining to the main_file"

    @staticmethod
    def run(engine, config: dict | None = None):
        if config is None:
            print("Error: This test requires config. It needs to know the DB Tables")
            exit()
            
        child_DBTable = DBTable(schema=config['MY_RAW_SCHEMA'], table='child_file')
        main_DBTable = DBTable(schema=config['MY_RAW_SCHEMA'], table='main_file')
        
        # 1. Pre-flight checks for dependency table
        with engine.connect() as connection:
            table_exists_sql = text(f"SELECT to_regclass('{config['MY_RAW_SCHEMA']}.main_file')")
            if not connection.execute(table_exists_sql).scalar():
                return f"SKIPPED: The '{config['MY_RAW_SCHEMA']}.main_file' table does not exist."

            row_count_sql = text(f"SELECT COUNT(*) FROM {config['MY_RAW_SCHEMA']}.main_file")
            row_count = connection.execute(row_count_sql).scalar()
            if row_count <= 1000000:
                return f"SKIPPED: The '{config['MY_RAW_SCHEMA']}.main_file' has only {row_count} rows. Needs > 1,000,000."

        # 2. If pre-flight checks pass, run the dependent tests
        failures = []

        # Test 1: All records in child must exist in main
        sql1 = f"""
            SELECT COUNT(child.id) AS missing_id_count
            FROM {child_DBTable} AS child
            LEFT JOIN {main_DBTable} AS main ON child.id = main.id
            WHERE main.id IS NULL
        """
        gx_df1 = InLaw.to_gx_dataframe(sql1, engine)
        
        expected_missing_floor = 0
        expected_missing_ceiling = 0
        
        result1 = gx_df1.expect_column_values_to_be_between("missing_id_count", expected_missing_floor, expected_missing_ceiling)
        if not result1.success:
            failures.append(f"Found IDs in child that are not in main (expected {expected_missing_floor}-{expected_missing_ceiling}): {result1.result}")

        # Test 2: Additional validation
        sql2 = f"""
            SELECT COUNT(*) as duplicate_count
            FROM (
                SELECT id, COUNT(*) 
                FROM {child_DBTable} 
                GROUP BY id 
                HAVING COUNT(*) > 1
            ) duplicates
        """
        gx_df2 = InLaw.to_gx_dataframe(sql2, engine)
        
        expected_duplicates_floor = 0
        expected_duplicates_ceiling = 0
        
        result2 = gx_df2.expect_column_values_to_be_between("duplicate_count", expected_duplicates_floor, expected_duplicates_ceiling)
        if not result2.success:
            failures.append(f"Found duplicate IDs (expected {expected_duplicates_floor}-{expected_duplicates_ceiling}): {result2.result}")

        if not failures:
            return True
        return "; ".join(failures)
```

## Example: Null Value Validation

```python
class ValidateNoNullValues(InLaw):
    title = "Check for null values in required columns"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Runs a Great Expectations test to validate the integrity of the data.
        """
        if config is None:
            print("Error: This test requires config. It needs to know the DB Tables")
            exit()

        my_DBTable = DBTable(schema=config['MY_SCHEMA'], table='my_table')

        sql = f"SELECT COUNT(*) as null_count FROM {my_DBTable} WHERE required_column IS NULL"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        expected_null_count_floor = 0
        expected_null_count_ceiling = 0
        
        # Expect exactly 0 null values (using min=0, max=0 to check for equality)
        result = gx_df.expect_column_values_to_be_between(
            column="null_count", 
            min_value=expected_null_count_floor,
            max_value=expected_null_count_ceiling
        )
        
        if result.success:
            return True
        return f"Found null values in required column (expected {expected_null_count_floor}-{expected_null_count_ceiling})"
```

## Complete Example: Multiple Validation Tests

```python
#!/usr/bin/env python3
"""
InLaw validation tests for date column conversions.
Validates that date columns were converted properly.
"""

import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from inlaw import InLaw
from dbtable import DBTable

# Load environment variables
load_dotenv()

# Database connection from environment
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/dbname')
engine = create_engine(DATABASE_URL)

# Configuration from environment
config = {
    'CLIA_SCHEMA': os.getenv('CLIA_SCHEMA', 'silver_clia_pos'),
}

# Run all InLaw classes defined in this file
# InLaw will auto-detect classes in the current file when run_all is called
if __name__ == "__main__":
    InLaw.run_all(engine=engine, config=config)


class ValidateDateColumnsConverted(InLaw):
    title = "All date columns should be converted to DATE type"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validates that all expected date columns are now DATE type, not VARCHAR.
        """
        if config is None:
            print("Error: This test requires config. It needs to know the DB Tables")
            exit()
        
        schema = config.get('CLIA_SCHEMA', 'silver_clia_pos')
        
        sql = f"""
            SELECT COUNT(*) as varchar_date_count
            FROM information_schema.columns 
            WHERE table_schema = '{schema}'
              AND table_name = 'clia'
              AND column_name IN (
                'crtfctn_dt', 'chow_dt', 'orgnl_prtcptn_dt', 'chow_prior_dt', 
                'trmntn_exprtn_dt', 'a2la_acrdtd_y_match_dt', 'aabb_acrdtd_y_match_dt',
                'aoa_acrdtd_y_match_dt', 'ashi_acrdtd_y_match_dt', 'cap_acrdtd_y_match_dt',
                'cola_acrdtd_y_match_dt', 'jcaho_acrdtd_y_match_dt', 'aplctn_rcvd_dt',
                'crtfct_efctv_dt', 'crtfct_mail_dt'
              )
              AND data_type = 'character varying'
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        expected_varchar_floor = 0
        expected_varchar_ceiling = 0
        
        result = gx_df.expect_column_values_to_be_between(
            column="varchar_date_count", 
            min_value=expected_varchar_floor,
            max_value=expected_varchar_ceiling
        )
        
        if result.success:
            return True
        return f"Some date columns are still VARCHAR type and were not converted (expected {expected_varchar_floor}-{expected_varchar_ceiling})"


class ValidateRowCount(InLaw):
    title = "Table should have expected number of rows"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validates that the table has data.
        """
        if config is None:
            print("Error: This test requires config")
            exit()

        schema = config.get('CLIA_SCHEMA', 'silver_clia_pos')
        clia_DBTable = DBTable(schema=schema, table='clia')

        sql = f"SELECT COUNT(*) as row_count FROM {clia_DBTable}"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        count_floor = 1
        count_ceiling = 10000000
        
        result = gx_df.expect_column_values_to_be_between(
            column="row_count", 
            min_value=count_floor, 
            max_value=count_ceiling
        )
        
        if result.success:
            return True
        return f"Row count validation failed: expected {count_floor}-{count_ceiling} rows"
```

## Key Features

- **Auto-Discovery**: InLaw automatically finds validation classes in the current file when `run_all()` is called
- **Environment-Based Configuration**: Use `.env` files to store database credentials and configuration
- **Skip Tests**: Set `SKIP_TESTS=1` in your `.env` file to skip all validation tests
- **Dictionary-Based Config**: Pass configuration as a simple Python dictionary
- **No Boilerplate**: InLaw handles all Great Expectations setup internally
