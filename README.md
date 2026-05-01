# InLaw

A lightweight wrapper around Great Expectations for database validation testing with minimal boilerplate.

InLaw allows you to write simple database validation tests as Python classes and run them from the command line or programmatically.

## Features

- 🚀 **Simple CLI tool** - Run tests with a single command
- 🔍 **Auto-discovery** - Automatically finds and loads test files from directories
- 🔌 **Environment-based configuration** - Database connections via .env files
- 📊 **Great Expectations integration** - Leverage the power of GX without the complexity
- 🗃️ **DBTable support** - Flexible database table identification across SQL dialects

## Installation

```bash
# Basic installation
pip install inlaw

# With DuckDB support
pip install inlaw[duckdb]

# With PostgreSQL support
pip install inlaw[postgres]

# With MySQL support
pip install inlaw[mysql]

# With all database drivers
pip install inlaw[duckdb,postgres,mysql]
```

For local development:

```bash
git clone https://github.com/ftrotter/inlaw.git
cd inlaw
pip install -e .
```

### Python Version Compatibility

**Important**: While this package supports Python 3.10+, Great Expectations (a core dependency) currently has runtime compatibility issues with Python 3.14 due to Pydantic v1 limitations. For best results, use Python 3.10, 3.11, 3.12, or 3.13.

If you encounter Pydantic-related errors on Python 3.14, please downgrade to Python 3.13 or earlier.

## Quick Start

### 1. Create a test file

Create a file with your InLaw test classes (e.g., `my_tests.py`):

```python
from inlaw import InLaw, DBTable

class TestUsersTableExists(InLaw):
    title = "Verify users table exists and has records"
    
    @staticmethod
    def run(engine, config=None):
        # Create a DBTable reference
        users_table = DBTable(database='mydb', schema='public', table='users')
        
        # Query the table
        sql = f"SELECT COUNT(*) as count FROM {users_table}"
        gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        # Use Great Expectations validations
        result = gx_df.expect_column_values_to_be_between(
            column='count',
            min_value=1
        )
        
        if result.success:
            return True
        else:
            return "Users table is empty"
```

### 2. Configure database connection

Create a `.env` file in your project directory:

```bash
# Complete connection URL (option 1)
INLAW_URL=postgresql://user:password@localhost:5432/mydb

# OR individual components (option 2)
INLAW_DIALECT=postgresql
INLAW_DRIVER=psycopg2
INLAW_USER=myuser
INLAW_PASSWORD=mypassword
INLAW_HOST=localhost
INLAW_PORT=5432
INLAW_DATABASE=mydb
```

### 3. Run tests

```bash
# Run tests in current directory
inlaw

# Run tests in specific directory
inlaw /path/to/tests

# Use custom .env file
inlaw --db-config-file /path/to/custom.env

# Run tests in specific directory with custom config
inlaw /path/to/tests --db-config-file /path/to/.env
```

## Environment Variables

InLaw uses environment variables with the `INLAW_` prefix for database configuration:

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `INLAW_URL` | * | Complete SQLAlchemy connection URL | `postgresql://user:pass@host:5432/db` |
| `INLAW_DIALECT` | ** | Database dialect | `postgresql`, `mysql`, `sqlite`, `duckdb` |
| `INLAW_DRIVER` | No | Database driver | `psycopg2`, `pymysql` |
| `INLAW_USER` | ** | Database username | `myuser` |
| `INLAW_PASSWORD` | No | Database password | `secretpass` |
| `INLAW_HOST` | ** | Database host | `localhost`, `db.example.com` |
| `INLAW_PORT` | No | Database port | `5432`, `3306` |
| `INLAW_DATABASE` | ** | Database name/path | `mydb`, `/path/to/db.duckdb` |

\* Either `INLAW_URL` OR the individual components (\*\*) are required.

### Database-Specific Configuration

#### DuckDB

DuckDB can be used with either an in-memory database or a file-based database:

```bash
# File-based DuckDB
INLAW_DIALECT=duckdb
INLAW_DATABASE=/path/to/mydata.duckdb

# Or using URL format
INLAW_URL=duckdb:///path/to/mydata.duckdb

# In-memory DuckDB (for testing)
INLAW_URL=duckdb:///:memory:
```

#### PostgreSQL

```bash
# Using URL
INLAW_URL=postgresql://user:password@localhost:5432/mydb

# Or individual components
INLAW_DIALECT=postgresql
INLAW_DRIVER=psycopg2
INLAW_USER=myuser
INLAW_PASSWORD=mypassword
INLAW_HOST=localhost
INLAW_PORT=5432
INLAW_DATABASE=mydb
```

#### MySQL

```bash
# Using URL
INLAW_URL=mysql+pymysql://user:password@localhost:3306/mydb

# Or individual components
INLAW_DIALECT=mysql
INLAW_DRIVER=pymysql
INLAW_USER=myuser
INLAW_PASSWORD=mypassword
INLAW_HOST=localhost
INLAW_PORT=3306
INLAW_DATABASE=mydb
```

#### SQLite

```bash
# Using URL
INLAW_URL=sqlite:////absolute/path/to/database.db

# Or individual components
INLAW_DIALECT=sqlite
INLAW_DATABASE=/absolute/path/to/database.db
```

## .env File Discovery

InLaw automatically searches for `.env` files in the following order:

1. Custom file specified with `--db-config-file`
2. `.env` in the test directory
3. `.env` in the parent directory of the test directory

## Writing InLaw Tests

### Basic Test Structure

Every InLaw test is a class that:

1. Inherits from `InLaw`
2. Has a `title` class attribute
3. Implements a `run(engine, config)` static method

```python
from inlaw import InLaw

class MyTest(InLaw):
    title = "My Test Description"
    
    @staticmethod
    def run(engine, config=None):
        # Your test logic here
        # Return True for pass, or a string error message for fail
        return True
```

### Using DBTable

The `DBTable` class provides flexible table identification across different SQL dialects:

```python
from inlaw import DBTable

# PostgreSQL style (database + schema + table)
table = DBTable(database='mydb', schema='public', table='users')

# MySQL style (database + table)
table = DBTable(database='mydb', table='users')

# Databricks style (catalog + database + table)
table = DBTable(catalog='main', database='mydb', table='users')

# Use in SQL queries
sql = f"SELECT * FROM {table}"  # Becomes: "mydb.public.users"

# Create related tables
staging_table = table.make_child('staging')  # Creates "users_staging"
```

### Using Great Expectations

InLaw provides the `sql_to_gx_df()` helper to convert SQL queries into GX validators:

```python
@staticmethod
def run(engine, config=None):
    sql = "SELECT age, email FROM users"
    gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
    
    # Use any Great Expectations expectation
    result = gx_df.expect_column_values_to_not_be_null(column='email')
    
    if result.success:
        return True
    else:
        return f"Found {result.result['unexpected_count']} null emails"
```

### Return Values

Your `run()` method should return:

- `True` - Test passed
- `str` - Test failed with this error message

```python
@staticmethod
def run(engine, config=None):
    # ... test logic ...
    
    if everything_ok:
        return True
    else:
        return "Specific error message explaining what failed"
```

## Programmatic Usage

You can also use InLaw programmatically in your Python code:

```python
from sqlalchemy import create_engine
from inlaw import InLaw

# Create engine
engine = create_engine('postgresql://user:pass@localhost/mydb')

# Run all tests in a directory
results = InLaw.run_all(
    engine=engine,
    inlaw_dir='/path/to/tests',
    config={'threshold': 100}  # Optional config passed to tests
)

print(f"Passed: {results['passed']}")
print(f"Failed: {results['failed']}")
print(f"Errors: {results['errors']}")
```

## CLI Options

```
usage: inlaw [-h] [--db-config-file DB_CONFIG_FILE] [directory]

InLaw - Run database validation tests

positional arguments:
  directory             Directory containing InLaw test files (default: current directory)

optional arguments:
  -h, --help            show this help message and exit
  --db-config-file DB_CONFIG_FILE
                        Path to .env file with database configuration

Examples:
  inlaw                              # Run tests in current directory
  inlaw /path/to/tests              # Run tests in specific directory
  inlaw --db-config-file custom.env  # Use custom .env file
  inlaw /path/to/tests --db-config-file /path/to/.env
```

## Exit Codes

The CLI returns the following exit codes:

- `0` - All tests passed
- `1` - One or more tests failed or an error occurred

## License

See [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/ftrotter/inlaw).
