"""
InLaw CLI - Command-line interface for running InLaw database validation tests.
"""

import sys
import os
import argparse
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import sqlalchemy
from src.inlaw import InLaw
from src.dbtable import DBTable


class InLawCLI:
    """Command-line interface for InLaw database testing."""

    @staticmethod
    def _find_env_file(*, start_directory: str) -> Optional[str]:
        """
        Search for .env file in the current directory and parent directory.

        Args:
            start_directory: Directory to start searching from

        Returns:
            Path to .env file if found, None otherwise
        """
        current_dir = Path(start_directory).resolve()

        # Check current directory
        env_path = current_dir / ".env"
        if env_path.exists():
            return str(env_path)

        # Check parent directory
        parent_env_path = current_dir.parent / ".env"
        if parent_env_path.exists():
            return str(parent_env_path)

        return None

    @staticmethod
    def _load_env_file(*, env_file_path: Optional[str] = None, search_directory: Optional[str] = None) -> bool:
        """
        Load environment variables from .env file.

        Args:
            env_file_path: Explicit path to .env file (from --db-config-file)
            search_directory: Directory to search for .env file if env_file_path not provided

        Returns:
            True if .env file was loaded successfully, False otherwise
        """
        if env_file_path:
            # Use explicitly specified .env file
            if not os.path.exists(env_file_path):
                print(f"Error: Specified .env file not found: {env_file_path}", file=sys.stderr)
                return False
            load_dotenv(env_file_path)
            print(f"Loaded configuration from: {env_file_path}")
            return True

        # Auto-discover .env file
        if search_directory:
            found_env = InLawCLI._find_env_file(start_directory=search_directory)
            if found_env:
                load_dotenv(found_env)
                print(f"Auto-discovered configuration file: {found_env}")
                return True

        print("Warning: No .env file found in current or parent directory", file=sys.stderr)
        return False

    @staticmethod
    def _build_connection_url() -> Optional[str]:
        """
        Build SQLAlchemy connection URL from INLAW_* environment variables.

        Supports two modes:
        1. INLAW_URL - complete connection string
        2. Individual components: INLAW_DIALECT, INLAW_USER, INLAW_PASSWORD, etc.

        Returns:
            SQLAlchemy connection URL string, or None if required variables missing
        """
        # Check for complete URL first
        inlaw_url = os.getenv("INLAW_URL")
        if inlaw_url:
            return inlaw_url

        # Build from components
        dialect = os.getenv("INLAW_DIALECT")
        if not dialect:
            print("Error: INLAW_DIALECT environment variable is required", file=sys.stderr)
            return None

        driver = os.getenv("INLAW_DRIVER")
        user = os.getenv("INLAW_USER")
        password = os.getenv("INLAW_PASSWORD")
        host = os.getenv("INLAW_HOST")
        port = os.getenv("INLAW_PORT")
        database = os.getenv("INLAW_DATABASE")

        # Build dialect string
        if driver:
            dialect_str = f"{dialect}+{driver}"
        else:
            dialect_str = dialect

        # Build connection URL based on what's available
        if dialect == "sqlite":
            # SQLite can use just database path
            if not database:
                print("Error: INLAW_DATABASE is required for SQLite", file=sys.stderr)
                return None
            return f"{dialect_str}:///{database}"

        # For other databases, we typically need user, password, host, and database
        if not all([user, host, database]):
            print(
                "Error: INLAW_USER, INLAW_HOST, and INLAW_DATABASE are required "
                f"for {dialect} connections",
                file=sys.stderr
            )
            return None

        # Build URL with optional password and port
        if password and port:
            url = f"{dialect_str}://{user}:{password}@{host}:{port}/{database}"
        elif password:
            url = f"{dialect_str}://{user}:{password}@{host}/{database}"
        elif port:
            url = f"{dialect_str}://{user}@{host}:{port}/{database}"
        else:
            url = f"{dialect_str}://{user}@{host}/{database}"

        return url

    @staticmethod
    def _create_engine() -> Optional[sqlalchemy.engine.Engine]:
        """
        Create SQLAlchemy engine from environment variables.

        Returns:
            SQLAlchemy engine, or None if creation failed
        """
        connection_url = InLawCLI._build_connection_url()
        if not connection_url:
            return None

        try:
            engine = sqlalchemy.create_engine(connection_url)
            # Test connection
            with engine.connect() as conn:
                pass
            print(f"Successfully connected to database")
            return engine
        except Exception as e:
            print(f"Error: Failed to connect to database: {e}", file=sys.stderr)
            return None

    @staticmethod
    def _parse_arguments() -> argparse.Namespace:
        """
        Parse command-line arguments.

        Returns:
            Parsed arguments namespace
        """
        parser = argparse.ArgumentParser(
            description="InLaw - Run database validation tests",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  inlaw                              # Run tests in current directory
  inlaw /path/to/tests              # Run tests in specific directory
  inlaw --db-config-file custom.env  # Use custom .env file
  inlaw /path/to/tests --db-config-file /path/to/.env
            """
        )

        parser.add_argument(
            "directory",
            nargs="?",
            default=".",
            help="Directory containing InLaw test files (default: current directory)"
        )

        parser.add_argument(
            "--db-config-file",
            dest="db_config_file",
            help="Path to .env file with database configuration"
        )

        return parser.parse_args()

    @staticmethod
    def main() -> int:
        """
        Main CLI entry point.

        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        # Parse arguments
        args = InLawCLI._parse_arguments()

        # Resolve directory path
        test_directory = os.path.abspath(args.directory)
        if not os.path.isdir(test_directory):
            print(f"Error: Directory not found: {test_directory}", file=sys.stderr)
            return 1

        print(f"InLaw CLI - Running tests from: {test_directory}")
        print("=" * 60)

        # Load .env file
        env_loaded = InLawCLI._load_env_file(
            env_file_path=args.db_config_file,
            search_directory=test_directory
        )

        if not env_loaded and not args.db_config_file:
            print("Continuing without .env file configuration...", file=sys.stderr)

        # Create database engine
        engine = InLawCLI._create_engine()
        if not engine:
            print("Error: Failed to create database connection", file=sys.stderr)
            return 1

        # Run InLaw tests
        try:
            results = InLaw.run_all(
                engine=engine,
                inlaw_dir=test_directory,
                config=None
            )

            # Check results
            if results.get("skipped"):
                print("Tests were skipped")
                return 0

            total_tests = results.get("total", 0)
            failed_tests = results.get("failed", 0)
            error_tests = results.get("errors", 0)

            if total_tests == 0:
                print("Warning: No tests were found or executed", file=sys.stderr)
                return 1

            if failed_tests > 0 or error_tests > 0:
                return 1

            return 0

        except Exception as e:
            print(f"Error: Failed to run tests: {e}", file=sys.stderr)
            return 1


def main():
    """Entry point for console script."""
    sys.exit(InLawCLI.main())


if __name__ == "__main__":
    main()
