"""
InLaw - A lightweight wrapper around Great Expectations (GX)

Single-file child classes with zero GX boilerplate.

"""
import ast
import inspect
import sys
import os
import importlib.util
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union, List, Dict, Any, Optional, cast, TYPE_CHECKING
import sqlalchemy
import pandas as pd

if TYPE_CHECKING:
    pass  # Type hints for GX validators if needed


try:
    import great_expectations as gx
except ImportError:
    raise ImportError(
        "Great Expectations is required for InLaw. Install with: pip install great-expectations"
    )

# Suppress Great Expectations checkpoint warnings since InLaw replaces checkpoints
warnings.filterwarnings(
    "ignore",
    message=r".*result_format.*configured at the Validator-level will not be persisted.*",
    category=UserWarning,
    module="great_expectations.expectations.expectation"
)


class _SuppressGXWarnings:
    """Context manager to suppress Great Expectations checkpoint warnings."""

    def __enter__(self):
        warnings.filterwarnings(
            "ignore",
            message=r".*result_format.*configured at the Validator-level will not be persisted.*",
            category=UserWarning
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Reset warnings to default behavior for this specific warning
        warnings.filterwarnings(
            "default",
            message=r".*result_format.*configured at the Validator-level will not be persisted.*",
            category=UserWarning
        )


class InLaw(ABC):
    """
    Abstract base class for Great Expectations validation tests.

    Child classes must implement:
    - title: str (class attribute)
    - run(engine, config) -> bool | str (static method)
    """

    title: str = "Unnamed Test"

    @staticmethod
    @abstractmethod
    def run(engine, config: Dict[str, Any] | None = None) -> Union[bool, str]:
        """
        Run the validation test.

        Args:
            engine: SQLAlchemy engine for database connection
            config: Configuration dictionary with table names, schemas, thresholds, etc.

        Returns:
            True if test passes
            str if test fails (error message)
        """
        pass

    @staticmethod
    def sql_to_gx_df(*, sql: str, engine):
        """
        Convert SQL query result to Great Expectations DataFrame.

        Args:
            sql: SQL query string
            engine: SQLAlchemy engine

        Returns:
            Great Expectations Validator (works like PandasDataset in older GX)
        """

        try:
            # Execute SQL and get pandas DataFrame
            with engine.connect() as conn:
                pandas_df = pd.read_sql_query(sqlalchemy.text(sql), conn)

            # Suppress only the specific result_format warning during GX operations
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r".*result_format.*configured at the Validator-level will not be persisted.*",
                    category=UserWarning
                )

                # Use GX 1.x API - create validator directly from pandas DataFrame
                # This returns a Validator object which has all the expect_* methods
                context = gx.get_context()
                
                # Create a validator from pandas dataframe
                # The validator object has the same API as the old PandasDataset
                validator = context.sources.pandas_default.read_dataframe(pandas_df)

            return validator

        except Exception as e:
            raise RuntimeError(f"Failed to execute SQL and create GX DataFrame: {e}")


    @staticmethod
    def to_gx_dataframe(sql: str, engine):
        """
        Legacy method name - use sql_to_gx_df instead.
        Convert SQL query result to Great Expectations Validator.

        Args:
            sql: SQL query string
            engine: SQLAlchemy engine

        Returns:
            Great Expectations Validator
        """
        return InLaw.sql_to_gx_df(sql=sql, engine=engine)


    @staticmethod
    def ansi_green(text: str) -> str:
        """Return text with ANSI green color codes."""
        return f"\033[92m{text}\033[0m"

    @staticmethod
    def ansi_red(text: str) -> str:
        """Return text with ANSI red color codes."""
        return f"\033[91m{text}\033[0m"

    @staticmethod
    def _import_file(*, file_path: str) -> None:
        """
        Import a Python file to discover InLaw subclasses.

        Args:
            file_path: Path to the Python file to import
        """
        try:
            # Get absolute path
            abs_path = os.path.abspath(file_path)

            # Create module name from file path
            module_name = os.path.splitext(os.path.basename(abs_path))[0]

            # Load the module
            spec = importlib.util.spec_from_file_location(module_name, abs_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

        except Exception as e:
            print(f"Warning: Failed to import {file_path}: {e}")

    @staticmethod
    def _import_directory(*, directory_path: str) -> None:
        """
        Import all Python files in a directory to discover InLaw subclasses.

        Args:
            directory_path: Path to the directory containing Python files
        """
        try:
            print(f"InLaw instructed to scan for InLaw classes in: {os.path.abspath(directory_path)}")
            if not os.path.isdir(directory_path):
                print(f"Warning: Directory {directory_path} does not exist")
                return

            for filename in os.listdir(directory_path):
                if filename.endswith('.py') and not filename.startswith('__'):
                    file_path = os.path.join(directory_path, filename)
                    InLaw._import_file(file_path=file_path)

        except Exception as e:
            print(f"Warning: Failed to import from directory {directory_path}: {e}")

    @staticmethod
    def run_all(
        *,
        engine,
        inlaw_files: Optional[List[str]] = None,
        inlaw_dir: Optional[str] = None,
        config: Dict[str, Any] | None = None,
        ignore_skip_test: bool = False
    ) -> Dict[str, Any]:
        """
        Discover and run all InLaw subclasses.
        
        Args:
            engine: SQLAlchemy engine for database connection
            inlaw_files: Optional list of relative file paths to import for InLaw tests
            inlaw_dir: Optional directory path to scan for InLaw test files
            config: Configuration dictionary with table names, schemas, thresholds, etc.
            ignore_skip_test: If True, override SKIP_TESTS environment variable and run tests anyway
            
        Returns:
            Dictionary with test results summary
        """
        if os.getenv("SKIP_TESTS") and not ignore_skip_test:
            print("Skipped tests due to SKIP_TEST .env setting")
            return {"passed": 0, "failed": 0, "errors": 0, "total": 0, "skipped": True}

        print("===== IN-LAW TESTS =====")
        
        # Import specified files
        subclasses = []
        files_to_check = []
        
        # Add explicitly specified files
        if inlaw_files:
            files_to_check.extend(inlaw_files)
        
        # Add calling file automatically if no files or directories are explicitly specified
        if not inlaw_files and not inlaw_dir:
            # Get the calling file (skip current frame and InLaw.run_all frame)
            current_frame = inspect.currentframe()
            if current_frame is not None and current_frame.f_back is not None:
                calling_frame = current_frame.f_back
                if calling_frame.f_code.co_filename:
                    calling_file = calling_frame.f_code.co_filename
                    if calling_file.endswith('.py') and os.path.exists(calling_file):
                        files_to_check.append(calling_file)
                        print(f"Auto-discovered calling file: {calling_file}")
        
        # Process all files to check
        for file_path in files_to_check:
            file_classes = InLaw.get_classes_from_file(file_path)
            subclasses.extend(file_classes)
        
        # Import from directory if specified
        if inlaw_dir:
            InLaw._import_directory(directory_path=inlaw_dir)
            # After importing, get all InLaw subclasses from the imported modules
            for name, obj in inspect.getmembers(sys.modules[__name__], inspect.isclass):
                if issubclass(obj, InLaw) and obj is not InLaw:
                    if obj not in subclasses:
                        subclasses.append(obj)

        if not subclasses:
            print("No InLaw test classes found.")
            return {"passed": 0, "failed": 0, "errors": 0, "total": 0}

        passed = 0
        failed = 0
        errors = 0
        results = []
        printme = ''

        for test_class in subclasses:
            test_title = getattr(test_class, 'title', test_class.__name__)
            printme += f"▶ Running: {test_title}"

            try:
                # Suppress only the specific result_format warning during test execution
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message=r".*result_format.*configured at the Validator-level will not be persisted.*",
                        category=UserWarning
                    )
                    result = test_class.run(engine, config=config)

                if result is True:
                    printme += InLaw.ansi_green(" ✅ PASS\n")
                    passed += 1
                    results.append({"test": test_title, "status": "PASS", "message": None})
                elif isinstance(result, str):
                    printme += InLaw.ansi_red(f" ❌ FAIL: {result}\n")
                    failed += 1
                    results.append({"test": test_title, "status": "FAIL", "message": result})
                else:
                    error_msg = f"Invalid return type from test: {type(result)}. Expected bool or str."
                    printme += InLaw.ansi_red(f" 💥 ERROR: {error_msg}\n")
                    errors += 1
                    results.append({"test": test_title, "status": "ERROR", "message": error_msg})

            except Exception as e:
                error_msg = f"Exception in test: {str(e)}"
                printme += InLaw.ansi_red(f" 💥 ERROR: {error_msg}\n")
                errors += 1
                results.append({"test": test_title, "status": "ERROR", "message": error_msg})

        # Print summary
        print("=" * 44)
        summary_parts = []
        if passed > 0:
            summary_parts.append(f"{passed} passed")
        if failed > 0:
            summary_parts.append(f"{failed} failed")
        if errors > 0:
            summary_parts.append(f"{errors} errors")

        summary = "Summary: " + " · ".join(summary_parts) if summary_parts else "Summary: No tests run"
        print(summary + "\n" + printme)

        if failed > 0:
            raise InlawError(summary + " \nlook in stdout for pretty print\n " + printme)
        
        return {
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "total": passed + failed + errors,
            "results": results
        }

    @staticmethod
    def get_classes_from_file(file_path):
        """Extract InLaw subclasses from a Python file."""
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Create module spec and load
        spec = importlib.util.spec_from_file_location(file_path.stem, str(file_path))

        if spec is None or spec.loader is None:
            raise ImportError(f"Could not create module spec for {file_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Extract classes that inherit from InLaw
        classes = []
        for name, obj in inspect.getmembers(module, inspect.isclass):
            # Check if class inherits from InLaw and is not InLaw itself
            if issubclass(obj, InLaw) and obj is not InLaw:
                classes.append(obj)

        return classes

    @staticmethod
    def run_all_legacy(engine, inlaw_files: Optional[List[str]] = None, inlaw_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Legacy version of run_all that accepts engine as positional argument.
        Use run_all with named parameters instead.
        """
        return InLaw.run_all(engine=engine, inlaw_files=inlaw_files, inlaw_dir=inlaw_dir)


class InlawError(Exception):
    """Exception raised when InLaw tests fail."""
    pass
