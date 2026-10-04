"""Lightweight Great Expectations database validation helpers."""

from __future__ import annotations

import importlib.util
import inspect
import os
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Mapping, Sequence

import great_expectations as gx
import pandas as pd
import sqlalchemy
from dynaconf import Dynaconf
from great_expectations import expectations as gxe


class GXValidatorAdapter:
    """Expose common legacy ``expect_*`` methods over a GX 1.x Batch."""

    _EXPECTATION_MAP = {
        "expect_column_sum_to_be_between": gxe.ExpectColumnSumToBeBetween,
        "expect_column_values_to_be_between": gxe.ExpectColumnValuesToBeBetween,
        "expect_column_values_to_be_unique": gxe.ExpectColumnValuesToBeUnique,
        "expect_column_values_to_not_be_null": gxe.ExpectColumnValuesToNotBeNull,
        "expect_column_values_to_be_null": gxe.ExpectColumnValuesToBeNull,
        "expect_column_values_to_be_in_set": gxe.ExpectColumnValuesToBeInSet,
        "expect_column_values_to_match_regex": gxe.ExpectColumnValuesToMatchRegex,
        "expect_table_row_count_to_equal": gxe.ExpectTableRowCountToEqual,
        "expect_table_row_count_to_be_between": gxe.ExpectTableRowCountToBeBetween,
    }

    def __init__(self, batch: Any):
        self._batch = batch

    def __getattr__(self, expectation_name: str):
        expectation_class = self._EXPECTATION_MAP.get(expectation_name)
        if expectation_class is None:
            raise AttributeError(
                f"{type(self).__name__} does not map the GX expectation "
                f"{expectation_name!r}. Available: {sorted(self._EXPECTATION_MAP)}"
            )

        def _run_expectation(**expectation_arguments: Any):
            return self._batch.validate(expectation_class(**expectation_arguments))

        return _run_expectation


class InLaw(ABC):
    """Base class for database validation checks backed by Great Expectations."""

    title = "Unnamed Test"

    @staticmethod
    @abstractmethod
    def run(engine: Any, settings: Dynaconf | Mapping[str, Any] | None = None) -> bool | str:
        """Return ``True`` for success or a descriptive string for failure."""
        raise NotImplementedError

    @staticmethod
    def sql_to_gx_df(*, sql: str, engine: Any) -> GXValidatorAdapter:
        """Execute SQL and expose its result through the documented expectation API."""
        try:
            with engine.connect() as connection:
                pandas_dataframe = pd.read_sql_query(sqlalchemy.text(sql), connection)

            context = gx.get_context(mode="ephemeral")
            batch = context.data_sources.pandas_default.read_dataframe(pandas_dataframe)
            return GXValidatorAdapter(batch)
        except Exception as exception:
            raise RuntimeError(
                f"InLaw.sql_to_gx_df Error: Failed to execute SQL and create a GX batch: {exception}"
            ) from exception

    @staticmethod
    def to_gx_dataframe(sql: str, engine: Any) -> GXValidatorAdapter:
        """Backward-compatible positional alias for :meth:`sql_to_gx_df`."""
        return InLaw.sql_to_gx_df(sql=sql, engine=engine)

    @staticmethod
    def ansi_green(text: str) -> str:
        return f"\033[92m{text}\033[0m"

    @staticmethod
    def ansi_red(text: str) -> str:
        return f"\033[91m{text}\033[0m"

    @staticmethod
    def get_classes_from_file(file_path: str | Path) -> list[type[InLaw]]:
        """Load one Python file and return the InLaw subclasses defined by it."""
        resolved_file_path = Path(file_path).resolve()
        if not resolved_file_path.exists():
            raise FileNotFoundError(f"InLaw.get_classes_from_file Error: File not found: {resolved_file_path}")

        module_name = f"_inlaw_check_{abs(hash(resolved_file_path))}"
        module_spec = importlib.util.spec_from_file_location(module_name, resolved_file_path)
        if module_spec is None or module_spec.loader is None:
            raise ImportError(
                f"InLaw.get_classes_from_file Error: Could not load module spec for {resolved_file_path}"
            )

        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        return [
            class_object
            for _, class_object in inspect.getmembers(module, inspect.isclass)
            if issubclass(class_object, InLaw)
            and class_object is not InLaw
            and class_object.__module__ == module.__name__
        ]

    @staticmethod
    def _get_classes_from_directory(*, directory_path: str | Path) -> list[type[InLaw]]:
        resolved_directory_path = Path(directory_path).resolve()
        if not resolved_directory_path.is_dir():
            raise NotADirectoryError(
                f"InLaw._get_classes_from_directory Error: Directory not found: {resolved_directory_path}"
            )

        discovered_classes: list[type[InLaw]] = []
        for python_file_path in sorted(resolved_directory_path.glob("*.py")):
            if not python_file_path.name.startswith("__"):
                discovered_classes.extend(InLaw.get_classes_from_file(python_file_path))
        return discovered_classes

    @staticmethod
    def _calling_file_path() -> Path | None:
        current_frame = inspect.currentframe()
        if current_frame is None or current_frame.f_back is None or current_frame.f_back.f_back is None:
            return None
        calling_path = Path(current_frame.f_back.f_back.f_code.co_filename).resolve()
        return calling_path if calling_path.exists() else None

    @staticmethod
    def _run_test(*, test_class: type[InLaw], engine: Any, settings: Any) -> bool | str:
        """Call settings-based checks while retaining 0.1.0 config compatibility."""
        run_parameters = inspect.signature(test_class.run).parameters
        if "settings" in run_parameters:
            return test_class.run(engine, settings=settings)
        if "config" in run_parameters:
            warnings.warn(
                f"{test_class.__name__}.run(config=...) is deprecated; use settings=... instead.",
                DeprecationWarning,
                stacklevel=3,
            )
            return test_class.run(engine, config=settings)
        return test_class.run(engine)

    @staticmethod
    def run_all(
        *,
        engine: Any,
        inlaw_files: Sequence[str | Path] | None = None,
        inlaw_dir: str | Path | None = None,
        settings: Dynaconf | Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
        ignore_skip_test: bool = False,
    ) -> dict[str, Any]:
        """Discover and execute InLaw checks.

        ``config`` is retained as a deprecated alias for ``settings``. Supplying
        both is an error because the intended value would be ambiguous.
        """
        if settings is not None and config is not None:
            raise ValueError("InLaw.run_all Error: Use settings or config, not both")
        if config is not None:
            warnings.warn(
                "InLaw.run_all(config=...) is deprecated; use settings=... instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            settings = config

        if os.getenv("SKIP_TESTS") and not ignore_skip_test:
            print("Skipped tests due to SKIP_TESTS environment setting")
            return {"passed": 0, "failed": 0, "errors": 0, "total": 0, "skipped": True, "results": []}

        test_classes: list[type[InLaw]] = []
        if inlaw_files:
            for file_path in inlaw_files:
                test_classes.extend(InLaw.get_classes_from_file(file_path))
        elif inlaw_dir is not None:
            test_classes.extend(InLaw._get_classes_from_directory(directory_path=inlaw_dir))
        else:
            calling_file_path = InLaw._calling_file_path()
            if calling_file_path is not None:
                test_classes.extend(InLaw.get_classes_from_file(calling_file_path))

        test_classes = list(dict.fromkeys(test_classes))
        if not test_classes:
            print("No InLaw test classes found.")
            return {"passed": 0, "failed": 0, "errors": 0, "total": 0, "results": []}

        passed = failed = errors = 0
        results: list[dict[str, Any]] = []
        output_lines: list[str] = []
        for test_class in test_classes:
            test_title = getattr(test_class, "title", test_class.__name__)
            try:
                result = InLaw._run_test(test_class=test_class, engine=engine, settings=settings)
                if result is True:
                    passed += 1
                    output_lines.append(f"▶ Running: {test_title}{InLaw.ansi_green(' ✅ PASS')}")
                    results.append({"test": test_title, "status": "PASS", "message": None})
                elif isinstance(result, str):
                    failed += 1
                    output_lines.append(f"▶ Running: {test_title}{InLaw.ansi_red(f' ❌ FAIL: {result}')}")
                    results.append({"test": test_title, "status": "FAIL", "message": result})
                else:
                    raise TypeError(f"Invalid return type {type(result)}; expected bool or str")
            except Exception as exception:
                errors += 1
                error_message = f"{test_class.__name__} Error: {exception}"
                output_lines.append(f"▶ Running: {test_title}{InLaw.ansi_red(f' 💥 ERROR: {error_message}')}")
                results.append({"test": test_title, "status": "ERROR", "message": error_message})

        summary = f"Summary: {passed} passed · {failed} failed · {errors} errors"
        print("=" * 44)
        print(summary)
        print("\n".join(output_lines))
        if failed:
            raise InlawError(f"{summary}\n" + "\n".join(output_lines))
        return {"passed": passed, "failed": failed, "errors": errors, "total": len(results), "results": results}

    @staticmethod
    def run_all_legacy(
        engine: Any,
        inlaw_files: Sequence[str | Path] | None = None,
        inlaw_dir: str | Path | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Backward-compatible positional-engine wrapper."""
        return InLaw.run_all(
            engine=engine,
            inlaw_files=inlaw_files,
            inlaw_dir=inlaw_dir,
            config=config,
        )


class InlawError(Exception):
    """Raised when one or more validation checks report failure."""