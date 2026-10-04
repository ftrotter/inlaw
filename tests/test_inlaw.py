from pathlib import Path
import subprocess
import sys

import pytest
import sqlalchemy
from dynaconf import Dynaconf

from inlaw import GXValidatorAdapter, InLaw, InlawError


class FakeBatch:
    def __init__(self):
        self.expectations = []

    def validate(self, expectation):
        self.expectations.append(expectation)
        return type("Result", (), {"success": True, "result": {}})()


@pytest.mark.parametrize("expectation_name", sorted(GXValidatorAdapter._EXPECTATION_MAP))
def test_adapter_maps_supported_expectations(expectation_name):
    fake_batch = FakeBatch()
    adapter = GXValidatorAdapter(fake_batch)

    expectation_arguments = {}
    if "column_values" in expectation_name or "column_sum" in expectation_name:
        expectation_arguments["column"] = "value"
    if expectation_name.endswith("to_be_between"):
        expectation_arguments["min_value"] = 0
    if expectation_name == "expect_column_values_to_be_in_set":
        expectation_arguments["value_set"] = ["value"]
    if expectation_name == "expect_column_values_to_match_regex":
        expectation_arguments["regex"] = r"^value$"
    if expectation_name == "expect_table_row_count_to_equal":
        expectation_arguments["value"] = 1
    getattr(adapter, expectation_name)(**expectation_arguments)

    assert len(fake_batch.expectations) == 1


def test_adapter_rejects_unmapped_expectation():
    with pytest.raises(AttributeError, match="Available"):
        GXValidatorAdapter(FakeBatch()).expect_something_unsupported()


def test_sql_to_gx_df_runs_documented_expectation():
    engine = sqlalchemy.create_engine("sqlite://")

    gx_dataframe = InLaw.sql_to_gx_df(sql="SELECT 3 AS value", engine=engine)
    result = gx_dataframe.expect_column_values_to_be_between(
        column="value",
        min_value=1,
        max_value=5,
    )

    assert result.success is True


def test_sql_to_gx_df_runs_column_sum_expectation():
    engine = sqlalchemy.create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(sqlalchemy.text("CREATE TABLE amounts (amount INTEGER NOT NULL)"))
        connection.execute(
            sqlalchemy.text("INSERT INTO amounts (amount) VALUES (:amount)"),
            [{"amount": 2}, {"amount": 3}, {"amount": 5}],
        )

    gx_dataframe = InLaw.sql_to_gx_df(sql="SELECT amount FROM amounts", engine=engine)
    passing_result = gx_dataframe.expect_column_sum_to_be_between(
        column="amount",
        min_value=10,
        max_value=10,
    )
    failing_result = gx_dataframe.expect_column_sum_to_be_between(
        column="amount",
        min_value=11,
        max_value=20,
    )

    assert passing_result.success is True
    assert failing_result.success is False


def test_sql_to_gx_df_runs_column_regex_expectation():
    engine = sqlalchemy.create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(sqlalchemy.text("CREATE TABLE identifiers (identifier TEXT NOT NULL)"))
        connection.execute(
            sqlalchemy.text("INSERT INTO identifiers (identifier) VALUES (:identifier)"),
            [{"identifier": "NPI-123"}, {"identifier": "NPI-456"}],
        )

    gx_dataframe = InLaw.sql_to_gx_df(
        sql="SELECT identifier FROM identifiers",
        engine=engine,
    )
    passing_result = gx_dataframe.expect_column_values_to_match_regex(
        column="identifier",
        regex=r"^NPI-[0-9]{3}$",
    )
    failing_result = gx_dataframe.expect_column_values_to_match_regex(
        column="identifier",
        regex=r"^TIN-[0-9]{3}$",
    )

    assert passing_result.success is True
    assert failing_result.success is False


def test_run_all_passes_settings_and_discovers_directory(tmp_path):
    check_file = tmp_path / "settings_check.py"
    check_file.write_text(
        "from inlaw import InLaw\n"
        "class SettingsCheck(InLaw):\n"
        "    title = 'settings check'\n"
        "    @staticmethod\n"
        "    def run(engine, settings=None):\n"
        "        return settings.answer == 42\n"
    )
    settings = Dynaconf(environments=False)
    settings.set("answer", 42)

    result = InLaw.run_all(engine=object(), inlaw_dir=tmp_path, settings=settings)

    assert result["passed"] == 1
    assert result["total"] == 1


def test_run_all_discovers_calling_file(tmp_path):
    check_file = tmp_path / "automatic_check.py"
    check_file.write_text(
        "from inlaw import InLaw\n"
        "class AutomaticCheck(InLaw):\n"
        "    @staticmethod\n"
        "    def run(engine, settings=None):\n"
        "        return True\n"
        "if __name__ == '__main__':\n"
        "    result = InLaw.run_all(engine=object())\n"
        "    assert result['passed'] == 1\n"
    )

    completed_process = subprocess.run(
        [sys.executable, str(check_file)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed_process.returncode == 0, completed_process.stderr


def test_run_all_supports_legacy_config_check(tmp_path):
    check_file = tmp_path / "config_check.py"
    check_file.write_text(
        "from inlaw import InLaw\n"
        "class ConfigCheck(InLaw):\n"
        "    @staticmethod\n"
        "    def run(engine, config=None):\n"
        "        return config['answer'] == 42\n"
    )

    with pytest.warns(DeprecationWarning):
        result = InLaw.run_all(
            engine=object(),
            inlaw_files=[check_file],
            config={"answer": 42},
        )

    assert result["passed"] == 1


def test_run_all_rejects_settings_and_config_together():
    with pytest.raises(ValueError, match="not both"):
        InLaw.run_all(engine=object(), settings={}, config={})


def test_run_all_reports_errors_without_masking_them(tmp_path):
    check_file = tmp_path / "error_check.py"
    check_file.write_text(
        "from inlaw import InLaw\n"
        "class ErrorCheck(InLaw):\n"
        "    @staticmethod\n"
        "    def run(engine, settings=None):\n"
        "        raise RuntimeError('database unavailable')\n"
    )

    result = InLaw.run_all(engine=object(), inlaw_files=[check_file])

    assert result["errors"] == 1
    assert "ErrorCheck Error: database unavailable" in result["results"][0]["message"]


def test_run_all_raises_for_validation_failure(tmp_path):
    check_file = tmp_path / "failure_check.py"
    check_file.write_text(
        "from inlaw import InLaw\n"
        "class FailureCheck(InLaw):\n"
        "    @staticmethod\n"
        "    def run(engine, settings=None):\n"
        "        return 'row count was below the minimum'\n"
    )

    with pytest.raises(InlawError, match="row count was below"):
        InLaw.run_all(engine=object(), inlaw_files=[check_file])


def test_skip_tests_environment_setting(monkeypatch, tmp_path):
    monkeypatch.setenv("SKIP_TESTS", "1")

    result = InLaw.run_all(engine=object(), inlaw_dir=tmp_path)

    assert result["skipped"] is True
    assert result["total"] == 0


def test_source_tree_uses_real_inlaw_package():
    import inlaw

    assert Path(inlaw.__file__).parent.name == "inlaw"