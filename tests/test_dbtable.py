import pytest

from inlaw import DBTable, DBTableHierarchyError, DBTableValidationError


def test_dbtable_renders_and_creates_child():
    table = DBTable(database="analytics", schema="public", table="providers")

    assert str(table) == "analytics.public.providers"
    assert str(table.make_child("staging")) == "analytics.public.providers_staging"


def test_dbtable_requires_two_hierarchy_levels():
    with pytest.raises(DBTableHierarchyError):
        DBTable(table="providers")


def test_dbtable_rejects_unsafe_identifier():
    with pytest.raises(DBTableValidationError):
        DBTable(schema="public", table="providers;DROP_TABLE")